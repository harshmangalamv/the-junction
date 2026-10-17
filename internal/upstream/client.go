// Package upstream connects Junction to the MCP servers behind it.
//
// Each Client detects which protocol era its server speaks (the
// "dual-era client" behaviour from the 2026-07-28 spec): it probes with
// server/discover and falls back to the legacy initialize handshake.
package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/harshmangalamv/the-junction/internal/config"
	"github.com/harshmangalamv/the-junction/internal/jsonrpc"
	"github.com/harshmangalamv/the-junction/internal/mcp"
)

// ClientInfo is how Junction identifies itself to upstream servers.
var ClientInfo = mcp.Implementation{Name: "junction", Version: "dev"}

// probeTimeout bounds the stdio server/discover probe. Legacy servers that
// ignore unknown pre-initialize requests make us wait this long once.
var probeTimeout = 3 * time.Second

// Client is a connection to one upstream server.
type Client struct {
	cfg      config.Server
	self     string
	Logs     *LogRing
	onNotify func(server string, m *jsonrpc.Message)

	mu           sync.Mutex
	t            transport
	era          mcp.Era
	version      string
	sessionID    string
	serverInfo   mcp.Implementation
	capabilities json.RawMessage
	instructions string
	connectedAt  time.Time
	lastErr      string
	lastProbe    time.Time

	nextID atomic.Int64
}

// New creates a client. self is the path of the junction binary, used to
// expand "{self}" in commands.
func New(cfg config.Server, self string, onNotify func(string, *jsonrpc.Message)) *Client {
	return &Client{cfg: cfg, self: self, Logs: NewLogRing(500), onNotify: onNotify}
}

// Name returns the configured server name.
func (c *Client) Name() string { return c.cfg.Name }

// Config returns the server's configuration.
func (c *Client) Config() config.Server { return c.cfg }

// Era returns the detected protocol era.
func (c *Client) Era() mcp.Era {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.era
}

// Instructions returns the server's instructions, if any.
func (c *Client) Instructions() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.instructions
}

// Connect starts the transport and detects the server's era.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectLocked(ctx)
}

func (c *Client) connectLocked(ctx context.Context) error {
	c.lastProbe = time.Now()
	if c.t != nil {
		_ = c.t.close()
		c.t = nil
	}
	c.era, c.version, c.sessionID = mcp.EraUnknown, "", ""
	notify := func(m *jsonrpc.Message) {
		if c.onNotify != nil {
			c.onNotify(c.cfg.Name, m)
		}
	}
	switch c.cfg.Transport() {
	case "http":
		c.t = newHTTP(c.cfg.URL, c.cfg.Headers, c.Logs, notify)
	default:
		cmd := make([]string, len(c.cfg.Command))
		for i, a := range c.cfg.Command {
			cmd[i] = strings.ReplaceAll(a, "{self}", c.self)
		}
		t, err := startStdio(cmd, c.cfg.Env, c.cfg.Dir, c.Logs, notify)
		if err != nil {
			return c.fail(err)
		}
		c.t = t
	}

	var err error
	switch c.cfg.Era {
	case "modern":
		err = c.discoverLocked(ctx, c.cfg.Timeout)
	case "legacy":
		err = c.initializeLocked(ctx)
	default:
		err = c.discoverLocked(ctx, probeTimeout)
		if err != nil {
			var me *modernError
			if errors.As(err, &me) {
				// A modern server that rejected us: no point falling back.
				return c.fail(err)
			}
			c.Logs.Add("junction", "server/discover probe failed ("+err.Error()+"); falling back to initialize")
			if st, ok := c.t.(*stdioTransport); ok && !st.alive() {
				// Some legacy servers exit on unknown methods; restart once.
				_ = c.t.close()
				cmd := make([]string, len(c.cfg.Command))
				for i, a := range c.cfg.Command {
					cmd[i] = strings.ReplaceAll(a, "{self}", c.self)
				}
				t, serr := startStdio(cmd, c.cfg.Env, c.cfg.Dir, c.Logs, notify)
				if serr != nil {
					return c.fail(serr)
				}
				c.t = t
			}
			err = c.initializeLocked(ctx)
		}
	}
	if err != nil {
		return c.fail(err)
	}
	c.connectedAt = time.Now()
	c.lastErr = ""
	c.Logs.Add("junction", fmt.Sprintf("connected: %s era, protocol %s, server %s %s", c.era, c.version, c.serverInfo.Name, c.serverInfo.Version))
	return nil
}

func (c *Client) fail(err error) error {
	c.lastErr = err.Error()
	c.Logs.Add("junction", "connect failed: "+err.Error())
	if c.t != nil {
		_ = c.t.close()
		c.t = nil
	}
	return err
}

// modernError marks a recognized 2026-07-28 protocol error during probing.
type modernError struct{ e *jsonrpc.Error }

func (m *modernError) Error() string { return "modern server rejected request: " + m.e.Message }

func (c *Client) discoverLocked(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	p := mcp.Params{}
	c.addModernMeta(p, nil)
	req, _ := jsonrpc.NewRequest(c.nextID.Add(1), "server/discover", p.Raw())
	resp, err := c.t.roundTrip(ctx, req, headers{protocolVersion: mcp.VersionModern, method: "server/discover"})
	if err != nil {
		return err
	}
	if resp.Error != nil {
		if jsonrpc.IsModernErrorCode(resp.Error.Code) {
			// e.g. UnsupportedProtocolVersion: the server is modern but does
			// not speak our version. If it lists a legacy version, use that.
			var d struct {
				Supported []string `json:"supported"`
			}
			_ = json.Unmarshal(resp.Error.Data, &d)
			for _, v := range d.Supported {
				if mcp.IsLegacyVersion(v) {
					return fmt.Errorf("server prefers legacy version %s", v)
				}
			}
			return &modernError{resp.Error}
		}
		return resp.Error
	}
	var r struct {
		SupportedVersions []string                   `json:"supportedVersions"`
		Capabilities      json.RawMessage            `json:"capabilities"`
		Instructions      string                     `json:"instructions"`
		Meta              map[string]json.RawMessage `json:"_meta"`
	}
	if err := json.Unmarshal(resp.Result, &r); err != nil {
		return fmt.Errorf("bad server/discover result: %w", err)
	}
	ok := false
	for _, v := range r.SupportedVersions {
		if mcp.IsModernVersion(v) {
			ok = true
		}
	}
	if !ok {
		return fmt.Errorf("server/discover lists no version Junction speaks: %v", r.SupportedVersions)
	}
	c.era, c.version = mcp.EraModern, mcp.VersionModern
	c.capabilities, c.instructions = r.Capabilities, r.Instructions
	c.serverInfo = mcp.Implementation{}
	if b, ok := r.Meta[mcp.MetaServerInfo]; ok {
		_ = json.Unmarshal(b, &c.serverInfo)
	}
	return nil
}

func (c *Client) initializeLocked(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	if ht, ok := c.t.(*httpTransport); ok {
		ht.resetSession()
	}
	req, _ := jsonrpc.NewRequest(c.nextID.Add(1), "initialize", map[string]any{
		"protocolVersion": mcp.VersionLatestLegacy,
		"capabilities":    map[string]any{"roots": map[string]any{"listChanged": false}},
		"clientInfo":      ClientInfo,
	})
	resp, err := c.t.roundTrip(ctx, req, headers{})
	if err != nil {
		return err
	}
	if resp.Error != nil {
		return resp.Error
	}
	var r struct {
		ProtocolVersion string             `json:"protocolVersion"`
		Capabilities    json.RawMessage    `json:"capabilities"`
		ServerInfo      mcp.Implementation `json:"serverInfo"`
		Instructions    string             `json:"instructions"`
	}
	if err := json.Unmarshal(resp.Result, &r); err != nil {
		return fmt.Errorf("bad initialize result: %w", err)
	}
	if !mcp.IsLegacyVersion(r.ProtocolVersion) {
		return fmt.Errorf("server negotiated unsupported protocol version %q", r.ProtocolVersion)
	}
	c.era, c.version = mcp.EraLegacy, r.ProtocolVersion
	c.capabilities, c.serverInfo, c.instructions = r.Capabilities, r.ServerInfo, r.Instructions
	if ht, ok := c.t.(*httpTransport); ok {
		c.sessionID = ht.session()
	}
	n, _ := jsonrpc.NewNotification("notifications/initialized", nil)
	return c.t.notify(ctx, n, c.legacyHeaders())
}

func (c *Client) legacyHeaders() headers {
	h := headers{sessionID: c.sessionID}
	// The MCP-Protocol-Version header exists from 2025-06-18 on.
	if c.version != "2025-03-26" && c.version != "2024-11-05" {
		h.protocolVersion = c.version
	}
	return h
}

func (c *Client) addModernMeta(p mcp.Params, clientCaps json.RawMessage) {
	m := p.Meta()
	m[mcp.MetaProtocolVersion], _ = json.Marshal(mcp.VersionModern)
	if len(clientCaps) == 0 {
		clientCaps = json.RawMessage(`{}`)
	}
	m[mcp.MetaClientCapabilities] = clientCaps
	m[mcp.MetaClientInfo], _ = json.Marshal(ClientInfo)
	p.SetMeta(m)
}

// Request forwards one request upstream. params should already have the
// caller's era metadata stripped; clientCaps are the end client's declared
// capabilities (forwarded to modern servers so they never ask a client for
// input it cannot give).
func (c *Client) Request(ctx context.Context, method string, params mcp.Params, clientCaps json.RawMessage) (*jsonrpc.Message, error) {
	c.mu.Lock()
	if c.t == nil || !c.aliveLocked() {
		// Reconnect at most every 5 seconds so a dead server does not
		// stall every request.
		if time.Since(c.lastProbe) < 5*time.Second && c.t == nil {
			err := c.lastErr
			c.mu.Unlock()
			return nil, fmt.Errorf("server %s unavailable: %s", c.cfg.Name, err)
		}
		if err := c.connectLocked(ctx); err != nil {
			c.mu.Unlock()
			return nil, fmt.Errorf("server %s unavailable: %w", c.cfg.Name, err)
		}
	}
	t, era := c.t, c.era
	h := c.legacyHeaders()
	c.mu.Unlock()

	if params == nil {
		params = mcp.Params{}
	}
	if era == mcp.EraModern {
		c.addModernMeta(params, clientCaps)
		h = headers{protocolVersion: mcp.VersionModern}
	}
	h.method = method
	switch method {
	case "tools/call", "prompts/get":
		h.name = params.String("name")
	case "resources/read":
		h.name = params.String("uri")
	}
	if era == mcp.EraLegacy {
		h.method, h.name = "", ""
	}

	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	req, _ := jsonrpc.NewRequest(c.nextID.Add(1), method, params.Raw())
	resp, err := t.roundTrip(ctx, req, h)

	var se *HTTPStatusError
	if era == mcp.EraLegacy && errors.As(err, &se) && se.Status == 404 && h.sessionID != "" {
		// Legacy session expired upstream: re-initialize once and retry.
		c.Logs.Add("junction", "upstream session expired; re-initializing")
		c.mu.Lock()
		rerr := c.initializeLocked(ctx)
		h = c.legacyHeaders()
		c.mu.Unlock()
		if rerr != nil {
			return nil, rerr
		}
		req, _ = jsonrpc.NewRequest(c.nextID.Add(1), method, params.Raw())
		resp, err = t.roundTrip(ctx, req, h)
	}
	if err != nil {
		c.mu.Lock()
		c.lastErr = err.Error()
		c.mu.Unlock()
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("server %s timed out after %s", c.cfg.Name, c.cfg.Timeout)
		}
		return nil, err
	}
	return resp, nil
}

func (c *Client) aliveLocked() bool {
	if st, ok := c.t.(*stdioTransport); ok {
		return st.alive()
	}
	return c.era != mcp.EraUnknown
}

// Close shuts the connection down.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.t == nil {
		return
	}
	if ht, ok := c.t.(*httpTransport); ok && c.era == mcp.EraLegacy {
		ht.terminate(c.sessionID)
	}
	_ = c.t.close()
	c.t = nil
}

// Status is a snapshot for the dashboard.
type Status struct {
	Name         string             `json:"name"`
	Transport    string             `json:"transport"`
	Target       string             `json:"target"`
	Era          mcp.Era            `json:"era"`
	Version      string             `json:"version"`
	Connected    bool               `json:"connected"`
	ServerInfo   mcp.Implementation `json:"serverInfo"`
	Capabilities json.RawMessage    `json:"capabilities,omitempty"`
	Instructions string             `json:"instructions,omitempty"`
	ConnectedAt  time.Time          `json:"connectedAt"`
	LastError    string             `json:"lastError,omitempty"`
}

// Status returns the current connection state.
func (c *Client) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	target := c.cfg.URL
	if target == "" {
		target = strings.ReplaceAll(strings.Join(c.cfg.Command, " "), "{self}", "junction")
	}
	return Status{
		Name: c.cfg.Name, Transport: c.cfg.Transport(), Target: target,
		Era: c.era, Version: c.version,
		Connected:  c.t != nil && c.aliveLocked(),
		ServerInfo: c.serverInfo, Capabilities: c.capabilities, Instructions: c.instructions,
		ConnectedAt: c.connectedAt, LastError: c.lastErr,
	}
}
