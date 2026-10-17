package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/harshmangalamv/the-junction/internal/jsonrpc"
	"github.com/harshmangalamv/the-junction/internal/mcp"
)

// Client is a minimal Streamable HTTP MCP client for either era. Junction
// uses it for the demo traffic generator and in tests.
type Client struct {
	URL     string
	Era     mcp.Era
	Info    mcp.Implementation
	Caps    map[string]any
	Token   string
	session string
	version string
	id      atomic.Int64
	HTTP    *http.Client
}

// Connect performs the legacy initialize handshake (no-op for modern).
func (c *Client) Connect(ctx context.Context) error {
	if c.HTTP == nil {
		c.HTTP = http.DefaultClient
	}
	if c.Caps == nil {
		c.Caps = map[string]any{}
	}
	if c.Era == mcp.EraModern {
		c.version = mcp.VersionModern
		_, err := c.Call(ctx, "server/discover", nil)
		return err
	}
	c.version = mcp.VersionLatestLegacy
	res, hdr, err := c.post(ctx, "initialize", map[string]any{
		"protocolVersion": mcp.VersionLatestLegacy, "capabilities": c.Caps, "clientInfo": c.Info,
	}, false)
	if err != nil {
		return err
	}
	c.session = hdr.Get(mcp.HeaderSessionID)
	var r struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	_ = json.Unmarshal(res, &r)
	c.version = r.ProtocolVersion
	n, _ := jsonrpc.NewNotification("notifications/initialized", nil)
	b, _ := json.Marshal(n)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(b))
	c.headers(req, "", "")
	resp, err := c.HTTP.Do(req)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

// Call sends a request and returns its result.
func (c *Client) Call(ctx context.Context, method string, params map[string]any) (json.RawMessage, error) {
	res, _, err := c.post(ctx, method, params, c.Era == mcp.EraModern)
	return res, err
}

func (c *Client) headers(req *http.Request, method, name string) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if c.version != "" {
		req.Header.Set(mcp.HeaderProtocolVersion, c.version)
	}
	if c.session != "" {
		req.Header.Set(mcp.HeaderSessionID, c.session)
	}
	if c.Era == mcp.EraModern && method != "" {
		req.Header.Set(mcp.HeaderMethod, method)
		if name != "" {
			req.Header.Set(mcp.HeaderName, name)
		}
	}
}

func (c *Client) post(ctx context.Context, method string, params map[string]any, modern bool) (json.RawMessage, http.Header, error) {
	if params == nil {
		params = map[string]any{}
	}
	if modern {
		params["_meta"] = map[string]any{
			mcp.MetaProtocolVersion:    mcp.VersionModern,
			mcp.MetaClientInfo:         c.Info,
			mcp.MetaClientCapabilities: c.Caps,
		}
	}
	req0, _ := jsonrpc.NewRequest(c.id.Add(1), method, params)
	b, _ := json.Marshal(req0)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(b))
	if err != nil {
		return nil, nil, err
	}
	name := ""
	switch method {
	case "tools/call", "prompts/get":
		name, _ = params["name"].(string)
	case "resources/read":
		name, _ = params["uri"].(string)
	}
	if method == "initialize" {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if c.Token != "" {
			req.Header.Set("Authorization", "Bearer "+c.Token)
		}
	} else {
		c.headers(req, method, name)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	m, perr := jsonrpc.Parse(body)
	if perr != nil {
		return nil, resp.Header, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	if m.Error != nil {
		return nil, resp.Header, m.Error
	}
	return m.Result, resp.Header, nil
}
