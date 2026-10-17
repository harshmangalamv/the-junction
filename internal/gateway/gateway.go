// Package gateway is Junction's core: it accepts MCP clients of either
// protocol era, aggregates the upstream servers' tools, prompts and
// resources, applies policy and redaction, and records every exchange.
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/harshmangalamv/the-junction/internal/config"
	"github.com/harshmangalamv/the-junction/internal/jsonrpc"
	"github.com/harshmangalamv/the-junction/internal/mcp"
	"github.com/harshmangalamv/the-junction/internal/policy"
	"github.com/harshmangalamv/the-junction/internal/recorder"
	"github.com/harshmangalamv/the-junction/internal/redact"
	"github.com/harshmangalamv/the-junction/internal/upstream"
)

// Version is set at build time.
var Version = "dev"

// Gateway ties everything together.
type Gateway struct {
	cfg      *config.Config
	clients  []*upstream.Client
	byName   map[string]*upstream.Client
	policy   *policy.Policy
	redactor *redact.Redactor
	Rec      *recorder.Recorder
	sessions *sessionStore
	log      *log.Logger

	catMu     sync.RWMutex
	catalog   *catalog
	refreshMu sync.Mutex
	stale     atomic.Bool
}

// New builds a gateway from config. It does not connect yet.
func New(cfg *config.Config, logger *log.Logger) (*Gateway, error) {
	if logger == nil {
		logger = log.New(os.Stderr, "junction ", log.LstdFlags)
	}
	rec, err := recorder.New(cfg.Resolve(cfg.Record.File), cfg.Record.Keep, cfg.Record.MaxBody)
	if err != nil {
		return nil, fmt.Errorf("recorder: %w", err)
	}
	upstream.ClientInfo.Version = Version
	g := &Gateway{
		cfg: cfg, byName: map[string]*upstream.Client{},
		policy: policy.New(cfg.Policy), redactor: redact.New(cfg.Redact),
		Rec: rec, sessions: newSessionStore(), log: logger, catalog: emptyCatalog(),
	}
	self, _ := os.Executable()
	for _, s := range cfg.Servers {
		if s.Disabled {
			continue
		}
		if s.Dir != "" {
			s.Dir = cfg.Resolve(s.Dir)
		}
		c := upstream.New(s, self, g.onUpstreamNotification)
		g.clients = append(g.clients, c)
		g.byName[s.Name] = c
	}
	return g, nil
}

// Start connects to every upstream (in parallel) and builds the catalog.
// Servers that fail to connect are retried lazily on later requests.
func (g *Gateway) Start(ctx context.Context) {
	var wg sync.WaitGroup
	for _, c := range g.clients {
		wg.Add(1)
		go func(c *upstream.Client) {
			defer wg.Done()
			if err := c.Connect(ctx); err != nil {
				g.log.Printf("server %s: connect failed: %v", c.Name(), err)
				return
			}
			st := c.Status()
			g.log.Printf("server %s: connected (%s era, protocol %s)", c.Name(), st.Era, st.Version)
		}(c)
	}
	wg.Wait()
	cat := g.refresh(ctx)
	g.log.Printf("catalog: %d tools, %d prompts, %d resources from %d servers", len(cat.tools), len(cat.prompts), len(cat.resources), len(g.clients))
	go g.sessions.janitor(ctx, 2*time.Hour)
}

// Close disconnects all upstreams and flushes the recording.
func (g *Gateway) Close() {
	for _, c := range g.clients {
		c.Close()
	}
	_ = g.Rec.Close()
}

// Config returns the gateway configuration.
func (g *Gateway) Config() *config.Config { return g.cfg }

// ClientCtx describes the client a request came from.
type ClientCtx struct {
	Era       mcp.Era
	Version   string
	Info      mcp.Implementation
	Caps      json.RawMessage // client capabilities
	SessionID string
	Transport string // http | stdio
}

// exchange collects what the recorder needs while a request is handled.
type exchange struct {
	server, tool, upstreamName string
	serverEra                  mcp.Era
	status                     string
	isError                    bool
	redactions                 redact.Findings
}

// Handle processes one request and returns its response.
func (g *Gateway) Handle(ctx context.Context, cc *ClientCtx, req *jsonrpc.Message) *jsonrpc.Message {
	start := time.Now()
	ex := &exchange{status: "ok", redactions: redact.Findings{}}
	resp := g.dispatch(ctx, cc, req, ex)
	if resp.Error != nil && ex.status == "ok" {
		ex.status = "error"
	}
	if resp.Error == nil {
		resp.Result = adaptResult(cc, req.Method, resp.Result)
	}
	g.record(cc, req, resp, ex, time.Since(start))
	return resp
}

func (g *Gateway) dispatch(ctx context.Context, cc *ClientCtx, req *jsonrpc.Message, ex *exchange) *jsonrpc.Message {
	p, err := mcp.DecodeParams(req.Params)
	if err != nil {
		return jsonrpc.NewErrorResponse(req.ID, jsonrpc.NewError(jsonrpc.CodeInvalidParams, "params must be an object", nil))
	}
	switch req.Method {
	case "initialize":
		return g.handleInitialize(cc, req, p)
	case "server/discover":
		return jsonrpc.NewResult(req.ID, g.discoverResult())
	case "ping", "logging/setLevel":
		return jsonrpc.NewResult(req.ID, json.RawMessage(`{}`))
	case "tools/list":
		return g.handleToolsList(ctx, req)
	case "tools/call":
		return g.handleToolsCall(ctx, cc, req, p, ex)
	case "prompts/list":
		cat := g.refresh(ctx)
		items := make([]json.RawMessage, 0, len(cat.promptOrd))
		for _, n := range cat.promptOrd {
			items = append(items, cat.prompts[n].Def)
		}
		return jsonrpc.NewResult(req.ID, map[string]any{"prompts": items})
	case "prompts/get":
		return g.handlePromptsGet(ctx, cc, req, p, ex)
	case "resources/list":
		cat := g.refresh(ctx)
		return jsonrpc.NewResult(req.ID, map[string]any{"resources": nonNil(cat.resources)})
	case "resources/templates/list":
		cat := g.refresh(ctx)
		return jsonrpc.NewResult(req.ID, map[string]any{"resourceTemplates": nonNil(cat.templates)})
	case "resources/read":
		return g.handleResourcesRead(ctx, cc, req, p, ex)
	case "completion/complete":
		return g.handleComplete(ctx, cc, req, p, ex)
	}
	return jsonrpc.NewErrorResponse(req.ID, jsonrpc.NewError(jsonrpc.CodeMethodNotFound, "method not found: "+req.Method, nil))
}

func nonNil(v []json.RawMessage) []json.RawMessage {
	if v == nil {
		return []json.RawMessage{}
	}
	return v
}

func (g *Gateway) serverInfo() mcp.Implementation {
	return mcp.Implementation{Name: "junction", Title: "Junction MCP Gateway", Version: Version}
}

func (g *Gateway) capabilities() map[string]any {
	return map[string]any{
		"tools":       map[string]any{"listChanged": false},
		"prompts":     map[string]any{"listChanged": false},
		"resources":   map[string]any{"subscribe": false, "listChanged": false},
		"completions": map[string]any{},
	}
}

func (g *Gateway) instructions() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Junction gateway in front of %d MCP servers. Tool and prompt names are prefixed with the server name and %q.", len(g.clients), g.cfg.Separator)
	for _, c := range g.clients {
		if ins := strings.TrimSpace(c.Instructions()); ins != "" {
			fmt.Fprintf(&b, "\n\n[%s] %s", c.Name(), ins)
		}
	}
	return b.String()
}

func (g *Gateway) handleInitialize(cc *ClientCtx, req *jsonrpc.Message, p mcp.Params) *jsonrpc.Message {
	requested := p.String("protocolVersion")
	version := mcp.VersionLatestLegacy
	if mcp.IsLegacyVersion(requested) {
		version = requested
	}
	cc.Era, cc.Version = mcp.EraLegacy, version
	_ = json.Unmarshal(p["clientInfo"], &cc.Info)
	cc.Caps = p["capabilities"]
	return jsonrpc.NewResult(req.ID, map[string]any{
		"protocolVersion": version,
		"capabilities":    g.capabilities(),
		"serverInfo":      g.serverInfo(),
		"instructions":    g.instructions(),
	})
}

func (g *Gateway) discoverResult() map[string]any {
	return map[string]any{
		"supportedVersions": mcp.AllVersions(),
		"capabilities":      g.capabilities(),
		"instructions":      g.instructions(),
	}
}

func (g *Gateway) handleToolsList(ctx context.Context, req *jsonrpc.Message) *jsonrpc.Message {
	cat := g.refresh(ctx)
	tools := make([]json.RawMessage, 0, len(cat.toolOrder))
	for _, n := range cat.toolOrder {
		if t := cat.tools[n]; t.Decision.Allowed {
			tools = append(tools, t.Def)
		}
	}
	return jsonrpc.NewResult(req.ID, map[string]any{"tools": tools})
}

// lookupTool finds a tool, refreshing the catalog once if it is unknown.
func (g *Gateway) lookupTool(ctx context.Context, name string) *toolEntry {
	if t, ok := g.cat().tools[name]; ok {
		return t
	}
	if t, ok := g.refresh(ctx).tools[name]; ok {
		return t
	}
	return nil
}

// upstreamCaps are the capabilities forwarded to a modern upstream: the
// client's own, so the server never asks for input the client can't give.
func upstreamCaps(cc *ClientCtx) json.RawMessage {
	if cc.Era == mcp.EraModern && len(cc.Caps) > 0 {
		return cc.Caps
	}
	return json.RawMessage(`{}`)
}

func toolErrorResult(id json.RawMessage, text string) *jsonrpc.Message {
	return jsonrpc.NewResult(id, map[string]any{
		"content": []map[string]any{{"type": "text", "text": text}},
		"isError": true,
	})
}

func (g *Gateway) handleToolsCall(ctx context.Context, cc *ClientCtx, req *jsonrpc.Message, p mcp.Params, ex *exchange) *jsonrpc.Message {
	name := p.String("name")
	ex.tool = name
	t := g.lookupTool(ctx, name)
	if t == nil {
		return jsonrpc.NewErrorResponse(req.ID, jsonrpc.NewError(jsonrpc.CodeInvalidParams, "unknown tool: "+name, nil))
	}
	ex.server, ex.upstreamName = t.Server, t.UpstreamName
	if !t.Decision.Allowed {
		ex.status = "blocked"
		return toolErrorResult(req.ID, fmt.Sprintf("Junction blocked this call: tool %q is not permitted by gateway policy (%s).", name, t.Decision.Rule))
	}
	c := g.byName[t.Server]
	ex.serverEra = c.Era()
	fwd := mcp.StripEraMeta(p)
	fwd.Set("name", t.UpstreamName)
	resp, err := c.Request(ctx, "tools/call", fwd, upstreamCaps(cc))
	if err != nil {
		return jsonrpc.NewErrorResponse(req.ID, jsonrpc.NewError(jsonrpc.CodeInternalError, err.Error(), nil))
	}
	return g.finish(cc, req, resp, ex, true)
}

func (g *Gateway) handlePromptsGet(ctx context.Context, cc *ClientCtx, req *jsonrpc.Message, p mcp.Params, ex *exchange) *jsonrpc.Message {
	name := p.String("name")
	ex.tool = name
	pe, ok := g.cat().prompts[name]
	if !ok {
		pe, ok = g.refresh(ctx).prompts[name]
	}
	if !ok {
		return jsonrpc.NewErrorResponse(req.ID, jsonrpc.NewError(jsonrpc.CodeInvalidParams, "unknown prompt: "+name, nil))
	}
	ex.server, ex.upstreamName = pe.Server, pe.UpstreamName
	c := g.byName[pe.Server]
	ex.serverEra = c.Era()
	fwd := mcp.StripEraMeta(p)
	fwd.Set("name", pe.UpstreamName)
	resp, err := c.Request(ctx, "prompts/get", fwd, upstreamCaps(cc))
	if err != nil {
		return jsonrpc.NewErrorResponse(req.ID, jsonrpc.NewError(jsonrpc.CodeInternalError, err.Error(), nil))
	}
	return g.finish(cc, req, resp, ex, false)
}

func (g *Gateway) handleResourcesRead(ctx context.Context, cc *ClientCtx, req *jsonrpc.Message, p mcp.Params, ex *exchange) *jsonrpc.Message {
	uri := p.String("uri")
	ex.tool = uri
	owner, ok := g.cat().resOwner[uri]
	if !ok {
		owner, ok = g.refresh(ctx).resOwner[uri]
	}
	candidates := g.clients
	if ok {
		candidates = []*upstream.Client{g.byName[owner]}
	}
	fwd := mcp.StripEraMeta(p)
	var last *jsonrpc.Message
	for _, c := range candidates {
		// Unknown URIs (e.g. from templates): try servers in order.
		resp, err := c.Request(ctx, "resources/read", fwd, upstreamCaps(cc))
		if err != nil {
			continue
		}
		last = resp
		ex.server, ex.serverEra = c.Name(), c.Era()
		if resp.Error == nil {
			return g.finish(cc, req, resp, ex, false)
		}
	}
	if last != nil {
		return g.finish(cc, req, last, ex, false)
	}
	return jsonrpc.NewErrorResponse(req.ID, jsonrpc.NewError(jsonrpc.CodeInvalidParams, "resource not found: "+uri, map[string]string{"uri": uri}))
}

func (g *Gateway) handleComplete(ctx context.Context, cc *ClientCtx, req *jsonrpc.Message, p mcp.Params, ex *exchange) *jsonrpc.Message {
	var ref struct {
		Type string `json:"type"`
		Name string `json:"name"`
		URI  string `json:"uri"`
	}
	_ = json.Unmarshal(p["ref"], &ref)
	fwd := mcp.StripEraMeta(p)
	var c *upstream.Client
	switch ref.Type {
	case "ref/prompt":
		if pe, ok := g.refresh(ctx).prompts[ref.Name]; ok {
			c = g.byName[pe.Server]
			fwd.Set("ref", map[string]string{"type": ref.Type, "name": pe.UpstreamName})
		}
	case "ref/resource":
		if owner, ok := g.cat().resOwner[ref.URI]; ok {
			c = g.byName[owner]
		}
	}
	if c == nil {
		return jsonrpc.NewResult(req.ID, map[string]any{"completion": map[string]any{"values": []string{}}})
	}
	ex.server, ex.serverEra = c.Name(), c.Era()
	resp, err := c.Request(ctx, "completion/complete", fwd, upstreamCaps(cc))
	if err != nil {
		return jsonrpc.NewErrorResponse(req.ID, jsonrpc.NewError(jsonrpc.CodeInternalError, err.Error(), nil))
	}
	return g.finish(cc, req, resp, ex, false)
}

// finish turns an upstream response into the client's response: it
// bridges input_required results, applies redaction and copies the id.
func (g *Gateway) finish(cc *ClientCtx, req *jsonrpc.Message, up *jsonrpc.Message, ex *exchange, isTool bool) *jsonrpc.Message {
	if up.Error != nil {
		return jsonrpc.NewErrorResponse(req.ID, up.Error)
	}
	result := up.Result
	if mcp.ResultType(result) == mcp.ResultTypeInputRequired {
		ex.status = "input_required"
		if cc.Era != mcp.EraModern {
			// A legacy client cannot answer a multi round-trip request.
			msg := "The upstream server needs interactive input (elicitation/sampling) to complete this request. Junction does not yet bridge that to " + cc.Version + " clients; connect with a 2026-07-28 client."
			if isTool {
				return toolErrorResult(req.ID, msg)
			}
			return jsonrpc.NewErrorResponse(req.ID, jsonrpc.NewError(jsonrpc.CodeInternalError, msg, nil))
		}
		// Modern client: pass inputRequests/requestState through unchanged.
		return &jsonrpc.Message{JSONRPC: "2.0", ID: req.ID, Result: result}
	}
	redacted, f := g.redactor.JSON(result)
	for k, v := range f {
		ex.redactions[k] += v
	}
	if isTool {
		var r struct {
			IsError bool `json:"isError"`
		}
		_ = json.Unmarshal(redacted, &r)
		ex.isError = r.IsError
	}
	return &jsonrpc.Message{JSONRPC: "2.0", ID: req.ID, Result: redacted}
}

// adaptResult shapes a result for the client's era: modern results carry
// resultType, serverInfo and cache hints; legacy results must not.
func adaptResult(cc *ClientCtx, method string, raw json.RawMessage) json.RawMessage {
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil || m == nil {
		return raw
	}
	if cc.Era == mcp.EraModern {
		if _, ok := m["resultType"]; !ok {
			m["resultType"], _ = json.Marshal(mcp.ResultTypeComplete)
		}
		meta := map[string]json.RawMessage{}
		_ = json.Unmarshal(m["_meta"], &meta)
		meta[mcp.MetaServerInfo], _ = json.Marshal(mcp.Implementation{Name: "junction", Version: Version})
		m["_meta"], _ = json.Marshal(meta)
		if mcp.CacheableMethods[method] {
			if _, ok := m["ttlMs"]; !ok {
				m["ttlMs"] = json.RawMessage(`30000`)
			}
			if _, ok := m["cacheScope"]; !ok {
				m["cacheScope"] = json.RawMessage(`"private"`)
			}
		}
	} else {
		delete(m, "resultType")
		delete(m, "ttlMs")
		delete(m, "cacheScope")
		if meta, ok := m["_meta"]; ok {
			var mm map[string]json.RawMessage
			if json.Unmarshal(meta, &mm) == nil {
				delete(mm, mcp.MetaServerInfo)
				if len(mm) == 0 {
					delete(m, "_meta")
				} else {
					m["_meta"], _ = json.Marshal(mm)
				}
			}
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return raw
	}
	return b
}

func (g *Gateway) record(cc *ClientCtx, req, resp *jsonrpc.Message, ex *exchange, d time.Duration) {
	params, pf := g.redactor.JSON(req.Params) // never store raw PII from requests either
	for k, v := range pf {
		ex.redactions[k] += v
	}
	e := recorder.Entry{
		Time: time.Now().Add(-d), Kind: "request",
		Client: clientLabel(cc), ClientEra: cc.Era.String(), Version: cc.Version, Session: cc.SessionID,
		Method: req.Method, Server: ex.server, Tool: ex.tool, Upstream: ex.upstreamName,
		Params: params, Result: resp.Result, Status: ex.status, IsError: ex.isError,
		DurationMs: float64(d.Microseconds()) / 1000,
	}
	if ex.serverEra != mcp.EraUnknown {
		e.ServerEra = ex.serverEra.String()
		e.Bridged = ex.serverEra != cc.Era
	}
	if len(ex.redactions) > 0 {
		e.Redactions = ex.redactions
	}
	if resp.Error != nil {
		e.Error, e.ErrorCode = resp.Error.Message, resp.Error.Code
	}
	g.Rec.Add(e)
}

func clientLabel(cc *ClientCtx) string {
	if cc.Info.Name == "" {
		return "unknown"
	}
	if cc.Info.Version != "" {
		return cc.Info.Name + " " + cc.Info.Version
	}
	return cc.Info.Name
}

func (g *Gateway) onUpstreamNotification(server string, m *jsonrpc.Message) {
	params, _ := g.redactor.JSON(m.Params)
	g.Rec.Add(recorder.Entry{Time: time.Now(), Kind: "notification", Method: m.Method, Server: server, Params: params, Status: "ok"})
	if m.Method == "notifications/tools/list_changed" || m.Method == "notifications/prompts/list_changed" || m.Method == "notifications/resources/list_changed" {
		g.stale.Store(true) // force a refresh on the next list
	}
}
