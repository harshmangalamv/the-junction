package gateway

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/harshmangalamv/the-junction/internal/jsonrpc"
	"github.com/harshmangalamv/the-junction/internal/mcp"
	"github.com/harshmangalamv/the-junction/internal/upstream"
)

// ---- legacy sessions ----------------------------------------------------

type session struct {
	ctx      ClientCtx
	lastSeen time.Time
}

type sessionStore struct {
	mu sync.Mutex
	m  map[string]*session
}

func newSessionStore() *sessionStore { return &sessionStore{m: map[string]*session{}} }

func newSessionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *sessionStore) put(id string, cc ClientCtx) {
	s.mu.Lock()
	s.m[id] = &session{ctx: cc, lastSeen: time.Now()}
	s.mu.Unlock()
}

func (s *sessionStore) get(id string) (ClientCtx, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss, ok := s.m[id]
	if !ok {
		return ClientCtx{}, false
	}
	ss.lastSeen = time.Now()
	return ss.ctx, true
}

func (s *sessionStore) del(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[id]
	delete(s.m, id)
	return ok
}

func (s *sessionStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}

func (s *sessionStore) janitor(ctx context.Context, idle time.Duration) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.mu.Lock()
			for id, ss := range s.m {
				if time.Since(ss.lastSeen) > idle {
					delete(s.m, id)
				}
			}
			s.mu.Unlock()
		}
	}
}

// ---- Streamable HTTP front ---------------------------------------------

const maxBody = 16 << 20

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func rpcError(w http.ResponseWriter, status int, id json.RawMessage, code int, msg string, data any) {
	writeJSON(w, status, jsonrpc.NewErrorResponse(id, jsonrpc.NewError(code, msg, data)))
}

func (g *Gateway) originAllowed(origin string) bool {
	if origin == "" {
		return true
	}
	if slices.Contains(g.cfg.AllowedOrigins, "*") || slices.Contains(g.cfg.AllowedOrigins, origin) {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	switch u.Hostname() {
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	return false
}

func (g *Gateway) authorized(r *http.Request) bool {
	if len(g.cfg.Auth.BearerTokens) == 0 {
		return true
	}
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return false
	}
	for _, t := range g.cfg.Auth.BearerTokens {
		if subtle.ConstantTimeCompare([]byte(got), []byte(t)) == 1 {
			return true
		}
	}
	return false
}

// ServeHTTP implements the MCP endpoint for both protocol eras.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !g.originAllowed(r.Header.Get("Origin")) {
		rpcError(w, http.StatusForbidden, nil, jsonrpc.CodeInvalidRequest, "origin not allowed", nil)
		return
	}
	if !g.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="junction"`)
		rpcError(w, http.StatusUnauthorized, nil, jsonrpc.CodeInvalidRequest, "missing or invalid bearer token", nil)
		return
	}
	switch r.Method {
	case http.MethodPost:
		g.servePost(w, r)
	case http.MethodDelete:
		// Legacy session termination.
		sid := r.Header.Get(mcp.HeaderSessionID)
		if sid == "" {
			w.Header().Set("Allow", "POST, DELETE")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if !g.sessions.del(sid) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		// GET (legacy standalone SSE stream) is not offered: Junction never
		// sends server-initiated messages outside a request.
		w.Header().Set("Allow", "POST, DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (g *Gateway) servePost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil || len(body) > maxBody {
		rpcError(w, http.StatusRequestEntityTooLarge, nil, jsonrpc.CodeInvalidRequest, "request body too large or unreadable", nil)
		return
	}
	msg, perr := jsonrpc.Parse(body)
	if perr != nil {
		var id json.RawMessage
		if msg != nil {
			id = msg.ID
		}
		writeJSON(w, http.StatusBadRequest, jsonrpc.NewErrorResponse(id, perr))
		return
	}
	if msg.IsResponse() || msg.IsNotification() {
		// Junction sends clients no requests, so responses are ignored;
		// notifications (initialized, cancelled) need no action.
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if !msg.IsRequest() {
		rpcError(w, http.StatusBadRequest, msg.ID, jsonrpc.CodeInvalidRequest, "invalid JSON-RPC message", nil)
		return
	}

	hdrVersion := r.Header.Get(mcp.HeaderProtocolVersion)
	metaVersion := mcp.RequestVersion(msg.Params)

	// 1. Legacy handshake.
	if msg.Method == "initialize" && metaVersion == "" {
		cc := &ClientCtx{Transport: "http"}
		resp := g.Handle(r.Context(), cc, msg)
		if resp.Error == nil {
			sid := newSessionID()
			cc.SessionID = sid
			g.sessions.put(sid, *cc)
			w.Header().Set(mcp.HeaderSessionID, sid)
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}

	// 2. Modern, stateless request.
	if metaVersion != "" {
		if !mcp.IsModernVersion(metaVersion) {
			rpcError(w, http.StatusBadRequest, msg.ID, jsonrpc.CodeUnsupportedProtocolVersion, "Unsupported protocol version",
				map[string]any{"supported": mcp.AllVersions(), "requested": metaVersion})
			return
		}
		if bad := validateModernHeaders(r, msg, metaVersion); bad != "" {
			rpcError(w, http.StatusBadRequest, msg.ID, jsonrpc.CodeHeaderMismatch, "Header mismatch: "+bad, nil)
			return
		}
		p, _ := mcp.DecodeParams(msg.Params)
		meta := p.Meta()
		cc := &ClientCtx{Era: mcp.EraModern, Version: metaVersion, Transport: "http", Caps: meta[mcp.MetaClientCapabilities]}
		_ = json.Unmarshal(meta[mcp.MetaClientInfo], &cc.Info)
		resp := g.Handle(r.Context(), cc, msg)
		status := http.StatusOK
		if resp.Error != nil {
			switch resp.Error.Code {
			case jsonrpc.CodeMethodNotFound:
				status = http.StatusNotFound
			case jsonrpc.CodeHeaderMismatch, jsonrpc.CodeMissingRequiredClientCapability, jsonrpc.CodeUnsupportedProtocolVersion:
				status = http.StatusBadRequest
			}
		}
		writeJSON(w, status, resp)
		return
	}

	// 3. Legacy request after initialize.
	if mcp.IsModernVersion(hdrVersion) {
		rpcError(w, http.StatusBadRequest, msg.ID, jsonrpc.CodeHeaderMismatch,
			"Header mismatch: MCP-Protocol-Version is "+hdrVersion+" but params._meta lacks "+mcp.MetaProtocolVersion, nil)
		return
	}
	var cc ClientCtx
	if sid := r.Header.Get(mcp.HeaderSessionID); sid != "" {
		c, ok := g.sessions.get(sid)
		if !ok {
			// Per the legacy spec the client must start a new session.
			rpcError(w, http.StatusNotFound, msg.ID, -32001, "session not found; send initialize again", nil)
			return
		}
		cc = c
	} else {
		// Lenient: accept session-less legacy requests.
		v := hdrVersion
		if v == "" {
			v = "2025-03-26"
		}
		cc = ClientCtx{Era: mcp.EraLegacy, Version: v, Transport: "http"}
	}
	writeJSON(w, http.StatusOK, g.Handle(r.Context(), &cc, msg))
}

// validateModernHeaders checks the 2026-07-28 header/body mirroring rules.
// It returns a description of the first problem, or "".
func validateModernHeaders(r *http.Request, msg *jsonrpc.Message, version string) string {
	if h := r.Header.Get(mcp.HeaderProtocolVersion); h != version {
		if h == "" {
			return "MCP-Protocol-Version header is missing"
		}
		return "MCP-Protocol-Version header value '" + h + "' does not match body value '" + version + "'"
	}
	if h := r.Header.Get(mcp.HeaderMethod); h != msg.Method {
		if h == "" {
			return "Mcp-Method header is missing"
		}
		return "Mcp-Method header value '" + h + "' does not match body value '" + msg.Method + "'"
	}
	var field string
	switch msg.Method {
	case "tools/call", "prompts/get":
		field = "name"
	case "resources/read":
		field = "uri"
	default:
		return ""
	}
	p, _ := mcp.DecodeParams(msg.Params)
	want := p.String(field)
	raw := r.Header.Get(mcp.HeaderName)
	if raw == "" {
		return "Mcp-Name header is missing"
	}
	got, ok := upstream.DecodeHeaderValue(raw)
	if !ok {
		return "Mcp-Name header has invalid base64 encoding"
	}
	if got != want {
		return "Mcp-Name header value '" + got + "' does not match body value '" + want + "'"
	}
	return ""
}

// ---- stdio front ---------------------------------------------------------

// ServeStdio speaks MCP over stdin/stdout so desktop clients that only
// launch local commands can use Junction. A legacy client's session is the
// lifetime of the process.
func (g *Gateway) ServeStdio(ctx context.Context, in io.Reader, out io.Writer) error {
	var (
		writeMu sync.Mutex
		legacy  *ClientCtx
		lmu     sync.Mutex
		wg      sync.WaitGroup
	)
	write := func(m *jsonrpc.Message) {
		b, _ := json.Marshal(m)
		writeMu.Lock()
		out.Write(append(b, '\n'))
		writeMu.Unlock()
	}
	rd := bufio.NewReaderSize(in, 64*1024)
	for {
		line, err := rd.ReadBytes('\n')
		if len(strings.TrimSpace(string(line))) > 0 {
			msg, perr := jsonrpc.Parse(line)
			switch {
			case perr != nil:
				var id json.RawMessage
				if msg != nil {
					id = msg.ID
				}
				write(jsonrpc.NewErrorResponse(id, perr))
			case msg.IsRequest():
				wg.Add(1)
				go func(msg *jsonrpc.Message) {
					defer wg.Done()
					var cc *ClientCtx
					metaVersion := mcp.RequestVersion(msg.Params)
					switch {
					case metaVersion != "":
						if !mcp.IsModernVersion(metaVersion) {
							write(jsonrpc.NewErrorResponse(msg.ID, jsonrpc.NewError(jsonrpc.CodeUnsupportedProtocolVersion, "Unsupported protocol version",
								map[string]any{"supported": mcp.AllVersions(), "requested": metaVersion})))
							return
						}
						p, _ := mcp.DecodeParams(msg.Params)
						meta := p.Meta()
						cc = &ClientCtx{Era: mcp.EraModern, Version: metaVersion, Transport: "stdio", Caps: meta[mcp.MetaClientCapabilities]}
						_ = json.Unmarshal(meta[mcp.MetaClientInfo], &cc.Info)
					case msg.Method == "initialize":
						cc = &ClientCtx{Transport: "stdio"}
						resp := g.Handle(ctx, cc, msg)
						if resp.Error == nil {
							lmu.Lock()
							legacy = cc
							lmu.Unlock()
						}
						write(resp)
						return
					default:
						lmu.Lock()
						cc = legacy
						lmu.Unlock()
						if cc == nil {
							cc = &ClientCtx{Era: mcp.EraLegacy, Version: mcp.VersionLatestLegacy, Transport: "stdio"}
						}
					}
					write(g.Handle(ctx, cc, msg))
				}(msg)
			}
		}
		if err != nil {
			wg.Wait()
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// Sessions returns the number of open legacy sessions.
func (g *Gateway) Sessions() int { return g.sessions.count() }
