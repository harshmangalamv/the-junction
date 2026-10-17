package upstream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"

	"github.com/harshmangalamv/the-junction/internal/jsonrpc"
	"github.com/harshmangalamv/the-junction/internal/mcp"
)

// httpTransport speaks Streamable HTTP (both eras) to an upstream server.
type httpTransport struct {
	url      string
	static   map[string]string
	client   *http.Client
	onNotify func(*jsonrpc.Message)
	logs     *LogRing

	mu          sync.Mutex
	lastSession string
}

func newHTTP(url string, static map[string]string, logs *LogRing, onNotify func(*jsonrpc.Message)) *httpTransport {
	return &httpTransport{url: url, static: static, client: &http.Client{}, logs: logs, onNotify: onNotify}
}

func (t *httpTransport) newPost(ctx context.Context, m *jsonrpc.Message, h headers) (*http.Request, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, v := range t.static {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if h.protocolVersion != "" {
		req.Header.Set(mcp.HeaderProtocolVersion, h.protocolVersion)
	}
	if h.sessionID != "" {
		req.Header.Set(mcp.HeaderSessionID, h.sessionID)
	}
	if h.method != "" {
		req.Header.Set(mcp.HeaderMethod, h.method)
	}
	if h.name != "" {
		req.Header.Set(mcp.HeaderName, EncodeHeaderValue(h.name))
	}
	return req, nil
}

func (t *httpTransport) roundTrip(ctx context.Context, m *jsonrpc.Message, h headers) (*jsonrpc.Message, error) {
	req, err := t.newPost(ctx, m, h)
	if err != nil {
		return nil, err
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if sid := resp.Header.Get(mcp.HeaderSessionID); sid != "" {
		t.mu.Lock()
		t.lastSession = sid
		t.mu.Unlock()
	}

	ct, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if ct == "text/event-stream" && resp.StatusCode < 300 {
		return t.readSSE(ctx, resp.Body, m.IDString(), h)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(body)) > 0 {
		if out, perr := jsonrpc.Parse(body); perr == nil && out.IsResponse() {
			return out, nil
		}
	}
	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		return nil, &HTTPStatusError{Status: resp.StatusCode, Body: string(body)}
	}
	return nil, fmt.Errorf("upstream returned HTTP %d without a JSON-RPC response", resp.StatusCode)
}

// readSSE reads events until the response for wantID arrives.
func (t *httpTransport) readSSE(ctx context.Context, r io.Reader, wantID string, h headers) (*jsonrpc.Message, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 32<<20)
	var data strings.Builder
	flush := func() *jsonrpc.Message {
		defer data.Reset()
		if data.Len() == 0 {
			return nil
		}
		m, perr := jsonrpc.Parse([]byte(data.String()))
		if perr != nil {
			t.logs.Add("junction", "bad SSE event: "+truncate(data.String(), 200))
			return nil
		}
		switch {
		case m.IsResponse() && m.IDString() == wantID:
			return m
		case m.IsRequest():
			// Legacy server-initiated request on the response stream:
			// answer it with a separate POST.
			t.logs.Add("junction", "server asked for "+m.Method)
			go func() {
				reply := serverRequestReply(m)
				if req, err := t.newPost(context.Background(), reply, headers{protocolVersion: h.protocolVersion, sessionID: h.sessionID}); err == nil {
					if resp, err := t.client.Do(req); err == nil {
						resp.Body.Close()
					}
				}
			}()
		case m.IsNotification():
			if t.onNotify != nil {
				t.onNotify(m)
			}
		}
		return nil
	}
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if m := flush(); m != nil {
				return m, nil
			}
		case strings.HasPrefix(line, ":"):
			// comment / keep-alive
		case strings.HasPrefix(line, "data:"):
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if m := flush(); m != nil {
		return m, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("SSE stream ended before the response arrived")
}

func (t *httpTransport) notify(ctx context.Context, m *jsonrpc.Message, h headers) error {
	req, err := t.newPost(ctx, m, h)
	if err != nil {
		return err
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return &HTTPStatusError{Status: resp.StatusCode}
	}
	return nil
}

func (t *httpTransport) session() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastSession
}

func (t *httpTransport) resetSession() {
	t.mu.Lock()
	t.lastSession = ""
	t.mu.Unlock()
}

// terminate sends DELETE to end a legacy session.
func (t *httpTransport) terminate(sessionID string) {
	if sessionID == "" {
		return
	}
	req, err := http.NewRequest(http.MethodDelete, t.url, nil)
	if err != nil {
		return
	}
	for k, v := range t.static {
		req.Header.Set(k, v)
	}
	req.Header.Set(mcp.HeaderSessionID, sessionID)
	if resp, err := t.client.Do(req); err == nil {
		resp.Body.Close()
	}
}

func (t *httpTransport) close() error { return nil }

// EncodeHeaderValue applies the spec's Base64 sentinel encoding to values
// that are not plain printable ASCII.
func EncodeHeaderValue(v string) string {
	safe := v == strings.TrimSpace(v)
	for i := 0; i < len(v) && safe; i++ {
		if v[i] < 0x20 || v[i] > 0x7e {
			safe = false
		}
	}
	if safe && !(strings.HasPrefix(v, "=?base64?") && strings.HasSuffix(v, "?=")) {
		return v
	}
	return "=?base64?" + base64.StdEncoding.EncodeToString([]byte(v)) + "?="
}

// DecodeHeaderValue reverses EncodeHeaderValue.
func DecodeHeaderValue(v string) (string, bool) {
	if strings.HasPrefix(v, "=?base64?") && strings.HasSuffix(v, "?=") {
		b, err := base64.StdEncoding.DecodeString(v[len("=?base64?") : len(v)-2])
		if err != nil {
			return "", false
		}
		return string(b), true
	}
	return v, true
}
