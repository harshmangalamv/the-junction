package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/harshmangalamv/the-junction/internal/config"
	"github.com/harshmangalamv/the-junction/internal/demo"
	"github.com/harshmangalamv/the-junction/internal/gateway"
	"github.com/harshmangalamv/the-junction/internal/jsonrpc"
	"github.com/harshmangalamv/the-junction/internal/mcp"
	"github.com/harshmangalamv/the-junction/internal/recorder"
	"github.com/harshmangalamv/the-junction/internal/replay"
)

// The test binary doubles as the demo upstream servers.
func TestMain(m *testing.M) {
	if p := os.Getenv("JUNCTION_TEST_PERSONA"); p != "" {
		_ = demo.Serve(os.Stdin, os.Stdout, demo.Options{Persona: p, Era: os.Getenv("JUNCTION_TEST_ERA")})
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func newGateway(t *testing.T, extra string) (*gateway.Gateway, *httptest.Server) {
	t.Helper()
	exe := os.Args[0]
	yaml := `
servers:
  - name: crm
    command: ["` + jsonEsc(exe) + `"]
    env: {JUNCTION_TEST_PERSONA: crm}
  - name: billing
    command: ["` + jsonEsc(exe) + `"]
    env: {JUNCTION_TEST_PERSONA: billing}
policy:
  deny: ["crm__delete_*"]
` + extra
	cfg, err := config.Parse([]byte(yaml), "")
	if err != nil {
		t.Fatal(err)
	}
	g, err := gateway.New(cfg, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	g.Start(context.Background())
	srv := httptest.NewServer(g)
	t.Cleanup(func() { srv.Close(); g.Close() })
	return g, srv
}

func jsonEsc(s string) string { b, _ := json.Marshal(s); return string(b[1 : len(b)-1]) }

func client(srv *httptest.Server, era mcp.Era, caps map[string]any) *demo.Client {
	return &demo.Client{URL: srv.URL, Era: era, Info: mcp.Implementation{Name: "test-" + era.String(), Version: "1"}, Caps: caps}
}

func call(t *testing.T, c *demo.Client, method string, params map[string]any) map[string]any {
	t.Helper()
	res, err := c.Call(context.Background(), method, params)
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	var m map[string]any
	_ = json.Unmarshal(res, &m)
	return m
}

func toolNames(r map[string]any) []string {
	var out []string
	for _, t := range r["tools"].([]any) {
		out = append(out, t.(map[string]any)["name"].(string))
	}
	return out
}

func text(r map[string]any) string {
	c := r["content"].([]any)
	return c[0].(map[string]any)["text"].(string)
}

func TestUpstreamEraDetection(t *testing.T) {
	g, _ := newGateway(t, "")
	eras := map[string]string{}
	for _, s := range g.Servers() {
		eras[s.Name] = s.Era.String() + " " + s.Version
	}
	if eras["crm"] != "legacy 2025-11-25" || eras["billing"] != "modern 2026-07-28" {
		t.Fatalf("eras: %v", eras)
	}
}

func TestLegacyClient(t *testing.T) {
	_, srv := newGateway(t, "")
	c := client(srv, mcp.EraLegacy, nil)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	tools := call(t, c, "tools/list", nil)
	names := strings.Join(toolNames(tools), ",")
	if !strings.Contains(names, "crm__lookup_customer") || !strings.Contains(names, "billing__get_invoice") {
		t.Fatalf("missing tools: %s", names)
	}
	if strings.Contains(names, "crm__delete_customer") {
		t.Fatalf("denied tool listed: %s", names)
	}
	if _, ok := tools["resultType"]; ok {
		t.Fatal("legacy client got modern resultType field")
	}

	// Legacy client → legacy server, with redaction.
	r := call(t, c, "tools/call", map[string]any{"name": "crm__lookup_customer", "arguments": map[string]any{"customer_id": "C-1001"}})
	txt := text(r)
	for _, leak := range []string{"priya.sharma@example.com", "4111 1111 1111 1111", "2345 6789 0124", "ABCPS1234K"} {
		if strings.Contains(txt, leak) {
			t.Errorf("PII leaked: %s in %q", leak, txt)
		}
	}
	if !strings.Contains(txt, "[REDACTED:EMAIL]") {
		t.Errorf("expected redaction marker: %q", txt)
	}

	// Legacy client → modern server (bridged).
	r = call(t, c, "tools/call", map[string]any{"name": "billing__get_invoice", "arguments": map[string]any{"invoice_id": "INV-1"}})
	if !strings.Contains(text(r), "INV-1") {
		t.Errorf("bridged call: %v", r)
	}

	// Modern server wanting elicitation from a legacy client: refused safely.
	r = call(t, c, "tools/call", map[string]any{"name": "billing__refund_invoice", "arguments": map[string]any{"invoice_id": "INV-1"}})
	if r["isError"] != true {
		t.Errorf("refund via legacy client should be an error result: %v", r)
	}

	// Policy block.
	r = call(t, c, "tools/call", map[string]any{"name": "crm__delete_customer", "arguments": map[string]any{"customer_id": "C-1"}})
	if r["isError"] != true || !strings.Contains(text(r), "not permitted") {
		t.Errorf("delete should be blocked: %v", r)
	}

	// Prompts and resources pass through.
	r = call(t, c, "prompts/get", map[string]any{"name": "crm__summarize_customer", "arguments": map[string]any{"customer_id": "C-1"}})
	if r["messages"] == nil {
		t.Errorf("prompts/get: %v", r)
	}
	r = call(t, c, "resources/read", map[string]any{"uri": "crm://schema/customer"})
	if r["contents"] == nil {
		t.Errorf("resources/read: %v", r)
	}
}

func TestModernClient(t *testing.T) {
	_, srv := newGateway(t, "")
	c := client(srv, mcp.EraModern, map[string]any{"elicitation": map[string]any{"form": map[string]any{}}})
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	d := call(t, c, "server/discover", nil)
	if d["resultType"] != "complete" || d["supportedVersions"] == nil || d["ttlMs"] == nil {
		t.Fatalf("discover: %v", d)
	}

	// Modern client → legacy server (bridged), result shaped for modern.
	r := call(t, c, "tools/call", map[string]any{"name": "crm__search_customers", "arguments": map[string]any{"query": "pune"}})
	if r["resultType"] != "complete" || !strings.Contains(text(r), "Arjun") {
		t.Fatalf("bridged search: %v", r)
	}
	meta, _ := r["_meta"].(map[string]any)
	if meta[mcp.MetaServerInfo] == nil {
		t.Errorf("missing serverInfo in _meta: %v", r)
	}

	// Multi round-trip request passes through to a capable client.
	r = call(t, c, "tools/call", map[string]any{"name": "billing__refund_invoice", "arguments": map[string]any{"invoice_id": "INV-9"}})
	if r["resultType"] != "input_required" {
		t.Fatalf("expected input_required: %v", r)
	}
	if _, ok := r["inputRequests"].(map[string]any)["confirm_refund"]; !ok {
		t.Fatalf("missing inputRequests: %v", r)
	}
	r = call(t, c, "tools/call", map[string]any{
		"name": "billing__refund_invoice", "arguments": map[string]any{"invoice_id": "INV-9"},
		"inputResponses": map[string]any{"confirm_refund": map[string]any{"action": "accept", "content": map[string]any{"confirm": true}}},
		"requestState":   r["requestState"],
	})
	if r["resultType"] != "complete" || !strings.Contains(text(r), "Refund issued") {
		t.Fatalf("retry: %v", r)
	}
}

func post(t *testing.T, url string, hdr map[string]string, body string) (int, *jsonrpc.Message) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	m, _ := jsonrpc.Parse(b)
	return resp.StatusCode, m
}

func TestModernValidation(t *testing.T) {
	_, srv := newGateway(t, "")
	meta := func(v string) string {
		return `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"crm__search_customers","arguments":{"query":"x"},"_meta":{"io.modelcontextprotocol/protocolVersion":"` + v + `","io.modelcontextprotocol/clientCapabilities":{}}}}`
	}
	good := map[string]string{"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "tools/call", "Mcp-Name": "crm__search_customers"}

	if st, m := post(t, srv.URL, good, meta("2026-07-28")); st != 200 || m.Error != nil {
		t.Fatalf("valid request: %d %+v", st, m.Error)
	}
	bad := map[string]string{"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "tools/call", "Mcp-Name": "other"}
	if st, m := post(t, srv.URL, bad, meta("2026-07-28")); st != 400 || m.Error == nil || m.Error.Code != jsonrpc.CodeHeaderMismatch {
		t.Fatalf("Mcp-Name mismatch: %d %+v", st, m)
	}
	noName := map[string]string{"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "tools/call"}
	if st, m := post(t, srv.URL, noName, meta("2026-07-28")); st != 400 || m.Error.Code != jsonrpc.CodeHeaderMismatch {
		t.Fatalf("missing Mcp-Name: %d %+v", st, m)
	}
	future := map[string]string{"MCP-Protocol-Version": "2099-01-01", "Mcp-Method": "tools/call", "Mcp-Name": "crm__search_customers"}
	st, m := post(t, srv.URL, future, meta("2099-01-01"))
	if st != 400 || m.Error.Code != jsonrpc.CodeUnsupportedProtocolVersion || !strings.Contains(string(m.Error.Data), "2026-07-28") {
		t.Fatalf("unsupported version: %d %+v", st, m)
	}
	unknown := `{"jsonrpc":"2.0","id":2,"method":"nope/nope","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}`
	if st, m := post(t, srv.URL, map[string]string{"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "nope/nope"}, unknown); st != 404 || m.Error.Code != jsonrpc.CodeMethodNotFound {
		t.Fatalf("unknown method: %d %+v", st, m)
	}
}

func TestLegacyTransportRules(t *testing.T) {
	_, srv := newGateway(t, "")
	st, _ := post(t, srv.URL, map[string]string{"Mcp-Session-Id": "doesnotexist", "MCP-Protocol-Version": "2025-11-25"},
		`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	if st != 404 {
		t.Fatalf("unknown session should be 404, got %d", st)
	}
	resp, _ := http.Get(srv.URL)
	if resp.StatusCode != 405 {
		t.Fatalf("GET should be 405, got %d", resp.StatusCode)
	}
	st, _ = post(t, srv.URL, nil, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	if st != 202 {
		t.Fatalf("notification should be 202, got %d", st)
	}
	st, m := post(t, srv.URL, nil, `{not json`)
	if st != 400 || m == nil || m.Error.Code != jsonrpc.CodeParseError {
		t.Fatalf("parse error: %d %+v", st, m)
	}
}

func TestOriginAndAuth(t *testing.T) {
	_, srv := newGateway(t, "auth:\n  bearer_tokens: [s3cret]\n")
	body := `{"jsonrpc":"2.0","id":1,"method":"ping"}`
	if st, _ := post(t, srv.URL, nil, body); st != 401 {
		t.Fatalf("missing token should be 401, got %d", st)
	}
	if st, _ := post(t, srv.URL, map[string]string{"Authorization": "Bearer s3cret"}, body); st != 200 {
		t.Fatalf("valid token should pass, got %d", st)
	}
	if st, _ := post(t, srv.URL, map[string]string{"Authorization": "Bearer s3cret", "Origin": "https://evil.example"}, body); st != 403 {
		t.Fatalf("foreign origin should be 403, got %d", st)
	}
}

func TestStdioFront(t *testing.T) {
	g, _ := newGateway(t, "")
	in := strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"stdio-test","version":"1"}}}` + "\n" +
			`{"jsonrpc":"2.0","method":"notifications/initialized"}` + "\n")
	var out bytes.Buffer
	if err := g.ServeStdio(context.Background(), in, &out); err != nil {
		t.Fatal(err)
	}
	var init struct {
		Result struct {
			ProtocolVersion string `json:"protocolVersion"`
		} `json:"result"`
	}
	if err := json.Unmarshal(bytes.Split(out.Bytes(), []byte("\n"))[0], &init); err != nil || init.Result.ProtocolVersion != "2025-06-18" {
		t.Fatalf("stdio initialize: %s", out.String())
	}

	// Modern request on stdio.
	in = strings.NewReader(`{"jsonrpc":"2.0","id":7,"method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}` + "\n")
	out.Reset()
	_ = g.ServeStdio(context.Background(), in, &out)
	if !strings.Contains(out.String(), `"resultType":"complete"`) || !strings.Contains(out.String(), "crm__lookup_customer") {
		t.Fatalf("stdio modern tools/list: %s", out.String())
	}
}

func TestRecordingAndReplay(t *testing.T) {
	g, srv := newGateway(t, "")
	c := client(srv, mcp.EraModern, nil)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	call(t, c, "tools/call", map[string]any{"name": "crm__search_customers", "arguments": map[string]any{"query": "a"}})
	call(t, c, "tools/call", map[string]any{"name": "crm__lookup_customer", "arguments": map[string]any{"customer_id": "C-1002"}})

	entries := g.Rec.Recent(0)
	var calls []recorder.Entry
	for i := len(entries) - 1; i >= 0; i-- {
		calls = append(calls, entries[i])
	}
	rep := replay.Run(context.Background(), g, calls, replay.Options{})
	if rep.Same != 2 || rep.Changed != 0 {
		t.Fatalf("replay of unchanged servers: %+v", rep)
	}

	// Simulate a server change by editing the recorded result.
	for i := range calls {
		if calls[i].Tool == "crm__search_customers" {
			calls[i].Result = json.RawMessage(`{"content":[{"type":"text","text":"something else"}]}`)
		}
	}
	rep = replay.Run(context.Background(), g, calls, replay.Options{})
	if rep.Changed != 1 || !strings.Contains(rep.Results[0].Diff+rep.Results[1].Diff, "something else") {
		t.Fatalf("expected one change: %+v", rep)
	}

	st := g.Rec.Stats()
	if st.Bridged == 0 || st.Redactions["email"] == 0 {
		t.Fatalf("stats should count bridged calls and redactions: %+v", st)
	}
}

func TestDeadUpstreamIsReported(t *testing.T) {
	cfg, _ := config.Parse([]byte(`
servers:
  - name: ghost
    command: ["definitely-not-a-real-binary-xyz"]
    timeout: 2s
`), "")
	g, _ := gateway.New(cfg, log.New(io.Discard, "", 0))
	defer g.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	g.Start(ctx)
	s := g.Servers()[0]
	if s.Connected || s.LastError == "" {
		t.Fatalf("ghost server should be down with an error: %+v", s)
	}
	srv := httptest.NewServer(g)
	defer srv.Close()
	c := client(srv, mcp.EraLegacy, nil)
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Call(ctx, "tools/list", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(res), `"tools":[]`) {
		t.Fatalf("tools/list with dead upstream: %s", res)
	}
	_, err = c.Call(ctx, "tools/call", map[string]any{"name": "ghost__x"})
	var je *jsonrpc.Error
	if !errors.As(err, &je) {
		t.Fatalf("calling unknown tool should be a JSON-RPC error, got %v", err)
	}
}
