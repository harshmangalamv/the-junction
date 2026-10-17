// Package demo provides small built-in MCP servers so Junction can be tried
// with zero setup. All data is fictional.
//
//   - "crm" speaks the legacy protocol (initialize handshake, 2025-11-25).
//   - "billing" speaks the modern stateless protocol (2026-07-28) and uses
//     a multi round-trip request (elicitation) to confirm refunds.
package demo

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/harshmangalamv/the-junction/internal/jsonrpc"
	"github.com/harshmangalamv/the-junction/internal/mcp"
)

// Options selects which demo server to run.
type Options struct {
	Persona string // crm | billing
	Era     string // legacy | modern | dual (default: legacy for crm, modern for billing)
}

type server struct {
	opts        Options
	initialized bool
	version     string
	mu          sync.Mutex
	out         io.Writer
}

// Serve runs a demo server over stdio until in is closed.
func Serve(in io.Reader, out io.Writer, opts Options) error {
	if opts.Persona == "" {
		opts.Persona = "crm"
	}
	if opts.Era == "" {
		opts.Era = map[string]string{"crm": "legacy", "billing": "modern"}[opts.Persona]
	}
	s := &server{opts: opts, out: out}
	fmt.Fprintf(os.Stderr, "demo %s server ready (%s protocol)\n", opts.Persona, opts.Era)
	rd := bufio.NewReader(in)
	for {
		line, err := rd.ReadBytes('\n')
		if len(strings.TrimSpace(string(line))) > 0 {
			s.handle(line)
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (s *server) send(m *jsonrpc.Message) {
	b, _ := json.Marshal(m)
	s.mu.Lock()
	s.out.Write(append(b, '\n'))
	s.mu.Unlock()
}

func (s *server) handle(line []byte) {
	m, perr := jsonrpc.Parse(line)
	if perr != nil {
		s.send(jsonrpc.NewErrorResponse(nil, perr))
		return
	}
	if !m.IsRequest() {
		if m.Method == "notifications/initialized" {
			s.initialized = true
		}
		return
	}
	p, _ := mcp.DecodeParams(m.Params)
	modernReq := mcp.RequestVersion(m.Params) != ""
	legacyOK := s.opts.Era == "legacy" || s.opts.Era == "dual"
	modernOK := s.opts.Era == "modern" || s.opts.Era == "dual"

	switch {
	case m.Method == "initialize":
		if !legacyOK {
			s.send(jsonrpc.NewErrorResponse(m.ID, jsonrpc.NewError(jsonrpc.CodeMethodNotFound,
				"this server only supports protocol "+mcp.VersionModern+"; initialize is not used", nil)))
			return
		}
		v := p.String("protocolVersion")
		if !mcp.IsLegacyVersion(v) {
			v = mcp.VersionLatestLegacy
		}
		s.version = v
		s.send(jsonrpc.NewResult(m.ID, map[string]any{
			"protocolVersion": v,
			"capabilities":    s.capabilities(),
			"serverInfo":      s.info(),
			"instructions":    s.instructions(),
		}))
		return
	case modernReq && !modernOK:
		// A legacy server does not know server/discover or _meta.
		s.send(jsonrpc.NewErrorResponse(m.ID, jsonrpc.NewError(jsonrpc.CodeMethodNotFound, "method not found: "+m.Method, nil)))
		return
	case !modernReq && !legacyOK:
		s.send(jsonrpc.NewErrorResponse(m.ID, jsonrpc.NewError(jsonrpc.CodeInvalidParams,
			"missing _meta."+mcp.MetaProtocolVersion+"; this server speaks "+mcp.VersionModern, nil)))
		return
	case modernReq && mcp.RequestVersion(m.Params) != mcp.VersionModern:
		s.send(jsonrpc.NewErrorResponse(m.ID, jsonrpc.NewError(jsonrpc.CodeUnsupportedProtocolVersion, "Unsupported protocol version",
			map[string]any{"supported": []string{mcp.VersionModern}, "requested": mcp.RequestVersion(m.Params)})))
		return
	}

	var caps map[string]json.RawMessage
	_ = json.Unmarshal(p.Meta()[mcp.MetaClientCapabilities], &caps)
	result, rerr := s.dispatch(m.Method, p, caps)
	if rerr != nil {
		s.send(jsonrpc.NewErrorResponse(m.ID, rerr))
		return
	}
	if modernReq {
		result = modernize(m.Method, result, s.info())
	}
	s.send(jsonrpc.NewResult(m.ID, result))
}

func modernize(method string, r map[string]any, info mcp.Implementation) map[string]any {
	if _, ok := r["resultType"]; !ok {
		r["resultType"] = mcp.ResultTypeComplete
	}
	r["_meta"] = map[string]any{mcp.MetaServerInfo: info}
	if mcp.CacheableMethods[method] {
		r["ttlMs"], r["cacheScope"] = 60000, "public"
	}
	return r
}

func (s *server) info() mcp.Implementation {
	return mcp.Implementation{Name: "demo-" + s.opts.Persona, Version: "1.0.0"}
}

func (s *server) capabilities() map[string]any {
	c := map[string]any{"tools": map[string]any{}}
	if s.opts.Persona == "crm" {
		c["prompts"] = map[string]any{}
		c["resources"] = map[string]any{}
	}
	return c
}

func (s *server) instructions() string {
	if s.opts.Persona == "crm" {
		return "Demo CRM with fictional customers. Use lookup_customer for details."
	}
	return "Demo billing system. Refunds require explicit user confirmation."
}

func text(t string) []map[string]any { return []map[string]any{{"type": "text", "text": t}} }

func (s *server) dispatch(method string, p mcp.Params, caps map[string]json.RawMessage) (map[string]any, *jsonrpc.Error) {
	switch method {
	case "server/discover":
		return map[string]any{
			"supportedVersions": []string{mcp.VersionModern},
			"capabilities":      s.capabilities(),
			"instructions":      s.instructions(),
		}, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		return map[string]any{"tools": s.tools()}, nil
	case "tools/call":
		var args map[string]any
		_ = json.Unmarshal(p["arguments"], &args)
		return s.call(p.String("name"), args, p, caps)
	case "prompts/list":
		if s.opts.Persona != "crm" {
			break
		}
		return map[string]any{"prompts": []any{map[string]any{
			"name": "summarize_customer", "description": "Summarize a customer's account and recent orders",
			"arguments": []any{map[string]any{"name": "customer_id", "description": "Customer ID, e.g. C-1001", "required": true}},
		}}}, nil
	case "prompts/get":
		if s.opts.Persona != "crm" {
			break
		}
		var args map[string]string
		_ = json.Unmarshal(p["arguments"], &args)
		return map[string]any{"messages": []any{map[string]any{
			"role":    "user",
			"content": map[string]any{"type": "text", "text": "Summarize customer " + args["customer_id"] + " using the crm tools. Be concise."},
		}}}, nil
	case "resources/list":
		if s.opts.Persona != "crm" {
			break
		}
		return map[string]any{"resources": []any{map[string]any{
			"uri": "crm://schema/customer", "name": "customer-schema", "mimeType": "application/json",
			"description": "JSON schema of a customer record",
		}}}, nil
	case "resources/templates/list":
		return map[string]any{"resourceTemplates": []any{}}, nil
	case "resources/read":
		if s.opts.Persona == "crm" && p.String("uri") == "crm://schema/customer" {
			return map[string]any{"contents": []any{map[string]any{
				"uri": "crm://schema/customer", "mimeType": "application/json",
				"text": `{"type":"object","properties":{"id":{"type":"string"},"name":{"type":"string"},"email":{"type":"string"},"phone":{"type":"string"}}}`,
			}}}, nil
		}
		return nil, jsonrpc.NewError(jsonrpc.CodeInvalidParams, "resource not found", map[string]string{"uri": p.String("uri")})
	}
	return nil, jsonrpc.NewError(jsonrpc.CodeMethodNotFound, "method not found: "+method, nil)
}

func obj(props map[string]any, required ...string) map[string]any {
	o := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		o["required"] = required
	}
	return o
}

func str(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }

func (s *server) tools() []any {
	if s.opts.Persona == "billing" {
		return []any{
			map[string]any{
				"name": "get_invoice", "title": "Get invoice",
				"description": "Fetch an invoice by ID, including billing contact and payment method.",
				"inputSchema": obj(map[string]any{"invoice_id": str("Invoice ID, e.g. INV-2024-001")}, "invoice_id"),
				"annotations": map[string]any{"readOnlyHint": true},
			},
			map[string]any{
				"name": "refund_invoice", "title": "Refund invoice",
				"description": "Refund an invoice. Asks the user to confirm before money moves.",
				"inputSchema": obj(map[string]any{
					"invoice_id": str("Invoice ID"),
					"reason":     str("Why the refund is issued"),
				}, "invoice_id"),
				"annotations": map[string]any{"destructiveHint": true},
			},
		}
	}
	return []any{
		map[string]any{
			"name": "lookup_customer", "title": "Look up customer",
			"description": "Return the full customer record (contact details, payment card, tax ID) for a customer ID.",
			"inputSchema": obj(map[string]any{"customer_id": str("Customer ID, e.g. C-1001")}, "customer_id"),
			"annotations": map[string]any{"readOnlyHint": true},
		},
		map[string]any{
			"name": "search_customers", "title": "Search customers",
			"description": "Search customers by name or city. Returns IDs and names.",
			"inputSchema": obj(map[string]any{"query": str("Name or city"), "limit": map[string]any{"type": "integer", "description": "Max results", "default": 5}}, "query"),
			"annotations": map[string]any{"readOnlyHint": true},
		},
		map[string]any{
			"name": "export_report", "title": "Export report",
			"description": "Generate a slow CSV report of all accounts (takes about a second).",
			"inputSchema": obj(map[string]any{"format": map[string]any{"type": "string", "enum": []string{"csv", "json"}}}),
			"annotations": map[string]any{"readOnlyHint": true},
		},
		map[string]any{
			"name": "delete_customer", "title": "Delete customer",
			"description": "Permanently delete a customer and all their data.",
			"inputSchema": obj(map[string]any{"customer_id": str("Customer ID")}, "customer_id"),
			"annotations": map[string]any{"destructiveHint": true},
		},
	}
}

type customer struct {
	ID, Name, City, Email, Phone, Card, PAN, Aadhaar string
}

var customers = []customer{
	{"C-1001", "Priya Sharma", "Hyderabad", "priya.sharma@example.com", "+91 98765 43210", "4111 1111 1111 1111", "ABCPS1234K", "2345 6789 0124"},
	{"C-1002", "Arjun Mehta", "Pune", "arjun.mehta@example.org", "+91 91234 56780", "5555 5555 5555 4444", "BCDPM5678L", "9876 5432 1012"},
	{"C-1003", "Sara Khan", "Bengaluru", "sara.k@example.net", "+91 99887 76655", "4012 8888 8888 1881", "CDEPK9012M", "2345 6789 0124"},
}

func (s *server) call(name string, args map[string]any, p mcp.Params, caps map[string]json.RawMessage) (map[string]any, *jsonrpc.Error) {
	get := func(k string) string { v, _ := args[k].(string); return v }
	switch name {
	case "lookup_customer":
		for _, c := range customers {
			if strings.EqualFold(c.ID, get("customer_id")) {
				body := fmt.Sprintf("Customer %s (fictional demo data)\nName: %s\nCity: %s\nEmail: %s\nPhone: %s\nCard on file: %s\nPAN: %s\nAadhaar: %s",
					c.ID, c.Name, c.City, c.Email, c.Phone, c.Card, c.PAN, c.Aadhaar)
				return map[string]any{
					"content": text(body),
					"structuredContent": map[string]any{
						"id": c.ID, "name": c.Name, "city": c.City, "email": c.Email, "phone": c.Phone,
					},
				}, nil
			}
		}
		return map[string]any{"content": text("No customer with ID " + get("customer_id")), "isError": true}, nil
	case "search_customers":
		q := strings.ToLower(get("query"))
		var lines []string
		for _, c := range customers {
			if q == "" || strings.Contains(strings.ToLower(c.Name+" "+c.City), q) {
				lines = append(lines, c.ID+"  "+c.Name+" ("+c.City+")")
			}
		}
		if len(lines) == 0 {
			return map[string]any{"content": text("No matches.")}, nil
		}
		return map[string]any{"content": text(strings.Join(lines, "\n"))}, nil
	case "export_report":
		time.Sleep(1200 * time.Millisecond)
		var b strings.Builder
		b.WriteString("id,name,city,email\n")
		for _, c := range customers {
			fmt.Fprintf(&b, "%s,%s,%s,%s\n", c.ID, c.Name, c.City, c.Email)
		}
		return map[string]any{"content": text(b.String())}, nil
	case "delete_customer":
		return map[string]any{"content": text("Deleted " + get("customer_id") + " (not really — demo).")}, nil
	case "get_invoice":
		id := get("invoice_id")
		if id == "" {
			id = "INV-2024-001"
		}
		return map[string]any{"content": text(fmt.Sprintf(
			"Invoice %s (fictional)\nAmount: ₹48,500.00\nStatus: paid\nBilling contact: accounts@example.com, +91 90000 12345\nPaid with card 4111 1111 1111 1111", id))}, nil
	case "refund_invoice":
		return s.refund(get("invoice_id"), p, caps)
	}
	return nil, jsonrpc.NewError(jsonrpc.CodeInvalidParams, "unknown tool: "+name, nil)
}

// refund demonstrates a multi round-trip request: the first call returns
// input_required with an elicitation; the retry carries the user's answer.
func (s *server) refund(id string, p mcp.Params, caps map[string]json.RawMessage) (map[string]any, *jsonrpc.Error) {
	var responses map[string]struct {
		Action  string         `json:"action"`
		Content map[string]any `json:"content"`
	}
	_ = json.Unmarshal(p["inputResponses"], &responses)
	if r, ok := responses["confirm_refund"]; ok {
		if r.Action == "accept" && r.Content["confirm"] == true {
			return map[string]any{"content": text("Refund issued for " + id + " (fictional).")}, nil
		}
		return map[string]any{"content": text("Refund for " + id + " cancelled by the user.")}, nil
	}
	if _, ok := caps["elicitation"]; !ok {
		// The spec forbids asking a client for input it did not declare.
		return map[string]any{"content": text("Refunds need user confirmation, but this client does not support elicitation. Nothing was refunded."), "isError": true}, nil
	}
	return map[string]any{
		"resultType": mcp.ResultTypeInputRequired,
		"inputRequests": map[string]any{
			"confirm_refund": map[string]any{
				"method": "elicitation/create",
				"params": map[string]any{
					"mode":    "form",
					"message": "Refund invoice " + id + " for ₹48,500.00?",
					"requestedSchema": map[string]any{
						"type":       "object",
						"properties": map[string]any{"confirm": map[string]any{"type": "boolean", "title": "Yes, refund it"}},
						"required":   []string{"confirm"},
					},
				},
			},
		},
		"requestState": "refund:" + id,
	}, nil
}
