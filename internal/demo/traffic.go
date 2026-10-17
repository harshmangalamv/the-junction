package demo

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/harshmangalamv/the-junction/internal/mcp"
)

// SimulateTraffic plays two fake agents against the gateway at url so the
// dashboard has something to show: an older client on the legacy protocol
// and a newer one on 2026-07-28. It runs until ctx is cancelled.
func SimulateTraffic(ctx context.Context, url, token string, logger *log.Logger) {
	legacy := &Client{URL: url, Era: mcp.EraLegacy, Token: token, Info: mcp.Implementation{Name: "sim-legacy-agent", Version: "2025.11"}}
	modern := &Client{URL: url, Era: mcp.EraModern, Token: token, Info: mcp.Implementation{Name: "sim-modern-agent", Version: "2026.7"},
		Caps: map[string]any{"elicitation": map[string]any{"form": map[string]any{}}}}

	type step struct {
		c      *Client
		method string
		params map[string]any
	}
	steps := []step{
		{legacy, "tools/list", nil},
		{legacy, "tools/call", map[string]any{"name": "crm__search_customers", "arguments": map[string]any{"query": "hyderabad"}}},
		{legacy, "tools/call", map[string]any{"name": "crm__lookup_customer", "arguments": map[string]any{"customer_id": "C-1001"}}},
		{modern, "tools/list", nil},
		{modern, "tools/call", map[string]any{"name": "crm__lookup_customer", "arguments": map[string]any{"customer_id": "C-1002"}}},
		{modern, "tools/call", map[string]any{"name": "billing__get_invoice", "arguments": map[string]any{"invoice_id": "INV-2024-001"}}},
		{legacy, "tools/call", map[string]any{"name": "billing__get_invoice", "arguments": map[string]any{"invoice_id": "INV-2024-007"}}},
		{modern, "tools/call", map[string]any{"name": "billing__refund_invoice", "arguments": map[string]any{"invoice_id": "INV-2024-001", "reason": "duplicate charge"}}},
		{legacy, "tools/call", map[string]any{"name": "billing__refund_invoice", "arguments": map[string]any{"invoice_id": "INV-2024-002"}}},
		{legacy, "tools/call", map[string]any{"name": "crm__delete_customer", "arguments": map[string]any{"customer_id": "C-1003"}}},
		{modern, "tools/call", map[string]any{"name": "crm__export_report", "arguments": map[string]any{"format": "csv"}}},
		{legacy, "resources/read", map[string]any{"uri": "crm://schema/customer"}},
		{modern, "prompts/get", map[string]any{"name": "crm__summarize_customer", "arguments": map[string]any{"customer_id": "C-1003"}}},
	}

	connected := false
	i := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(1200+rand.Intn(1800)) * time.Millisecond):
		}
		if !connected {
			if err := legacy.Connect(ctx); err != nil {
				logger.Printf("demo traffic: legacy connect: %v", err)
				continue
			}
			if err := modern.Connect(ctx); err != nil {
				logger.Printf("demo traffic: modern connect: %v", err)
				continue
			}
			connected = true
		}
		s := steps[i%len(steps)]
		i++
		_, _ = s.c.Call(ctx, s.method, s.params)
	}
}
