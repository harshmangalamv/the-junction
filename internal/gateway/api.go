package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/harshmangalamv/the-junction/internal/recorder"
	"github.com/harshmangalamv/the-junction/internal/upstream"
)

// ServerView is a server as the dashboard shows it.
type ServerView struct {
	upstream.Status
	Tools     int    `json:"tools"`
	ListError string `json:"listError,omitempty"`
}

// ToolView is a tool as the dashboard shows it.
type ToolView struct {
	Name         string          `json:"name"`
	Server       string          `json:"server"`
	UpstreamName string          `json:"upstreamName"`
	Description  string          `json:"description"`
	Tokens       int             `json:"tokens"`
	Allowed      bool            `json:"allowed"`
	Rule         string          `json:"rule,omitempty"`
	Annotations  json.RawMessage `json:"annotations,omitempty"`
	Definition   json.RawMessage `json:"definition"`
	Calls        int64           `json:"calls"`
	Errors       int64           `json:"errors"`
	AvgMs        float64         `json:"avgMs"`
	Redactions   int64           `json:"redactions"`
}

// Servers returns every upstream's status.
func (g *Gateway) Servers() []ServerView {
	cat := g.cat()
	counts := map[string]int{}
	for _, t := range cat.tools {
		counts[t.Server]++
	}
	out := make([]ServerView, 0, len(g.clients))
	for _, c := range g.clients {
		out = append(out, ServerView{Status: c.Status(), Tools: counts[c.Name()], ListError: cat.errors[c.Name()]})
	}
	return out
}

// Tools returns the catalog with policy decisions and call stats.
func (g *Gateway) Tools() []ToolView {
	cat := g.cat()
	stats := g.Rec.Stats().PerTool
	out := make([]ToolView, 0, len(cat.toolOrder))
	for _, n := range cat.toolOrder {
		t := cat.tools[n]
		var d struct {
			Description string          `json:"description"`
			Annotations json.RawMessage `json:"annotations"`
		}
		_ = json.Unmarshal(t.Def, &d)
		v := ToolView{
			Name: t.Name, Server: t.Server, UpstreamName: t.UpstreamName, Description: d.Description,
			Tokens: t.Tokens, Allowed: t.Decision.Allowed, Rule: t.Decision.Rule,
			Annotations: d.Annotations, Definition: t.Def,
		}
		if s, ok := stats[t.Name]; ok {
			v.Calls, v.Errors, v.Redactions = s.Calls, s.Errors, s.Redactions
			if s.Calls > 0 {
				v.AvgMs = s.TotalMs / float64(s.Calls)
			}
		}
		out = append(out, v)
	}
	return out
}

// TokenReport sums what the visible tool list costs per request.
type TokenReport struct {
	VisibleTools  int            `json:"visibleTools"`
	HiddenTools   int            `json:"hiddenTools"`
	VisibleTokens int            `json:"visibleTokens"`
	HiddenTokens  int            `json:"hiddenTokens"`
	PerServer     map[string]int `json:"perServer"`
}

// Tokens computes the token report.
func (g *Gateway) Tokens() TokenReport {
	r := TokenReport{PerServer: map[string]int{}}
	for _, t := range g.cat().tools {
		if t.Decision.Allowed {
			r.VisibleTools++
			r.VisibleTokens += t.Tokens
			r.PerServer[t.Server] += t.Tokens
		} else {
			r.HiddenTools++
			r.HiddenTokens += t.Tokens
		}
	}
	return r
}

// RegisterAPI adds the dashboard's JSON API to mux.
func (g *Gateway) RegisterAPI(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/overview", func(w http.ResponseWriter, r *http.Request) {
		servers := g.Servers()
		up := 0
		for _, s := range servers {
			if s.Connected {
				up++
			}
		}
		writeJSON(w, 200, map[string]any{
			"version": Version, "listen": g.cfg.Listen, "endpoint": "http://" + g.cfg.Listen + "/mcp",
			"stats": g.Rec.Stats(), "sessions": g.Sessions(),
			"servers": len(servers), "serversUp": up,
			"tokens": g.Tokens(), "redactMode": g.redactor.Mode(),
			"recordFile": g.cfg.Resolve(g.cfg.Record.File), "separator": g.cfg.Separator,
			"policy": g.cfg.Policy,
		})
	})
	mux.HandleFunc("GET /api/servers", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, g.Servers())
	})
	mux.HandleFunc("GET /api/servers/{name}/logs", func(w http.ResponseWriter, r *http.Request) {
		c, ok := g.byName[r.PathValue("name")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, 200, c.Logs.Lines())
	})
	mux.HandleFunc("GET /api/tools", func(w http.ResponseWriter, r *http.Request) {
		tools := g.Tools()
		sort.SliceStable(tools, func(i, j int) bool { return tools[i].Tokens > tools[j].Tokens })
		writeJSON(w, 200, tools)
	})
	mux.HandleFunc("POST /api/refresh", func(w http.ResponseWriter, r *http.Request) {
		g.stale.Store(true)
		cat := g.refresh(r.Context())
		writeJSON(w, 200, map[string]int{"tools": len(cat.tools), "prompts": len(cat.prompts), "resources": len(cat.resources)})
	})
	mux.HandleFunc("GET /api/traffic", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if n == 0 {
			n = 200
		}
		writeJSON(w, 200, g.Rec.Recent(n))
	})
	mux.HandleFunc("GET /api/traffic/{seq}", func(w http.ResponseWriter, r *http.Request) {
		seq, _ := strconv.ParseInt(r.PathValue("seq"), 10, 64)
		e, ok := g.Rec.Get(seq)
		if !ok {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, 200, e)
	})
	mux.HandleFunc("GET /api/events", g.serveEvents)
}

// serveEvents streams new exchanges to the dashboard over SSE.
func (g *Gateway) serveEvents(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	ch, cancel := g.Rec.Subscribe()
	defer cancel()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	fmt.Fprint(w, ": connected\n\n")
	fl.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		case e := <-ch:
			b, _ := json.Marshal(summary(e))
			fmt.Fprintf(w, "data: %s\n\n", b)
			fl.Flush()
		}
	}
}

// summary drops bodies; the UI fetches full entries on demand.
func summary(e recorder.Entry) recorder.Entry {
	e.Params, e.Result = nil, nil
	return e
}
