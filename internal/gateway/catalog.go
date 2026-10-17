package gateway

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/harshmangalamv/the-junction/internal/mcp"
	"github.com/harshmangalamv/the-junction/internal/policy"
	"github.com/harshmangalamv/the-junction/internal/upstream"
)

// toolEntry is one tool as clients see it.
type toolEntry struct {
	Name         string          // namespaced, e.g. crm__lookup_customer
	Server       string          // upstream server name
	UpstreamName string          // name the upstream knows it by
	Def          json.RawMessage // definition with the namespaced name
	Tokens       int             // estimated tokens the definition costs per request
	Decision     policy.Decision
}

type promptEntry struct {
	Name, Server, UpstreamName string
	Def                        json.RawMessage
}

type catalog struct {
	tools     map[string]*toolEntry
	toolOrder []string
	prompts   map[string]*promptEntry
	promptOrd []string
	resources []json.RawMessage
	resOwner  map[string]string // uri → server
	templates []json.RawMessage
	errors    map[string]string // server → last listing error
	updated   time.Time
}

func emptyCatalog() *catalog {
	return &catalog{tools: map[string]*toolEntry{}, prompts: map[string]*promptEntry{}, resOwner: map[string]string{}, errors: map[string]string{}}
}

// EstimateTokens approximates how many LLM tokens a JSON definition costs.
// ~4 characters per token is the usual rule of thumb for English + JSON;
// it is an estimate, not a tokenizer.
func EstimateTokens(b []byte) int { return (len(b) + 3) / 4 }

// listAll pages through a list method and returns the items under key.
func listAll(ctx context.Context, c *upstream.Client, method, key string) ([]json.RawMessage, error) {
	var out []json.RawMessage
	cursor := ""
	for page := 0; page < 100; page++ {
		p := mcp.Params{}
		if cursor != "" {
			p.Set("cursor", cursor)
		}
		resp, err := c.Request(ctx, method, p, nil)
		if err != nil {
			return out, err
		}
		if resp.Error != nil {
			return out, resp.Error
		}
		var r map[string]json.RawMessage
		if err := json.Unmarshal(resp.Result, &r); err != nil {
			return out, err
		}
		var items []json.RawMessage
		_ = json.Unmarshal(r[key], &items)
		out = append(out, items...)
		cursor = ""
		_ = json.Unmarshal(r["nextCursor"], &cursor)
		if cursor == "" {
			break
		}
	}
	return out, nil
}

func hasCapability(st upstream.Status, name string) bool {
	if len(st.Capabilities) == 0 {
		return true // unknown: try anyway
	}
	var caps map[string]json.RawMessage
	if json.Unmarshal(st.Capabilities, &caps) != nil {
		return true
	}
	_, ok := caps[name]
	return ok
}

func renamed(def json.RawMessage, newName string) (json.RawMessage, string) {
	var m map[string]json.RawMessage
	if json.Unmarshal(def, &m) != nil {
		return nil, ""
	}
	var orig string
	_ = json.Unmarshal(m["name"], &orig)
	m["name"], _ = json.Marshal(newName)
	b, _ := json.Marshal(m)
	return b, orig
}

// refresh lists every upstream's tools, prompts and resources in parallel
// and swaps in a new catalog.
func (g *Gateway) refresh(ctx context.Context) *catalog {
	g.refreshMu.Lock()
	defer g.refreshMu.Unlock()
	// Coalesce bursts: reuse a catalog built in the last second.
	if cur := g.cat(); !g.stale.Load() && time.Since(cur.updated) < time.Second {
		return cur
	}
	g.stale.Store(false)

	type result struct {
		server               string
		tools, prompts       []json.RawMessage
		resources, templates []json.RawMessage
		err                  string
	}
	results := make([]result, len(g.clients))
	var wg sync.WaitGroup
	for i, c := range g.clients {
		wg.Add(1)
		go func(i int, c *upstream.Client) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			r := result{server: c.Name()}
			var errs []string
			// Calling Request first also (re)connects if needed.
			tools, err := listAll(ctx, c, "tools/list", "tools")
			if err != nil {
				errs = append(errs, "tools: "+err.Error())
			}
			r.tools = tools
			st := c.Status()
			if st.Connected {
				if hasCapability(st, "prompts") {
					r.prompts, _ = listAll(ctx, c, "prompts/list", "prompts")
				}
				if hasCapability(st, "resources") {
					r.resources, _ = listAll(ctx, c, "resources/list", "resources")
					r.templates, _ = listAll(ctx, c, "resources/templates/list", "resourceTemplates")
				}
			}
			r.err = strings.Join(errs, "; ")
			results[i] = r
		}(i, c)
	}
	wg.Wait()

	cat := emptyCatalog()
	cat.updated = time.Now()
	sep := g.cfg.Separator
	for _, r := range results {
		if r.err != "" {
			cat.errors[r.server] = r.err
		}
		for _, def := range r.tools {
			var probe struct {
				Name string `json:"name"`
			}
			_ = json.Unmarshal(def, &probe)
			ns := r.server + sep + probe.Name
			nd, orig := renamed(def, ns)
			if nd == nil {
				continue
			}
			cat.tools[ns] = &toolEntry{
				Name: ns, Server: r.server, UpstreamName: orig, Def: nd,
				Tokens: EstimateTokens(nd), Decision: g.policy.Tool(ns, nd),
			}
			cat.toolOrder = append(cat.toolOrder, ns)
		}
		for _, def := range r.prompts {
			var probe struct {
				Name string `json:"name"`
			}
			_ = json.Unmarshal(def, &probe)
			ns := r.server + sep + probe.Name
			nd, orig := renamed(def, ns)
			if nd == nil {
				continue
			}
			cat.prompts[ns] = &promptEntry{Name: ns, Server: r.server, UpstreamName: orig, Def: nd}
			cat.promptOrd = append(cat.promptOrd, ns)
		}
		for _, def := range r.resources {
			var probe struct {
				URI string `json:"uri"`
			}
			_ = json.Unmarshal(def, &probe)
			if _, dup := cat.resOwner[probe.URI]; dup {
				continue // first server wins on URI collisions
			}
			cat.resOwner[probe.URI] = r.server
			cat.resources = append(cat.resources, def)
		}
		cat.templates = append(cat.templates, r.templates...)
	}
	// Deterministic order helps client caching (2026-07-28 SHOULD).
	sort.Strings(cat.toolOrder)
	sort.Strings(cat.promptOrd)

	g.catMu.Lock()
	g.catalog = cat
	g.catMu.Unlock()
	return cat
}

func (g *Gateway) cat() *catalog {
	g.catMu.RLock()
	defer g.catMu.RUnlock()
	return g.catalog
}
