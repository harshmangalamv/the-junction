// Package replay re-sends recorded tool calls through the gateway and
// reports any whose results changed — a regression test built from real
// agent traffic.
package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/harshmangalamv/the-junction/internal/gateway"
	"github.com/harshmangalamv/the-junction/internal/jsonrpc"
	"github.com/harshmangalamv/the-junction/internal/mcp"
	"github.com/harshmangalamv/the-junction/internal/recorder"
)

// Options controls a replay run.
type Options struct {
	// Tools filters by namespaced tool name glob (empty = all).
	Tools string
	// Ignore are regexes whose matches are blanked before comparing
	// (timestamps, generated IDs…).
	Ignore []*regexp.Regexp
	// Dedupe replays each distinct (tool, arguments) pair once.
	Dedupe bool
}

// Result is the outcome for one recorded call.
type Result struct {
	Seq    int64  `json:"seq"`
	Tool   string `json:"tool"`
	Status string `json:"status"` // same | changed | error | skipped
	Diff   string `json:"diff,omitempty"`
	Note   string `json:"note,omitempty"`
}

// Report summarizes a run.
type Report struct {
	Results                        []Result
	Same, Changed, Errors, Skipped int
	Duration                       time.Duration
}

// Run replays tools/call entries through g (already started).
func Run(ctx context.Context, g *gateway.Gateway, entries []recorder.Entry, opt Options) Report {
	start := time.Now()
	var rep Report
	seen := map[string]bool{}
	for _, e := range entries {
		if e.Kind != "request" || e.Method != "tools/call" {
			continue
		}
		if opt.Tools != "" {
			if ok, _ := path.Match(opt.Tools, e.Tool); !ok {
				continue
			}
		}
		if opt.Dedupe {
			key := e.Tool + "\x00" + string(canonical(e.Params, nil))
			if seen[key] {
				continue
			}
			seen[key] = true
		}
		res := Result{Seq: e.Seq, Tool: e.Tool}
		switch {
		case e.Truncated:
			res.Status, res.Note = "skipped", "recording was truncated"
		case e.Status != "ok":
			res.Status, res.Note = "skipped", "original call was "+e.Status
		case len(e.Redactions) > 0 && hasRedactedArgs(e.Params):
			res.Status, res.Note = "skipped", "arguments were redacted when recorded"
		}
		if res.Status == "" {
			res = replayOne(ctx, g, e, opt)
		}
		switch res.Status {
		case "same":
			rep.Same++
		case "changed":
			rep.Changed++
		case "error":
			rep.Errors++
		default:
			rep.Skipped++
		}
		rep.Results = append(rep.Results, res)
	}
	rep.Duration = time.Since(start)
	return rep
}

func hasRedactedArgs(p json.RawMessage) bool { return bytes.Contains(p, []byte("[REDACTED:")) }

func replayOne(ctx context.Context, g *gateway.Gateway, e recorder.Entry, opt Options) Result {
	res := Result{Seq: e.Seq, Tool: e.Tool}
	p, err := mcp.DecodeParams(e.Params)
	if err != nil {
		res.Status, res.Note = "error", "bad recorded params"
		return res
	}
	mcp.StripEraMeta(p)
	req := &jsonrpc.Message{JSONRPC: "2.0", ID: json.RawMessage(fmt.Sprint(e.Seq)), Method: "tools/call", Params: p.Raw()}
	cc := &gateway.ClientCtx{Era: mcp.EraLegacy, Version: mcp.VersionLatestLegacy, Info: mcp.Implementation{Name: "junction-replay"}, Transport: "replay"}
	resp := g.Handle(ctx, cc, req)
	if resp.Error != nil {
		res.Status, res.Note = "error", resp.Error.Message
		return res
	}
	before := canonical(e.Result, opt.Ignore)
	after := canonical(resp.Result, opt.Ignore)
	if bytes.Equal(before, after) {
		res.Status = "same"
		return res
	}
	res.Status = "changed"
	res.Diff = lineDiff(string(before), string(after))
	return res
}

// volatile fields differ between eras or runs and never count as changes.
var volatile = []string{"_meta", "resultType", "ttlMs", "cacheScope"}

// canonical pretty-prints JSON with sorted keys, minus volatile fields and
// with ignore patterns blanked.
func canonical(raw json.RawMessage, ignore []*regexp.Regexp) []byte {
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(&v) != nil {
		return raw
	}
	if m, ok := v.(map[string]any); ok {
		for _, k := range volatile {
			delete(m, k)
		}
	}
	b, _ := json.MarshalIndent(v, "", "  ")
	s := string(b)
	for _, re := range ignore {
		s = re.ReplaceAllString(s, "<ignored>")
	}
	return []byte(s)
}

// lineDiff renders a minimal line diff (LCS) of a and b.
func lineDiff(a, b string) string {
	al, bl := strings.Split(a, "\n"), strings.Split(b, "\n")
	if len(al)*len(bl) > 400*400 {
		return "--- before\n" + a + "\n+++ after\n" + b
	}
	lcs := make([][]int, len(al)+1)
	for i := range lcs {
		lcs[i] = make([]int, len(bl)+1)
	}
	for i := len(al) - 1; i >= 0; i-- {
		for j := len(bl) - 1; j >= 0; j-- {
			if al[i] == bl[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	var out strings.Builder
	i, j := 0, 0
	for i < len(al) || j < len(bl) {
		switch {
		case i < len(al) && j < len(bl) && al[i] == bl[j]:
			i, j = i+1, j+1
		case j < len(bl) && (i == len(al) || lcs[i][j+1] >= lcs[i+1][j]):
			out.WriteString("+ " + bl[j] + "\n")
			j++
		default:
			out.WriteString("- " + al[i] + "\n")
			i++
		}
	}
	return out.String()
}

// Print writes a human-readable report.
func Print(w io.Writer, rep Report, verbose bool) {
	for _, r := range rep.Results {
		if r.Status == "skipped" && !verbose {
			continue
		}
		mark := map[string]string{"same": "✓", "changed": "✗", "error": "!", "skipped": "-"}[r.Status]
		fmt.Fprintf(w, "%s #%-5d %-40s %s", mark, r.Seq, r.Tool, r.Status)
		if r.Note != "" {
			fmt.Fprintf(w, " (%s)", r.Note)
		}
		fmt.Fprintln(w)
		if r.Status == "changed" {
			for _, l := range strings.Split(strings.TrimRight(r.Diff, "\n"), "\n") {
				fmt.Fprintln(w, "      "+l)
			}
		}
	}
	if rep.Skipped > 0 && !verbose {
		fmt.Fprintf(w, "(%d skipped calls hidden: blocked, truncated, needed user input or had redacted arguments; use -v to list them)\n", rep.Skipped)
	}
	fmt.Fprintf(w, "\n%d same, %d changed, %d errors, %d skipped in %s\n", rep.Same, rep.Changed, rep.Errors, rep.Skipped, rep.Duration.Round(time.Millisecond))
}
