// Package recorder keeps every exchange that passes through Junction: in
// memory for the dashboard, and optionally appended to a JSONL file for
// later replay.
package recorder

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Entry is one request/response exchange (or a notification).
type Entry struct {
	Seq        int64           `json:"seq"`
	Time       time.Time       `json:"time"`
	Kind       string          `json:"kind"` // request | notification
	Client     string          `json:"client"`
	ClientEra  string          `json:"clientEra"`
	Version    string          `json:"version"`
	Session    string          `json:"session,omitempty"`
	Method     string          `json:"method"`
	Server     string          `json:"server,omitempty"`
	Tool       string          `json:"tool,omitempty"`     // namespaced tool / prompt / resource
	Upstream   string          `json:"upstream,omitempty"` // name as the upstream knows it
	ServerEra  string          `json:"serverEra,omitempty"`
	Params     json.RawMessage `json:"params,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      string          `json:"error,omitempty"`
	ErrorCode  int             `json:"errorCode,omitempty"`
	IsError    bool            `json:"isError,omitempty"` // tool result with isError=true
	Status     string          `json:"status"`            // ok | error | blocked | input_required
	DurationMs float64         `json:"durationMs"`
	Redactions map[string]int  `json:"redactions,omitempty"`
	Bridged    bool            `json:"bridged,omitempty"` // client and server eras differ
	Truncated  bool            `json:"truncated,omitempty"`
}

// Recorder stores entries.
type Recorder struct {
	mu      sync.Mutex
	seq     int64
	ring    []Entry
	keep    int
	maxBody int
	file    *os.File
	w       *bufio.Writer
	subs    map[chan Entry]struct{}
	stats   stats
}

type stats struct {
	started    time.Time
	requests   int64
	errors     int64
	blocked    int64
	bridged    int64
	redactions map[string]int64
	durations  []float64 // recent, for percentiles
	perTool    map[string]*ToolStats
	perClient  map[string]int64
}

// ToolStats aggregates calls to one tool.
type ToolStats struct {
	Calls      int64   `json:"calls"`
	Errors     int64   `json:"errors"`
	TotalMs    float64 `json:"totalMs"`
	MaxMs      float64 `json:"maxMs"`
	Redactions int64   `json:"redactions"`
}

// New creates a recorder. file may be empty.
func New(file string, keep, maxBody int) (*Recorder, error) {
	r := &Recorder{keep: keep, maxBody: maxBody, subs: map[chan Entry]struct{}{}}
	r.stats = stats{started: time.Now(), redactions: map[string]int64{}, perTool: map[string]*ToolStats{}, perClient: map[string]int64{}}
	if file != "" {
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, err
		}
		r.file, r.w = f, bufio.NewWriter(f)
	}
	return r, nil
}

func (r *Recorder) trim(b json.RawMessage) (json.RawMessage, bool) {
	if r.maxBody <= 0 || len(b) <= r.maxBody {
		return b, false
	}
	s, _ := json.Marshal(string(b[:r.maxBody]) + "…[truncated]")
	return s, true
}

// Add stores e, assigning its sequence number.
func (r *Recorder) Add(e Entry) Entry {
	var t1, t2 bool
	e.Params, t1 = r.trim(e.Params)
	e.Result, t2 = r.trim(e.Result)
	e.Truncated = t1 || t2

	r.mu.Lock()
	r.seq++
	e.Seq = r.seq
	r.ring = append(r.ring, e)
	if len(r.ring) > r.keep {
		r.ring = r.ring[len(r.ring)-r.keep:]
	}
	r.account(e)
	if r.w != nil {
		if b, err := json.Marshal(e); err == nil {
			r.w.Write(b)
			r.w.WriteByte('\n')
			r.w.Flush()
		}
	}
	for ch := range r.subs {
		select {
		case ch <- e:
		default: // slow subscriber: drop rather than block traffic
		}
	}
	r.mu.Unlock()
	return e
}

func (r *Recorder) account(e Entry) {
	s := &r.stats
	if e.Kind != "request" {
		return
	}
	s.requests++
	s.perClient[e.Client]++
	switch e.Status {
	case "error":
		s.errors++
	case "blocked":
		s.blocked++
	}
	if e.Bridged {
		s.bridged++
	}
	n := 0
	for k, v := range e.Redactions {
		s.redactions[k] += int64(v)
		n += v
	}
	s.durations = append(s.durations, e.DurationMs)
	if len(s.durations) > 5000 {
		s.durations = s.durations[len(s.durations)-5000:]
	}
	if e.Method == "tools/call" && e.Tool != "" {
		ts := s.perTool[e.Tool]
		if ts == nil {
			ts = &ToolStats{}
			s.perTool[e.Tool] = ts
		}
		ts.Calls++
		if e.Status == "error" || e.IsError {
			ts.Errors++
		}
		ts.TotalMs += e.DurationMs
		if e.DurationMs > ts.MaxMs {
			ts.MaxMs = e.DurationMs
		}
		ts.Redactions += int64(n)
	}
}

// Recent returns up to n newest entries, newest first.
func (r *Recorder) Recent(n int) []Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n <= 0 || n > len(r.ring) {
		n = len(r.ring)
	}
	out := make([]Entry, n)
	for i := 0; i < n; i++ {
		out[i] = r.ring[len(r.ring)-1-i]
	}
	return out
}

// Get returns the entry with sequence number seq.
func (r *Recorder) Get(seq int64) (Entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.ring) - 1; i >= 0; i-- {
		if r.ring[i].Seq == seq {
			return r.ring[i], true
		}
	}
	return Entry{}, false
}

// Subscribe returns a channel of new entries; call cancel when done.
func (r *Recorder) Subscribe() (ch chan Entry, cancel func()) {
	ch = make(chan Entry, 64)
	r.mu.Lock()
	r.subs[ch] = struct{}{}
	r.mu.Unlock()
	return ch, func() {
		r.mu.Lock()
		delete(r.subs, ch)
		r.mu.Unlock()
	}
}

// Stats is a snapshot for the dashboard.
type Stats struct {
	UptimeSec  float64              `json:"uptimeSec"`
	Requests   int64                `json:"requests"`
	Errors     int64                `json:"errors"`
	Blocked    int64                `json:"blocked"`
	Bridged    int64                `json:"bridged"`
	P50Ms      float64              `json:"p50Ms"`
	P95Ms      float64              `json:"p95Ms"`
	P99Ms      float64              `json:"p99Ms"`
	Redactions map[string]int64     `json:"redactions"`
	PerTool    map[string]ToolStats `json:"perTool"`
	PerClient  map[string]int64     `json:"perClient"`
}

// Stats returns aggregate counters.
func (r *Recorder) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stats
	out := Stats{
		UptimeSec: time.Since(s.started).Seconds(), Requests: s.requests, Errors: s.errors,
		Blocked: s.blocked, Bridged: s.bridged,
		Redactions: map[string]int64{}, PerTool: map[string]ToolStats{}, PerClient: map[string]int64{},
	}
	for k, v := range s.redactions {
		out.Redactions[k] = v
	}
	for k, v := range s.perTool {
		out.PerTool[k] = *v
	}
	for k, v := range s.perClient {
		out.PerClient[k] = v
	}
	d := append([]float64(nil), s.durations...)
	sort.Float64s(d)
	out.P50Ms, out.P95Ms, out.P99Ms = pct(d, 50), pct(d, 95), pct(d, 99)
	return out
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p / 100)
	return sorted[i]
}

// Close flushes the file.
func (r *Recorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.w != nil {
		r.w.Flush()
		return r.file.Close()
	}
	return nil
}

// ReadFile loads entries from a JSONL recording.
func ReadFile(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Entry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 64<<20)
	for sc.Scan() {
		var e Entry
		if json.Unmarshal(sc.Bytes(), &e) == nil {
			out = append(out, e)
		}
	}
	return out, sc.Err()
}
