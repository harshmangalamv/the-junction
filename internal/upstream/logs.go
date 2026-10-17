package upstream

import (
	"sync"
	"time"
)

// LogLine is one line of server output or a Junction event about a server.
type LogLine struct {
	Time   time.Time `json:"time"`
	Stream string    `json:"stream"` // stderr, stdout, junction
	Text   string    `json:"text"`
}

// LogRing keeps the most recent log lines.
type LogRing struct {
	mu    sync.Mutex
	lines []LogLine
	max   int
}

// NewLogRing makes a ring holding max lines.
func NewLogRing(max int) *LogRing { return &LogRing{max: max} }

// Add appends a line, dropping the oldest when full.
func (r *LogRing) Add(stream, text string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, LogLine{Time: time.Now(), Stream: stream, Text: text})
	if len(r.lines) > r.max {
		r.lines = r.lines[len(r.lines)-r.max:]
	}
}

// Lines returns a copy of the buffered lines, oldest first.
func (r *LogRing) Lines() []LogLine {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]LogLine(nil), r.lines...)
}
