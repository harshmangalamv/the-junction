// Package redact finds and masks PII and secrets in JSON payloads.
package redact

import (
	"bytes"
	"encoding/json"
	"regexp"
	"sort"
	"strings"

	"github.com/harshmangalamv/the-junction/internal/config"
)

// Rule is one detector.
type Rule struct {
	Name     string
	re       *regexp.Regexp
	validate func(match string) bool
}

// builtins are ordered: longer/more specific patterns first so a card
// number is not half-matched as something else.
var builtins = []Rule{
	{Name: "private_key", re: regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----`)},
	{Name: "jwt", re: regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}`)},
	{Name: "aws_access_key", re: regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`)},
	{Name: "github_token", re: regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{36,}\b`)},
	{Name: "bearer_token", re: regexp.MustCompile(`(?i)\bbearer\s+[A-Za-z0-9._~+/-]{20,}=*`)},
	{Name: "email", re: regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)},
	{Name: "credit_card", re: regexp.MustCompile(`\b(?:\d[ -]?){12,18}\d\b`), validate: luhn},
	{Name: "aadhaar", re: regexp.MustCompile(`\b[2-9]\d{3}[ -]?\d{4}[ -]?\d{4}\b`), validate: verhoeff},
	{Name: "pan", re: regexp.MustCompile(`\b[A-Z]{3}[ABCFGHLJPTK][A-Z]\d{4}[A-Z]\b`)},
	{Name: "ssn", re: regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)},
	{Name: "phone", re: regexp.MustCompile(`(?:\+91[ -]?)?\b[6-9]\d{4}[ -]?\d{5}\b|(?:\+1[ -]?)?\(?\b\d{3}\)?[ .-]\d{3}[ .-]\d{4}\b`)},
}

// BuiltinNames lists the built-in rule names.
func BuiltinNames() []string {
	out := make([]string, len(builtins))
	for i, r := range builtins {
		out[i] = r.Name
	}
	return out
}

// Redactor applies rules to payloads.
type Redactor struct {
	mode  string // redact | detect | off
	rules []Rule
}

// New builds a redactor from config.
func New(cfg config.Redact) *Redactor {
	r := &Redactor{mode: cfg.Mode}
	want := map[string]bool{}
	for _, n := range cfg.Rules {
		want[n] = true
	}
	for _, b := range builtins {
		if len(want) == 0 || want[b.Name] {
			r.rules = append(r.rules, b)
		}
	}
	for _, c := range cfg.Custom {
		r.rules = append(r.rules, Rule{Name: c.Name, re: regexp.MustCompile(c.Pattern)})
	}
	return r
}

// Mode returns the configured mode.
func (r *Redactor) Mode() string { return r.mode }

// Findings counts matches per rule.
type Findings map[string]int

// Total is the number of matches.
func (f Findings) Total() int {
	n := 0
	for _, v := range f {
		n += v
	}
	return n
}

// String renders "email×2, phone×1".
func (f Findings) String() string {
	keys := make([]string, 0, len(f))
	for k := range f {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "×" + itoa(f[k])
	}
	return strings.Join(parts, ", ")
}

// String scans s. In redact mode matches are replaced with [REDACTED:RULE].
func (r *Redactor) String(s string, f Findings) string {
	if r.mode == "off" {
		return s
	}
	for _, rule := range r.rules {
		s = rule.re.ReplaceAllStringFunc(s, func(m string) string {
			if rule.validate != nil && !rule.validate(m) {
				return m
			}
			f[rule.Name]++
			if r.mode == "detect" {
				return m
			}
			return "[REDACTED:" + strings.ToUpper(rule.Name) + "]"
		})
	}
	return s
}

// JSON scans every string value (not keys) in raw. The original bytes are
// returned untouched when nothing was replaced.
func (r *Redactor) JSON(raw json.RawMessage) (json.RawMessage, Findings) {
	f := Findings{}
	if r.mode == "off" || len(raw) == 0 {
		return raw, f
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return raw, f
	}
	changed := false
	v = r.walk(v, f, &changed)
	if !changed {
		return raw, f
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return raw, f
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), f
}

func (r *Redactor) walk(v any, f Findings, changed *bool) any {
	switch t := v.(type) {
	case string:
		out := r.String(t, f)
		if out != t {
			*changed = true
		}
		return out
	case []any:
		for i := range t {
			t[i] = r.walk(t[i], f, changed)
		}
	case map[string]any:
		for k := range t {
			t[k] = r.walk(t[k], f, changed)
		}
	}
	return v
}

func digits(s string) []int {
	var d []int
	for _, c := range s {
		if c >= '0' && c <= '9' {
			d = append(d, int(c-'0'))
		}
	}
	return d
}

// luhn validates card numbers (13–19 digits).
func luhn(s string) bool {
	d := digits(s)
	if len(d) < 13 || len(d) > 19 {
		return false
	}
	sum := 0
	for i := len(d) - 1; i >= 0; i-- {
		n := d[i]
		if (len(d)-1-i)%2 == 1 {
			n *= 2
			if n > 9 {
				n -= 9
			}
		}
		sum += n
	}
	return sum%10 == 0
}

var verhoeffD = [10][10]int{
	{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, {1, 2, 3, 4, 0, 6, 7, 8, 9, 5},
	{2, 3, 4, 0, 1, 7, 8, 9, 5, 6}, {3, 4, 0, 1, 2, 8, 9, 5, 6, 7},
	{4, 0, 1, 2, 3, 9, 5, 6, 7, 8}, {5, 9, 8, 7, 6, 0, 4, 3, 2, 1},
	{6, 5, 9, 8, 7, 1, 0, 4, 3, 2}, {7, 6, 5, 9, 8, 2, 1, 0, 4, 3},
	{8, 7, 6, 5, 9, 3, 2, 1, 0, 4}, {9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
}
var verhoeffP = [8][10]int{
	{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, {1, 5, 7, 6, 2, 8, 3, 0, 9, 4},
	{5, 8, 0, 3, 7, 9, 6, 1, 4, 2}, {8, 9, 1, 6, 0, 4, 3, 5, 2, 7},
	{9, 4, 5, 3, 1, 2, 6, 8, 7, 0}, {4, 2, 8, 6, 5, 7, 3, 9, 0, 1},
	{2, 7, 9, 3, 8, 0, 6, 4, 1, 5}, {7, 0, 4, 6, 9, 1, 3, 2, 5, 8},
}

// verhoeff validates Aadhaar numbers (12 digits with a Verhoeff checksum),
// which keeps random 12-digit numbers from being flagged.
func verhoeff(s string) bool {
	d := digits(s)
	if len(d) != 12 {
		return false
	}
	c := 0
	for i := 0; i < len(d); i++ {
		c = verhoeffD[c][verhoeffP[i%8][d[len(d)-1-i]]]
	}
	return c == 0
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
