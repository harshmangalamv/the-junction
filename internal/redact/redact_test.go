package redact

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/harshmangalamv/the-junction/internal/config"
)

func TestBuiltinRules(t *testing.T) {
	r := New(config.Redact{Mode: "redact"})
	cases := []struct {
		in, rule string
		want     bool
	}{
		{"mail priya.sharma@example.com now", "email", true},
		{"card 4111 1111 1111 1111", "credit_card", true},
		{"card 4111 1111 1111 1112", "credit_card", false}, // fails Luhn
		{"aadhaar 2345 6789 0124", "aadhaar", true},
		{"aadhaar 2345 6789 0125", "aadhaar", false}, // fails Verhoeff
		{"order 123456789012", "aadhaar", false},
		{"PAN ABCPS1234K", "pan", true},
		{"call +91 98765 43210", "phone", true},
		{"call (415) 555-0132", "phone", true},
		{"ssn 123-45-6789", "ssn", true},
		{"key AKIAABCDEFGHIJKLMNOP", "aws_access_key", true},
		{"Authorization: Bearer abcdefghijklmnopqrstuvwxyz123456", "bearer_token", true},
		{"version 2026-07-28 is fine", "", false},
	}
	for _, c := range cases {
		f := Findings{}
		out := r.String(c.in, f)
		got := c.rule != "" && f[c.rule] > 0
		if got != c.want {
			t.Errorf("%q: rule %s matched=%v want %v (findings %v, out %q)", c.in, c.rule, got, c.want, f, out)
		}
		if c.rule == "" && f.Total() != 0 {
			t.Errorf("%q: unexpected findings %v", c.in, f)
		}
		if got && !strings.Contains(out, "[REDACTED:"+strings.ToUpper(c.rule)+"]") {
			t.Errorf("%q: not replaced: %q", c.in, out)
		}
	}
}

func TestDetectModeLeavesText(t *testing.T) {
	r := New(config.Redact{Mode: "detect"})
	f := Findings{}
	in := "priya@example.com"
	if out := r.String(in, f); out != in || f["email"] != 1 {
		t.Fatalf("detect mode changed text or missed: %q %v", out, f)
	}
}

func TestJSONOnlyTouchesStringsAndKeepsBytes(t *testing.T) {
	r := New(config.Redact{Mode: "redact"})
	clean := json.RawMessage(`{"b":1,"a":"nothing here"}`)
	out, f := r.JSON(clean)
	if string(out) != string(clean) || f.Total() != 0 {
		t.Fatalf("clean payload modified: %s", out)
	}
	out, f = r.JSON(json.RawMessage(`{"content":[{"type":"text","text":"x@example.com"}],"n":42}`))
	if f["email"] != 1 || !strings.Contains(string(out), "[REDACTED:EMAIL]") || !strings.Contains(string(out), `"n":42`) {
		t.Fatalf("bad redaction: %s %v", out, f)
	}
}

func TestRuleSelectionAndCustom(t *testing.T) {
	r := New(config.Redact{Mode: "redact", Rules: []string{"email"}, Custom: []config.CustomRule{{Name: "employee_id", Pattern: `EMP-[0-9]{6}`}}})
	f := Findings{}
	out := r.String("EMP-123456 x@example.com 4111 1111 1111 1111", f)
	if f["employee_id"] != 1 || f["email"] != 1 || f["credit_card"] != 0 {
		t.Fatalf("findings %v out %q", f, out)
	}
}
