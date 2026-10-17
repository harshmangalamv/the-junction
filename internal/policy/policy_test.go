package policy

import (
	"encoding/json"
	"testing"

	"github.com/harshmangalamv/the-junction/internal/config"
)

func TestPolicy(t *testing.T) {
	destructive := json.RawMessage(`{"name":"x","annotations":{"destructiveHint":true}}`)
	readOnly := json.RawMessage(`{"name":"x","annotations":{"readOnlyHint":true,"destructiveHint":true}}`)
	plain := json.RawMessage(`{"name":"x"}`)

	p := New(config.Policy{Deny: []string{"crm__delete_*"}})
	if d := p.Tool("crm__delete_customer", plain); d.Allowed || d.Rule != "deny: crm__delete_*" {
		t.Errorf("deny glob: %+v", d)
	}
	if d := p.Tool("crm__lookup_customer", plain); !d.Allowed {
		t.Errorf("default allow: %+v", d)
	}

	p = New(config.Policy{Allow: []string{"billing__get_*"}, Deny: []string{"billing__get_secret"}})
	if !p.Tool("billing__get_invoice", plain).Allowed {
		t.Error("allow list should permit billing__get_invoice")
	}
	if p.Tool("billing__get_secret", plain).Allowed {
		t.Error("deny must win over allow")
	}
	if d := p.Tool("crm__lookup", plain); d.Allowed || d.Rule != "not in allow list" {
		t.Errorf("allow list should block others: %+v", d)
	}

	p = New(config.Policy{DenyDestructive: true})
	if p.Tool("a", destructive).Allowed {
		t.Error("destructive tool should be hidden")
	}
	if !p.Tool("a", readOnly).Allowed {
		t.Error("readOnlyHint overrides destructiveHint")
	}
	if !p.Tool("a", plain).Allowed {
		t.Error("unannotated tools are not blocked")
	}
}
