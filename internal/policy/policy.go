// Package policy decides which tools clients may see and call.
package policy

import (
	"encoding/json"
	"path"

	"github.com/harshmangalamv/the-junction/internal/config"
)

// Decision explains whether a tool is allowed.
type Decision struct {
	Allowed bool   `json:"allowed"`
	Rule    string `json:"rule,omitempty"` // the rule that decided, e.g. "deny: *delete*"
}

// Policy evaluates config rules.
type Policy struct{ cfg config.Policy }

// New builds a policy from config.
func New(cfg config.Policy) *Policy { return &Policy{cfg: cfg} }

// Tool decides for a namespaced tool name and its raw definition (used to
// read annotations such as destructiveHint).
func (p *Policy) Tool(name string, def json.RawMessage) Decision {
	for _, pat := range p.cfg.Deny {
		if match(pat, name) {
			return Decision{Allowed: false, Rule: "deny: " + pat}
		}
	}
	if p.cfg.DenyDestructive && isDestructive(def) {
		return Decision{Allowed: false, Rule: "deny_destructive"}
	}
	if len(p.cfg.Allow) == 0 {
		return Decision{Allowed: true}
	}
	for _, pat := range p.cfg.Allow {
		if match(pat, name) {
			return Decision{Allowed: true, Rule: "allow: " + pat}
		}
	}
	return Decision{Allowed: false, Rule: "not in allow list"}
}

func match(pattern, name string) bool {
	ok, err := path.Match(pattern, name)
	return err == nil && ok
}

// isDestructive follows the MCP tool annotation defaults: destructiveHint
// defaults to true unless the tool is read-only.
func isDestructive(def json.RawMessage) bool {
	var t struct {
		Annotations *struct {
			ReadOnlyHint    *bool `json:"readOnlyHint"`
			DestructiveHint *bool `json:"destructiveHint"`
		} `json:"annotations"`
	}
	if json.Unmarshal(def, &t) != nil || t.Annotations == nil {
		// No annotations: we can't know; don't block.
		return false
	}
	a := t.Annotations
	if a.ReadOnlyHint != nil && *a.ReadOnlyHint {
		return false
	}
	return a.DestructiveHint != nil && *a.DestructiveHint
}
