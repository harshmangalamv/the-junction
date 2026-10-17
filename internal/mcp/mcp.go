// Package mcp holds Model Context Protocol constants and helpers shared by
// the gateway's client-facing side and its upstream clients.
//
// Two protocol "eras" exist:
//   - Legacy (2025-11-25 and earlier): an initialize handshake opens a
//     session; HTTP uses the Mcp-Session-Id header.
//   - Modern (2026-07-28): stateless. Every request carries its protocol
//     version and client capabilities in params._meta.
package mcp

import (
	"encoding/json"
	"slices"
)

// Protocol versions.
const (
	VersionModern       = "2026-07-28"
	VersionLatestLegacy = "2025-11-25"
)

// LegacyVersions are the initialize-era versions Junction understands,
// newest first.
var LegacyVersions = []string{"2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"}

// ModernVersions are the stateless versions Junction understands.
var ModernVersions = []string{VersionModern}

// AllVersions lists every version Junction can serve, newest first.
func AllVersions() []string { return append(slices.Clone(ModernVersions), LegacyVersions...) }

// IsModernVersion reports whether v is a stateless (2026-07-28+) version.
func IsModernVersion(v string) bool { return slices.Contains(ModernVersions, v) }

// IsLegacyVersion reports whether v is an initialize-era version.
func IsLegacyVersion(v string) bool { return slices.Contains(LegacyVersions, v) }

// Reserved _meta keys from the 2026-07-28 spec.
const (
	MetaProtocolVersion    = "io.modelcontextprotocol/protocolVersion"
	MetaClientCapabilities = "io.modelcontextprotocol/clientCapabilities"
	MetaClientInfo         = "io.modelcontextprotocol/clientInfo"
	MetaServerInfo         = "io.modelcontextprotocol/serverInfo"
	MetaLogLevel           = "io.modelcontextprotocol/logLevel"
)

// eraMetaKeys are the per-request keys that describe the *sender*; a gateway
// must replace them rather than forward them.
var eraMetaKeys = []string{MetaProtocolVersion, MetaClientCapabilities, MetaClientInfo, MetaLogLevel}

// HTTP header names.
const (
	HeaderProtocolVersion = "MCP-Protocol-Version"
	HeaderSessionID       = "Mcp-Session-Id"
	HeaderMethod          = "Mcp-Method"
	HeaderName            = "Mcp-Name"
)

// Era identifies which side of the 2026 protocol split a peer speaks.
type Era int

const (
	EraUnknown Era = iota
	EraLegacy
	EraModern
)

func (e Era) String() string {
	switch e {
	case EraLegacy:
		return "legacy"
	case EraModern:
		return "modern"
	}
	return "unknown"
}

// MarshalJSON renders the era as a string.
func (e Era) MarshalJSON() ([]byte, error) { return json.Marshal(e.String()) }

// Implementation identifies a client or server.
type Implementation struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Title   string `json:"title,omitempty"`
}

// Params is a decoded params object. Using a map keeps unknown fields.
type Params map[string]json.RawMessage

// DecodeParams decodes raw params into a map (nil/empty → empty map).
func DecodeParams(raw json.RawMessage) (Params, error) {
	p := Params{}
	if len(raw) == 0 || string(raw) == "null" {
		return p, nil
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	return p, nil
}

// Meta returns params._meta as a map (never nil).
func (p Params) Meta() map[string]json.RawMessage {
	m := map[string]json.RawMessage{}
	if raw, ok := p["_meta"]; ok {
		_ = json.Unmarshal(raw, &m)
	}
	return m
}

// SetMeta writes the map back as params._meta (removing it when empty).
func (p Params) SetMeta(m map[string]json.RawMessage) {
	if len(m) == 0 {
		delete(p, "_meta")
		return
	}
	b, _ := json.Marshal(m)
	p["_meta"] = b
}

// String returns a string field ("" if absent or not a string).
func (p Params) String(key string) string {
	var s string
	if raw, ok := p[key]; ok {
		_ = json.Unmarshal(raw, &s)
	}
	return s
}

// Set marshals v into key.
func (p Params) Set(key string, v any) {
	b, _ := json.Marshal(v)
	p[key] = b
}

// Raw re-encodes the params.
func (p Params) Raw() json.RawMessage {
	b, _ := json.Marshal(map[string]json.RawMessage(p))
	return b
}

// RequestVersion returns the protocol version declared in params._meta, if any.
func RequestVersion(raw json.RawMessage) string {
	p, err := DecodeParams(raw)
	if err != nil {
		return ""
	}
	var v string
	if b, ok := p.Meta()[MetaProtocolVersion]; ok {
		_ = json.Unmarshal(b, &v)
	}
	return v
}

// StripEraMeta removes sender-describing _meta keys (version, capabilities,
// client info, log level) and returns the remaining params.
func StripEraMeta(p Params) Params {
	m := p.Meta()
	for _, k := range eraMetaKeys {
		delete(m, k)
	}
	p.SetMeta(m)
	return p
}

// Result shape fields added in 2026-07-28.
const (
	ResultTypeComplete      = "complete"
	ResultTypeInputRequired = "input_required"
)

// ResultType returns result.resultType, treating absence as "complete" as
// the spec requires for results from legacy servers.
func ResultType(raw json.RawMessage) string {
	var r struct {
		ResultType string `json:"resultType"`
	}
	_ = json.Unmarshal(raw, &r)
	if r.ResultType == "" {
		return ResultTypeComplete
	}
	return r.ResultType
}

// CacheableMethods return results that must carry ttlMs and cacheScope in
// the modern protocol.
var CacheableMethods = map[string]bool{
	"tools/list": true, "prompts/list": true, "resources/list": true,
	"resources/read": true, "resources/templates/list": true, "server/discover": true,
}
