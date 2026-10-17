// Package config loads junction.yaml.
package config

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
)

// Config is the whole gateway configuration.
type Config struct {
	// Listen is the HTTP address for the MCP endpoint and dashboard.
	Listen string `yaml:"listen"`
	// Separator joins server and tool names, e.g. "crm__lookup_customer".
	Separator string `yaml:"separator"`
	// AllowedOrigins for browser requests (DNS-rebinding protection).
	// Empty means localhost origins only.
	AllowedOrigins []string `yaml:"allowed_origins"`
	// Auth protects the MCP endpoint.
	Auth Auth `yaml:"auth"`

	Servers   []Server  `yaml:"servers"`
	Policy    Policy    `yaml:"policy"`
	Redact    Redact    `yaml:"redact"`
	Record    Record    `yaml:"record"`
	Dashboard Dashboard `yaml:"dashboard"`

	// path is the file the config came from; relative paths resolve from its directory.
	path string
}

// Auth configures client authentication on the MCP endpoint.
type Auth struct {
	// BearerTokens, when non-empty, are the accepted "Authorization: Bearer" values.
	BearerTokens []string `yaml:"bearer_tokens"`
}

// Server is one upstream MCP server.
type Server struct {
	Name string `yaml:"name"`
	// stdio transport: command and args. "{self}" expands to the junction binary.
	Command []string          `yaml:"command"`
	Env     map[string]string `yaml:"env"`
	Dir     string            `yaml:"dir"`
	// HTTP transport.
	URL     string            `yaml:"url"`
	Headers map[string]string `yaml:"headers"`
	// Era forces "legacy" or "modern"; default "auto" probes the server.
	Era     string        `yaml:"era"`
	Timeout time.Duration `yaml:"timeout"`
	// Disabled skips the server without deleting its config.
	Disabled bool `yaml:"disabled"`
}

// Transport returns "stdio" or "http".
func (s Server) Transport() string {
	if s.URL != "" {
		return "http"
	}
	return "stdio"
}

// Policy decides which tools clients can see and call.
type Policy struct {
	// Allow lists glob patterns on namespaced tool names. Empty = allow all.
	Allow []string `yaml:"allow"`
	// Deny lists glob patterns; deny always wins over allow.
	Deny []string `yaml:"deny"`
	// DenyDestructive hides tools annotated destructiveHint=true.
	DenyDestructive bool `yaml:"deny_destructive"`
}

// Redact configures PII/secret redaction on responses.
type Redact struct {
	// Mode: "redact" (replace), "detect" (log only) or "off".
	Mode string `yaml:"mode"`
	// Rules picks built-in detectors; empty = all built-ins.
	Rules []string `yaml:"rules"`
	// Custom adds regex detectors.
	Custom []CustomRule `yaml:"custom"`
}

// CustomRule is a user-defined regex detector.
type CustomRule struct {
	Name    string `yaml:"name"`
	Pattern string `yaml:"pattern"`
}

// Record configures traffic recording.
type Record struct {
	// File is a JSONL file every exchange is appended to. Empty = memory only.
	File string `yaml:"file"`
	// MaxBody truncates recorded params/results to this many bytes.
	MaxBody int `yaml:"max_body"`
	// Keep is how many exchanges the dashboard keeps in memory.
	Keep int `yaml:"keep"`
}

// Dashboard toggles the web UI.
type Dashboard struct {
	Enabled *bool `yaml:"enabled"`
}

// On reports whether the dashboard is enabled (default true).
func (d Dashboard) On() bool { return d.Enabled == nil || *d.Enabled }

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnv replaces ${VAR} with the environment value.
func expandEnv(s string) string {
	return envRef.ReplaceAllStringFunc(s, func(m string) string {
		return os.Getenv(envRef.FindStringSubmatch(m)[1])
	})
}

var validName = regexp.MustCompile(`^[A-Za-z0-9-]+$`)

// Load reads and validates a config file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b, path)
}

// Parse decodes config bytes. path is used to resolve relative files.
func Parse(b []byte, path string) (*Config, error) {
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	c.path = path
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = "127.0.0.1:7777"
	}
	if c.Separator == "" {
		c.Separator = "__"
	}
	if c.Redact.Mode == "" {
		c.Redact.Mode = "redact"
	}
	if c.Record.MaxBody == 0 {
		c.Record.MaxBody = 64 * 1024
	}
	if c.Record.Keep == 0 {
		c.Record.Keep = 2000
	}
	for i := range c.Servers {
		s := &c.Servers[i]
		if s.Timeout == 0 {
			s.Timeout = 60 * time.Second
		}
		if s.Era == "" {
			s.Era = "auto"
		}
		s.URL = expandEnv(s.URL)
		for k, v := range s.Headers {
			s.Headers[k] = expandEnv(v)
		}
		for k, v := range s.Env {
			s.Env[k] = expandEnv(v)
		}
		for j, a := range s.Command {
			s.Command[j] = expandEnv(a)
		}
	}
	for i, t := range c.Auth.BearerTokens {
		c.Auth.BearerTokens[i] = expandEnv(t)
	}
}

func (c *Config) validate() error {
	seen := map[string]bool{}
	for _, s := range c.Servers {
		if !validName.MatchString(s.Name) {
			return fmt.Errorf("config: server name %q must use only letters, digits and '-'", s.Name)
		}
		if strings.Contains(s.Name, c.Separator) {
			return fmt.Errorf("config: server name %q contains the separator %q", s.Name, c.Separator)
		}
		if seen[s.Name] {
			return fmt.Errorf("config: duplicate server name %q", s.Name)
		}
		seen[s.Name] = true
		if (len(s.Command) == 0) == (s.URL == "") {
			return fmt.Errorf("config: server %q needs exactly one of command or url", s.Name)
		}
		switch s.Era {
		case "auto", "legacy", "modern":
		default:
			return fmt.Errorf("config: server %q era must be auto, legacy or modern", s.Name)
		}
	}
	switch c.Redact.Mode {
	case "redact", "detect", "off":
	default:
		return fmt.Errorf("config: redact.mode must be redact, detect or off")
	}
	for _, r := range c.Redact.Custom {
		if _, err := regexp.Compile(r.Pattern); err != nil {
			return fmt.Errorf("config: custom redact rule %q: %w", r.Name, err)
		}
	}
	return nil
}

// Resolve makes a path relative to the config file's directory.
func (c *Config) Resolve(p string) string {
	if p == "" || filepath.IsAbs(p) || c.path == "" {
		return p
	}
	return filepath.Join(filepath.Dir(c.path), p)
}

// Example is the starter config written by `junction init`.
//
//go:embed example.yaml
var Example []byte
