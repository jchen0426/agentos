// Package policy evaluates policy.yaml (D8): anything not listed is denied, and
// evaluation errors deny too.
package policy

import (
	"bytes"
	"fmt"
	"os"
	"slices"

	"go.yaml.in/yaml/v3"
)

type Decision int

const (
	Deny Decision = iota
	Allow
	RequireApproval
)

func (d Decision) String() string {
	switch d {
	case Allow:
		return "allow"
	case RequireApproval:
		return "require_approval"
	default:
		return "deny"
	}
}

// Effect classes (design doc, section 6). A tool that declares none is unsafe.
const (
	EffectNone       = "none"
	EffectIdempotent = "idempotent"
	EffectLookup     = "lookup"
	EffectUnsafe     = "unsafe"
)

type Tool struct {
	Decision    string   `yaml:"decision"`
	Effect      string   `yaml:"effect"`
	DenyPaths   []string `yaml:"deny_paths"`   // enforced by the tool itself (M3)
	ApprovalTTL string   `yaml:"approval_ttl"` // M4
	Render      string   `yaml:"render"`       // M3
}

type Model struct {
	AllowedProviders []string `yaml:"allowed_providers"`
}

type Policy struct {
	Repos  []string         `yaml:"repos"`
	Tools  map[string]Tool  `yaml:"tools"`
	Models map[string]Model `yaml:"models"`
}

// Load reads a policy file. Unknown keys are an error, so a typo cannot silently allow
// or drop a rule.
func Load(path string) (*Policy, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	var p Policy
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("policy %s: %w", path, err)
	}
	for name, t := range p.Tools {
		switch t.Decision {
		case "allow", "require_approval", "deny":
		default:
			return nil, fmt.Errorf("policy %s: tool %s: unknown decision %q", path, name, t.Decision)
		}
		switch t.Effect {
		case EffectNone, EffectIdempotent, EffectLookup, EffectUnsafe:
		case "":
			t.Effect = EffectUnsafe
			p.Tools[name] = t
		default:
			return nil, fmt.Errorf("policy %s: tool %s: unknown effect class %q", path, name, t.Effect)
		}
	}
	return &p, nil
}

// Verdict is the outcome of one evaluation.
type Verdict struct {
	Decision Decision
	Reason   string
	Effect   string // the tool's effect class; empty for model calls
}

// EvaluateTool decides a tool call. args is the decoded JSON object of the call.
func (p *Policy) EvaluateTool(name string, args map[string]any) Verdict {
	t, ok := p.Tools[name]
	if !ok {
		return Verdict{Decision: Deny, Reason: "tool not in policy"}
	}
	// A policy that lists repos scopes every tool call to them: a call that names no repo
	// is denied, like one that names another.
	if repo, ok := args["repo"]; ok || len(p.Repos) > 0 {
		s, isString := repo.(string)
		if !isString || !slices.Contains(p.Repos, s) {
			return Verdict{Decision: Deny, Reason: "repo missing or not in policy"}
		}
	}
	switch t.Decision {
	case "allow":
		return Verdict{Decision: Allow, Effect: t.Effect}
	case "require_approval":
		return Verdict{Decision: RequireApproval, Effect: t.Effect}
	default:
		return Verdict{Decision: Deny, Reason: "denied by policy"}
	}
}

// EvaluateModel decides whether task data may be sent to provider for the named model task.
func (p *Policy) EvaluateModel(name, provider string) Verdict {
	m, ok := p.Models[name]
	if !ok {
		return Verdict{Decision: Deny, Reason: "model task not in policy"}
	}
	if !slices.Contains(m.AllowedProviders, provider) {
		return Verdict{Decision: Deny, Reason: fmt.Sprintf("provider %q not allowed for %s", provider, name)}
	}
	return Verdict{Decision: Allow}
}

// HasRepo reports whether the logical repo id is allowed.
func (p *Policy) HasRepo(repo string) bool { return slices.Contains(p.Repos, repo) }
