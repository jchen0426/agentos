// Package policy evaluates policy.yaml (D8): anything not listed is denied, and
// evaluation errors deny too.
package policy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"os"
	"slices"
	"time"

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

// Effect classes (design doc, section 6). A tool declares its own; one that declares none
// is unsafe.
const (
	EffectNone       = "none"
	EffectIdempotent = "idempotent"
	EffectLookup     = "lookup"
	EffectUnsafe     = "unsafe"
)

type Tool struct {
	Decision string `yaml:"decision"`
	// Effect is optional: the tool declares its class. When set it must be that class, or
	// unsafe for a write, which only makes recovery more careful.
	Effect    string   `yaml:"effect"`
	DenyPaths []string `yaml:"deny_paths"` // enforced by the tool itself, not by the workflow
	// ApprovalTTL is how long an approval of the tool's call stays open, and once approved how
	// long the run has to act on it (a Go duration; DefaultApprovalTTL when left out). Only
	// with require_approval.
	ApprovalTTL string `yaml:"approval_ttl"`
	Render      string `yaml:"render"` // "template": the tool renders what it writes (all of agentd's do)
	// MaxPerRun is how many effects of a write one run may have, sent or maybe sent; 0 is
	// the default, 1.
	MaxPerRun int `yaml:"max_per_run"`
}

// DefaultApprovalTTL is how long an approval stays open when the policy does not say: as long
// as the workflow's wait for it (design doc, section 5).
const DefaultApprovalTTL = 24 * time.Hour

// MaxApprovalTTL bounds approval_ttl: a run waiting for an approval wakes every 24 hours to ask
// again, and each time is a call its loop limit counts.
const MaxApprovalTTL = 30 * 24 * time.Hour

type Model struct {
	AllowedProviders []string `yaml:"allowed_providers"`
}

type Policy struct {
	Repos  []string         `yaml:"repos"`
	Tools  map[string]Tool  `yaml:"tools"`
	Models map[string]Model `yaml:"models"`

	SHA256 string `yaml:"-"` // of the file as Load read it: agentd records it at start
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
	sum := sha256.Sum256(b)
	p.SHA256 = hex.EncodeToString(sum[:])
	for name, t := range p.Tools {
		switch t.Decision {
		case "allow", "require_approval", "deny":
		default:
			return nil, fmt.Errorf("policy %s: tool %s: unknown decision %q", path, name, t.Decision)
		}
		switch t.Effect {
		case "", EffectNone, EffectIdempotent, EffectLookup, EffectUnsafe:
		default:
			return nil, fmt.Errorf("policy %s: tool %s: unknown effect class %q", path, name, t.Effect)
		}
		switch t.Render {
		case "", "template":
		default:
			return nil, fmt.Errorf("policy %s: tool %s: unknown render %q", path, name, t.Render)
		}
		if t.MaxPerRun < 0 {
			return nil, fmt.Errorf("policy %s: tool %s: max_per_run must not be negative", path, name)
		}
		if t.ApprovalTTL != "" {
			if t.Decision != "require_approval" {
				return nil, fmt.Errorf("policy %s: tool %s: approval_ttl goes with decision require_approval", path, name)
			}
			if ttl, err := time.ParseDuration(t.ApprovalTTL); err != nil || ttl <= 0 || ttl > MaxApprovalTTL {
				return nil, fmt.Errorf("policy %s: tool %s: approval_ttl %q is not a positive duration of at most %v, such as 24h",
					path, name, t.ApprovalTTL, MaxApprovalTTL)
			}
		}
	}
	return &p, nil
}

// Verdict is the outcome of one evaluation.
type Verdict struct {
	Decision Decision
	Reason   string
	Effect   string // the effect class the policy states for the tool, if any; empty for model calls
	// MaxPerRun is the tool's max_per_run; 0 when the policy leaves the default.
	MaxPerRun int
	// ApprovalTTL is how long an approval stays open, for RequireApproval.
	ApprovalTTL time.Duration
}

// EvaluateTool decides a tool call. args is the decoded JSON object of the call; scope holds
// the values the run is confined to, by argument name (the task's Scope, filled in from the
// run's resolved parameters).
func (p *Policy) EvaluateTool(name string, args map[string]any, scope map[string]string) Verdict {
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
	// The policy says what any run may touch; a run may touch only its own target.
	for _, arg := range slices.Sorted(maps.Keys(scope)) {
		if v, ok := args[arg]; ok {
			if s, isString := v.(string); !isString || s != scope[arg] {
				return Verdict{Decision: Deny, Reason: fmt.Sprintf("%s is not this run's", arg)}
			}
		}
	}
	switch t.Decision {
	case "allow":
		return Verdict{Decision: Allow, Effect: t.Effect, MaxPerRun: t.MaxPerRun}
	case "require_approval":
		ttl := DefaultApprovalTTL
		if t.ApprovalTTL != "" {
			ttl, _ = time.ParseDuration(t.ApprovalTTL) // checked by Load
		}
		return Verdict{Decision: RequireApproval, Effect: t.Effect, MaxPerRun: t.MaxPerRun, ApprovalTTL: ttl}
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
