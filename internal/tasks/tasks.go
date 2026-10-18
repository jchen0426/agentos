// Package tasks registers the tasks agentd can start. The platform knows a task only
// through this registration (D12): its parameters, how to check them, and the workflow
// type that runs it.
package tasks

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/jchen0426/agentos/internal/gitmcp"
	"github.com/jchen0426/agentos/internal/policy"
)

type Task struct {
	Name          string
	WorkflowType  string
	DefaultBudget int64 // micros
	// Resolve checks params and returns the workflow input and the params as resolved. Its
	// errors refuse the params, except an *Unavailable.
	Resolve func(ctx context.Context, params map[string]string) (input any, resolved map[string]string, err error)
	// Scope maps tool arguments to the resolved parameter they must equal: a run acts only
	// on what it was started for, not on everything the policy allows.
	Scope map[string]string
}

type Registry map[string]Task

// Unavailable means Resolve could not check the params (the git tool server failed), not that
// it refused them: the same start may succeed later.
type Unavailable struct{ Err error }

func (e *Unavailable) Error() string { return e.Err.Error() }
func (e *Unavailable) Unwrap() error { return e.Err }

// AuditInput is the input of the security-audit workflow (Python dataclass AuditInput).
type AuditInput struct {
	Repo       string `json:"repo"`        // logical repo id
	Commit     string `json:"commit"`      // full 40-char SHA
	BaseBranch string `json:"base_branch"` // target branch of the draft PR
}

var (
	commitRe = regexp.MustCompile(`^[0-9a-f]{7,40}$`)
	branchRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,99}$`)
)

// SecurityAudit is the first task (design doc, section 3). repos maps logical repo ids to
// read-only mirrors; arbitrary paths are never accepted. git is the git tool server, which
// resolves the commit in its sandbox.
func SecurityAudit(pol *policy.Policy, repos map[string]string, git gitmcp.Caller) Task {
	return Task{
		Name:          "security-audit",
		WorkflowType:  "security-audit",
		DefaultBudget: 5_000_000,
		Scope:         map[string]string{"repo": "repo", "commit": "commit", "base_branch": "base"},
		Resolve: func(ctx context.Context, params map[string]string) (any, map[string]string, error) {
			for k := range params {
				if !slices.Contains([]string{"repo", "commit", "base"}, k) {
					return nil, nil, fmt.Errorf("unknown parameter --%s (want --repo, --commit, --base)", k)
				}
			}
			repo, commit, base := params["repo"], params["commit"], params["base"]
			if !pol.HasRepo(repo) {
				return nil, nil, fmt.Errorf("repo %q is not in the policy", repo)
			}
			mirror, ok := repos[repo]
			if !ok {
				return nil, nil, fmt.Errorf("no mirror configured for repo %q", repo)
			}
			if !commitRe.MatchString(commit) {
				return nil, nil, errors.New("commit must match ^[0-9a-f]{7,40}$")
			}
			if !branchRe.MatchString(base) || strings.Contains(base, "..") {
				return nil, nil, fmt.Errorf("bad base branch %q", base)
			}
			full, err := resolveCommit(ctx, git, mirror, commit)
			if err != nil {
				return nil, nil, err
			}
			return AuditInput{Repo: repo, Commit: full, BaseBranch: base},
				map[string]string{"repo": repo, "commit": full, "base": base}, nil
		},
	}
}

// resolveCommit turns an abbreviated commit into the full SHA, using the mirror only: the git
// tool server's resolve_commit, which reads object headers alone, in its sandbox. Its refusal
// (no such commit, more than one, one over the size limit) refuses the parameter; a server that
// failed is Unavailable.
func resolveCommit(ctx context.Context, git gitmcp.Caller, mirror, commit string) (string, error) {
	full, err := gitmcp.ResolveCommit(ctx, git, mirror, commit)
	switch {
	case gitmcp.IsToolError(err):
		return "", err
	case err != nil:
		return "", &Unavailable{Err: err}
	}
	return full, nil
}
