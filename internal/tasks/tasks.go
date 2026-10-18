// Package tasks registers the tasks agentd can start. The platform knows a task only
// through this registration (D12): its parameters, how to check them, and the workflow
// type that runs it.
package tasks

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/jchen0426/agentos/internal/gitenv"
	"github.com/jchen0426/agentos/internal/gitmcp"
	"github.com/jchen0426/agentos/internal/policy"
)

type Task struct {
	Name          string
	WorkflowType  string
	DefaultBudget int64 // micros
	// Resolve checks params and returns the workflow input and the params as resolved.
	Resolve func(ctx context.Context, params map[string]string) (input any, resolved map[string]string, err error)
	// Scope maps tool arguments to the resolved parameter they must equal: a run acts only
	// on what it was started for, not on everything the policy allows.
	Scope map[string]string
}

type Registry map[string]Task

// AuditInput is the input of the security-audit workflow (Python dataclass AuditInput).
type AuditInput struct {
	Repo       string `json:"repo"`        // logical repo id
	Commit     string `json:"commit"`      // full 40-char SHA
	BaseBranch string `json:"base_branch"` // target branch of the draft PR
}

var (
	commitRe  = regexp.MustCompile(`^[0-9a-f]{7,40}$`)
	fullSHARe = regexp.MustCompile(`^[0-9a-f]{40}$`)
	branchRe  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,99}$`)
)

// SecurityAudit is the first task (design doc, section 3). repos maps logical repo ids to
// read-only mirrors; arbitrary paths are never accepted.
func SecurityAudit(pol *policy.Policy, repos map[string]string) Task {
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
			full, err := resolveCommit(ctx, mirror, commit)
			if err != nil {
				return nil, nil, err
			}
			return AuditInput{Repo: repo, Commit: full, BaseBranch: base},
				map[string]string{"repo": repo, "commit": full, "base": base}, nil
		},
	}
}

// resolveCommit turns an abbreviated commit into the full SHA, using the mirror only, and from
// object headers alone: git reads a commit whole to peel it, and a crafted one can hold
// gigabytes that pack into megabytes.
func resolveCommit(ctx context.Context, mirror, commit string) (string, error) {
	names, err := mirrorGit(ctx, mirror, "", "rev-parse", "--disambiguate="+commit) // every object it may name
	if err != nil {
		return "", err
	}
	var found [][]string // object, type, size
	if names != "" {
		headers, err := mirrorGit(ctx, mirror, names, "cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)")
		if err != nil {
			return "", err
		}
		for line := range strings.Lines(headers) {
			if f := strings.Fields(line); len(f) == 3 && f[1] == "commit" {
				found = append(found, f)
			}
		}
	}
	switch {
	case len(found) == 0:
		return "", fmt.Errorf("commit %s not found in the mirror", commit)
	case len(found) > 1:
		return "", fmt.Errorf("commit %s names %d commits in the mirror: give more of it", commit, len(found))
	case !fullSHARe.MatchString(found[0][0]):
		return "", fmt.Errorf("commit %s resolved to %q", commit, found[0][0])
	}
	if n, err := strconv.ParseInt(found[0][2], 10, 64); err != nil || n > gitmcp.MaxCommitBytes {
		return "", fmt.Errorf("commit %s is %s bytes, over the limit of %d", commit, found[0][2], gitmcp.MaxCommitBytes)
	}
	return found[0][0], nil
}

// mirrorGit runs a read-only git command in the mirror, in gitenv's environment. Its errors
// name the mirror "<mirror>".
func mirrorGit(ctx context.Context, mirror, stdin string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", mirror}, args...)...)
	cmd.Env = gitenv.ForMirror(mirror)
	cmd.Stdin = strings.NewReader(stdin)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return "", fmt.Errorf("git %s: %s", args[0], strings.ReplaceAll(msg, mirror, "<mirror>"))
		}
		return "", fmt.Errorf("git %s: %w", args[0], err)
	}
	return strings.TrimSpace(string(out)), nil
}
