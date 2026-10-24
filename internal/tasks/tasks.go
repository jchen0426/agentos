// Package tasks registers the tasks agentd can start. The platform knows a task only
// through this registration (D12): its parameters, how to check them, the workflow type that
// runs it, and the names of its steps.
package tasks

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"

	"github.com/jchen0426/agentos/internal/gitmcp"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tools"
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
	// ApprovalContext is what the task shows the reviewer of a call of one of its runs' writes,
	// beyond what the write itself describes, from what agentd recorded of the run and the call's
	// arguments (design doc, section 10). Best-effort: what it cannot read, it says.
	ApprovalContext func(ctx context.Context, run RunOutputs, args map[string]any) map[string]any
	// Steps names the task's steps by the gateway call (kind and name) that makes each, as agentctl
	// runs explain and the run's report show them.
	Steps map[Call]string
	// Report is the call that reports a run's end, if the task has one: a run that ends without it
	// ok has not reported (runs explain).
	Report Call
	// Required are the calls a run must have an accepted result of to have ended well (runs explain).
	Required []Requirement
}

// Call is a gateway call by its kind and name: a step of a task is the call that makes it.
type Call struct{ Kind, Name string }

// Step is the task's name for the step c makes, if it names one.
func (t Task) Step(c Call) (string, bool) {
	s, ok := t.Steps[c]
	return s, ok
}

// Requirement is a call a run must have an accepted result of to have ended well: always, or When
// another call's accepted result holds a field.
type Requirement struct {
	Call Call
	When *Holds
}

// Holds is a condition on a call's accepted result: that its value holds Field, not empty.
type Holds struct {
	Call  Call
	Field string
}

// RunOutputs reads what agentd recorded of one run.
type RunOutputs interface {
	// ModelOutputs is what the workflow got from each of the run's accepted answers of a model
	// task, newest first.
	ModelOutputs(ctx context.Context, task string) ([]map[string]any, error)
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

var commitRe = regexp.MustCompile(`^[0-9a-f]{7,40}$`)

// AuditModel is the security audit's model task: its accepted answer is the audit's report.
const AuditModel = "audit_v1"

// The security audit's calls (design doc, section 3).
var (
	showDiff   = Call{"tool", "git.show_diff"}
	analyze    = Call{"model", AuditModel}
	applyCheck = Call{"tool", "git.apply_check"}
	openPR     = Call{"tool", "github.open_draft_pr"}
)

// SecurityAudit is the first task (design doc, section 3). repos maps logical repo ids to
// read-only mirrors; arbitrary paths are never accepted. git is the git tool server, which
// resolves the commit in its sandbox.
func SecurityAudit(pol *policy.Policy, repos map[string]string, git gitmcp.Caller) Task {
	return Task{
		Name:            "security-audit",
		WorkflowType:    "security-audit",
		DefaultBudget:   5_000_000,
		Scope:           map[string]string{"repo": "repo", "commit": "commit", "base_branch": "base"},
		ApprovalContext: auditFindings,
		// The pipeline of section 3 of the design doc, step by step: the diff and the audit always; the
		// patch's check and its pull request when the audit proposed a patch.
		Steps: map[Call]string{
			showDiff: "fetch_diff", analyze: "analyze", applyCheck: "validate_patch", openPR: "open_pr",
			{"tool", "github.comment_commit"}: "post_report",
		},
		Report: Call{"tool", "github.comment_commit"},
		Required: []Requirement{{Call: showDiff}, {Call: analyze},
			{Call: applyCheck, When: &Holds{Call: analyze, Field: "patch_ref"}},
			{Call: openPR, When: &Holds{Call: analyze, Field: "patch_ref"}}},
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
			if !tools.ValidBranch(base) {
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

// auditFindings is the security audit's context for a reviewer: the summary and the findings
// (severity, CWE, where, what), as the model reported them, of the audit that proposed the patch
// under review. A patch no audit of the run proposed (a reviewer's edit) gets the latest audit's,
// and says so: a workflow can ask for several audits, and show one's patch with another's findings.
func auditFindings(ctx context.Context, run RunOutputs, args map[string]any) map[string]any {
	answers, err := run.ModelOutputs(ctx, AuditModel)
	switch {
	case err != nil:
		return map[string]any{"findings_error": "agentd could not read the run's audits: " + err.Error()}
	case len(answers) == 0:
		return map[string]any{"findings_error": "the run has no accepted audit"}
	}
	v, proposed := answers[0], false
	for _, a := range answers {
		if ref, ok := a["patch_ref"].(string); ok && ref != "" && ref == args["patch_ref"] {
			v, proposed = a, true
			break
		}
	}
	var findings []any
	list, _ := v["findings"].([]any)
	for _, f := range list {
		m, ok := f.(map[string]any)
		if !ok {
			continue
		}
		findings = append(findings, map[string]any{
			"severity": m["severity"], "cwe": m["cwe"], "file": m["file"], "line": m["line"], "title": m["title"],
		})
	}
	return map[string]any{"summary": v["summary"], "findings": findings, "audits": len(answers),
		"patch_from_this_audit": proposed}
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
