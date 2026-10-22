package tools

import (
	"context"
	"errors"
	"fmt"

	"github.com/jchen0426/agentos/internal/gitmcp"
	"github.com/jchen0426/agentos/internal/patch"
	"github.com/jchen0426/agentos/internal/policy"
)

// ShowDiff is git.show_diff: the audited commit's diff, read by the git MCP server in its
// sandbox and stored as a blob of the run.
type ShowDiff struct {
	Blobs   BlobStore
	Mirrors map[string]string // logical repository ID -> read-only mirror
	Git     gitmcp.Caller
}

var showDiffArgs = MustSchema(`{
  "type": "object", "additionalProperties": false, "required": ["repo", "commit"],
  "properties": {
    "repo": {"type": "string", "minLength": 1},
    "commit": {"type": "string", "pattern": "` + CommitPattern + `"}
  }
}`)

func (*ShowDiff) Name() string  { return "git.show_diff" }
func (*ShowDiff) Class() string { return policy.EffectNone }
func (*ShowDiff) Args() *Schema { return showDiffArgs }

func (t *ShowDiff) Read(ctx context.Context, req Request) (map[string]any, error) {
	mirror, ok := t.Mirrors[String(req.Args, "repo")]
	if !ok {
		return nil, &DeniedError{Reason: "no mirror for the repository"}
	}
	var out gitmcp.ShowDiffOut
	if err := t.Git.Call(ctx, mirror, "show_diff", gitmcp.ShowDiffIn{Commit: String(req.Args, "commit")}, &out); err != nil {
		if gitmcp.IsToolError(err) {
			return nil, &RefusedError{Reason: err.Error()}
		}
		return nil, err
	}
	diff, err := out.Diff()
	if err != nil {
		return nil, fmt.Errorf("git.show_diff: %w", err)
	}
	ref, err := t.Blobs.PutRunBlob(ctx, req.WorkflowID, diff) // the exact bytes, UTF-8 or not
	if err != nil {
		return nil, err
	}
	return map[string]any{"blob": ref}, nil
}

// ApplyCheck is git.apply_check: does the patch apply to the audited commit, and may it? Two
// checks must both pass: agentd's own (the rules, DenyPaths, and applying each hunk exactly where
// its lines are, which is what github.open_draft_pr commits), then git's own git apply --check, in
// the git MCP server's sandbox, of the patch as agentd applied it: its hunk headers say what the
// hunks' lines do (patch.Patch.Text). agentd gives it github.open_draft_pr's protected paths too,
// so a patch it passes is one that tool takes; github.open_draft_pr still runs agentd's checks
// again itself: a workflow can skip this tool, never those checks.
type ApplyCheck struct {
	Blobs     BlobStore
	Mirrors   map[string]string // logical repository ID -> read-only mirror
	DenyPaths []string
	Git       gitmcp.Caller
}

var applyCheckArgs = MustSchema(`{
  "type": "object", "additionalProperties": false, "required": ["repo", "commit", "patch_ref"],
  "properties": {
    "repo": {"type": "string", "minLength": 1},
    "commit": {"type": "string", "pattern": "` + CommitPattern + `"},
    "patch_ref": {"type": "string", "pattern": "` + BlobPattern + `"}
  }
}`)

func (*ApplyCheck) Name() string  { return "git.apply_check" }
func (*ApplyCheck) Class() string { return policy.EffectNone }
func (*ApplyCheck) Args() *Schema { return applyCheckArgs }

func (t *ApplyCheck) Read(ctx context.Context, req Request) (map[string]any, error) {
	mirror, ok := t.Mirrors[String(req.Args, "repo")]
	if !ok {
		return nil, &DeniedError{Reason: "no mirror for the repository"}
	}
	commit, ref := String(req.Args, "commit"), String(req.Args, "patch_ref")
	changes, applied, err := CheckedPatch(ctx, t.Git, t.Blobs, req.WorkflowID, ref, t.DenyPaths, mirror, commit)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(changes))
	for _, c := range changes {
		paths = append(paths, c.Path)
	}
	if err := GitApplyCheck(ctx, t.Git, mirror, commit, applied.Text(), changes); err != nil {
		return nil, err
	}
	files := make([]any, 0, len(paths))
	for _, p := range paths {
		files = append(files, p)
	}
	out := map[string]any{"applies": true, "files": files}
	if n := len(applied.Rewritten()); n > 0 {
		out["rewritten_hunks"] = n // headers the hunks' lines contradict: the patch is not quite as written
	}
	return out, nil
}

// GitApplyCheck is git's own check of a patch, run by the git MCP server in its sandbox on the
// commit in the mirror, with a temporary index: git apply takes the patch, and makes of the files
// what changes are, agentd's application of it. A patch git refuses, or makes another change of,
// is a *DeniedError; the server failing is any other error.
func GitApplyCheck(ctx context.Context, git gitmcp.Caller, mirror, commit string, text []byte, changes []patch.Change) error {
	paths := make([]string, 0, len(changes))
	for _, c := range changes {
		paths = append(paths, c.Path)
	}
	var out gitmcp.ApplyCheckOut
	if err := git.Call(ctx, mirror, "apply_check", gitmcp.ApplyCheckIn{Commit: commit, Patch: string(text), Paths: paths}, &out); err != nil {
		if gitmcp.IsToolError(err) {
			return &DeniedError{Reason: err.Error()}
		}
		return err
	}
	switch {
	case !out.Applies:
		return &DeniedError{Reason: gitmcp.Reason(mirror, out.Reason)}
	case out.Index == nil:
		return errors.New("the git tool server did not say what git apply made of the patch")
	}
	made := map[string]gitmcp.IndexEntry{}
	for _, e := range *out.Index {
		if _, twice := made[e.Path]; twice {
			return &DeniedError{Reason: fmt.Sprintf("git apply makes %.200q twice of the patch", e.Path)}
		}
		made[e.Path] = e
	}
	for _, c := range changes {
		e, ok := made[c.Path]
		delete(made, c.Path)
		if ok == c.Delete || ok && (e.Mode != "100644" || e.Stage != 0 || e.Object != patch.BlobID(c.Content)) {
			return &DeniedError{Reason: fmt.Sprintf("%s: git apply makes another file of the patch than agentd does: the patch reads two ways", c.Path)}
		}
	}
	for p := range made {
		return &DeniedError{Reason: fmt.Sprintf("%.200q: git apply makes a file of the patch that agentd does not: the patch reads two ways", p)}
	}
	return nil
}

// CheckedPatch reads a patch the run produced, checks it against the rules and the policy's
// deny_paths, and applies it to the commit in the mirror, whose files the git MCP server reads
// in its sandbox. It returns the changes, and the patch as applied: the patch whose hunk headers
// say what its hunks' lines do (patch.Patch.Text), which the changes are. A patch that fails is
// a *DeniedError.
func CheckedPatch(ctx context.Context, git gitmcp.Caller, blobs BlobStore, workflowID, ref string, denyPaths []string,
	mirror, commit string) ([]patch.Change, *patch.Patch, error) {
	b, err := blobs.GetRunBlob(ctx, workflowID, ref)
	if err != nil {
		return nil, nil, &DeniedError{Reason: "patch_ref: " + err.Error()}
	}
	p, err := patch.Parse(b)
	if err == nil {
		err = p.Check(denyPaths)
	}
	var changes []patch.Change
	if err == nil {
		changes, err = p.Apply(patch.MirrorSource(ctx, git, mirror, commit))
	}
	if v := (*patch.Violation)(nil); errors.As(err, &v) {
		return nil, nil, &DeniedError{Reason: v.Reason}
	}
	if err != nil {
		return nil, nil, err
	}
	return changes, p, nil
}
