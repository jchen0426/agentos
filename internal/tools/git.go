package tools

import (
	"context"
	"errors"

	"github.com/jchen0426/agentos/internal/patch"
	"github.com/jchen0426/agentos/internal/policy"
)

// stubDiff is what git.show_diff returns until M3b reads the commit from the mirror in a
// sandboxed MCP server. The test mirrors hold exactly this change.
const stubDiff = `diff --git a/app/db.py b/app/db.py
--- a/app/db.py
+++ b/app/db.py
@@ -1,2 +1,2 @@
 def find_user(conn, name):
-    return conn.execute("SELECT * FROM users WHERE name = ?", (name,))
+    return conn.execute("SELECT * FROM users WHERE name = '%s'" % name)
`

// ShowDiff is git.show_diff: the audited commit's diff, stored as a blob of the run.
type ShowDiff struct{ Blobs BlobStore }

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
	ref, err := t.Blobs.PutRunBlob(ctx, req.WorkflowID, []byte(stubDiff))
	if err != nil {
		return nil, err
	}
	return map[string]any{"blob": ref}, nil
}

// ApplyCheck is git.apply_check: does the patch apply to the audited commit, and may it? It
// runs the same checks as github.open_draft_pr, which runs them again itself: a workflow can
// skip this tool, never those checks.
type ApplyCheck struct {
	Blobs     BlobStore
	Mirrors   map[string]string // logical repository ID -> read-only mirror
	DenyPaths []string
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
	changes, err := CheckedPatch(ctx, t.Blobs, req.WorkflowID, String(req.Args, "patch_ref"), t.DenyPaths, mirror, String(req.Args, "commit"))
	if err != nil {
		return nil, err
	}
	paths := make([]any, 0, len(changes))
	for _, c := range changes {
		paths = append(paths, c.Path)
	}
	return map[string]any{"applies": true, "files": paths}, nil
}

// CheckedPatch reads a patch the run produced, checks it against the rules and the policy's
// deny_paths, and applies it to the commit in the mirror. A patch that fails is a
// *DeniedError.
func CheckedPatch(ctx context.Context, blobs BlobStore, workflowID, ref string, denyPaths []string,
	mirror, commit string) ([]patch.Change, error) {
	b, err := blobs.GetRunBlob(ctx, workflowID, ref)
	if err != nil {
		return nil, &DeniedError{Reason: "patch_ref: " + err.Error()}
	}
	p, err := patch.Parse(b)
	if err == nil {
		err = p.Check(denyPaths)
	}
	var changes []patch.Change
	if err == nil {
		changes, err = p.Apply(patch.MirrorSource(ctx, mirror, commit))
	}
	if v := (*patch.Violation)(nil); errors.As(err, &v) {
		return nil, &DeniedError{Reason: v.Reason}
	}
	return changes, err
}
