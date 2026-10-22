package githubtool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/go-github/v92/github"

	"github.com/jchen0426/agentos/internal/gitmcp"
	"github.com/jchen0426/agentos/internal/patch"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tools"
)

// OpenDraftPR is github.open_draft_pr. Two steps (section 6): a branch with the patch on the
// tip of the base branch, created under a name derived from the audited commit, the run and the
// patch, which a retry finds, on that tip or on the one an earlier attempt read (idempotent);
// then a draft pull request, which is looked up by its head branch before it is opened
// (lookup). Before either, it checks on GitHub that the audited commit is in the base branch's
// history (inBase) and that the files the patch touches are what the patch was made for: at
// that commit (checkFiles), and at the tip the branch goes on (atTip).
type OpenDraftPR struct {
	Client    *Client
	Repos     map[string]Repo
	Blobs     tools.BlobStore
	DenyPaths []string      // the policy's: the tool checks the patch itself
	Git       gitmcp.Caller // reads the files the patch touches, in the git MCP server's sandbox
}

// ProtectedPaths are refused whatever the policy says: GitHub itself acts on them. Workflows run
// with the repository's secrets, CODEOWNERS (in any of its three places) decides who must
// review, dependabot opens pull requests, and a dev container's commands run in the Codespace
// of whoever opens the branch there.
var ProtectedPaths = []string{".github/**", "CODEOWNERS", "docs/CODEOWNERS", ".devcontainer/**", ".devcontainer.json"}

var prArgs = tools.MustSchema(`{
  "type": "object", "additionalProperties": false, "required": ["repo", "commit", "base_branch", "patch_ref"],
  "properties": {
    "repo": {"type": "string", "minLength": 1},
    "commit": {"type": "string", "pattern": "` + tools.CommitPattern + `"},
    "base_branch": {"type": "string", "minLength": 1, "maxLength": 100},
    "patch_ref": {"type": "string", "pattern": "` + tools.BlobPattern + `"}
  }
}`)

var prResult = tools.MustSchema(`{
  "type": "object", "required": ["url", "number"],
  "properties": {
    "url": {"type": "string", "minLength": 1},
    "number": {"type": "integer", "minimum": 1},
    "branch": {"type": "string"}
  }
}`)

func (*OpenDraftPR) Name() string            { return "github.open_draft_pr" }
func (*OpenDraftPR) Class() string           { return policy.EffectLookup }
func (*OpenDraftPR) Args() *tools.Schema     { return prArgs }
func (*OpenDraftPR) Result() *tools.Schema   { return prResult }
func (t *OpenDraftPR) Live() bool            { return t.Client.live() }
func (t *OpenDraftPR) Settle() time.Duration { return t.Client.settle() }

// prPayload is everything GitHub receives for one draft pull request, but for the tip of the
// base branch, which the pull request's commit is made on: Execute reads it.
type prPayload struct {
	Owner string `json:"owner"`
	Repo  string `json:"repo"`
	Base  string `json:"base"`
	// Commit is the audited commit. Before the pull request's commit was made on the tip, it
	// was made on this one: hence the name it is recorded under.
	Commit  string    `json:"parent"`
	Branch  string    `json:"branch"`
	Message string    `json:"message"`
	Author  signature `json:"author"`
	Files   []prFile  `json:"files"`
	Title   string    `json:"title"`
	Body    string    `json:"body"`
	Draft   bool      `json:"draft"`
	Marker  string    `json:"marker"`
}

type signature struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Date  string `json:"date"` // the run's creation time, so every attempt builds the same commit
}

type prFile struct {
	Path    string  `json:"path"`
	Content *string `json:"content"` // nil: the patch deletes the file
	// New says the patch creates the file: the git tool server found nothing at Path in the
	// mirror. Otherwise OldBlob is the file the patch was applied to, as the server read it.
	// Execute checks either against GitHub. A payload recorded before agentd wrote them down
	// has neither, and is refused.
	New     bool   `json:"new,omitempty"`
	OldBlob string `json:"old_blob,omitempty"`
}

func (t *OpenDraftPR) Prepare(ctx context.Context, req tools.Request) (json.RawMessage, error) {
	repo, ok := t.Repos[tools.String(req.Args, "repo")]
	if !ok {
		return nil, &tools.DeniedError{Reason: "no GitHub repository is configured for " + tools.String(req.Args, "repo")}
	}
	commit, ref := tools.String(req.Args, "commit"), tools.String(req.Args, "patch_ref")
	if base := tools.String(req.Args, "base_branch"); !tools.ValidBranch(base) {
		return nil, &tools.DeniedError{Reason: fmt.Sprintf("%.100q is not a branch name agentd takes", base)}
	}
	changes, _, err := tools.CheckedPatch(ctx, t.Git, t.Blobs, req.WorkflowID, ref, t.denyPaths(), repo.Mirror, commit)
	if err != nil {
		return nil, err
	}
	files := make([]prFile, 0, len(changes))
	for _, c := range changes {
		f := prFile{Path: c.Path, OldBlob: c.OldBlob, New: c.OldBlob == ""} // a file the commit has always names its blob
		if !c.Delete {
			if !utf8.Valid(c.Content) {
				return nil, &tools.DeniedError{Reason: c.Path + ": not UTF-8 text after the patch"}
			}
			s := string(c.Content)
			f.Content = &s
		}
		files = append(files, f)
	}
	// The branch name carries the commit, the run and the patch: the same patch of the same
	// run always lands on the same branch, and a reviewer's edited patch on a new one.
	patchHash := strings.TrimPrefix(ref, "sha256:")
	branch := fmt.Sprintf("agent/audit-%s-%s-%s", commit[:8], req.WorkflowID[:8], patchHash[:8])
	title := "agentos: security fix for " + commit[:12]
	author := t.author()
	return json.Marshal(prPayload{
		Owner: repo.Owner, Repo: repo.Name, Base: tools.String(req.Args, "base_branch"), Commit: commit,
		Branch: branch, Message: title + "\n\nProposed by the agentos security audit of " + commit + ".",
		Author: signature{Name: author.Name, Email: author.Email, Date: req.RunCreatedAt.UTC().Format(time.RFC3339)},
		Files:  files, Title: title, Draft: true, Marker: marker(req.EffectID),
		Body: "A fix proposed by the agentos security audit of " + commit + ". The audit report is a comment on that commit.\n\n" +
			marker(req.EffectID) + "\n",
	})
}

// denyPaths are the paths a patch may not touch: GitHub's own, and the policy's.
func (t *OpenDraftPR) denyPaths() []string {
	return append(slices.Clone(ProtectedPaths), t.DenyPaths...)
}

func (t *OpenDraftPR) author() Author {
	if t.Client != nil && t.Client.cfg.Author.Name != "" {
		return t.Client.cfg.Author
	}
	return Author{Name: "agentos", Email: "agentos@agentos.invalid"}
}

// DryRun is the pull request the payload would open. A dry run sends nothing to GitHub and
// reads nothing from it either, so nothing checks the payload's commit or files there (Execute
// does): a commit outside the base branch dry-runs a pull request too, and the files are as the
// git tool server read them.
func (t *OpenDraftPR) DryRun(payload json.RawMessage) map[string]any {
	var p prPayload
	_ = json.Unmarshal(payload, &p)
	return map[string]any{"url": "dry-run://github/" + p.Owner + "/" + p.Repo + "/" + p.Branch, "number": 0,
		"branch": p.Branch, "dry_run": true}
}

// EditArg is what a reviewer's edit replaces: the patch (tools.Editable).
func (*OpenDraftPR) EditArg() string { return "patch_ref" }

// Describe is what the reviewer of a pull request sees besides its payload (design doc, section
// 10, tools.Describer): the patch as Prepare applied it, with the hunk headers agentd rewrote, the
// files it changes, git's own check of it, and where the base branch stands on GitHub now, with
// the checks Execute makes on GitHub before it writes, made now (they only read). Each part says
// what agentd could not find out. Things change before the write is sent; Execute checks again.
func (t *OpenDraftPR) Describe(ctx context.Context, req tools.Request, raw json.RawMessage) map[string]any {
	var p prPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return map[string]any{"error": "the payload: " + err.Error()}
	}
	files := make([]map[string]any, 0, len(p.Files))
	paths := make([]string, 0, len(p.Files))
	for _, f := range p.Files {
		change := "modify"
		switch {
		case f.New:
			change = "create"
		case f.Content == nil:
			change = "delete"
		}
		files = append(files, map[string]any{"path": f.Path, "change": change})
		paths = append(paths, f.Path)
	}
	out := map[string]any{
		"repository": p.Owner + "/" + p.Repo, "base_branch": p.Base, "branch": p.Branch, "commit": p.Commit,
		"title": p.Title, "files": files,
		"notes": []string{
			"The pull request's commit is made on the tip of " + p.Base + " when the write is sent, not on the audited commit. " +
				"If the files the patch touches have changed on the tip by then, agentd pushes no branch and opens no pull request.",
			"Pushing branch " + p.Branch + " runs the repository's workflows that start on a push or on a new branch.",
		},
	}
	// The patch is applied again, and shown as applied only if that makes the payload's files: the
	// same patch on the same commit always does, unless agentd changed in between.
	repo, ref := tools.String(req.Args, "repo"), tools.String(req.Args, "patch_ref")
	changes, applied, err := tools.CheckedPatch(ctx, t.Git, t.Blobs, req.WorkflowID, ref, t.denyPaths(), t.Repos[repo].Mirror, p.Commit)
	if err == nil && !sameFiles(changes, p.Files) {
		err = errors.New("applied again, it makes other files than the payload holds")
	}
	if err != nil {
		// As agentd reads it, its hunks not placed in the files: no text of the patch that is in no
		// file's header or hunk, but nothing that says this is the change the payload makes.
		if b, berr := t.Blobs.GetRunBlob(ctx, req.WorkflowID, ref); berr == nil {
			if read, perr := patch.Parse(b); perr == nil {
				out["patch_as_read"] = string(read.Text())
			}
		}
		out["patch_error"] = "agentd could not show the patch as it applied it (" + err.Error() + "): the patch as read is not " +
			"what the payload's files are, where its hunk headers are wrong. The payload is."
	} else {
		out["patch"] = string(applied.Text())
		if rw := applied.Rewritten(); len(rw) > 0 {
			out["patch_rewritten"] = rewrites(rw)
			out["notes"] = append(out["notes"].([]string), fmt.Sprintf("agentd rewrote %d of the patch's hunk headers, "+
				"whose counts or places of lines the hunks' own lines contradict (patch_rewritten): the patch shown is the one applied.", len(rw)))
		}
		out["apply_check"] = t.applyCheck(ctx, repo, p.Commit, applied.Text(), changes)
	}
	out["github"] = t.onGitHub(ctx, p)
	return out
}

// sameFiles reports whether changes are the files a payload holds: the same paths, in the same
// order, each the same content made of the same blob.
func sameFiles(changes []patch.Change, files []prFile) bool {
	return slices.EqualFunc(changes, files, func(c patch.Change, f prFile) bool {
		return c.Path == f.Path && c.OldBlob == f.OldBlob && c.Delete == (f.Content == nil) &&
			(f.Content == nil || string(c.Content) == *f.Content)
	})
}

// maxRewrites is how many rewritten hunk headers a reviewer is shown one by one.
const maxRewrites = 20

// rewrites lists the hunk headers agentd rewrote, as the patch had them and as applied.
func rewrites(rw []patch.Rewrite) []any {
	out := make([]any, 0, min(len(rw), maxRewrites)+1)
	for _, r := range rw[:min(len(rw), maxRewrites)] {
		out = append(out, map[string]any{"file": r.Path, "hunk": r.Hunk, "was": r.Was, "now": r.Now})
	}
	if len(rw) > maxRewrites {
		out = append(out, fmt.Sprintf("and %d more", len(rw)-maxRewrites))
	}
	return out
}

// applyCheck is git's own check of the patch as applied, for the reviewer: git apply takes it, and
// makes the same files of it.
func (t *OpenDraftPR) applyCheck(ctx context.Context, repo, commit string, text []byte, changes []patch.Change) map[string]any {
	r, ok := t.Repos[repo]
	if !ok || t.Git == nil {
		return map[string]any{"error": "no git tool server for " + repo}
	}
	err := tools.GitApplyCheck(ctx, t.Git, r.Mirror, commit, text, changes)
	var denied *tools.DeniedError
	switch {
	case err == nil:
		return map[string]any{"applies": true}
	case errors.As(err, &denied):
		return map[string]any{"applies": false, "reason": denied.Reason}
	}
	return map[string]any{"error": err.Error()}
}

// onGitHub is where the base branch stands on GitHub now: its tip, how the audited commit
// compares with it, and the checks of the patch's files Execute makes before it writes.
func (t *OpenDraftPR) onGitHub(ctx context.Context, p prPayload) map[string]any {
	if !t.Live() {
		return map[string]any{"error": "GitHub is not configured"}
	}
	gh := t.Client.gh
	ref, resp, err := gh.Git.GetRef(ctx, p.Owner, p.Repo, "heads/"+p.Base)
	tip := ref.GetObject().GetSHA()
	switch {
	case status(resp) == http.StatusNotFound:
		return map[string]any{"error": fmt.Sprintf("GitHub has no branch %s in %s/%s (or the token cannot see the repository)", p.Base, p.Owner, p.Repo)}
	case err != nil:
		return map[string]any{"error": unseen(err).Error()}
	case ref.GetRef() != "refs/heads/"+p.Base || ref.GetObject().GetType() != "commit" || !fullSHA.MatchString(tip):
		return map[string]any{"error": fmt.Sprintf("GitHub answered for branch %s with %.100q, a %.20q %.100q",
			p.Base, ref.GetRef(), ref.GetObject().GetType(), tip)}
	}
	out := map[string]any{"base_tip": tip}
	where := fmt.Sprintf("branch %s of %s/%s at %s", p.Base, p.Owner, p.Repo, tip)
	switch cmp, why, _, err := t.compare(ctx, p, tip, p.Commit, where); {
	case err != nil:
		out["audited_commit_error"] = err.Error()
	case why != "":
		out["audited_commit_error"] = why
	default:
		out["audited_commit"] = map[string]any{"status": cmp.Status, "behind_by": cmp.BehindBy, "ahead_by": cmp.AheadBy,
			"in_base_branch": cmp.holds(p.Commit)}
	}
	verdict := func(err error) string {
		var denied *tools.DeniedError
		switch {
		case err == nil:
			return "ok"
		case errors.As(err, &denied):
			return "refused: " + denied.Reason
		}
		return "unknown: " + err.Error()
	}
	audited, _, err := gh.Git.GetCommit(ctx, p.Owner, p.Repo, p.Commit)
	if err != nil {
		out["files_at_commit"] = verdict(unseen(err))
		return out
	}
	w := newTreeWalk(gh, p)
	out["files_at_commit"] = verdict(t.checkFiles(ctx, w, p, audited.GetTree().GetSHA()))
	tipTree := audited.GetTree().GetSHA()
	if tip != p.Commit {
		if tipTree, err = t.commitTree(ctx, p, tip); err != nil {
			out["files_at_tip"] = verdict(err)
			return out
		}
	}
	out["files_at_tip"] = verdict(t.atTip(ctx, w, p, tip, tipTree))
	return out
}

func (t *OpenDraftPR) Execute(ctx context.Context, raw json.RawMessage, _ string, proceed tools.Proceed) (map[string]any, error) {
	if !t.Live() {
		return nil, fmt.Errorf("%w: GitHub is not configured", tools.ErrNotSent)
	}
	var p prPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("%w: %v", tools.ErrNotSent, err)
	}
	if !tools.ValidBranch(p.Base) { // a payload recorded before agentd checked the name
		return nil, t.refused(ctx, p, false, &tools.DeniedError{Reason: fmt.Sprintf("%.100q is not a branch name agentd takes", p.Base)})
	}
	gh := t.Client.gh
	audited, resp, err := gh.Git.GetCommit(ctx, p.Owner, p.Repo, p.Commit)
	switch {
	case status(resp) == http.StatusNotFound:
		// The git tool server resolved the run's commit, and agentd could check only that it
		// begins with the digits it was given.
		return nil, t.refused(ctx, p, false, &tools.DeniedError{Reason: fmt.Sprintf(
			"GitHub has no commit %s in %s/%s (or the token cannot see the repository)", p.Commit, p.Owner, p.Repo)})
	case err != nil:
		return nil, t.refused(ctx, p, false, unseen(err))
	case audited.GetSHA() != p.Commit || !fullSHA.MatchString(audited.GetTree().GetSHA()):
		return nil, fmt.Errorf("%w: GitHub answered for commit %s with %.100q, of tree %.100q", tools.ErrNotSent,
			p.Commit, audited.GetSHA(), audited.GetTree().GetSHA())
	}
	// Before anything is written: the pull request holds the patch alone, on the tip of the
	// base branch.
	tip, err := t.inBase(ctx, p)
	if err != nil {
		return nil, err
	}
	tipTree := audited.GetTree().GetSHA()
	if tip != p.Commit {
		if tipTree, err = t.commitTree(ctx, p, tip); err != nil {
			return nil, t.refused(ctx, p, false, err)
		}
	}
	// One walk lists the trees this call checks: each directory once, within one limit.
	w := newTreeWalk(t.Client.gh, p)
	if err := t.checkFiles(ctx, w, p, audited.GetTree().GetSHA()); err != nil {
		return nil, t.refused(ctx, p, true, err)
	}
	// From here on agentd writes. GitHub declining a request leaves the branch on GitHub when
	// this attempt or an earlier one made it.
	v, err := t.write(ctx, w, p, tip, tipTree, proceed)
	if err != nil {
		return nil, t.refused(ctx, p, true, err)
	}
	return v, nil
}

// commitTree is the tree of a commit of the base branch's history on GitHub.
func (t *OpenDraftPR) commitTree(ctx context.Context, p prPayload, sha string) (string, error) {
	c, _, err := t.Client.gh.Git.GetCommit(ctx, p.Owner, p.Repo, sha)
	switch {
	case err != nil:
		return "", unseen(err)
	case c.GetSHA() != sha || !fullSHA.MatchString(c.GetTree().GetSHA()):
		// An empty tree in particular: go-github leaves it out of a CreateTree, which would make
		// a tree of the patch's files alone.
		return "", fmt.Errorf("%w: GitHub answered for commit %s with %.100q, of tree %.100q", tools.ErrNotSent,
			sha, c.GetSHA(), c.GetTree().GetSHA())
	}
	return c.GetTree().GetSHA(), nil
}

// write makes the branch, with the patch on the tip (or finds an earlier attempt's), and the
// pull request.
func (t *OpenDraftPR) write(ctx context.Context, w *treeWalk, p prPayload, tip, tipTree string, proceed tools.Proceed) (map[string]any, error) {
	gh := t.Client.gh
	parent, err := t.branch(ctx, w, p, tip, tipTree, proceed)
	if err != nil {
		return nil, err
	}
	// GitHub has the branch now, pushed by this attempt or an earlier one: part of the pull
	// request went out, and the push ran the repository's workflows. Neither GitHub declining a
	// request from here on nor a cancel means nothing happened: a person settles either. GitHub
	// failing is tried again, as before: the next attempt finds the branch.
	if v, found, err := t.find(ctx, p); err != nil {
		return nil, onBranch(p, unseen(err))
	} else if found {
		return v, nil
	}
	// The pull request pushes nothing, but GitHub merges the branch into the base for it, and
	// pull_request workflows run on that merge: the base must still hold the branch's parent.
	if why, err := t.stillHolds(ctx, p, parent); err != nil {
		return nil, onBranch(p, err)
	} else if why != "" {
		return nil, &tools.ConflictError{Reason: fmt.Sprintf(
			"branch %s exists, on commit %s, which branch %s no longer holds (%s); agentd does not open the pull request on it",
			p.Branch, parent, p.Base, why)}
	}
	if err := proceed(ctx); errors.Is(err, tools.ErrStopped) {
		return nil, &tools.ConflictError{Reason: "branch " + p.Branch +
			" exists, but the pull request was not opened: the run was cancelled, its approval expired, or the effect changed"}
	} else if err != nil {
		return nil, err
	}
	pr, resp, err := gh.PullRequests.Create(ctx, p.Owner, p.Repo, github.CreatePullRequest{
		Title: new(p.Title), Head: p.Branch, Base: p.Base, Body: new(p.Body), Draft: new(p.Draft),
		MaintainerCanModify: new(false),
	})
	if err != nil {
		if status(resp) == http.StatusUnprocessableEntity && strings.Contains(err.Error(), "already exists") {
			// GitHub allows one open pull request per head branch: an earlier request of ours
			// got there first. If the lookup cannot see it yet, the outcome is unknown, not
			// refused: the pull request exists.
			v, found, ferr := t.find(ctx, p)
			if ferr == nil && found {
				return v, nil
			}
			return nil, fmt.Errorf("a pull request for %s exists but was not found: %v (lookup: %v)", p.Branch, err, ferr)
		}
		return nil, onBranch(p, classify(err))
	}
	return prValue(pr, p.Branch), nil
}

// patchTree makes the tree of base with the payload's files. Nothing points at it, and GitHub
// names it by its content: the same base makes the same tree on every attempt.
func (t *OpenDraftPR) patchTree(ctx context.Context, p prPayload, base string) (string, error) {
	entries := make([]*github.TreeEntry, 0, len(p.Files))
	for _, f := range p.Files {
		entries = append(entries, &github.TreeEntry{Path: new(f.Path), Mode: new("100644"),
			Type: new("blob"), Content: f.Content})
	}
	tree, _, err := t.Client.gh.Git.CreateTree(ctx, p.Owner, p.Repo, base, entries)
	if err != nil {
		return "", err
	}
	return tree.GetSHA(), nil
}

// onBranch is what an error comes to once agentd has seen the payload's branch on GitHub:
// GitHub declining a request goes to a person, the branch being one that this attempt or an
// earlier one may have pushed, and refused does not ask GitHub about it again (someone may
// have deleted it meanwhile, or GitHub may fail to say). Anything else stays as it is.
func onBranch(p prPayload, err error) error {
	var declined *tools.RefusedError
	if errors.As(err, &declined) && !declined.Retryable {
		return &tools.ConflictError{Reason: "GitHub refused: " + declined.Reason + "; and branch " + p.Branch +
			" exists, so part of the pull request may have gone out"}
	}
	return err
}

// refused passes err on, unless it refuses the payload: agentd's own refusal (a DeniedError), or
// GitHub declining a request (a RefusedError that is not to be tried again). A refusal is only
// for a payload nothing of which went out, and GitHub must show no branch of its name. When the
// branch exists, this attempt or an earlier one sent part of the pull request (this one is a
// resend after recovery, say), and a person settles the effect; so too when agentd cannot tell.
// GitHub answers 404 for anything in a repository the token cannot see, a branch too: unless
// the repository was seen (seen), it is looked at first, and always after GitHub declined a
// request, which may be the token losing sight of it. GitHub failing to say is tried again.
func (t *OpenDraftPR) refused(ctx context.Context, p prPayload, seen bool, err error) error {
	var denied *tools.DeniedError
	var declined *tools.RefusedError
	var reason string
	switch {
	case errors.As(err, &denied):
		reason = denied.Reason
	case errors.As(err, &declined) && !declined.Retryable:
		reason, seen = "GitHub refused: "+declined.Reason, false
	default:
		return err
	}
	unsure := func(why string) error {
		return &tools.ConflictError{Reason: reason + "; and " + why + ", so part of the pull request may have gone out"}
	}
	// failed is GitHub's failure to answer: tried again, unless it refused to.
	failed := func(rerr error) error {
		rerr = unseen(rerr)
		if refused := (*tools.RefusedError)(nil); errors.As(rerr, &refused) && !refused.Retryable {
			return unsure("agentd cannot tell whether branch " + p.Branch + " exists (" + refused.Reason + ")")
		}
		return rerr
	}
	gh := t.Client.gh
	if !seen {
		switch _, resp, rerr := gh.Repositories.Get(ctx, p.Owner, p.Repo); {
		case status(resp) == http.StatusNotFound:
			return unsure("agentd cannot see " + p.Owner + "/" + p.Repo + " to tell whether branch " + p.Branch + " exists")
		case rerr != nil:
			return failed(rerr)
		}
	}
	switch _, resp, rerr := gh.Git.GetRef(ctx, p.Owner, p.Repo, "heads/"+p.Branch); {
	case rerr == nil:
		return unsure("branch " + p.Branch + " exists")
	case status(resp) == http.StatusNotFound:
		return err
	default:
		return failed(rerr)
	}
}

// inBase makes sure the audited commit is in the history of the base branch on GitHub. GitHub
// serves any commit of the repository's network of forks through the repository, a fork's own
// too, so finding the commit there (Git.GetCommit) does not say the repository holds it. A
// branch on a commit outside the base branch would put the commits it holds of its own into the
// repository, made by anyone who can open a fork: a push runs their CI workflows with the
// repository's secrets, and the pull request would carry them. So agentd opens one only on a
// commit the branch holds: GitHub's comparison of the branch with the commit has the commit
// as the merge base, and the branch at it or ahead of it. It returns the branch's commit, the
// tip the pull request's commit is made on. A refusal goes through refused.
func (t *OpenDraftPR) inBase(ctx context.Context, p prPayload) (string, error) {
	gh := t.Client.gh
	// The branch's own commit: a name in the comparison could be a tag's as well.
	ref, resp, err := gh.Git.GetRef(ctx, p.Owner, p.Repo, "heads/"+p.Base)
	switch {
	case status(resp) == http.StatusNotFound:
		return "", t.refused(ctx, p, false, &tools.DeniedError{Reason: fmt.Sprintf(
			"GitHub has no branch %s in %s/%s (or the token cannot see the repository)", p.Base, p.Owner, p.Repo)})
	case err != nil:
		return "", t.refused(ctx, p, false, unseen(err))
	}
	// The answer must be about the branch the pull request names (the name was checked, but GitHub
	// answers for "a//b" with a/b's commit), and that branch a commit.
	tip := ref.GetObject().GetSHA()
	if ref.GetRef() != "refs/heads/"+p.Base || ref.GetObject().GetType() != "commit" || !fullSHA.MatchString(tip) {
		return "", t.refused(ctx, p, true, &tools.DeniedError{Reason: fmt.Sprintf(
			"GitHub answered for branch %s of %s/%s with %.100q, a %.20q %.100q: agentd cannot compare the commit with it",
			p.Base, p.Owner, p.Repo, ref.GetRef(), ref.GetObject().GetType(), tip)})
	}
	switch why, missing, err := t.behind(ctx, p, tip, p.Commit, fmt.Sprintf("branch %s of %s/%s at %s", p.Base, p.Owner, p.Repo, tip)); {
	case err != nil:
		return "", t.refused(ctx, p, false, err)
	case why != "":
		return "", t.refused(ctx, p, !missing, &tools.DeniedError{Reason: why})
	}
	return tip, nil
}

// behind says whether commit is in the history of base, another commit, as GitHub compares the
// two: commit is the merge base, and base at it or ahead of it. When it is not, why says so, with
// where naming base; missing, when GitHub has no comparison of the two, which have no history in
// common (both are on GitHub), or the token lost sight of the repository. GitHub failing is err.
func (t *OpenDraftPR) behind(ctx context.Context, p prPayload, base, commit, where string) (why string, missing bool, err error) {
	cmp, why, missing, err := t.compare(ctx, p, base, commit, where)
	if why != "" || err != nil {
		return why, missing, err
	}
	if !cmp.holds(commit) {
		return fmt.Sprintf("commit %s is not in the history of %s: GitHub compares it as %.20q, with %d commits of its own",
			commit, where, cmp.Status, cmp.AheadBy), false, nil
	}
	return "", false, nil
}

// comparison is GitHub's comparison of a commit with a base.
type comparison struct {
	Status    string `json:"status"`
	AheadBy   int    `json:"ahead_by"`  // the commit's own commits
	BehindBy  int    `json:"behind_by"` // the base's commits the commit does not have
	MergeBase struct {
		SHA string `json:"sha"`
	} `json:"merge_base_commit"`
}

// holds says whether the base holds commit: commit is the merge base, and the base at it or ahead.
func (c comparison) holds(commit string) bool {
	return c.MergeBase.SHA == commit && (c.Status == "behind" || c.Status == "identical")
}

// compare is GitHub's comparison of commit with base (base...commit). When GitHub has no
// comparison, why says so (missing: see behind), as when it is over the limit. GitHub failing is err.
func (t *OpenDraftPR) compare(ctx context.Context, p prPayload, base, commit, where string) (cmp comparison, why string, missing bool, err error) {
	gh := t.Client.gh
	// Only the first of the commit's own commits: the answer stays small when it has many.
	req, err := gh.NewRequest(ctx, http.MethodGet, fmt.Sprintf("repos/%v/%v/compare/%v...%v?per_page=1", p.Owner, p.Repo, base, commit), nil)
	if err != nil {
		return cmp, "", false, fmt.Errorf("%w: %v", tools.ErrNotSent, err)
	}
	body := &cappedBuffer{max: maxCompareBytes}
	resp, err := gh.Do(req, body)
	switch {
	case status(resp) == http.StatusNotFound:
		return cmp, fmt.Sprintf("commit %s has no history in common with %s (or the token cannot see the repository)", commit, where), true, nil
	case errors.Is(err, errOverLimit):
		return cmp, fmt.Sprintf("GitHub's comparison of commit %s with %s is over the limit of %d bytes (that of a commit in the branch's history lists no commits and no files)",
			commit, where, maxCompareBytes), false, nil
	case err != nil:
		return cmp, "", false, unseen(err)
	}
	if err := json.Unmarshal(body.buf.Bytes(), &cmp); err != nil {
		return cmp, "", false, fmt.Errorf("%w: GitHub's comparison of commit %s with %s: %v", tools.ErrNotSent, commit, where, err)
	}
	return cmp, "", false, nil
}

// Bounds on one call's checks of the payload's files on GitHub, which list every directory on
// the way to each file once, in each tree the call checks: the audited commit's, the tip's, and
// the older tip a branch of the payload's name is on (a directory they share is listed once). At
// most three times patch.MaxFiles times patch.MaxDepth listings (a fix needs a handful). A
// crafted repository can hold directories of hundreds of thousands of entries, which GitHub
// lists whole. And on the comparison of the base branch with the commit: that of
// a commit in the branch's history lists neither commits nor files, only the two commits, whose
// messages (twice each when signed) can still be long.
const (
	maxListingBytes = 8 << 20  // one listing, as GitHub sends it: some 40,000 entries
	maxCheckBytes   = 64 << 20 // all of them
	maxCompareBytes = 8 << 20
)

var fullSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

// checkFiles makes sure each file the payload touches is, at the audited commit on GitHub (its
// tree), what the git tool server read from the mirror: a file the patch changes or deletes is a
// regular file (100644) whose blob is the one the patch was applied to; where the patch creates
// one, GitHub has nothing, and nothing above it but directories, and nothing whose name differs
// only in case. A crafted repository can subvert the server (design doc, section 8): agentd
// checks the shape of its answers, and here their content. A mismatch is a refusal; GitHub
// failing is not, and the call is tried again.
func (t *OpenDraftPR) checkFiles(ctx context.Context, w *treeWalk, p prPayload, tree string) error {
	for _, f := range p.Files {
		if !f.New && f.OldBlob == "" {
			return &tools.DeniedError{Reason: f.Path + ": the payload names neither the file it changes nor a new one (an older agentd recorded it)"}
		}
	}
	return t.filesIn(ctx, w, p, tree, " at commit "+p.Commit, func(f prFile) string {
		return "the git tool server read the regular file " + f.OldBlob + " from the mirror"
	})
}

// atTip makes sure the files the patch touches are, at the tip of the base branch (tipTree),
// what they were at the audited commit, by the same rules: a branch on the tip then holds the
// tip and the patch, and nothing else. A file the base branch has changed since, or something
// in the way of a new one, is a refusal: the operator can audit the tip. Where the tip's tree is
// the audited commit's, its listings are those checkFiles read.
func (t *OpenDraftPR) atTip(ctx context.Context, w *treeWalk, p prPayload, tip, tipTree string) error {
	return t.filesIn(ctx, w, p, tipTree, fmt.Sprintf(" at commit %s, the tip of branch %s", tip, p.Base), madeFor(p))
}

// madeFor says what the patch was made for, of a file it changes or deletes.
func madeFor(p prPayload) func(prFile) string {
	return func(f prFile) string {
		return "the patch was made for the regular file " + f.OldBlob + " at commit " + p.Commit
	}
}

// filesIn checks the payload's files in one tree, root, which at says where it is on GitHub.
// was says what a file the patch changes or deletes was.
func (t *OpenDraftPR) filesIn(ctx context.Context, w *treeWalk, p prPayload, root, at string, was func(prFile) string) error {
	for _, f := range p.Files {
		e, notDir, clash, err := w.lookUp(ctx, root, f.Path)
		if err != nil {
			return err
		}
		var reason string
		switch {
		case notDir != nil:
			reason = fmt.Sprintf("%s: GitHub has %s at %s, above it,%s", f.Path, describe(notDir), notDir.Path, at)
		case f.New && e != nil:
			reason = fmt.Sprintf("%s: the patch creates it, but GitHub has %s there%s", f.Path, describe(e), at)
		case f.New && clash != nil:
			reason = fmt.Sprintf("%s: the patch creates it, but GitHub has %s at %s, a name that differs only in case,%s",
				f.Path, describe(clash), clash.Path, at)
		case f.New:
		case e == nil || !e.regular() || e.SHA != f.OldBlob:
			reason = fmt.Sprintf("%s: %s, but GitHub has %s there%s", f.Path, was(f), describe(e), at)
		}
		if reason != "" {
			return &tools.DeniedError{Reason: reason}
		}
	}
	return nil
}

// entry is a tree entry as GitHub's Git Trees API lists it.
type entry struct {
	Path string `json:"path"` // its name in the tree listed; where it is, once found
	Mode string `json:"mode"`
	Type string `json:"type"`
	SHA  string `json:"sha"`
}

func (e *entry) regular() bool { return e.Mode == "100644" && e.Type == "blob" }
func (e *entry) dir() bool     { return e.Mode == "040000" && e.Type == "tree" }

// describe names a tree entry, for a refusal.
func describe(e *entry) string {
	switch {
	case e == nil:
		return "nothing"
	case e.regular():
		return "the regular file " + e.SHA
	case e.Mode == "100755" && e.Type == "blob":
		return "an executable file"
	case e.Mode == "120000" && e.Type == "blob":
		return "a symbolic link"
	case e.dir():
		return "a directory"
	case e.Mode == "160000" && e.Type == "commit":
		return "a submodule"
	}
	return fmt.Sprintf("a %.20q of mode %.20q", e.Type, e.Mode)
}

// treeWalk looks paths up in a commit's tree on GitHub, one directory listing at a time,
// listing each directory once. Of a listing it keeps only the entries that some part of some
// path names, in any case: what it looks up, and what could clash with that.
type treeWalk struct {
	gh          *github.Client
	owner, repo string
	need        map[string]bool              // the paths' parts, folded
	trees       map[string]map[string]*entry // tree -> the entries kept, by name
	read        int                          // bytes of listings
}

func newTreeWalk(gh *github.Client, p prPayload) *treeWalk {
	need := map[string]bool{}
	for _, f := range p.Files {
		for part := range strings.SplitSeq(f.Path, "/") {
			need[fold(part)] = true
		}
	}
	return &treeWalk{gh: gh, owner: p.Owner, repo: p.Repo, need: need, trees: map[string]map[string]*entry{}}
}

// fold is a name as a case-insensitive file system tells names apart: "ſ" (long s) is "s"
// there, and the Kelvin sign "k", which lowering alone does not show.
func fold(name string) string { return strings.ToLower(strings.ToUpper(name)) }

// lookUp is the entry at p in the tree root: nil when there is none. When something other than
// a directory is on the way to p, nothing can be at p; notDir is that entry. When nothing is
// at a part of p, but something of a name that differs from it only in case is, clash is that.
// Their Paths are set to where they are.
func (w *treeWalk) lookUp(ctx context.Context, root, p string) (e, notDir, clash *entry, err error) {
	parts := strings.Split(p, "/")
	tree := root
	for i, part := range parts {
		entries, err := w.list(ctx, strings.Join(parts[:i], "/"), tree)
		if err != nil {
			return nil, nil, nil, err
		}
		at := func(found *entry, name string) *entry {
			c := *found
			c.Path = strings.Join(append(slices.Clone(parts[:i]), name), "/")
			return &c
		}
		e := entries[part]
		switch {
		case e == nil:
			for _, name := range slices.Sorted(maps.Keys(entries)) {
				if fold(name) == fold(part) {
					return nil, nil, at(entries[name], name), nil
				}
			}
			return nil, nil, nil, nil
		case i == len(parts)-1:
			return at(e, part), nil, nil, nil
		case !e.dir():
			return nil, at(e, part), nil, nil
		}
		tree = e.SHA
	}
	return nil, nil, nil, nil // no parts: never, the patch has no empty paths
}

// list is the entries kept of a tree, which is at dir. A listing GitHub cuts short, one over
// maxListingBytes (or over what is left of maxCheckBytes), or one that names an entry twice,
// is a refusal: the same tree is listed the same way every time.
func (w *treeWalk) list(ctx context.Context, dir, sha string) (map[string]*entry, error) {
	if entries, ok := w.trees[sha]; ok {
		return entries, nil
	}
	if dir == "" {
		dir = "the top directory"
	}
	req, err := w.gh.NewRequest(ctx, http.MethodGet, fmt.Sprintf("repos/%v/%v/git/trees/%v", w.owner, w.repo, sha), nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", tools.ErrNotSent, err)
	}
	body := &cappedBuffer{max: min(maxListingBytes, maxCheckBytes-w.read)}
	_, err = w.gh.Do(req, body)
	w.read += body.buf.Len()
	switch {
	case errors.Is(err, errOverLimit) && body.max < maxListingBytes:
		return nil, &tools.DeniedError{Reason: fmt.Sprintf(
			"checking the patch's files on GitHub takes listings of over %d bytes in all", maxCheckBytes)}
	case errors.Is(err, errOverLimit):
		return nil, &tools.DeniedError{Reason: fmt.Sprintf("GitHub's listing of %s is over the limit of %d bytes", dir, maxListingBytes)}
	case err != nil:
		return nil, unseen(err)
	}
	var listing struct {
		Entries   []entry `json:"tree"`
		Truncated bool    `json:"truncated"`
	}
	if err := json.Unmarshal(body.buf.Bytes(), &listing); err != nil {
		return nil, fmt.Errorf("%w: GitHub's listing of %s: %v", tools.ErrNotSent, dir, err)
	}
	if listing.Truncated {
		return nil, &tools.DeniedError{Reason: "GitHub lists " + dir + " only in part, so agentd cannot check the patch against it"}
	}
	entries := map[string]*entry{}
	for _, e := range listing.Entries {
		if !w.need[fold(e.Path)] {
			continue
		}
		if _, twice := entries[e.Path]; twice {
			return nil, &tools.DeniedError{Reason: fmt.Sprintf("GitHub lists %.200q twice in %s", e.Path, dir)}
		}
		entries[e.Path] = &e // a copy: the rest of the listing goes
	}
	w.trees[sha] = entries
	return entries, nil
}

// errOverLimit stops the reading of an answer larger than agentd takes.
var errOverLimit = errors.New("the answer is over the limit")

// cappedBuffer keeps what is written to it, up to max bytes. (Not an embedded bytes.Buffer:
// io.Copy would use its ReadFrom, which knows no limit.)
type cappedBuffer struct {
	buf bytes.Buffer
	max int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if b.buf.Len()+len(p) > b.max {
		return 0, errOverLimit
	}
	return b.buf.Write(p)
}

// branch makes sure the branch exists and holds the patch on the base branch: on the tip, as this
// attempt makes it, or as an earlier attempt made it on the tip it read (checkBranch). It
// returns the branch commit's parent. A branch of that name that holds anything else is left
// alone: a person decides.
func (t *OpenDraftPR) branch(ctx context.Context, w *treeWalk, p prPayload, tip, tipTree string, proceed tools.Proceed) (string, error) {
	gh := t.Client.gh
	ref, resp, err := gh.Git.GetRef(ctx, p.Owner, p.Repo, "heads/"+p.Branch)
	switch {
	case err == nil:
		parent, err := t.checkBranch(ctx, w, p, ref.GetObject().GetSHA(), tip, tipTree)
		return parent, onBranch(p, err)
	case status(resp) != http.StatusNotFound:
		return "", unseen(err)
	}
	if err := t.atTip(ctx, w, p, tip, tipTree); err != nil {
		return "", err
	}
	tree, err := t.patchTree(ctx, p, tipTree)
	if err != nil {
		return "", unseen(err)
	}
	date, err := time.Parse(time.RFC3339, p.Author.Date)
	if err != nil {
		return "", fmt.Errorf("%w: author date: %v", tools.ErrNotSent, err)
	}
	who := &github.CommitAuthor{Name: new(p.Author.Name), Email: new(p.Author.Email),
		Date: &github.Timestamp{Time: date}}
	commit, _, err := gh.Git.CreateCommit(ctx, p.Owner, p.Repo, github.Commit{
		Message: new(p.Message), Tree: &github.Tree{SHA: new(tree)},
		Parents: []*github.Commit{{SHA: new(tip)}}, Author: who, Committer: who,
	}, nil)
	if err != nil {
		return "", unseen(err)
	}
	// Just before the branch goes out: the base branch may have been rewritten while agentd
	// checked, and a push on a tip it no longer holds would run that tip's workflows again.
	// Nothing goes out then, and the call is tried again, on the tip there is by then.
	if why, err := t.stillHolds(ctx, p, tip); err != nil {
		return "", err
	} else if why != "" {
		return "", fmt.Errorf("%w: branch %s moved while agentd checked it: %s", tools.ErrNotSent, p.Base, why)
	}
	if err := proceed(ctx); err != nil {
		return "", err
	}
	_, resp, err = gh.Git.CreateRef(ctx, p.Owner, p.Repo, github.CreateRef{Ref: "refs/heads/" + p.Branch, SHA: commit.GetSHA()})
	if err == nil {
		return tip, nil
	}
	if status(resp) != http.StatusUnprocessableEntity {
		return "", unseen(err)
	}
	// GitHub declined to create it: the branch appeared meanwhile, or for a reason of its own.
	declined, exists := unseen(err), strings.Contains(err.Error(), "Reference already exists")
	switch ref, resp, err := gh.Git.GetRef(ctx, p.Owner, p.Repo, "heads/"+p.Branch); {
	case err == nil:
		parent, err := t.checkBranch(ctx, w, p, ref.GetObject().GetSHA(), tip, tipTree)
		return parent, onBranch(p, err)
	case status(resp) == http.StatusNotFound && exists:
		// GitHub's read behind its write (an earlier attempt's, whose answer was lost, say): as
		// far as agentd can tell, the branch is there.
		return "", &tools.ConflictError{Reason: "GitHub refused to create branch " + p.Branch +
			", which it says exists, and does not show it (or a branch whose name is a directory above it, " +
			"such as agent, is in the way); so part of the pull request may have gone out"}
	case status(resp) == http.StatusNotFound:
		return "", declined
	default:
		return "", unseen(err)
	}
}

// stillHolds says why the base branch no longer holds commit, or "" when it does: GitHub's
// branch is at commit, or ahead of it. agentd asks just before a request that brings commit into
// the repository (the push of a branch on it, a pull request of such a branch, which GitHub merges
// into the base): the branch may have been rewritten since agentd read it, by a force push that
// took commit out of its history, as in an incident. GitHub's answer not being about the branch
// (no branch, another ref) is err, tried again: the next attempt checks the branch from the start.
func (t *OpenDraftPR) stillHolds(ctx context.Context, p prPayload, commit string) (string, error) {
	ref, resp, err := t.Client.gh.Git.GetRef(ctx, p.Owner, p.Repo, "heads/"+p.Base)
	now := ref.GetObject().GetSHA()
	switch {
	case status(resp) == http.StatusNotFound:
		return "", fmt.Errorf("%w: GitHub no longer shows branch %s", tools.ErrNotSent, p.Base)
	case err != nil:
		return "", unseen(err)
	case ref.GetRef() != "refs/heads/"+p.Base || ref.GetObject().GetType() != "commit" || !fullSHA.MatchString(now):
		return "", fmt.Errorf("%w: GitHub answered for branch %s with %.100q, a %.20q %.100q", tools.ErrNotSent,
			p.Base, ref.GetRef(), ref.GetObject().GetType(), now)
	case now == commit:
		return "", nil
	}
	why, _, err := t.behind(ctx, p, now, commit, fmt.Sprintf("branch %s of %s/%s at %s, which it has moved to", p.Base, p.Owner, p.Repo, now))
	return why, err
}

// checkBranch compares the commit a branch points at with the one an attempt makes for the
// payload, and leaves anything else to a person. It returns the commit's parent. Not the
// commit's hash, which would need GitHub to keep the commit object exactly as sent (no signature
// added, for example).
func (t *OpenDraftPR) checkBranch(ctx context.Context, w *treeWalk, p prPayload, sha, tip, tipTree string) (string, error) {
	c, _, err := t.Client.gh.Git.GetCommit(ctx, p.Owner, p.Repo, sha)
	if err != nil {
		return "", unseen(err)
	}
	why, err := t.notAttempt(ctx, w, p, c, tip, tipTree)
	switch {
	case err != nil:
		return "", err
	case why == "":
		return c.Parents[0].GetSHA(), nil
	}
	return "", &tools.ConflictError{Reason: fmt.Sprintf(
		"branch %s already exists and points at %s, which is not this patch on branch %s as agentd makes it (%s); agentd does not overwrite it",
		p.Branch, sha, p.Base, why)}
}

// notAttempt says why c is not the commit an attempt makes for the payload, or "" when it is:
// the patch on the tip this attempt read; or on the tip an earlier attempt read, which the
// branch has moved on from since. That commit has one parent, in the branch's history, with
// the audited commit in its own history (an older agentd made it on the audited commit itself);
// the files the patch touches are, at the parent, what it was made for; and its tree is the
// parent's with the patch. A commit on an older parent must also carry the payload's message
// and author, as every attempt makes it: someone else's could be on any commit of the history.
func (t *OpenDraftPR) notAttempt(ctx context.Context, w *treeWalk, p prPayload, c *github.Commit, tip, tipTree string) (string, error) {
	if len(c.Parents) != 1 {
		return fmt.Sprintf("a commit of %d parents", len(c.Parents)), nil
	}
	parent := c.Parents[0].GetSHA()
	if parent == tip {
		return t.patchOn(ctx, w, p, c, tip, tipTree, fmt.Sprintf(" at commit %s, the tip of branch %s", tip, p.Base))
	}
	if !madeAs(c, p) {
		return "a commit on an older parent, without the message and author agentd gives this one", nil
	}
	switch why, _, err := t.behind(ctx, p, tip, parent, fmt.Sprintf("branch %s of %s/%s at %s", p.Base, p.Owner, p.Repo, tip)); {
	case err != nil:
		return "", err
	case why != "":
		return "its parent: " + why, nil
	}
	if parent != p.Commit {
		switch why, _, err := t.behind(ctx, p, parent, p.Commit, "commit "+parent+", its parent"); {
		case err != nil:
			return "", err
		case why != "":
			return why, nil
		}
	}
	parentTree, err := t.commitTree(ctx, p, parent)
	if err != nil {
		return "", err
	}
	return t.patchOn(ctx, w, p, c, parent, parentTree, fmt.Sprintf(" at commit %s, its parent", parent))
}

// patchOn says why c is not the patch on parent (whose tree is parentTree), or "" when it is: the
// files the patch touches are, there, what it was made for, and c's tree is parent's with the
// patch, made again (GitHub names a tree by its content, and nothing points at this one).
func (t *OpenDraftPR) patchOn(ctx context.Context, w *treeWalk, p prPayload, c *github.Commit, parent, parentTree, at string) (string, error) {
	err := t.filesIn(ctx, w, p, parentTree, at, madeFor(p))
	if denied := (*tools.DeniedError)(nil); errors.As(err, &denied) {
		return denied.Reason, nil
	} else if err != nil {
		return "", err
	}
	tree, err := t.patchTree(ctx, p, parentTree)
	if err != nil {
		return "", unseen(err)
	}
	if c.GetTree().GetSHA() != tree {
		return "its tree is not the patch on " + parent, nil
	}
	return "", nil
}

// madeAs reports whether c carries what every attempt gives the payload's commit: its message,
// and its author at the run's creation time. (GitHub may sign a commit, which changes neither.)
func madeAs(c *github.Commit, p prPayload) bool {
	date, err := time.Parse(time.RFC3339, p.Author.Date)
	a := c.GetAuthor()
	return err == nil && strings.TrimRight(c.GetMessage(), "\n") == strings.TrimRight(p.Message, "\n") &&
		a.GetName() == p.Author.Name && a.GetEmail() == p.Author.Email && a.GetDate().Time.Equal(date)
}

func (t *OpenDraftPR) Find(ctx context.Context, raw json.RawMessage) (map[string]any, bool, error) {
	if !t.Live() {
		return nil, false, errors.New("GitHub is not configured")
	}
	var p prPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, false, err
	}
	return t.find(ctx, p)
}

// find looks for the pull request by its head branch, open or not: one a person closed is
// still the effect of this call, and is not opened again.
func (t *OpenDraftPR) find(ctx context.Context, p prPayload) (map[string]any, bool, error) {
	prs, _, err := t.Client.gh.PullRequests.List(ctx, p.Owner, p.Repo, &github.PullRequestListOptions{
		State: "all", Head: p.Owner + ":" + p.Branch, ListOptions: github.ListOptions{PerPage: 100},
	})
	if err != nil {
		return nil, false, err
	}
	var first *github.PullRequest
	for _, pr := range prs {
		if pr.GetHead().GetRef() == p.Branch && (first == nil || pr.GetNumber() < first.GetNumber()) {
			first = pr
		}
	}
	if first == nil {
		return nil, false, nil
	}
	return prValue(first, p.Branch), true, nil
}

func prValue(pr *github.PullRequest, branch string) map[string]any {
	return map[string]any{"url": pr.GetHTMLURL(), "number": pr.GetNumber(), "branch": branch}
}
