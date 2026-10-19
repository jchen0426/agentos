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
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tools"
)

// OpenDraftPR is github.open_draft_pr. Two steps (section 6): a branch with the patch on the
// tip of the base branch, created under a name derived from the audited commit, the run and the
// patch, so a retry recreates the same thing (idempotent); then a draft pull request, which is
// looked up by its head branch before it is opened (lookup). Before either, it checks on GitHub
// that the audited commit is in the base branch's history (inBase) and that the files the patch
// touches are what the patch was made for, at that commit and at the tip (checkFiles).
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
	changes, err := tools.CheckedPatch(ctx, t.Git, t.Blobs, req.WorkflowID, ref, append(slices.Clone(ProtectedPaths), t.DenyPaths...), repo.Mirror, commit)
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
	case audited.GetSHA() != p.Commit:
		return nil, fmt.Errorf("%w: GitHub answered for commit %s with %.100q", tools.ErrNotSent, p.Commit, audited.GetSHA())
	}
	// Before anything is written: the pull request holds the patch alone, on the tip of the
	// base branch, and its tree is the tip's with the patch.
	tip, err := t.inBase(ctx, p)
	if err != nil {
		return nil, err
	}
	tree, tipTree := audited.GetTree().GetSHA(), audited.GetTree().GetSHA()
	if tip != p.Commit {
		if tipTree, err = t.commitTree(ctx, p, tip); err != nil {
			return nil, t.refused(ctx, p, false, err)
		}
	}
	if err := t.checkFiles(ctx, p, tree, tip, tipTree); err != nil {
		return nil, t.refused(ctx, p, true, err)
	}
	// From here on agentd writes. GitHub declining a request leaves the branch on GitHub when
	// this attempt or an earlier one made it.
	v, err := t.write(ctx, p, tip, tipTree, proceed)
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
	case c.GetSHA() != sha:
		return "", fmt.Errorf("%w: GitHub answered for commit %s with %.100q", tools.ErrNotSent, sha, c.GetSHA())
	}
	return c.GetTree().GetSHA(), nil
}

// write makes the patch's tree on top of the tip's (tipTree), the branch on the tip, and the
// pull request.
func (t *OpenDraftPR) write(ctx context.Context, p prPayload, tip, tipTree string, proceed tools.Proceed) (map[string]any, error) {
	gh := t.Client.gh
	tree, err := t.patchTree(ctx, p, tipTree)
	if err != nil {
		return nil, unseen(err)
	}
	if err := t.branch(ctx, p, tip, tree, proceed); err != nil {
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
	if err := proceed(ctx); err != nil {
		return nil, &tools.ConflictError{Reason: fmt.Sprintf("branch %s exists, but the pull request was not opened (%v)", p.Branch, err)}
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
	switch why, missing, err := t.behind(ctx, p, tip, p.Commit); {
	case err != nil:
		return "", t.refused(ctx, p, false, err)
	case why != "":
		return "", t.refused(ctx, p, !missing, &tools.DeniedError{Reason: why})
	}
	return tip, nil
}

// behind says whether a commit is in the history of the base branch at tip, as GitHub compares
// the two: the commit is the merge base, and the branch at it or ahead of it. When it is not,
// why says so; missing, when GitHub has no comparison of the two, which have no history in
// common (both are on GitHub), or the token lost sight of the repository. GitHub failing is err.
func (t *OpenDraftPR) behind(ctx context.Context, p prPayload, tip, commit string) (why string, missing bool, err error) {
	gh := t.Client.gh
	where := fmt.Sprintf("branch %s of %s/%s at %s", p.Base, p.Owner, p.Repo, tip)
	// Only the first of the commit's own commits: the answer stays small when it has many.
	req, err := gh.NewRequest(ctx, http.MethodGet, fmt.Sprintf("repos/%v/%v/compare/%v...%v?per_page=1", p.Owner, p.Repo, tip, commit), nil)
	if err != nil {
		return "", false, fmt.Errorf("%w: %v", tools.ErrNotSent, err)
	}
	body := &cappedBuffer{max: maxCompareBytes}
	resp, err := gh.Do(req, body)
	switch {
	case status(resp) == http.StatusNotFound:
		return fmt.Sprintf("commit %s has no history in common with %s (or the token cannot see the repository)", commit, where), true, nil
	case errors.Is(err, errOverLimit):
		return fmt.Sprintf("GitHub's comparison of commit %s with %s is over the limit of %d bytes (that of a commit in the branch's history lists no commits and no files)",
			commit, where, maxCompareBytes), false, nil
	case err != nil:
		return "", false, unseen(err)
	}
	var cmp struct {
		Status    string `json:"status"`
		AheadBy   int    `json:"ahead_by"`
		MergeBase struct {
			SHA string `json:"sha"`
		} `json:"merge_base_commit"`
	}
	if err := json.Unmarshal(body.buf.Bytes(), &cmp); err != nil {
		return "", false, fmt.Errorf("%w: GitHub's comparison of commit %s with %s: %v", tools.ErrNotSent, commit, where, err)
	}
	if cmp.MergeBase.SHA != commit || cmp.Status != "behind" && cmp.Status != "identical" {
		return fmt.Sprintf("commit %s is not in the history of %s: GitHub compares it as %.20q, with %d commits of its own, which a pull request on it would bring in",
			commit, where, cmp.Status, cmp.AheadBy), false, nil
	}
	return "", false, nil
}

// Bounds on one check of the payload's files on GitHub, which lists every directory on the way
// to each file once, in the audited commit's tree and in the tip's (a directory the two share
// is listed once): at most twice patch.MaxFiles times patch.MaxDepth listings (a fix needs a
// handful). A crafted repository can hold directories of hundreds of thousands of entries,
// which GitHub lists whole. And on the comparison of the base branch with the commit: that of
// a commit in the branch's history lists neither commits nor files, only the two commits, whose
// messages (twice each when signed) can still be long.
const (
	maxListingBytes = 8 << 20  // one listing, as GitHub sends it: some 40,000 entries
	maxCheckBytes   = 64 << 20 // all of them
	maxCompareBytes = 8 << 20
)

var fullSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

// checkFiles makes sure each file the payload touches is on GitHub what the patch was made for:
// in the audited commit's tree, what the git tool server read from the mirror; and the same in
// the tree of the base branch's tip, which the pull request's commit is made on, when that is
// another tree. A file the patch changes or deletes is a regular file (100644) whose blob is the
// one the patch was applied to; where the patch creates one, GitHub has nothing, and nothing
// above it but directories, and nothing whose name differs only in case. A crafted repository
// can subvert the server (design doc, section 8): agentd checks the shape of its answers, and
// here their content. A mismatch is a refusal; GitHub failing is not, and the call is tried
// again.
func (t *OpenDraftPR) checkFiles(ctx context.Context, p prPayload, tree, tip, tipTree string) error {
	for _, f := range p.Files {
		if !f.New && f.OldBlob == "" {
			return &tools.DeniedError{Reason: f.Path + ": the payload names neither the file it changes nor a new one (an older agentd recorded it)"}
		}
	}
	w := newTreeWalk(t.Client.gh, p)
	read := func(f prFile) string {
		return "the git tool server read the regular file " + f.OldBlob + " from the mirror"
	}
	if err := t.filesIn(ctx, w, p, tree, " at commit "+p.Commit, read); err != nil || tipTree == tree {
		return err
	}
	madeFor := func(f prFile) string {
		return "the patch was made for the regular file " + f.OldBlob + " at commit " + p.Commit
	}
	return t.filesIn(ctx, w, p, tipTree, fmt.Sprintf(" at commit %s, the tip of branch %s", tip, p.Base), madeFor)
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
// attempt makes it (tree), or as an earlier attempt made it on the tip it read (checkBranch). A
// branch of that name that holds anything else is left alone: a person decides.
func (t *OpenDraftPR) branch(ctx context.Context, p prPayload, tip, tree string, proceed tools.Proceed) error {
	gh := t.Client.gh
	ref, resp, err := gh.Git.GetRef(ctx, p.Owner, p.Repo, "heads/"+p.Branch)
	switch {
	case err == nil:
		return onBranch(p, t.checkBranch(ctx, p, ref.GetObject().GetSHA(), tip, tree))
	case status(resp) != http.StatusNotFound:
		return unseen(err)
	}
	date, err := time.Parse(time.RFC3339, p.Author.Date)
	if err != nil {
		return fmt.Errorf("%w: author date: %v", tools.ErrNotSent, err)
	}
	who := &github.CommitAuthor{Name: new(p.Author.Name), Email: new(p.Author.Email),
		Date: &github.Timestamp{Time: date}}
	commit, _, err := gh.Git.CreateCommit(ctx, p.Owner, p.Repo, github.Commit{
		Message: new(p.Message), Tree: &github.Tree{SHA: new(tree)},
		Parents: []*github.Commit{{SHA: new(tip)}}, Author: who, Committer: who,
	}, nil)
	if err != nil {
		return unseen(err)
	}
	if err := proceed(ctx); err != nil {
		return err
	}
	_, resp, err = gh.Git.CreateRef(ctx, p.Owner, p.Repo, github.CreateRef{Ref: "refs/heads/" + p.Branch, SHA: commit.GetSHA()})
	if err == nil {
		return nil
	}
	if status(resp) != http.StatusUnprocessableEntity {
		return unseen(err)
	}
	// GitHub declined to create it: the branch appeared meanwhile, or for a reason of its own.
	declined := unseen(err)
	switch ref, resp, err := gh.Git.GetRef(ctx, p.Owner, p.Repo, "heads/"+p.Branch); {
	case err == nil:
		return onBranch(p, t.checkBranch(ctx, p, ref.GetObject().GetSHA(), tip, tree))
	case status(resp) == http.StatusNotFound:
		return declined
	default:
		return unseen(err)
	}
}

// checkBranch compares the commit a branch points at with the patch on the base branch: the
// commit this attempt would create, the patch's tree on the tip; or the one an earlier attempt
// created on the tip it read, which the branch has moved on from since: one parent, in the
// branch's history, and that parent's tree with the patch. (An older agentd made it on the
// audited commit, which is in the history too.) Not the commit's hash, which would need GitHub
// to keep the commit object exactly as sent (no signature added, for example).
func (t *OpenDraftPR) checkBranch(ctx context.Context, p prPayload, sha, tip, tree string) error {
	c, _, err := t.Client.gh.Git.GetCommit(ctx, p.Owner, p.Repo, sha)
	if err != nil {
		return unseen(err)
	}
	var why string
	switch parents := c.Parents; {
	case len(parents) != 1:
		why = fmt.Sprintf("a commit of %d parents", len(parents))
	case parents[0].GetSHA() == tip && c.GetTree().GetSHA() == tree:
		return nil
	case parents[0].GetSHA() == tip:
		why = "its tree is not the patch on the tip " + tip
	default:
		old := parents[0].GetSHA()
		if notIn, _, err := t.behind(ctx, p, tip, old); err != nil {
			return err
		} else if notIn != "" {
			why = "its parent: " + notIn
			break
		}
		oldTree, err := t.commitTree(ctx, p, old)
		if err != nil {
			return err
		}
		// Made again on the parent's tree: GitHub names a tree by its content, and nothing
		// points at this one.
		patched, err := t.patchTree(ctx, p, oldTree)
		if err != nil {
			return unseen(err)
		}
		if c.GetTree().GetSHA() == patched {
			return nil
		}
		why = "its tree is not the patch on its parent " + old
	}
	return &tools.ConflictError{Reason: fmt.Sprintf(
		"branch %s already exists and points at %s, which is not this patch on branch %s (%s); agentd does not overwrite it",
		p.Branch, sha, p.Base, why)}
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
