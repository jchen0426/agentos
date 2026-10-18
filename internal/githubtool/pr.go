package githubtool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
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
// audited commit, created under a name derived from the commit, the run and the patch, so a
// retry recreates the same thing (idempotent); then a draft pull request, which is looked up
// by its head branch before it is opened (lookup). Before either, it checks the files the
// patch touches against the commit on GitHub (checkFiles).
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

// prPayload is everything GitHub receives for one draft pull request.
type prPayload struct {
	Owner   string    `json:"owner"`
	Repo    string    `json:"repo"`
	Base    string    `json:"base"`
	Parent  string    `json:"parent"` // the audited commit
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
		Owner: repo.Owner, Repo: repo.Name, Base: tools.String(req.Args, "base_branch"), Parent: commit,
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
// reads nothing from it either, so nothing checks the payload's files there (Execute does):
// the files are as the git tool server read them.
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
	gh := t.Client.gh
	parent, resp, err := gh.Git.GetCommit(ctx, p.Owner, p.Repo, p.Parent)
	switch {
	case status(resp) == http.StatusNotFound:
		// The git tool server resolved the run's commit, and agentd could check only that it
		// begins with the digits it was given.
		return nil, t.refused(ctx, p, false, &tools.DeniedError{Reason: fmt.Sprintf(
			"GitHub has no commit %s in %s/%s (or the token cannot see the repository)", p.Parent, p.Owner, p.Repo)})
	case err != nil:
		return nil, unseen(err)
	case parent.GetSHA() != p.Parent:
		return nil, fmt.Errorf("%w: GitHub answered for commit %s with %.100q", tools.ErrNotSent, p.Parent, parent.GetSHA())
	}
	// Before anything is written: the tree is made on top of the commit's own tree.
	if err := t.checkFiles(ctx, p, parent.GetTree().GetSHA()); err != nil {
		return nil, t.refused(ctx, p, true, err)
	}
	entries := make([]*github.TreeEntry, 0, len(p.Files))
	for _, f := range p.Files {
		entries = append(entries, &github.TreeEntry{Path: new(f.Path), Mode: new("100644"),
			Type: new("blob"), Content: f.Content})
	}
	tree, _, err := gh.Git.CreateTree(ctx, p.Owner, p.Repo, parent.GetTree().GetSHA(), entries)
	if err != nil {
		return nil, unseen(err)
	}
	if err := t.branch(ctx, p, tree.GetSHA(), proceed); err != nil {
		return nil, err
	}
	if v, found, err := t.find(ctx, p); err != nil {
		return nil, unseen(err)
	} else if found {
		return v, nil
	}
	if err := proceed(ctx); err != nil {
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
		return nil, classify(err)
	}
	return prValue(pr, p.Branch), nil
}

// refused passes err on, unless it refuses the payload: a refusal is only for a payload nothing
// of which went out, and GitHub must show no branch of its name. When the branch exists, an
// earlier attempt may have sent part of it (this one is a resend after recovery, say), and a
// person settles the effect; so too when agentd cannot tell. GitHub answers 404 for anything in
// a repository the token cannot see, a branch too: unless the repository was seen (seen), it is
// looked at first. GitHub failing to say is tried again.
func (t *OpenDraftPR) refused(ctx context.Context, p prPayload, seen bool, err error) error {
	var denied *tools.DeniedError
	if !errors.As(err, &denied) {
		return err
	}
	unsure := func(why string) error {
		return &tools.ConflictError{Reason: denied.Reason + "; and " + why + ", so an earlier attempt may have sent part of the pull request"}
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

// Bounds on one check of the payload's files on GitHub, which lists every directory on the way
// to each file once: at most patch.MaxFiles times patch.MaxDepth listings (a fix needs a
// handful). A crafted repository can hold directories of hundreds of thousands of entries,
// which GitHub lists whole.
const (
	maxListingBytes = 8 << 20  // one listing, as GitHub sends it: some 40,000 entries
	maxCheckBytes   = 64 << 20 // all of them
)

// checkFiles makes sure each file the payload touches is, at the audited commit on GitHub,
// what the git tool server read from the mirror: a file the patch changes or deletes is a
// regular file (100644) whose blob is the one the patch was applied to; where the patch creates
// one, GitHub has nothing, and nothing above it but directories, and nothing whose name differs
// only in case. A crafted repository can subvert the server (design doc, section 8): agentd
// checks the shape of its answers, and here their content. A mismatch is a refusal; GitHub
// failing is not, and the call is tried again.
func (t *OpenDraftPR) checkFiles(ctx context.Context, p prPayload, root string) error {
	w := newTreeWalk(t.Client.gh, p)
	at := " at commit " + p.Parent
	for _, f := range p.Files {
		if !f.New && f.OldBlob == "" {
			return &tools.DeniedError{Reason: f.Path + ": the payload names neither the file it changes nor a new one (an older agentd recorded it)"}
		}
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
			reason = fmt.Sprintf("%s: the git tool server read the regular file %s from the mirror, but GitHub has %s there%s",
				f.Path, f.OldBlob, describe(e), at)
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

// branch makes sure the branch exists and holds the patch's tree on the audited commit. A
// branch of that name that holds anything else is left alone: a person decides.
func (t *OpenDraftPR) branch(ctx context.Context, p prPayload, tree string, proceed tools.Proceed) error {
	gh := t.Client.gh
	ref, resp, err := gh.Git.GetRef(ctx, p.Owner, p.Repo, "heads/"+p.Branch)
	switch {
	case err == nil:
		return t.checkBranch(ctx, p, ref.GetObject().GetSHA(), tree)
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
		Parents: []*github.Commit{{SHA: new(p.Parent)}}, Author: who, Committer: who,
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
	// The branch appeared meanwhile.
	ref, _, err = gh.Git.GetRef(ctx, p.Owner, p.Repo, "heads/"+p.Branch)
	if err != nil {
		return unseen(err)
	}
	return t.checkBranch(ctx, p, ref.GetObject().GetSHA(), tree)
}

// checkBranch compares the commit a branch points at with the one the payload would create:
// the same tree on the same parent. Not the commit's hash, which would need GitHub to keep
// the commit object exactly as sent (no signature added, for example).
func (t *OpenDraftPR) checkBranch(ctx context.Context, p prPayload, sha, tree string) error {
	c, _, err := t.Client.gh.Git.GetCommit(ctx, p.Owner, p.Repo, sha)
	if err != nil {
		return unseen(err)
	}
	parents := make([]string, 0, len(c.Parents))
	for _, pc := range c.Parents {
		parents = append(parents, pc.GetSHA())
	}
	if c.GetTree().GetSHA() == tree && slices.Equal(parents, []string{p.Parent}) {
		return nil
	}
	return &tools.ConflictError{Reason: fmt.Sprintf(
		"branch %s already exists and points at %s, which is not this patch on %s; agentd does not overwrite it",
		p.Branch, sha, p.Parent)}
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
