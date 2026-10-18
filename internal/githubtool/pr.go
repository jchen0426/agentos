package githubtool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/go-github/v92/github"

	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tools"
)

// OpenDraftPR is github.open_draft_pr. Two steps (section 6): a branch with the patch on the
// audited commit, created under a name derived from the commit, the run and the patch, so a
// retry recreates the same thing (idempotent); then a draft pull request, which is looked up
// by its head branch before it is opened (lookup).
type OpenDraftPR struct {
	Client    *Client
	Repos     map[string]Repo
	Blobs     tools.BlobStore
	DenyPaths []string // the policy's: the tool checks the patch itself
}

// githubPaths are refused whatever the policy says: GitHub itself acts on them (workflows run
// with the repository's secrets, CODEOWNERS routes reviews, dependabot opens pull requests).
var githubPaths = []string{".github/**"}

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
}

func (t *OpenDraftPR) Prepare(ctx context.Context, req tools.Request) (json.RawMessage, error) {
	repo, ok := t.Repos[tools.String(req.Args, "repo")]
	if !ok {
		return nil, &tools.DeniedError{Reason: "no GitHub repository is configured for " + tools.String(req.Args, "repo")}
	}
	commit, ref := tools.String(req.Args, "commit"), tools.String(req.Args, "patch_ref")
	changes, err := tools.CheckedPatch(ctx, t.Blobs, req.WorkflowID, ref, append(githubPaths, t.DenyPaths...), repo.Mirror, commit)
	if err != nil {
		return nil, err
	}
	files := make([]prFile, 0, len(changes))
	for _, c := range changes {
		f := prFile{Path: c.Path}
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
	parent, _, err := gh.Git.GetCommit(ctx, p.Owner, p.Repo, p.Parent)
	if err != nil {
		return nil, unseen(err)
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
