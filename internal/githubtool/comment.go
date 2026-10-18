package githubtool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/google/go-github/v92/github"

	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tools"
)

// CommentCommit is github.comment_commit: the audit report, posted as a comment on the
// audited commit (section 3). agentd renders it from a fixed template, so a model can put
// neither links nor mentions in it.
type CommentCommit struct {
	Client  *Client
	Repos   map[string]Repo
	Blobs   tools.BlobStore
	Effects Effects
}

// Effects reads agentd's own record of a run's effects.
type Effects interface {
	// LatestEffectResult is the result of the run's latest finished effect of tool.
	LatestEffectResult(ctx context.Context, workflowID, tool string) (map[string]any, bool, error)
}

var commentArgs = tools.MustSchema(`{
  "type": "object", "additionalProperties": false, "required": ["repo", "commit", "outcome"],
  "properties": {
    "repo": {"type": "string", "minLength": 1},
    "commit": {"type": "string", "pattern": "` + tools.CommitPattern + `"},
    "report_ref": {"anyOf": [{"type": "string", "pattern": "` + tools.BlobPattern + `"}, {"type": "null"}]},
    "outcome": {"type": "string", "maxLength": 2000},
    "pr_url": {"anyOf": [{"type": "string"}, {"type": "null"}]}
  }
}`)

var commentResult = tools.MustSchema(`{
  "type": "object", "required": ["url"],
  "properties": {"url": {"type": "string", "minLength": 1}, "id": {"type": "integer"}}
}`)

func (*CommentCommit) Name() string            { return "github.comment_commit" }
func (*CommentCommit) Class() string           { return policy.EffectLookup }
func (*CommentCommit) Args() *tools.Schema     { return commentArgs }
func (*CommentCommit) Result() *tools.Schema   { return commentResult }
func (t *CommentCommit) Live() bool            { return t.Client.live() }
func (t *CommentCommit) Settle() time.Duration { return t.Client.settle() }

type commentPayload struct {
	Owner  string `json:"owner"`
	Repo   string `json:"repo"`
	Commit string `json:"commit"`
	Body   string `json:"body"`
	Marker string `json:"marker"`
}

// report is the part of the audit report (config/schemas/audit_report.json) the comment shows.
type report struct {
	Summary  string    `json:"summary"`
	Findings []finding `json:"findings"`
}

type finding struct {
	Severity    string `json:"severity"`
	CWE         string `json:"cwe"`
	File        string `json:"file"`
	Line        int64  `json:"line"`
	Title       string `json:"title"`
	Explanation string `json:"explanation"`
}

var severities = []string{"critical", "high", "medium", "low"}

func (t *CommentCommit) Prepare(ctx context.Context, req tools.Request) (json.RawMessage, error) {
	repo, ok := t.Repos[tools.String(req.Args, "repo")]
	if !ok {
		return nil, &tools.DeniedError{Reason: "no GitHub repository is configured for " + tools.String(req.Args, "repo")}
	}
	var rep *report
	if ref := tools.String(req.Args, "report_ref"); ref != "" {
		b, err := t.Blobs.GetRunBlob(ctx, req.WorkflowID, ref)
		if err != nil {
			return nil, &tools.DeniedError{Reason: "report_ref: " + err.Error()}
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		rep = &report{}
		if err := dec.Decode(rep); err != nil {
			return nil, &tools.DeniedError{Reason: "report_ref is not an audit report: " + err.Error()}
		}
		for _, f := range rep.Findings {
			if !slices.Contains(severities, f.Severity) {
				return nil, &tools.DeniedError{Reason: fmt.Sprintf("report_ref is not an audit report: severity %q", f.Severity)}
			}
		}
	}
	// The pull request comes from agentd's own record of this run, never from the workflow.
	var pr int64
	if v, found, err := t.Effects.LatestEffectResult(ctx, req.WorkflowID, (&OpenDraftPR{}).Name()); err != nil {
		return nil, err
	} else if found {
		if n, ok := v["number"].(json.Number); ok {
			pr, _ = n.Int64()
		}
	}
	commit := tools.String(req.Args, "commit")
	m := marker(req.EffectID)
	return json.Marshal(commentPayload{
		Owner: repo.Owner, Repo: repo.Name, Commit: commit, Marker: m,
		Body: render(commit, tools.String(req.Args, "outcome"), rep, pr, m),
	})
}

// maxText keeps a comment under GitHub's 65,536 characters, with the fences: no run of
// backticks in the text is longer than maxBackticks, so neither fence is either.
const (
	maxText      = 60000
	maxBackticks = 16
)

// render builds the comment from what agentd checked itself (the commit, the severities, the
// pull request's number), and puts everything a model or the workflow wrote into one code
// fence, longer than any run of backticks inside it: GitHub renders neither links nor
// mentions there.
func render(commit, outcome string, rep *report, pr int64, marker string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "**agentos security audit** of commit `%s`\n\n", commit)
	if rep != nil {
		counts := map[string]int{}
		for _, f := range rep.Findings {
			counts[f.Severity]++
		}
		var parts []string
		for _, s := range severities {
			if counts[s] > 0 {
				parts = append(parts, fmt.Sprintf("%d %s", counts[s], s))
			}
		}
		if len(parts) == 0 {
			b.WriteString("Findings: none\n\n")
		} else {
			fmt.Fprintf(&b, "Findings: %d (%s)\n\n", len(rep.Findings), strings.Join(parts, ", "))
		}
	}
	if pr > 0 {
		fmt.Fprintf(&b, "A draft pull request proposes a fix: #%d\n\n", pr)
	}
	var text strings.Builder
	text.WriteString("Outcome: " + outcome)
	if rep != nil {
		text.WriteString("\n\nSummary:\n" + rep.Summary)
		for i, f := range rep.Findings {
			fmt.Fprintf(&text, "\n\n%d. [%s] %s at %s:%d\n%s\n%s", i+1, f.Severity, f.CWE, f.File, f.Line, f.Title, f.Explanation)
		}
	}
	body := shortenRuns(clean(text.String()), '`', maxBackticks)
	if len(body) > maxText {
		body = strings.ToValidUTF8(body[:maxText], "") + "\n[truncated]"
	}
	fence := strings.Repeat("`", max(3, longestRun(body, '`')+1))
	fmt.Fprintf(&b, "%stext\n%s\n%s\n\n%s\n", fence, body, fence, marker)
	return b.String()
}

// clean drops control characters other than newlines and tabs, and format characters, such
// as the bidirectional overrides that make text read differently from how it is stored.
func clean(s string) string {
	return strings.Map(func(r rune) rune {
		if (unicode.IsControl(r) || unicode.Is(unicode.Cf, r)) && r != '\n' && r != '\t' {
			return -1
		}
		return r
	}, strings.ReplaceAll(s, "\r\n", "\n"))
}

// shortenRuns cuts every run of c in s to at most n.
func shortenRuns(s string, c rune, n int) string {
	var b strings.Builder
	run := 0
	for _, r := range s {
		if r == c {
			run++
			if run > n {
				continue
			}
		} else {
			run = 0
		}
		b.WriteRune(r)
	}
	return b.String()
}

func lastLine(s string) string {
	s = strings.TrimRight(s, "\n")
	return s[strings.LastIndexByte(s, '\n')+1:]
}

func longestRun(s string, c rune) int {
	longest, run := 0, 0
	for _, r := range s {
		if r == c {
			run++
			longest = max(longest, run)
		} else {
			run = 0
		}
	}
	return longest
}

func (*CommentCommit) DryRun(json.RawMessage) map[string]any {
	return map[string]any{"url": "dry-run://github/comment", "dry_run": true}
}

func (t *CommentCommit) Execute(ctx context.Context, raw json.RawMessage, _ string, proceed tools.Proceed) (map[string]any, error) {
	if !t.Live() {
		return nil, fmt.Errorf("%w: GitHub is not configured", tools.ErrNotSent)
	}
	var p commentPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("%w: %v", tools.ErrNotSent, err)
	}
	if err := proceed(ctx); err != nil {
		return nil, err
	}
	c, _, err := t.Client.gh.Repositories.CreateComment(ctx, p.Owner, p.Repo, p.Commit, github.CreateCommitCommentRequest{Body: p.Body})
	if err != nil {
		return nil, classify(err)
	}
	return map[string]any{"url": c.GetHTMLURL(), "id": c.GetID()}, nil
}

// maxPages bounds a lookup: an audited commit has a handful of comments.
const maxPages = 10

// Find looks for the comment by its marker among the commit's comments. It trusts only a
// comment the token's own account wrote, whose last line is the marker: anyone can post a
// comment with a marker in it, and a workflow could get another effect's marker into the
// fenced text of one of agentd's. The last line is always the comment's own.
func (t *CommentCommit) Find(ctx context.Context, raw json.RawMessage) (map[string]any, bool, error) {
	if !t.Live() {
		return nil, false, errors.New("GitHub is not configured")
	}
	var p commentPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, false, err
	}
	login, err := t.Client.self(ctx)
	if err != nil {
		return nil, false, err
	}
	opts := &github.ListOptions{PerPage: 100}
	for page := 0; page < maxPages; page++ {
		comments, resp, err := t.Client.gh.Repositories.ListCommitComments(ctx, p.Owner, p.Repo, p.Commit, opts)
		if err != nil {
			return nil, false, err
		}
		for _, c := range comments {
			if c.GetUser().GetLogin() == login && lastLine(c.GetBody()) == p.Marker {
				return map[string]any{"url": c.GetHTMLURL(), "id": c.GetID()}, true, nil
			}
		}
		if resp.NextPage == 0 {
			return nil, false, nil
		}
		opts.Page = resp.NextPage
	}
	return nil, false, fmt.Errorf("more than %d pages of comments on %s", maxPages, p.Commit)
}
