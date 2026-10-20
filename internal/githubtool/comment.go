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
	// RunApprovals is the run's approvals, oldest first (design doc, section 10).
	RunApprovals(ctx context.Context, workflowID string) ([]Approval, error)
}

// Approval is one approval of a run's write, and how it was decided, as agentd recorded it. The
// report shows them: a rejection's reason goes into it (design doc, section 10).
type Approval struct {
	ID, Tool  string
	Status    string // PENDING, APPROVED, REJECTED, EXPIRED, EDITED or SUPERSEDED
	DecidedBy string // the reviewer, as they named themselves; agentd for an expiry
	DecidedAt string
	Reason    string // a rejection's reason, or why it expired
	ExpiresAt string
	// Write says what became of an approved write that did not go out as approved: "not sent"
	// (agentd or the tool refused it before sending anything, or the run was cancelled), "failed"
	// (the external system refused it) or "outcome unknown". Empty when it was sent, or is on its
	// way. WriteReason says why.
	Write, WriteReason string
}

// maxApprovalLines bounds the approvals the report names outside its fence: GitHub takes 65,536
// characters, and the fenced text has its own bound.
const maxApprovalLines = 20

var commentArgs = tools.MustSchema(`{
  "type": "object", "additionalProperties": false, "required": ["repo", "commit", "outcome"],
  "properties": {
    "repo": {"type": "string", "minLength": 1},
    "commit": {"type": "string", "pattern": "` + tools.CommitPattern + `"},
    "report_ref": {"anyOf": [{"type": "string", "pattern": "` + tools.BlobPattern + `"}, {"type": "null"}]},
    "outcome": {"type": "string", "maxLength": 100000},
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
	// So are the decisions on the run's writes, a rejection's reason with them.
	approvals, err := t.Effects.RunApprovals(ctx, req.WorkflowID)
	if err != nil {
		return nil, err
	}
	commit := tools.String(req.Args, "commit")
	m := marker(req.EffectID)
	return json.Marshal(commentPayload{
		Owner: repo.Owner, Repo: repo.Name, Commit: commit, Marker: m,
		Body: render(commit, tools.String(req.Args, "outcome"), rep, pr, approvals, m),
	})
}

// maxText keeps a comment under GitHub's 65,536 characters, with the fences: no run of
// backticks in the text is longer than maxBackticks, so neither fence is either.
const (
	maxText      = 60000
	maxBackticks = 16
	maxOutcome   = 2000
)

// render builds the comment from what agentd checked itself (the commit, the severities, the
// pull request's number, how each approval ended), and puts everything a model, the workflow or a
// reviewer wrote into one code fence, longer than any run of backticks inside it: GitHub renders
// neither links nor mentions there.
func render(commit, outcome string, rep *report, pr int64, approvals []Approval, marker string) string {
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
	for i, a := range approvals {
		if i == maxApprovalLines {
			fmt.Fprintf(&b, "And %d approvals more\n\n", len(approvals)-i)
			break
		}
		status := strings.ToLower(a.Status)
		if a.Write != "" {
			status += ", " + a.Write
		}
		fmt.Fprintf(&b, "Approval of %s: %s\n\n", a.Tool, status)
	}
	// The workflow's outcome may carry a long error: cut it rather than refuse the report.
	if len(outcome) > maxOutcome {
		outcome = strings.ToValidUTF8(outcome[:maxOutcome], "") + " [truncated]"
	}
	var text strings.Builder
	text.WriteString("Outcome: " + outcome)
	if len(approvals) > 0 {
		text.WriteString("\n\nApprovals:")
		for _, a := range approvals {
			fmt.Fprintf(&text, "\n- %s, approval %s: %s", a.Tool, a.ID, a.Status)
			switch a.Status {
			case "PENDING":
				fmt.Fprintf(&text, ", until %s", a.ExpiresAt)
			case "EXPIRED":
				fmt.Fprintf(&text, " at %s: %s", a.DecidedAt, a.Reason)
			case "REJECTED":
				fmt.Fprintf(&text, " by %s at %s. Reason: %s", a.DecidedBy, a.DecidedAt, a.Reason)
			case "EDITED", "SUPERSEDED":
				fmt.Fprintf(&text, " (edited by %s at %s)", a.DecidedBy, a.DecidedAt)
			default:
				fmt.Fprintf(&text, " by %s at %s", a.DecidedBy, a.DecidedAt)
			}
			if a.Write != "" {
				fmt.Fprintf(&text, "; %s: %s", a.Write, a.WriteReason)
			}
		}
	}
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
