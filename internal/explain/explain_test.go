package explain

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/budget"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// The pipeline up to the report, every step returning ok: the audit proposes a patch, which is
// checked and opened as a pull request.
func (w *world) pipeline() {
	w.patched()
	w.ok("github.open_draft_pr")
}

// patched is the pipeline up to the pull request: the audit proposes a patch, which is checked.
func (w *world) patched() {
	w.ok("git.show_diff")
	w.returns("audit_v1", statusOK, nil, map[string]any{"report_ref": "sha256:" + strings.Repeat("a", 64),
		"patch_ref": "sha256:" + strings.Repeat("b", 64)})
	w.ok("git.apply_check")
}

// want is what an explanation must say.
type want struct {
	verdict, kind, step, activity string
	attempt                       int32
	cause                         []string // in the cause, each
	incidents                     []string // kind@step of each incident, in order
	seqs                          []int64  // audit records the evidence must point at
	events                        []int64  // history events it must point at
	also                          func(e *agentosv1.Explanation)
}

func check(t *testing.T, e *agentosv1.Explanation, w want) {
	t.Helper()
	if e.GetVerdict() != w.verdict || e.GetKind() != w.kind || e.GetStep() != w.step || e.GetActivityId() != w.activity ||
		e.GetAttempt() != w.attempt {
		t.Fatalf("got verdict %q kind %q step %q activity %q attempt %d; want %q %q %q %q %d\ncause: %s",
			e.GetVerdict(), e.GetKind(), e.GetStep(), e.GetActivityId(), e.GetAttempt(),
			w.verdict, w.kind, w.step, w.activity, w.attempt, e.GetCause())
	}
	for _, c := range w.cause {
		if !strings.Contains(e.GetCause(), c) {
			t.Errorf("the cause does not say %q: %s", c, e.GetCause())
		}
	}
	var got []string
	for _, inc := range e.GetIncidents() {
		got = append(got, inc.GetKind()+"@"+inc.GetStep())
	}
	if w.incidents != nil && !slices.Equal(got, w.incidents) {
		t.Errorf("incidents %v, want %v: %v", got, w.incidents, e.GetIncidents())
	}
	for _, seq := range w.seqs {
		if !slices.ContainsFunc(e.GetSources(), func(s *agentosv1.Evidence) bool { return s.Source == "audit" && s.Seq == seq }) {
			t.Errorf("no evidence of audit seq %d: %v", seq, e.GetSources())
		}
	}
	for _, id := range w.events {
		if !slices.ContainsFunc(e.GetSources(), func(s *agentosv1.Evidence) bool { return s.Source == "temporal" && s.EventId == id }) {
			t.Errorf("no evidence of history event %d: %v", id, e.GetSources())
		}
	}
}

func TestVerdicts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		world func(w *world) want
	}{{
		name: "every call returned ok: the run ended well",
		world: func(w *world) want {
			w.pipeline()
			r := w.ok("github.comment_commit")
			w.completed("ok")
			return want{verdict: VerdictOK, cause: []string{"post_report went out"}, incidents: []string{},
				seqs: []int64{w.l.Records[len(w.l.Records)-1].Seq}, events: []int64{r.scheduled + 2}}
		},
	}, {
		name: "the model refused: the first call that changed the run's course is named",
		world: func(w *world) want {
			w.ok("git.show_diff")
			c := w.returns("audit_v1", statusRefused, nil, nil)
			w.l.Requests = []ModelRequest{{ModelCall: db.ModelCall{ActivityID: c.id, Attempt: 1, RequestSeq: 1,
				Provider: "anthropic", Model: "claude-opus-5-5"}, Status: statusRefused, Category: "cyber", Explanation: "no"}}
			seq := w.l.Records[len(w.l.Records)-1].Seq
			w.ok("github.comment_commit")
			w.completed("ok, honestly") // the workflow's word counts for nothing
			return want{verdict: VerdictStepFailed, kind: KindModelRefused, step: "analyze", activity: c.id, attempt: 1,
				cause: []string{"declined", "anthropic/claude-opus-5-5", `"cyber"`, "post_report went out"},
				seqs:  []int64{seq}, events: []int64{c.scheduled + 2}}.with(func(e *agentosv1.Explanation) {
				if !slices.ContainsFunc(e.GetSources(), func(s *agentosv1.Evidence) bool {
					return s.Source == "ledger" && s.Table == "model_calls" && s.Key == c.id+"/1/1"
				}) {
					t.Errorf("no ledger evidence: %v", e.GetSources())
				}
			})
		},
	}, {
		name: "answers that broke the schema twice",
		world: func(w *world) want {
			w.ok("git.show_diff")
			c := w.returns("audit_v1", statusInvalidOutput, map[string]any{"reason": "missing summary"}, nil)
			w.ok("github.comment_commit")
			w.completed("invalid_output")
			return want{verdict: VerdictStepFailed, kind: KindModelInvalidOutput, step: "analyze", activity: c.id, attempt: 1,
				cause: []string{"output schema", "missing summary"}}
		},
	}, {
		name: "the policy denied the model's provider",
		world: func(w *world) want {
			w.ok("git.show_diff")
			c := w.returns("audit_v1", statusDenied, map[string]any{"policy": map[string]any{"decision": "deny",
				"reason": `provider "fake" not allowed for audit_v1`}, "reason": `provider "fake" not allowed for audit_v1`}, nil)
			w.ok("github.comment_commit")
			w.completed("denied")
			return want{verdict: VerdictStepFailed, kind: KindPolicyDenied, step: "analyze", activity: c.id, attempt: 1,
				cause: []string{"the policy denied analyze", `provider \"fake\" not allowed`}}
		},
	}, {
		name: "the tool refused the patch as it stood",
		world: func(w *world) want {
			w.ok("git.show_diff")
			w.ok("audit_v1")
			c := w.returns("git.apply_check", statusDenied, map[string]any{"reason": "the patch does not apply",
				"refused_by": audit.RefusedByTool}, nil)
			w.ok("github.comment_commit")
			w.completed("patch rejected")
			return want{verdict: VerdictStepFailed, kind: KindToolDenied, step: "validate_patch", activity: c.id, attempt: 1,
				cause: []string{"the patch does not apply"}}
		},
	}, {
		name: "the loop guard refused a call",
		world: func(w *world) want {
			c := w.returns("git.show_diff", statusDenied, map[string]any{"reason": "loop guard: more than 100 calls in one run",
				"refused_by": audit.RefusedByLoopGuard}, nil)
			w.ok("github.comment_commit")
			w.completed("denied")
			return want{verdict: VerdictStepFailed, kind: KindLoopGuard, step: "fetch_diff", activity: c.id, attempt: 1,
				cause: []string{"loop guard"}}
		},
	}, {
		name: "a tool error, for good",
		world: func(w *world) want {
			c := w.schedule("git.show_diff")
			seq := c.failsForGood(1, "tool git.show_diff: bad object", "ToolFailed", nil)
			w.ok("github.comment_commit")
			w.completed("failed: ...")
			return want{verdict: VerdictStepFailed, kind: KindToolError, step: "fetch_diff", activity: c.id, attempt: 1,
				cause: []string{"bad object"}, seqs: []int64{seq}, events: []int64{c.scheduled + 2}, incidents: []string{}}
		},
	}, {
		name: "a model call that failed at every attempt",
		world: func(w *world) want {
			w.ok("git.show_diff")
			c := w.schedule("audit_v1")
			for k := int32(1); k < 5; k++ {
				c.fails(k, "anthropic: 529 overloaded")
			}
			c.failsForGood(5, "anthropic: 529 overloaded", "", appFailure("anthropic: 529 overloaded", "", false))
			w.ok("github.comment_commit")
			w.completed("failed: ...")
			return want{verdict: VerdictStepFailed, kind: KindModelError, step: "analyze", activity: c.id, attempt: 5,
				cause: []string{"after 5 attempts", "529"}, incidents: []string{"model_error@analyze", "model_error@analyze",
					"model_error@analyze", "model_error@analyze"}}
		},
	}, {
		name: "a person rejected the pull request: not a failure",
		world: func(w *world) want {
			w.ok("git.show_diff")
			w.ok("audit_v1")
			w.ok("git.apply_check")
			w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
			d := w.rec("approval.decided", "operator:bob", map[string]any{"approval_id": "ap-1", "decision": "reject",
				"decided_by": "robert", "reason": "not this patch"})
			c := w.returns("github.open_draft_pr", statusRejected, map[string]any{"reason": "not this patch"}, nil)
			w.l.Approvals = []db.Approval{{ApprovalID: "ap-1", Tool: "github.open_draft_pr", Status: "REJECTED",
				DecidedBy: valid("robert"), DecidedAt: valid("2026-10-22T10:01:00Z"), Reason: valid("not this patch")}}
			w.ok("github.comment_commit")
			w.completed("rejected")
			return want{verdict: VerdictNotApproved, kind: KindApprovalRejected, step: "open_pr", activity: c.id, attempt: 1,
				cause: []string{"robert (as operator:bob) rejected", `"not this patch"`}, seqs: []int64{d}}.with(func(e *agentosv1.Explanation) {
				if !slices.ContainsFunc(e.GetSources(), func(s *agentosv1.Evidence) bool { return s.Table == "approvals" && s.Key == "ap-1" }) {
					t.Errorf("no ledger evidence: %v", e.GetSources())
				}
			})
		},
	}, {
		name: "nobody approved in time",
		world: func(w *world) want {
			w.ok("git.show_diff")
			w.ok("audit_v1")
			w.ok("git.apply_check")
			w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
			c := w.returns("github.open_draft_pr", statusExpired, nil, nil)
			w.l.Approvals = []db.Approval{{ApprovalID: "ap-1", Tool: "github.open_draft_pr", Status: "EXPIRED",
				ExpiresAt: "2026-10-23T10:00:00.000000000Z", DecidedBy: valid("agentd")}}
			w.ok("github.comment_commit")
			w.completed("expired")
			return want{verdict: VerdictNotApproved, kind: KindApprovalExpired, step: "open_pr", activity: c.id, attempt: 1,
				cause: []string{"expired at 2026-10-23T10:00:00"}}
		},
	}, {
		name: "waits a person resolved, and an edit, do not count",
		world: func(w *world) want {
			w.ok("git.show_diff")
			w.ok("audit_v1")
			w.ok("git.apply_check")
			w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
			w.returns("github.open_draft_pr", statusEdited, nil, nil)
			w.ok("git.apply_check")
			w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
			w.ok("github.open_draft_pr")
			w.ok("github.comment_commit")
			w.completed("ok")
			return want{verdict: VerdictOK, incidents: []string{}}
		},
	}, {
		name: "a budget pause someone extended: ok, with the pause apart",
		world: func(w *world) want {
			w.ok("git.show_diff")
			w.returns("audit_v1", statusBudgetExhausted, nil, map[string]any{"needed_micros": 20480, "available_micros": 1000})
			x := w.rec("budget.extended", "operator:carol", map[string]any{"amount_micros": 1_000_000})
			w.ok("audit_v1")
			w.ok("github.comment_commit")
			w.completed("ok")
			return want{verdict: VerdictOK, incidents: []string{"budget_exhausted@analyze"}, cause: []string{"ended well"}}.with(func(e *agentosv1.Explanation) {
				if inc := e.GetIncidents()[0]; !strings.Contains(inc.GetCause(), "needed $0.02048") ||
					!strings.Contains(inc.GetCause(), "carol extended it by $1.00") ||
					!slices.ContainsFunc(inc.GetEvidence(), func(s *agentosv1.Evidence) bool { return s.Seq == x }) {
					t.Errorf("incident: %v", inc)
				}
			})
		},
	}, {
		name: "a run that returned without its report",
		world: func(w *world) want {
			w.ok("git.show_diff")
			c := w.returns("github.comment_commit", statusDenied, map[string]any{"reason": "the run's report: no"}, nil)
			w.completed("ok")
			return want{verdict: VerdictStepFailed, kind: KindReportNotPosted, step: "post_report", activity: c.id, attempt: 1,
				cause: []string{"ended without its report"}}
		},
	}, {
		name: "a wait the run did not go on from: it changed the run's course",
		world: func(w *world) want {
			w.ok("git.show_diff")
			w.ok("audit_v1")
			w.ok("git.apply_check")
			c := w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
			w.ok("github.comment_commit")
			w.completed("gave up")
			return want{verdict: VerdictStepFailed, kind: KindApprovalPending, step: "open_pr", activity: c.id, attempt: 1}
		},
	}, {
		name: "agentd's record says what a call returned, not the history's result",
		world: func(w *world) want {
			c := w.schedule("audit_v1")
			seq := c.record(1, statusDenied, map[string]any{"reason": "provider not allowed"})
			c.end(1, nil, func(e *historypb.HistoryEvent, started int64) {
				b, _ := json.Marshal(map[string]any{"_audit": audit.Head{Seq: seq}})
				res, _ := converter.GetDefaultDataConverter().ToPayloads(&agentosv1.Result{Status: statusOK, ValueJson: string(b)})
				e.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
				e.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
					ScheduledEventId: c.scheduled, StartedEventId: started, Result: res}}
			})
			w.ok("github.comment_commit")
			w.completed("ok")
			return want{verdict: VerdictStepFailed, kind: KindDenied, step: "analyze", activity: c.id, attempt: 1, seqs: []int64{seq}}
		},
	}, {
		name: "a call whose input the history does not name: agentd's record does",
		world: func(w *world) want {
			c := w.schedule("git.show_diff")
			w.events[c.scheduled-1].GetActivityTaskScheduledEventAttributes().Input = nil
			c.completes(1, statusDenied, map[string]any{"reason": "no", "refused_by": audit.RefusedByTool}, nil, nil)
			w.ok("github.comment_commit")
			w.completed("ok")
			return want{verdict: VerdictStepFailed, kind: KindToolDenied, step: "fetch_diff", activity: c.id, attempt: 1}
		},
	}, {
		name: "the run ran past its execution timeout",
		world: func(w *world) want {
			c := w.ok("git.show_diff")
			w.status, w.state = enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, "FAILED"
			w.event(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT, nil)
			return want{verdict: VerdictFailed, kind: KindWorkflowError, step: "fetch_diff", activity: c.id, attempt: 1,
				cause: []string{"execution timeout"}}
		},
	}, {
		name: "the report did not go out: the run failed",
		world: func(w *world) want {
			w.ok("git.show_diff")
			w.returns("audit_v1", statusRefused, nil, nil)
			c := w.returns("github.comment_commit", statusDenied, map[string]any{"policy": map[string]any{"decision": "deny",
				"reason": "tool not in policy"}}, nil)
			w.failed("report not posted: denied", "")
			return want{verdict: VerdictFailed, kind: KindReportNotPosted, step: "post_report", activity: c.id, attempt: 1,
				cause:     []string{"the report did not go out", "tool not in policy"},
				incidents: []string{"model_refused@analyze"}}
		},
	}, {
		name: "the workflow raised after its report went out",
		world: func(w *world) want {
			c := w.ok("git.show_diff")
			r := w.ok("github.comment_commit")
			w.failed("the workflow raised KeyError: 'blob'", "KeyError")
			return want{verdict: VerdictFailed, kind: KindWorkflowError, step: "fetch_diff", activity: c.id, attempt: 1,
				cause:  []string{"own code failed after fetch_diff returned ok", "post_report went out"},
				events: []int64{r.scheduled + 2}}.with(func(e *agentosv1.Explanation) {
				if e.GetClaim() != "the workflow raised KeyError: 'blob'" {
					t.Errorf("claim %q", e.GetClaim())
				}
			})
		},
	}, {
		name: "the workflow raised without reporting",
		world: func(w *world) want {
			w.ok("git.show_diff")
			c := w.ok("audit_v1")
			w.failed("boom", "RuntimeError")
			return want{verdict: VerdictFailed, kind: KindWorkflowError, step: "analyze", activity: c.id, attempt: 1,
				cause: []string{"raised without reporting", "after analyze returned ok"}}
		},
	}, {
		name: "a person cancelled the run",
		world: func(w *world) want {
			w.ok("git.show_diff")
			w.ok("audit_v1")
			w.ok("git.apply_check")
			c := w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
			seq := w.cancel("operator:dave")
			w.canceled()
			w.state = "CANCELLED"
			return want{verdict: VerdictCancelled, kind: KindCancelled, step: "open_pr", activity: c.id, attempt: 1,
				cause: []string{"dave cancelled the run at"}, seqs: []int64{seq}}
		},
	}, {
		name: "a cancelled run that did not stop: agentd terminated it",
		world: func(w *world) want {
			c := w.ok("git.show_diff")
			w.cancel("operator:dave")
			seq := w.rec("run.terminated", audit.Agentd, map[string]any{"why": "cancel not honoured within 1m0s"})
			w.terminated("cancel not honoured within 1m0s", "agentd@host")
			return want{verdict: VerdictCancelled, kind: KindCancelled, step: "fetch_diff", activity: c.id, attempt: 1,
				cause: []string{"dave cancelled", "agentd terminated it (cancel not honoured"}, seqs: []int64{seq}}
		},
	}, {
		name: "terminated by agentd",
		world: func(w *world) want {
			seq := w.rec("run.terminated", audit.Agentd, map[string]any{"why": "agentd could not record the run"})
			w.terminated("agentd could not record the run", "agentd@host")
			return want{verdict: VerdictTerminated, kind: KindTerminated, cause: []string{"agentd terminated the run"},
				seqs: []int64{seq}}
		},
	}, {
		name: "terminated by agentd, killed before it recorded the termination done",
		world: func(w *world) want {
			seq := w.rec("run.terminating", audit.Agentd, map[string]any{"why": "cancel not honoured within 1m0s",
				"temporal_run_id": "exec-1"})
			w.terminated("cancel not honoured within 1m0s", "agentd@host")
			return want{verdict: VerdictTerminated, kind: KindTerminated, cause: []string{"agentd terminated the run: cancel not"},
				seqs: []int64{seq}}
		},
	}, {
		name: "another execution's termination is not the run's",
		world: func(w *world) want {
			w.rec("run.terminating", audit.Agentd, map[string]any{"why": "old", "temporal_run_id": "exec-0"})
			w.terminated("just because", "mallory@laptop")
			return want{verdict: VerdictTerminated, kind: KindTerminated, cause: []string{"not by agentd"}}
		},
	}, {
		name: "terminated by someone else: their reason is a claim",
		world: func(w *world) want {
			c := w.ok("git.show_diff")
			w.terminated("just because", "mallory@laptop")
			return want{verdict: VerdictTerminated, kind: KindTerminated, step: "fetch_diff", activity: c.id, attempt: 1,
				cause: []string{"not by agentd"}}.with(func(e *agentosv1.Explanation) {
				if !strings.Contains(e.GetClaim(), "mallory@laptop") || strings.Contains(e.GetCause(), "just because") {
					t.Errorf("claim %q, cause %q", e.GetClaim(), e.GetCause())
				}
			})
		},
	}, {
		name: "waiting for an approval: next says how",
		world: func(w *world) want {
			w.ok("git.show_diff")
			w.ok("audit_v1")
			w.ok("git.apply_check")
			c := w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
			w.l.Approvals = []db.Approval{{ApprovalID: "ap-9", Tool: "github.open_draft_pr", Status: "PENDING",
				PayloadSha256: "abc", ExpiresAt: "2099-01-01T00:00:00.000000000Z"}}
			w.state = "WAITING_APPROVAL"
			return want{verdict: VerdictWaiting, kind: KindApprovalPending, step: "open_pr", activity: c.id, attempt: 1}.with(
				func(e *agentosv1.Explanation) {
					if !strings.Contains(e.GetNext(), "agentctl approvals show ap-9") || !strings.Contains(e.GetNext(), "--payload-sha256 abc") {
						t.Errorf("next %q", e.GetNext())
					}
				})
		},
	}, {
		name: "an approval past its time is not waited for: agentd expires it any moment",
		world: func(w *world) want {
			c := w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
			w.l.Approvals = []db.Approval{{ApprovalID: "ap-9", Tool: "github.open_draft_pr", Status: "PENDING",
				ExpiresAt: "2000-01-01T00:00:00.000000000Z"}}
			w.state = "WAITING_APPROVAL" // the Worker's word
			return want{verdict: VerdictRunning, step: "open_pr", activity: c.id, attempt: 1}
		},
	}, {
		name: "the Worker says it waits for an approval agentd has no record of",
		world: func(w *world) want {
			c := w.ok("git.show_diff")
			w.state = "WAITING_APPROVAL"
			return want{verdict: VerdictRunning, step: "fetch_diff", activity: c.id, attempt: 1}
		},
	}, {
		name: "waiting for a reconciliation",
		world: func(w *world) want {
			c := w.returns("github.open_draft_pr", statusNeedsReconcile, nil, nil)
			w.l.Effects = []db.Effect{{EffectID: "ef-1", Tool: "github.open_draft_pr", Status: "UNKNOWN",
				Reason: valid("GitHub did not answer")}}
			return want{verdict: VerdictWaiting, kind: KindReconcilePending, step: "open_pr", activity: c.id, attempt: 1,
				cause: []string{"GitHub did not answer"}}.with(func(e *agentosv1.Explanation) {
				if !strings.Contains(e.GetNext(), "agentctl reconcile show ef-1") {
					t.Errorf("next %q", e.GetNext())
				}
			})
		},
	}, {
		name: "waiting for budget: next names the shortfall",
		world: func(w *world) want {
			w.ok("git.show_diff")
			c := w.returns("audit_v1", statusBudgetExhausted, nil, map[string]any{"needed_micros": 20480, "available_micros": 1000})
			return want{verdict: VerdictWaiting, kind: KindBudgetExhausted, step: "analyze", activity: c.id, attempt: 1,
				cause: []string{"needed $0.02048", "$0.001 was left"}}.with(func(e *agentosv1.Explanation) {
				if e.GetNext() != "agentctl budget extend run-1 --usd 0.04" {
					t.Errorf("next %q", e.GetNext())
				}
			})
		},
	}, {
		name: "waiting for budget someone has extended: nothing to do",
		world: func(w *world) want {
			c := w.returns("audit_v1", statusBudgetExhausted, nil, map[string]any{"needed_micros": 20480, "available_micros": 1000})
			w.rec("budget.extended", "operator:carol", map[string]any{"amount_micros": 1_000_000})
			return want{verdict: VerdictWaiting, kind: KindBudgetExhausted, step: "analyze", activity: c.id, attempt: 1,
				cause: []string{"carol extended it"}}.with(func(e *agentosv1.Explanation) {
				if e.GetNext() != "" {
					t.Errorf("next %q", e.GetNext())
				}
			})
		},
	}, {
		name: "workflow tasks that keep failing: stuck",
		world: func(w *world) want {
			c := w.ok("git.show_diff")
			ev := w.taskFailed("a bug", "RuntimeError")
			w.pending(&workflowpb.PendingWorkflowTaskInfo{Attempt: 4, State: enumspb.PENDING_WORKFLOW_TASK_STATE_STARTED,
				OriginalScheduledTime: timestamppb.New(w.tick())})
			return want{verdict: VerdictStuck, kind: KindWorkflowTaskFailed, step: "fetch_diff", activity: c.id, attempt: 1,
				cause: []string{"attempt 4"}, events: []int64{ev}, incidents: []string{"workflow_task_failed@fetch_diff"}}.with(
				func(e *agentosv1.Explanation) {
					if e.GetClaim() != "RuntimeError: a bug" {
						t.Errorf("claim %q", e.GetClaim())
					}
				})
		},
	}, {
		name: "workflow tasks that keep timing out: stuck",
		world: func(w *world) want {
			w.taskTimedOut()
			ev := w.taskTimedOut()
			w.pending(&workflowpb.PendingWorkflowTaskInfo{Attempt: 3, State: enumspb.PENDING_WORKFLOW_TASK_STATE_STARTED,
				OriginalScheduledTime: timestamppb.New(w.tick())})
			return want{verdict: VerdictStuck, kind: KindWorkerStopped, cause: []string{"timing out"}, events: []int64{ev}}
		},
	}, {
		name: "a workflow task no Worker takes: stuck",
		world: func(w *world) want {
			c := w.ok("git.show_diff")
			w.pending(&workflowpb.PendingWorkflowTaskInfo{Attempt: 1, State: enumspb.PENDING_WORKFLOW_TASK_STATE_SCHEDULED,
				OriginalScheduledTime: timestamppb.New(w.clock.Add(-5 * time.Minute))})
			return want{verdict: VerdictStuck, kind: KindNoWorker, step: "fetch_diff", activity: c.id, attempt: 1,
				cause: []string{"no Worker"}}
		},
	}, {
		name: "a workflow task a Worker will take in a moment: running",
		world: func(w *world) want {
			c := w.ok("git.show_diff")
			w.pending(&workflowpb.PendingWorkflowTaskInfo{Attempt: 1, State: enumspb.PENDING_WORKFLOW_TASK_STATE_SCHEDULED,
				OriginalScheduledTime: timestamppb.New(w.clock)})
			return want{verdict: VerdictRunning, step: "fetch_diff", activity: c.id, attempt: 1}
		},
	}, {
		name: "one workflow task that failed, and a call that failed once: not stuck yet",
		world: func(w *world) want {
			w.ok("git.show_diff")
			c := w.schedule("audit_v1")
			c.fails(1, "anthropic: 529 overloaded")
			w.taskFailed("a bug", "RuntimeError")
			w.pending(&workflowpb.PendingWorkflowTaskInfo{Attempt: 2, State: enumspb.PENDING_WORKFLOW_TASK_STATE_STARTED,
				OriginalScheduledTime: timestamppb.New(w.tick())}, &workflowpb.PendingActivityInfo{ActivityId: c.id, Attempt: 2})
			return want{verdict: VerdictRunning, step: "analyze", activity: c.id}
		},
	}, {
		name: "a call whose attempts keep failing: stuck",
		world: func(w *world) want {
			c := w.schedule("git.show_diff")
			c.serverFails(1)
			seq := c.serverFails(2)
			w.pending(nil, &workflowpb.PendingActivityInfo{ActivityId: c.id, Attempt: 3})
			return want{verdict: VerdictStuck, kind: KindToolServerFailed, step: "fetch_diff", activity: c.id, attempt: 3,
				cause: []string{"attempt 3", "2 that failed"}, seqs: []int64{seq},
				incidents: []string{"tool_server_failed@fetch_diff", "tool_server_failed@fetch_diff"}}
		},
	}, {
		name: "a call under way: running at its step",
		world: func(w *world) want {
			w.ok("git.show_diff")
			c := w.schedule("audit_v1")
			w.pending(nil, &workflowpb.PendingActivityInfo{ActivityId: c.id, Attempt: 1})
			return want{verdict: VerdictRunning, step: "analyze", activity: c.id, cause: []string{"attempt 1 is under way"}}
		},
	}, {
		name: "the Worker stopped: a workflow task timed out, and the run went on",
		world: func(w *world) want {
			w.ok("git.show_diff")
			w.ok("audit_v1")
			ev := w.taskTimedOut()
			w.ok("git.apply_check")
			w.ok("github.open_draft_pr")
			w.ok("github.comment_commit")
			w.completed("ok")
			return want{verdict: VerdictOK, incidents: []string{"worker_stopped@analyze"}}.with(func(e *agentosv1.Explanation) {
				inc := e.GetIncidents()[0]
				if !strings.Contains(inc.GetCause(), "stopped or hung") || inc.GetEvidence()[0].GetEventId() != ev {
					t.Errorf("incident %v", inc)
				}
			})
		},
	}, {
		name: "agentd stopped during an attempt: no record of it, and agentd started again before the next",
		world: func(w *world) want {
			w.ok("git.show_diff")
			c := w.schedule("audit_v1")
			start := w.start("agentd.started", audit.Agentd, nil)
			c.completes(2, statusOK, nil, nil, timeoutFailure(enumspb.TIMEOUT_TYPE_HEARTBEAT))
			w.ok("git.apply_check")
			w.ok("github.open_draft_pr")
			w.ok("github.comment_commit")
			w.completed("ok")
			return want{verdict: VerdictOK, incidents: []string{"agentd_stopped@analyze"}}.with(func(e *agentosv1.Explanation) {
				inc := e.GetIncidents()[0]
				if inc.GetAttempt() != 1 || !strings.Contains(inc.GetCause(), "Heartbeat") ||
					!slices.ContainsFunc(inc.GetEvidence(), func(s *agentosv1.Evidence) bool { return s.Seq == start }) {
					t.Errorf("incident %v", inc)
				}
			})
		},
	}, {
		name: "agentd stopped during an attempt its call.started record names",
		world: func(w *world) want {
			w.ok("git.show_diff")
			c := w.schedule("audit_v1")
			began := w.rec("call.started", audit.Run, map[string]any{"activity_id": c.id, "attempt": 1, "kind": "model",
				"name": "audit_v1"})
			start := w.start("agentd.started", audit.Agentd, nil)
			c.completes(2, statusOK, nil, nil, timeoutFailure(enumspb.TIMEOUT_TYPE_HEARTBEAT))
			w.ok("github.comment_commit")
			w.completed("ok")
			return want{verdict: VerdictOK, incidents: []string{"agentd_stopped@analyze"}}.with(func(e *agentosv1.Explanation) {
				inc := e.GetIncidents()[0]
				if !strings.Contains(inc.GetCause(), "began and never ended") ||
					!slices.ContainsFunc(inc.GetEvidence(), func(s *agentosv1.Evidence) bool { return s.Seq == began }) ||
					!slices.ContainsFunc(inc.GetEvidence(), func(s *agentosv1.Evidence) bool { return s.Seq == start }) {
					t.Errorf("incident %v", inc)
				}
			})
		},
	}, {
		name: "an attempt that timed out with agentd up: a timeout, not a crash",
		world: func(w *world) want {
			w.ok("git.show_diff")
			c := w.schedule("audit_v1")
			c.completes(2, statusOK, nil, nil, timeoutFailure(enumspb.TIMEOUT_TYPE_START_TO_CLOSE))
			w.ok("github.comment_commit")
			w.completed("ok")
			return want{verdict: VerdictOK, incidents: []string{"activity_timeout@analyze"}}
		},
	}, {
		name: "an attempt's result that never reached Temporal, agentd having stopped",
		world: func(w *world) want {
			w.ok("git.show_diff")
			c := w.schedule("audit_v1")
			c.record(1, statusOK, nil)
			w.start("agentd.started", audit.Agentd, nil)
			c.completes(2, statusOK, nil, nil, timeoutFailure(enumspb.TIMEOUT_TYPE_HEARTBEAT))
			w.ok("github.comment_commit")
			w.completed("ok")
			return want{verdict: VerdictOK, incidents: []string{"agentd_stopped@analyze"}}
		},
	}, {
		name: "an answer that broke the schema, asked for again",
		world: func(w *world) want {
			w.ok("git.show_diff")
			c := w.ok("audit_v1")
			w.l.Requests = []ModelRequest{
				{ModelCall: db.ModelCall{ActivityID: c.id, Attempt: 1, RequestSeq: 1, Provider: "openai", Model: "gpt-6.1-sol"}, Status: statusInvalidOutput},
				{ModelCall: db.ModelCall{ActivityID: c.id, Attempt: 1, RequestSeq: 2, Provider: "openai", Model: "gpt-6.1-sol"}, Status: statusOK}}
			w.ok("github.comment_commit")
			w.completed("ok")
			return want{verdict: VerdictOK, incidents: []string{"model_invalid_output@analyze"}}
		},
	}, {
		name: "the git tool server failed, and the next attempt ran it again",
		world: func(w *world) want {
			c := w.schedule("git.show_diff")
			c.serverFails(1)
			c.completes(2, statusOK, nil, nil, appFailure(serverError, "", false))
			w.ok("audit_v1")
			w.ok("github.comment_commit")
			w.completed("ok")
			return want{verdict: VerdictOK, incidents: []string{"tool_server_failed@fetch_diff"}}
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			wnt := tc.world(w)
			e := w.explain()
			check(t, e, wnt)
			if wnt.also != nil {
				wnt.also(e)
			}
		})
	}
}

// with is want with a further check of the explanation.
func (w want) with(also func(e *agentosv1.Explanation)) want {
	w.also = also
	return w
}

func valid(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }

var _ = fmt.Sprint
var _ = budget.Totals{}
