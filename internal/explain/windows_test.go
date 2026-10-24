package explain

import (
	"database/sql"
	"slices"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// kinds is the kind@step:attempt of each incident.
func kinds(e *agentosv1.Explanation) []string {
	var out []string
	for _, inc := range e.GetIncidents() {
		out = append(out, inc.GetKind()+"@"+inc.GetStep()+":"+string(rune('0'+inc.GetAttempt())))
	}
	return out
}

// Each attempt before the one that ended a call, or the one under way, is an incident, its kind
// inferred from what it left and when: its record, its call.started, agentd's starts, and the times
// between them.
func TestTheWindowsOfAnAttempt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		world func(w *world)
		want  []string
		says  string
	}{{
		name: "an attempt of a call under way failed: the attempt under way says so",
		world: func(w *world) {
			c := w.schedule("audit_v1")
			c.fails(1, "anthropic: 529 overloaded")
			w.pending(nil, &workflowpb.PendingActivityInfo{ActivityId: c.id, Attempt: 2})
		},
		want: []string{"model_error@analyze:1"}, says: "529",
	}, {
		name: "agentd started before an attempt returned ok: no crash of the attempt",
		world: func(w *world) {
			c := w.schedule("audit_v1")
			w.start("agentd.started", audit.Agentd, nil) // before the attempt ran
			c.record(1, statusOK, map[string]any{"result_sha256": resultSHA(w.t, nil)})
			c.completes(2, statusOK, nil, nil, timeoutFailure(enumspb.TIMEOUT_TYPE_HEARTBEAT))
		},
		want: []string{"activity_timeout@analyze:1"}, says: "never reached Temporal",
	}, {
		name: "agentd started before the attempt before: no crash of the one without a record",
		world: func(w *world) {
			c := w.schedule("audit_v1")
			w.start("agentd.started", audit.Agentd, nil)
			c.fails(1, "anthropic: 529 overloaded")
			// attempt 2 leaves nothing
			c.completes(3, statusOK, nil, nil, timeoutFailure(enumspb.TIMEOUT_TYPE_START_TO_CLOSE))
		},
		want: []string{"model_error@analyze:1", "activity_timeout@analyze:2"}, says: "timed out",
	}, {
		name: "agentd started after the next attempt began: the attempt that began and left nothing timed out",
		world: func(w *world) {
			c := w.schedule("audit_v1")
			w.rec("call.started", audit.Run, map[string]any{"activity_id": c.id, "attempt": 1, "kind": "model",
				"name": "audit_v1"})
			began := w.clock.Add(time.Second)
			w.rec("call.started", audit.Run, map[string]any{"activity_id": c.id, "attempt": 2, "kind": "model",
				"name": "audit_v1"})
			w.start("agentd.started", audit.Agentd, nil) // during attempt 2, after it began
			c.completes(2, statusOK, nil, nil, timeoutFailure(enumspb.TIMEOUT_TYPE_HEARTBEAT))
			// Temporal's started event has the time the attempt began, written when it ends.
			w.events[c.scheduled].EventTime = timestamppb.New(began)
		},
		want: []string{"activity_timeout@analyze:1"}, says: "began and left no record of its end",
	}, {
		name: "an attempt that failed after the next began: Temporal had given up on it",
		world: func(w *world) {
			c := w.schedule("git.show_diff")
			c.completes(2, statusOK, nil, nil, timeoutFailure(enumspb.TIMEOUT_TYPE_HEARTBEAT))
			c.fails(1, "context canceled")
		},
		want: []string{"activity_timeout@fetch_diff:1"}, says: "it ended later, with an error",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			tc.world(w)
			e := w.explain()
			if got := kinds(e); !slices.Equal(got, tc.want) {
				t.Fatalf("incidents %v, want %v: %v", got, tc.want, e.GetIncidents())
			}
			if inc := e.GetIncidents()[len(e.GetIncidents())-1]; !strings.Contains(inc.GetCause(), tc.says) {
				t.Fatalf("%v", inc)
			}
		})
	}
}

// Incidents are listed oldest first, whatever found them.
func TestIncidentsAreInTheOrderTheyHappened(t *testing.T) {
	w := newWorld(t)
	w.taskTimedOut() // the first workflow task: before any call
	c := w.schedule("git.show_diff")
	c.fails(1, "tool git.show_diff: bad object")
	c.completes(2, statusOK, nil, nil, nil)
	w.ok("audit_v1")
	w.ok("github.comment_commit")
	w.completed("ok")
	e := w.explain()
	if got := kinds(e); !slices.Equal(got, []string{"worker_stopped@:0", "tool_error@fetch_diff:1"}) {
		t.Fatalf("%v", got)
	}
}

// The model answers agentd did not take are incidents of a call that got an answer in the end; of a
// call that ended refused, the refusal is the verdict.
func TestAnswersNotTakenAreIncidentsOfACallThatGotOne(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	c := w.returns("audit_v1", statusRefused, nil, nil)
	w.l.Requests = []ModelRequest{{ModelCall: db.ModelCall{ActivityID: c.id, Attempt: 1, RequestSeq: 1, Provider: "anthropic",
		Model: "claude-opus-5-5"}, Status: statusRefused}, {ModelCall: db.ModelCall{ActivityID: c.id, Attempt: 1, RequestSeq: 2,
		Provider: "anthropic", Model: "claude-opus-4-8"}, Status: statusRefused}}
	w.ok("github.comment_commit")
	w.completed("refused")
	if e := w.explain(); e.GetKind() != KindModelRefused || len(e.GetIncidents()) != 0 {
		t.Fatalf("%v", e)
	}
}

// What a person is told to do must move the run now: a run being cancelled, or stuck, does not
// wait for an approval or for budget.
func TestWhatMovesTheRunComesFirst(t *testing.T) {
	pendingApproval := func(w *world) *call {
		w.patched()
		c := w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
		w.l.Approvals = []db.Approval{{ApprovalID: "ap-1", Tool: "github.open_draft_pr", Status: "PENDING",
			ExpiresAt: w.clock.Add(24 * time.Hour).Format(time.RFC3339Nano), PayloadSha256: "abc"}}
		return c
	}
	t.Run("stuck, with an approval pending", func(t *testing.T) {
		w := newWorld(t)
		pendingApproval(w)
		w.taskFailed("boom", "TypeError")
		w.pending(&workflowpb.PendingWorkflowTaskInfo{State: enumspb.PENDING_WORKFLOW_TASK_STATE_SCHEDULED,
			OriginalScheduledTime: timestamppb.New(w.clock), Attempt: 5})
		if e := w.explain(); e.GetVerdict() != VerdictStuck || e.GetKind() != KindWorkflowTaskFailed || e.GetNext() != "" {
			t.Fatalf("%v", e)
		}
	})
	t.Run("cancelled, with an approval pending", func(t *testing.T) {
		w := newWorld(t)
		c := pendingApproval(w)
		w.cancel("operator:alice")
		check(t, w.explain(), want{verdict: VerdictCancelled, kind: KindCancelled, step: "open_pr", activity: c.id, attempt: 1,
			cause: []string{"alice cancelled the run", "has not ended yet"}}.with(func(e *agentosv1.Explanation) {
			if e.GetNext() != "" {
				t.Errorf("next %q", e.GetNext())
			}
		}))
	})
	t.Run("cancelled, and stuck", func(t *testing.T) {
		w := newWorld(t)
		pendingApproval(w)
		w.taskFailed("boom", "TypeError")
		w.cancel("operator:alice")
		w.pending(&workflowpb.PendingWorkflowTaskInfo{State: enumspb.PENDING_WORKFLOW_TASK_STATE_SCHEDULED,
			OriginalScheduledTime: timestamppb.New(w.clock), Attempt: 5})
		if e := w.explain(); e.GetVerdict() != VerdictCancelled || e.GetNext() != "" {
			t.Fatalf("%v", e)
		}
	})
	t.Run("budget extended, and no Worker", func(t *testing.T) {
		w := newWorld(t)
		w.ok("git.show_diff")
		w.returns("audit_v1", statusBudgetExhausted, nil, map[string]any{"needed_micros": 20480, "available_micros": 1000})
		w.rec("budget.extended", "operator:carol", map[string]any{"amount_micros": 1_000_000})
		w.pending(&workflowpb.PendingWorkflowTaskInfo{State: enumspb.PENDING_WORKFLOW_TASK_STATE_SCHEDULED,
			OriginalScheduledTime: timestamppb.New(w.clock.Add(-10 * time.Minute)), Attempt: 1})
		if e := w.explain(); e.GetVerdict() != VerdictStuck || e.GetKind() != KindNoWorker {
			t.Fatalf("%v", e)
		}
	})
}

// A run waits for a reconciliation only while an effect's outcome is unknown.
func TestOnlyAnUnknownOutcomeIsWaitedFor(t *testing.T) {
	for _, st := range []string{"SUCCEEDED", "FAILED", "DRY_RUN", "DENIED", "ABANDONED", "UNKNOWN"} {
		w := newWorld(t)
		w.ok("git.show_diff")
		w.l.Effects = []db.Effect{{EffectID: "ef-1", Tool: "github.open_draft_pr", Status: st}}
		if e := w.explain(); (e.GetVerdict() == VerdictWaiting) != (st == "UNKNOWN") {
			t.Fatalf("%s: %v", st, e)
		}
	}
}

// A workflow task waits for a Worker for a minute before the run is stuck for want of one; an
// approval is waited for until the moment it expires.
func TestTheBoundsOfAWait(t *testing.T) {
	for _, tc := range []struct {
		waited time.Duration
		stuck  bool
	}{{59 * time.Second, false}, {61 * time.Second, true}, {119 * time.Second, true}} {
		w := newWorld(t)
		w.ok("git.show_diff")
		w.now = w.clock.Add(time.Hour)
		w.pending(&workflowpb.PendingWorkflowTaskInfo{State: enumspb.PENDING_WORKFLOW_TASK_STATE_SCHEDULED,
			OriginalScheduledTime: timestamppb.New(w.now.Add(-tc.waited)), Attempt: 1})
		if e := w.explain(); (e.GetVerdict() == VerdictStuck) != tc.stuck {
			t.Fatalf("after %s: %v", tc.waited, e)
		}
	}
	for _, tc := range []struct {
		left    time.Duration
		waiting bool
	}{{time.Second, true}, {0, false}, {-time.Second, false}} {
		w := newWorld(t)
		w.patched()
		w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
		w.now = w.clock.Add(time.Hour)
		w.l.Approvals = []db.Approval{{ApprovalID: "ap-1", Tool: "github.open_draft_pr", Status: "PENDING",
			ExpiresAt: w.now.Add(tc.left).Format(time.RFC3339Nano)}}
		if e := w.explain(); (e.GetVerdict() == VerdictWaiting) != tc.waiting {
			t.Fatalf("%s left: %v", tc.left, e)
		}
	}
}

// A cancelled run that completed all the same is judged as it completed; one that ended cancelled
// was not terminated.
func TestACancelAndHowTheRunEnded(t *testing.T) {
	w := newWorld(t)
	w.pipeline()
	w.cancel("operator:dave")
	w.ok("github.comment_commit")
	w.completed("ok")
	if e := w.explain(); e.GetVerdict() != VerdictOK {
		t.Fatalf("%v", e)
	}
	w = newWorld(t)
	w.ok("git.show_diff")
	w.cancel("operator:dave")
	w.canceled()
	if e := w.explain(); e.GetVerdict() != VerdictCancelled || strings.Contains(e.GetCause(), "terminated") {
		t.Fatalf("%v", e)
	}
}

// A run older than the audit log that waits for budget, extended since the log began: its wait has
// no record of agentd's.
func TestAnOlderRunWaitingForBudget(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	w.returns("audit_v1", statusBudgetExhausted, nil, map[string]any{"needed_micros": 20480, "available_micros": 1000})
	w.stripAnchors()
	w.l.Records, w.l.LogBegan = nil, w.clock.Add(1)
	w.rec("budget.extended", "operator:carol", map[string]any{"amount_micros": 1_000_000})
	check(t, w.explain(), want{verdict: VerdictWaiting, kind: KindBudgetExhausted, step: "analyze", activity: "2:audit_v1",
		attempt: 1, cause: []string{"carol extended it"}})
}

// Without its history, a run's steps are its call records', or, without those, the calls table's:
// never both.
func TestStepsWithoutAHistory(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	w.ok("audit_v1")
	w.noTemp = true
	w.l.Calls = []db.Call{{ActivityID: "9:github.open_draft_pr", Kind: "tool", Name: "github.open_draft_pr",
		Outcome: sql.NullString{String: "rejected", Valid: true}}}
	e := w.explain()
	if e.GetVerdict() != VerdictUnknown || e.GetKind() != KindHistoryUnavailable || e.GetStep() != "analyze" {
		t.Fatalf("%v", e)
	}
}
