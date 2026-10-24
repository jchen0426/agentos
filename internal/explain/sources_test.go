package explain

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unicode"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// A run Temporal no longer has is explained from the audit log and the ledger, which say how far
// they go.
func TestARunTemporalNoLongerHas(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	c := w.returns("audit_v1", statusRefused, nil, nil)
	w.ok("github.comment_commit")
	w.completed("refused")
	w.noTemp, w.state = true, "UNKNOWN"
	e := w.explain()
	check(t, e, want{verdict: VerdictUnknown, kind: KindModelRefused, step: "analyze", activity: c.id, attempt: 1,
		cause: []string{"Temporal no longer has the execution", "from agentd's records"}})
	if len(e.GetMissing()) != 1 || !strings.HasPrefix(e.GetMissing()[0], "temporal:") {
		t.Fatalf("missing %v", e.GetMissing())
	}

	// Every call ok, the report out: as far as the records go.
	w = newWorld(t)
	w.pipeline()
	w.ok("github.comment_commit")
	w.noTemp = true
	check(t, w.explain(), want{verdict: VerdictUnknown, kind: KindHistoryUnavailable, cause: []string{"every call returned ok"}})
}

// A run Temporal says completed, whose history is gone before agentd reads it: no result can be held
// to agentd's records, so whether it ended well is not known, never a step missing.
func TestACompletedRunWithoutItsHistory(t *testing.T) {
	w := newWorld(t)
	w.pipeline()
	w.ok("github.comment_commit")
	w.completed("ok")
	w.events, w.histErr = nil, ErrHistoryGone
	check(t, w.explain(), want{verdict: VerdictUnknown, kind: KindHistoryUnavailable,
		cause: []string{"Temporal says the run completed, but no longer has its history", "every call returned ok"}})
}

// The history can go between describing the execution and reading it; a history Temporal fails to
// give fails the explanation.
func TestAHistoryThatGoesOrFails(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	w.histErr = ErrHistoryGone
	e := w.explain()
	if !slices.ContainsFunc(e.GetMissing(), func(m string) bool { return strings.Contains(m, "no longer has") }) {
		t.Fatalf("missing %v", e.GetMissing())
	}
	w.histErr = errBoom
	if _, err := Explain(w.input(), w.read); !errors.Is(err, errBoom) {
		t.Fatalf("got %v", err)
	}
	// And a ledger that cannot be read.
	w.histErr = nil
	if _, err := Explain(w.input(), func(Wanted) (Ledger, error) { return Ledger{}, errBoom }); !errors.Is(err, errBoom) {
		t.Fatalf("got %v", err)
	}
}

// A run from before the audit log is explained from its history, the calls table and the ledger,
// and says the log has nothing of it.
func TestARunOlderThanTheAuditLog(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	c := w.returns("audit_v1", statusRefused, nil, nil)
	w.ok("github.comment_commit")
	w.completed("refused")
	w.l.Records, w.l.LogBegan = nil, w.clock.Add(1)
	w.stripAnchors() // results from before the log name no record
	e := w.explain()
	check(t, e, want{verdict: VerdictStepFailed, kind: KindModelRefused, step: "analyze", activity: c.id, attempt: 1})
	if len(e.GetMissing()) != 1 || !strings.Contains(e.GetMissing()[0], "predates") {
		t.Fatalf("missing %v", e.GetMissing())
	}
	// Without its history either: the calls table's outcomes.
	w.noTemp = true
	w.l.Calls = []db.Call{{ActivityID: "1:git.show_diff", Kind: "tool", Name: "git.show_diff", Outcome: valid("ok")},
		{ActivityID: "2:audit_v1", Kind: "model", Name: "audit_v1", Outcome: valid("refused")}}
	check(t, w.explain(), want{verdict: VerdictUnknown, kind: KindModelRefused, step: "analyze", activity: "2:audit_v1"})
	// No log at all, and one that began before the run but has nothing of it.
	w.l.LogBegan = w.clock.Add(-1).AddDate(-1, 0, 0)
	w.run.CreatedAt = w.clock.Format("2006-01-02T15:04:05Z07:00")
	if m := w.explain().GetMissing(); !slices.ContainsFunc(m, func(s string) bool { return strings.Contains(s, "though the log began") }) {
		t.Fatalf("missing %v", m)
	}
}

// Record types explain does not know (A's later ones among them) change nothing.
func TestRecordTypesExplainDoesNotKnowChangeNothing(t *testing.T) {
	build := func(extra bool) *agentosv1.Explanation {
		w := newWorld(t)
		w.ok("git.show_diff")
		if extra {
			for _, typ := range []string{"call.counted", "budget.reserved", "budget.settled", "budget.released",
				"model.requested", "model.answered", "blob.owned", "agentd.read", "something.new"} {
				w.rec(typ, audit.Agentd, map[string]any{"activity_id": "2:audit_v1", "attempt": 1, "status": "error",
					"error": "made up"})
			}
		}
		w.returns("audit_v1", statusRefused, map[string]any{"scheduled_at": "2026-10-22T10:00:00Z", "outcome": "refused"}, nil)
		w.ok("github.comment_commit")
		w.completed("refused")
		return w.explain()
	}
	with, without := build(true), build(false)
	if with.GetVerdict() != without.GetVerdict() || with.GetKind() != without.GetKind() || with.GetStep() != without.GetStep() ||
		len(with.GetIncidents()) != len(without.GetIncidents()) || with.GetCause() != without.GetCause() {
		t.Fatalf("with unknown records:\n%v\nwithout:\n%v", with, without)
	}
}

// A step the task does not name shows as its call's name; records of executions agentd did not
// start, under the run's workflow ID, are not the run's.
func TestStepNamesAndOtherExecutions(t *testing.T) {
	w := newWorld(t)
	c := w.returns("test.unsafe_write", statusDenied, map[string]any{"reason": "no such tool",
		"refused_by": audit.RefusedByAgentd}, nil)
	w.rec("call", audit.UnknownRun, map[string]any{"activity_id": c.id, "attempt": 1, "kind": "tool",
		"name": "test.unsafe_write", "status": statusOK})
	w.ok("github.comment_commit")
	w.completed("denied")
	check(t, w.explain(), want{verdict: VerdictStepFailed, kind: KindDenied, step: `"tool test.unsafe_write"`, activity: c.id,
		attempt: 1, cause: []string{`agentd refused "tool test.unsafe_write": "no such tool"`}})

	// Without a history, an execution agentd did not start makes no step of the run's.
	w = newWorld(t)
	w.rec("call", audit.UnknownRun, map[string]any{"activity_id": "1:github.open_draft_pr", "attempt": 1, "kind": "tool",
		"name": "github.open_draft_pr", "status": statusDenied})
	w.pipeline()
	w.noTemp = true
	check(t, w.explain(), want{verdict: VerdictUnknown, kind: KindHistoryUnavailable, step: "open_pr",
		activity: "4:github.open_draft_pr", attempt: 1, cause: []string{"the last call was open_pr, which returned ok"}})
}

// The Worker shapes the history: what it puts in an explanation is bounded, and printable.
func TestAHostileHistoryIsBounded(t *testing.T) {
	w := newWorld(t)
	long := "\x1b[2J\x1b[31m" + strings.Repeat("x\n", 600) + "‮"
	for i := 0; i < 120; i++ {
		c := w.schedule(long)
		c.fails(1, "boom\x1b[0m\r\n"+strings.Repeat("y", 3000))
		c.completes(2, statusOK, nil, nil, nil)
	}
	c := w.schedule("git.show_diff")
	c.failsForGood(1, "tool git.show_diff: \x1b[31mbad\nobject\r", "ToolFailed", nil)
	w.ok("github.comment_commit")
	w.completed(long)
	e := w.explain()
	if e.GetKind() != KindToolError || e.GetStep() != "fetch_diff" {
		t.Fatalf("%s %s", e.GetKind(), e.GetStep())
	}
	if len(e.GetIncidents()) != maxIncidents || e.GetIncidentsOmitted() != 70 {
		t.Fatalf("%d incidents, %d omitted", len(e.GetIncidents()), e.GetIncidentsOmitted())
	}
	texts := []string{e.GetCause(), e.GetClaim(), e.GetStep(), e.GetActivityId(), e.GetNext()}
	texts = append(texts, e.GetEvidence()...)
	for _, inc := range e.GetIncidents() {
		texts = append(texts, inc.GetCause(), inc.GetStep(), inc.GetActivityId())
	}
	for _, s := range e.GetSources() {
		texts = append(texts, s.GetWhat())
	}
	for _, s := range texts {
		if len(s) > maxText+4 || strings.ContainsFunc(s, func(r rune) bool { return !unicode.IsPrint(r) }) {
			t.Fatalf("%d bytes, or not printable: %q", len(s), s)
		}
	}
	if !strings.Contains(e.GetCause(), `\x1b[31mbad\nobject\r`) {
		t.Fatalf("cause %q", e.GetCause())
	}
}

// A history longer than explain reads is cut short, and the explanation says so. How the run went
// past the cut is not known: its report, say, may be there.
func TestAHistoryLongerThanExplainReads(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	for len(w.events) <= maxEvents {
		w.task()
	}
	w.ok("audit_v1")
	w.ok("github.comment_commit")
	w.completed("done")
	e := w.explain()
	if !slices.ContainsFunc(e.GetMissing(), func(m string) bool { return strings.Contains(m, "longer than agentd reads") }) {
		t.Fatalf("missing %v", e.GetMissing())
	}
	check(t, e, want{verdict: VerdictUnknown, kind: KindHistoryUnavailable, cause: []string{"how the run went past them is not known"}})
}

// Usage is by the model that billed each request, cache tokens and refusals counted.
func TestUsage(t *testing.T) {
	got := usage([]ModelRequest{
		{ModelCall: db.ModelCall{Provider: "anthropic", Model: "claude-opus-5-5", UsageJson: valid(`[{"model":"anthropic/claude-opus-5-5",
			"input_tokens":100,"cache_read_tokens":900,"cache_write_5m_tokens":10,"cache_write_1h_tokens":20,"output_tokens":50},
			{"model":"anthropic/claude-opus-4-8","input_tokens":5,"output_tokens":7}]`)}, Status: statusRefused},
		{ModelCall: db.ModelCall{Provider: "openai", Model: "gpt-6.1-sol", UsageJson: valid(`[{"model":"openai/gpt-6.1-sol",
			"input_tokens":3,"cache_write_tokens":4,"output_tokens":2}]`)}, Status: statusOK},
		{ModelCall: db.ModelCall{Provider: "openai", Model: "gpt-6.1-sol"}}, // unanswered: billed nothing known
	})
	want := []*agentosv1.ModelUsage{
		{Model: "anthropic/claude-opus-4-8", Requests: 1, InputTokens: 5, OutputTokens: 7},
		{Model: "anthropic/claude-opus-5-5", Requests: 1, InputTokens: 100, OutputTokens: 50, CacheReadTokens: 900,
			CacheWriteTokens: 30, Refusals: 1},
		{Model: "openai/gpt-6.1-sol", Requests: 1, InputTokens: 3, OutputTokens: 2, CacheWriteTokens: 4},
	}
	if len(got) != len(want) {
		t.Fatalf("%v", got)
	}
	for i := range want {
		if got[i].String() != want[i].String() {
			t.Errorf("%d: got %v, want %v", i, got[i], want[i])
		}
	}
}

// reportScenarios are runs up to their report, each as a workflow could make it.
var reportScenarios = map[string]func(w *world){
	"ok": func(w *world) { w.pipeline() },
	"ok, no patch proposed": func(w *world) {
		w.ok("git.show_diff")
		w.ok("audit_v1")
	},
	"refused": func(w *world) {
		w.ok("git.show_diff")
		w.returns("audit_v1", statusRefused, nil, nil)
	},
	"denied": func(w *world) {
		w.ok("git.show_diff")
		w.returns("audit_v1", statusDenied, map[string]any{"policy": map[string]any{"decision": "deny", "reason": "no"},
			"refused_by": audit.RefusedByPolicy}, nil)
	},
	"tool error": func(w *world) {
		w.schedule("git.show_diff").failsForGood(1, "tool git.show_diff: bad", "ToolFailed", nil)
	},
	"rejected": func(w *world) {
		w.patched()
		w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
		w.returns("github.open_draft_pr", statusRejected, map[string]any{"reason": "no"}, nil)
	},
	"expired": func(w *world) {
		w.patched()
		w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
		w.returns("github.open_draft_pr", statusExpired, nil, nil)
	},
	"budget, then ok": func(w *world) {
		w.ok("git.show_diff")
		w.returns("audit_v1", statusBudgetExhausted, nil, nil)
		w.ok("audit_v1")
	},
	"a retry, then ok": func(w *world) {
		c := w.schedule("git.show_diff")
		c.serverFails(1)
		c.completes(2, statusOK, nil, nil, nil)
		w.ok("audit_v1")
	},
	// T8's kind: the first attempt hangs, Temporal gives up on it and the second returns ok; the first
	// ends later, with an error, and its record is the call's last.
	"a stale attempt that failed late": func(w *world) {
		c := w.schedule("git.show_diff")
		c.completes(2, statusOK, nil, nil, timeoutFailure(enumspb.TIMEOUT_TYPE_HEARTBEAT))
		c.fails(1, "context canceled")
		w.ok("audit_v1")
	},
	// The model's call hangs at every attempt: Temporal times the call out; agentd's records of its
	// attempts come later, ok.
	"the model hangs": func(w *world) {
		w.ok("git.show_diff")
		c := w.schedule("audit_v1")
		c.end(5, timeoutFailure(enumspb.TIMEOUT_TYPE_HEARTBEAT), func(e *historypb.HistoryEvent, started int64) {
			e.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
			e.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
				ScheduledEventId: c.scheduled, StartedEventId: started, Failure: timeoutFailure(enumspb.TIMEOUT_TYPE_HEARTBEAT)}}
		})
		for k := int32(1); k <= 5; k++ {
			c.record(k, statusOK, nil)
		}
	},
	"no analysis": func(w *world) { w.ok("git.show_diff") },
	"a patch nobody checked": func(w *world) {
		w.ok("git.show_diff")
		w.returns("audit_v1", statusOK, nil, map[string]any{"report_ref": "sha256:" + strings.Repeat("a", 64),
			"patch_ref": "sha256:" + strings.Repeat("b", 64)})
	},
	"an edit the validation refused": func(w *world) {
		w.patched()
		w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
		w.returns("github.open_draft_pr", statusEdited, nil, nil)
		w.returns("git.apply_check", statusDenied, map[string]any{"reason": "the patch does not apply",
			"refused_by": audit.RefusedByTool}, nil)
	},
	"a result agentd did not send": func(w *world) {
		w.ok("git.show_diff")
		c := w.schedule("audit_v1")
		c.record(1, statusRefused, nil)
		c.result(1, statusOK, valueOf(w.t, nil, nil), nil)
	},
}

// The report says what explain says once the run has ended: the same verdict, kind and step, from
// the history and agentd's records of the calls before the report.
func TestTheReportAgreesWithExplain(t *testing.T) {
	want := map[string][3]string{
		"ok":                               {VerdictOK, "", ""},
		"ok, no patch proposed":            {VerdictOK, "", ""},
		"refused":                          {VerdictStepFailed, KindModelRefused, "analyze"},
		"denied":                           {VerdictStepFailed, KindPolicyDenied, "analyze"},
		"tool error":                       {VerdictStepFailed, KindToolError, "fetch_diff"},
		"rejected":                         {VerdictNotApproved, KindApprovalRejected, "open_pr"},
		"expired":                          {VerdictNotApproved, KindApprovalExpired, "open_pr"},
		"budget, then ok":                  {VerdictOK, "", ""},
		"a retry, then ok":                 {VerdictOK, "", ""},
		"a stale attempt that failed late": {VerdictOK, "", ""},
		"the model hangs":                  {VerdictStepFailed, KindActivityTimeout, "analyze"},
		"no analysis":                      {VerdictStepFailed, KindStepMissing, "analyze"},
		"a patch nobody checked":           {VerdictStepFailed, KindStepMissing, "validate_patch"},
		"an edit the validation refused":   {VerdictStepFailed, KindToolDenied, "validate_patch"},
		"a result agentd did not send":     {VerdictStepFailed, KindModelRefused, "analyze"},
	}
	for name, before := range reportScenarios {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			before(w)
			// As agentd renders it: at the report's attempt after one that failed, which is not the
			// report's to judge.
			r := w.schedule("github.comment_commit")
			r.fails(1, "GitHub said 502")
			o := w.report(ReasonDone, "")
			r.completes(2, statusOK, nil, nil, nil)
			w.completed("whatever")
			e := w.explain()
			if got := [3]string{o.Verdict, o.Kind, o.Step}; got != want[name] {
				t.Fatalf("report %+v, want %v", o, want[name])
			}
			if o.Verdict != e.GetVerdict() || o.Kind != e.GetKind() || o.Step != e.GetStep() {
				t.Fatalf("report %+v; explain %s %s %s: %s", o, e.GetVerdict(), e.GetKind(), e.GetStep(), e.GetCause())
			}
			if o.Step != "" && !o.Named || len(o.Cut) != 0 {
				t.Errorf("%+v: %q is one of the task's steps, and the run is read whole", o, o.Step)
			}
		})
	}

	// The workflow's own code failed: it says so, and the run fails after the report.
	w := newWorld(t)
	w.ok("git.show_diff")
	r := w.schedule("github.comment_commit")
	o := w.report(ReasonWorkflowError, "KeyError")
	r.completes(1, statusOK, nil, nil, nil)
	w.failed("the workflow raised KeyError: 'blob'", "KeyError")
	e := w.explain()
	if o.Verdict != e.GetVerdict() || o.Kind != e.GetKind() || o.Step != e.GetStep() || !strings.Contains(o.Claim, "(KeyError)") ||
		!strings.Contains(o.Cause, "by the workflow's own account") {
		t.Fatalf("report %+v; explain %s %s %s", o, e.GetVerdict(), e.GetKind(), e.GetStep())
	}
	// An error type the workflow cannot name, or a reason it does not give, says nothing it chose.
	if o := w.report(ReasonWorkflowError, "x y"); strings.Contains(o.Claim, "x y") || !strings.Contains(o.Claim, "did not name") {
		t.Fatalf("%+v", o)
	}
	if o, d := w.report("", ""), w.report(ReasonDone, ""); !reflect.DeepEqual(o, d) {
		t.Fatalf("no reason: %+v; done: %+v", o, d)
	}
}

// Without the run's history the report's verdict is unknown, never ok, and says why.
func TestTheReportNeedsTheHistory(t *testing.T) {
	for name, set := range map[string]func(w *world){
		"Temporal cannot be read":    func(w *world) { w.unread = "Temporal is down" },
		"Temporal has no execution":  func(w *world) { w.noTemp = true },
		"the history cannot be read": func(w *world) { w.histErr = errBoom },
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			w.pipeline()
			w.schedule("github.comment_commit")
			set(w)
			o := w.report(ReasonDone, "")
			if o.Verdict != VerdictUnknown || o.Kind != KindHistoryUnavailable || o.Cause == "" {
				t.Fatalf("%+v", o)
			}
		})
	}
	// A ledger that cannot be read fails the report: its call is tried again. So does a history Temporal
	// could not be reached for as it was read: it may answer later, when an unknown would have stood.
	w := newWorld(t)
	if _, err := Report(w.input(), func(Wanted) (Ledger, error) { return Ledger{}, errBoom }, ReasonDone, ""); !errors.Is(err, errBoom) {
		t.Fatalf("got %v", err)
	}
	for _, gone := range []error{serviceerror.NewUnavailable("connection refused"), serviceerror.NewDeadlineExceeded("deadline"),
		context.DeadlineExceeded} {
		w = newWorld(t)
		w.pipeline()
		w.schedule("github.comment_commit")
		w.histErr = gone
		if o, err := Report(w.input(), w.read, ReasonDone, ""); !Unreachable(err) || o.Verdict != "" {
			t.Fatalf("%v: %+v %v", gone, o, err)
		}
	}
}

// What says Temporal did not answer, and what is an answer of its own.
func TestUnreachable(t *testing.T) {
	for err, want := range map[error]bool{
		serviceerror.NewUnavailable("dial tcp: connection refused"):           true,
		serviceerror.NewDeadlineExceeded("deadline exceeded"):                 true,
		serviceerror.NewResourceExhausted(0, "busy"):                          true,
		serviceerror.NewCanceled("canceled"):                                  true,
		context.DeadlineExceeded:                                              true,
		fmt.Errorf("the history of run r: %w", context.Canceled):              true,
		status.Error(codes.Unavailable, "transport is closing"):               true,
		status.Error(codes.DeadlineExceeded, "context deadline exceeded"):     true,
		serviceerror.NewNotFound("workflow execution not found"):              false,
		serviceerror.NewInvalidArgument("bad"):                                false,
		serviceerror.NewPermissionDenied("no", ""):                            false,
		status.Error(codes.Internal, "boom"):                                  false,
		errBoom:                                                               false,
		fmt.Errorf("wrapped: %w", serviceerror.NewUnavailable("unavailable")): true,
	} {
		if got := Unreachable(err); got != want {
			t.Errorf("%v: %v, want %v", err, got, want)
		}
	}
	if Unreachable(nil) {
		t.Error("nil")
	}
}

// A step whose name is the workflow's, not the task's, is not named in the report's words.
func TestTheReportNamesOnlyTheTasksSteps(t *testing.T) {
	w := newWorld(t)
	w.returns("@mallory see www.evil.example", statusDenied, map[string]any{"reason": "no such tool",
		"refused_by": audit.RefusedByAgentd}, nil)
	o := w.report(ReasonDone, "")
	if o.Named || o.Kind != KindDenied || o.Step != `"tool @mallory see www.evil.example"` || !strings.Contains(o.Cause, "@mallory") {
		t.Fatalf("%+v", o)
	}
}

var _ = sql.NullString{}
