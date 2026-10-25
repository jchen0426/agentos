package explain

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/tasks"
)

// Each wait for a reconciliation is told with the person's answer that ended it, not an earlier one.
func TestEachReconciliationEndsItsOwnWait(t *testing.T) {
	w := newWorld(t)
	w.patched()
	w.returns("github.open_draft_pr", statusNeedsReconcile, nil, nil)
	w.rec("effect.reconciled", "operator:alice", map[string]any{"effect_id": "ef-1", "executed": false})
	w.returns("github.open_draft_pr", statusNeedsReconcile, nil, nil)
	w.rec("effect.reconciled", "operator:bob", map[string]any{"effect_id": "ef-1", "executed": true})
	w.ok("github.open_draft_pr")
	w.ok("github.comment_commit")
	w.completed("ok")
	e := w.explain()
	incs := incidentsOf(e, KindReconcilePending, "")
	if len(incs) != 2 || !strings.Contains(incs[0].GetCause(), "until alice said it did not happen") ||
		!strings.Contains(incs[1].GetCause(), "until bob said it happened") {
		t.Fatalf("%v", e.GetIncidents())
	}
}

// An edit is gone on from by the calls that check the reviewer's patch, not by the report.
func TestAnEditOnlyTheReportFollowed(t *testing.T) {
	w := newWorld(t)
	w.patched()
	w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
	c := w.returns("github.open_draft_pr", statusEdited, nil, nil)
	w.ok("github.comment_commit")
	w.completed("edited")
	check(t, w.explain(), want{verdict: VerdictStepFailed, kind: KindApprovalEdited, step: "open_pr", activity: c.id, attempt: 1})
}

// A workflow that reuses an activity ID gets its records split by time, with a second's slack for
// the clocks of agentd and Temporal: a record a moment before its call's scheduled event is its.
// It tells the records no result names: a failed call's.
func TestRecordsOfAReusedActivityID(t *testing.T) {
	w := newWorld(t)
	first := w.ok("git.show_diff")
	again := &call{w: w, id: first.id, kind: "tool", name: "git.show_diff"}
	input, _ := converter.GetDefaultDataConverter().ToPayloads(&agentosv1.Call{Kind: "tool", Name: "git.show_diff"})
	again.scheduled = w.event(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
			ActivityId: again.id, Input: input}}
	})
	seq := again.record(1, statusError, map[string]any{"error": serverError, "error_cause": audit.ErrorCauseToolServer})
	again.end(1, nil, func(e *historypb.HistoryEvent, started int64) {
		e.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
		e.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
			ScheduledEventId: again.scheduled, StartedEventId: started, Failure: appFailure(serverError, "", false)}}
	})
	// agentd's clock half a second behind Temporal's: the record reads as written before the call
	// was scheduled.
	scheduled := w.events[again.scheduled-1].GetEventTime().AsTime()
	for i := range w.l.Records {
		if w.l.Records[i].Seq == seq {
			w.l.Records[i].At = scheduled.Add(-500 * time.Millisecond).Format(time.RFC3339Nano)
		}
	}
	w.ok("github.comment_commit")
	w.completed("failed")
	check(t, w.explain(), want{verdict: VerdictStepFailed, kind: KindToolServerFailed, step: "fetch_diff", activity: first.id,
		attempt: 1, seqs: []int64{seq}})
}

// A call that failed for good at an attempt agentd has no record of, agentd having stopped during
// it, is told by Temporal's failure, not by an earlier attempt's record.
func TestAFailureWithoutItsRecord(t *testing.T) {
	w := newWorld(t)
	c := w.schedule("git.show_diff")
	c.fails(1, "tool git.show_diff: bad object")
	c.end(2, appFailure("tool git.show_diff: bad object", "", false), func(e *historypb.HistoryEvent, started int64) {
		e.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
		e.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
			ScheduledEventId: c.scheduled, StartedEventId: started, Failure: appFailure("more than 1 attempts of one call", "TooManyAttempts", true)}}
	})
	began := w.events[c.scheduled].GetEventTime().AsTime()
	w.start("agentd.started", audit.Agentd, nil)
	w.l.Starts[len(w.l.Starts)-1].At = began.Add(500 * time.Millisecond).Format(time.RFC3339Nano)
	w.ok("github.comment_commit")
	w.completed("failed")
	check(t, w.explain(), want{verdict: VerdictStepFailed, kind: KindToolError, step: "fetch_diff", activity: c.id, attempt: 2,
		cause: []string{"TooManyAttempts: more than 1 attempts"}})
}

// A task without a report has no report call, whatever a call is named.
func TestATaskWithoutAReport(t *testing.T) {
	w := newWorld(t)
	w.calls++
	c := &call{w: w, id: "1:x", kind: "", name: ""}
	c.scheduled = w.event(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
			ActivityId: c.id}}
	})
	c.completes(1, statusDenied, map[string]any{"reason": "no", "refused_by": audit.RefusedByAgentd}, nil, nil)
	w.completed("denied")
	in := w.input()
	in.Task = tasks.Task{Name: "bare"}
	e, err := Explain(in, w.read)
	if err != nil || e.GetKind() != KindDenied || e.GetVerdict() != VerdictStepFailed {
		t.Fatalf("%v %v", e, err)
	}
}

// A history of exactly as many events as explain reads is read whole.
func TestAHistoryAsLongAsExplainReads(t *testing.T) {
	w := newWorld(t)
	for len(w.events)+3 <= maxEvents {
		w.task()
	}
	for len(w.events) < maxEvents {
		w.event(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, nil)
	}
	if e := w.explain(); len(e.GetMissing()) != 0 {
		t.Fatalf("missing %v", e.GetMissing())
	}
}

// A refusal is told by the request the model refused, not by the call's latest request.
func TestARefusalIsTheRefusingRequests(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	c := w.returns("audit_v1", statusRefused, nil, nil)
	w.l.Requests = []ModelRequest{
		{ModelCall: db.ModelCall{ActivityID: c.id, Attempt: 1, RequestSeq: 1, Provider: "anthropic", Model: "claude-opus-5-5"},
			Status: statusRefused, Category: "cyber", Explanation: "no"},
		{ModelCall: db.ModelCall{ActivityID: c.id, Attempt: 1, RequestSeq: 2, Provider: "anthropic", Model: "claude-opus-4-8"}}}
	w.ok("github.comment_commit")
	w.completed("refused")
	check(t, w.explain(), want{verdict: VerdictStepFailed, kind: KindModelRefused, step: "analyze", activity: c.id, attempt: 1,
		cause: []string{"anthropic/claude-opus-5-5", `"cyber"`}})
}

// A rejection is told by the approval that was rejected, not by a later one of the same tool.
func TestARejectionIsItsApprovals(t *testing.T) {
	w := newWorld(t)
	w.patched()
	w.returns("github.open_draft_pr", statusNeedsApproval, nil, nil)
	c := w.returns("github.open_draft_pr", statusRejected, nil, nil)
	w.l.Approvals = []db.Approval{
		{ApprovalID: "ap-1", Tool: "github.open_draft_pr", Status: "REJECTED", DecidedBy: valid("robert"),
			DecidedAt: valid("2026-10-22T10:01:00Z"), Reason: valid("not this patch")},
		{ApprovalID: "ap-2", Tool: "github.open_draft_pr", Status: "EXPIRED", DecidedBy: valid("agentd"), ExpiresAt: "x"}}
	w.ok("github.comment_commit")
	w.completed("rejected")
	check(t, w.explain(), want{verdict: VerdictNotApproved, kind: KindApprovalRejected, step: "open_pr", activity: c.id,
		attempt: 1, cause: []string{"robert rejected open_pr's write", `"not this patch"`}})
}

// A workflow task a Worker has taken is not waiting for one, however long ago it was scheduled.
func TestAWorkflowTaskAWorkerHas(t *testing.T) {
	w := newWorld(t)
	c := w.ok("git.show_diff")
	w.pending(&workflowpb.PendingWorkflowTaskInfo{Attempt: 1, State: enumspb.PENDING_WORKFLOW_TASK_STATE_STARTED,
		OriginalScheduledTime: timestamppb.New(w.clock.Add(-5 * time.Minute))})
	check(t, w.explain(), want{verdict: VerdictRunning, step: "fetch_diff", activity: c.id, attempt: 1})
}

// A run whose execution agentd has not recorded yet is starting.
func TestARunStarting(t *testing.T) {
	w := newWorld(t)
	w.run.RunID = sql.NullString{}
	w.noTemp = true
	check(t, w.explain(), want{verdict: VerdictRunning, cause: []string{"the run is starting"}})
}

// Without its history, a report whose call has not ended did not go out.
func TestAReportThatHasNotEnded(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	w.ok("audit_v1")
	w.rec("call.started", audit.Run, map[string]any{"activity_id": "3:github.comment_commit", "attempt": 1, "kind": "tool",
		"name": "github.comment_commit"})
	w.noTemp = true
	e := w.explain()
	if e.GetVerdict() != VerdictUnknown || strings.Contains(e.GetCause(), "went out") || e.GetStep() != "post_report" {
		t.Fatalf("%v", e)
	}
}

// Who cancelled a run is the name an operator gave, as the audit log has it.
func TestWhoCancelled(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	w.cancel("operator:dave")
	w.canceled()
	if e := w.explain(); !strings.HasPrefix(e.GetCause(), "dave cancelled the run at ") {
		t.Fatalf("cause %q", e.GetCause())
	}
}

// finish bounds each part of an explanation, and keeps evidence of the execution now apart by what
// it shows.
func TestAnExplanationIsBounded(t *testing.T) {
	e := &agentosv1.Explanation{Missing: []string{strings.Repeat("m", 5000)}}
	for i := 0; i < 25; i++ {
		e.Usage = append(e.Usage, &agentosv1.ModelUsage{Model: strings.Repeat("u", i+1)})
	}
	inc := &agentosv1.Incident{Cause: "c"}
	for i := int64(1); i <= 10; i++ {
		inc.Evidence = append(inc.Evidence, auditSource(i, "x"))
	}
	e.Incidents = []*agentosv1.Incident{inc}
	e.Sources = []*agentosv1.Evidence{temporalSource(0, "pending workflow task, attempt 3"),
		temporalSource(0, "pending activity \"1:x\", attempt 3"), temporalSource(0, "pending workflow task, attempt 3")}
	finish(e)
	if len(e.GetUsage()) != maxUsage || len(e.GetMissing()[0]) > maxText || len(e.GetIncidents()[0].GetEvidence()) != maxIncidentEvidence ||
		len(e.GetSources()) != 2 {
		t.Fatalf("%d usage, missing of %d bytes, %d evidence, %d sources", len(e.GetUsage()), len(e.GetMissing()[0]),
			len(e.GetIncidents()[0].GetEvidence()), len(e.GetSources()))
	}
}

// The report names no error type the workflow made up, however well formed (64 characters, as the
// report call's arguments allow), and says nothing longer than its bounds, on one line.
func TestTheReportsBounds(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	for _, name := range []string{strings.Repeat("E", 64), strings.Repeat("E", 65)} {
		if o := w.report(ReasonWorkflowError, name); strings.Contains(o.Claim, "EEEE") ||
			!strings.Contains(o.Claim, "(an exception of another type)") {
			t.Fatalf("%+v", o)
		}
	}
	c := w.schedule("audit_v1")
	c.failsForGood(1, strings.Repeat("boom\n", 400), "", nil)
	o := w.report(ReasonDone, "")
	if len(o.Cause) > maxText || strings.ContainsFunc(o.Cause, func(r rune) bool { return !unicode.IsPrint(r) }) {
		t.Fatalf("%d bytes: %q", len(o.Cause), o.Cause)
	}
}

// What a result agentd did not send says counts for nothing: not the budget it claims was short.
func TestAForeignResultsFiguresCountForNothing(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	c := w.schedule("audit_v1")
	c.record(1, statusBudgetExhausted, map[string]any{"result_sha256": resultSHA(t, nil)})
	c.result(1, statusBudgetExhausted, valueOf(t, map[string]any{"needed_micros": 99_000_000, "available_micros": 1}, nil), nil)
	e := w.explain()
	if e.GetVerdict() != VerdictWaiting || strings.Contains(e.GetCause(), "$99") || strings.Contains(e.GetNext(), "99") {
		t.Fatalf("%v", e)
	}
}

// The evidence names a call by its activity ID quoted: the workflow chose it.
func TestTheEvidenceQuotesTheActivityID(t *testing.T) {
	w := newWorld(t)
	c := w.schedule("git.show_diff")
	c.failsForGood(1, "tool git.show_diff: bad object", "ToolFailed", nil)
	w.ok("github.comment_commit")
	w.completed("failed")
	e := w.explain()
	want := `audit log seq ` // the record's line, then the event's
	for _, line := range []string{fmt.Sprintf("call %q, attempt 1: error", c.id), fmt.Sprintf("ActivityTaskFailed of %q", c.id)} {
		if !slices.ContainsFunc(e.GetEvidence(), func(ev string) bool { return strings.Contains(ev, line) }) {
			t.Fatalf("no %s in %v (%s)", line, e.GetEvidence(), want)
		}
	}
}

// A field that holds nothing (null, an empty string) does not make its condition hold: an audit with
// no patch needs no check of one.
func TestAnEmptyFieldHoldsNothing(t *testing.T) {
	for _, patch := range []any{nil, "", []any{}} {
		w := newWorld(t)
		w.ok("git.show_diff")
		w.returns("audit_v1", statusOK, nil, map[string]any{"report_ref": "sha256:" + strings.Repeat("a", 64), "patch_ref": patch})
		w.ok("github.comment_commit")
		w.completed("ok")
		if e := w.explain(); e.GetVerdict() != VerdictOK {
			t.Fatalf("patch_ref %#v: %v", patch, e)
		}
	}
}

// A write of a run that is not a dry run, which agentd only dry-ran (an agentd without GitHub took
// it up), did not reach GitHub: it is not the step's done, and a report it would be never went out.
// A dry run's own writes are as they should be.
func TestAWriteOnlyDryRun(t *testing.T) {
	dry := map[string]any{"url": "dry-run://github/acme/payments/agent/x", "number": 0, "dry_run": true}
	w := newWorld(t)
	w.patched()
	c := w.returns("github.open_draft_pr", statusOK, nil, dry)
	w.l.Effects = []db.Effect{{EffectID: "ef-9", Tool: "github.open_draft_pr", Status: "DRY_RUN"}}
	w.returns("github.comment_commit", statusOK, nil, map[string]any{"url": "dry-run://github/comment", "dry_run": true})
	w.completed("ok")
	e := w.explain()
	check(t, e, want{verdict: VerdictStepFailed, kind: KindDryRan, step: "open_pr", activity: c.id, attempt: 1,
		cause: []string{"open_pr did not reach its system", "though the run is not a dry run"}})
	if strings.Contains(e.GetCause(), "went out") ||
		!slices.ContainsFunc(e.GetSources(), func(s *agentosv1.Evidence) bool { return s.Table == "effects" && s.Key == "ef-9" }) {
		t.Fatalf("%v", e)
	}
	// The report only dry-run: the run did not report.
	w = newWorld(t)
	w.patched()
	w.ok("github.open_draft_pr")
	w.returns("github.comment_commit", statusOK, nil, map[string]any{"url": "dry-run://github/comment", "dry_run": true})
	w.completed("ok")
	check(t, w.explain(), want{verdict: VerdictStepFailed, kind: KindReportNotPosted, step: "post_report", activity: "5:github.comment_commit",
		attempt: 1, cause: []string{"post_report did not reach its system"}})
	// A dry run.
	w = newWorld(t)
	w.run.DryRun = 1
	w.patched()
	w.returns("github.open_draft_pr", statusOK, nil, dry)
	w.returns("github.comment_commit", statusOK, nil, map[string]any{"url": "dry-run://github/comment", "dry_run": true})
	w.completed("ok")
	check(t, w.explain(), want{verdict: VerdictOK})
}

// A step the task requires is met by a call of its kind and name, the latest accepted one for a
// condition; the report is the task's call by its kind and name; a wait is gone on from by a call of
// its kind and name; and a workflow's error is told at the step it names, by name only when the task
// names it, else as a call the task does not name.
func TestTheTasksCallsByKindAndName(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	w.calls++
	c := &call{w: w, id: "2:audit_v1", kind: "tool", name: "audit_v1"}
	input, _ := converter.GetDefaultDataConverter().ToPayloads(&agentosv1.Call{Kind: "tool", Name: "audit_v1"})
	c.scheduled = w.event(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
			ActivityId: c.id, Input: input}}
	})
	c.completes(1, statusOK, nil, nil, nil)
	w.schedule("github.comment_commit")
	if o := w.report(ReasonDone, ""); o.Kind != KindStepMissing || o.Step != "analyze" {
		t.Fatalf("a tool named audit_v1 met analyze: %+v", o)
	}

	// The latest accepted analysis decides whether the patch's check is required.
	w = newWorld(t)
	w.ok("git.show_diff")
	w.ok("audit_v1")
	w.returns("audit_v1", statusOK, nil, map[string]any{"report_ref": "sha256:" + strings.Repeat("a", 64),
		"patch_ref": "sha256:" + strings.Repeat("b", 64)})
	w.schedule("github.comment_commit")
	if o := w.report(ReasonDone, ""); o.Kind != KindStepMissing || o.Step != "validate_patch" {
		t.Fatalf("the latest analysis proposed a patch: %+v", o)
	}

	// A model call named like the report is no report.
	w = newWorld(t)
	w.ok("git.show_diff")
	w.ok("audit_v1")
	w.calls++
	m := &call{w: w, id: "3:github.comment_commit", kind: "model", name: "github.comment_commit"}
	input, _ = converter.GetDefaultDataConverter().ToPayloads(&agentosv1.Call{Kind: "model", Name: "github.comment_commit"})
	m.scheduled = w.event(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
			ActivityId: m.id, Input: input}}
	})
	m.completes(1, statusOK, nil, nil, nil)
	w.completed("ok")
	check(t, w.explain(), want{verdict: VerdictStepFailed, kind: KindReportNotPosted, step: "post_report",
		cause: []string{"made no call of post_report"}})

	// A wait of the model's is not gone on from by a tool of its name.
	w = newWorld(t)
	w.ok("git.show_diff")
	b := w.returns("audit_v1", statusBudgetExhausted, nil, nil)
	w.calls++
	c = &call{w: w, id: "3:audit_v1", kind: "tool", name: "audit_v1"}
	input, _ = converter.GetDefaultDataConverter().ToPayloads(&agentosv1.Call{Kind: "tool", Name: "audit_v1"})
	c.scheduled = w.event(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
			ActivityId: c.id, Input: input}}
	})
	c.completes(1, statusOK, nil, nil, nil)
	w.ok("github.comment_commit")
	w.completed("ok")
	check(t, w.explain(), want{verdict: VerdictStepFailed, kind: KindBudgetExhausted, step: "analyze", activity: b.id, attempt: 1})

	// The workflow's own code failed after a call the task does not name: not named outside the fence.
	w = newWorld(t)
	w.ok("git.show_diff")
	w.returns("test.unsafe_write", statusOK, nil, nil)
	w.schedule("github.comment_commit")
	if o := w.report(ReasonWorkflowError, "KeyError"); o.Named || o.Step != unnamedStep || strings.Contains(o.Cause, "unsafe") ||
		o.Cause != "by the workflow's own account, its code failed after a call the task does not name returned ok" {
		t.Fatalf("%+v", o)
	}
}

// A result with an error's status is never agentd's, whatever record it names: agentd ends an attempt
// that failed with an error, never a result.
func TestAResultNamingAnError(t *testing.T) {
	w := newWorld(t)
	c := w.schedule("git.show_diff")
	seq := c.record(1, statusError, map[string]any{"error": "bad", "result_sha256": resultSHA(t, nil)})
	c.result(1, statusError, valueOf(t, nil, &audit.Head{Seq: seq, Sha256: sha(seq)}), nil)
	w.ok("github.comment_commit")
	w.completed("failed")
	if e := w.explain(); len(incidentsOf(e, KindForeignResult, "whose call ended error")) != 1 {
		t.Fatalf("%v", e.GetIncidents())
	}
}

// An attempt that began (its call.started) and left no record of its end, Temporal saying it failed:
// an error agentd could not record (AuditUnavailable, when the record cannot be written), or a
// failure someone else made of it. Said as Temporal's, unverified, not as a timeout; one Temporal says
// timed out did.
func TestABegunAttemptThatFailedWithoutItsRecord(t *testing.T) {
	for name, tc := range map[string]struct {
		last *failurepb.Failure
		kind string
		says string
	}{
		"failed": {appFailure("agentd could not record the call; retry", "AuditUnavailable", false), KindModelError,
			`attempt 1 began and left no record of its end; Temporal says it failed with "AuditUnavailable: agentd could not record the call; retry", unverified`},
		"timed out": {timeoutFailure(enumspb.TIMEOUT_TYPE_HEARTBEAT), KindActivityTimeout,
			"attempt 1 began and left no record of its end: it timed out"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			w.ok("git.show_diff")
			c := w.schedule("audit_v1")
			w.rec("call.started", audit.Run, map[string]any{"activity_id": c.id, "attempt": 1, "kind": c.kind, "name": c.name})
			c.completes(2, statusOK, nil, nil, tc.last)
			w.ok("github.comment_commit")
			w.completed("ok")
			e := w.explain()
			incs := incidentsOf(e, tc.kind, tc.says)
			if len(incs) != 1 || incs[0].GetAttempt() != 1 || e.GetVerdict() != VerdictOK {
				t.Fatalf("%s: incidents %v", e.GetVerdict(), e.GetIncidents())
			}
		})
	}
}
