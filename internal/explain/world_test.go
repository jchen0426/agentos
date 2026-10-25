package explain

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/budget"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/tasks"
)

// The security audit's task, as agentd configures it: its steps, its report, the steps it requires.
var auditTask = tasks.SecurityAudit(nil, nil, nil)

var t0 = time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)

const runID = "run-1"

// world is a made-up run: its history in Temporal, its records in the audit log and its rows in
// the ledger, as agentd and Temporal would have written them.
type world struct {
	t       *testing.T
	clock   time.Time
	events  []*historypb.HistoryEvent
	calls   int // gateway calls the workflow made, for their activity IDs
	l       Ledger
	run     db.Run
	status  enumspb.WorkflowExecutionStatus
	desc    *workflowservice.DescribeWorkflowExecutionResponse
	noTemp  bool  // Temporal has no execution
	histErr error // what reading the history fails with
	unread  string
	state   string
	now     time.Time // when the explanation is asked for; zero: a moment after the last
	// Every record appended, by seq, whoever's: what a read of one on its own finds, whatever a test
	// did to l.Records. namedErr and attemptErr are the reads of one that fail.
	all        map[int64]db.AuditLog
	namedErr   map[int64]string
	attemptErr map[Attempt]string
}

func newWorld(t *testing.T) *world {
	w := &world{t: t, clock: t0, status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, state: "RUNNING",
		all: map[int64]db.AuditLog{}, namedErr: map[int64]string{}, attemptErr: map[Attempt]string{},
		run: db.Run{WorkflowID: runID, RunID: sql.NullString{String: "exec-1", Valid: true}, Task: "security-audit",
			CreatedAt: t0.Format(time.RFC3339Nano), BudgetLimitMicros: 5_000_000},
		l: Ledger{LogBegan: t0.Add(-time.Hour), Budget: budget.Totals{Limit: 5_000_000}}}
	w.start("agentd.started", audit.Agentd, nil) // the agentd that runs it
	w.rec("run.created", "operator:alice", map[string]any{"task": "security-audit"})
	w.rec("run.started", "operator:alice", map[string]any{"temporal_run_id": "exec-1"})
	w.event(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, nil)
	w.task()
	return w
}

// tick is the next moment: a second after the last.
func (w *world) tick() time.Time {
	w.clock = w.clock.Add(time.Second)
	return w.clock
}

// event appends an event of the history with the attributes given, and returns its ID.
func (w *world) event(typ enumspb.EventType, set func(e *historypb.HistoryEvent)) int64 {
	e := &historypb.HistoryEvent{EventId: int64(len(w.events) + 1), EventType: typ, EventTime: timestamppb.New(w.tick())}
	if set != nil {
		set(e)
	}
	w.events = append(w.events, e)
	return e.EventId
}

// rec appends a record of the run to the audit log, and returns its seq.
func (w *world) rec(typ, actor string, payload map[string]any) int64 {
	return w.append(&w.l.Records, runID, typ, actor, payload)
}

// start appends one of agentd's start records.
func (w *world) start(typ, actor string, payload map[string]any) int64 {
	return w.append(&w.l.Starts, "", typ, actor, payload)
}

func (w *world) append(to *[]db.AuditLog, workflowID, typ, actor string, payload map[string]any) int64 {
	if payload == nil {
		payload = map[string]any{}
	}
	b, err := json.Marshal(payload)
	if err != nil {
		w.t.Fatal(err)
	}
	seq := int64(len(w.all) + 1)
	r := db.AuditLog{Seq: seq, At: w.tick().Format(time.RFC3339Nano), WorkflowID: workflowID, Type: typ,
		Actor: actor, Payload: string(b), Sha256: sha(seq)}
	*to = append(*to, r)
	w.all[seq] = r
	return seq
}

// other appends a record of another run to the audit log: not one the run's rows hold.
func (w *world) other(workflowID, typ string, payload map[string]any) int64 {
	var elsewhere []db.AuditLog
	return w.append(&elsewhere, workflowID, typ, audit.Run, payload)
}

// read is the world's ledger as ReadLedger reads it: its rows (l), and each record want names that
// they do not hold, read on its own: by seq, and, when the rows were cut short, by attempt.
func (w *world) read(want Wanted) (Ledger, error) {
	l := w.l
	l.Named, l.NamedErr, l.AttemptErr = map[int64]db.AuditLog{}, map[int64]string{}, map[Attempt]string{}
	have := map[int64]bool{}
	for _, r := range l.Records {
		have[r.Seq] = true
	}
	for _, seq := range want.Seqs {
		if e, ok := w.namedErr[seq]; ok && !have[seq] {
			l.NamedErr[seq] = e
		} else if r, ok := w.all[seq]; ok && !have[seq] {
			l.Named[seq] = r
		}
	}
	if !l.RecordsCut {
		return l, nil
	}
	for _, a := range want.Attempts {
		if e, ok := w.attemptErr[a]; ok {
			l.AttemptErr[a] = e
			continue
		}
		for seq, r := range w.all {
			x := parse(r)
			if !have[seq] && r.WorkflowID == runID && (x.typ == "call" || x.typ == "call.started") && x.activityID == a.ActivityID &&
				x.attempt == a.Attempt {
				l.Named[seq] = r
			}
		}
	}
	return l, nil
}

// sha is the sha256 the record at seq has in the world.
func sha(seq int64) string { return fmt.Sprintf("%064x", seq) }

// valueOf is a result's value_json: value with the record it names (_audit), if any.
func valueOf(t *testing.T, value map[string]any, anchor *audit.Head) string {
	v := map[string]any{}
	for k, x := range value {
		v[k] = x
	}
	if anchor != nil {
		v["_audit"] = anchor
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// resultSHA is the sha256 agentd's record of a result holds of its value.
func resultSHA(t *testing.T, value map[string]any) string {
	sum, err := audit.ResultSHA256(valueOf(t, value, nil))
	if err != nil {
		t.Fatal(err)
	}
	return sum
}

// task is a workflow task that the Worker completed.
func (w *world) task() {
	w.event(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, nil)
	w.event(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, nil)
	w.event(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, nil)
}

func (w *world) taskTimedOut() int64 {
	w.event(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, nil)
	w.event(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, nil)
	return w.event(enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
			TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE}}
	})
}

func (w *world) taskFailed(message, typ string) int64 {
	w.event(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, nil)
	w.event(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, nil)
	return w.event(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE, Failure: appFailure(message, typ, false)}}
	})
}

func appFailure(message, typ string, nonRetryable bool) *failurepb.Failure {
	return &failurepb.Failure{Message: message, FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
		ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: typ, NonRetryable: nonRetryable}}}
}

func timeoutFailure(typ enumspb.TimeoutType) *failurepb.Failure {
	return &failurepb.Failure{Message: "activity " + typ.String() + " timeout", FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
		TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: typ}}}
}

// call is one gateway call the workflow schedules, as the scenario goes on with it.
type call struct {
	w          *world
	id         string
	kind, name string
	scheduled  int64
	attempt    int32
}

// schedule has the workflow schedule a call of name, of kind model for audit_v1, tool otherwise.
func (w *world) schedule(name string) *call {
	kind := "tool"
	if name == "audit_v1" {
		kind = "model"
	}
	return w.scheduleAs(fmt.Sprintf("%d:%s", w.calls+1, name), kind, name)
}

// scheduleAs has the workflow schedule a call of kind and name under activity ID id, all three its
// own choice: the history's input is what it wrote.
func (w *world) scheduleAs(id, kind, name string) *call {
	w.calls++
	c := &call{w: w, id: id, kind: kind, name: name}
	input, err := converter.GetDefaultDataConverter().ToPayloads(&agentosv1.Call{Kind: kind, Name: name, ArgsJson: "{}"})
	if err != nil {
		w.t.Fatal(err)
	}
	c.scheduled = w.event(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
			ActivityId: c.id, Input: input}}
	})
	return c
}

// record is agentd's call record of an attempt of the call, ending with status, and its extra fields:
// its activity ID, kind and name bounded, as agentd's gateway records them.
func (c *call) record(attempt int32, status string, extra map[string]any) int64 {
	p := map[string]any{"activity_id": audit.Text(c.id), "attempt": attempt, "kind": audit.Text(c.kind), "name": audit.Text(c.name),
		"status": status, "policy": map[string]any{"decision": "allow"}}
	for k, v := range extra {
		p[k] = v
	}
	return c.w.rec("call", audit.Run, p)
}

// fails records an attempt that failed with err, Temporal retrying it.
func (c *call) fails(attempt int32, err string) int64 {
	return c.record(attempt, statusError, map[string]any{"error": err})
}

// serverError is what agentd's git client says when the git tool server fails.
const serverError = "the git tool server failed on show_diff; agentd's log has the details"

// serverFails records an attempt whose git tool server failed, as agentd's gateway records it.
func (c *call) serverFails(attempt int32) int64 {
	return c.record(attempt, statusError, map[string]any{"error": serverError, "error_cause": audit.ErrorCauseToolServer})
}

// completes has the call's attempt complete with status: agentd's record of it, its started and
// completed events, and the workflow task that took the result, which names the record (_audit), as
// agentd's gateway returns it. lastFailure is the attempt before's.
func (c *call) completes(attempt int32, status string, extra map[string]any, value map[string]any, lastFailure *failurepb.Failure) int64 {
	x := map[string]any{"result_sha256": resultSHA(c.w.t, value)}
	for k, v := range extra {
		x[k] = v
	}
	seq := c.record(attempt, status, x)
	c.result(attempt, status, valueOf(c.w.t, value, &audit.Head{Seq: seq, Sha256: sha(seq)}), lastFailure)
	return seq
}

// result has the call's attempt complete with a result of status and value_json, whoever sent it.
func (c *call) result(attempt int32, status, valueJSON string, lastFailure *failurepb.Failure) {
	c.end(attempt, lastFailure, func(e *historypb.HistoryEvent, started int64) {
		res, err := converter.GetDefaultDataConverter().ToPayloads(&agentosv1.Result{Status: status, ValueJson: valueJSON})
		if err != nil {
			c.w.t.Fatal(err)
		}
		e.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
		e.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
			ScheduledEventId: c.scheduled, StartedEventId: started, Result: res}}
	})
}

// stripAnchors makes every result in the history one from before the audit log: it names no record.
func (w *world) stripAnchors() {
	for _, e := range w.events {
		a := e.GetActivityTaskCompletedEventAttributes()
		if a == nil {
			continue
		}
		var res agentosv1.Result
		if err := converter.GetDefaultDataConverter().FromPayloads(a.GetResult(), &res); err != nil {
			w.t.Fatal(err)
		}
		var v map[string]any
		if err := json.Unmarshal([]byte(res.GetValueJson()), &v); err != nil {
			w.t.Fatal(err)
		}
		delete(v, "_audit")
		res.ValueJson = valueOf(w.t, v, nil)
		p, err := converter.GetDefaultDataConverter().ToPayloads(&res)
		if err != nil {
			w.t.Fatal(err)
		}
		a.Result = p
	}
}

// failsForGood has the call end with an error the workflow gets: agentd's record of its last
// attempt, and Temporal's ActivityTaskFailed.
func (c *call) failsForGood(attempt int32, err, typ string, lastFailure *failurepb.Failure) int64 {
	extra := map[string]any{"error": err}
	if typ != "" {
		extra["error_type"] = typ
	}
	seq := c.record(attempt, statusError, extra)
	c.end(attempt, lastFailure, func(e *historypb.HistoryEvent, started int64) {
		e.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
		e.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
			ScheduledEventId: c.scheduled, StartedEventId: started, Failure: appFailure(err, typ, typ != "")}}
	})
	return seq
}

func (c *call) end(attempt int32, lastFailure *failurepb.Failure, set func(e *historypb.HistoryEvent, started int64)) {
	c.attempt = attempt
	started := c.w.event(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
			ScheduledEventId: c.scheduled, Attempt: attempt, LastFailure: lastFailure}}
	})
	c.w.event(enumspb.EVENT_TYPE_UNSPECIFIED, func(e *historypb.HistoryEvent) { set(e, started) })
	c.w.task()
}

// failedAt is when the history's latest ActivityTaskFailed happened.
func (w *world) failedAt(t *testing.T) time.Time {
	t.Helper()
	for i := len(w.events) - 1; i >= 0; i-- {
		if w.events[i].GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED {
			return w.events[i].GetEventTime().AsTime()
		}
	}
	t.Fatal("no call failed")
	return time.Time{}
}

// ok is a call of name that returned ok at its first attempt.
func (w *world) ok(name string) *call {
	c := w.schedule(name)
	c.completes(1, statusOK, nil, nil, nil)
	return c
}

// returns is a call of name that returned status at its first attempt, with the record's extra
// fields and the result's value.
func (w *world) returns(name, status string, extra, value map[string]any) *call {
	c := w.schedule(name)
	c.completes(1, status, extra, value, nil)
	return c
}

// completed ends the run as the workflow returned this outcome.
func (w *world) completed(outcome string) {
	w.status, w.state = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, "SUCCEEDED"
	res, _ := converter.GetDefaultDataConverter().ToPayloads(map[string]any{"outcome": outcome, "pr_url": nil})
	w.event(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
			Result: res}}
	})
}

// failed ends the run as the workflow raised message.
func (w *world) failed(message, typ string) {
	w.status, w.state = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, "FAILED"
	w.event(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
			Failure: appFailure(message, typ, true)}}
	})
}

func (w *world) terminated(reason, identity string) {
	w.status, w.state = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, "FAILED"
	w.event(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
			Reason: reason, Identity: identity}}
	})
}

func (w *world) canceled() {
	w.status, w.state = enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, "CANCELLED"
	w.event(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED, nil)
	w.task()
	w.event(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED, nil)
}

// cancel is a person's cancel, as agentd records it.
func (w *world) cancel(actor string) int64 {
	at := w.tick()
	w.run.CancelledAt = sql.NullString{String: at.Format(time.RFC3339Nano), Valid: true}
	return w.rec("run.cancelled", actor, map[string]any{"cancelled_at": at.Format(time.RFC3339Nano)})
}

// pending sets what Temporal describes of the execution now: its workflow task pending, at attempt,
// scheduled at since, and its activities pending.
func (w *world) pending(task *workflowpb.PendingWorkflowTaskInfo, activities ...*workflowpb.PendingActivityInfo) {
	w.desc = &workflowservice.DescribeWorkflowExecutionResponse{PendingWorkflowTask: task, PendingActivities: activities}
}

// iterator is a history read an event at a time.
type iterator struct {
	events []*historypb.HistoryEvent
	err    error
	i      int
}

func (it *iterator) HasNext() bool { return it.i < len(it.events) || it.err != nil }

func (it *iterator) Next() (*historypb.HistoryEvent, error) {
	if it.i == len(it.events) {
		err := it.err
		it.err = nil
		return nil, err
	}
	it.i++
	return it.events[it.i-1], nil
}

// input is what Explain is given of the world.
func (w *world) input() Input {
	now := w.now
	if now.IsZero() {
		now = w.tick()
	}
	in := Input{RunID: runID, Task: auditTask, Run: w.run, State: w.state, Now: now, Unread: w.unread}
	if !w.noTemp {
		d := w.desc
		if d == nil {
			d = &workflowservice.DescribeWorkflowExecutionResponse{}
		}
		d.WorkflowExecutionInfo = &workflowpb.WorkflowExecutionInfo{Status: w.status,
			Execution: &commonpb.WorkflowExecution{WorkflowId: runID, RunId: "exec-1"}}
		in.Describe = d
		in.History = &iterator{events: w.events, err: w.histErr}
	}
	return in
}

// explain is Explain's explanation of the world.
func (w *world) explain() *agentosv1.Explanation {
	w.t.Helper()
	e, err := Explain(w.input(), w.read)
	if err != nil {
		w.t.Fatal(err)
	}
	return e
}

// report is what the run's report would say now, as agentd renders it during its report call.
func (w *world) report(reason, errorType string) Outcome {
	w.t.Helper()
	o, err := Report(w.input(), w.read, reason, errorType)
	if err != nil {
		w.t.Fatal(err)
	}
	return o
}

var errBoom = errors.New("Temporal is down")
