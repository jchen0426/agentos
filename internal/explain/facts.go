package explain

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/tasks"
)

// Statuses of a gateway call: the results agentd returns (proto Result.status), and how a call
// can end without one.
const (
	statusOK              = "ok"
	statusNeedsApproval   = "needs_approval"
	statusNeedsReconcile  = "needs_reconcile"
	statusBudgetExhausted = "budget_exhausted"
	statusDenied          = "denied"
	statusRefused         = "refused"
	statusInvalidOutput   = "invalid_output"
	statusRejected        = "rejected"
	statusExpired         = "expired"
	statusEdited          = "edited"
	statusError           = "error"     // an attempt failed (a call record's); for the call, the workflow got an error
	statusTimeout         = "timed_out" // Temporal timed the call out
	statusCancelled       = "cancelled" // Temporal cancelled the call
	// The workflow got a result agentd did not send, and agentd has no record of how the call ended.
	statusForeign = "foreign"
	// A write of a run that is not a dry run, which agentd only dry-ran: it did not reach its system.
	statusDryRan = "dry_ran"
)

// waits are the results a workflow waits on, then sends the same call again: for a person, or for
// budget. (An edit sends the reviewer's patch through the steps again: resolve.)
var waits = map[string]bool{statusNeedsApproval: true, statusNeedsReconcile: true, statusBudgetExhausted: true}

// ErrHistoryGone is what a History's iterator fails with when Temporal no longer has the history.
var ErrHistoryGone = errors.New("Temporal no longer has the execution's history")

// Bounds on what Explain reads of a history, which the Worker shapes: Temporal itself keeps up to
// 51,200 events of one execution.
const (
	maxEvents = 100_000
	maxSteps  = 20_000
)

// facts is what Explain found of a run.
type facts struct {
	in        Input
	public    bool // for a report: an internal error's text is not shown, only where it is
	l         Ledger
	history   bool      // the history was read
	truncated int       // events past maxEvents, or steps past maxSteps, not read
	began     time.Time // when the execution started, as its history's first event says
	steps     []*step
	tasks     []*workflowTask // workflow tasks that timed out or failed, in order
	closed    *closing

	bySeq        map[int64]*rec // the run's records, every type and actor, by seq: what a result names
	named        map[int64]*step
	calls        []*rec // call and call.started records of the run's own executions, by seq
	starts       []*rec // agentd.started
	cancel       *rec   // run.cancelled
	terminations []*rec
	extensions   []*rec          // budget.extended
	reconciled   []*rec          // effect.reconciled
	decisions    map[string]*rec // approval.decided, by approval ID

	missing []string
	extra   []*agentosv1.Incident // incidents the verdict found
}

// rec is a record of the audit log, as Explain reads it.
type rec struct {
	seq         int64
	at          time.Time
	workflowID  string
	typ, actor  string
	sha256      string // the record's own
	execution   string // an unknown-run call record's: the Temporal run ID of the execution that made the call
	activityID  string
	attempt     int32
	kind, name  string
	status      string
	resultSHA   string // a call record's result_sha256
	err         string
	errType     string // the type of the failure Temporal has of the attempt (the SDK's, of agentd's error)
	hasErrType  bool   // the record says the type: every error record does, but those agentd wrote before it did
	errCause    string // agentd's word for where an error came from: tool_server
	refusedBy   string // a denied call's: who refused it (audit.RefusedBy*)
	decision    string // the policy's
	policyWhy   string
	reason      string
	why         string // run.terminated
	amount      int64  // budget.extended
	approvalID  string
	effectID    string
	decidedBy   string // approval.decided: the name agentctl gave
	decidedKind string // approval.decided: approve, reject, edit
	executed    bool   // effect.reconciled
}

// step is one gateway call of the run: an activity the workflow scheduled, with the records agentd
// wrote of its attempts.
type step struct {
	f           *facts
	step        string // the task's name for it, else its kind and name, quoted
	mapped      bool   // step is the task's own name for it (D12)
	id          string // the activity ID, as the workflow named it
	kind, name  string
	scheduled   int64 // its ActivityTaskScheduled; 0 without a history
	scheduledAt time.Time
	attempt     int32 // the attempt that ended it, or the latest
	started     int64 // the ActivityTaskStarted of that attempt
	startedAt   time.Time
	lastFailure *failurepb.Failure // of the attempt before it, as Temporal kept it
	end         int64              // the event that ended it
	endAt       time.Time
	endType     enumspb.EventType
	failure     *failurepb.Failure // failed, timed out

	// completed: its result as the history has it, which counts only once it names agentd's record
	// of it (settle).
	result     string           // the result's status
	anchor     *audit.Head      // the record it names (_audit)
	resultSHA  string           // the sha256 of its value, as agentd's record of it holds one (audit.ResultSHA256)
	unreadable string           // why the result could not be read
	value      map[string]bool  // the value's fields that hold something
	numbers    map[string]int64 // its integer fields: what a request short of budget needed, and what was left
	foreign    string           // why the result (or the failure) is not agentd's, when it is not
	foreignErr bool             // what is not agentd's is a failure, not a result
	// The failure that ended it is Temporal's word only: agentd has no record of the attempt to hold
	// it to (an error agentd could not record, or a failure someone else made of an attempt it never
	// ran), and it did not stop during it.
	unrecorded bool
	unchecked  bool   // agentd's record of how it ended could not be read: it is not judged on
	records    []*rec // its call and call.started records, by seq
	final      string // what the workflow got: a result's status, error, timed_out, cancelled; "" while open
	finalRec   *rec   // the call record of it
	isReport   bool
	resolved   bool // a wait, an edit or a report that a later call went on from
}

// workflowTask is a workflow task that timed out, or that the Worker failed.
type workflowTask struct {
	event    int64
	at       time.Time
	timedOut bool
	timeout  enumspb.TimeoutType
	cause    string // failed: the Worker's cause
	message  string // failed: the Worker's message
	after    *step  // the last call that had ended before it: the result it was handling
}

// closing is the history's last event, when the execution has ended.
type closing struct {
	event            int64
	at               time.Time
	typ              enumspb.EventType
	result           string // completed: the outcome the workflow returned (its claim)
	message          string // failed: its failure's message (its claim)
	reason, identity string // terminated: why, and by whom, as whoever terminated it said
}

// gather reads the history, then the ledger, into facts.
func gather(in Input, read func(Wanted) (Ledger, error)) (*facts, error) {
	f := &facts{in: in, decisions: map[string]*rec{}, bySeq: map[int64]*rec{}, named: map[int64]*step{}}
	if in.History != nil {
		if err := f.readHistory(in.History); err != nil {
			return nil, err
		}
	}
	l, err := read(f.wanted())
	if err != nil {
		return nil, err
	}
	f.l = l
	f.readRecords()
	if !f.history {
		f.stepsFromRecords()
	}
	f.attach()
	for _, s := range f.steps {
		f.settle(s)
	}
	f.resolve()
	f.sources()
	return f, nil
}

// wanted is what the history needs of the ledger: the record each result names, and agentd's
// records of each attempt that ended a call with a failure.
func (f *facts) wanted() Wanted {
	var w Wanted
	for _, s := range f.steps {
		switch {
		case s.anchor != nil:
			w.Seqs = append(w.Seqs, s.anchor.Seq)
		case s.endType == enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			w.Attempts = append(w.Attempts, Attempt{ActivityID: audit.Text(s.id), Attempt: s.attempt})
		}
	}
	return w
}

// resolve marks what a later call went on from: a wait, sent again (the same kind and name); an
// edit, whose patch the later calls check; a report, sent again.
func (f *facts) resolve() {
	for i, s := range f.steps {
		for _, later := range f.steps[i+1:] {
			same := later.kind == s.kind && later.name == s.name
			if (s.final == statusEdited && !later.isReport) || ((waits[s.final] || s.isReport) && same) {
				s.resolved = true
				break
			}
		}
	}
}

// readRecords reads the run's records, and agentd's starts.
func (f *facts) readRecords() {
	for _, r := range f.l.Records {
		x := parse(r)
		f.bySeq[x.seq] = x
		switch x.typ {
		case "call", "call.started":
			if !f.ofTheRun(x) {
				continue // an execution agentd did not start, under the run's workflow ID
			}
			f.calls = append(f.calls, x)
		case "run.cancelled":
			f.cancel = x
		case "run.terminating", "run.terminated", "run.terminate_failed":
			// agentd's termination of the run's execution: run.terminating before it asked Temporal,
			// then run.terminated, or run.terminate_failed when Temporal did not (termination).
			if x.actor == audit.Agentd && (x.execution == "" || x.execution == f.in.Run.RunID.String) {
				f.terminations = append(f.terminations, x)
			}
		case "budget.extended":
			f.extensions = append(f.extensions, x)
		case "effect.reconciled":
			f.reconciled = append(f.reconciled, x)
		case "approval.decided":
			f.decisions[x.approvalID] = x
		}
	}
	for _, r := range f.l.Starts {
		if r.Type == "agentd.started" {
			f.starts = append(f.starts, parse(r))
		}
	}
	// The records read on their own (Named): what results name, whichever run's, and the run's
	// records of its failed attempts past the bound.
	named := make([]int64, 0, len(f.l.Named))
	for seq := range f.l.Named {
		named = append(named, seq)
	}
	slices.Sort(named)
	for _, seq := range named {
		x := parse(f.l.Named[seq])
		f.bySeq[x.seq] = x
		if x.workflowID == f.in.RunID && (x.typ == "call" || x.typ == "call.started") && f.ofTheRun(x) {
			f.calls = append(f.calls, x)
		}
	}
	slices.SortFunc(f.calls, func(a, b *rec) int { return cmp.Compare(a.seq, b.seq) })
}

// ofTheRun reports whether a call record under the run's workflow ID is of a call from the run's own
// execution: one agentd knew as the run's (actor run), or one it refused before StartRun recorded the
// execution's ID (unknown-run, from the execution ReadLedger reads the records of).
func (f *facts) ofTheRun(x *rec) bool {
	return x.actor != audit.UnknownRun || x.execution != "" && x.execution == f.in.Run.RunID.String
}

// parse reads a record's payload.
func parse(r db.AuditLog) *rec {
	at, _ := time.Parse(time.RFC3339Nano, r.At)
	x := &rec{seq: r.Seq, at: at, workflowID: r.WorkflowID, typ: r.Type, actor: r.Actor, sha256: r.Sha256}
	var p struct {
		ActivityID   string  `json:"activity_id"`
		Attempt      int32   `json:"attempt"`
		Kind         string  `json:"kind"`
		Name         string  `json:"name"`
		Status       string  `json:"status"`
		ResultSHA256 string  `json:"result_sha256"`
		Error        string  `json:"error"`
		ErrorType    *string `json:"error_type"`
		ErrorCause   string  `json:"error_cause"`
		RefusedBy    string  `json:"refused_by"`
		Execution    string  `json:"temporal_run_id"`
		Policy       struct {
			Decision string `json:"decision"`
			Reason   string `json:"reason"`
		} `json:"policy"`
		Reason     string `json:"reason"`
		Why        string `json:"why"`
		Amount     int64  `json:"amount_micros"`
		ApprovalID string `json:"approval_id"`
		EffectID   string `json:"effect_id"`
		DecidedBy  string `json:"decided_by"`
		Decision   string `json:"decision"`
		Executed   bool   `json:"executed"`
	}
	if json.Unmarshal([]byte(r.Payload), &p) != nil {
		return x
	}
	x.activityID, x.attempt, x.kind, x.name = p.ActivityID, p.Attempt, p.Kind, p.Name
	x.status, x.resultSHA, x.err, x.errCause = p.Status, p.ResultSHA256, p.Error, p.ErrorCause
	if p.ErrorType != nil {
		x.errType, x.hasErrType = *p.ErrorType, true
	}
	x.refusedBy, x.reason, x.why, x.execution = p.RefusedBy, p.Reason, p.Why, p.Execution
	x.decision, x.policyWhy = p.Policy.Decision, p.Policy.Reason
	x.amount, x.approvalID, x.effectID = p.Amount, p.ApprovalID, p.EffectID
	x.decidedBy, x.decidedKind, x.executed = p.DecidedBy, p.Decision, p.Executed
	return x
}

// readHistory reads the execution's history, an event at a time, keeping what explains it.
func (f *facts) readHistory(h History) error {
	byScheduled := map[int64]*step{}
	var lastEnded *step
	read := 0
	for h.HasNext() {
		ev, err := h.Next()
		if errors.Is(err, ErrHistoryGone) {
			f.missing = append(f.missing, "temporal: "+err.Error())
			break
		}
		if err != nil {
			return fmt.Errorf("the history of run %s: %w", f.in.RunID, err)
		}
		f.history = true
		if read++; read > maxEvents {
			f.truncated++
			continue
		}
		at := ev.GetEventTime().AsTime()
		switch ev.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
			f.began = at
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			if len(f.steps) == maxSteps {
				f.truncated++
				continue
			}
			a := ev.GetActivityTaskScheduledEventAttributes()
			s := &step{f: f, id: a.GetActivityId(), scheduled: ev.GetEventId(), scheduledAt: at}
			var c agentosv1.Call
			if converter.GetDefaultDataConverter().FromPayloads(a.GetInput(), &c) == nil {
				s.kind, s.name = c.GetKind(), c.GetName()
			}
			byScheduled[ev.GetEventId()] = s
			f.steps = append(f.steps, s)
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			a := ev.GetActivityTaskStartedEventAttributes()
			if s := byScheduled[a.GetScheduledEventId()]; s != nil {
				s.started, s.startedAt, s.attempt, s.lastFailure = ev.GetEventId(), at, a.GetAttempt(), a.GetLastFailure()
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			a := ev.GetActivityTaskCompletedEventAttributes()
			if s := byScheduled[a.GetScheduledEventId()]; s != nil {
				s.end, s.endAt, s.endType = ev.GetEventId(), at, ev.GetEventType()
				s.readResult(a)
				lastEnded = s
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			a := ev.GetActivityTaskFailedEventAttributes()
			if s := byScheduled[a.GetScheduledEventId()]; s != nil {
				s.end, s.endAt, s.endType, s.failure = ev.GetEventId(), at, ev.GetEventType(), a.GetFailure()
				lastEnded = s
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			a := ev.GetActivityTaskTimedOutEventAttributes()
			if s := byScheduled[a.GetScheduledEventId()]; s != nil {
				s.end, s.endAt, s.endType, s.failure = ev.GetEventId(), at, ev.GetEventType(), a.GetFailure()
				lastEnded = s
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
			a := ev.GetActivityTaskCanceledEventAttributes()
			if s := byScheduled[a.GetScheduledEventId()]; s != nil {
				s.end, s.endAt, s.endType = ev.GetEventId(), at, ev.GetEventType()
				lastEnded = s
			}
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
			a := ev.GetWorkflowTaskTimedOutEventAttributes()
			f.tasks = append(f.tasks, &workflowTask{event: ev.GetEventId(), at: at, timedOut: true,
				timeout: a.GetTimeoutType(), after: lastEnded})
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:
			a := ev.GetWorkflowTaskFailedEventAttributes()
			f.tasks = append(f.tasks, &workflowTask{event: ev.GetEventId(), at: at,
				cause:   a.GetCause().String(),
				message: failureText(a.GetFailure()), after: lastEnded})
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
			f.closed = &closing{event: ev.GetEventId(), at: at, typ: ev.GetEventType(),
				result: outcome(ev.GetWorkflowExecutionCompletedEventAttributes().GetResult().GetPayloads())}
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
			f.closed = &closing{event: ev.GetEventId(), at: at, typ: ev.GetEventType(),
				message: failureText(ev.GetWorkflowExecutionFailedEventAttributes().GetFailure())}
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
			a := ev.GetWorkflowExecutionTerminatedEventAttributes()
			f.closed = &closing{event: ev.GetEventId(), at: at, typ: ev.GetEventType(), reason: a.GetReason(),
				identity: a.GetIdentity()}
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
			f.closed = &closing{event: ev.GetEventId(), at: at, typ: ev.GetEventType()}
		}
	}
	if f.truncated > 0 {
		f.missing = append(f.missing, fmt.Sprintf("temporal: the history is longer than agentd reads (%d events, "+
			"%d gateway calls): what comes after is not explained", maxEvents, maxSteps))
	}
	return nil
}

// readResult reads a completed call's result as the history has it: its status, the record it
// names, the sha256 agentd's record of it would hold, and which of its fields hold something.
func (s *step) readResult(a *historypb.ActivityTaskCompletedEventAttributes) {
	var res agentosv1.Result
	if err := converter.GetDefaultDataConverter().FromPayloads(a.GetResult(), &res); err != nil {
		s.unreadable = "its result is not a gateway result"
		return
	}
	s.result = res.GetStatus()
	sum, err := audit.ResultSHA256(res.GetValueJson())
	if err != nil {
		s.unreadable = "its result's value cannot be read"
		return
	}
	s.resultSHA = sum
	dec := json.NewDecoder(strings.NewReader(res.GetValueJson()))
	dec.UseNumber()
	var v map[string]json.RawMessage
	if dec.Decode(&v) != nil {
		s.unreadable = "its result's value cannot be read"
		return
	}
	if raw, ok := v["_audit"]; ok {
		var h audit.Head
		if json.Unmarshal(raw, &h) == nil {
			s.anchor = &h
		}
		delete(v, "_audit")
	}
	s.value, s.numbers = map[string]bool{}, map[string]int64{}
	for k, raw := range v {
		switch t := string(bytes.TrimSpace(raw)); t {
		case "null", `""`, "[]", "{}", "false":
		default:
			s.value[k] = true
			if n, err := strconv.ParseInt(t, 10, 64); err == nil {
				s.numbers[k] = n
			}
		}
	}
}

// outcome is what the workflow's result says it reported: its outcome, or the result as it is.
func outcome(payloads []*commonpb.Payload) string {
	if len(payloads) == 0 {
		return ""
	}
	data := payloads[0].GetData()
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v map[string]any
	if dec.Decode(&v) == nil {
		if o, ok := v["outcome"].(string); ok {
			return o
		}
	}
	return string(data)
}

// failureText is a failure's message, after its application error's type when the message does not
// name it already.
func failureText(fl *failurepb.Failure) string {
	if fl == nil {
		return ""
	}
	if t := fl.GetApplicationFailureInfo().GetType(); t != "" && !strings.Contains(fl.GetMessage(), t) {
		return t + ": " + fl.GetMessage()
	}
	return fl.GetMessage()
}

// stepsFromRecords is the run's calls without a history: one step for each activity ID, in the order
// the call records first name it; or, without records either, the calls table's.
func (f *facts) stepsFromRecords() {
	seen := map[string]*step{}
	for _, r := range f.calls {
		if seen[r.activityID] == nil {
			seen[r.activityID] = &step{f: f, id: r.activityID, kind: r.kind, name: r.name}
			f.steps = append(f.steps, seen[r.activityID])
		}
	}
	if len(f.steps) > 0 {
		return
	}
	for _, c := range f.l.Calls {
		f.steps = append(f.steps, &step{f: f, id: c.ActivityID, kind: c.Kind, name: c.Name, final: c.Outcome.String})
	}
}

// attach gives each call record to its step: the latest scheduled under its activity ID by the time
// it was written. A workflow names each call afresh; one that reuses an ID gets its records split
// by time.
func (f *facts) attach() {
	byID := map[string][]*step{}
	for _, s := range f.steps {
		key := audit.Text(s.id)
		byID[key] = append(byID[key], s)
	}
	for _, r := range f.calls {
		candidates := byID[r.activityID]
		if len(candidates) == 0 {
			continue // the history was cut short, or does not name the call: nothing to give it to
		}
		pick := candidates[0]
		for _, s := range candidates[1:] {
			if !s.scheduledAt.After(r.at.Add(time.Second)) {
				pick = s
			}
		}
		pick.records = append(pick.records, r)
	}
}

// settle finds what the workflow got from the step, and agentd's record of it. A result counts as
// agentd's when it names agentd's record of it (anchored), or comes from before the audit log; one
// that does not came from someone else in Temporal's namespace, and the step is what agentd's record
// of its attempt says. Without a history, the step is what its latest record says.
func (f *facts) settle(s *step) {
	calls := s.callRecords()
	switch s.endType {
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		r, why := f.anchored(s)
		switch {
		case r != nil:
			s.final, s.finalRec = r.status, r
		case why == "":
			s.final = s.result
		case why == unread:
			// agentd's record could not be read: the result is not judged on (the verdict is unknown).
			s.final, s.unchecked, s.value, s.numbers = s.result, true, nil, nil
		default:
			s.foreign, s.value, s.numbers = why, nil, nil
			s.final = statusForeign
			if r := s.latestCall(s.attempt); r != nil {
				s.final, s.finalRec = r.status, r
			}
		}
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		// agentd's record of the attempt that failed; without one, Temporal's failure says how it
		// ended, not an earlier attempt's record. A failure agentd's record of its attempt does not
		// bear out came from someone else in Temporal's namespace: the step is what the record says.
		s.final = statusError
		switch r, why := f.failedBy(s); {
		case why == "":
			s.finalRec = r
		case why == unread:
			s.unchecked = true
		case why == unrecorded:
			s.unrecorded = true
		default:
			s.foreign, s.foreignErr, s.final = why, true, statusForeign
			if r != nil {
				s.final, s.finalRec = r.status, r
			}
		}
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		s.final = statusTimeout
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		s.final = statusCancelled
	default:
		if !f.history && len(calls) > 0 {
			s.finalRec = calls[len(calls)-1]
			s.final, s.attempt = s.finalRec.status, s.finalRec.attempt
		}
	}
	// A run that is not a dry run whose write agentd only dry-ran (an agentd that could not reach the
	// tool's system took it up): the write did not reach it, whatever the result says.
	if s.final == statusOK && f.in.Run.DryRun == 0 && s.value["dry_run"] {
		s.final = statusDryRan
	}
	// What agentd ran is what its records say, the record of how the call ended first: the history's
	// input is the Worker's.
	switch {
	case s.finalRec != nil:
		s.kind, s.name = s.finalRec.kind, s.finalRec.name
	case len(s.records) > 0:
		s.kind, s.name = s.records[0].kind, s.records[0].name
	}
	if s.attempt == 0 && s.finalRec != nil {
		s.attempt = s.finalRec.attempt
	}
	c := tasks.Call{Kind: s.kind, Name: s.name}
	s.step, s.mapped = f.stepName(c), false
	if _, ok := f.in.Task.Step(c); ok {
		s.mapped = true
	}
	s.isReport = f.in.Task.Report != (tasks.Call{}) && c == f.in.Task.Report
}

// unread: agentd could not read its record of how the call ended (the read of it on its own failed).
const unread = "unread"

// unrecorded: agentd has no record of the attempt whose failure ended the call, and nothing explains
// why (it did not stop during it, and the execution is not older than the audit log).
const unrecorded = "unrecorded"

// anchored is agentd's record that the step's result names, by A's anchor rules (control's
// checkAnchors, stricter on the actor): a call record of this run's own execution, the record named
// by its seq and sha256, of the step's activity ID and of the attempt that completed it, ending as
// the result says (never with an error), holding the result's sha256, and named by no other result
// before it. Otherwise it says why the result is not agentd's: "" for a result of an execution older
// than the audit log, which names none; unread when agentd could not read the record it names.
func (f *facts) anchored(s *step) (*rec, string) {
	if s.unreadable != "" {
		return nil, s.unreadable
	}
	a := s.anchor
	if a == nil {
		if f.l.LogBegan.IsZero() || f.began.Before(f.l.LogBegan) {
			return nil, ""
		}
		return nil, "it names no record of agentd's (_audit)"
	}
	r := f.bySeq[a.Seq]
	if _, failed := f.l.NamedErr[a.Seq]; r == nil && failed {
		return nil, unread
	}
	first, again := f.named[a.Seq]
	if !again {
		f.named[a.Seq] = s
	}
	switch {
	case r == nil:
		return nil, fmt.Sprintf("it names seq %d, where the audit log has no record", a.Seq)
	case r.workflowID != f.in.RunID:
		return nil, fmt.Sprintf("it names seq %d, another run's record", a.Seq)
	case r.sha256 != a.Sha256:
		return nil, fmt.Sprintf("it names seq %d with a sha256 the record there does not have", a.Seq)
	case r.typ != "call":
		return nil, fmt.Sprintf("it names seq %d, a %s record, not a call's", a.Seq, r.typ)
	case r.actor != audit.Run:
		return nil, fmt.Sprintf("it names seq %d, the record of a call from an execution agentd did not start", a.Seq)
	case r.activityID != audit.Text(s.id):
		return nil, fmt.Sprintf("it names seq %d, another call's record (%q)", a.Seq, r.activityID)
	case again && first != s:
		return nil, fmt.Sprintf("it names seq %d, which the result of %q names too", a.Seq, first.id)
	case r.status == statusError || r.status != s.result:
		return nil, fmt.Sprintf("it names seq %d, whose call ended %s, not %s", a.Seq, r.status, s.result)
	case r.attempt != s.attempt:
		return nil, fmt.Sprintf("it names seq %d, attempt %d's record; attempt %d completed the call", a.Seq, r.attempt, s.attempt)
	case r.resultSHA != s.resultSHA:
		return nil, fmt.Sprintf("it names seq %d, whose result has another sha256", a.Seq)
	}
	return r, ""
}

// failedBy is agentd's record of the attempt whose failure ended the step, when the failure is
// agentd's: the record says the attempt ended with an error of the type Temporal's failure has (a
// record agentd wrote before it said the type is taken on its status). Otherwise it says why the
// failure is not agentd's, the record contradicting it; or that agentd has no record of the attempt:
// "" when the execution is older than the audit log or agentd started again during the attempt,
// cutting it short with its record, unrecorded when nothing explains it (Temporal's failure is then
// all there is, unverified); unread when agentd's record of it could not be read. A timeout is
// Temporal's own.
func (f *facts) failedBy(s *step) (*rec, string) {
	r := s.record("call", s.attempt)
	typ := s.failure.GetApplicationFailureInfo().GetType()
	_, failed := f.l.AttemptErr[Attempt{ActivityID: audit.Text(s.id), Attempt: s.attempt}]
	switch {
	case r == nil && failed:
		return nil, unread
	case r == nil && (f.l.LogBegan.IsZero() || f.began.Before(f.l.LogBegan)):
		return nil, ""
	case r == nil && f.startBetween(s.startedAt, s.endAt.Add(time.Second)) != nil:
		return nil, ""
	case r == nil:
		return nil, unrecorded
	case r.status != statusError:
		return r, fmt.Sprintf("agentd's record of attempt %d says it returned %s", s.attempt, r.status)
	case r.hasErrType && r.errType != typ:
		return r, fmt.Sprintf("agentd's record of attempt %d says it failed with an error of type %q, not %q", s.attempt,
			r.errType, typ)
	}
	return r, ""
}

// stepName is the task's name for the step call c makes (D12), else its kind and name, quoted: the
// workflow chose them.
func (f *facts) stepName(c tasks.Call) string {
	if name, ok := f.in.Task.Step(c); ok {
		return name
	}
	return strconv.Quote(strings.TrimSpace(c.Kind + " " + c.Name))
}

// callRecords are the step's call records (not its call.started ones), by seq.
func (s *step) callRecords() []*rec {
	var out []*rec
	for _, r := range s.records {
		if r.typ == "call" {
			out = append(out, r)
		}
	}
	return out
}

// latestCall is the step's latest call record of attempt n, else its latest call record.
func (s *step) latestCall(n int32) *rec {
	if r := s.record("call", n); r != nil {
		return r
	}
	calls := s.callRecords()
	if len(calls) == 0 {
		return nil
	}
	return calls[len(calls)-1]
}

// record is the step's record of type typ of attempt n, the latest when there are several.
func (s *step) record(typ string, n int32) *rec {
	var found *rec
	for _, r := range s.records {
		if r.typ == typ && r.attempt == n {
			found = r
		}
	}
	return found
}

// bad reports whether the step's result changed the run's course: anything but ok, and but what a
// later call went on from.
func (s *step) bad() bool { return s.final != "" && s.final != statusOK && !s.resolved }

// accepted reports whether the workflow got the step's result ok, and agentd sent it.
func (s *step) accepted() bool {
	return s.end != 0 && s.final == statusOK && s.foreign == "" && !s.unchecked
}

// outcome says how the step ended, after its name: "returned ok", "failed".
func (s *step) outcome() string {
	switch s.final {
	case "":
		return "has not ended"
	case statusError:
		return "failed"
	case statusTimeout:
		return "timed out"
	case statusCancelled:
		return "was cancelled"
	case statusForeign:
		if s.foreignErr {
			return "failed by a failure agentd did not send"
		}
		return "returned a result agentd did not send"
	case statusDryRan:
		return "was only dry-run"
	}
	return "returned " + s.final
}

// evidence is where the step's end is: its call record and the event that ended it.
func (s *step) evidence() []*agentosv1.Evidence {
	var out []*agentosv1.Evidence
	if r := s.finalRec; r != nil {
		what := fmt.Sprintf("call %q, attempt %d: %s", s.id, r.attempt, r.status)
		if r.status == statusError && r.err != "" {
			what += quoted(": ", r.err)
		}
		out = append(out, auditSource(r.seq, what))
	}
	if s.end != 0 {
		out = append(out, temporalSource(s.end, fmt.Sprintf("%s of %q", eventName(s.endType), s.id)))
	} else if s.started != 0 {
		out = append(out, temporalSource(s.started, fmt.Sprintf("ActivityTaskStarted of %q, attempt %d", s.id, s.attempt)))
	}
	return out
}

// firstUncheckedOrBad is the first step whose end agentd could not check, or whose result changed
// the run's course.
func (f *facts) firstUncheckedOrBad() *step {
	for _, s := range f.steps {
		if s.unchecked || s.bad() {
			return s
		}
	}
	return nil
}

// uncheckedWhy is why agentd could not read its record of how the step ended.
func (f *facts) uncheckedWhy(s *step) string {
	if a := s.anchor; a != nil {
		return f.l.NamedErr[a.Seq]
	}
	return f.l.AttemptErr[Attempt{ActivityID: audit.Text(s.id), Attempt: s.attempt}]
}

// firstBad is the first step whose result changed the run's course.
func (f *facts) firstBad() *step {
	for _, s := range f.steps {
		if s.bad() {
			return s
		}
	}
	return nil
}

// missingRequired is the first call the task requires (D12) that the run has no accepted result of,
// and why it is required. A requirement on a condition holds once the latest accepted result of the
// call it names holds the field.
func (f *facts) missingRequired() (tasks.Call, string, bool) {
	for _, r := range f.in.Task.Required {
		cond := ""
		if w := r.When; w != nil {
			if a := f.accepted(w.Call); a == nil || !a.value[w.Field] {
				continue
			}
			cond = fmt.Sprintf(", once %s's result holds %s", f.stepName(w.Call), w.Field)
		}
		if f.accepted(r.Call) == nil {
			return r.Call, fmt.Sprintf("the run has no accepted result of %s, which the task requires%s", f.stepName(r.Call), cond), true
		}
	}
	return tasks.Call{}, "", false
}

// accepted is the run's latest step of call c whose result agentd sent and the workflow got ok.
func (f *facts) accepted(c tasks.Call) *step {
	var found *step
	for _, s := range f.steps {
		if s.kind == c.Kind && s.name == c.Name && s.accepted() {
			found = s
		}
	}
	return found
}

// report is the task's report call, the run's last: nil when the run made none.
func (f *facts) report() *step {
	for i := len(f.steps) - 1; i >= 0; i-- {
		if f.steps[i].isReport {
			return f.steps[i]
		}
	}
	return nil
}

// lastBefore is the last step before r that is not a report.
func (f *facts) lastBefore(r *step) *step {
	var last *step
	for _, s := range f.steps {
		if s == r {
			break
		}
		if !s.isReport {
			last = s
		}
	}
	return last
}

// lastStep is the run's last step.
func (f *facts) lastStep() *step {
	if len(f.steps) == 0 {
		return nil
	}
	return f.steps[len(f.steps)-1]
}

// current is where the run is: its last step still open, else its last step.
func (f *facts) current() *step {
	for i := len(f.steps) - 1; i >= 0; i-- {
		if f.steps[i].final == "" && f.steps[i].end == 0 && f.history {
			return f.steps[i]
		}
	}
	return f.lastStep()
}

// What agentd's records say of its latest termination of the run's execution (termination).
const (
	terminationDone    = "done"    // run.terminated; or run.terminating, and agentd stopped right after it
	terminationFailed  = "failed"  // run.terminating, then run.terminate_failed: Temporal did not
	terminationUnknown = "unknown" // run.terminating, and nothing of how it went
)

// termination is agentd's latest termination of the run's execution, by its records: what it asked
// (the run.terminating, or a run.terminated alone, of an agentd from before run.terminating), how
// that went, and the run.terminate_failed when Temporal did not. A run.terminating is closed by its
// run.terminated, or by its run.terminate_failed; or by agentd's next start only when no other record
// of the agentd that wrote it comes between them (the next record of the log is that start: agentd
// stopped right after it asked). Otherwise how it went is not known: agentd kept running, and
// recorded nothing of it.
func (f *facts) termination() (t *rec, outcome string, failed *rec) {
	for _, x := range f.terminations {
		switch x.typ {
		case "run.terminating":
			t, outcome, failed = x, "", nil
		case "run.terminate_failed":
			if t != nil && outcome == "" {
				outcome, failed = terminationFailed, x
			}
		case "run.terminated":
			if t == nil || outcome != "" {
				t = x
			}
			outcome, failed = terminationDone, nil
		}
	}
	if t == nil || outcome != "" {
		return t, outcome, failed
	}
	for _, x := range f.starts {
		if x.seq == t.seq+1 {
			return t, terminationDone, nil
		}
	}
	return t, terminationUnknown, nil
}

// pendingAttempt is the attempt of the step under way, as Temporal describes it: 0 when it is not.
func (f *facts) pendingAttempt(s *step) int32 {
	for _, p := range f.in.Describe.GetPendingActivities() {
		if p.GetActivityId() == s.id {
			return p.GetAttempt()
		}
	}
	return 0
}

// sources notes what the explanation could not read.
func (f *facts) sources() {
	run := f.in.Run
	if run.RunID.Valid && f.in.Describe == nil {
		why := "Temporal no longer has the execution (past the namespace's retention, or a dev server that lost its state)"
		if f.in.Unread != "" {
			why = "agentd could not read the execution in Temporal: " + f.in.Unread
		}
		f.missing = append(f.missing, "temporal: "+why)
	}
	if n := f.l.Unknown; n > 0 {
		f.missing = append(f.missing, fmt.Sprintf("audit log: %d records under the run's workflow ID are of calls from "+
			"executions agentd did not start: not the run's, and not read", n))
	}
	if len(f.l.Records) > 0 {
		return
	}
	created, _ := time.Parse(time.RFC3339Nano, run.CreatedAt)
	switch {
	case f.l.LogBegan.IsZero():
		f.missing = append(f.missing, "audit log: agentd has no audit log yet")
	case created.Before(f.l.LogBegan):
		f.missing = append(f.missing, "audit log: the run predates it (it began at "+stamp(f.l.LogBegan)+")")
	default:
		f.missing = append(f.missing, "audit log: no record of the run, though the log began before it")
	}
}
