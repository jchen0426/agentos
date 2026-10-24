// Package explain says why a run is where it is (design doc, section 5, item 9): it reads the run's
// history in Temporal with agentd's ledger, the audit log among it, and names the step that went
// wrong and why, apart from the trouble the run met along the way. Explain is for agentctl runs
// explain, and Report, the same rules over the calls before a run's report, for the report itself.
//
// Trust. The verdict, the step and the kind come from what agentd recorded itself (the call records
// of the audit log, the effects, approvals, model requests and budget, agentd's own starts) and from
// what Temporal recorded: event types, timeouts and attempts. A result in the history counts as
// agentd's only when it names the record agentd wrote of it, by A's anchor rules (control's
// checkAnchors); one that does not was put there by someone else in Temporal's namespace, and the
// call is judged by agentd's record of it. The Worker writes its result's outcome, its failure
// messages, the activity IDs it schedules and the names of the calls it makes: they are shown as its
// claim, quoted, and never judged on. Every text from the Worker, a tool, a model or the outside
// world leaves bounded, as one line of printable text without runs of spaces (internal/safetext).
//
// A crash leaves no record of its own: each is inferred from what it leaves, and the explanation
// says so.
//   - The Worker: a workflow task timed out, as Temporal times out a task whose Worker stopped or hung.
//   - agentd: an attempt of a gateway call ended without its call record, and agentd started again
//     (agentd.started) after the call was scheduled and before its next attempt began. An attempt
//     with a call.started record and no call record began and never ended, whatever the times say.
//   - The git tool server: the attempt's call record says its error came from the server
//     (error_cause), as agentd's git client types it (gitmcp.ServerFailure).
package explain

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/budget"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/tasks"
)

// Verdicts: what became of the run, or what it is doing.
const (
	VerdictOK          = "ok"           // ended well; trouble along the way is in the incidents
	VerdictStepFailed  = "step_failed"  // ended, but a step's result changed its course, or a step it needs is missing
	VerdictNotApproved = "not_approved" // ended without a write a person rejected, or nobody approved in time
	VerdictFailed      = "failed"       // its report did not go out, or the workflow raised
	VerdictCancelled   = "cancelled"    // a person cancelled it: it has ended, or is ending
	VerdictTerminated  = "terminated"
	VerdictWaiting     = "waiting" // for a person: Next says how
	VerdictStuck       = "stuck"   // its workflow tasks, or a call's attempts, keep failing; or no Worker takes them
	VerdictRunning     = "running"
	VerdictUnknown     = "unknown" // Temporal no longer has the execution, or could not be read
)

// Kinds: the categories of a cause.
const (
	KindModelRefused       = "model_refused"
	KindModelInvalidOutput = "model_invalid_output"
	KindModelError         = "model_error"
	KindToolError          = "tool_error"
	KindToolDenied         = "tool_denied" // the tool refused the call as it stood
	KindToolServerFailed   = "tool_server_failed"
	KindPolicyDenied       = "policy_denied"
	KindDenied             = "denied" // agentd refused the call otherwise, or its record does not say who did
	KindLoopGuard          = "loop_guard"
	KindBudgetExhausted    = "budget_exhausted"
	KindApprovalPending    = "approval_pending"
	KindApprovalRejected   = "approval_rejected"
	KindApprovalExpired    = "approval_expired"
	KindApprovalEdited     = "approval_edited"
	KindReconcilePending   = "reconcile_pending"
	KindReportNotPosted    = "report_not_posted"
	KindStepMissing        = "step_missing"      // a step the task requires has no accepted result
	KindForeignResult      = "foreign_result"    // a result in Temporal that agentd did not send
	KindRecordUnreadable   = "record_unreadable" // agentd could not read its own record of a call's end
	KindDryRan             = "dry_ran"           // a write of a run that is not a dry run, which agentd only dry-ran
	KindWorkflowError      = "workflow_error"
	KindWorkflowTaskFailed = "workflow_task_failed"
	KindWorkerStopped      = "worker_stopped"
	KindNoWorker           = "no_worker"
	KindAgentdStopped      = "agentd_stopped"
	KindActivityTimeout    = "activity_timeout"
	KindCancelled          = "cancelled"
	KindTerminated         = "terminated"
	KindHistoryUnavailable = "history_unavailable"
)

// History is an execution's history, an event at a time: Temporal's iterator, which reads it a page
// at a time.
type History interface {
	HasNext() bool
	Next() (*historypb.HistoryEvent, error)
}

// ModelRequest is one network request of a model call, as model_calls records it, and its answer
// as its response blob holds it.
type ModelRequest struct {
	db.ModelCall
	Status      string // the answer's: ok, refused, invalid_output; "" without one
	Category    string // a refusal's category, as the provider gave it
	Explanation string // a refusal's explanation: the provider's or the model's words
}

// Input is what Explain reads of one run in Temporal (ReadTemporal), and the run's row in the
// ledger. Control and the report gather it; a test makes one up.
type Input struct {
	RunID string
	Task  tasks.Task
	Run   db.Run // the ledger's row: Explain is for runs agentd started
	State string // as GetRun says it
	// Describe is what Temporal says of the execution now; nil when it has none (past the namespace's
	// retention, or a dev server that lost its state), or could not be asked (Unread says why).
	Describe *workflowservice.DescribeWorkflowExecutionResponse
	// History is the execution's history; nil when there is none to read. Its iterator's ErrHistoryGone
	// says Temporal no longer has it; any other error fails Explain.
	History History
	Unread  string // why Temporal could not be read, when it could not: Report says so
	Now     time.Time
}

// Wanted is what Explain needs of the ledger besides the run's rows, each read on its own whatever
// the bounds on those: the records its history's results name (by seq), and agentd's records of the
// attempts its history says failed.
type Wanted struct {
	Seqs     []int64
	Attempts []Attempt
}

// Attempt is one attempt of one of the run's calls, by its activity ID.
type Attempt struct {
	ActivityID string
	Attempt    int32
}

// Ledger is what Explain reads of the run in agentd's ledger (ReadLedger).
type Ledger struct {
	Records   []db.AuditLog // the run's records, by seq, to the bound: not those of executions agentd did not start
	Unknown   int64         // the records under the run's workflow ID of calls from executions agentd did not start
	Starts    []db.AuditLog // agentd's start records (agentd.started) since the run began, by seq
	LogBegan  time.Time     // when the audit log's first record was appended; zero when it has none
	Requests  []ModelRequest
	Approvals []db.Approval
	Effects   []db.Effect
	Calls     []db.Call // the loop guard's calls table: each call's last outcome, for runs older than the log
	Budget    budget.Totals
	// Cut says what of the run's rows was not read, past a bound: the explanation is partial.
	// RecordsCut says the run's records were among them: those after the last read were not.
	// AttemptsCut says the call records after those, read for the attempts that failed, were cut too.
	Cut         []string
	CutWhat     []string // what Cut says, as the evidence that was not read: "audit records past the first 5000"
	RecordsCut  bool
	AttemptsCut bool
	// Named are the records Wanted that Records does not hold, by seq: whichever run's (a result
	// can name any record), and agentd's records of the attempts Wanted. A seq no record has is not
	// there. NamedErr and AttemptErr say why a record could not be read, when it could not.
	Named      map[int64]db.AuditLog
	NamedErr   map[int64]string
	AttemptErr map[Attempt]string
}

// Explain says why the run is where it is. It reads the history first, then the ledger (read, with
// what the history needs of it): every record a result in the history names was committed before
// the result went out, so the ledger read after the history holds it.
func Explain(in Input, read func(Wanted) (Ledger, error)) (*agentosv1.Explanation, error) {
	f, err := gather(in, read)
	if err != nil {
		return nil, err
	}
	e := &agentosv1.Explanation{RunId: in.RunID, State: in.State,
		Cost:  &agentosv1.Cost{SpentMicros: f.l.Budget.Spent, ReservedMicros: f.l.Budget.Held, LimitMicros: f.l.Budget.Limit},
		Usage: usage(f.l.Requests)}
	f.verdict(e)
	f.incidents(e)
	e.Missing = append(f.missing, f.l.Cut...)
	finish(e)
	return e, nil
}

// status is the execution's status in Temporal: Describe's, else the history's closing event's;
// unspecified when Temporal has neither.
func (f *facts) status() enumspb.WorkflowExecutionStatus {
	if d := f.in.Describe; d != nil {
		return d.GetWorkflowExecutionInfo().GetStatus()
	}
	if f.closed != nil {
		return closeStatus[f.closed.typ]
	}
	return enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED
}

var closeStatus = map[enumspb.EventType]enumspb.WorkflowExecutionStatus{
	enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:        enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:         enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
	enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:       enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
	enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:        enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW: enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
}

// verdict judges the run by its state first: how it ended, or what it is doing.
func (f *facts) verdict(e *agentosv1.Explanation) {
	st := f.status()
	switch {
	case !f.in.Run.RunID.Valid:
		e.Verdict, e.Cause = VerdictRunning, "the run is starting: agentd has not recorded its execution in Temporal yet"
	case f.in.Run.CancelledAt.Valid && st != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING &&
		st != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED && st != enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED:
		// Ended after a cancel, whether it gave up, failed or was terminated, as GetRun says. One that
		// completed all the same is judged as it completed.
		f.cancelled(e, st)
	case st == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		f.completed(e)
	case st == enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, st == enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		f.failed(e, st)
	case st == enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		f.cancelled(e, st)
	case st == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		f.terminated(e)
	case st == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, st == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED:
		f.running(e)
	case st == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED:
		f.unknown(e)
	default:
		e.Verdict, e.Kind = VerdictUnknown, KindTerminated
		e.Cause = fmt.Sprintf("Temporal says the execution ended %s, which agentd's runs never do: someone with "+
			"access to Temporal did it", statusName(st))
		f.sourceClose(e)
	}
}

// judgment is the verdict on a run that ends with the steps it has.
type judgment struct {
	verdict, kind, cause string
	at                   *step  // the step it names, when the run made a call of it
	step                 string // the step's name
	named                bool   // the name is the task's own (D12)
}

// judge is the verdict on a run that ends with the steps it has: the first whose result changed its
// course (a person's decision against a write reads as such, not as a failure); else a step the task
// requires and the run has no accepted result of; else ok.
func (f *facts) judge() judgment {
	if f.truncated > 0 {
		// The calls past what agentd read may be where the run's course changed.
		return judgment{verdict: VerdictUnknown, kind: KindHistoryUnavailable, cause: fmt.Sprintf("the run's history is "+
			"longer than agentd reads (%d events, %d gateway calls): how the run went past them is not known", maxEvents, maxSteps)}
	}
	if s := f.firstUncheckedOrBad(); s != nil && s.unchecked {
		// How it ended agentd cannot say: it may be the step that changed the run's course.
		cause := fmt.Sprintf("agentd could not read its own record of how %s ended, so how the run went is not known", s.step)
		switch {
		case s.anchor == nil && f.l.AttemptsCut: // not an error: a bound, which the report may name
			cause = fmt.Sprintf("agentd's own record of how %s ended is past its bound on the run's records it reads, so "+
				"how the run went is not known", s.step)
		case f.public:
			cause += "; " + inTheLog
		default:
			cause += quoted(": ", f.uncheckedWhy(s))
		}
		return judgment{verdict: VerdictUnknown, kind: KindRecordUnreadable, at: s, step: s.step, named: s.mapped, cause: cause}
	}
	if bad := f.firstBad(); bad != nil {
		j := judgment{verdict: VerdictStepFailed, at: bad, step: bad.step, named: bad.mapped}
		switch {
		case bad.isReport:
			// The workflow returned without its report: the task's workflow raises instead.
			j.kind, j.cause = KindReportNotPosted, "the run ended without its report: "+f.because(bad)
			return j
		case bad.final == statusRejected || bad.final == statusExpired:
			j.verdict = VerdictNotApproved
		}
		j.kind, j.cause = f.kindOf(bad)
		return j
	}
	if c, why, ok := f.missingRequired(); ok {
		_, named := f.in.Task.Step(c)
		return judgment{verdict: VerdictStepFailed, kind: KindStepMissing, step: f.stepName(c), named: named, cause: why}
	}
	return judgment{verdict: VerdictOK, cause: "the run ended well"}
}

// apply sets the explanation's verdict from j.
func (f *facts) apply(e *agentosv1.Explanation, j judgment) {
	e.Verdict, e.Kind, e.Cause, e.Step = j.verdict, j.kind, j.cause, j.step
	if j.at != nil {
		f.point(e, j.at)
	}
}

// completed: the workflow returned. A task with a report must have reported; then judge.
func (f *facts) completed(e *agentosv1.Explanation) {
	f.claimResult(e)
	defer f.sourceClose(e)
	if !f.history {
		// No call's result can be held to agentd's record of it: whether the run ended well is not known.
		f.asTheRecordsGo(e, "Temporal says the run completed, but no longer has its history, so whether it ended well "+
			"is not known")
		return
	}
	// Its report may be past what agentd read of a history longer than that (judge says so).
	if rc := f.in.Task.Report; rc != (tasks.Call{}) && f.report() == nil && f.truncated == 0 {
		name := f.stepName(rc)
		e.Verdict, e.Kind, e.Step = VerdictStepFailed, KindReportNotPosted, name
		e.Cause = "the run ended without its report: it made no call of " + name
		f.alsoBefore(nil)
		return
	}
	j := f.judge()
	f.apply(e, j)
	r := f.report()
	switch {
	case j.verdict == VerdictOK && r != nil:
		e.Cause += fmt.Sprintf(": %s went out", r.step)
		e.Sources = append(e.Sources, r.evidence()...)
	case j.verdict != VerdictOK && (j.at == nil || !j.at.isReport) && r != nil && r.final == statusOK:
		e.Cause += fmt.Sprintf("; the run reported it (%s went out)", r.step)
	}
}

// failed: the run ended FAILED. Its report did not go out; or the workflow raised, after its report
// went out (its own code failed, and the report says so) or without one.
func (f *facts) failed(e *agentosv1.Explanation, st enumspb.WorkflowExecutionStatus) {
	e.Verdict = VerdictFailed
	f.claimFailure(e)
	defer f.sourceClose(e)
	if st == enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT {
		e.Kind, e.Cause = KindWorkflowError, "the run ran past its workflow's execution timeout"
		if s := f.current(); s != nil {
			e.Cause += fmt.Sprintf(", at %s", s.step)
			f.point(e, s)
		}
		return
	}
	r := f.report()
	switch {
	case r != nil && r.final != statusOK:
		f.name(e, r, KindReportNotPosted, "the report did not go out: "+f.because(r))
	case r != nil:
		// The report went out, and the run failed all the same: the workflow raised, as the task's
		// workflow does when its own code failed before it reported, or as it read the report's result.
		e.Kind = KindWorkflowError
		e.Cause = fmt.Sprintf("the workflow raised, and %s went out", r.step)
		if last := f.lastBefore(r); last != nil {
			e.Cause = fmt.Sprintf("the workflow's own code failed after %s %s; %s went out, and the run failed",
				last.step, last.outcome(), r.step)
			f.point(e, last)
		}
		e.Sources = append(e.Sources, r.evidence()...)
	default:
		e.Kind, e.Cause = KindWorkflowError, "the workflow raised"
		if f.in.Task.Report != (tasks.Call{}) {
			e.Cause = "the workflow raised without reporting"
		}
		if last := f.lastStep(); last != nil {
			e.Cause += fmt.Sprintf(", after %s %s", last.step, last.outcome())
			f.point(e, last)
		}
	}
	f.alsoBefore(r)
}

// alsoBefore lists the first step that changed the run's course, other than the report r, as an
// incident: a verdict about the report, or the workflow's own code, does not name it.
func (f *facts) alsoBefore(r *step) {
	if bad := f.firstBad(); bad != nil && bad != r && !bad.isReport {
		kind, cause := f.kindOf(bad)
		f.extra = append(f.extra, incidentOf(bad, kind, "before that, "+cause))
	}
}

// cancelled: a person cancelled the run (run.cancelled), and it ended cancelled; or agentd terminated
// it when it did not stop in time.
func (f *facts) cancelled(e *agentosv1.Explanation, st enumspb.WorkflowExecutionStatus) {
	e.Verdict, e.Kind = VerdictCancelled, KindCancelled
	e.Cause = f.whoCancelled(e)
	if st == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED {
		if t := f.lastTerminated(); t != nil {
			e.Cause += fmt.Sprintf("; it did not stop in time, and agentd terminated it (%s)", t.why)
			e.Sources = append(e.Sources, auditSource(t.seq, t.typ+": "+t.why))
		} else {
			e.Cause += "; it was terminated after that, not by agentd"
		}
	}
	if s := f.current(); s != nil {
		e.Cause += fmt.Sprintf("; it was at %s", s.step)
		f.point(e, s)
	}
	f.sourceClose(e)
}

// whoCancelled says who cancelled the run, and when, from run.cancelled or the runs table.
func (f *facts) whoCancelled(e *agentosv1.Explanation) string {
	switch c := f.cancel; {
	case c != nil:
		e.Sources = append(e.Sources, auditSource(c.seq, "run.cancelled by "+c.actor))
		return fmt.Sprintf("%s cancelled the run at %s", who(c.actor), stamp(c.at))
	case f.in.Run.CancelledAt.Valid:
		e.Sources = append(e.Sources, ledgerSource("runs", f.in.RunID, "cancelled_at "+f.in.Run.CancelledAt.String))
		return "the run was cancelled at " + f.in.Run.CancelledAt.String + " (as the runs table says: the audit log has no record of it)"
	}
	return "the run was cancelled, not through agentd: agentd has no record of a cancel"
}

// terminated: Temporal ended the run at someone's request: agentd's (run.terminated says why), or
// someone else's, whose reason is theirs to claim.
func (f *facts) terminated(e *agentosv1.Explanation) {
	e.Verdict, e.Kind = VerdictTerminated, KindTerminated
	if t := f.lastTerminated(); t != nil {
		e.Cause = "agentd terminated the run: " + t.why
		e.Sources = append(e.Sources, auditSource(t.seq, t.typ+": "+t.why))
	} else {
		e.Cause = "the run was terminated, not by agentd: agentd has no record of it"
		if c := f.closed; c != nil && c.typ == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED {
			e.Claim = fmt.Sprintf("terminated by %q: %q", c.identity, c.reason)
		}
	}
	if s := f.current(); s != nil {
		e.Cause += fmt.Sprintf("; it was at %s", s.step)
		f.point(e, s)
	}
	f.sourceClose(e)
}

// running: the run is being cancelled, is stuck, waits for a person (as agentd's own tables say), or
// is at a step; in that order: what a person is told to do must be what moves the run now.
func (f *facts) running(e *agentosv1.Explanation) {
	if f.cancelling(e) || f.stuck(e) || f.waiting(e) {
		return
	}
	e.Verdict = VerdictRunning
	s := f.current()
	switch {
	case s == nil:
		e.Cause = "the run has made no gateway call yet"
	case s.final == "":
		e.Cause = fmt.Sprintf("the run is at %s: its attempt %d is under way", s.step, max(s.attempt, f.pendingAttempt(s), 1))
		f.point(e, s)
	default:
		e.Cause = fmt.Sprintf("the run is past %s, which %s", s.step, s.outcome())
		f.point(e, s)
	}
}

// cancelling reports whether a person cancelled the run, which has not ended yet: agentd refuses
// its calls from then on, and has Temporal end it. Nothing is waited for any more.
func (f *facts) cancelling(e *agentosv1.Explanation) bool {
	if !f.in.Run.CancelledAt.Valid {
		return false
	}
	e.Verdict, e.Kind = VerdictCancelled, KindCancelled
	e.Cause = f.whoCancelled(e) + "; it has not ended yet: agentd refuses its calls, and terminates it if it does not end on its own"
	if s := f.current(); s != nil {
		e.Cause += fmt.Sprintf("; it is at %s", s.step)
		f.point(e, s)
	}
	return true
}

// unknown: Temporal no longer has the execution. What the audit log and the rest of the ledger say
// is all there is, and the explanation goes as far as they go.
func (f *facts) unknown(e *agentosv1.Explanation) {
	f.asTheRecordsGo(e, "Temporal no longer has the execution, so how it ended is not known")
}

// asTheRecordsGo is the explanation of a run without its history: not known (cause says why), and
// what agentd's records say of its calls, unchecked.
func (f *facts) asTheRecordsGo(e *agentosv1.Explanation, cause string) {
	e.Verdict, e.Kind, e.Cause = VerdictUnknown, KindHistoryUnavailable, cause
	if bad := f.firstBad(); bad != nil {
		kind, cause := f.kindOf(bad)
		e.Kind = kind
		e.Cause += "; from agentd's records: " + cause
		f.point(e, bad)
		return
	}
	if r := f.report(); r != nil && r.final == statusOK {
		e.Cause += fmt.Sprintf("; from agentd's records, every call returned ok and %s went out", r.step)
		e.Sources = append(e.Sources, r.evidence()...)
	} else if s := f.lastStep(); s != nil {
		e.Cause += fmt.Sprintf("; from agentd's records, the last call was %s, which %s", s.step, s.outcome())
		f.point(e, s)
	}
}

// name sets the explanation's step from s, with the kind and cause given.
func (f *facts) name(e *agentosv1.Explanation, s *step, kind, cause string) {
	e.Kind, e.Cause = kind, cause
	f.point(e, s)
}

// point sets the explanation's step, call, activity and attempt from s, with s's evidence.
func (f *facts) point(e *agentosv1.Explanation, s *step) {
	e.Step, e.Call, e.ActivityId, e.Attempt = s.step, s.name, s.id, s.attempt
	e.Sources = append(e.Sources, s.evidence()...)
	e.Sources = append(e.Sources, f.ledgerEvidence(s)...)
}

// sourceClose adds the history's closing event to the evidence.
func (f *facts) sourceClose(e *agentosv1.Explanation) {
	if c := f.closed; c != nil {
		e.Sources = append(e.Sources, temporalSource(c.event, eventName(c.typ)))
	}
}

// claimResult is the workflow's result, as its claim: the outcome it says it reported.
func (f *facts) claimResult(e *agentosv1.Explanation) {
	if c := f.closed; c != nil && c.result != "" {
		e.Claim = c.result
	}
}

// claimFailure is the workflow's failure message, as its claim.
func (f *facts) claimFailure(e *agentosv1.Explanation) {
	if c := f.closed; c != nil && c.message != "" {
		e.Claim = c.message
	}
}

// who is the name an actor of the audit log stands for.
func who(actor string) string {
	if name, ok := strings.CutPrefix(actor, "operator:"); ok {
		return name
	}
	return actor
}

func statusName(st enumspb.WorkflowExecutionStatus) string { return st.String() }

func stamp(t time.Time) string {
	if t.IsZero() {
		return "an unknown time"
	}
	return t.UTC().Format(time.RFC3339)
}

// usage is what the run's model requests were billed, by the model each was billed to.
func usage(requests []ModelRequest) []*agentosv1.ModelUsage {
	by := map[string]*agentosv1.ModelUsage{}
	get := func(model string) *agentosv1.ModelUsage {
		if by[model] == nil {
			by[model] = &agentosv1.ModelUsage{Model: model}
		}
		return by[model]
	}
	for _, r := range requests {
		if r.Status == statusRefused {
			get(r.Provider+"/"+r.Model).Refusals++
		}
		var billed []struct {
			Model              string `json:"model"`
			InputTokens        int64  `json:"input_tokens"`
			CacheWrite5mTokens int64  `json:"cache_write_5m_tokens"`
			CacheWrite1hTokens int64  `json:"cache_write_1h_tokens"`
			CacheWriteTokens   int64  `json:"cache_write_tokens"`
			CacheReadTokens    int64  `json:"cache_read_tokens"`
			OutputTokens       int64  `json:"output_tokens"`
		}
		if !r.UsageJson.Valid || json.Unmarshal([]byte(r.UsageJson.String), &billed) != nil {
			continue
		}
		for _, b := range billed {
			model := b.Model
			if model == "" {
				model = r.Provider + "/" + r.Model
			}
			u := get(model)
			u.Requests++
			u.InputTokens += b.InputTokens
			u.OutputTokens += b.OutputTokens
			u.CacheReadTokens += b.CacheReadTokens
			u.CacheWriteTokens += b.CacheWrite5mTokens + b.CacheWrite1hTokens + b.CacheWriteTokens
		}
	}
	out := make([]*agentosv1.ModelUsage, 0, len(by))
	for _, u := range by {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}
