package explain

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// stuckAttempts is the attempt at which a workflow task, or a call, that keeps failing makes the run
// stuck: the one after two failures in a row. noWorkerAfter is how long a workflow task may wait for
// a Worker before the run is stuck for want of one.
const (
	stuckAttempts = 3
	noWorkerAfter = time.Minute
)

// kindOf is the kind and the cause of a step whose result changed the run's course. Its kind comes
// from agentd's own fields of the call's record (who refused it, where its error came from) and from
// Temporal's event types, never from a tool's or the Worker's words, which the cause quotes (quote).
func (f *facts) kindOf(s *step) (string, string) {
	r := s.finalRec
	reason := ""
	if r != nil {
		reason = r.reason
	}
	switch s.final {
	case statusRefused:
		return KindModelRefused, f.refusal(s)
	case statusInvalidOutput:
		return KindModelInvalidOutput, fmt.Sprintf("%s: the model's answer broke the task's output schema, and so did the "+
			"answer agentd asked for again%s", s.step, f.quote(s, ": ", reason))
	case statusBudgetExhausted:
		return KindBudgetExhausted, f.budgetCause(s)
	case statusNeedsApproval:
		return KindApprovalPending, fmt.Sprintf("%s waits for a person to approve its write", s.step)
	case statusNeedsReconcile:
		return KindReconcilePending, fmt.Sprintf("%s's write has an outcome nobody knows: a person must say whether it happened", s.step)
	case statusRejected, statusExpired:
		return f.decision(s)
	case statusEdited:
		return KindApprovalEdited, fmt.Sprintf("a reviewer edited %s's write, and the run did not send the edit on", s.step)
	case statusDenied:
		return f.denial(s, r, reason)
	case statusError:
		text, _ := s.errorText()
		kind := errorKind(s, s.errorRecord())
		cause := fmt.Sprintf("%s failed%s", s.step, attemptsSaid(s))
		if s.unrecorded {
			// Temporal's word only, said as such: whose failure it is agentd cannot tell. A report says
			// nothing of it: whoever failed the call (a Worker can, on a task queue of its own) chose
			// its type and its message.
			cause += ", by a failure with no record of agentd's"
			if !f.public {
				cause += "; it says " + typeSaid(s.failure) + quoted(": ", s.failure.GetMessage())
			}
			return kind, cause
		}
		switch {
		case kind == KindToolServerFailed:
			cause += ": the git tool server failed"
			if f.public {
				return kind, cause + "; " + inTheLog
			}
		case f.public && s.errorRecord() == nil:
			// agentd started again during the attempt, and has no record of it: the failure's words are
			// Temporal's, which whoever failed the call wrote, a tool's type and all.
			return kind, cause + "; agentd has no record of its error"
		case f.public && (!s.mapped || !s.toolsWords()):
			// An error of agentd's own, its model adapter's or its ledger's can name hosts, ports and
			// paths, and one of a call the task does not name the call's own name: a report says only
			// where it is.
			return kind, cause + "; " + inTheLog
		}
		return kind, cause + quoted(": ", text)
	case statusTimeout:
		if f.public {
			return KindActivityTimeout, fmt.Sprintf("%s timed out%s", s.step, timeoutSaid(s.failure))
		}
		return KindActivityTimeout, fmt.Sprintf("%s timed out%s", s.step, quoted(": ", failureText(s.failure)))
	case statusCancelled:
		return KindCancelled, fmt.Sprintf("%s was cancelled", s.step)
	case statusDryRan:
		return KindDryRan, fmt.Sprintf("%s did not reach its system: the agentd that took it up could only dry-run it, though "+
			"the run is not a dry run", s.step)
	case statusForeign:
		// Why it is not agentd's quotes what its sender chose (the record it names, the activity IDs, its
		// status, its type): a report says only that it is not.
		why := ""
		if !f.public {
			why = " (" + s.foreign + ")"
		}
		if s.foreignErr {
			return KindForeignResult, fmt.Sprintf("%s failed, by a failure agentd did not send%s, and agentd has no "+
				"record of how the call ended", s.step, why)
		}
		return KindForeignResult, fmt.Sprintf("%s returned a result agentd did not send%s, and agentd has no record of "+
			"how the call ended", s.step, why)
	}
	return KindDenied, fmt.Sprintf("%s returned %q%s", s.step, s.final, f.quote(s, ": ", reason))
}

// denial is the kind and cause of a call that ended denied, by who refused it, as agentd's record of
// it says (refused_by). A record from before agentd said so says only the policy's decision.
func (f *facts) denial(s *step, r *rec, reason string) (string, string) {
	by := ""
	if r != nil {
		by = r.refusedBy
	}
	switch by {
	case audit.RefusedByPolicy:
		why := r.policyWhy
		if why == "" {
			why = reason
		}
		return KindPolicyDenied, fmt.Sprintf("the policy denied %s%s", s.step, f.quote(s, ": ", why))
	case audit.RefusedByLoopGuard:
		return KindLoopGuard, fmt.Sprintf("agentd's loop guard refused %s%s", s.step, f.quote(s, ": ", reason))
	case audit.RefusedByCancel:
		return KindCancelled, fmt.Sprintf("agentd refused %s: the run was cancelled", s.step)
	case audit.RefusedByTool:
		return KindToolDenied, fmt.Sprintf("%s refused the call as it stood%s", s.step, f.quote(s, ": ", reason))
	case audit.RefusedByAgentd:
		return KindDenied, fmt.Sprintf("agentd refused %s%s", s.step, f.quote(s, ": ", reason))
	}
	if r != nil && r.decision == "deny" {
		why := r.policyWhy
		if why == "" {
			why = reason
		}
		return KindPolicyDenied, fmt.Sprintf("the policy denied %s%s", s.step, f.quote(s, ": ", why))
	}
	who := "agentd's record of it does not say who refused it"
	if r == nil {
		who = "agentd has no record of it"
	}
	return KindDenied, fmt.Sprintf("%s was refused, and %s%s", s.step, who, f.quote(s, ": ", reason))
}

// inTheLog is where a report sends its reader for an internal error, whose text it does not show.
const inTheLog = "agentd's log has the details"

// quote is the words of step s (a reason, an error, a tool's, a model's or a person's) after prefix,
// quoted, as agentd's sentence of the step quotes them: none in a report, of a step the task does
// not name (unnamedStep). Its kind and name, which the workflow chose, can be in them: an unknown
// model task's error names it, a refusal of an older agentd's loop guard did, a tool's can echo the
// arguments.
func (f *facts) quote(s *step, prefix, text string) string {
	if f.public && !s.mapped {
		return ""
	}
	return quoted(prefix, text)
}

// typeSaid is a failure's type, quoted, as the failure names it.
func typeSaid(fl *failurepb.Failure) string {
	if t := fl.GetApplicationFailureInfo().GetType(); t != "" {
		return strconv.Quote(t)
	}
	return "no type"
}

// toolsWords reports whether the error that ended the step is a tool's refusal for good, in the
// tool's words (ToolFailed, agentd's type for it), as against an error of agentd's own.
func (s *step) toolsWords() bool {
	if r := s.errorRecord(); r != nil {
		return r.errType == "ToolFailed"
	}
	return s.failure.GetApplicationFailureInfo().GetType() == "ToolFailed"
}

// timeoutSaid is which of Temporal's timeouts ended a call, as a clause.
func timeoutSaid(fl *failurepb.Failure) string {
	if t := fl.GetTimeoutFailureInfo().GetTimeoutType(); t != enumspb.TIMEOUT_TYPE_UNSPECIFIED {
		return " (Temporal's " + t.String() + " timeout)"
	}
	return ""
}

// because is why a step did not return ok, for a cause that names it.
func (f *facts) because(s *step) string {
	_, cause := f.kindOf(s)
	return cause
}

// errorRecord is agentd's record of the attempt that ended the step with an error, if it has one.
func (s *step) errorRecord() *rec {
	if r := s.finalRec; r != nil && r.status == statusError {
		return r
	}
	return nil
}

// errorText is the failure of a step that ended with an error: agentd's record of its last attempt,
// else Temporal's record of the failure, which agentd's gateway returned.
func (s *step) errorText() (string, string) {
	if r := s.errorRecord(); r != nil {
		return r.err, r.errType
	}
	return failureText(s.failure), s.failure.GetApplicationFailureInfo().GetType()
}

// errorKind is the kind of an attempt of s that failed, as agentd's record of it (r) says: the git
// tool server failed (its error_cause, as agentd's git client types the error), or the call's own
// kind failed: a model's, or a tool's. Without a record, the call's kind.
func errorKind(s *step, r *rec) string {
	switch {
	case r != nil && r.errCause == audit.ErrorCauseToolServer:
		return KindToolServerFailed
	case s.kind == "model":
		return KindModelError
	}
	return KindToolError
}

// attemptsSaid says how many attempts a call that failed had, when it had more than one.
func attemptsSaid(s *step) string {
	if s.attempt > 1 {
		return fmt.Sprintf(" after %d attempts", s.attempt)
	}
	return ""
}

// refusal is the cause of a model's refusal: which model, and what the provider said.
func (f *facts) refusal(s *step) string {
	cause := s.step + ": the model declined to answer"
	if q := f.lastRequest(s, statusRefused); q != nil {
		cause += fmt.Sprintf(" (%s/%s%s)", q.Provider, q.Model, f.quote(s, ", category ", q.Category)) +
			f.quote(s, ": ", q.Explanation)
	}
	return cause
}

// lastRequest is the latest model request of the step whose answer had the status given.
func (f *facts) lastRequest(s *step, status string) *ModelRequest {
	var found *ModelRequest
	for i, q := range f.l.Requests {
		if q.ActivityID == s.id && q.Status == status {
			found = &f.l.Requests[i]
		}
	}
	return found
}

// budgetCause is the cause of a request that found the budget short, as its result says.
func (f *facts) budgetCause(s *step) string {
	cause := s.step + ": the run's budget ran out"
	if needed := s.numbers["needed_micros"]; needed > 0 {
		cause += fmt.Sprintf(": the request needed %s, and %s was left", dollars(needed), dollars(s.numbers["available_micros"]))
	}
	return cause
}

// decision is the kind and cause of a write a person did not approve: who rejected it and why, or
// when its approval expired. Not an error: what the policy asked for, decided against.
func (f *facts) decision(s *step) (string, string) {
	a := f.approvalOf(s)
	if s.final == statusExpired {
		cause := fmt.Sprintf("nobody approved %s's write in time", s.step)
		if a != nil {
			cause = fmt.Sprintf("nobody approved %s's write before its approval expired at %s", s.step, a.ExpiresAt)
		}
		return KindApprovalExpired, cause
	}
	cause := fmt.Sprintf("a person rejected %s's write", s.step)
	if a != nil {
		by := a.DecidedBy.String
		if d := f.decisions[a.ApprovalID]; d != nil && who(d.actor) != by {
			by += " (as " + d.actor + ")"
		}
		cause = fmt.Sprintf("%s rejected %s's write at %s%s", by, s.step, a.DecidedAt.String, f.quote(s, ", saying ", a.Reason.String))
	}
	return KindApprovalRejected, cause
}

// approvalOf is the step's approval: the latest of the run's for its tool that was decided as the
// step's result says.
func (f *facts) approvalOf(s *step) *db.Approval {
	want := map[string]string{statusRejected: "REJECTED", statusExpired: "EXPIRED"}[s.final]
	var found *db.Approval
	for i, a := range f.l.Approvals {
		if a.Tool == s.name && (want == "" || a.Status == want) {
			found = &f.l.Approvals[i]
		}
	}
	return found
}

// ledgerEvidence is where the ledger's rows behind a step's result are.
func (f *facts) ledgerEvidence(s *step) []*agentosv1.Evidence {
	var out []*agentosv1.Evidence
	switch s.final {
	case statusRefused, statusInvalidOutput:
		for _, q := range f.l.Requests {
			if q.ActivityID == s.id && q.Status == s.final {
				out = append(out, ledgerSource("model_calls", fmt.Sprintf("%s/%d/%d", q.ActivityID, q.Attempt, q.RequestSeq),
					fmt.Sprintf("%s/%s answered %s", q.Provider, q.Model, q.Status)))
			}
		}
	case statusRejected, statusExpired:
		if a := f.approvalOf(s); a != nil {
			out = append(out, ledgerSource("approvals", a.ApprovalID, a.Status+" by "+a.DecidedBy.String))
			if d := f.decisions[a.ApprovalID]; d != nil {
				out = append(out, auditSource(d.seq, "approval.decided by "+d.actor))
			}
		}
	case statusBudgetExhausted:
		out = append(out, ledgerSource("budget_entries", f.in.RunID, fmt.Sprintf("limit %s, %s spent, %s reserved",
			dollars(f.l.Budget.Limit), dollars(f.l.Budget.Spent), dollars(f.l.Budget.Held))))
	case statusDryRan:
		for _, ef := range f.l.Effects {
			if ef.Tool == s.name && ef.Status == "DRY_RUN" {
				out = append(out, ledgerSource("effects", ef.EffectID, "DRY_RUN"))
			}
		}
	}
	return out
}

// waiting reports whether the run waits for a person, as agentd's own tables say, and says how to
// let it go on: what agentd accepts now.
func (f *facts) waiting(e *agentosv1.Explanation) bool {
	for _, a := range f.l.Approvals {
		if a.Status != "PENDING" {
			continue
		}
		if at, err := time.Parse(time.RFC3339Nano, a.ExpiresAt); err == nil && !f.in.Now.Before(at) {
			continue // expired: agentd records it as such any moment, refuses a decision on it, and the run goes on
		}
		e.Verdict, e.Kind = VerdictWaiting, KindApprovalPending
		e.Cause = fmt.Sprintf("the run waits for a person to approve its write %s, until %s", a.Tool, a.ExpiresAt)
		if s := f.lastWith(a.Tool, statusNeedsApproval); s != nil {
			e.Cause = fmt.Sprintf("%s waits for a person to approve its write (%s), until %s", s.step, a.Tool, a.ExpiresAt)
			f.point(e, s)
		}
		e.Next = fmt.Sprintf("agentctl approvals show %s, then agentctl approvals approve %s --payload-sha256 %s, "+
			"or reject it with --reason", a.ApprovalID, a.ApprovalID, a.PayloadSha256)
		e.Sources = append(e.Sources, ledgerSource("approvals", a.ApprovalID, "PENDING until "+a.ExpiresAt))
		return true
	}
	for _, ef := range f.l.Effects {
		if ef.Status != "UNKNOWN" {
			continue
		}
		e.Verdict, e.Kind = VerdictWaiting, KindReconcilePending
		e.Cause = fmt.Sprintf("the outcome of the run's write %s is unknown: a person must say whether it happened%s",
			ef.Tool, quoted(": ", ef.Reason.String))
		if s := f.lastWith(ef.Tool, statusNeedsReconcile); s != nil {
			e.Cause = fmt.Sprintf("%s waits: the outcome of its write (%s) is unknown, and a person must say whether it "+
				"happened%s", s.step, ef.Tool, quoted(": ", ef.Reason.String))
			f.point(e, s)
		}
		e.Next = fmt.Sprintf("agentctl reconcile show %s, then agentctl reconcile resolve %s --executed or --not-executed",
			ef.EffectID, ef.EffectID)
		e.Sources = append(e.Sources, ledgerSource("effects", ef.EffectID, "UNKNOWN"))
		return true
	}
	if s := f.lastStep(); s != nil && s.final == statusBudgetExhausted && !s.resolved {
		e.Verdict, e.Kind = VerdictWaiting, KindBudgetExhausted
		e.Cause = f.budgetCause(s) + fmt.Sprintf("; the run waits for more (now: limit %s, %s spent, %s reserved)",
			dollars(f.l.Budget.Limit), dollars(f.l.Budget.Spent), dollars(f.l.Budget.Held))
		f.point(e, s)
		if x := f.extendedAfter(s); x != nil {
			e.Cause += fmt.Sprintf("; %s extended it by %s at %s, and the run asks again when it is woken", who(x.actor),
				dollars(x.amount), stamp(x.at))
			e.Sources = append(e.Sources, auditSource(x.seq, "budget.extended by "+x.actor))
			return true
		}
		// What the request lacks, and as much again: a request refused before its input was counted
		// knows only the bound of its output, and asks for more once it is counted.
		needed := s.numbers["needed_micros"]
		more := needed - s.numbers["available_micros"] + needed
		if more <= 0 {
			more = 10_000 // a cent: the run's own figures are gone
		}
		e.Next = fmt.Sprintf("agentctl budget extend %s --usd %s", f.in.RunID, cents(more))
		return true
	}
	return false
}

// lastWith is the run's latest step of call name that returned status.
func (f *facts) lastWith(name, status string) *step {
	var found *step
	for _, s := range f.steps {
		if s.name == name && s.final == status {
			found = s
		}
	}
	return found
}

// extendedAfter is the first extension of the run's budget after the step returned.
func (f *facts) extendedAfter(s *step) *rec {
	for _, x := range f.extensions {
		if s.finalRec == nil || x.seq > s.finalRec.seq {
			return x
		}
	}
	return nil
}

// stuck reports whether the run is stuck: its workflow task keeps failing or timing out, or waits
// for a Worker that never takes it, or a call keeps failing; as Temporal describes it now.
func (f *facts) stuck(e *agentosv1.Explanation) bool {
	d := f.in.Describe
	if t := d.GetPendingWorkflowTask(); t != nil {
		switch since := t.GetOriginalScheduledTime().AsTime(); {
		case t.GetAttempt() >= stuckAttempts:
			e.Verdict = VerdictStuck
			last := f.lastTask()
			e.Kind = KindWorkerStopped
			e.Cause = fmt.Sprintf("its workflow task keeps timing out (now attempt %d): the Workers handling it stop or hang, "+
				"or what they send does not reach Temporal", t.GetAttempt())
			if last != nil && !last.timedOut {
				e.Kind = KindWorkflowTaskFailed
				e.Cause = fmt.Sprintf("its workflow task keeps failing (now attempt %d): the Worker fails it, as it did at event %d",
					t.GetAttempt(), last.event)
				e.Claim = last.message
			}
			if last != nil {
				e.Sources = append(e.Sources, temporalSource(last.event, last.name()))
				if last.after != nil {
					e.Cause += fmt.Sprintf(", after %s %s", last.after.step, last.after.outcome())
					f.point(e, last.after)
				}
			}
			e.Sources = append(e.Sources, temporalSource(0, fmt.Sprintf("pending workflow task, attempt %d", t.GetAttempt())))
			return true
		case t.GetState() == enumspb.PENDING_WORKFLOW_TASK_STATE_SCHEDULED && f.in.Now.Sub(since) > noWorkerAfter:
			e.Verdict, e.Kind = VerdictStuck, KindNoWorker
			e.Cause = fmt.Sprintf("no Worker has taken the run's workflow task since %s: is a Worker running?", stamp(since))
			if s := f.current(); s != nil {
				f.point(e, s)
			}
			e.Sources = append(e.Sources, temporalSource(0, "pending workflow task, scheduled at "+stamp(since)))
			return true
		}
	}
	for _, p := range d.GetPendingActivities() {
		if p.GetAttempt() < stuckAttempts {
			continue
		}
		s := f.open(p.GetActivityId())
		if s == nil {
			continue
		}
		s.attempt = p.GetAttempt()
		text := failureText(p.GetLastFailure())
		r := s.latestError()
		if r != nil {
			text = r.err
			e.Sources = append(e.Sources, auditSource(r.seq, fmt.Sprintf("call %q, attempt %d: error", s.id, r.attempt)))
		}
		e.Verdict, e.Kind = VerdictStuck, errorKind(s, r)
		e.Cause = fmt.Sprintf("%s keeps failing: attempt %d is due after %d that failed%s", s.step, p.GetAttempt(),
			p.GetAttempt()-1, quoted(", the last with: ", text))
		f.point(e, s)
		e.Sources = append(e.Sources, temporalSource(0, fmt.Sprintf("pending activity %q, attempt %d", s.id, p.GetAttempt())))
		return true
	}
	return false
}

// open is the step under way with this activity ID.
func (f *facts) open(activityID string) *step {
	for i := len(f.steps) - 1; i >= 0; i-- {
		if s := f.steps[i]; s.id == activityID && s.end == 0 {
			return s
		}
	}
	return nil
}

// latestError is the step's latest call record of an attempt that failed.
func (s *step) latestError() *rec {
	var found *rec
	for _, r := range s.callRecords() {
		if r.status == statusError {
			found = r
		}
	}
	return found
}

// lastTask is the run's latest workflow task that timed out or failed.
func (f *facts) lastTask() *workflowTask {
	if len(f.tasks) == 0 {
		return nil
	}
	return f.tasks[len(f.tasks)-1]
}

func (t *workflowTask) name() string {
	if t.timedOut {
		return "WorkflowTaskTimedOut"
	}
	return "WorkflowTaskFailed"
}

// incidents is the trouble the run met along the way, apart from its verdict, oldest first.
func (f *facts) incidents(e *agentosv1.Explanation) {
	var out []*agentosv1.Incident
	for _, s := range f.steps {
		out = append(out, f.attemptIncidents(s)...)
		out = append(out, f.waitIncidents(s)...)
		out = append(out, f.answerIncidents(s)...)
		if inc := f.resentIncident(s); inc != nil {
			out = append(out, inc)
		}
		if inc := f.foreignIncident(s); inc != nil {
			out = append(out, inc)
		}
	}
	for _, t := range f.tasks {
		out = append(out, f.taskIncident(t))
	}
	out = append(out, f.extra...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].At < out[j].At })
	e.Incidents = out
}

// incidentOf is an incident of step s.
func incidentOf(s *step, kind, cause string, evidence ...*agentosv1.Evidence) *agentosv1.Incident {
	at := s.endAt
	if r := s.finalRec; at.IsZero() && r != nil {
		at = r.at
	}
	return &agentosv1.Incident{At: stampOrEmpty(at), Step: s.step, ActivityId: s.id, Attempt: s.attempt, Kind: kind,
		Cause: cause, Evidence: evidence}
}

// foreignIncident is a result of the step in Temporal, or a failure, that agentd did not send:
// whoever completed or failed the call in Temporal's namespace claims it, and the step is judged by
// agentd's record.
func (f *facts) foreignIncident(s *step) *agentosv1.Incident {
	if s.unrecorded {
		return incidentOf(s, errorKind(s, nil), fmt.Sprintf("a failure with no record of agentd's: the history says attempt %d "+
			"failed%s, unverified: agentd has no record of the attempt to hold it to, and did not stop during it", s.attempt,
			quoted(" with ", failureText(s.failure))), s.evidence()...)
	}
	if s.foreign == "" {
		return nil
	}
	said := "agentd has no record of how the call ended"
	if r := s.finalRec; r != nil {
		said = fmt.Sprintf("agentd's record of attempt %d says it %s", r.attempt, s.outcome())
	}
	if s.foreignErr {
		return incidentOf(s, KindForeignResult, fmt.Sprintf("a failure agentd did not send, as far as its records go: the "+
			"history says attempt %d failed%s, the claim of someone with access to Temporal's namespace, unless agentd "+
			"could not record it (%s); %s", s.attempt, quoted(" with ", failureText(s.failure)), s.foreign, said), s.evidence()...)
	}
	holds := "a result"
	if s.result != "" {
		holds = fmt.Sprintf("a result %q", s.result)
	}
	return incidentOf(s, KindForeignResult, fmt.Sprintf("a result agentd did not send: the history holds %s (%s), the claim "+
		"of someone with access to Temporal's namespace; %s", holds, s.foreign, said), s.evidence()...)
}

// resentIncident is a report that did not go out, which the run sent again.
func (f *facts) resentIncident(s *step) *agentosv1.Incident {
	if !s.isReport || !s.resolved || s.final == statusOK || s.final == "" {
		return nil
	}
	_, cause := f.kindOf(s)
	return incidentOf(s, KindReportNotPosted, "a report that did not go out, which the run sent again: "+cause, s.evidence()...)
}

// attemptIncidents are the attempts of the step before the one that ended it (or the one under way):
// each failed, timed out, or was cut short by agentd stopping, as its records, or their absence, say.
func (f *facts) attemptIncidents(s *step) []*agentosv1.Incident {
	last := s.attempt
	if s.final == "" && f.history {
		last = max(f.pendingAttempt(s), s.attempt)
	}
	var out []*agentosv1.Incident
	for k := int32(1); k < last; k++ {
		inc := &agentosv1.Incident{Step: s.step, ActivityId: s.id, Attempt: k}
		next := f.attemptStart(s, k+1)
		switch r, began := s.record("call", k), s.record("call.started", k); {
		case r != nil && r.status == statusError && r.at.After(next):
			// It ended after the next attempt began: Temporal had given up on it.
			inc.At, inc.Kind = stampOrEmpty(r.at), KindActivityTimeout
			inc.Cause = fmt.Sprintf("attempt %d timed out, and Temporal tried again; it ended later, with an error%s", k,
				quoted(": ", r.err))
			inc.Evidence = append(inc.Evidence, auditSource(r.seq, fmt.Sprintf("call %q, attempt %d: error", s.id, k)))
		case r != nil && r.status == statusError:
			inc.At, inc.Kind = stampOrEmpty(r.at), errorKind(s, r)
			inc.Cause = fmt.Sprintf("attempt %d failed, and Temporal tried again%s", k, quoted(": ", r.err))
			if inc.Kind == KindToolServerFailed {
				inc.Cause = fmt.Sprintf("the git tool server failed during attempt %d, and the next attempt ran it again%s", k,
					quoted(": ", r.err))
			}
			inc.Evidence = append(inc.Evidence, auditSource(r.seq, fmt.Sprintf("call %q, attempt %d: error", s.id, k)))
		case r != nil:
			inc.At = stampOrEmpty(r.at)
			inc.Evidence = append(inc.Evidence, auditSource(r.seq, fmt.Sprintf("call %q, attempt %d: %s", s.id, k, r.status)))
			if x := f.startBetween(r.at, next); x != nil {
				inc.Kind = KindAgentdStopped
				inc.Cause = fmt.Sprintf("attempt %d returned %s, and agentd stopped before Temporal had it: agentd started again "+
					"at %s", k, r.status, stamp(x.at))
				inc.Evidence = append(inc.Evidence, auditSource(x.seq, "agentd.started"))
			} else {
				inc.Kind = KindActivityTimeout
				inc.Cause = fmt.Sprintf("attempt %d returned %s, which never reached Temporal: the attempt timed out", k, r.status)
			}
		case began != nil:
			inc.At = stampOrEmpty(began.at)
			inc.Evidence = append(inc.Evidence, auditSource(began.seq, fmt.Sprintf("call.started %q, attempt %d", s.id, k)))
			if x := f.startAfterSeq(began.seq, next); x != nil {
				inc.Kind = KindAgentdStopped
				inc.Cause = fmt.Sprintf("attempt %d began and never ended: agentd stopped during it, and started again at %s",
					k, stamp(x.at))
				inc.Evidence = append(inc.Evidence, auditSource(x.seq, "agentd.started"))
			} else if fl := f.lastFailure(s, k); fl != nil && fl.GetTimeoutFailureInfo() == nil {
				// Temporal says it failed, not that it timed out: an error agentd could not record (its
				// record's write failed), or a failure someone else made of the attempt. Unverified.
				inc.Kind = errorKind(s, nil)
				inc.Cause = fmt.Sprintf("attempt %d began and left no record of its end; Temporal says it failed%s, unverified",
					k, quoted(" with ", failureText(fl)))
			} else {
				inc.Kind = KindActivityTimeout
				inc.Cause = fmt.Sprintf("attempt %d began and left no record of its end: it timed out%s", k,
					f.lastFailureSaid(s, k))
			}
		default:
			from := s.scheduledAt
			if prev := s.record("call", k-1); prev != nil {
				from = prev.at
			}
			inc.At = stampOrEmpty(from)
			if x := f.startBetween(from, next); x != nil {
				inc.Kind = KindAgentdStopped
				inc.Cause = fmt.Sprintf("attempt %d left no record of its end%s, and agentd started again at %s, before "+
					"attempt %d began: agentd stopped during it, as inferred from the two", k, f.lastFailureSaid(s, k),
					stamp(x.at), k+1)
				inc.Evidence = append(inc.Evidence, auditSource(x.seq, "agentd.started"))
			} else if fl := f.lastFailure(s, k); fl != nil && fl.GetTimeoutFailureInfo() == nil {
				inc.Kind = errorKind(s, nil)
				inc.Cause = fmt.Sprintf("attempt %d failed, and the audit log has no record of it%s", k, quoted(": ", failureText(fl)))
			} else {
				inc.Kind = KindActivityTimeout
				inc.Cause = fmt.Sprintf("attempt %d left no record of its end: it timed out%s", k, f.lastFailureSaid(s, k))
			}
		}
		if k == s.attempt-1 && s.started != 0 {
			inc.Evidence = append(inc.Evidence, temporalSource(s.started, fmt.Sprintf("ActivityTaskStarted of %q, attempt %d, "+
				"after: %s", s.id, s.attempt, failureText(s.lastFailure))))
		}
		out = append(out, inc)
	}
	return out
}

// lastFailure is Temporal's record of how attempt k of the step ended: kept only for the attempt
// before the one that ended the step.
func (f *facts) lastFailure(s *step, k int32) *failurepb.Failure {
	if k == s.attempt-1 {
		return s.lastFailure
	}
	return nil
}

// lastFailureSaid is Temporal's record of how attempt k ended, as a clause, when it has one.
func (f *facts) lastFailureSaid(s *step, k int32) string {
	if fl := f.lastFailure(s, k); fl != nil {
		return " (Temporal: " + quoted("", failureText(fl)) + ")"
	}
	return ""
}

// attemptStart is when attempt k of the step began, as far as anything says: its started event for
// the attempt that ended the step; else its record, which came at its end; else now.
func (f *facts) attemptStart(s *step, k int32) time.Time {
	switch {
	case k == s.attempt && !s.startedAt.IsZero():
		return s.startedAt
	case s.record("call.started", k) != nil:
		return s.record("call.started", k).at
	case s.record("call", k) != nil:
		return s.record("call", k).at
	case !s.endAt.IsZero():
		return s.endAt
	}
	return f.in.Now
}

// startBetween is agentd's first start after from and before to.
func (f *facts) startBetween(from, to time.Time) *rec {
	for _, x := range f.starts {
		if x.at.After(from) && x.at.Before(to) {
			return x
		}
	}
	return nil
}

// startAfterSeq is agentd's first start after the record at seq, before to.
func (f *facts) startAfterSeq(seq int64, to time.Time) *rec {
	for _, x := range f.starts {
		if x.seq > seq && x.at.Before(to) {
			return x
		}
	}
	return nil
}

// waitIncidents are the waits the run went on from that held it up: for budget, and for a
// reconciliation. (A wait for an approval is the policy's design.)
func (f *facts) waitIncidents(s *step) []*agentosv1.Incident {
	if !s.resolved || s.finalRec == nil {
		return nil
	}
	ev := auditSource(s.finalRec.seq, fmt.Sprintf("call %q, attempt %d: %s", s.id, s.finalRec.attempt, s.final))
	switch s.final {
	case statusBudgetExhausted:
		cause := f.budgetCause(s)
		if x := f.extendedAfter(s); x != nil {
			cause += fmt.Sprintf("; %s extended it by %s at %s, and the run went on", who(x.actor), dollars(x.amount), stamp(x.at))
			return []*agentosv1.Incident{incidentOf(s, KindBudgetExhausted, cause, ev,
				auditSource(x.seq, "budget.extended by "+x.actor))}
		}
		return []*agentosv1.Incident{incidentOf(s, KindBudgetExhausted, cause+"; the run went on", ev)}
	case statusNeedsReconcile:
		cause := fmt.Sprintf("%s's write had an outcome nobody knew", s.step)
		for _, x := range f.reconciled {
			if x.seq > s.finalRec.seq {
				said := "it happened"
				if !x.executed {
					said = "it did not happen"
				}
				cause += fmt.Sprintf(", until %s said %s, at %s", who(x.actor), said, stamp(x.at))
				return []*agentosv1.Incident{incidentOf(s, KindReconcilePending, cause, ev, auditSource(x.seq, "effect.reconciled by "+x.actor))}
			}
		}
		return []*agentosv1.Incident{incidentOf(s, KindReconcilePending, cause, ev)}
	}
	return nil
}

// answerIncidents are the model's answers agentd did not take, in a call that got one in the end:
// one that broke the output schema (agentd asked again), and a refusal a fallback answered.
func (f *facts) answerIncidents(s *step) []*agentosv1.Incident {
	if s.final != statusOK {
		return nil
	}
	var out []*agentosv1.Incident
	for _, q := range f.l.Requests {
		if q.ActivityID != s.id || (q.Status != statusInvalidOutput && q.Status != statusRefused) {
			continue
		}
		key := fmt.Sprintf("%s/%d/%d", q.ActivityID, q.Attempt, q.RequestSeq)
		inc := &agentosv1.Incident{At: stampOrEmpty(parseTime(q.FinishedAt.String)), Step: s.step, ActivityId: s.id,
			Attempt: int32(q.Attempt), Kind: KindModelInvalidOutput,
			Cause: fmt.Sprintf("attempt %d: %s/%s's answer broke the task's output schema, and agentd asked again", q.Attempt,
				q.Provider, q.Model),
			Evidence: []*agentosv1.Evidence{ledgerSource("model_calls", key, q.Provider+"/"+q.Model+" answered "+q.Status)}}
		if q.Status == statusRefused {
			inc.Kind = KindModelRefused
			inc.Cause = fmt.Sprintf("attempt %d: %s/%s declined to answer, and another model answered%s", q.Attempt, q.Provider,
				q.Model, quoted(": ", q.Explanation))
		}
		out = append(out, inc)
	}
	return out
}

// taskIncident is a workflow task that timed out (its Worker stopped or hung, inferred from the
// timeout) or that the Worker failed (the Worker's own words).
func (f *facts) taskIncident(t *workflowTask) *agentosv1.Incident {
	inc := &agentosv1.Incident{At: stampOrEmpty(t.at), Evidence: []*agentosv1.Evidence{temporalSource(t.event, t.name())}}
	where := "the run's first workflow task"
	if t.after != nil {
		inc.Step, inc.ActivityId = t.after.step, t.after.id
		where = fmt.Sprintf("the workflow task after %s %s", t.after.step, t.after.outcome())
	}
	if t.timedOut {
		inc.Kind = KindWorkerStopped
		inc.Cause = fmt.Sprintf("%s timed out (%s, event %d): the Worker handling it stopped or hung, as Temporal's timeout "+
			"says; another attempt went to a Worker", where, t.timeout.String(), t.event)
		return inc
	}
	inc.Kind = KindWorkflowTaskFailed
	inc.Cause = fmt.Sprintf("the Worker failed %s (%s, event %d)%s", where, t.cause, t.event, quoted(", saying: ", t.message))
	return inc
}

// quoted is s after prefix, quoted, or nothing when s is empty: text from the Worker, a tool, a
// model or a person, as theirs.
func quoted(prefix, s string) string {
	if s == "" {
		return ""
	}
	return prefix + fmt.Sprintf("%q", s)
}

func parseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}
