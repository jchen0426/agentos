package gateway

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jchen0426/agentos/internal/explain"
	"github.com/jchen0426/agentos/internal/githubtool"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/tasks"
)

// RunRecords is what github.comment_commit reads of agentd's own record of a run: its effects'
// results, and its approvals.
type RunRecords struct{ *ledger.Ledger }

func (r RunRecords) RunApprovals(ctx context.Context, workflowID string) ([]githubtool.Approval, error) {
	rows, err := r.Ledger.RunApprovals(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	out := make([]githubtool.Approval, 0, len(rows))
	for _, a := range rows {
		ga := githubtool.Approval{ID: a.ApprovalID, Tool: a.Tool, Status: a.Status, DecidedBy: a.DecidedBy.String,
			DecidedAt: a.DecidedAt.String, Reason: a.Reason.String, ExpiresAt: a.ExpiresAt}
		if a.Status == approvalApproved {
			// Approved, and refused or lost after: the report must not read as if it went out.
			e, err := r.Ledger.Q.GetEffect(ctx, a.EffectID)
			if err != nil {
				return nil, err
			}
			ga.Write = map[string]string{effectAbandoned: "not sent", effectDenied: "not sent", effectDryRun: "not sent",
				effectFailed: "failed", effectUnknown: "outcome unknown"}[e.Status]
			if ga.Write != "" {
				ga.WriteReason = e.Reason.String
			}
			if e.Status == effectDryRun {
				// The approval was decided under an agentd that could reach the tool's system, and the
				// write taken up by one that could not. An earlier attempt may have made a request of
				// it, which did not take effect, and what that request made may remain.
				ga.WriteReason = "agentd could not reach the tool's system, and only dry-ran it"
				if e.LastRequestAt.Valid {
					ga.WriteReason += fmt.Sprintf(". A request of it went out at %s, and what that request made may remain",
						e.LastRequestAt.String)
				}
			}
		}
		out = append(out, ga)
	}
	return out, nil
}

// ReportVerdicts gives a run's report its outcome (githubtool.Verdicts): runs explain's verdict on the
// run's history and agentd's records of its calls before the report (explain.Report). The report is
// rendered once, in the first attempt of the call that gets this far, and what it says stands. So
// while Temporal cannot be reached (explain.Unreachable), ReportVerdict fails, before anything of the
// write is recorded: the attempt fails, and Temporal tries the call again once it answers (the
// attempt could not have told Temporal it was done anyway). The verdict is unknown only when Temporal
// answers that it has no history of the run, or with an error of its own: the report then says only
// that agentd could not read the run, and the log has the error. A step the task does not name goes
// in the verdict's words only, which the report fences.
type ReportVerdicts struct {
	Ledger   *ledger.Ledger
	Temporal explain.Temporal
	Tasks    tasks.Registry
	Log      *slog.Logger
}

func (r ReportVerdicts) ReportVerdict(ctx context.Context, workflowID, reason, errorType string) (githubtool.Verdict, error) {
	// The run's row, as one of the ledger's read transactions reads it (Ledger.Read checks the schema
	// first): it names the execution and the task the report judges, which ReadLedger holds the
	// snapshot it reads after Temporal to.
	var run db.Run
	if err := r.Ledger.Read(ctx, func(q *db.Queries) error {
		var err error
		run, err = q.GetRun(ctx, workflowID)
		return err
	}); err != nil {
		return githubtool.Verdict{}, err
	}
	log := r.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With("workflow_id", workflowID)
	in := explain.Input{RunID: run.WorkflowID, Task: r.Tasks[run.Task], Run: run, Now: ledger.Clock()}
	err := explain.ReadTemporal(ctx, r.Temporal, &in)
	if err != nil && !explain.Unreachable(err) {
		in.Unread = err.Error() // Temporal's own answer: the verdict is unknown
	}
	var o explain.Outcome
	if err == nil || in.Unread != "" {
		o, err = explain.Report(in, func(want explain.Wanted) (explain.Ledger, error) {
			var l explain.Ledger
			err := r.Ledger.Read(ctx, func(q *db.Queries) error {
				var err error
				l, err = explain.ReadLedger(ctx, q, run, want)
				return err
			})
			return l, err
		}, reason, errorType)
	}
	switch {
	case explain.Unreachable(err):
		log.Warn("the run's report waits for Temporal, which agentd could not reach: the call fails, for Temporal to try "+
			"it again", "error", err)
		return githubtool.Verdict{}, fmt.Errorf("the run's report waits for Temporal, which agentd could not reach: %w", err)
	case err != nil:
		return githubtool.Verdict{}, err
	case o.Detail != "":
		log.Warn("the run's report says its outcome is unknown: agentd could not read the run in Temporal", "error", o.Detail)
	}
	return verdictOf(o), nil
}

// verdictOf is the report's verdict from the outcome: the step only when the task names it.
func verdictOf(o explain.Outcome) githubtool.Verdict {
	v := githubtool.Verdict{Verdict: o.Verdict, Kind: o.Kind, Cause: o.Cause, Claim: o.Claim, Cut: o.Cut}
	if o.Named {
		v.Step = o.Step
	}
	return v
}
