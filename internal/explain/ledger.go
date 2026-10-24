package explain

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jchen0426/agentos/internal/budget"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// Bounds on what ReadLedger reads of a run, whose Worker makes its calls: the run's records, model
// requests (and the answers whose words it reads), approvals, effects and loop-guard rows, and
// agentd's starts since the run began. What is past a bound is not read, and Ledger.Cut says so.
// Variables, for a test to lower.
var (
	maxRecords   = 5000
	maxAfterCut  = 20000 // the run's call records past maxRecords, read for the attempts that failed
	maxRequests  = 2000
	maxResponses = 200
	maxApprovals = 500
	maxEffects   = 1000
	maxCalls     = 5000
	maxStarts    = 1000
)

// ReadLedger reads what Explain and Report need of a run in agentd's ledger, through q, each kind of
// row to its bound, and what they want of it besides (want): each record a result names, by its seq,
// whatever the bounds; and, when the run's records were cut short, agentd's records of the attempts
// that failed, from the run's call records past the cut, to a bound of their own. The run's records
// are those of its execution in Temporal (run.RunID). One snapshot, when q is a read transaction's
// (ledger.Read).
func ReadLedger(ctx context.Context, q *db.Queries, run db.Run, want Wanted) (Ledger, error) {
	workflowID, execution := run.WorkflowID, run.RunID.String
	var l Ledger
	// The run judged is the run in this snapshot: its execution and its task, which the caller read
	// before it read Temporal.
	switch cur, err := q.GetRun(ctx, workflowID); {
	case err != nil:
		return l, fmt.Errorf("run %s: %w", workflowID, err)
	case cur.RunID != run.RunID || cur.Task != run.Task:
		return l, fmt.Errorf("%w: run %s is execution %q of task %q, not %q of %q", ErrRunChanged, workflowID, cur.RunID.String,
			cur.Task, run.RunID.String, run.Task)
	}
	cut := l.cut
	// The run's own records: those of calls from executions agentd did not start are not the run's,
	// and a flood of them must not crowd its own out of the bound. They are counted.
	records, err := q.ListRunOwnAuditRecords(ctx, db.ListRunOwnAuditRecordsParams{WorkflowID: workflowID, Seq: 0,
		TemporalRunID: execution, Max: int64(maxRecords) + 1})
	if err != nil {
		return l, err
	}
	if l.Unknown, err = q.CountRunUnknownRecords(ctx, db.CountRunUnknownRecordsParams{WorkflowID: workflowID,
		TemporalRunID: execution}); err != nil {
		return l, err
	}
	if l.RecordsCut = cut(len(records), maxRecords, "audit records"); l.RecordsCut {
		records = records[:maxRecords]
	}
	l.Records = records
	if err := readWanted(ctx, q, run, want, &l); err != nil {
		return l, err
	}
	// agentd's starts since the run's first record; all of them, to the bound, for a run older than
	// the log.
	var after int64
	if len(l.Records) > 0 {
		after = l.Records[0].Seq
	}
	if l.Starts, err = q.ListAgentdStarts(ctx, db.ListAgentdStartsParams{Seq: after, Limit: int64(maxStarts) + 1}); err != nil {
		return l, err
	}
	if cut(len(l.Starts), maxStarts, "starts of agentd since it began") {
		l.Starts = l.Starts[:maxStarts]
	}
	first, err := q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: 1, Limit: 1})
	if err != nil {
		return l, err
	}
	if len(first) == 1 {
		l.LogBegan, _ = time.Parse(time.RFC3339Nano, first[0].At)
	}
	calls, err := q.ListRunModelCalls(ctx, db.ListRunModelCallsParams{WorkflowID: workflowID, Limit: int64(maxRequests) + 1})
	if err != nil {
		return l, err
	}
	if cut(len(calls), maxRequests, "model requests") {
		calls = calls[:maxRequests]
	}
	for i, c := range calls {
		m := ModelRequest{ModelCall: c}
		if c.ResponseBlob.Valid && i < maxResponses {
			if err := readAnswer(ctx, q, c.ResponseBlob.String, &m); err != nil {
				return l, err
			}
		}
		l.Requests = append(l.Requests, m)
	}
	if l.Approvals, err = q.ListRunApprovalsLimit(ctx, db.ListRunApprovalsLimitParams{WorkflowID: workflowID,
		Limit: int64(maxApprovals) + 1}); err != nil {
		return l, err
	}
	if cut(len(l.Approvals), maxApprovals, "approvals") {
		l.Approvals = l.Approvals[:maxApprovals]
	}
	if l.Effects, err = q.ListRunEffects(ctx, db.ListRunEffectsParams{WorkflowID: workflowID, Limit: int64(maxEffects) + 1}); err != nil {
		return l, err
	}
	if cut(len(l.Effects), maxEffects, "effects") {
		l.Effects = l.Effects[:maxEffects]
	}
	if l.Calls, err = q.ListRunCalls(ctx, db.ListRunCallsParams{WorkflowID: workflowID, Limit: int64(maxCalls) + 1}); err != nil {
		return l, err
	}
	if cut(len(l.Calls), maxCalls, "calls in the loop guard's table") {
		l.Calls = l.Calls[:maxCalls]
	}
	t, err := q.GetBudgetTotals(ctx, workflowID)
	l.Budget = budget.Totals{Limit: t.LimitMicros, Spent: t.SpentMicros, Held: t.HeldMicros}
	return l, err
}

// cut says that the run has more than bound rows of what (rows of them read): Cut and CutWhat say so.
func (l *Ledger) cut(rows int, bound int, what string) bool {
	if rows <= bound {
		return false
	}
	l.Cut = append(l.Cut, fmt.Sprintf("ledger: the run has more than %d %s; the explanation reads the first %d", bound,
		what, bound))
	l.CutWhat = append(l.CutWhat, fmt.Sprintf("%s past the first %d", what, bound))
	return true
}

// readWanted reads the records want names that l.Records does not hold. A record a result names, by
// seq, on its own (whichever run's it is, or none: a seq past the chain's head has none). When
// l.Records was cut short, agentd's records of the attempts want names, from the run's call records
// after the last one l.Records holds, in one pass to maxAfterCut: an attempt with none among them is
// one whose record agentd did not read, when the pass was cut short too, or could not be read. A
// read that fails is said in l.NamedErr or l.AttemptErr, for the result or failure it was for to go
// unchecked; one the context ended fails ReadLedger.
func readWanted(ctx context.Context, q *db.Queries, run db.Run, want Wanted, l *Ledger) error {
	l.Named, l.NamedErr, l.AttemptErr = map[int64]db.AuditLog{}, map[int64]string{}, map[Attempt]string{}
	have := make(map[int64]bool, len(l.Records))
	for _, r := range l.Records {
		have[r.Seq] = true
	}
	for _, seq := range want.Seqs {
		if _, done := l.Named[seq]; have[seq] || done {
			continue
		}
		switch r, err := q.GetAuditRecord(ctx, seq); {
		case errors.Is(err, sql.ErrNoRows):
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil:
			l.NamedErr[seq] = err.Error()
		default:
			l.Named[seq] = r
		}
	}
	if !l.RecordsCut || len(want.Attempts) == 0 {
		return nil
	}
	var after int64
	if n := len(l.Records); n > 0 {
		after = l.Records[n-1].Seq
	}
	rows, err := q.ListRunOwnCallRecords(ctx, db.ListRunOwnCallRecordsParams{WorkflowID: run.WorkflowID, Seq: after,
		TemporalRunID: run.RunID.String, Max: int64(maxAfterCut) + 1})
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case err != nil:
		for _, a := range want.Attempts {
			l.AttemptErr[a] = err.Error()
		}
		return nil
	}
	if l.AttemptsCut = len(rows) > maxAfterCut; l.AttemptsCut {
		rows = rows[:maxAfterCut]
		l.Cut = append(l.Cut, fmt.Sprintf("ledger: the run has more than %d call records after its first %d audit records; "+
			"the explanation reads the first %d of them, for the attempts that failed", maxAfterCut, maxRecords, maxAfterCut))
		l.CutWhat = append(l.CutWhat, fmt.Sprintf("call records past the first %d after those", maxAfterCut))
	}
	wanted := make(map[Attempt]bool, len(want.Attempts))
	for _, a := range want.Attempts {
		wanted[a] = true
	}
	found := map[Attempt]bool{}
	for _, r := range rows {
		var p struct {
			ActivityID string `json:"activity_id"`
			Attempt    int32  `json:"attempt"`
		}
		if json.Unmarshal([]byte(r.Payload), &p) != nil {
			continue
		}
		if a := (Attempt{ActivityID: p.ActivityID, Attempt: p.Attempt}); wanted[a] {
			l.Named[r.Seq], found[a] = r, true
		}
	}
	if l.AttemptsCut {
		// Its record may be past what was read: how the attempt ended is not known (one l.Records
		// holds is, whatever this says).
		for _, a := range want.Attempts {
			if !found[a] {
				l.AttemptErr[a] = "past agentd's bound on the run's records it reads"
			}
		}
	}
	return nil
}

// ErrRunChanged: the run's row is not the one the caller read before it read Temporal (another
// execution, or another task): what was read of Temporal is not of the run in the ledger now.
var ErrRunChanged = errors.New("the run changed while it was read")

// readAnswer reads a model request's answer from its response blob: its status, and a refusal's
// words. A blob the ledger no longer has leaves them empty: what the answer was is not essential.
func readAnswer(ctx context.Context, q *db.Queries, ref string, m *ModelRequest) error {
	if m.StopReason.String == statusInvalidOutput {
		m.Status = statusInvalidOutput // the gateway's own verdict on the answer, kept with it
		return nil
	}
	h, ok := strings.CutPrefix(ref, "sha256:")
	if !ok {
		return nil
	}
	b, err := q.GetBlob(ctx, h)
	if err != nil {
		return nil //nolint:nilerr // see above
	}
	var resp struct {
		Status string `json:"status"`
		Value  struct {
			Category    string `json:"category"`
			Explanation string `json:"explanation"`
		} `json:"value"`
	}
	if json.Unmarshal(b, &resp) == nil {
		m.Status, m.Category, m.Explanation = resp.Status, resp.Value.Category, resp.Value.Explanation
	}
	return nil
}
