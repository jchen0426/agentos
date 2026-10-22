package control

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/safetext"
)

// VerifyAudit checks the audit log (design doc, section 8), against what the ledger cannot change:
//   - The hash chain, walked from the first record: whether the ledger still holds the records as
//     they were appended.
//   - The anchors of it Temporal's history holds: the _audit of every result Invoke returned, which
//     names its call's record, and the chain's heads agentd pushes to the chain's anchor workflows.
//     They say whether it is the chain that was appended: one rewritten consistently from some record
//     on is whole again, but its records are not the ones the anchors named. Records older than
//     twice the push interval that no pushed head covers are a mismatch too: their anchor workflow
//     is gone, or was never agentd's.
//   - The state the records imply, against the tables: a table changed without its record; and every
//     blob's bytes against the sha256 that is their key.
//   - Which runs there are: the executions on the Worker's task queue that Temporal lists, the runs
//     the audit log records, the runs table's rows. A run one of them has and the runs table has
//     otherwise, or not at all, is a mismatch.
//
// A history Temporal has no more is a mismatch while the namespace's retention says it should have
// it, counted from when the execution was last seen live (its last call), and is reported unchecked
// after that, or apart for a run made before the log began; any other failure to read one fails the
// verification.
func (s *Server) VerifyAudit(ctx context.Context, req *agentosv1.VerifyAuditRequest) (*agentosv1.AuditReport, error) {
	retention, err := s.retention(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "the namespace's retention: %v", err)
	}
	out := &agentosv1.AuditReport{RetentionSeconds: int64(retention / time.Second)}
	// The ledger itself first: verify reads everything else through its schema and its pages.
	if stop, err := s.ledgerUnsound(ctx, out); err != nil {
		return nil, err
	} else if stop {
		return out, nil
	}
	// Temporal first: an execution it lists now was started after its run's creation was recorded,
	// so the ledger read after it holds that record.
	infos, err := s.listExecutionInfos(ctx, fmt.Sprintf("TaskQueue = '%s'", quote(s.TaskQueue)))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "list the runs Temporal has: %v", err)
	}
	listed, running := map[string][]string{}, map[string]bool{}
	var terminated [][2]string // (workflow ID, run ID) of each execution Temporal says was terminated
	for _, e := range infos {
		id := e.GetExecution().GetWorkflowId()
		listed[id] = append(listed[id], e.GetExecution().GetRunId())
		running[id] = running[id] || e.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		if e.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED {
			terminated = append(terminated, [2]string{id, e.GetExecution().GetRunId()})
		}
	}
	// The runs table, the records of runs and the chain's first record, as one snapshot.
	var rows []db.Run
	var lives, first []db.AuditLog
	var lasts []db.ListRunLastRecordsRow
	if err := s.Ledger.Read(ctx, func(q *db.Queries) error {
		var err error
		if rows, err = q.ListAllRuns(ctx); err != nil {
			return err
		}
		if lives, err = q.ListRunLifeRecords(ctx); err != nil {
			return err
		}
		if lasts, err = q.ListRunLastRecords(ctx); err != nil {
			return err
		}
		first, err = q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: 1, Limit: 1})
		return err
	}); err != nil {
		return nil, err
	}
	// When agentd began to keep the log: rows and results from before then have no records. The first
	// record names the chain, and its anchor workflows.
	var began time.Time
	var chain string
	if len(first) == 1 {
		began, _ = time.Parse(time.RFC3339Nano, first[0].At)
		chain = audit.AnchorChain(first[0].Sha256)
	}
	out.Chain = chain
	runs := runChecks(listed, rows, lives, lasts)
	if id := req.GetRunId(); id != "" {
		c, ok := runs[id]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "neither Temporal, the audit log nor the ledger has a run %q", id)
		}
		runs = map[string]*runCheck{id: c}
	}
	type execution struct {
		workflowID string
		anchors    []anchor
		started    time.Time // when Temporal started it
	}
	var read []execution
	closes := map[[2]string]*historypb.HistoryEvent{} // the last event of each history read: how it ended
	anchored := false                                 // some result names a record
	now := ledger.Clock()
	for _, workflowID := range slices.Sorted(maps.Keys(runs)) {
		c := runs[workflowID]
		for _, why := range c.mismatches() {
			out.RunsMismatched = append(out.RunsMismatched, &agentosv1.RunMismatch{RunId: safetext.Line(workflowID),
				Why: safetext.Line(why)})
		}
		for _, runID := range c.executions() {
			events, err := s.history(ctx, workflowID, runID)
			switch {
			case errors.Is(err, errNoHistory) && !c.lastAt.IsZero() && now.Sub(c.lastAt) < retention:
				out.RunsMismatched = append(out.RunsMismatched, &agentosv1.RunMismatch{RunId: safetext.Line(workflowID),
					Why: safetext.Line(fmt.Sprintf("Temporal has no history of execution %s, yet the run's last record, at %s, "+
						"is within the namespace's retention of %s", runID, c.lastAt.Format(time.RFC3339), retention))})
				continue
			case errors.Is(err, errNoHistory) && c.beforeLog(began):
				out.RunsBeforeLog = append(out.RunsBeforeLog, &agentosv1.UnavailableRun{RunId: safetext.Line(workflowID),
					Why: safetext.Line(fmt.Sprintf("execution %s: the run was made at %s, before the audit log began, and the log "+
						"has no record of it; Temporal no longer has it", runID, c.row.CreatedAt))})
				continue
			case errors.Is(err, errNoHistory):
				out.RunsUnavailable = append(out.RunsUnavailable, &agentosv1.UnavailableRun{RunId: safetext.Line(workflowID),
					Why: safetext.Line(fmt.Sprintf("execution %s: Temporal no longer has it, past the namespace's retention of %s",
						runID, retention))})
				continue
			case err != nil:
				return nil, status.Errorf(codes.Unavailable, "the history of run %s, execution %s: %v", workflowID, runID, err)
			}
			x := execution{workflowID: workflowID, anchors: anchorsOf(events)}
			if len(events) > 0 {
				x.started = events[0].GetEventTime().AsTime()
				closes[[2]string{workflowID, runID}] = events[len(events)-1]
			}
			unanchored := false
			for _, a := range x.anchors {
				anchored = anchored || a.head != nil
				unanchored = unanchored || (a.head == nil && a.unreadable == "")
			}
			if unanchored && !began.IsZero() && x.started.Before(began) {
				out.RunsBeforeLog = append(out.RunsBeforeLog, &agentosv1.UnavailableRun{RunId: safetext.Line(workflowID),
					Why: safetext.Line(fmt.Sprintf("execution %s started at %s, before the audit log began: results of it name no record",
						runID, x.started.UTC().Format(time.RFC3339)))})
			}
			read = append(read, x)
		}
	}
	allowChain, allowReplaced := map[string]bool{}, map[string]bool{}
	for _, key := range req.GetAllowChains() {
		allowChain[key] = true
	}
	for _, key := range req.GetAllowReplacedLedgers() {
		allowReplaced[key] = true
	}
	external, misplaced, foreign, unread, err := s.externalAnchors(ctx, chain, allowChain, allowReplaced)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "the external anchors: %v", err)
	}
	out.Unread = unread
	out.OtherChains, out.OtherAnchorWorkflows = foreign.chains, foreign.accepted
	if out.UncheckedTables, err = s.tablesUnchecked(ctx); err != nil {
		return nil, err
	}
	// A log with no records cannot be agentd's (each start appends one), and its anchors say there were.
	if len(first) == 0 && (foreign.refused || anchored) {
		misplaced = append(misplaced, &agentosv1.AnchorMismatch{Why: "Temporal holds heads or anchored results, " +
			"but the audit log has no records"})
	}
	// The chain, the state it implies and the records the anchors name, as one snapshot, after the
	// anchors were read: each names a record committed before it went out, so the snapshot has it.
	// What this agentd appended and pushed last, and whether it found the chain moved, are read as the
	// snapshot is taken, under the write turn (ReadAtTurn): this agentd appends only in its write
	// transactions, which take the turn, and its tip moves as each commits, before the turn goes. So
	// the snapshot holds the last record this agentd appended (and the head it pushed, appended
	// before), and every record the snapshot holds after that one, this agentd did not append. Read
	// the other way round, the tip before the snapshot, an append of this agentd's committed in
	// between would show as one of someone else's; the snapshot before the tip, a record committed
	// but not yet noted in the tip would.
	var appended, pushed audit.Head
	var moved error
	at := func() {
		appended, pushed = audit.Tip(s.Ledger)
		moved = audit.Moved(s.Ledger)
	}
	err = s.Ledger.ReadAtTurn(ctx, at, func(q *db.Queries) error {
		rep, err := audit.Verify(ctx, q)
		if err != nil {
			return err
		}
		out.Records, out.HeadSeq, out.HeadSha256 = rep.Records, rep.Head.Seq, rep.Head.Sha256
		out.BrokenAtSeq, out.BrokenReason = rep.BrokenAt, rep.Broken
		for _, c := range []struct {
			what string
			head audit.Head
		}{{"this agentd's start (agentd.started)", s.StartRecord}, {"the last record this agentd appended", appended},
			{"the last head this agentd pushed to Temporal", pushed}} {
			switch err := audit.Held(ctx, q, c.head); {
			case errors.Is(err, audit.ErrChainMoved):
				out.AnchorsMismatched = append(out.AnchorsMismatched, &agentosv1.AnchorMismatch{Seq: c.head.Seq,
					Why: safetext.Line(fmt.Sprintf("%s: %v; the log was cut, rewritten or replaced under agentd", c.what, err))})
			case err != nil:
				return err
			}
		}
		// Records after the last one this agentd appended were written by someone else: correctly
		// chained, they leave every record this agentd appended in place, and agentd, which saw its
		// next append land on another's record, appends nothing more (Moved).
		if appended != (audit.Head{}) {
			after, err := q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: appended.Seq + 1, Limit: maxForeign + 1})
			if err != nil {
				return err
			}
			if len(after) > 0 {
				out.AnchorsMismatched = append(out.AnchorsMismatched, foreignRecords(appended, after))
			}
		}
		if moved != nil {
			out.AnchorsMismatched = append(out.AnchorsMismatched, &agentosv1.AnchorMismatch{Why: safetext.Line(fmt.Sprintf(
				"agentd found the audit log changed under it, and has appended nothing to it since: %v", moved))})
		}
		var p audit.Projection
		if err := p.Read(ctx, q); err != nil {
			return err
		}
		// Rows from before the log began have no records; with no log, no row has, and every one is a difference.
		since := audit.Since{At: began, Runs: true}
		if began.IsZero() {
			since.At = time.Unix(0, 0)
		}
		diffs, err := p.Differences(ctx, q, since)
		if err != nil {
			return err
		}
		for _, d := range diffs {
			out.StateMismatched = append(out.StateMismatched, &agentosv1.StateMismatch{Kind: d.Kind,
				Id: safetext.Line(d.ID), Why: safetext.Line(d.Why)})
		}
		bad, err := blobMismatches(ctx, q)
		if err != nil {
			return err
		}
		out.StateMismatched = append(out.StateMismatched, bad...)
		// Each execution Temporal says was terminated, agentd recorded terminating first (terminate):
		// one with no record of it was terminated outside agentd, by someone with Temporal's API or
		// by Temporal itself (a workflow task too large for it). That changes nothing in the ledger,
		// so it is no mismatch; it is listed, with what its history says of it. Of a run made before
		// the log began, nothing is said.
		for _, x := range terminated {
			if c := runs[x[0]]; c == nil || c.beforeLog(began) {
				continue
			}
			ok, err := terminatedByAgentd(ctx, q, x[0], x[1])
			if err != nil {
				return err
			}
			if !ok {
				out.TerminatedOutsideAgentd = append(out.TerminatedOutsideAgentd, terminatedOutside(x[0], x[1], closes[x]))
			}
		}
		for _, x := range read {
			records, err := q.ListRunAuditRecords(ctx, x.workflowID)
			if err != nil {
				return err
			}
			n, bad := checkAnchors(x.workflowID, x.anchors, records, began, x.started)
			out.AnchorsChecked += n
			out.AnchorsMismatched = append(out.AnchorsMismatched, bad...)
			out.RunsChecked++
		}
		for _, a := range external {
			rec, err := q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: a.head.Seq, Limit: 1})
			if err != nil {
				return err
			}
			out.ExternalAnchorsChecked++
			switch {
			case len(rec) == 0 || rec[0].Seq != a.head.Seq:
				out.AnchorsMismatched = append(out.AnchorsMismatched, &agentosv1.AnchorMismatch{RunId: a.workflowID,
					Seq: a.head.Seq, Why: fmt.Sprintf("the chain's head pushed to Temporal was record %d, which the audit log does not have",
						a.head.Seq)})
			case rec[0].Sha256 != a.head.Sha256:
				out.AnchorsMismatched = append(out.AnchorsMismatched, &agentosv1.AnchorMismatch{RunId: a.workflowID,
					Seq: a.head.Seq, Why: fmt.Sprintf("the chain's head pushed to Temporal was record %d with sha256 %s; the audit log has %s",
						a.head.Seq, a.head.Sha256, rec[0].Sha256)})
			default:
				out.LastExternalAnchorSeq = max(out.LastExternalAnchorSeq, a.head.Seq)
			}
		}
		out.AnchorsMismatched = append(out.AnchorsMismatched, misplaced...)
		out.RecordsAfterExternalAnchor = max(0, out.HeadSeq-out.LastExternalAnchorSeq)
		bad, cut, unrecorded, err := attempts(ctx, q, running, s.Attempts, s.closedNow(ctx))
		if err != nil {
			return err
		}
		out.StateMismatched, out.AttemptsCutShort, out.AttemptsUnrecorded = append(out.StateMismatched, bad...), cut, unrecorded
		// Records old enough that a pushed head should cover them. Not while this agentd is new: the
		// records a crash left before its start get their head with its first push.
		if s.AnchorEvery > 0 && chain != "" && now.Sub(s.StartedAt) > 2*s.AnchorEvery {
			m, err := uncovered(ctx, q, chain, out.LastExternalAnchorSeq, now, 2*s.AnchorEvery, retention)
			if err != nil {
				return err
			}
			if m != nil {
				out.AnchorsMismatched = append(out.AnchorsMismatched, m)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// maxForeign is how many of the records after the last one this agentd appended a report names.
const maxForeign = 10

// foreignRecords is the mismatch of records after appended, the last record this agentd appended,
// which it did not append: the first maxForeign of them named, what they say they are quoted.
func foreignRecords(appended audit.Head, after []db.AuditLog) *agentosv1.AnchorMismatch {
	var named []string
	for _, r := range after[:min(len(after), maxForeign)] {
		named = append(named, fmt.Sprintf("%d (%q by %q)", r.Seq, r.Type, r.Actor))
	}
	if len(after) > maxForeign {
		named = append(named, "and more")
	}
	return &agentosv1.AnchorMismatch{Seq: after[0].Seq, Why: safetext.Line(fmt.Sprintf("the audit log goes on past the "+
		"last record this agentd appended (%d) with records it did not append: %s; someone else wrote them",
		appended.Seq, strings.Join(named, ", ")))}
}

// maxIntegrityLines is how much of what PRAGMA integrity_check finds a report names.
const maxIntegrityLines = 10

// ledgerUnsound reports, as mismatches, how the ledger's schema differs from its migrations' (a
// trigger or an index of anyone else's changes what agentd's writes do, and what a read returns:
// internal/ledger/schema.go), and else what PRAGMA integrity_check finds wrong with its file (its
// pages, and each index against its table: an index whose rootpage was swapped, or whose rows were
// changed beside it, which a comparison of sqlite_master does not see). Either way verify reads no
// further, and says so: what it would read through such a ledger could be anything.
func (s *Server) ledgerUnsound(ctx context.Context, out *agentosv1.AuditReport) (bool, error) {
	schema, err := s.Ledger.SchemaDifferences(ctx)
	if err != nil {
		return false, err
	}
	for _, d := range schema {
		out.StateMismatched = append(out.StateMismatched, &agentosv1.StateMismatch{Kind: "schema", Id: "ledger",
			Why: safetext.Line(d)})
	}
	if len(schema) == 0 {
		found, err := s.Ledger.IntegrityCheck(ctx, maxIntegrityLines)
		if err != nil {
			return false, err
		}
		for _, f := range found {
			out.StateMismatched = append(out.StateMismatched, &agentosv1.StateMismatch{Kind: "integrity", Id: "ledger",
				Why: fmt.Sprintf("PRAGMA integrity_check: %.300q", f)})
		}
		if len(found) == maxIntegrityLines {
			out.StateMismatched = append(out.StateMismatched, &agentosv1.StateMismatch{Kind: "integrity", Id: "ledger",
				Why: fmt.Sprintf("PRAGMA integrity_check stops at %d lines: there may be more", maxIntegrityLines)})
		}
	}
	if len(out.StateMismatched) == 0 {
		return false, nil
	}
	out.Unread = append(out.Unread, &agentosv1.Unread{What: "the rest of the audit", Count: 1, Why: "the ledger's " +
		"schema is not its migrations', or SQLite finds its file damaged: what verify would read through it could be anything"})
	return true, nil
}

// terminatedOutside is execution runID of run workflowID, terminated outside agentd, as the last
// event of its history says: when, and who its caller said it was and why, which is that caller's
// own word. Nothing but the IDs when the history was not read, or does not end so.
func terminatedOutside(workflowID, runID string, last *historypb.HistoryEvent) *agentosv1.TerminatedExecution {
	t := &agentosv1.TerminatedExecution{RunId: safetext.Line(workflowID), TemporalRunId: safetext.Line(runID)}
	if a := last.GetWorkflowExecutionTerminatedEventAttributes(); a != nil {
		if at := audit.EventTime(last); !at.IsZero() {
			t.At = at.UTC().Format(time.RFC3339Nano)
		}
		t.ClaimedIdentity, t.ClaimedReason = safetext.Line(audit.Text(a.GetIdentity())), safetext.Line(audit.Text(a.GetReason()))
	}
	return t
}

// terminatedByAgentd reports whether the run's records hold agentd's run.terminating of execution
// runID, or its run.terminated (alone in the records of an agentd from before run.terminating).
func terminatedByAgentd(ctx context.Context, q *db.Queries, workflowID, runID string) (bool, error) {
	records, err := q.ListRunAuditRecords(ctx, workflowID)
	if err != nil {
		return false, err
	}
	for _, r := range records {
		var p struct {
			TemporalRunID string `json:"temporal_run_id"`
		}
		if (r.Type == "run.terminating" || r.Type == "run.terminated") && r.Actor == audit.Agentd &&
			json.Unmarshal([]byte(r.Payload), &p) == nil && p.TemporalRunID == runID {
			return true, nil
		}
	}
	return false, nil
}

// quote is s inside a visibility query's single quotes.
func quote(s string) string { return strings.ReplaceAll(s, "'", `\'`) }

// retention is how long the namespace keeps a closed execution's history.
func (s *Server) retention(ctx context.Context) (time.Duration, error) {
	ns := s.Namespace
	if ns == "" {
		ns = "default"
	}
	d, err := s.Temporal.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: ns})
	if err != nil {
		return 0, err
	}
	return d.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration(), nil
}

// runCheck is what Temporal, the audit log and the runs table each say of one run (workflow ID).
type runCheck struct {
	workflowID string
	listed     []string // the executions Temporal lists under the workflow ID, by run ID
	row        *db.Run  // the runs table's row
	created    int64    // the seq of its run.created; 0 for none
	started    string   // the execution its run.started names
	startedSeq int64
	discarded  int64     // the seq of its run.discarded
	lastAt     time.Time // when its execution was last seen live: its last call's record, or its start's
	records    int64     // all its records
}

// beforeLog reports whether the run was made before the audit log began (at began), and the log
// has no record of it: a run of an older agentd, which Temporal may well have forgotten. Without a
// time the log began at, nothing was before it.
func (c *runCheck) beforeLog(began time.Time) bool {
	return c.row != nil && c.records == 0 && !began.IsZero() && !after(c.row.CreatedAt, began)
}

// after reports whether ledger time s is at or after t; a time that cannot be read is.
func after(s string, t time.Time) bool {
	at, err := time.Parse(time.RFC3339Nano, s)
	return err != nil || !at.Before(t)
}

// runChecks gathers what Temporal (listed), the runs table and the records say of each run.
func runChecks(listed map[string][]string, rows []db.Run, lives []db.AuditLog, lasts []db.ListRunLastRecordsRow) map[string]*runCheck {
	runs := map[string]*runCheck{}
	get := func(workflowID string) *runCheck {
		if runs[workflowID] == nil {
			runs[workflowID] = &runCheck{workflowID: workflowID}
		}
		return runs[workflowID]
	}
	for workflowID, ids := range listed {
		get(workflowID).listed = ids
	}
	for _, r := range rows {
		get(r.WorkflowID).row = &r
	}
	for _, rec := range lives {
		c := get(rec.WorkflowID)
		switch rec.Type {
		case "run.created":
			c.created = rec.Seq
		case "run.started":
			var p struct {
				TemporalRunID string `json:"temporal_run_id"`
			}
			if json.Unmarshal([]byte(rec.Payload), &p) == nil {
				c.started, c.startedSeq = p.TemporalRunID, rec.Seq
			}
		case "run.discarded":
			c.discarded = rec.Seq
		}
	}
	for _, l := range lasts {
		if c := runs[l.WorkflowID]; c != nil {
			c.lastAt, _ = time.Parse(time.RFC3339Nano, l.At)
			c.records = l.Records
		}
	}
	return runs
}

// mismatches are the ways the runs table disagrees with Temporal and the audit log about the run,
// each said once. None is passed over: a run whose start is being recorded at that moment is one
// too, which verifying again a moment later no longer finds.
func (c *runCheck) mismatches() []string {
	var out []string
	recorded := "none"
	if c.row != nil && c.row.RunID.Valid {
		recorded = c.row.RunID.String
	}
	for _, id := range c.listed {
		switch {
		case c.row == nil && c.discarded != 0:
			out = append(out, fmt.Sprintf("Temporal has execution %s of the run, which the audit log records as discarded "+
				"(seq %d) and the ledger has no row of", id, c.discarded))
		case c.row == nil:
			out = append(out, fmt.Sprintf("Temporal has execution %s of the run, and the ledger has no row of the run", id))
		case !c.row.RunID.Valid && c.started == "": // with a start in the audit log, said below
			out = append(out, fmt.Sprintf("Temporal has execution %s of the run, and the ledger records no execution of it", id))
		case c.row.RunID.Valid && c.row.RunID.String != id:
			out = append(out, fmt.Sprintf("Temporal has execution %s under the run's ID, and the ledger records execution %s",
				id, recorded))
		}
	}
	switch {
	case c.row == nil && c.discarded == 0 && (c.created != 0 || c.started != ""):
		out = append(out, fmt.Sprintf("the audit log records the run (seq %d), and the ledger has no row of it",
			max(c.created, c.startedSeq)))
	case c.row != nil && c.discarded != 0:
		out = append(out, fmt.Sprintf("the audit log records the run as discarded (seq %d), and the ledger has a row of it",
			c.discarded))
	case c.row != nil && c.started != "" && recorded != c.started:
		out = append(out, fmt.Sprintf("the audit log records its execution as %s (seq %d), and the ledger as %s",
			c.started, c.startedSeq, recorded))
	}
	return out
}

// executions are the run's executions whose histories hold its anchors: those Temporal lists, and
// those the runs table and the audit log name.
func (c *runCheck) executions() []string {
	ids := slices.Clone(c.listed)
	if c.row != nil && c.row.RunID.Valid {
		ids = append(ids, c.row.RunID.String)
	}
	if c.started != "" {
		ids = append(ids, c.started)
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// listExecutions lists the executions query matches in agentd's namespace, by workflow ID: running
// or closed, until the namespace's retention removes them.
func (s *Server) listExecutions(ctx context.Context, query string) (map[string][]string, error) {
	infos, err := s.listExecutionInfos(ctx, query)
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, e := range infos {
		id := e.GetExecution().GetWorkflowId()
		out[id] = append(out[id], e.GetExecution().GetRunId())
	}
	return out, nil
}

// listExecutionInfos is every execution query matches, a page at a time.
func (s *Server) listExecutionInfos(ctx context.Context, query string) ([]*workflowpb.WorkflowExecutionInfo, error) {
	var out []*workflowpb.WorkflowExecutionInfo
	req := &workflowservice.ListWorkflowExecutionsRequest{Query: query, PageSize: 1000}
	for {
		resp, err := s.Temporal.ListWorkflow(ctx, req)
		if err != nil {
			return nil, err
		}
		out = append(out, resp.GetExecutions()...)
		if len(resp.GetNextPageToken()) == 0 {
			return out, nil
		}
		req.NextPageToken = resp.GetNextPageToken()
	}
}

// verify's bounds on what anyone with Temporal's API can add to: the anchor workflows it lists and
// reads, and the events it reads of each (agentd pushes at most 2880 heads a day). Past them, the
// report says what it did not read (Unread), and agentctl does not exit 0 on it.
var (
	maxAnchorWorkflows = 2000
	maxAnchorEvents    = 20000
)

// listExecutionInfosUpTo is listExecutionInfos of at most limit executions, and how many more it
// saw past them: at least that many were not listed.
func (s *Server) listExecutionInfosUpTo(ctx context.Context, query string, limit int) ([]*workflowpb.WorkflowExecutionInfo, int, error) {
	var out []*workflowpb.WorkflowExecutionInfo
	req := &workflowservice.ListWorkflowExecutionsRequest{Query: query, PageSize: int32(min(limit, 1000))}
	for {
		resp, err := s.Temporal.ListWorkflow(ctx, req)
		if err != nil {
			return nil, 0, err
		}
		page := resp.GetExecutions()
		if room := limit - len(out); len(page) > room {
			return append(out, page[:room]...), len(page) - room, nil
		}
		out = append(out, page...)
		switch {
		case len(resp.GetNextPageToken()) == 0:
			return out, 0, nil
		case len(out) == limit:
			return out, 1, nil // a page more, of at least one
		}
		req.NextPageToken = resp.GetNextPageToken()
	}
}

// historyUpTo is history, of at most limit events; cut says there were more.
func (s *Server) historyUpTo(ctx context.Context, workflowID, runID string, limit int) ([]*historypb.HistoryEvent, bool, error) {
	it := s.Temporal.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var events []*historypb.HistoryEvent
	for it.HasNext() {
		if len(events) == limit {
			return events, true, nil
		}
		e, err := it.Next()
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil, false, errNoHistory
		}
		if err != nil {
			return nil, false, err
		}
		events = append(events, e)
	}
	return events, false, nil
}

// errNoHistory: Temporal has no history of the execution.
var errNoHistory = errors.New("Temporal has no history of it")

// history is an execution's history in Temporal.
func (s *Server) history(ctx context.Context, workflowID, runID string) ([]*historypb.HistoryEvent, error) {
	it := s.Temporal.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var events []*historypb.HistoryEvent
	for it.HasNext() {
		e, err := it.Next()
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil, errNoHistory
		}
		if err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, nil
}

// externalAnchor is a head of the chain agentd pushed to the anchor workflow of workflowID.
type externalAnchor struct {
	workflowID string
	head       audit.Head
}

// foreignAnchors is what the namespace holds of other chains' anchor workflows.
type foreignAnchors struct {
	chains   []string // their keys, each once
	accepted int64    // the workflows of the chains allowed
	refused  bool     // some chain's that is not allowed
}

// otherChain is what another chain's anchor workflows hold: who pushed its heads, and its last.
type otherChain struct {
	ids    []string        // its workflows
	agentd map[string]bool // the agentds its heads name; "" for heads that name none
	last   *audit.Pushed   // its last head, by Temporal's time
}

// externalAnchors reads the heads agentd pushed to the chain's anchor workflows that Temporal still
// has. It lists every chain's, by their IDs: an anchor workflow is the chain's by its ID, whatever
// its type, and one that is not agentd's (another type or task queue) is a mismatch, as is a signal
// in one that agentd did not send. A head pushed after a higher one is a mismatch too: agentd
// pushes in order, so the ledger went backwards between the two. Another chain's workflows are a
// mismatch unless allowed: a chain rewritten from its first record has a new key, and the old
// one's workflows are what shows it. With no chain (the audit log has no records) every one is
// another chain's.
//
// Who pushed another chain's heads tells which (audit.AnchorHead). Another agentd's chain is
// accepted with --allow-chain, which its mismatch suggests. One whose heads this agentd pushed
// (AnchorIdentity) is of a ledger this one replaced: a reset or a rewrite, which only the operator
// can tell apart, and Temporal's times do not (both show an old chain that stopped and a new one
// that began). Its mismatch states those times, and suggests nothing; --allow-replaced-ledger
// accepts it, unless it pushed after this chain's first push, by Temporal's times: two ledgers of
// one agentd live at once, which no reset makes. Heads that name no agentd were pushed before agentd
// named itself in them: either flag accepts them, and their mismatch suggests neither.
func (s *Server) externalAnchors(ctx context.Context, chain string, allowChain, allowReplaced map[string]bool) ([]externalAnchor,
	[]*agentosv1.AnchorMismatch, foreignAnchors, []*agentosv1.Unread, error) {
	var foreign foreignAnchors
	var unread []*agentosv1.Unread
	listed, more, err := s.listExecutionInfosUpTo(ctx, fmt.Sprintf("WorkflowId STARTS_WITH '%s'", quote(audit.AnchorIDPrefix)),
		maxAnchorWorkflows)
	if err != nil {
		return nil, nil, foreign, nil, err
	}
	if more > 0 {
		unread = append(unread, &agentosv1.Unread{What: "anchor workflows", Count: int64(more), Why: fmt.Sprintf(
			"verify reads at most %d anchor workflows, and the namespace holds more: at least %d not read", maxAnchorWorkflows, more)})
	}
	mine := func(workflowID string) bool {
		return chain != "" && strings.HasPrefix(workflowID, audit.AnchorPrefix(chain))
	}
	others := map[string]*otherChain{} // chain key -> its workflows
	var pushed []audit.Pushed          // this chain's heads
	var bad []*agentosv1.AnchorMismatch
	mismatch := func(workflowID string, seq int64, why string) {
		bad = append(bad, &agentosv1.AnchorMismatch{RunId: safetext.Line(workflowID), Seq: seq, Why: safetext.Line(why)})
	}
	for _, e := range listed {
		workflowID, runID := e.GetExecution().GetWorkflowId(), e.GetExecution().GetRunId()
		if !mine(workflowID) {
			key := audit.AnchorChainOf(workflowID)
			o := others[key]
			if o == nil {
				o = &otherChain{agentd: map[string]bool{}}
				others[key] = o
			}
			o.ids = append(o.ids, workflowID)
			events, cut, err := s.historyUpTo(ctx, workflowID, runID, maxAnchorEvents)
			if errors.Is(err, errNoHistory) {
				continue
			} else if err != nil {
				return nil, nil, foreign, nil, err
			}
			if cut {
				unread = append(unread, eventsUnread(workflowID))
			}
			for _, e := range events {
				attrs := e.GetWorkflowExecutionSignaledEventAttributes()
				var h audit.AnchorHead
				if e.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED || attrs.GetSignalName() != audit.AnchorSignal ||
					converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &h) != nil || !h.Valid() {
					continue // another chain's head that cannot be read says nothing of whose it is
				}
				o.agentd[h.Agentd] = true
				if at := audit.EventTime(e); o.last == nil || at.After(o.last.At) {
					o.last = &audit.Pushed{AnchorHead: h, WorkflowID: workflowID, EventID: e.GetEventId(), At: at}
				}
			}
			continue
		}
		if typ, queue := e.GetType().GetName(), e.GetTaskQueue(); typ != audit.AnchorWorkflowType || queue != audit.AnchorTaskQueue {
			mismatch(workflowID, 0, fmt.Sprintf("execution %s is a %q on task queue %q, not agentd's anchor workflow: "+
				"what it holds is not the chain's heads", runID, typ, queue))
			continue
		}
		events, cut, err := s.historyUpTo(ctx, workflowID, runID, maxAnchorEvents)
		if errors.Is(err, errNoHistory) {
			continue // removed between the list and the read: past its retention
		}
		if err != nil {
			return nil, nil, foreign, nil, err
		}
		if cut {
			unread = append(unread, eventsUnread(workflowID))
		}
		for _, e := range events {
			if e.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				continue
			}
			attrs := e.GetWorkflowExecutionSignaledEventAttributes()
			var h audit.AnchorHead
			switch err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &h); {
			case attrs.GetSignalName() != audit.AnchorSignal:
				mismatch(workflowID, 0, fmt.Sprintf("event %d of execution %s is a signal %q, which agentd does not send",
					e.GetEventId(), runID, attrs.GetSignalName()))
			case err != nil || !h.Valid(): // nothing of it copied
				mismatch(workflowID, 0, fmt.Sprintf("the head signalled at event %d of execution %s cannot be read: %s",
					e.GetEventId(), runID, cmp.Or(errText(err), "it is not a head agentd pushes")))
			default:
				pushed = append(pushed, audit.Pushed{AnchorHead: h, WorkflowID: workflowID, EventID: e.GetEventId(),
					At: audit.EventTime(e)})
			}
		}
	}
	out := make([]externalAnchor, len(pushed))
	var first *audit.Pushed // this chain's first push, by Temporal's time
	for i, p := range pushed {
		out[i] = externalAnchor{workflowID: p.WorkflowID, head: p.Head()}
		if first == nil || p.At.Before(first.At) {
			first = &pushed[i]
		}
	}
	for _, f := range audit.OutOfOrder(pushed) {
		mismatch(f.WorkflowID, f.Seq, f.Why)
	}
	this := chain
	if this == "" {
		this = "(none: the audit log has no records)"
	}
	firstPush := fmt.Sprintf("this ledger's chain %q has pushed no head yet", this)
	if first != nil {
		firstPush = fmt.Sprintf("this ledger's chain %q first pushed record %d at %s", this, first.Seq, fmtTime(first.At))
	}
	for _, key := range slices.Sorted(maps.Keys(others)) {
		foreign.chains = append(foreign.chains, key)
		o := others[key]
		holds := fmt.Sprintf("the namespace holds %d anchor workflows of chain %q (%q), not of this ledger's chain %q",
			len(o.ids), key, slices.Max(o.ids), this)
		var pushers []string // other agentds
		for id := range o.agentd {
			if id != "" && id != s.AnchorIdentity {
				pushers = append(pushers, id)
			}
		}
		slices.Sort(pushers)
		own := s.AnchorIdentity != "" && o.agentd[s.AnchorIdentity]
		who := "an agentd that names itself in none of them" // pushed before agentd named itself in its heads
		switch {
		case own:
			who = "this agentd"
		case len(pushers) > 0:
			who = "another agentd (" + strings.Join(pushers, ", ") + ")"
		}
		last := "its workflows hold no head agentd pushes"
		if o.last != nil {
			last = fmt.Sprintf("%s pushed its heads, the last record %d (sha256 %s) at %s", who, o.last.Seq, o.last.Sha256,
				fmtTime(o.last.At))
		}
		// Live beside this one: it pushed after this chain's first push, by Temporal's times. An
		// agentd beside this one is that; a ledger replaced (a reset, a rewrite) is not, whatever
		// identity its heads name, which comes from configuration, and a respelled one changes it.
		live := first != nil && !first.At.IsZero() && o.last != nil && o.last.At.After(first.At)
		switch {
		case own && live:
			mismatch(audit.AnchorPrefix(key)+"*", 0, fmt.Sprintf("%s: %s, after %s: two ledgers of this agentd were live at "+
				"once, which neither --allow-chain nor --allow-replaced-ledger accepts", holds, last, firstPush))
		case live && allowChain[key], !live && allowReplaced[key]:
			foreign.accepted += int64(len(o.ids)) // an agentd beside this one; a ledger the operator says they replaced
			continue
		case live && len(pushers) > 0:
			mismatch(audit.AnchorPrefix(key)+"*", 0, fmt.Sprintf("%s: %s, after %s: an agentd beside this one, whose chain "+
				"is accepted with --allow-chain %s", holds, last, firstPush, key))
		case live:
			mismatch(audit.AnchorPrefix(key)+"*", 0, fmt.Sprintf("%s: %s, after %s", holds, last, firstPush))
		default:
			why := fmt.Sprintf("%s: %s; %s. A ledger rewritten from its first record, or put in place of the one before, "+
				"has a chain with a new key, and the old chain's workflows stay", holds, last, firstPush)
			if allowChain[key] {
				why += "; --allow-chain accepts only a chain that pushed after this chain's first push"
			}
			mismatch(audit.AnchorPrefix(key)+"*", 0, why)
		}
		foreign.refused = true
	}
	return out, bad, foreign, unread, nil
}

// eventsUnread is the events of an anchor workflow verify did not read, past maxAnchorEvents.
func eventsUnread(workflowID string) *agentosv1.Unread {
	return &agentosv1.Unread{What: fmt.Sprintf("events of anchor workflow %q", workflowID), Count: 1, Why: fmt.Sprintf(
		"verify reads at most %d events of an anchor workflow, and it holds more: at least one not read", maxAnchorEvents)}
}

// errText is err's text, bounded; "" for nil.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return audit.Text(err.Error())
}

// fmtTime is Temporal's time t, as RFC 3339 in UTC.
func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "a time Temporal did not give"
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// attemptOf names one attempt of a call: its run, its activity, and which attempt.
type attemptOf struct {
	workflowID, activityID string
	attempt                int64
}

// started is a call.started as attempts reads the chain: closed by its call record, or cut short by
// an agentd.started after it.
type started struct {
	attemptOf
	seq, cutBy int64
	closed     bool
}

// Attempts is what the gateway knows of its attempts that the ledger does not say: those executing
// in this agentd now, and those whose call record a fault lost (gateway.Executing, Faulted).
type Attempts interface {
	Executing(workflowID, activityID string, attempt int64) bool
	Faulted(workflowID, activityID string, attempt int64) (why string, ok bool)
}

// attempts checks the records of the calls' attempts (design doc, section 7: no entry, no call).
// Every call.started is closed by its attempt's call record, which names it, or by an agentd.started
// after it: that attempt was cut short, as by a crash, which is reported, not a mismatch. One with
// neither is in flight while its run runs, whichever attempt it is (Temporal may have begun the next
// while agentd still works on it), and while the attempt is executing in this agentd, whatever its
// run's state (a run cancelled ends at once, its call still going); one whose record a fault lost
// is reported apart (unrecorded); any other is a mismatch once its run has ended. A call record
// that names a call.started must find that attempt's, before it; one of an attempt the policy let
// past, with a result, must name one.
//
// The records are a snapshot: an attempt that ended after it has its record now, and is asked of
// so (closedNow), after the gateway, which forgets an attempt only once its record is in.
func attempts(ctx context.Context, q *db.Queries, running map[string]bool, gw Attempts,
	closedNow func(workflowID string, started int64) (bool, error)) ([]*agentosv1.StateMismatch,
	[]*agentosv1.CutShortAttempt, []*agentosv1.UnrecordedAttempt, error) {
	var bad []*agentosv1.StateMismatch
	mismatch := func(workflowID, why string) {
		bad = append(bad, &agentosv1.StateMismatch{Kind: "call", Id: safetext.Line(workflowID), Why: safetext.Line(why)})
	}
	starts := map[int64]*started{}
	var open []*started // in the order appended
	for seq := int64(1); ; {
		rows, err := q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: seq, Limit: page})
		if err != nil {
			return nil, nil, nil, err
		}
		for _, r := range rows {
			seq = r.Seq + 1
			var p struct {
				ActivityID string `json:"activity_id"`
				Attempt    int64  `json:"attempt"`
				Status     string `json:"status"`
				StartedSeq int64  `json:"started_seq"`
				Policy     *struct {
					Decision string `json:"decision"`
				} `json:"policy"`
			}
			switch r.Type {
			case "agentd.started":
				for _, st := range open {
					st.cutBy = r.Seq
				}
				open = nil
				continue
			case "call.started", "call":
			default:
				continue
			}
			if err := json.Unmarshal([]byte(r.Payload), &p); err != nil {
				mismatch(r.WorkflowID, fmt.Sprintf("record %d, a %s, cannot be read: %v", r.Seq, r.Type, err))
				continue
			}
			at := attemptOf{r.WorkflowID, p.ActivityID, p.Attempt}
			if r.Type == "call.started" {
				st := &started{attemptOf: at, seq: r.Seq}
				starts[r.Seq], open = st, append(open, st)
				continue
			}
			passed := p.Policy != nil && (p.Policy.Decision == "allow" || p.Policy.Decision == "require_approval")
			st := starts[p.StartedSeq]
			switch {
			case p.StartedSeq == 0 && passed && p.Status != "error":
				mismatch(r.WorkflowID, fmt.Sprintf("record %d, the call of attempt %d of %q, which the policy let past, names no "+
					"call.started: agentd records an attempt before anything of it leaves", r.Seq, p.Attempt, p.ActivityID))
			case p.StartedSeq == 0:
			case st == nil || st.attemptOf != at:
				mismatch(r.WorkflowID, fmt.Sprintf("record %d, the call of attempt %d of %q, names record %d, which is not that "+
					"attempt's call.started before it", r.Seq, p.Attempt, p.ActivityID, p.StartedSeq))
			case st.closed || st.cutBy != 0:
				mismatch(r.WorkflowID, fmt.Sprintf("record %d, the call of attempt %d of %q, names call.started %d, which another "+
					"call record or an agentd.started closed before it", r.Seq, p.Attempt, p.ActivityID, st.seq))
			default:
				st.closed = true
				open = slices.DeleteFunc(open, func(o *started) bool { return o == st })
			}
		}
		if len(rows) < page {
			break
		}
	}
	var cut []*agentosv1.CutShortAttempt
	var unrecorded []*agentosv1.UnrecordedAttempt
	for _, seq := range slices.Sorted(maps.Keys(starts)) {
		st := starts[seq]
		switch {
		case st.closed:
			continue
		case st.cutBy != 0:
			cut = append(cut, &agentosv1.CutShortAttempt{RunId: safetext.Line(st.workflowID), ActivityId: safetext.Line(st.activityID),
				Attempt: st.attempt, Seq: st.seq, RestartedSeq: st.cutBy})
			continue
		case running[st.workflowID]:
			continue // in flight: its call record is yet to come
		case gw != nil && gw.Executing(st.workflowID, st.activityID, st.attempt):
			continue // in flight in this agentd, its run ended meanwhile
		}
		if closed, err := closedNow(st.workflowID, st.seq); err != nil {
			return nil, nil, nil, err
		} else if closed {
			continue // it ended after the snapshot: in flight then
		}
		if gw != nil {
			if why, ok := gw.Faulted(st.workflowID, st.activityID, st.attempt); ok {
				unrecorded = append(unrecorded, &agentosv1.UnrecordedAttempt{RunId: safetext.Line(st.workflowID),
					ActivityId: safetext.Line(st.activityID), Attempt: st.attempt, Seq: st.seq, Why: safetext.Line(audit.Text(why))})
				continue
			}
		}
		mismatch(st.workflowID, fmt.Sprintf("call.started %d, of attempt %d of %q, has no call record and no agentd.started "+
			"after it, though its run is not running", st.seq, st.attempt, st.activityID))
	}
	return bad, cut, unrecorded, nil
}

// closedNow reports whether the run's records hold a call record that names call.started started,
// as the ledger is now: the attempt ended, though maybe after a snapshot that had it open.
func (s *Server) closedNow(ctx context.Context) func(workflowID string, started int64) (bool, error) {
	return func(workflowID string, started int64) (bool, error) {
		var records []db.AuditLog
		if err := s.Ledger.Read(ctx, func(q *db.Queries) error {
			var err error
			records, err = q.ListRunAuditRecords(ctx, workflowID)
			return err
		}); err != nil {
			return false, err
		}
		for _, r := range records {
			var p struct {
				StartedSeq int64 `json:"started_seq"`
			}
			if r.Type == "call" && r.Seq > started && json.Unmarshal([]byte(r.Payload), &p) == nil && p.StartedSeq == started {
				return true, nil
			}
		}
		return false, nil
	}
}

// coveredTables are the ledger's tables VerifyAudit compares with the records (audit.Projection,
// the blobs' hashes, the chain itself); uncheckedTables those it does not, and why. A table that is
// in neither is reported unchecked too, as one verify does not know of.
var (
	coveredTables = []string{"approvals", "audit_log", "blob_owners", "blobs", "budget_entries", "calls", "effect_resolutions",
		"effects", "model_calls", "runs"}
	uncheckedTables = map[string]string{
		"outbox": "it holds the runs' doorbells only: one changed, removed or added changes when a run wakes to ask again, " +
			"not what it does or did",
		"goose_db_version": "the schema's version, which agentd checks when it opens the ledger",
		"migration_files":  "the migrations applied to the ledger, which agentd checks when it opens it",
	}
)

// tablesUnchecked are the ledger's tables VerifyAudit does not compare with the records.
func (s *Server) tablesUnchecked(ctx context.Context) ([]*agentosv1.UncheckedTable, error) {
	rows, err := s.Ledger.DB.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*agentosv1.UncheckedTable
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		switch why, ok := uncheckedTables[name]; {
		case slices.Contains(coveredTables, name):
		case ok:
			out = append(out, &agentosv1.UncheckedTable{Name: name, Why: why})
		default:
			out = append(out, &agentosv1.UncheckedTable{Name: safetext.Line(name), Why: "a table verify does not know of"})
		}
	}
	return out, rows.Err()
}

// page is how many records a verification reads at a time.
const page = 1000

// uncovered is the mismatch of the first record after the chain's last pushed head (last) that is
// older than old, and younger than the retention: a head pushed after it would cover it, and agentd
// pushes one within old of it. Nil when there is none.
func uncovered(ctx context.Context, q *db.Queries, chain string, last int64, now time.Time, old, retention time.Duration) (*agentosv1.AnchorMismatch, error) {
	for seq := last + 1; ; {
		rows, err := q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: seq, Limit: page})
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			at, err := time.Parse(time.RFC3339Nano, r.At)
			if age := now.Sub(at); err == nil && age > old && age < retention {
				return &agentosv1.AnchorMismatch{RunId: audit.AnchorPrefix(chain) + "*", Seq: r.Seq, Why: safetext.Line(fmt.Sprintf(
					"record %d, appended at %s, and the records after it: no head pushed to Temporal covers them, though agentd "+
						"pushes one within %s; the anchor workflow that held one is gone, or not agentd's", r.Seq, r.At, old))}, nil
			}
			seq = r.Seq + 1
		}
		if len(rows) < page {
			return nil, nil
		}
	}
}

// blobMismatches are the blobs whose bytes do not hash to their key, and the blobs effects and
// approvals refer to that the ledger does not have.
func blobMismatches(ctx context.Context, q *db.Queries) ([]*agentosv1.StateMismatch, error) {
	var out []*agentosv1.StateMismatch
	for from := ""; ; {
		blobs, err := q.ListBlobsAfter(ctx, db.ListBlobsAfterParams{Sha256: from, Limit: 100})
		if err != nil {
			return nil, err
		}
		for _, b := range blobs {
			sum := sha256.Sum256(b.Bytes)
			if got := hex.EncodeToString(sum[:]); got != b.Sha256 {
				out = append(out, &agentosv1.StateMismatch{Kind: "blob", Id: safetext.Line(b.Sha256),
					Why: safetext.Line(fmt.Sprintf("blob %s holds bytes with sha256 %s", b.Sha256, got))})
			}
		}
		if len(blobs) < 100 {
			break
		}
		from = blobs[len(blobs)-1].Sha256
	}
	missing, err := q.ListMissingBlobs(ctx)
	if err != nil {
		return nil, err
	}
	for _, m := range missing {
		out = append(out, &agentosv1.StateMismatch{Kind: m.Kind, Id: safetext.Line(m.ID),
			Why: safetext.Line(fmt.Sprintf("%s %s refers to blob %s, which the ledger does not have", m.Kind, m.ID, m.Ref))})
	}
	return out, nil
}

// anchor is what one gateway call's result in a history says of the audit log.
type anchor struct {
	activityID string
	attempt    int32       // the attempt that completed it (its ActivityTaskStarted)
	status     string      // the result's
	resultSHA  string      // of the result's value without _audit (audit.ResultSHA256)
	head       *audit.Head // the record the result names (_audit); nil when it names none
	unreadable string      // why the result could not be read
}

// anchorsOf reads the anchor of each completed gateway call in a history. The Worker schedules the
// calls and names them; agentd's gateway alone completes them.
func anchorsOf(events []*historypb.HistoryEvent) []anchor {
	scheduled := map[int64]string{} // event ID -> activity ID
	attempts := map[int64]int32{}   // event ID of an ActivityTaskStarted -> its attempt
	var out []anchor
	for _, e := range events {
		switch e.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			scheduled[e.GetEventId()] = e.GetActivityTaskScheduledEventAttributes().GetActivityId()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			attempts[e.GetEventId()] = e.GetActivityTaskStartedEventAttributes().GetAttempt()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			attrs := e.GetActivityTaskCompletedEventAttributes()
			a := anchor{activityID: scheduled[attrs.GetScheduledEventId()], attempt: attempts[attrs.GetStartedEventId()]}
			var res agentosv1.Result
			var v struct {
				Audit *audit.Head `json:"_audit"`
			}
			if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetResult(), &res); err != nil {
				a.unreadable = "its result is not a gateway result: " + err.Error()
			} else if err := json.Unmarshal([]byte(res.GetValueJson()), &v); err != nil {
				a.unreadable = "its result's value cannot be read: " + err.Error()
			} else if a.resultSHA, err = audit.ResultSHA256(res.GetValueJson()); err != nil {
				a.unreadable = "its result's value cannot be read: " + err.Error()
			}
			a.status, a.head = res.GetStatus(), v.Audit
			out = append(out, a)
		}
	}
	return out
}

// checkAnchors checks an execution's anchors against the run's records in the ledger. Each result
// names one record, by seq and sha256: the record of its own call, of this run, with its activity ID,
// the attempt that completed it, its status (a result's, never "error") and the sha256 of its value;
// and no other result of the execution names the same record. It returns how many it checked, and
// those that do not match. A result that names no record is one agentd did not return, unless the
// execution started before agentd kept the audit log (began: when its first record was appended).
func checkAnchors(workflowID string, anchors []anchor, records []db.AuditLog, began, started time.Time) (int64, []*agentosv1.AnchorMismatch) {
	bySeq := make(map[int64]db.AuditLog, len(records))
	for _, rec := range records {
		bySeq[rec.Seq] = rec
	}
	named := map[int64]string{} // seq -> the activity whose result named it first
	var checked int64
	var bad []*agentosv1.AnchorMismatch
	for _, a := range anchors {
		mismatch := func(seq int64, why string) {
			bad = append(bad, &agentosv1.AnchorMismatch{RunId: safetext.Line(workflowID), ActivityId: safetext.Line(a.activityID),
				Seq: seq, Why: safetext.Line(why)})
		}
		switch {
		case a.unreadable != "":
			mismatch(0, a.unreadable)
			continue
		case a.head == nil && !began.IsZero() && started.Before(began):
			continue // from before agentd kept the audit log: the run is reported so (RunsBeforeLog)
		case a.head == nil:
			mismatch(0, "its result names no record (_audit): agentd returns none without the call's record")
			continue
		}
		checked++
		seq := a.head.Seq
		rec, ok := bySeq[seq]
		var call struct {
			ActivityID   string `json:"activity_id"`
			Attempt      int32  `json:"attempt"`
			Status       string `json:"status"`
			ResultSHA256 string `json:"result_sha256"`
		}
		decoded := ok && json.Unmarshal([]byte(rec.Payload), &call) == nil
		first, again := named[seq]
		if !again {
			named[seq] = a.activityID
		}
		switch {
		case !ok:
			mismatch(seq, fmt.Sprintf("the audit log has no record of this run at seq %d", seq))
		case rec.Sha256 != a.head.Sha256:
			mismatch(seq, fmt.Sprintf("the record at seq %d has sha256 %s; the result names %s", seq, rec.Sha256, a.head.Sha256))
		case rec.Type != "call" || !decoded:
			mismatch(seq, fmt.Sprintf("the record at seq %d is a %s, not a call's", seq, rec.Type))
		case call.ActivityID != audit.Text(a.activityID):
			mismatch(seq, fmt.Sprintf("the record at seq %d is the call %q's", seq, call.ActivityID))
		case again:
			mismatch(seq, fmt.Sprintf("the record at seq %d is named by the result of %q too", seq, first))
		case call.Status == "error" || call.Status != a.status:
			mismatch(seq, fmt.Sprintf("the record at seq %d says the call ended %q; the result is %q", seq, call.Status, a.status))
		case call.Attempt != a.attempt:
			mismatch(seq, fmt.Sprintf("the record at seq %d is attempt %d's; attempt %d completed the call", seq, call.Attempt, a.attempt))
		case call.ResultSHA256 != a.resultSHA:
			mismatch(seq, fmt.Sprintf("the record at seq %d holds a result with sha256 %s; the result in the history has %s",
				seq, call.ResultSHA256, a.resultSHA))
		}
	}
	return checked, bad
}
