package control

import (
	"bufio"
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// ExportAudit sends the audit log and the runs' histories, so that a reader checks them without
// agentd (design doc, section 4: the export merges Temporal's history with agentd's ledger). First
// each run's executions' histories, as agentd reads them from Temporal, with the anchors in them:
// which event carries which record, and the bytes the record's result_sha256 is of. Then, for the
// whole chain, the heads agentd pushed to the chain's anchor workflows. Then the chain, as one
// snapshot taken after the histories and the heads, so that it holds every record they name: its
// head, and every record up to it with all the fields its sha256 covers. The snapshot is written to a
// file in one short read transaction, then sent from it: a reader slow to take the export holds no
// transaction open. The runs are VerifyAudit's: those on the Worker's task queue in Temporal, those
// the audit log records, and the runs table's. With a run, only its histories and its records: their
// sha256s can be checked, not the links of the chain, which go through other runs' records.
func (s *Server) ExportAudit(req *agentosv1.ExportAuditRequest, stream agentosv1.Control_ExportAuditServer) error {
	ctx := stream.Context()
	listed, err := s.listExecutions(ctx, fmt.Sprintf("TaskQueue = '%s'", quote(s.TaskQueue)))
	if err != nil {
		return status.Errorf(codes.Unavailable, "list the runs Temporal has: %v", err)
	}
	var runs map[string]*runCheck
	var first []db.AuditLog
	if err := s.Ledger.Read(ctx, func(q *db.Queries) error {
		rows, err := q.ListAllRuns(ctx)
		if err != nil {
			return err
		}
		lives, err := q.ListRunLifeRecords(ctx)
		if err != nil {
			return err
		}
		lasts, err := q.ListRunLastRecords(ctx)
		if err != nil {
			return err
		}
		runs = runChecks(listed, rows, lives, lasts)
		first, err = q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: 1, Limit: 1})
		return err
	}); err != nil {
		return err
	}
	id := req.GetRunId()
	if id != "" {
		c, ok := runs[id]
		if !ok {
			return status.Errorf(codes.NotFound, "neither Temporal, the audit log nor the ledger has a run %q", id)
		}
		runs = map[string]*runCheck{id: c}
	}
	for _, workflowID := range slices.Sorted(maps.Keys(runs)) {
		for _, runID := range runs[workflowID].executions() {
			h, err := s.exportHistory(ctx, workflowID, runID)
			if err != nil {
				return err
			}
			if err := stream.Send(&agentosv1.AuditExportPart{Part: &agentosv1.AuditExportPart_History{History: h}}); err != nil {
				return err
			}
		}
	}
	var headsCut string // why the export may not hold every head this chain pushed, as verify says it
	if id == "" && len(first) == 1 {
		chain := audit.AnchorChain(first[0].Sha256)
		heads, bad, _, unread, err := s.externalAnchors(ctx, chain, nil, nil)
		if err != nil {
			return status.Errorf(codes.Unavailable, "the heads pushed to Temporal: %v", err)
		}
		headsCut = pushedCut(chain, bad, unread)
		for _, h := range heads {
			if err := stream.Send(&agentosv1.AuditExportPart{Part: &agentosv1.AuditExportPart_PushedHead{PushedHead: &agentosv1.PushedHead{
				AnchorWorkflowId: h.workflowID, Seq: h.head.Seq, Sha256: h.head.Sha256}}}); err != nil {
				return err
			}
		}
	}
	snapshot, err := os.CreateTemp(cmp.Or(s.SnapshotDir, os.TempDir()), snapshotPrefix+"*")
	if err != nil {
		return err
	}
	defer os.Remove(snapshot.Name())
	defer snapshot.Close()
	w := bufio.NewWriter(snapshot)
	var c chainExport
	if err := s.Ledger.Read(ctx, func(q *db.Queries) error {
		var err error
		c, err = exportChain(ctx, q, id, w, cmp.Or(s.MaxSnapshot, maxSnapshot))
		return err
	}); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if _, err := snapshot.Seek(0, io.SeekStart); err != nil {
		return err
	}
	cut := c.cut
	if headsCut != "" {
		cut = strings.TrimPrefix(cut+"; "+headsCut, "; ")
	}
	if err := stream.Send(&agentosv1.AuditExportPart{Part: &agentosv1.AuditExportPart_Head{Head: &agentosv1.AuditExportHead{
		Seq: c.head.Seq, Sha256: c.head.Sha256, Records: c.records, ExportedAt: ledger.Now(), RunId: id, Cut: cut}}}); err != nil {
		return err
	}
	r := bufio.NewReader(snapshot)
	for {
		part := &agentosv1.AuditExportPart{}
		switch err := protodelim.UnmarshalFrom(r, part); {
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return err
		}
		if err := stream.Send(part); err != nil {
			return err
		}
	}
}

// pushedCut says what of the heads chain pushed the export may not hold, as verify reads the anchor
// workflows (externalAnchors): those past its bounds (more anchor workflows than it reads, more events
// in one of this chain's than it reads), and this chain's anchor workflows it does not take heads from
// (not agentd's anchor workflow, a signal that is not a head, a head out of order). Other chains'
// workflows say nothing of this chain's heads. At most maxPushedCut of them, and how many more.
func pushedCut(chain string, bad []*agentosv1.AnchorMismatch, unread []*agentosv1.Unread) string {
	mine := audit.AnchorPrefix(chain)
	var why []string
	for _, u := range unread {
		if u.GetWhat() == "anchor workflows" || strings.HasPrefix(u.GetWhat(), `events of anchor workflow "`+mine) {
			why = append(why, u.GetWhat()+": "+u.GetWhy())
		}
	}
	for _, m := range bad {
		if strings.HasPrefix(m.GetRunId(), mine) {
			why = append(why, fmt.Sprintf("anchor workflow %q: %s", m.GetRunId(), m.GetWhy()))
		}
	}
	if len(why) == 0 {
		return ""
	}
	more := ""
	if len(why) > maxPushedCut {
		more = fmt.Sprintf("; and %d more", len(why)-maxPushedCut)
		why = why[:maxPushedCut]
	}
	return "the heads pushed to Temporal may not all be in the export, as verify reads them: " + strings.Join(why, "; ") + more
}

// maxPushedCut bounds what an export's head says of the pushed heads it may not hold.
const maxPushedCut = 5

// exportHistory is one execution's history in Temporal, and the anchors in it.
func (s *Server) exportHistory(ctx context.Context, workflowID, runID string) (*agentosv1.ExecutionHistory, error) {
	h := &agentosv1.ExecutionHistory{RunId: workflowID, TemporalRunId: runID}
	events, err := s.history(ctx, workflowID, runID)
	switch {
	case errors.Is(err, errNoHistory):
		h.Unavailable = "Temporal no longer has the execution's history: past the namespace's retention, or a dev server " +
			"that lost its state"
		return h, nil
	case err != nil:
		return nil, status.Errorf(codes.Unavailable, "the history of run %s, execution %s: %v", workflowID, runID, err)
	}
	b, err := protojson.Marshal(&historypb.History{Events: events})
	if err != nil {
		return nil, err
	}
	h.HistoryJson, h.Anchors = string(b), exportAnchors(events)
	return h, nil
}

// exportAnchors are the anchors in a history: each completed gateway call's result whose value
// names a record (_audit), with the event that carries it.
func exportAnchors(events []*historypb.HistoryEvent) []*agentosv1.ExportedAnchor {
	scheduled := map[int64]string{} // event ID -> activity ID
	attempts := map[int64]int32{}   // event ID of an ActivityTaskStarted -> its attempt
	var out []*agentosv1.ExportedAnchor
	for _, e := range events {
		switch e.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			scheduled[e.GetEventId()] = e.GetActivityTaskScheduledEventAttributes().GetActivityId()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			attempts[e.GetEventId()] = e.GetActivityTaskStartedEventAttributes().GetAttempt()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			attrs := e.GetActivityTaskCompletedEventAttributes()
			var res agentosv1.Result
			var v struct {
				Audit *audit.Head `json:"_audit"`
			}
			if converter.GetDefaultDataConverter().FromPayloads(attrs.GetResult(), &res) != nil ||
				json.Unmarshal([]byte(res.GetValueJson()), &v) != nil || v.Audit == nil {
				continue
			}
			hashed, _ := audit.ResultJSON(res.GetValueJson())
			out = append(out, &agentosv1.ExportedAnchor{EventId: e.GetEventId(), ActivityId: scheduled[attrs.GetScheduledEventId()],
				Attempt: attempts[attrs.GetStartedEventId()], Seq: v.Audit.Seq, Sha256: v.Audit.Sha256, ResultJson: hashed})
		}
	}
	return out
}

// chainExport is what exportChain wrote of the chain: its head, the records it holds, and, when it
// stopped short of the head at the bound, why.
type chainExport struct {
	head    audit.Head
	records int64
	cut     string
}

// exportChain writes the chain's records up to its head to w, a page at a time, through q, one
// snapshot, as parts of the export, each delimited (protodelim): run's records only, when run is not
// empty. It stops before the record that would take what it wrote past max bytes.
func exportChain(ctx context.Context, q *db.Queries, run string, w io.Writer, max int64) (chainExport, error) {
	c := chainExport{head: audit.Head{Sha256: audit.Genesis}}
	switch last, err := q.LastAuditRecord(ctx); {
	case err == nil:
		c.head = audit.Head{Seq: last.Seq, Sha256: last.Sha256}
	case !errors.Is(err, sql.ErrNoRows):
		return c, err
	}
	var written int64
	for seq := int64(1); seq <= c.head.Seq; {
		var rows []db.AuditLog
		var err error
		if run == "" {
			rows, err = q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: seq, Limit: page})
		} else {
			rows, err = q.ListRunAuditRecordsPage(ctx, db.ListRunAuditRecordsPageParams{WorkflowID: run, Seq: seq,
				Seq_2: c.head.Seq, Limit: page})
		}
		if err != nil {
			return c, err
		}
		for _, r := range rows {
			if r.Seq > c.head.Seq {
				return c, nil
			}
			rec := &agentosv1.AuditRecord{Seq: r.Seq, At: r.At, WorkflowId: r.WorkflowID, Type: r.Type, Actor: r.Actor,
				Payload: r.Payload, PrevSha256: r.PrevSha256, Sha256: r.Sha256}
			if r.PeerUid.Valid {
				rec.PeerUid = &r.PeerUid.Int64
			}
			part := &agentosv1.AuditExportPart{Part: &agentosv1.AuditExportPart_Record{Record: rec}}
			size := int64(proto.Size(part)) + int64(protowire.SizeVarint(uint64(proto.Size(part))))
			if written+size > max {
				c.cut = fmt.Sprintf("the chain's records from seq %d on are not in the export: they would take it past "+
					"agentd's bound on an export, %d bytes", r.Seq, max)
				return c, nil
			}
			if _, err := protodelim.MarshalTo(w, part); err != nil {
				return c, err
			}
			written += size
			c.records++
			seq = r.Seq + 1
		}
		if len(rows) < page {
			return c, nil
		}
	}
	return c, nil
}

// snapshotPrefix begins the name of the file an export's snapshot of the chain goes to, in
// Server.SnapshotDir: agentd's data directory, which only agentd's user can read. One left by an
// agentd that stopped mid-export is removed when agentd starts (RemoveSnapshots).
const snapshotPrefix = "export-snapshot-"

// maxSnapshot bounds an export's snapshot of the chain, unless Server.MaxSnapshot says otherwise.
const maxSnapshot = 1 << 30

// RemoveSnapshots removes the export snapshots in dir an agentd that stopped mid-export left behind:
// agentd holds its data directory alone, so none is in use. It returns how many it removed.
func RemoveSnapshots(dir string) (int, error) {
	names, err := filepath.Glob(filepath.Join(dir, snapshotPrefix+"*"))
	if err != nil {
		return 0, err
	}
	for _, n := range names {
		if err := os.Remove(n); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return 0, err
		}
	}
	return len(names), nil
}
