package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/explain"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/tasks"
)

func explainRun(t *testing.T, s *Server, runID string) *agentosv1.Explanation {
	t.Helper()
	e, err := s.ExplainRun(context.Background(), &agentosv1.ExplainRunRequest{RunId: runID})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// called has run make a gateway call of name, which returns status: agentd's record of it, and its
// events in the history, its input naming the call as a workflow's does.
func (f *verifyFixture) called(t *testing.T, run *agentosv1.Run, n int, name, status string) {
	t.Helper()
	ctx := context.Background()
	activityID := fmt.Sprintf("%d:%s", n, name)
	sum, _ := audit.ValueSHA256(map[string]any{})
	h, err := audit.Record(ctx, f.s.Ledger, audit.Event{WorkflowID: run.GetRunId(), Type: "call", Actor: audit.Run,
		Payload: map[string]any{"activity_id": activityID, "attempt": 1, "kind": "tool", "name": name, "status": status,
			"result_sha256": sum, "refused_by": audit.RefusedByTool}})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(map[string]any{"_audit": h})
	events := completed(t, int64(10*n+100), activityID, 1, &agentosv1.Result{Status: status, ValueJson: string(b)})
	input, err := converter.GetDefaultDataConverter().ToPayloads(&agentosv1.Call{Kind: "tool", Name: name})
	if err != nil {
		t.Fatal(err)
	}
	events[0].GetActivityTaskScheduledEventAttributes().Input = input
	key := run.GetRunId() + "/" + run.GetTemporalRunId()
	f.tc.mu.Lock()
	f.tc.histories[key] = append(f.tc.histories[key], events...)
	f.tc.mu.Unlock()
}

// ExplainRun reads the run's history in Temporal, then the ledger: its records, which the results in
// the history name, are there. The task names the steps.
func TestExplainRunReadsTheHistoryAndTheLedger(t *testing.T) {
	f := newVerifyFixture(t)
	run := f.run(t, 0)
	f.called(t, run, 1, "git.show_diff", "ok")
	f.called(t, run, 2, "git.apply_check", "denied")
	e := explainRun(t, f.s, run.GetRunId())
	if e.GetVerdict() != explain.VerdictRunning || e.GetStep() != `"tool git.apply_check"` || e.GetActivityId() != "2:git.apply_check" ||
		e.GetState() != "RUNNING" || len(e.GetMissing()) != 0 || len(e.GetIncidents()) != 0 {
		t.Fatalf("%v", e)
	}
	seqs := map[int64]bool{}
	for _, s := range e.GetSources() {
		seqs[s.GetSeq()] = s.GetSource() == "audit"
	}
	last := auditRecords(t, f.s)
	if !seqs[last[len(last)-1].Seq] || e.GetCost().GetLimitMicros() != 7 {
		t.Fatalf("sources %v, cost %v", e.GetSources(), e.GetCost())
	}
	// The task names its steps.
	f.s.Tasks["t"] = withSteps(f.s.Tasks["t"], map[tasks.Call]string{{Kind: "tool", Name: "git.apply_check"}: "validate_patch"})
	if e := explainRun(t, f.s, run.GetRunId()); e.GetStep() != "validate_patch" {
		t.Fatalf("step %q", e.GetStep())
	}
}

// A run agentd did not start is not found; Temporal failing to describe it, or to give its history,
// fails the explanation; a history Temporal no longer has is said so.
func TestExplainRunWhenSourcesAreMissing(t *testing.T) {
	f := newVerifyFixture(t)
	run := f.run(t, 1)
	if _, err := f.s.ExplainRun(context.Background(), &agentosv1.ExplainRunRequest{RunId: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("got %v", err)
	}
	key := run.GetRunId() + "/" + run.GetTemporalRunId()
	f.tc.historyErr[key] = errTemporal
	if _, err := f.s.ExplainRun(context.Background(), &agentosv1.ExplainRunRequest{RunId: run.GetRunId()}); status.Code(err) != codes.Unavailable {
		t.Fatalf("got %v", err)
	}
	delete(f.tc.historyErr, key)
	delete(f.tc.histories, key)
	e := explainRun(t, f.s, run.GetRunId())
	if !slices.ContainsFunc(e.GetMissing(), func(m string) bool { return strings.Contains(m, "history") }) {
		t.Fatalf("missing %v", e.GetMissing())
	}
	f.tc.missing = true
	e = explainRun(t, f.s, run.GetRunId())
	if e.GetVerdict() != explain.VerdictUnknown || e.GetState() != "UNKNOWN" || len(e.GetMissing()) != 1 {
		t.Fatalf("%v", e)
	}
}

var errTemporal = status.Error(codes.Unavailable, "Temporal is down")

// withSteps is t naming its steps.
func withSteps(t tasks.Task, steps map[tasks.Call]string) tasks.Task {
	t.Steps = steps
	return t
}

// exportStream is the server's end of an export, keeping what it is sent.
type exportStream struct {
	grpc.ServerStream
	parts  []*agentosv1.AuditExportPart
	onSend func(p *agentosv1.AuditExportPart)
}

func (s *exportStream) Send(p *agentosv1.AuditExportPart) error {
	if s.onSend != nil {
		s.onSend(p)
	}
	s.parts = append(s.parts, p)
	return nil
}
func (s *exportStream) Context() context.Context { return context.Background() }

func exportOf(t *testing.T, s *Server, runID string) (*agentosv1.AuditExportHead, []*agentosv1.AuditRecord, []*agentosv1.ExecutionHistory) {
	t.Helper()
	var st exportStream
	if err := s.ExportAudit(&agentosv1.ExportAuditRequest{RunId: runID}, &st); err != nil {
		t.Fatal(err)
	}
	var head *agentosv1.AuditExportHead
	var records []*agentosv1.AuditRecord
	var histories []*agentosv1.ExecutionHistory
	heads := false
	for _, p := range st.parts {
		switch {
		case p.GetHistory() != nil:
			if head != nil || heads {
				t.Fatal("a history after the heads")
			}
			histories = append(histories, p.GetHistory())
		case p.GetPushedHead() != nil:
			if head != nil {
				t.Fatal("a pushed head after the chain's")
			}
			heads = true
		case p.GetHead() != nil:
			if head != nil {
				t.Fatal("two heads")
			}
			head = p.GetHead()
		case p.GetRecord() != nil:
			if head == nil {
				t.Fatal("a record before the head")
			}
			records = append(records, p.GetRecord())
		}
	}
	if head == nil || head.GetRecords() != int64(len(records)) {
		t.Fatalf("head %v, %d records", head, len(records))
	}
	return head, records, histories
}

// An export holds each run's history and its anchors, then the chain's head and every record up to
// it, as the ledger has them; or one run's.
func TestExportAudit(t *testing.T) {
	f := newVerifyFixture(t)
	a, b := f.run(t, 2), f.run(t, 1)
	head, records, histories := exportOf(t, f.s, "")
	all := auditRecords(t, f.s)
	if head.GetSeq() != all[len(all)-1].Seq || head.GetSha256() != all[len(all)-1].Sha256 || len(records) != len(all) {
		t.Fatalf("head %v, %d records of %d", head, len(records), len(all))
	}
	for i, r := range records {
		w := all[i]
		peer := sql.NullInt64{}
		if r.PeerUid != nil {
			peer = sql.NullInt64{Int64: *r.PeerUid, Valid: true}
		}
		if (db.AuditLog{Seq: r.GetSeq(), At: r.GetAt(), WorkflowID: r.GetWorkflowId(), Type: r.GetType(), Actor: r.GetActor(),
			PeerUid: peer, Payload: r.GetPayload(), PrevSha256: r.GetPrevSha256(), Sha256: r.GetSha256()}) != w {
			t.Fatalf("record %d: %v, want %v", i, r, w)
		}
	}
	if len(histories) != 2 {
		t.Fatalf("%d histories", len(histories))
	}
	anchors := 0
	for _, h := range histories {
		if h.GetHistoryJson() == "" || h.GetUnavailable() != "" {
			t.Fatalf("%v", h)
		}
		for _, an := range h.GetAnchors() {
			if !slices.ContainsFunc(all, func(r db.AuditLog) bool { return r.Seq == an.GetSeq() && r.Sha256 == an.GetSha256() }) ||
				an.GetAttempt() != 1 || !strings.HasSuffix(an.GetActivityId(), ":git.show_diff") || an.GetEventId() == 0 {
				t.Fatalf("anchor %v", an)
			}
			anchors++
		}
	}
	if anchors != 3 {
		t.Fatalf("%d anchors", anchors)
	}
	// Each anchor holds the bytes its record's result_sha256 is of.
	for _, h := range histories {
		for _, an := range h.GetAnchors() {
			var p struct {
				ResultSHA256 string `json:"result_sha256"`
			}
			i := slices.IndexFunc(all, func(r db.AuditLog) bool { return r.Seq == an.GetSeq() })
			if err := json.Unmarshal([]byte(all[i].Payload), &p); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(an.GetResultJson())
			if hex.EncodeToString(sum[:]) != p.ResultSHA256 || !strings.HasPrefix(string(an.GetResultJson()), `{"blob":"sha256:`) {
				t.Fatalf("anchor %v: result_json %q, record %s", an, an.GetResultJson(), all[i].Payload)
			}
		}
	}

	// One run: its history and its records only.
	_, records, histories = exportOf(t, f.s, b.GetRunId())
	if len(histories) != 1 || histories[0].GetRunId() != b.GetRunId() || len(records) == 0 {
		t.Fatalf("%v, %d records", histories, len(records))
	}
	for _, r := range records {
		if r.GetWorkflowId() != b.GetRunId() {
			t.Fatalf("%v", r)
		}
	}
	// A run nobody has is not found; a history Temporal no longer has is said so; one it cannot give
	// fails the export.
	if err := f.s.ExportAudit(&agentosv1.ExportAuditRequest{RunId: "nope"}, &exportStream{}); status.Code(err) != codes.NotFound {
		t.Fatalf("got %v", err)
	}
	key := a.GetRunId() + "/" + a.GetTemporalRunId()
	f.tc.historyErr[key] = errTemporal
	if err := f.s.ExportAudit(&agentosv1.ExportAuditRequest{}, &exportStream{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("got %v", err)
	}
	delete(f.tc.historyErr, key)
	delete(f.tc.histories, key)
	_, _, histories = exportOf(t, f.s, a.GetRunId())
	if len(histories) != 1 || histories[0].GetUnavailable() == "" || histories[0].GetHistoryJson() != "" {
		t.Fatalf("%v", histories)
	}
}

// A whole chain's export holds the heads agentd pushed to the chain's anchor workflows, before the
// chain: each names a record the export holds. One run's export holds none.
func TestAnExportHoldsThePushedHeads(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 1)
	all := auditRecords(t, f.s)
	head := audit.Head{Seq: all[len(all)-1].Seq, Sha256: all[len(all)-1].Sha256}
	id := audit.AnchorWorkflowID(f.chain(t), time.Now())
	f.anchorWorkflow(id, [2]string{}, signaled(t, 2, head))
	f.anchorWorkflow(audit.AnchorWorkflowID("fedcba9876543210", time.Now()), [2]string{}, signaled(t, 2, audit.Head{Seq: 99}))
	var st exportStream
	if err := f.s.ExportAudit(&agentosv1.ExportAuditRequest{}, &st); err != nil {
		t.Fatal(err)
	}
	var pushed []*agentosv1.PushedHead
	for _, p := range st.parts {
		if h := p.GetPushedHead(); h != nil {
			pushed = append(pushed, h)
		}
	}
	if len(pushed) != 1 || pushed[0].GetAnchorWorkflowId() != id || pushed[0].GetSeq() != head.Seq || pushed[0].GetSha256() != head.Sha256 {
		t.Fatalf("pushed %v", pushed)
	}
	run := all[len(all)-1].WorkflowID
	st = exportStream{}
	if err := f.s.ExportAudit(&agentosv1.ExportAuditRequest{RunId: run}, &st); err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(st.parts, func(p *agentosv1.AuditExportPart) bool { return p.GetPushedHead() != nil }) {
		t.Fatal("a run's export holds pushed heads")
	}
}

// The chain an export sends is a snapshot taken in one short read transaction, which has ended
// before the export sends it: a reader slow to take the export keeps no snapshot of the ledger's
// open, which would keep its write-ahead log from being checkpointed.
func TestAnExportHoldsNoTransactionWhileItSends(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 2)
	var path string
	if err := f.s.Ledger.DB.QueryRow("SELECT file FROM pragma_database_list WHERE name = 'main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(200)")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	checked := 0
	st := exportStream{onSend: func(p *agentosv1.AuditExportPart) {
		if p.GetRecord() == nil || checked > 0 {
			return
		}
		checked++
		// A write after the snapshot, then a checkpoint that needs every reader done with the log.
		if _, err := audit.Record(context.Background(), f.s.Ledger, audit.Event{Type: "agentd.read", Actor: audit.Agentd,
			Payload: map[string]any{}}); err != nil {
			t.Fatal(err)
		}
		var busy, frames, done int
		if err := raw.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &frames, &done); err != nil {
			t.Fatal(err)
		}
		if busy != 0 {
			t.Errorf("a read transaction is open while the export sends its records")
		}
	}}
	if err := f.s.ExportAudit(&agentosv1.ExportAuditRequest{}, &st); err != nil {
		t.Fatal(err)
	}
	if checked != 1 {
		t.Fatal("no record sent")
	}
}

// panicky is a Temporal whose histories make whatever reads them panic.
type panicky struct{ *fakeTemporal }

func (panicky) GetWorkflowHistory(context.Context, string, string, bool, enumspb.HistoryEventFilterType) client.HistoryEventIterator {
	return panicIterator{}
}

type panicIterator struct{}

func (panicIterator) HasNext() bool                          { return true }
func (panicIterator) Next() (*historypb.HistoryEvent, error) { panic("a history nobody expected") }

// A panic in a Control call, unary (explaining a run) or streaming (an export), fails the call, not
// agentd: the recovery interceptors log it with its stack.
func TestAPanicInAControlCallFailsTheCall(t *testing.T) {
	f := newVerifyFixture(t)
	run := f.run(t, 1)
	dir, err := os.MkdirTemp("/tmp", "agentd-ctl-") // a socket path must be short
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	ln, err := net.Listen("unix", filepath.Join(dir, "c.sock"))
	if err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	var mu sync.Mutex
	srv := grpc.NewServer(ServerOptions(slog.New(slog.NewTextHandler(&lockedWriter{w: &logged, mu: &mu}, nil)))...)
	agentosv1.RegisterControlServer(srv, f.s)
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("unix://"+filepath.Join(dir, "c.sock"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := agentosv1.NewControlClient(conn)
	ctx := context.Background()
	f.s.Temporal = panicky{f.tc}
	if _, err := c.ExplainRun(ctx, &agentosv1.ExplainRunRequest{RunId: run.GetRunId()}); status.Code(err) != codes.Internal {
		t.Fatalf("explain: %v", err)
	}
	stream, err := c.ExportAudit(ctx, &agentosv1.ExportAuditRequest{})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.Internal {
		t.Fatalf("export: %v", err)
	}
	mu.Lock()
	text := logged.String()
	mu.Unlock()
	if strings.Count(text, "a Control call panicked") != 2 || !strings.Contains(text, "a history nobody expected") ||
		!strings.Contains(text, "goroutine") {
		t.Fatalf("log %s", text)
	}
	f.s.Temporal = f.tc
	if _, err := c.ExplainRun(ctx, &agentosv1.ExplainRunRequest{RunId: run.GetRunId()}); err != nil {
		t.Fatal(err)
	}
}

type lockedWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// An export's snapshot of the chain is a file of agentd's own: in its data directory, under the
// fixed prefix RemoveSnapshots looks for, readable by agentd's user alone, and gone with the export.
func TestAnExportsSnapshotIsAgentdsOwn(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 2)
	f.s.SnapshotDir = t.TempDir()
	seen := 0
	st := exportStream{onSend: func(p *agentosv1.AuditExportPart) {
		if p.GetRecord() == nil || seen > 0 {
			return
		}
		seen++
		names, err := filepath.Glob(filepath.Join(f.s.SnapshotDir, "*"))
		if err != nil || len(names) != 1 || !strings.HasPrefix(filepath.Base(names[0]), snapshotPrefix) {
			t.Fatalf("while the export sends, its data directory holds %v (%v)", names, err)
		}
		if fi, err := os.Stat(names[0]); err != nil || fi.Mode().Perm() != 0o600 {
			t.Errorf("the snapshot: %v, %v", fi.Mode(), err)
		}
	}}
	if err := f.s.ExportAudit(&agentosv1.ExportAuditRequest{}, &st); err != nil {
		t.Fatal(err)
	}
	if left, err := os.ReadDir(f.s.SnapshotDir); seen != 1 || err != nil || len(left) != 0 {
		t.Fatalf("%d records seen; left behind %v (%v)", seen, left, err)
	}
}

// An export is bounded: past the bound it holds the chain's records up to it, and its head says
// where it stopped, and why. The head is the chain's, whatever the cut.
func TestAnExportStopsAtItsBound(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 3)
	f.s.SnapshotDir = t.TempDir()
	head, whole, _ := exportOf(t, f.s, "")
	if head.GetCut() != "" || len(whole) < 3 {
		t.Fatalf("unbounded: %v, %d records", head, len(whole))
	}
	size := func(r *agentosv1.AuditRecord) int64 {
		n := proto.Size(&agentosv1.AuditExportPart{Part: &agentosv1.AuditExportPart_Record{Record: r}})
		return int64(n + protowire.SizeVarint(uint64(n)))
	}
	f.s.MaxSnapshot = size(whole[0]) + size(whole[1]) // the first two, to the byte
	cut, records, _ := exportOf(t, f.s, "")
	want := fmt.Sprintf("the chain's records from seq %d on are not in the export", whole[2].GetSeq())
	if len(records) != 2 || !proto.Equal(records[1], whole[1]) || !strings.Contains(cut.GetCut(), want) ||
		cut.GetSeq() != head.GetSeq() || cut.GetSha256() != head.GetSha256() {
		t.Fatalf("bounded: head %v, %d records", cut, len(records))
	}
	f.s.MaxSnapshot--
	if _, records, _ := exportOf(t, f.s, ""); len(records) != 1 {
		t.Fatalf("a byte short of two: %d records", len(records))
	}
}

// RemoveSnapshots removes what an agentd that stopped mid-export left in its data directory, and
// nothing else there.
func TestRemoveSnapshots(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{snapshotPrefix + "1", snapshotPrefix + "22", "ledger.db", "export-snapshot", "other"} {
		if err := os.WriteFile(filepath.Join(dir, n), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	n, err := RemoveSnapshots(dir)
	var left []string
	if entries, rerr := os.ReadDir(dir); rerr == nil {
		for _, e := range entries {
			left = append(left, e.Name())
		}
	}
	if err != nil || n != 2 || !slices.Equal(left, []string{"export-snapshot", "ledger.db", "other"}) {
		t.Fatalf("removed %d (%v), left %v", n, err, left)
	}
}
