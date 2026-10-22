package control

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/budget"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/tasks"
)

func newServer(t *testing.T) (*Server, *fakeTemporal) {
	t.Helper()
	l, err := ledger.Open(context.Background(), filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	tc := &fakeTemporal{}
	resolve := func(_ context.Context, p map[string]string) (any, map[string]string, error) { return p, p, nil }
	return &Server{Ledger: l, Budget: &budget.Budget{Ledger: l}, Temporal: tc, CancelGrace: time.Hour,
		Tasks: tasks.Registry{"t": {Name: "t", WorkflowType: "t", Resolve: resolve, DefaultBudget: 7}},
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil))}, tc
}

// What Control shows the operator, it reads in read transactions of the ledger's, which load the
// schema again and compare it first (ledger.Read). Someone who puts a view in place of approvals,
// and changes schema_version so that every connection loads it, would show the operator a made-up
// approval; each read fails instead, naming what is not the migrations'.
func TestControlShowsNothingThroughAnotherSchema(t *testing.T) {
	s, _ := newServer(t)
	ctx := context.Background()
	run, err := s.StartRun(asOperator(ctx), &agentosv1.StartRunRequest{Task: "t"})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := s.Ledger.DB.Query(`SELECT name FROM pragma_table_info('approvals') ORDER BY cid`)
	if err != nil {
		t.Fatal(err)
	}
	made := map[string]string{"approval_id": "'made-up'", "workflow_id": "'" + run.GetRunId() + "'", "status": "'PENDING'",
		"tool": "'github.open_draft_pr'", "created_at": "'2026-01-01T00:00:00Z'", "expires_at": "'9999-12-31T00:00:00.000000000Z'"}
	var values []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			t.Fatal(err)
		}
		values = append(values, cmp.Or(made[col], "''"))
	}
	rows.Close()
	if _, err := s.Ledger.DB.Exec(`ALTER TABLE approvals RENAME TO approvals_kept;
		CREATE VIEW approvals AS SELECT * FROM approvals_kept UNION ALL SELECT ` + strings.Join(values, ", ")); err != nil {
		t.Fatal(err)
	}
	named := `view "approvals" on "approvals", which no migration makes`
	for name, read := range map[string]func() error{
		"ListPending": func() error {
			resp, err := s.ListPending(ctx, &agentosv1.ListPendingRequest{})
			if err == nil {
				t.Logf("pending: %v", resp.GetItems())
			}
			return err
		},
		"GetApproval": func() error {
			_, err := s.GetApproval(ctx, &agentosv1.GetApprovalRequest{ApprovalId: "made-up"})
			return err
		},
		"ListRuns": func() error { _, err := s.ListRuns(ctx, &agentosv1.ListRunsRequest{}); return err },
		"GetRun":   func() error { _, err := s.GetRun(ctx, &agentosv1.GetRunRequest{RunId: run.GetRunId()}); return err },
	} {
		if err := read(); !errors.Is(err, ledger.ErrSchemaMismatch) || !strings.Contains(err.Error(), named) {
			t.Errorf("%s: %v, want an error naming the view", name, err)
		}
	}
}

func auditRecords(t *testing.T, s *Server) []db.AuditLog {
	t.Helper()
	rows, err := s.Ledger.Q.ListAuditRecords(context.Background(), db.ListAuditRecordsParams{Seq: 1, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// failAppends makes every append of a record of typ fail, as a full disk would, until the returned
// func drops the trigger that does it.
func failAppends(t *testing.T, s *Server, typ string) (drop func()) {
	t.Helper()
	s.Ledger.UncheckedSchema = true // the trigger is the test's
	if _, err := s.Ledger.DB.Exec(fmt.Sprintf(`CREATE TRIGGER fail_appends BEFORE INSERT ON audit_log WHEN NEW.type = '%s'
		BEGIN SELECT RAISE(ABORT, 'no room for the record'); END`, typ)); err != nil {
		t.Fatal(err)
	}
	return func() {
		if _, err := s.Ledger.DB.Exec(`DROP TRIGGER fail_appends`); err != nil {
			t.Fatal(err)
		}
	}
}

func me(t *testing.T) (string, sql.NullInt64) {
	t.Helper()
	who, err := operator(asOperator(context.Background()))
	if err != nil {
		t.Fatal(err)
	}
	return who.Actor, who.PeerUID
}

// A run's start is recorded by the operator who asked for it, with the uid the socket gave: its
// creation before Temporal is asked, its execution once Temporal has it.
func TestARunsStartIsRecordedByItsOperator(t *testing.T) {
	s, tc := newServer(t)
	run, err := s.StartRun(asOperator(context.Background()), &agentosv1.StartRunRequest{Task: "t",
		Params: map[string]string{"repo": "acme/payments"}, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	actor, uid := me(t)
	rows := auditRecords(t, s)
	if len(rows) != 2 || uid.Int64 != int64(os.Getuid()) || !strings.HasPrefix(actor, "operator:") {
		t.Fatalf("records %+v, by %s %v", rows, actor, uid)
	}
	want := []struct{ typ, payload string }{
		{"run.created", `{"budget_limit_micros":7,"dry_run":true,"params":{"repo":"acme/payments"},"task":"t"}`},
		{"run.started", `{"temporal_run_id":"run-of-` + run.GetRunId() + `"}`},
	}
	for i, r := range rows {
		if r.Type != want[i].typ || r.Payload != want[i].payload || r.WorkflowID != run.GetRunId() || r.Actor != actor || r.PeerUid != uid {
			t.Errorf("record %d: %+v", i+1, r)
		}
	}
	if len(tc.started) != 1 {
		t.Errorf("started %v", tc.started)
	}
}

// Each change of a run goes with its record, or is not made.
func TestARunsChangesAreMadeWithTheirRecords(t *testing.T) {
	ctx := asOperator(context.Background())
	start := &agentosv1.StartRunRequest{Task: "t", Params: map[string]string{}}
	t.Run("created", func(t *testing.T) {
		s, tc := newServer(t)
		drop := failAppends(t, s, "run.created")
		if _, err := s.StartRun(ctx, start); err == nil {
			t.Fatal("a run was started without the record of its creation")
		}
		if runs, _ := s.Ledger.Q.ListAllRuns(ctx); len(runs) != 0 || len(tc.started) != 0 {
			t.Fatalf("runs %+v, started in Temporal %v", runs, tc.started)
		}
		drop()
		if _, err := s.StartRun(ctx, start); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("started", func(t *testing.T) {
		s, tc := newServer(t)
		drop := failAppends(t, s, "run.started")
		if _, err := s.StartRun(ctx, start); status.Code(err) != codes.Internal {
			t.Fatalf("got %v, want the start refused", err)
		}
		// Not recorded as started, and stopped: without its run ID, the gateway refuses its calls.
		runs, _ := s.Ledger.Q.ListAllRuns(ctx)
		if len(runs) != 1 || runs[0].RunID.Valid || len(tc.terminated) != 1 {
			t.Fatalf("runs %+v, terminated %v", runs, tc.terminated)
		}
		drop()
		// The next agentd's start records it, as its own.
		if err := s.RecoverStarts(context.Background()); err != nil {
			t.Fatal(err)
		}
		rows := auditRecords(t, s)
		if last := rows[len(rows)-1]; last.Type != "run.started" || last.Actor != audit.Agentd || last.PeerUid.Valid {
			t.Fatalf("records %+v", rows)
		}
	})
	t.Run("discarded", func(t *testing.T) {
		s, tc := newServer(t)
		tc.startErr = errors.New("Temporal is down")
		drop := failAppends(t, s, "run.discarded")
		if _, err := s.StartRun(ctx, start); status.Code(err) != codes.Unavailable {
			t.Fatalf("got %v", err)
		}
		if runs, _ := s.Ledger.Q.ListAllRuns(ctx); len(runs) != 1 { // kept: its record could not be made
			t.Fatalf("runs %+v", runs)
		}
		drop()
		tc.missing = true // the next agentd's start finds it never reached Temporal
		if err := s.RecoverStarts(context.Background()); err != nil {
			t.Fatal(err)
		}
		rows := auditRecords(t, s)
		if runs, _ := s.Ledger.Q.ListAllRuns(ctx); len(runs) != 0 || rows[len(rows)-1].Type != "run.discarded" ||
			rows[len(rows)-1].Actor != audit.Agentd || !strings.Contains(rows[len(rows)-1].Payload, "never reached Temporal") {
			t.Fatalf("runs %+v, records %+v", runs, rows)
		}
		// A start that fails is discarded by the operator who asked for it.
		if _, err := s.StartRun(ctx, start); status.Code(err) != codes.Unavailable {
			t.Fatalf("got %v", err)
		}
		rows = auditRecords(t, s)
		last := rows[len(rows)-1]
		if actor, _ := me(t); last.Type != "run.discarded" || last.Actor != actor || !strings.Contains(last.Payload, "Temporal is down") {
			t.Fatalf("records %+v", rows)
		}
	})
	t.Run("cancelled", func(t *testing.T) {
		s, tc := newServer(t)
		run, err := s.StartRun(ctx, start)
		if err != nil {
			t.Fatal(err)
		}
		drop := failAppends(t, s, "run.cancelled")
		if _, err := s.CancelRun(ctx, &agentosv1.CancelRunRequest{RunId: run.GetRunId()}); err == nil {
			t.Fatal("a run was cancelled without its record")
		}
		if r, _ := s.Ledger.Q.GetRun(ctx, run.GetRunId()); r.CancelledAt.Valid || len(tc.cancelled) != 0 {
			t.Fatalf("run %+v, cancelled in Temporal %v", r, tc.cancelled)
		}
		drop()
		if _, err := s.CancelRun(ctx, &agentosv1.CancelRunRequest{RunId: run.GetRunId()}); err != nil {
			t.Fatal(err)
		}
		rows := auditRecords(t, s)
		last := rows[len(rows)-1]
		r, _ := s.Ledger.Q.GetRun(ctx, run.GetRunId())
		if actor, uid := me(t); last.Type != "run.cancelled" || last.Actor != actor || last.PeerUid != uid ||
			last.Payload != `{"cancelled_at":"`+r.CancelledAt.String+`"}` {
			t.Fatalf("records %+v", rows)
		}
		// Cancelled again, nothing changes, and there is no record of it.
		if _, err := s.CancelRun(ctx, &agentosv1.CancelRunRequest{RunId: run.GetRunId()}); err != nil {
			t.Fatal(err)
		}
		if again := auditRecords(t, s); len(again) != len(rows) {
			t.Fatalf("records after a second cancel %+v", again)
		}
	})
}

// A call that came without the socket's peer credentials changes nothing: its record would name
// nobody.
func TestNoChangeWithoutPeerCredentials(t *testing.T) {
	s, tc := newServer(t)
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"start":  func() error { _, err := s.StartRun(ctx, &agentosv1.StartRunRequest{Task: "t"}); return err },
		"cancel": func() error { _, err := s.CancelRun(ctx, &agentosv1.CancelRunRequest{RunId: "r"}); return err },
		"decide": func() error {
			_, err := s.ResolveApproval(ctx, &agentosv1.ResolveApprovalRequest{ApprovalId: "a"})
			return err
		},
		"reconcile": func() error {
			_, err := s.ResolveReconciliation(ctx, &agentosv1.ResolveReconciliationRequest{EffectId: "e"})
			return err
		},
		"extend": func() error { _, err := s.ExtendBudget(ctx, &agentosv1.ExtendBudgetRequest{RunId: "r"}); return err },
	} {
		if err := call(); status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s: %v", name, err)
		}
	}
	if rows := auditRecords(t, s); len(rows) != 0 || len(tc.started) != 0 {
		t.Errorf("records %+v, started %v", rows, tc.started)
	}
}

// An operator is named by the user the kernel says they are; a uid without a name, by the uid.
func TestAnOperatorIsTheUserOfTheirConnection(t *testing.T) {
	who, err := operator(asOperator(context.Background()))
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := json.Marshal(who); !strings.HasPrefix(who.Actor, "operator:") || who.Actor == "operator:" ||
		who.PeerUID != (sql.NullInt64{Int64: int64(os.Getuid()), Valid: true}) {
		t.Errorf("%s", b)
	}
	nobody := asUID(context.Background(), 2_000_000_123) // a uid no account has
	if who, err := operator(nobody); err != nil || who.Actor != "operator:uid:2000000123" || who.PeerUID.Int64 != 2_000_000_123 {
		t.Errorf("%+v %v", who, err)
	}
	for _, name := range []string{"alice", "a.b-c_d", "名前"} {
		if !plainName(name) {
			t.Errorf("%q is a plain name", name)
		}
	}
	for _, name := range []string{"", "a b", "a\x1b[2J", "a\n", strings.Repeat("a", 101)} {
		if plainName(name) {
			t.Errorf("%q is not a plain name", name)
		}
	}
}

// Over the control socket, the uid in each record is the one the kernel gives for the process at
// the other end; a connection that is not a Unix socket's is refused.
func TestThePeersUIDComesFromTheKernel(t *testing.T) {
	s, _ := newServer(t)
	dir, err := os.MkdirTemp("/tmp", "agentd-ctl-") // a socket path must be short
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	ln, err := net.Listen("unix", filepath.Join(dir, "c.sock"))
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.Creds(PeerCredentials()))
	agentosv1.RegisterControlServer(srv, s)
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("unix://"+filepath.Join(dir, "c.sock"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	if _, err := agentosv1.NewControlClient(conn).StartRun(ctx, &agentosv1.StartRunRequest{Task: "t"}); err != nil {
		t.Fatal(err)
	}
	actor, uid := me(t)
	if rows := auditRecords(t, s); len(rows) != 2 || rows[0].PeerUid != uid || rows[0].Actor != actor {
		t.Fatalf("records %+v, want them by %s %v", rows, actor, uid)
	}

	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(tcp)
	other, err := grpc.NewClient(tcp.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	short, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := agentosv1.NewControlClient(other).StartRun(short, &agentosv1.StartRunRequest{Task: "t"}); err == nil {
		t.Fatal("a TCP connection was served")
	}
	if rows := auditRecords(t, s); len(rows) != 2 {
		t.Fatalf("records %+v", rows)
	}
}

// Temporal ending a run at agentd's request is recorded, with why: when a start could not be
// recorded, and when a cancelled run did not stop in time.
func TestATerminationIsRecorded(t *testing.T) {
	ctx := asOperator(context.Background())
	start := &agentosv1.StartRunRequest{Task: "t", Params: map[string]string{}}
	terminated := func(s *Server) []db.AuditLog {
		var out []db.AuditLog
		for _, r := range auditRecords(t, s) {
			if r.Type == "run.terminated" {
				out = append(out, r)
			}
		}
		return out
	}
	s, tc := newServer(t)
	drop := failAppends(t, s, "run.started")
	if _, err := s.StartRun(ctx, start); status.Code(err) != codes.Internal {
		t.Fatalf("got %v", err)
	}
	drop()
	if got := terminated(s); len(got) != 1 || got[0].Actor != audit.Agentd || len(tc.terminated) != 1 ||
		got[0].WorkflowID != tc.terminated[0] || !strings.Contains(got[0].Payload, `"why":"agentd could not record the run"`) {
		t.Fatalf("records %+v, terminated %v", got, tc.terminated)
	}

	s, tc = newServer(t)
	s.CancelGrace = 50 * time.Millisecond
	run, err := s.StartRun(ctx, start)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CancelRun(ctx, &agentosv1.CancelRunRequest{RunId: run.GetRunId()}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(terminated(s)) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	got := terminated(s)
	if len(got) != 1 || got[0].WorkflowID != run.GetRunId() || !strings.Contains(got[0].Payload, "cancel not honoured within 50ms") ||
		!strings.Contains(got[0].Payload, run.GetTemporalRunId()) {
		t.Fatalf("records %+v", got)
	}
}

// A run's start or discard that changes nothing has no record: the run had its execution recorded
// already, or a row with an execution is no start that failed.
func TestNoRecordOfARunChangeThatChangedNothing(t *testing.T) {
	ctx := asOperator(context.Background())
	s, _ := newServer(t)
	run, err := s.StartRun(ctx, &agentosv1.StartRunRequest{Task: "t"})
	if err != nil {
		t.Fatal(err)
	}
	before := len(auditRecords(t, s))
	if err := s.started(context.Background(), run.GetRunId(), "another-execution", agentd); err != nil {
		t.Fatal(err)
	}
	if err := s.discard(context.Background(), run.GetRunId(), agentd, "a test"); err != nil {
		t.Fatal(err)
	}
	if got := auditRecords(t, s); len(got) != before {
		t.Fatalf("records %+v", got[before:])
	}
	if r, _ := s.Ledger.Q.GetRun(context.Background(), run.GetRunId()); r.RunID.String != run.GetTemporalRunId() {
		t.Fatalf("run %+v", r)
	}
}

// A run's write that the ledger drops (a trigger that makes it do nothing, past the ledger's check
// of its schema) is not taken for one that found its work done: a cancel, a start's execution, a
// discard each fail, with no record of a change that did not happen.
func TestARunWriteDroppedIsNotTakenAsDone(t *testing.T) {
	ctx := asOperator(context.Background())
	for name, c := range map[string]struct {
		trigger string
		do      func(s *Server, workflowID string) error
	}{
		"a cancel": {`BEFORE UPDATE OF cancelled_at ON runs`, func(s *Server, workflowID string) error {
			_, err := s.CancelRun(ctx, &agentosv1.CancelRunRequest{RunId: workflowID})
			return err
		}},
		"a start's execution": {`BEFORE UPDATE OF run_id ON runs`, func(s *Server, workflowID string) error {
			return s.started(ctx, workflowID, "an-execution", agentd)
		}},
		"a discard": {`BEFORE DELETE ON runs`, func(s *Server, workflowID string) error {
			return s.discard(ctx, workflowID, agentd, "a test")
		}},
	} {
		t.Run(name, func(t *testing.T) {
			s, _ := newServer(t)
			workflowID := ""
			if name == "a cancel" {
				run, err := s.StartRun(ctx, &agentosv1.StartRunRequest{Task: "t", Params: map[string]string{}})
				if err != nil {
					t.Fatal(err)
				}
				workflowID = run.GetRunId()
			} else { // a start that has not recorded its execution yet
				workflowID = "starting"
				if err := s.Ledger.Q.InsertRun(ctx, db.InsertRunParams{WorkflowID: workflowID, Task: "t", ParamsJson: "{}",
					BudgetLimitMicros: 1, CreatedAt: ledger.Now()}); err != nil {
					t.Fatal(err)
				}
			}
			before := len(auditRecords(t, s))
			s.Ledger.UncheckedSchema = true // the trigger is the test's
			if _, err := s.Ledger.DB.Exec(`CREATE TRIGGER dropped ` + c.trigger + ` BEGIN SELECT RAISE(IGNORE); END`); err != nil {
				t.Fatal(err)
			}
			if err := c.do(s, workflowID); !errors.Is(err, ledger.ErrWriteDropped) {
				t.Fatalf("%v, want %v", err, ledger.ErrWriteDropped)
			}
			if got := auditRecords(t, s); len(got) != before {
				t.Fatalf("records of a change that did not happen: %+v", got[before:])
			}
		})
	}
}

// The name an operator is recorded under is the system's for their uid when it can stand in a
// record as it is, and their uid otherwise.
func TestAnOperatorsName(t *testing.T) {
	lookup := func(name string, err error) func(string) (*user.User, error) {
		return func(string) (*user.User, error) { return &user.User{Username: name}, err }
	}
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"alice", nil, "alice"},
		{"bob smith", nil, "uid:42"},
		{"evil\x1b[2J", nil, "uid:42"},
		{"", nil, "uid:42"},
		{strings.Repeat("a", 101), nil, "uid:42"},
		{"alice", errors.New("no such user"), "uid:42"},
	} {
		if got := operatorName(42, lookup(tc.name, tc.err)); got != tc.want {
			t.Errorf("%q (%v): %q, want %q", tc.name, tc.err, got, tc.want)
		}
	}
}
