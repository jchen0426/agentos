package explain

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// ReadLedger reads agentd's starts since the run's first record, not before; and each kind of the
// run's rows to its bound, saying what it did not read.
func TestReadLedger(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(ctx, filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	record := func(workflowID, typ string) int64 {
		h, err := audit.Record(ctx, l, audit.Event{WorkflowID: workflowID, Type: typ, Actor: audit.Agentd, Payload: map[string]any{}})
		if err != nil {
			t.Fatal(err)
		}
		return h.Seq
	}
	read := func() Ledger {
		var got Ledger
		if err := l.Read(ctx, func(q *db.Queries) error {
			var err error
			run, err := q.GetRun(ctx, "run-1")
			if err != nil {
				return err
			}
			got, err = ReadLedger(ctx, q, run, Wanted{})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return got
	}
	if err := l.Tx(ctx, func(q *db.Queries) error {
		return q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "run-1", Task: "security-audit", ParamsJson: "{}",
			BudgetLimitMicros: 1, CreatedAt: ledger.Now()})
	}); err != nil {
		t.Fatal(err)
	}
	record("", "agentd.started") // before the run
	first := record("run-1", "run.created")
	after := record("", "agentd.started")
	for i := 0; i < 5; i++ {
		record("run-1", "call")
	}
	for i := 0; i < 3; i++ {
		if err := l.Tx(ctx, func(q *db.Queries) error {
			_, err := q.InsertCall(ctx, db.InsertCallParams{WorkflowID: "run-1", ActivityID: fmt.Sprintf("%d:x", i),
				ScheduledAt: ledger.Now(), Kind: "tool", Name: "x", ArgsSha256: "a", CreatedAt: ledger.Now()})
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	// An answer that broke the schema twice: the gateway's own verdict, kept with it, whatever its blob.
	if err := l.Tx(ctx, func(q *db.Queries) error {
		if err := q.InsertModelCall(ctx, db.InsertModelCallParams{WorkflowID: "run-1", ActivityID: "2:audit_v1", Attempt: 1,
			RequestSeq: 1, RequestBlob: "sha256:" + strings.Repeat("a", 64), Provider: "fake", Model: "m", PromptVersion: "v",
			StartedAt: ledger.Now()}); err != nil {
			return err
		}
		return q.FinishModelCall(ctx, db.FinishModelCallParams{ResponseBlob: sql.NullString{String: "sha256:" + strings.Repeat("b", 64),
			Valid: true}, StopReason: sql.NullString{String: statusInvalidOutput, Valid: true}, WorkflowID: "run-1",
			ActivityID: "2:audit_v1", Attempt: 1, RequestSeq: 1, FinishedAt: sql.NullString{String: ledger.Now(), Valid: true}})
	}); err != nil {
		t.Fatal(err)
	}
	got := read()
	if len(got.Starts) != 1 || got.Starts[0].Seq != after || len(got.Records) != 6 || got.Records[0].Seq != first ||
		len(got.Calls) != 3 || got.RecordsCut || len(got.Cut) != 0 || got.LogBegan.IsZero() ||
		len(got.Requests) != 1 || got.Requests[0].Status != statusInvalidOutput {
		t.Fatalf("%+v", got)
	}

	defer func(records, calls, starts int) { maxRecords, maxCalls, maxStarts = records, calls, starts }(maxRecords, maxCalls, maxStarts)
	maxRecords, maxCalls, maxStarts = 4, 2, 1
	record("", "agentd.started")
	got = read()
	if len(got.Records) != 4 || !got.RecordsCut || len(got.Calls) != 2 || len(got.Starts) != 1 || got.Starts[0].Seq != after ||
		len(got.Cut) != 3 || !slices.ContainsFunc(got.Cut, func(c string) bool { return strings.Contains(c, "more than 4 audit records") }) {
		t.Fatalf("%+v", got)
	}
	// What a report names, as the evidence it does not rest on.
	if want := []string{"audit records past the first 4", "starts of agentd since it began past the first 1",
		"calls in the loop guard's table past the first 2"}; !slices.Equal(got.CutWhat, want) {
		t.Fatalf("cut %q", got.CutWhat)
	}
}

// A record ReadLedger cannot read is said, for the result or failure it was for to go unchecked: one a
// result names, read on its own; and the run's call records past the bound, read in one pass for the
// attempts that failed, one of which cannot be read: every attempt it was for is unchecked (one whose
// record the run's records hold is read all the same: explain takes that). A context that ends fails it.
func TestReadLedgerSaysWhatItCouldNotRead(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(ctx, filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := l.Tx(ctx, func(q *db.Queries) error {
		return q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "run-1", Task: "security-audit", ParamsJson: "{}",
			BudgetLimitMicros: 1, CreatedAt: ledger.Now()})
	}); err != nil {
		t.Fatal(err)
	}
	var own []int64
	for k := 1; k <= 3; k++ {
		h, err := audit.Record(ctx, l, audit.Event{WorkflowID: "run-1", Type: "call", Actor: audit.Run,
			Payload: map[string]any{"activity_id": "1:x", "attempt": k, "status": "error"}})
		if err != nil {
			t.Fatal(err)
		}
		own = append(own, h.Seq)
	}
	// Rows that cannot be scanned (a peer_uid that is not a number): another run's, and one of the run's
	// own attempts, past the bound.
	for _, r := range []struct {
		seq             int64
		workflowID, pay string
	}{{1 << 30, "run-2", `{}`}, {1<<30 + 1, "run-1", `{"activity_id":"1:x","attempt":9}`}} {
		if _, err := l.DB.Exec(`INSERT INTO audit_log (seq, at, workflow_id, type, actor, peer_uid, payload, prev_sha256, sha256)
			VALUES (?, '', ?, 'call', 'run', 'not a uid', ?, '', ?)`, r.seq, r.workflowID, r.pay, fmt.Sprint(r.seq)); err != nil {
			t.Fatal(err)
		}
	}
	defer func(n int) { maxRecords = n }(maxRecords)
	maxRecords = 2
	var got Ledger
	if err := l.Read(ctx, func(q *db.Queries) error {
		var err error
		run, err := q.GetRun(ctx, "run-1")
		if err != nil {
			return err
		}
		got, err = ReadLedger(ctx, q, run, Wanted{Seqs: []int64{1 << 30, own[0]},
			Attempts: []Attempt{{ActivityID: "1:x", Attempt: 9}, {ActivityID: "1:x", Attempt: 1}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !got.RecordsCut || got.NamedErr[1<<30] == "" || got.AttemptErr[Attempt{ActivityID: "1:x", Attempt: 9}] == "" ||
		len(got.NamedErr) != 1 || len(got.Named) != 0 {
		t.Fatalf("%+v", got)
	}
	ended, cancel := context.WithCancel(ctx)
	cancel()
	var l2 Ledger
	if err := readWanted(ended, l.Q, db.Run{WorkflowID: "run-1"}, Wanted{Seqs: []int64{1 << 30}}, &l2); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, %+v", err, l2)
	}
	l2 = Ledger{RecordsCut: true}
	if err := readWanted(ended, l.Q, db.Run{WorkflowID: "run-1"}, Wanted{Attempts: []Attempt{{ActivityID: "1:x", Attempt: 9}}},
		&l2); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, %+v", err, l2)
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// ReadLedger reads the run's own records to the bound, those of its own execution's calls agentd
// refused before it knew the execution among them: those of calls from executions agentd did not
// start, under its workflow ID, are counted, not read, so that a flood of them cannot crowd out the
// run's own. And it reads what a history wants on its own, whatever the bound: the record at each seq
// a result names (whoever's, or none past the chain's head), and, once the run's records are cut,
// agentd's records of each attempt that failed, of the run's own execution.
func TestReadLedgerReadsWhatResultsName(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(ctx, filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	record := func(workflowID, actor string, payload map[string]any) int64 {
		h, err := audit.Record(ctx, l, audit.Event{WorkflowID: workflowID, Type: "call", Actor: actor, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		return h.Seq
	}
	if err := l.Tx(ctx, func(q *db.Queries) error {
		return q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "run-1", Task: "security-audit", ParamsJson: "{}",
			BudgetLimitMicros: 1, CreatedAt: ledger.Now()})
	}); err != nil {
		t.Fatal(err)
	}
	var own []int64
	for k := 1; k <= 3; k++ {
		own = append(own, record("run-1", audit.Run, map[string]any{"activity_id": "1:x", "attempt": k, "status": "error"}))
	}
	// Attempt 4, refused before StartRun recorded the execution's ID: the run's own.
	early := record("run-1", audit.UnknownRun, map[string]any{"activity_id": "1:x", "attempt": 4, "status": "error",
		"temporal_run_id": "exec-1"})
	for i := 0; i < 10; i++ { // another execution's, under the run's workflow ID: the same activity and attempt
		p := map[string]any{"activity_id": "1:x", "attempt": 4, "status": "error", "temporal_run_id": "exec-2"}
		if i%2 == 0 {
			delete(p, "temporal_run_id")
		}
		record("run-1", audit.UnknownRun, p)
	}
	other := record("run-2", audit.Run, map[string]any{"activity_id": "1:x", "attempt": 1, "status": "ok"})
	defer func(n int) { maxRecords = n }(maxRecords)
	maxRecords = 4
	run := setExecution(t, l, "run-1", "exec-1")
	read := func(want Wanted) Ledger {
		var got Ledger
		if err := l.Read(ctx, func(q *db.Queries) error {
			var err error
			got, err = ReadLedger(ctx, q, run, want)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return got
	}
	got := read(Wanted{Seqs: []int64{own[0], other, 1 << 40}})
	if len(got.Records) != 4 || got.Records[3].Seq != early || got.RecordsCut || got.Unknown != 10 || len(got.Named) != 1 ||
		got.Named[other].WorkflowID != "run-2" || len(got.NamedErr) != 0 {
		t.Fatalf("%+v", got)
	}
	maxRecords = 2
	got = read(Wanted{Seqs: []int64{own[2]}, Attempts: []Attempt{{ActivityID: "1:x", Attempt: 3}, {ActivityID: "1:x", Attempt: 7}}})
	if len(got.Records) != 2 || !got.RecordsCut || len(got.Named) != 1 || got.Named[own[2]].Seq != own[2] {
		t.Fatalf("%+v", got)
	}
	got = read(Wanted{Attempts: []Attempt{{ActivityID: "1:x", Attempt: 3}, {ActivityID: "1:x", Attempt: 4}}})
	if len(got.Named) != 2 || got.Named[own[2]].Seq != own[2] || got.Named[early].Seq != early {
		t.Fatalf("%+v", got)
	}
	// Before StartRun recorded the execution's ID, none of its records is the run's but agentd's own.
	run = setExecution(t, l, "run-1", "")
	maxRecords = 10
	if got = read(Wanted{}); len(got.Records) != 3 || got.Unknown != 11 {
		t.Fatalf("%+v", got)
	}
}

// A Worker can make thousands of calls that fail, one record each, past the bound on the run's
// records (the security review's shape: 12,000 calls refused for bad arguments, every one of them
// wanted): their records are read in one pass, and ReadLedger stays as quick as without them, not a
// scan of the run's records per attempt. Past the bound on that pass, an attempt whose record was not
// read is not known, and Cut says so.
func TestReadLedgerReadsTheAttemptsInOnePass(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(ctx, filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := l.Tx(ctx, func(q *db.Queries) error {
		return q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "run-1", Task: "security-audit", ParamsJson: "{}",
			BudgetLimitMicros: 1, CreatedAt: ledger.Now()})
	}); err != nil {
		t.Fatal(err)
	}
	run := setExecution(t, l, "run-1", "exec-1")
	const calls = 12000
	var want Wanted
	for done := 0; done < calls; {
		if err := l.Tx(ctx, func(q *db.Queries) error {
			for i := 0; i < 1000 && done < calls; i, done = i+1, done+1 {
				p := map[string]any{"activity_id": fmt.Sprintf("%d:x", done), "attempt": 1, "kind": "tool", "name": "x",
					"status": "error", "error": "args_json must be a JSON object", "error_type": "BadArguments", "raw_args_sha256": "a"}
				if _, err := audit.Append(ctx, q, audit.Event{WorkflowID: "run-1", Type: "call", Actor: audit.Run, Payload: p}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < calls; i++ {
		want.Attempts = append(want.Attempts, Attempt{ActivityID: fmt.Sprintf("%d:x", i), Attempt: 1})
	}
	read := func(want Wanted) (Ledger, time.Duration) {
		start := time.Now()
		var got Ledger
		if err := l.Read(ctx, func(q *db.Queries) error {
			var err error
			got, err = ReadLedger(ctx, q, run, want)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return got, time.Since(start)
	}
	bare, without := read(Wanted{})
	got, took := read(want)
	t.Logf("%d failed calls: ReadLedger took %v, %v without them wanted", calls, took, without)
	if !bare.RecordsCut || !got.RecordsCut || got.AttemptsCut || len(got.Records)+len(got.Named) != calls ||
		len(got.AttemptErr) != 0 || took > time.Second {
		t.Fatalf("%d records, %d named, %d unread, attempts cut %v, in %v", len(got.Records), len(got.Named), len(got.AttemptErr),
			got.AttemptsCut, took)
	}

	// The pass's own bound, to the record: 7,000 of the run's call records are past its first 5,000.
	defer func(n int) { maxAfterCut = n }(maxAfterCut)
	for bound, cut := range map[int]bool{calls - maxRecords: false, calls - maxRecords - 1: true} {
		maxAfterCut = bound
		if got, _ = read(want); got.AttemptsCut != cut || len(got.Records)+len(got.Named) != calls-btoi(cut) {
			t.Fatalf("bound %d: attempts cut %v, %d records, %d named", bound, got.AttemptsCut, len(got.Records), len(got.Named))
		}
	}
	maxAfterCut = 3
	got, _ = read(Wanted{Attempts: []Attempt{{ActivityID: "5000:x", Attempt: 1}, {ActivityID: "5003:x", Attempt: 1},
		{ActivityID: "1:x", Attempt: 1}}})
	if !got.AttemptsCut || len(got.Named) != 1 || got.AttemptErr[Attempt{ActivityID: "5003:x", Attempt: 1}] == "" ||
		got.AttemptErr[Attempt{ActivityID: "5000:x", Attempt: 1}] != "" ||
		!slices.Contains(got.CutWhat, "call records past the first 3 after those") ||
		!slices.ContainsFunc(got.Cut, func(c string) bool { return strings.Contains(c, "more than 3 call records after its first") }) {
		t.Fatalf("named %v, unread %v, cut %q", got.Named, got.AttemptErr, got.Cut)
	}
}

// setExecution records execution as the run's in Temporal ("" for none yet), as StartRun does, and
// returns the run's row.
func setExecution(t *testing.T, l *ledger.Ledger, workflowID, execution string) db.Run {
	t.Helper()
	id := sql.NullString{String: execution, Valid: execution != ""}
	if _, err := l.DB.Exec(`UPDATE runs SET run_id = ? WHERE workflow_id = ?`, id, workflowID); err != nil {
		t.Fatal(err)
	}
	r, err := l.Q.GetRun(context.Background(), workflowID)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// The run judged is the run in the snapshot: one whose row names another execution or task now, or
// none, is not read (the caller read Temporal for the other).
func TestReadLedgerHoldsTheSnapshotToTheRunJudged(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(ctx, filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := l.Tx(ctx, func(q *db.Queries) error {
		return q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "run-1", Task: "security-audit", ParamsJson: "{}",
			BudgetLimitMicros: 1, CreatedAt: ledger.Now()})
	}); err != nil {
		t.Fatal(err)
	}
	judged := setExecution(t, l, "run-1", "exec-1")
	read := func(run db.Run) error {
		return l.Read(ctx, func(q *db.Queries) error {
			_, err := ReadLedger(ctx, q, run, Wanted{})
			return err
		})
	}
	if err := read(judged); err != nil {
		t.Fatal(err)
	}
	other := judged
	other.Task = "another"
	gone := judged
	gone.WorkflowID = "run-2"
	for name, run := range map[string]db.Run{"another task": other, "no such run": gone} {
		if err := read(run); err == nil {
			t.Errorf("%s: read", name)
		}
	}
	setExecution(t, l, "run-1", "exec-2")
	if err := read(judged); !errors.Is(err, ErrRunChanged) {
		t.Errorf("another execution: %v", err)
	}
}
