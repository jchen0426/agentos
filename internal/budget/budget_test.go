package budget

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

var alice = audit.Who{Actor: audit.Operator("alice"), PeerUID: sql.NullInt64{Int64: 501, Valid: true}}

// records are the audit log's records, in order.
// records are the budget's records of type typ.
func records(t *testing.T, b *Budget, typ string) []db.AuditLog {
	t.Helper()
	rows, err := b.Ledger.Q.ListAuditRecords(context.Background(), db.ListAuditRecordsParams{Seq: 1, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	return slices.DeleteFunc(rows, func(r db.AuditLog) bool { return r.Type != typ })
}

func newRun(t *testing.T, limit int64) (*Budget, string) {
	t.Helper()
	ctx := context.Background()
	l, err := ledger.Open(ctx, filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := l.Q.InsertRun(ctx, db.InsertRunParams{
		WorkflowID: "wf", Task: "security-audit", ParamsJson: "{}", BudgetLimitMicros: limit, CreatedAt: ledger.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	return &Budget{Ledger: l}, "wf"
}

// settle and release are Settle and Release in a transaction of their own, as agentd's writes are.
func settle(ctx context.Context, b *Budget, requestID string, amount int64, model, priceVersion, reason string) (db.BudgetEntry, error) {
	var e db.BudgetEntry
	err := b.Ledger.Tx(ctx, func(q *db.Queries) error {
		var err error
		e, err = Settle(ctx, q, requestID, amount, model, priceVersion, reason)
		return err
	})
	return e, err
}

func release(ctx context.Context, b *Budget, requestID string) error {
	return b.Ledger.Tx(ctx, func(q *db.Queries) error { return Release(ctx, q, requestID) })
}

func req(id string) Request {
	return Request{ID: id, WorkflowID: "wf", ActivityID: "1:audit_v1", Attempt: 1, Model: "fake/cheap"}
}

func want(t *testing.T, b *Budget, w Totals) {
	t.Helper()
	got, err := b.Totals(context.Background(), "wf")
	if err != nil || got != w {
		t.Fatalf("totals %+v, %v; want %+v", got, err, w)
	}
}

func TestReserveSettleReleaseExtend(t *testing.T) {
	ctx := context.Background()
	b, _ := newRun(t, 100)
	if _, err := b.Reserve(ctx, req("r1"), 60, "v1"); err != nil {
		t.Fatal(err)
	}
	if got, err := b.Reserve(ctx, req("r2"), 50, "v1"); !errors.Is(err, ErrExhausted) || got.Available() != 40 {
		t.Fatalf("over the limit: %+v, %v", got, err)
	}
	want(t, b, Totals{Limit: 100, Held: 60})

	// Settling at the actual cost frees the rest of the reservation.
	if _, err := settle(ctx, b, "r1", 15, "fake/cheap", "v1", ReasonUsage); err != nil {
		t.Fatal(err)
	}
	want(t, b, Totals{Limit: 100, Spent: 15})
	if _, err := settle(ctx, b, "r1", 15, "fake/cheap", "v1", ReasonUsage); err == nil {
		t.Error("a reservation was settled twice")
	}
	if _, err := settle(ctx, b, "never-reserved", 1, "", "", ReasonUsage); err == nil {
		t.Error("settled a request with no reservation")
	}

	if _, err := b.Reserve(ctx, req("r2"), 50, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := release(ctx, b, "r2"); err != nil {
		t.Fatal(err)
	}
	if err := release(ctx, b, "r2"); err == nil {
		t.Error("released twice")
	}
	want(t, b, Totals{Limit: 100, Spent: 15})

	for i, key := range []string{"k1", "k1", "k2"} {
		added, err := b.Extend(ctx, "wf", 1000, key, alice)
		if err != nil || added != (i != 1) {
			t.Fatalf("extend %d (%s): added=%v, %v", i, key, added, err)
		}
	}
	want(t, b, Totals{Limit: 2100, Spent: 15})
	if _, err := b.Extend(ctx, "wf", 0, "k3", alice); err == nil {
		t.Error("extended by zero")
	}
	// Each extension that added to the limit is recorded, by whoever asked for it; the one sent
	// again, which added nothing, is not.
	rows := records(t, b, "budget.extended")
	if len(rows) != 2 {
		t.Fatalf("%d records, want the 2 extensions: %+v", len(rows), rows)
	}
	for i, r := range rows {
		if r.Type != "budget.extended" || r.WorkflowID != "wf" || r.Actor != "operator:alice" || r.PeerUid != alice.PeerUID ||
			r.Payload != fmt.Sprintf(`{"amount_micros":1000,"idempotency_key":"k%d"}`, i+1) {
			t.Errorf("record %d: %+v", r.Seq, r)
		}
	}
}

// The extension and its record are one transaction: without its record, there is no extension.
func TestAnExtensionNeedsItsRecord(t *testing.T) {
	ctx := context.Background()
	b, _ := newRun(t, 100)
	if _, err := b.Extend(ctx, "wf", 1000, "k1", audit.Who{}); err == nil {
		t.Fatal("an extension by nobody was made")
	}
	want(t, b, Totals{Limit: 100})
	if rows := records(t, b, "budget.extended"); len(rows) != 0 {
		t.Errorf("records: %+v", rows)
	}
}

func TestConcurrentReservationsNeverOverspend(t *testing.T) {
	ctx := context.Background()
	b, _ := newRun(t, 1000)
	var wg sync.WaitGroup
	var mu sync.Mutex
	granted := 0
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := b.Reserve(ctx, req(fmt.Sprint("r", i)), 70, "v1")
			switch {
			case err == nil:
				mu.Lock()
				granted++
				mu.Unlock()
			case !errors.Is(err, ErrExhausted):
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if granted != 1000/70 {
		t.Errorf("%d reservations of 70 granted against 1000, want %d", granted, 1000/70)
	}
	want(t, b, Totals{Limit: 1000, Held: int64(granted) * 70})
}

func TestSettleOpenChargesInFull(t *testing.T) {
	ctx := context.Background()
	b, _ := newRun(t, 1000)
	for _, id := range []string{"open1", "open2", "settled"} {
		if _, err := b.Reserve(ctx, req(id), 100, "v1"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := settle(ctx, b, "settled", 10, "fake/cheap", "v1", ReasonUsage); err != nil {
		t.Fatal(err)
	}
	n, err := b.SettleOpen(ctx)
	if err != nil || n != 2 {
		t.Fatalf("SettleOpen = %d, %v; want 2", n, err)
	}
	want(t, b, Totals{Limit: 1000, Spent: 210})
	if n, _ := b.SettleOpen(ctx); n != 0 {
		t.Errorf("second SettleOpen settled %d more", n)
	}
	// agentd's own record, once for the run: the second found nothing to settle.
	rows := records(t, b, "budget.settled_open")
	if len(rows) != 1 || rows[0].Type != "budget.settled_open" || rows[0].Actor != audit.Agentd || rows[0].WorkflowID != "wf" ||
		rows[0].Payload != `{"amount_micros":200,"count":2}` || rows[0].PeerUid.Valid {
		t.Errorf("records: %+v", rows)
	}
}

// Settling the reservations left open goes with its record, or does not happen: they stay open, for
// the next start to settle.
func TestSettlingWhatWasLeftOpenNeedsItsRecord(t *testing.T) {
	ctx := context.Background()
	b, _ := newRun(t, 1000)
	if _, err := b.Reserve(ctx, req("open"), 100, "v1"); err != nil {
		t.Fatal(err)
	}
	b.Ledger.UncheckedSchema = true // the trigger is the test's
	if _, err := b.Ledger.DB.Exec(`CREATE TRIGGER fail_appends BEFORE INSERT ON audit_log
		BEGIN SELECT RAISE(ABORT, 'no room for the record'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := b.SettleOpen(ctx); err == nil {
		t.Fatal("settled without its record")
	}
	want(t, b, Totals{Limit: 1000, Held: 100})
	if _, err := b.Ledger.DB.Exec(`DROP TRIGGER fail_appends`); err != nil {
		t.Fatal(err)
	}
	if n, err := b.SettleOpen(ctx); err != nil || n != 1 {
		t.Fatalf("SettleOpen = %d, %v", n, err)
	}
	want(t, b, Totals{Limit: 1000, Spent: 100})
	if rows := records(t, b, "budget.settled_open"); len(rows) != 1 || rows[0].Type != "budget.settled_open" {
		t.Errorf("records: %+v", rows)
	}
}

// Each run charged at start has a record of its own: the requests left open and what they cost.
func TestEachRunChargedAtStartHasItsRecord(t *testing.T) {
	ctx := context.Background()
	b, _ := newRun(t, 1000)
	if err := b.Ledger.Q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "wf2", Task: "security-audit", ParamsJson: "{}",
		BudgetLimitMicros: 1000, CreatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	for _, r := range []Request{req("a1"), req("a2"), {ID: "b1", WorkflowID: "wf2", ActivityID: "1:x", Attempt: 1, Model: "m"}} {
		if _, err := b.Reserve(ctx, r, map[string]int64{"a1": 30, "a2": 40, "b1": 50}[r.ID], "v1"); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := b.SettleOpen(ctx); err != nil || n != 3 {
		t.Fatalf("SettleOpen = %d, %v", n, err)
	}
	var got []string
	for _, r := range records(t, b, "budget.settled_open") {
		got = append(got, r.WorkflowID+" "+r.Payload)
	}
	if want := []string{`wf {"amount_micros":70,"count":2}`, `wf2 {"amount_micros":50,"count":1}`}; !slices.Equal(got, want) {
		t.Errorf("records %q, want %q", got, want)
	}
}

// Each entry of the budget has its record in its transaction: the reservation, and the charge or
// the release that closes it, each with what the entry holds.
func TestEachBudgetEntryHasItsRecord(t *testing.T) {
	ctx := context.Background()
	b, _ := newRun(t, 1000)
	if _, err := b.Reserve(ctx, req("r1"), 60, "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Reserve(ctx, req("r2"), 50, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := b.Ledger.Tx(ctx, func(q *db.Queries) error {
		if _, err := Settle(ctx, q, "r1", 15, "fake/cheap", "v1", ReasonUsage); err != nil {
			return err
		}
		return Release(ctx, q, "r2")
	}); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, typ := range []string{"budget.reserved", "budget.settled", "budget.released"} {
		for _, r := range records(t, b, typ) {
			got = append(got, r.Type+" "+r.Actor+" "+r.WorkflowID+" "+r.Payload)
		}
	}
	want := []string{
		`budget.reserved agentd wf {"activity_id":"1:audit_v1","amount_micros":60,"attempt":1,"model":"fake/cheap","price_table_version":"v1","request_id":"r1"}`,
		`budget.reserved agentd wf {"activity_id":"1:audit_v1","amount_micros":50,"attempt":1,"model":"fake/cheap","price_table_version":"v1","request_id":"r2"}`,
		`budget.settled agentd wf {"amount_micros":15,"model":"fake/cheap","price_table_version":"v1","reason":"usage","request_id":"r1"}`,
		`budget.released agentd wf {"reason":"not_sent","request_id":"r2"}`,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("records\n%q\nwant\n%q", got, want)
	}
	// A reservation whose record cannot be written is not made.
	b.Ledger.UncheckedSchema = true // the trigger is the test's
	if _, err := b.Ledger.DB.Exec(`CREATE TRIGGER fail_appends BEFORE INSERT ON audit_log WHEN NEW.type = 'budget.reserved'
		BEGIN SELECT RAISE(ABORT, 'no room for the record'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Reserve(ctx, req("r3"), 10, "v1"); err == nil {
		t.Fatal("reserved without its record")
	}
	var n int
	if err := b.Ledger.DB.QueryRow(`SELECT COUNT(*) FROM budget_entries WHERE request_id = 'r3'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d entries of r3, %v", n, err)
	}
}

// An extension the ledger drops (a trigger that makes it do nothing) is not taken for one made
// under its key before: the extension fails, with no record.
func TestADroppedExtensionIsNotTakenAsMade(t *testing.T) {
	ctx := context.Background()
	b, wf := newRun(t, 100)
	b.Ledger.UncheckedSchema = true // the trigger is the test's
	if _, err := b.Ledger.DB.Exec(`CREATE TRIGGER dropped BEFORE INSERT ON budget_entries BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
	if added, err := b.Extend(ctx, wf, 50, "k", audit.Who{Actor: audit.Operator("tester")}); added || !errors.Is(err, ledger.ErrWriteDropped) {
		t.Fatalf("added %v, err %v", added, err)
	}
	if got := records(t, b, "budget.extended"); len(got) != 0 {
		t.Fatalf("records %+v", got)
	}
}
