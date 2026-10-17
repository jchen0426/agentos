package budget

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

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
	if _, err := Settle(ctx, b.Ledger.Q, "r1", 15, "fake/cheap", "v1", ReasonUsage); err != nil {
		t.Fatal(err)
	}
	want(t, b, Totals{Limit: 100, Spent: 15})
	if _, err := Settle(ctx, b.Ledger.Q, "r1", 15, "fake/cheap", "v1", ReasonUsage); err == nil {
		t.Error("a reservation was settled twice")
	}
	if _, err := Settle(ctx, b.Ledger.Q, "never-reserved", 1, "", "", ReasonUsage); err == nil {
		t.Error("settled a request with no reservation")
	}

	if _, err := b.Reserve(ctx, req("r2"), 50, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := Release(ctx, b.Ledger.Q, "r2"); err != nil {
		t.Fatal(err)
	}
	if err := Release(ctx, b.Ledger.Q, "r2"); err == nil {
		t.Error("released twice")
	}
	want(t, b, Totals{Limit: 100, Spent: 15})

	for i, key := range []string{"k1", "k1", "k2"} {
		added, err := b.Extend(ctx, "wf", 1000, key)
		if err != nil || added != (i != 1) {
			t.Fatalf("extend %d (%s): added=%v, %v", i, key, added, err)
		}
	}
	want(t, b, Totals{Limit: 2100, Spent: 15})
	if _, err := b.Extend(ctx, "wf", 0, "k3"); err == nil {
		t.Error("extended by zero")
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
	if _, err := Settle(ctx, b.Ledger.Q, "settled", 10, "fake/cheap", "v1", ReasonUsage); err != nil {
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
}
