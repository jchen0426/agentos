package audit

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jchen0426/agentos/internal/ledger/db"
)

func refusal(i int) Event {
	return Event{Type: "runtime.refused", Actor: Agentd, Payload: map[string]any{"i": i}}
}

func payload(t *testing.T, r db.AuditLog) map[string]any {
	t.Helper()
	var p map[string]any
	if err := json.Unmarshal([]byte(r.Payload), &p); err != nil {
		t.Fatal(err)
	}
	return p
}

// An event that finds the queue full is dropped, and the next record says how many were; so is
// one that cannot be appended.
func TestAQueueSaysWhatItDropped(t *testing.T) {
	l := open(t)
	var failed []error
	q := NewQueue(l, 2, func(err error) { failed = append(failed, err) })
	for i := range 5 {
		q.Add(refusal(i)) // nothing takes them yet: three find it full
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { q.Run(ctx); close(done) }()
	cancel()
	<-done // what was queued is appended, though ctx ended
	got := records(t, l)
	if len(got) != 2 {
		t.Fatalf("%d records, want the two queued", len(got))
	}
	if first, second := payload(t, got[0]), payload(t, got[1]); first["dropped"] != float64(3) || first["i"] != float64(0) ||
		second["dropped"] != nil {
		t.Fatalf("records %s, %s", got[0].Payload, got[1].Payload)
	}

	l.UncheckedSchema = true // the trigger is the test's
	if _, err := l.DB.Exec(`CREATE TRIGGER fail_appends BEFORE INSERT ON audit_log
		BEGIN SELECT RAISE(ABORT, 'no room for the record'); END`); err != nil {
		t.Fatal(err)
	}
	q.append(context.Background(), refusal(5))
	if len(failed) != 1 || len(records(t, l)) != 2 {
		t.Fatalf("errors %v", failed)
	}
	if _, err := l.DB.Exec(`DROP TRIGGER fail_appends`); err != nil {
		t.Fatal(err)
	}
	q.append(context.Background(), refusal(6))
	if got := records(t, l); len(got) != 3 || payload(t, got[2])["dropped"] != float64(1) || payload(t, got[2])["i"] != float64(6) {
		t.Fatalf("records %v", got)
	}
}

// A queue's Run returns once its context ends, though the ledger does not answer: an append waits
// no longer than drainFor.
func TestAQueueDoesNotHoldUpTheShutdown(t *testing.T) {
	was := drainFor
	drainFor = 100 * time.Millisecond
	t.Cleanup(func() { drainFor = was })
	l := open(t)
	var said []error
	q := NewQueue(l, 4, func(err error) { said = append(said, err) })
	q.Add(refusal(0))
	q.Add(refusal(1))
	hold, release := make(chan struct{}), make(chan struct{})
	held := make(chan error, 1)
	go func() {
		held <- l.Tx(context.Background(), func(*db.Queries) error { close(hold); <-release; return nil })
	}()
	<-hold
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	start := time.Now()
	go func() { q.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run waited for the ledger")
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("Run took %v", took)
	}
	if len(said) == 0 || !strings.Contains(said[len(said)-1].Error(), "2 events were dropped") {
		t.Errorf("said %v", said)
	}
	close(release)
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	// What it could not append is counted: the next record says so.
	q.append(context.Background(), refusal(2))
	if got := records(t, l); len(got) != 1 || payload(t, got[0])["dropped"] != float64(2) {
		t.Fatalf("records %v", got)
	}
}

// An append that fails counts the drops it was to report with itself: the next record says all of
// them.
func TestAFailedAppendKeepsTheDropsBeforeIt(t *testing.T) {
	l := open(t)
	q := NewQueue(l, 1, nil)
	for i := range 3 {
		q.Add(refusal(i)) // the first is queued, the other two dropped
	}
	l.UncheckedSchema = true // the trigger is the test's
	if _, err := l.DB.Exec(`CREATE TRIGGER fail_appends BEFORE INSERT ON audit_log
		BEGIN SELECT RAISE(ABORT, 'no room for the record'); END`); err != nil {
		t.Fatal(err)
	}
	q.append(context.Background(), <-q.events) // with "dropped": 2, and it fails
	if _, err := l.DB.Exec(`DROP TRIGGER fail_appends`); err != nil {
		t.Fatal(err)
	}
	q.append(context.Background(), refusal(3))
	if got := records(t, l); len(got) != 1 || payload(t, got[0])["dropped"] != float64(3) {
		t.Fatalf("records %v", got)
	}
}
