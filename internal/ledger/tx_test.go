package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jchen0426/agentos/internal/ledger/db"
)

func openLedger(t *testing.T) *Ledger {
	t.Helper()
	l, err := Open(context.Background(), filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func blobs(t *testing.T, l *Ledger) int {
	t.Helper()
	var n int
	if err := l.DB.QueryRow(`SELECT COUNT(*) FROM blobs`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Hundreds of write transactions at once all commit: they take turns in the process, where left to
// SQLite's busy handler some waited past busy_timeout and failed with SQLITE_BUSY.
func TestWriteTransactionsTakeTurns(t *testing.T) {
	l := openLedger(t)
	var wg sync.WaitGroup
	errs := make(chan error, 400)
	start := time.Now()
	for i := range 400 {
		wg.Go(func() {
			errs <- l.Tx(context.Background(), func(q *db.Queries) error {
				return q.PutBlob(context.Background(), db.PutBlobParams{Sha256: fmt.Sprint(i), Bytes: []byte{byte(i)}})
			})
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := blobs(t, l); n != 400 {
		t.Fatalf("%d of 400 transactions committed", n)
	}
	t.Logf("400 at once: %v", time.Since(start))
}

// A transaction waiting for its turn waits as long as its context, no longer.
func TestATransactionWaitsForItsTurnAsLongAsItsContext(t *testing.T) {
	l := openLedger(t)
	holding, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- l.Tx(context.Background(), func(*db.Queries) error {
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	ran := false
	if err := l.Tx(ctx, func(*db.Queries) error { ran = true; return nil }); !errors.Is(err, context.DeadlineExceeded) || ran {
		t.Fatalf("got %v (ran %v), want it to give up waiting", err, ran)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// The turn is free again.
	if err := l.Tx(context.Background(), func(*db.Queries) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

// A read sees one snapshot, and holds up no writer meanwhile.
func TestAReadIsOneSnapshot(t *testing.T) {
	l := openLedger(t)
	put := func(name string) error {
		return l.Tx(context.Background(), func(q *db.Queries) error {
			return q.PutBlob(context.Background(), db.PutBlobParams{Sha256: name, Bytes: []byte(name)})
		})
	}
	if err := put("first"); err != nil {
		t.Fatal(err)
	}
	var before, after int
	err := l.Read(context.Background(), func(q *db.Queries) error {
		if _, err := q.GetBlob(context.Background(), "first"); err != nil { // the snapshot is taken here
			return err
		}
		before = blobsIn(t, q)
		written := make(chan error, 1)
		go func() { written <- put("second") }()
		select {
		case err := <-written:
			if err != nil {
				return err
			}
		case <-time.After(5 * time.Second):
			return errors.New("a write waited for the read")
		}
		after = blobsIn(t, q)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if before != 1 || after != 1 || blobs(t, l) != 2 {
		t.Fatalf("the read saw %d, then %d; the ledger has %d", before, after, blobs(t, l))
	}
}

func blobsIn(t *testing.T, q *db.Queries) int {
	t.Helper()
	n := 0
	for _, name := range []string{"first", "second"} {
		if _, err := q.GetBlob(context.Background(), name); err == nil {
			n++
		}
	}
	return n
}

// A function that panics inside Tx leaves nothing behind: its writes are rolled back, the write lock
// and the turn given back, and the panic goes on up.
func TestAPanicInATransactionRollsItBack(t *testing.T) {
	l := openLedger(t)
	func() {
		defer func() {
			if r := recover(); r != "a bug" {
				t.Fatalf("recovered %v, want the panic", r)
			}
		}()
		l.Tx(context.Background(), func(q *db.Queries) error {
			if err := q.PutBlob(context.Background(), db.PutBlobParams{Sha256: "lost", Bytes: []byte("x")}); err != nil {
				t.Fatal(err)
			}
			panic("a bug")
		})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Tx(ctx, func(q *db.Queries) error {
		return q.PutBlob(ctx, db.PutBlobParams{Sha256: "kept", Bytes: []byte("y")})
	}); err != nil {
		t.Fatalf("the next transaction: %v", err)
	}
	// Another connection's writer finds the lock free too.
	if _, err := l.DB.ExecContext(ctx, `INSERT INTO blobs (sha256, bytes) VALUES ('beside', x'00')`); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Q.GetBlob(context.Background(), "lost"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("the panicked transaction's write: %v", err)
	}
}

// A blob whose bytes no longer hash to its key is not read.
func TestABlobChangedSinceItWasStoredIsNotRead(t *testing.T) {
	l := openLedger(t)
	ref, err := l.PutBlob(context.Background(), []byte("the payload approved"))
	if err != nil {
		t.Fatal(err)
	}
	if b, err := l.GetBlob(context.Background(), ref); err != nil || string(b) != "the payload approved" {
		t.Fatalf("%q %v", b, err)
	}
	if _, err := l.DB.Exec(`UPDATE blobs SET bytes = ? WHERE sha256 = ?`, []byte("TAMPERED"), strings.TrimPrefix(ref, "sha256:")); err != nil {
		t.Fatal(err)
	}
	if b, err := l.GetBlob(context.Background(), ref); !errors.Is(err, ErrBadBlob) || b != nil {
		t.Fatalf("got %q, %v", b, err)
	}
}
