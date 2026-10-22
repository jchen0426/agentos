package audit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// The chain's end as this process left it. One agentd at a time keeps a data directory's ledger,
// and all its appends go through Append, so the last record it appended is the chain's last, and
// every head it pushed to Temporal is a record the chain holds. A ledger that no longer ends there
// was cut, rewritten or replaced under agentd, or written by someone else: Append refuses to chain
// onto it, Push to push its head, and VerifyAudit reports it (Held). Records appended after the
// last push, before agentd stopped, have no anchor anywhere: what cuts them while agentd is down,
// nothing tells.

// ErrChainMoved is what Append and Push fail with on a chain that no longer ends where this process
// left it.
var ErrChainMoved = errors.New("the audit log no longer ends where this agentd left it")

type tip struct {
	mu        sync.Mutex
	appended  Head // the last record this process committed; zero before its first
	pending   Head // the last record appended in transaction pendingTx, which has not committed yet
	pendingTx uint64
	pushed    Head  // the last head this process pushed to Temporal
	moved     error // why this process found the chain moved under it, the first time; nil until then
	onMoved   func(error)
	told      bool
}

var tips sync.Map // *ledger.Ledger -> *tip

func tipOf(l *ledger.Ledger) *tip {
	t, _ := tips.LoadOrStore(l, &tip{})
	return t.(*tip)
}

// OnChainMoved has fn hear, the first time it happens, that l's chain no longer ends where this
// process left it.
func OnChainMoved(l *ledger.Ledger, fn func(error)) {
	t := tipOf(l)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onMoved = fn
}

// Tip is the last record this process appended to l's chain, and the last head it pushed to
// Temporal: zero heads before the first of each.
func Tip(l *ledger.Ledger) (appended, pushed Head) {
	t := tipOf(l)
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.appended, t.pushed
}

// Moved is why this process found l's chain no longer ending where it left it, the first time (an
// append, or a push, it refused); nil while it has not.
func Moved(l *ledger.Ledger) error {
	t := tipOf(l)
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.moved
}

// Held says how q's ledger does not hold the records heads names (zero heads are none): nil when it
// holds each, by its seq and sha256.
func Held(ctx context.Context, q *db.Queries, heads ...Head) error {
	for _, h := range heads {
		if h == (Head{}) {
			continue
		}
		rows, err := q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: h.Seq, Limit: 1})
		switch {
		case err != nil:
			return err
		case len(rows) == 0 || rows[0].Seq != h.Seq:
			return fmt.Errorf("%w: it has no record %d (sha256 %s)", ErrChainMoved, h.Seq, h.Sha256)
		case rows[0].Sha256 != h.Sha256:
			return fmt.Errorf("%w: its record %d has sha256 %s, not %s", ErrChainMoved, h.Seq, rows[0].Sha256, h.Sha256)
		}
	}
	return nil
}

// onto checks, as tx appends after last, that the chain still ends where this process left it:
// last is the last record it appended, in tx or before it, and the head it pushed last is there.
func (t *tip) onto(ctx context.Context, q *db.Queries, tx *ledger.TxInfo, last Head) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	want := t.appended
	if t.pendingTx == tx.ID {
		want = t.pending
	}
	err := Held(ctx, q, t.pushed)
	if err == nil && want != (Head{}) && last != want {
		err = fmt.Errorf("%w: its last record is %d (sha256 %s), and the last this agentd appended was %d (sha256 %s)",
			ErrChainMoved, last.Seq, last.Sha256, want.Seq, want.Sha256)
	}
	if err != nil && errors.Is(err, ErrChainMoved) {
		err = fmt.Errorf("%w: the log was cut, rewritten or replaced under agentd, or written by someone else; "+
			"agentd appends nothing more to it", err)
		t.tell(err)
	}
	return err
}

// appending notes h, appended in tx: the last record of the chain once tx commits.
func (t *tip) appending(tx *ledger.TxInfo, h Head) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pendingTx != tx.ID {
		tx.OnCommit(func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			if t.pendingTx == tx.ID {
				t.appended = t.pending
			}
		})
	}
	t.pending, t.pendingTx = h, tx.ID
}

// head is the chain's head, to push: in a transaction, so that no append of this process moves it
// meanwhile, and only while the chain ends where this process left it.
func (t *tip) head(ctx context.Context, l *ledger.Ledger) (Head, error) {
	var head Head
	err := l.Tx(ctx, func(q *db.Queries) error {
		last, err := q.LastAuditRecord(ctx)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return err
		default:
			head = Head{Seq: last.Seq, Sha256: last.Sha256}
		}
		t.mu.Lock()
		appended, pushed := t.appended, t.pushed
		t.mu.Unlock()
		err = Held(ctx, q, pushed)
		if err == nil && appended != (Head{}) && head != appended {
			err = fmt.Errorf("%w: its last record is %d (sha256 %s), and the last this agentd appended was %d (sha256 %s)",
				ErrChainMoved, head.Seq, head.Sha256, appended.Seq, appended.Sha256)
		}
		if err != nil && errors.Is(err, ErrChainMoved) {
			err = fmt.Errorf("%w: agentd pushes no head of it", err)
			t.mu.Lock()
			t.tell(err)
			t.mu.Unlock()
		}
		return err
	})
	return head, err
}

// tell records err, the first time (Moved), and has onMoved hear it, the first time it is set; t.mu is
// held.
func (t *tip) tell(err error) {
	if t.moved == nil {
		t.moved = err
	}
	if !t.told && t.onMoved != nil {
		t.told = true
		t.onMoved(err)
	}
}
