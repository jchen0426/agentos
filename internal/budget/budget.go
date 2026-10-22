// Package budget keeps each run's spending under its limit (design doc, section 9). Every
// network request that may be billed first reserves the most it can cost; the reservation
// is then settled with what the provider billed, or released if the request never left
// agentd. At any moment: settled + open reservations <= the run's current limit.
package budget

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/outbox"
)

// ErrExhausted means the run cannot afford a reservation; nothing was recorded.
var ErrExhausted = errors.New("budget exhausted")

// Settlement reasons.
const (
	ReasonUsage          = "usage"           // what the provider reported it billed
	ReasonRejected       = "rejected"        // the provider answered 429 or 5xx: nothing billed
	ReasonOutcomeUnknown = "outcome_unknown" // no answer: the provider may have billed it all
	ReasonOpenAtStart    = "open_at_start"   // agentd stopped before settling it
	ReasonNotSent        = "not_sent"        // a release: the request never left agentd
)

type Budget struct {
	Ledger *ledger.Ledger
}

// Totals is where a run's budget stands.
type Totals struct {
	Limit int64 // the start's limit plus every extension
	Spent int64 // settled
	Held  int64 // reserved, not yet settled
}

func (t Totals) Available() int64 { return t.Limit - t.Spent - t.Held }

// Request is one network request that may be billed.
type Request struct {
	ID         string // unique per request
	WorkflowID string
	ActivityID string
	Attempt    int32
	Model      string // "<provider>/<model>" asked for
}

func (b *Budget) Totals(ctx context.Context, workflowID string) (Totals, error) {
	return totals(ctx, b.Ledger.Q, workflowID)
}

// TotalsIn is Totals as q reads them: in a transaction of the caller's.
func TotalsIn(ctx context.Context, q *db.Queries, workflowID string) (Totals, error) {
	return totals(ctx, q, workflowID)
}

func totals(ctx context.Context, q *db.Queries, workflowID string) (Totals, error) {
	t, err := q.GetBudgetTotals(ctx, workflowID)
	if err != nil {
		return Totals{}, err
	}
	return Totals{Limit: t.LimitMicros, Spent: t.SpentMicros, Held: t.HeldMicros}, nil
}

// Reserve holds amount for r, priced by priceVersion. It returns ErrExhausted, and the
// totals that refused it, when the run cannot afford it. The check and the write are one
// IMMEDIATE transaction, so concurrent reservations never share the same headroom.
func (b *Budget) Reserve(ctx context.Context, r Request, amount int64, priceVersion string) (Totals, error) {
	var t Totals
	err := b.Ledger.Tx(ctx, func(q *db.Queries) error {
		var err error
		if t, err = totals(ctx, q, r.WorkflowID); err != nil {
			return err
		}
		if t.Available() < amount {
			return ErrExhausted
		}
		t.Held += amount
		if err := q.InsertBudgetEntry(ctx, db.InsertBudgetEntryParams{
			WorkflowID: r.WorkflowID, Kind: "RESERVE", AmountMicros: amount,
			RequestID:         sql.NullString{String: r.ID, Valid: true},
			ActivityID:        sql.NullString{String: r.ActivityID, Valid: true},
			Attempt:           sql.NullInt64{Int64: int64(r.Attempt), Valid: true},
			Model:             sql.NullString{String: r.Model, Valid: true},
			PriceTableVersion: sql.NullString{String: priceVersion, Valid: true},
			CreatedAt:         ledger.Now(),
		}); err != nil {
			return err
		}
		// Each entry of the budget has its record in its transaction: a reservation or a charge
		// removed or changed would give the run another's room, and is found.
		_, err = audit.Append(ctx, q, audit.Event{WorkflowID: r.WorkflowID, Type: "budget.reserved", Actor: audit.Agentd,
			Payload: map[string]any{"request_id": audit.Text(r.ID), "activity_id": audit.Text(r.ActivityID), "attempt": r.Attempt,
				"model": r.Model, "amount_micros": amount, "price_table_version": priceVersion}})
		return err
	})
	return t, err
}

// Settle closes requestID's reservation with what was billed: amount, by model. It writes
// through q, so that a caller can settle in the same transaction as its own record of the
// answer. It returns the reservation it closed.
func Settle(ctx context.Context, q *db.Queries, requestID string, amount int64, model, priceVersion, reason string) (db.BudgetEntry, error) {
	res, err := q.GetOpenReservation(ctx, sql.NullString{String: requestID, Valid: true})
	if err != nil {
		return db.BudgetEntry{}, fmt.Errorf("settle %s: no open reservation: %w", requestID, err)
	}
	if err := q.InsertBudgetEntry(ctx, db.InsertBudgetEntryParams{
		WorkflowID: res.WorkflowID, Kind: "SETTLE", AmountMicros: amount, RequestID: res.RequestID,
		Model:             sql.NullString{String: model, Valid: model != ""},
		PriceTableVersion: sql.NullString{String: priceVersion, Valid: priceVersion != ""},
		Reason:            sql.NullString{String: reason, Valid: true},
		CreatedAt:         ledger.Now(),
	}); err != nil {
		return res, err
	}
	_, err = audit.Append(ctx, q, audit.Event{WorkflowID: res.WorkflowID, Type: "budget.settled", Actor: audit.Agentd,
		Payload: map[string]any{"request_id": audit.Text(res.RequestID.String), "amount_micros": amount, "model": model,
			"price_table_version": priceVersion, "reason": reason}})
	return res, err
}

// Release closes requestID's reservation without a charge: the request never left agentd.
func Release(ctx context.Context, q *db.Queries, requestID string) error {
	res, err := q.GetOpenReservation(ctx, sql.NullString{String: requestID, Valid: true})
	if err != nil {
		return fmt.Errorf("release %s: no open reservation: %w", requestID, err)
	}
	if err := q.InsertBudgetEntry(ctx, db.InsertBudgetEntryParams{
		WorkflowID: res.WorkflowID, Kind: "RELEASE", RequestID: res.RequestID,
		Reason:    sql.NullString{String: ReasonNotSent, Valid: true},
		CreatedAt: ledger.Now(),
	}); err != nil {
		return err
	}
	_, err = audit.Append(ctx, q, audit.Event{WorkflowID: res.WorkflowID, Type: "budget.released", Actor: audit.Agentd,
		Payload: map[string]any{"request_id": audit.Text(res.RequestID.String), "reason": ReasonNotSent}})
	return err
}

// Extend raises a run's limit by amount, and rings the run's doorbell in the same transaction:
// calls paused in PAUSED_BUDGET ask again (design doc, section 10, the outbox). The idempotency
// key makes a retried request a no-op; Extend reports whether this call added to the limit. The
// audit log records the extension, by the operator who asked for it (by), in the same transaction.
func (b *Budget) Extend(ctx context.Context, workflowID string, amount int64, idempotencyKey string, by audit.Who) (bool, error) {
	if amount <= 0 {
		return false, fmt.Errorf("extension of %d micros", amount)
	}
	var added bool
	err := b.Ledger.Tx(ctx, func(q *db.Queries) error {
		n, err := q.InsertBudgetExtension(ctx, db.InsertBudgetExtensionParams{
			WorkflowID: workflowID, AmountMicros: amount,
			IdempotencyKey: sql.NullString{String: idempotencyKey, Valid: true}, CreatedAt: ledger.Now(),
		})
		if added = n == 1; err != nil {
			return err
		}
		if !added { // under that key already: the row is there, or the write was dropped
			there, err := q.BudgetExtended(ctx, db.BudgetExtendedParams{WorkflowID: workflowID,
				IdempotencyKey: sql.NullString{String: idempotencyKey, Valid: true}})
			if err == nil && there == 0 {
				err = fmt.Errorf("%w: the extension of run %s under key %q", ledger.ErrWriteDropped, workflowID, idempotencyKey)
			}
			return err
		}
		if _, err := audit.Append(ctx, q, audit.Event{WorkflowID: workflowID, Type: "budget.extended", Actor: by.Actor,
			PeerUID: by.PeerUID, Payload: map[string]any{"amount_micros": amount, "idempotency_key": idempotencyKey}}); err != nil {
			return err
		}
		return outbox.Ring(ctx, q, workflowID, outbox.WhyBudget)
	})
	return added && err == nil, err
}

// SettleOpen charges every open reservation in full. agentd calls it at start, before it
// serves any request: a reservation still open then belongs to a request whose fate is
// unknown, and the provider may have billed it.
func (b *Budget) SettleOpen(ctx context.Context) (int, error) {
	var n int
	err := b.Ledger.Tx(ctx, func(q *db.Queries) error {
		open, err := q.ListOpenReservations(ctx)
		if err != nil {
			return err
		}
		type charged struct{ count, micros int64 }
		byRun := map[string]*charged{}
		for _, r := range open {
			if _, err := Settle(ctx, q, r.RequestID.String, r.AmountMicros, r.Model.String, r.PriceTableVersion.String, ReasonOpenAtStart); err != nil {
				return err
			}
			if byRun[r.WorkflowID] == nil {
				byRun[r.WorkflowID] = &charged{}
			}
			byRun[r.WorkflowID].count++
			byRun[r.WorkflowID].micros += r.AmountMicros
		}
		n = len(open)
		// One record for each run charged: what it was charged, and for how many requests.
		for _, wf := range slices.Sorted(maps.Keys(byRun)) {
			if _, err := audit.Append(ctx, q, audit.Event{WorkflowID: wf, Type: "budget.settled_open", Actor: audit.Agentd,
				Payload: map[string]any{"count": byRun[wf].count, "amount_micros": byRun[wf].micros}}); err != nil {
				return err
			}
		}
		return nil
	})
	return n, err
}
