package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tasks"
	"github.com/jchen0426/agentos/internal/tools"
)

// checkAudit checks the audit log against the ledger: the chain is whole, and the state its records
// imply (audit.Projection) is the ledger's, with no record that breaks the protocol. newFixture has
// every test's ledger checked so, at its end.
func checkAudit(t *testing.T, l *ledger.Ledger) {
	t.Helper()
	ctx := context.Background()
	rep, err := audit.Verify(ctx, l.Q)
	if err != nil {
		t.Fatal(err)
	}
	if rep.BrokenAt != 0 {
		t.Errorf("the audit log's chain breaks at %d: %s", rep.BrokenAt, rep.Broken)
	}
	var p audit.Projection
	if err := p.Read(ctx, l.Q); err != nil {
		t.Fatal(err)
	}
	diffs, err := p.Differences(ctx, l.Q, audit.Since{})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range diffs {
		t.Errorf("audit log: %s", d.Why)
	}
}

// failAppends makes every append of a record of typ fail, as a full disk would, until the returned
// func drops the trigger that does it.
func failAppends(t *testing.T, l *ledger.Ledger, typ string) (drop func()) {
	t.Helper()
	l.UncheckedSchema = true // the trigger is the test's
	if _, err := l.DB.Exec(fmt.Sprintf(`CREATE TRIGGER fail_appends BEFORE INSERT ON audit_log WHEN NEW.type = '%s'
		BEGIN SELECT RAISE(ABORT, 'no room for the record'); END`, typ)); err != nil {
		t.Fatal(err)
	}
	return func() {
		if _, err := l.DB.Exec(`DROP TRIGGER fail_appends`); err != nil {
			t.Fatal(err)
		}
	}
}

// snapshot is the ledger's state that changes go with their records: every row of these tables.
func snapshot(t *testing.T, l *ledger.Ledger) string {
	t.Helper()
	var b strings.Builder
	for _, table := range []string{"runs", "effects", "approvals", "outbox", "effect_resolutions", "calls", "budget_entries", "audit_log"} {
		rows, err := l.DB.Query("SELECT * FROM " + table + " ORDER BY rowid")
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&b, "%s %v\n", table, vals)
		}
		rows.Close()
	}
	return b.String()
}

// Section 8: each record is appended in the transaction of the change it records. A change whose
// record cannot be appended is not made: the ledger is as it was, and once the record can be
// appended, the same change is made with it.
func TestAChangeIsMadeWithItsRecordOrNotAtAll(t *testing.T) {
	ctx := context.Background()
	args := map[string]any{"n": 1}
	for _, c := range []struct {
		name, record string
		// prepare sets up, with the audit log working, and returns the change.
		prepare func(t *testing.T, f *fixture) func() error
	}{
		{"a write is recorded", "effect.recorded", func(t *testing.T, f *fixture) func() error {
			w := &writer{class: policy.EffectLookup}
			return func() error {
				res, err := f.g.record(ctx, f.run, w, w.Class(), 1, nil, f.req(), nil)
				return errors.Join(err, unexpected(res))
			}
		}},
		{"a write is recorded with its approval", "approval.created", func(t *testing.T, f *fixture) func() error {
			w := &writer{class: policy.EffectLookup}
			return func() error { return wantStatus(f.ask(t, w, args, 1, time.Hour))(StatusNeedsApproval) }
		}},
		{"a claim", "effect.claimed", func(t *testing.T, f *fixture) func() error {
			e := recorded(t, f, &writer{class: policy.EffectLookup})
			return func() error {
				_, ok, err := f.g.claim(ctx, e)
				if err == nil && !ok {
					return errors.New("not claimed")
				}
				return err
			}
		}},
		{"a request goes out", "effect.sending", func(t *testing.T, f *fixture) func() error {
			claimed := claimedEffect(t, f)
			return func() error { return f.g.proceed(claimed, nil)(ctx) }
		}},
		{"a claim given back", "effect.released", func(t *testing.T, f *fixture) func() error {
			claimed := claimedEffect(t, f)
			return func() error {
				n, err := f.g.giveBack(ctx, claimed, true)
				if err == nil && n != 1 {
					return errors.New("not given back")
				}
				return err
			}
		}},
		{"a write settled", "effect.settled", func(t *testing.T, f *fixture) func() error {
			claimed := claimedEffect(t, f)
			return func() error {
				res, err := f.g.settle(ctx, claimed, effectSucceeded, map[string]any{"id": 1}, "")
				return wantStatus(res, err)(StatusOK)
			}
		}},
		{"an approval", "approval.decided", func(t *testing.T, f *fixture) func() error {
			a := asked(t, f, &writer{class: policy.EffectLookup}, time.Hour)
			return func() error {
				_, err := f.decide(t, a, Decision{Kind: DecisionApprove, Key: "k"})
				return err
			}
		}},
		{"a rejection, which abandons the write", "effect.settled", func(t *testing.T, f *fixture) func() error {
			a := asked(t, f, &writer{class: policy.EffectLookup}, time.Hour)
			return func() error {
				_, err := f.decide(t, a, Decision{Kind: DecisionReject, Reason: "no", Key: "k"})
				return err
			}
		}},
		{"an expiry", "approval.expired", func(t *testing.T, f *fixture) func() error {
			clock := useClock(t)
			a := asked(t, f, &writer{class: policy.EffectLookup}, time.Minute)
			clock.advance(2 * time.Minute)
			return func() error {
				done, err := f.g.expire(ctx, a)
				if err == nil && !done {
					return errors.New("not expired")
				}
				return err
			}
		}},
		{"an approved write goes on", "approval.used", func(t *testing.T, f *fixture) func() error {
			w := &writer{class: policy.EffectLookup}
			a := asked(t, f, w, time.Hour)
			if _, err := f.decide(t, a, Decision{Kind: DecisionApprove}); err != nil {
				t.Fatal(err)
			}
			return func() error { return wantStatus(f.ask(t, w, args, 1, time.Hour))(StatusOK) }
		}},
		{"a write held back for an approval", "approval.created", func(t *testing.T, f *fixture) func() error {
			w := &writer{class: policy.EffectLookup}
			req, canon := call(t, w, args)
			if res, err := f.g.record(ctx, f.run, w, w.Class(), 1, nil, req, canon); res != nil || err != nil {
				t.Fatalf("record: %v %v", res, err)
			}
			return func() error { return wantStatus(f.ask(t, w, args, 1, time.Hour))(StatusNeedsApproval) }
		}},
		{"an approval the run did not act on in time", "approval.expired", approvedThenLate},
		{"the write of an approval the run did not act on in time, abandoned", "effect.settled", approvedThenLate},
		{"an approval that ran out before its write took effect", "approval.expired", func(t *testing.T, f *fixture) func() error {
			clock := useClock(t)
			w := &writer{class: policy.EffectLookup, unsent: true}
			a := asked(t, f, w, time.Minute)
			if _, err := f.decide(t, a, Decision{Kind: DecisionApprove}); err != nil {
				t.Fatal(err)
			}
			if _, err := f.ask(t, w, args, 1, time.Minute); err == nil { // approved, used, and the tool sent nothing
				t.Fatal("the tool sent it")
			}
			clock.advance(2 * time.Minute)
			return func() error { return wantStatus(f.ask(t, w, args, 1, time.Minute))(StatusExpired) }
		}},
		{"an edit superseded", "approval.superseded", func(t *testing.T, f *fixture) func() error {
			w := &editable{writer{class: policy.EffectLookup}}
			f.g.Tools = tools.Registry{w.Name(): w}
			original := map[string]any{"patch_ref": "sha256:" + strings.Repeat("1", 64)}
			if _, err := f.ask(t, w, original, 1, time.Hour); err != nil {
				t.Fatal(err)
			}
			if _, err := f.decide(t, f.one(t), Decision{Kind: DecisionEdit, Patch: []byte(editedPatch)}); err != nil {
				t.Fatal(err)
			}
			edited := map[string]any{"patch_ref": "sha256:" + sha256Hex([]byte(editedPatch))}
			return func() error { return wantStatus(f.ask(t, w, edited, 1, time.Hour))(StatusNeedsApproval) }
		}},
		{"a person's finding", "effect.reconciled", func(t *testing.T, f *fixture) func() error {
			w := &writer{class: policy.EffectUnsafe, err: errors.New("connection reset")}
			if status, err := f.do(t, w); err != nil || status != StatusNeedsReconcile {
				t.Fatalf("%s %v", status, err)
			}
			return func() error {
				_, _, err := f.g.Reconcile(ctx, Resolution{By: operator, EffectID: f.req().EffectID, Executed: true,
					ResultJSON: `{"id": 1}`, Key: "k"})
				return err
			}
		}},
		{"agentd's start abandons a cancelled run's write", "effect.settled", func(t *testing.T, f *fixture) func() error {
			recorded(t, f, &writer{class: policy.EffectLookup})
			f.cancel(t)
			return func() error { return f.g.RecoverEffects(ctx) }
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t, false)
			change := c.prepare(t, f)
			before := snapshot(t, f.g.Ledger)
			drop := failAppends(t, f.g.Ledger, c.record)
			if err := change(); err == nil {
				t.Fatalf("the change was made without its %s record", c.record)
			}
			if after := snapshot(t, f.g.Ledger); after != before {
				t.Fatalf("the change was made all the same; before:\n%s\nafter:\n%s", before, after)
			}
			drop()
			if err := change(); err != nil {
				t.Fatalf("with its record: %v", err)
			}
			rows, err := f.g.Ledger.Q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: 1, Limit: 1000})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.ContainsFunc(rows, func(r db.AuditLog) bool { return r.Type == c.record }) {
				t.Errorf("no %s record", c.record)
			}
		})
	}
}

// approvedThenLate has a person approve the fixture's write, and the run come back for it only
// after the approval expired: the change is that call, which voids the approval and abandons the write.
func approvedThenLate(t *testing.T, f *fixture) func() error {
	clock := useClock(t)
	w := &writer{class: policy.EffectLookup}
	a := asked(t, f, w, time.Minute)
	if _, err := f.decide(t, a, Decision{Kind: DecisionApprove}); err != nil {
		t.Fatal(err)
	}
	clock.advance(2 * time.Minute)
	return func() error { return wantStatus(f.ask(t, w, map[string]any{"n": 1}, 1, time.Minute))(StatusExpired) }
}

func unexpected(res *agentosv1.Result) error {
	if res != nil {
		return fmt.Errorf("unexpected result %s %s", res.GetStatus(), res.GetValueJson())
	}
	return nil
}

// wantStatus checks a call's result has status.
func wantStatus(res *agentosv1.Result, err error) func(status string) error {
	return func(status string) error {
		switch {
		case err != nil:
			return err
		case res.GetStatus() != status:
			return fmt.Errorf("status %s, want %s: %s", res.GetStatus(), status, res.GetValueJson())
		}
		return nil
	}
}

// recorded records the fixture's call of w, with no approval, and returns its effect.
func recorded(t *testing.T, f *fixture, w tools.Writer) db.Effect {
	t.Helper()
	if res, err := f.g.record(context.Background(), f.run, w, w.Class(), 1, nil, f.req(), nil); res != nil || err != nil {
		t.Fatalf("record: %v %v", res, err)
	}
	return f.status(t)
}

func claimedEffect(t *testing.T, f *fixture) db.Effect {
	t.Helper()
	claimed, ok, err := f.g.claim(context.Background(), recorded(t, f, &writer{class: policy.EffectLookup}))
	if !ok || err != nil {
		t.Fatalf("claim: %v %v", ok, err)
	}
	return claimed
}

// asked has the fixture's run ask for a write the policy wants approved within ttl, and returns
// its approval.
func asked(t *testing.T, f *fixture, w tools.Writer, ttl time.Duration) db.Approval {
	t.Helper()
	if err := wantStatus(f.ask(t, w, map[string]any{"n": 1}, 1, ttl))(StatusNeedsApproval); err != nil {
		t.Fatal(err)
	}
	return f.one(t)
}

// invokable is a gateway whose ledger holds the test execution's run, with a write the policy allows.
func invokable(t *testing.T) (*Gateway, *ledger.Ledger, *writer) {
	t.Helper()
	g, l := startingRun(t, time.Now())
	if err := recordRunID(l, testRunID)(context.Background()); err != nil {
		t.Fatal(err)
	}
	w := &writer{class: policy.EffectIdempotent}
	g.Policy = &policy.Policy{Tools: map[string]policy.Tool{w.Name(): {Decision: "allow"}}}
	g.Tasks = tasks.Registry{"security-audit": {Name: "security-audit"}}
	g.Tools = tools.Registry{w.Name(): w}
	g.Incarnation = "this-agentd"
	return g, l, w
}

// recordAt is the record at seq.
func recordAt(t *testing.T, l *ledger.Ledger, seq int64) db.AuditLog {
	t.Helper()
	rows, err := l.Q.ListAuditRecords(context.Background(), db.ListAuditRecordsParams{Seq: seq, Limit: 1})
	if err != nil || len(rows) != 1 || rows[0].Seq != seq {
		t.Fatalf("record %d: %v %v", seq, rows, err)
	}
	return rows[0]
}

func payloadOf(t *testing.T, r db.AuditLog) map[string]any {
	t.Helper()
	var p map[string]any
	if err := json.Unmarshal([]byte(r.Payload), &p); err != nil {
		t.Fatal(err)
	}
	return p
}

// Every attempt of a call ends with its record, by the run, after the records of what it did; and
// the result names that record, the chain's head: the anchor Temporal's history keeps.
func TestACallEndsWithItsRecordWhichItsResultNames(t *testing.T) {
	g, l, w := invokable(t)
	call := &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{"b": 2, "a": 1}`}
	res, err := invoke(g, call)
	if err != nil || res.GetStatus() != StatusOK {
		t.Fatalf("%v %v", res, err)
	}
	var v struct {
		ID    int        `json:"id"`
		Audit audit.Head `json:"_audit"`
	}
	if err := json.Unmarshal([]byte(res.GetValueJson()), &v); err != nil || v.ID != 1 {
		t.Fatalf("value %s: %v", res.GetValueJson(), err)
	}
	rep, err := audit.Verify(context.Background(), l.Q)
	if err != nil || rep.BrokenAt != 0 || rep.Head != v.Audit {
		t.Fatalf("the result names %+v; the chain: %+v %v", v.Audit, rep, err)
	}
	r := recordAt(t, l, v.Audit.Seq)
	if r.Type != "call" || r.Actor != audit.Run || r.WorkflowID != testWorkflowID || r.PeerUid.Valid {
		t.Errorf("the call's record: %+v", r)
	}
	p := payloadOf(t, r)
	canon, _ := encodeCanonical(map[string]any{"a": 1, "b": 2})
	if p["kind"] != "tool" || p["name"] != w.Name() || p["attempt"] != float64(1) || p["status"] != StatusOK ||
		p["args_sha256"] != sha256Hex(canon) || fmt.Sprint(p["policy"]) != "map[decision:allow]" || p["activity_id"] == nil {
		t.Errorf("the call's record says %v", p)
	}
	// Before it, the attempt's call.started, which it names, and then what the call did: the effect
	// protocol's steps.
	var types []string
	for seq := int64(1); seq < v.Audit.Seq; seq++ {
		types = append(types, recordAt(t, l, seq).Type)
	}
	if want := []string{"call.counted", "call.started", "effect.recorded", "effect.claimed", "effect.sending", "effect.settled"}; !slices.Equal(types, want) {
		t.Errorf("records before the call's: %v, want %v", types, want)
	}
	// The loop guard's count of the call, with what the row of calls holds; the call's record says
	// the outcome it set there, which the guard reads too.
	counted := payloadOf(t, recordAt(t, l, 1))
	if counted["activity_id"] != p["activity_id"] || counted["scheduled_at"] == nil || counted["scheduled_at"] != p["scheduled_at"] ||
		counted["kind"] != "tool" || counted["name"] != w.Name() || counted["args_sha256"] != sha256Hex(canon) || p["outcome"] != StatusOK {
		t.Errorf("call.counted says %v; the call's record %v", counted, p)
	}
	started := recordAt(t, l, 2)
	sp := payloadOf(t, started)
	if started.Actor != audit.Run || started.WorkflowID != testWorkflowID || p["started_seq"] != float64(2) ||
		sp["activity_id"] != p["activity_id"] || sp["attempt"] != float64(1) || sp["kind"] != "tool" || sp["name"] != w.Name() ||
		sp["args_sha256"] != sha256Hex(canon) || fmt.Sprint(sp["policy"]) != "map[decision:allow]" || sp["status"] != nil {
		t.Errorf("call.started %+v says %v; the call's record says %v", started, sp, p)
	}
	// The loop guard reads the outcome, written with the record.
	var outcome sql.NullString
	if err := l.DB.QueryRow(`SELECT outcome FROM calls`).Scan(&outcome); err != nil || outcome.String != StatusOK {
		t.Errorf("the call's outcome: %v %v", outcome, err)
	}
}

// A call refused before it does anything has its record all the same: who asked, and why not.
func TestARefusedCallHasItsRecord(t *testing.T) {
	for name, tc := range map[string]struct {
		call    *agentosv1.Call
		prepare func(g *Gateway, l *ledger.Ledger)
		want    map[string]any // what the record says
	}{
		"an unknown run": {noSuchTool, func(g *Gateway, l *ledger.Ledger) {
			if _, err := l.DB.Exec(`UPDATE runs SET run_id = 'another-run-id'`); err != nil {
				t.Fatal(err)
			}
		}, map[string]any{"status": "error", "error_type": "UnknownRun", "temporal_run_id": testRunID}},
		"a run whose run ID is never recorded": {noSuchTool, func(g *Gateway, l *ledger.Ledger) {
			g.runIDWait = 50 * time.Millisecond
			if _, err := l.DB.Exec(`UPDATE runs SET run_id = NULL`); err != nil {
				t.Fatal(err)
			}
		}, map[string]any{"status": "error", "error": ledger.ErrRunStarting.Error(), "temporal_run_id": testRunID}},
		"bad arguments": {&agentosv1.Call{Kind: "tool", Name: "test.write", ArgsJson: `[1]`}, nil,
			map[string]any{"status": "error", "error_type": "BadArguments", "raw_args_sha256": sha256Hex([]byte(`[1]`))}},
		"a cancelled run": {noSuchTool, func(g *Gateway, l *ledger.Ledger) {
			if _, err := l.DB.Exec(`UPDATE runs SET cancelled_at = ?`, ledger.Now()); err != nil {
				t.Fatal(err)
			}
		}, map[string]any{"status": StatusDenied, "reason": "run cancelled"}},
		"the loop guard": {noSuchTool, func(g *Gateway, l *ledger.Ledger) {
			g.Limits.GatewayCalls = 1
			if _, err := l.Q.InsertCall(context.Background(), db.InsertCallParams{WorkflowID: testWorkflowID, ActivityID: "0:x",
				ScheduledAt: ledger.Now(), Kind: "tool", Name: "x", ArgsSha256: "x", CreatedAt: ledger.Now()}); err != nil {
				t.Fatal(err)
			}
		}, map[string]any{"status": StatusDenied, "reason": "loop guard: more than 1 calls in one run"}},
		"no such tool": {noSuchTool, nil, map[string]any{"status": StatusDenied, "reason": "no such tool"}},
		"the policy denies it": {&agentosv1.Call{Kind: "tool", Name: "test.write", ArgsJson: `{}`}, func(g *Gateway, l *ledger.Ledger) {
			g.Policy.Tools["test.write"] = policy.Tool{Decision: "deny"}
		}, map[string]any{"status": StatusDenied, "reason": "denied by policy",
			"policy": map[string]any{"decision": "deny", "reason": "denied by policy"}}},
	} {
		t.Run(name, func(t *testing.T) {
			g, l, _ := invokable(t)
			if tc.prepare != nil {
				tc.prepare(g, l)
			}
			res, callErr := invoke(g, tc.call)
			rows, err := l.Q.ListAuditRecords(context.Background(), db.ListAuditRecordsParams{Seq: 1, Limit: 10})
			// A call the loop guard counted has that count recorded first (call.counted).
			rows = slices.DeleteFunc(rows, func(r db.AuditLog) bool { return r.Type == "call.counted" })
			if err != nil || len(rows) != 1 {
				t.Fatalf("records: %+v %v", rows, err)
			}
			p := payloadOf(t, rows[0])
			for k, want := range tc.want {
				if fmt.Sprint(p[k]) != fmt.Sprint(want) {
					t.Errorf("%s: %v, want %v (record %v)", k, p[k], want, p)
				}
			}
			if _, ok := tc.want["policy"]; !ok && p["policy"] != nil {
				t.Errorf("a policy verdict, though the policy was never asked: %v", p)
			}
			if _, ok := tc.want["error_type"]; !ok && p["error_type"] != nil {
				t.Errorf("an error type, though the call may be retried or did not fail: %v", p)
			}
			// A call from an execution agentd did not start, or cannot tell it did, is by nobody it knows.
			actor := audit.Run
			if tc.want["temporal_run_id"] != nil {
				actor = audit.UnknownRun
			}
			if rows[0].Type != "call" || rows[0].Actor != actor || rows[0].WorkflowID != testWorkflowID {
				t.Errorf("record %+v, want it by %s", rows[0], actor)
			}
			// A result names its record; an error has none to carry.
			if callErr == nil && !strings.Contains(res.GetValueJson(), fmt.Sprintf(`"_audit":{"seq":%d,`, rows[0].Seq)) {
				t.Errorf("the result names no record: %s", res.GetValueJson())
			}
		})
	}
}

// A result whose record cannot be appended does not go out: the call fails, and Temporal retries
// it; its outcome is not recorded either. The next attempt answers from the ledger, with its record.
func TestAResultWithoutItsRecordIsAnError(t *testing.T) {
	g, l, w := invokable(t)
	call := &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{}`}
	drop := failAppends(t, l, "call")
	_, err := invoke(g, call)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != "AuditUnavailable" || app.NonRetryable() {
		t.Fatalf("got %v, want a retryable AuditUnavailable", err)
	}
	var outcome sql.NullString
	if err := l.DB.QueryRow(`SELECT outcome FROM calls`).Scan(&outcome); err != nil || outcome.Valid {
		t.Fatalf("the outcome was recorded without the call's record: %v %v", outcome, err)
	}
	drop()
	res, err := invoke(g, call)
	if err != nil || res.GetStatus() != StatusOK || w.sent.Load() != 1 || !strings.Contains(res.GetValueJson(), `"_audit"`) {
		t.Fatalf("the next attempt: %v %v, sent %d", res, err, w.sent.Load())
	}
	checkAudit(t, l)
}

// The state machine's walks, with the audit log's projection checked after every step: every effect
// and approval the walk made is in it, as the ledger has it.
func (m *machine) checkAuditLog() {
	if err := m.proj.Read(m.ctx, m.cur.led.Q); err != nil {
		m.fatal(err)
	}
	diffs, err := m.proj.Differences(m.ctx, m.cur.led.Q, audit.Since{At: time.Unix(0, 0)})
	if err != nil {
		m.fatal(err)
	}
	if len(diffs) > 0 {
		m.fail("audit: %s", diffs[0].Why)
	}
}

// saying is a read whose result carries a "reason" of its own: the outside world's words.
type saying struct{}

func (saying) Name() string        { return "test.say" }
func (saying) Class() string       { return policy.EffectNone }
func (saying) Args() *tools.Schema { return noArgs }
func (saying) Read(context.Context, tools.Request) (map[string]any, error) {
	return map[string]any{"reason": "the repository's README says so", "n": 1}, nil
}

// lastRecord is the chain's head.
func lastRecord(t *testing.T, l *ledger.Ledger) db.AuditLog {
	t.Helper()
	r, err := l.Q.LastAuditRecord(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// A call's record holds the sha256 of the result it returned, as a verifier makes it from the
// history: the value without its anchor. The reason a tool's result carries stays out of it.
func TestTheCallsRecordHoldsItsResult(t *testing.T) {
	g, l, w := invokable(t)
	res, err := invoke(g, &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{}`})
	if err != nil {
		t.Fatal(err)
	}
	want, err := audit.ResultSHA256(res.GetValueJson())
	if err != nil {
		t.Fatal(err)
	}
	if p := payloadOf(t, lastRecord(t, l)); p["result_sha256"] != want || want != mustValueSHA(t, map[string]any{"id": 1}) {
		t.Errorf("record %v, want result_sha256 %s", p, want)
	}
	g.Tools.Add(saying{})
	g.Policy.Tools["test.say"] = policy.Tool{Decision: "allow"}
	if res, err := invoke(g, &agentosv1.Call{Kind: "tool", Name: "test.say", ArgsJson: `{}`}); err != nil || res.GetStatus() != StatusOK {
		t.Fatalf("%v %v", res, err)
	}
	if p := payloadOf(t, lastRecord(t, l)); p["reason"] != nil || p["status"] != StatusOK {
		t.Errorf("an ok result's reason went into the record: %v", p)
	}
}

func mustValueSHA(t *testing.T, v map[string]any) string {
	t.Helper()
	sum, err := audit.ValueSHA256(v)
	if err != nil {
		t.Fatal(err)
	}
	return sum
}

// An attempt that ended with an error whose record cannot be appended: a retryable one fails as
// AuditUnavailable, which the retry records; one refused for good keeps its own error, so that a
// retry is not refused again without end and the cap on attempts holds.
func TestAnErrorWhoseRecordFails(t *testing.T) {
	g, l, _ := invokable(t)
	drop := failAppends(t, l, "call")
	var app *temporal.ApplicationError
	if _, err := invoke(g, &agentosv1.Call{Kind: "tool", Name: "test.write", ArgsJson: `[1]`}); !errors.As(err, &app) ||
		app.Type() != "BadArguments" || !app.NonRetryable() {
		t.Fatalf("got %v, want BadArguments, for good", err)
	}
	info := activity.Info{ActivityID: "9:x", Attempt: 9}
	info.WorkflowExecution.ID, info.WorkflowExecution.RunID = testWorkflowID, testRunID
	tooMany := temporal.NewNonRetryableApplicationError("more than 8 attempts of one call", "TooManyAttempts", nil)
	if _, err := g.recordCall(context.Background(), info, noSuchTool, g.Log, &callRecord{argsSHA256: "x"}, nil, tooMany); err != tooMany {
		t.Fatalf("got %v, want the cap's own error", err)
	}
	if _, err := g.recordCall(context.Background(), info, noSuchTool, g.Log, &callRecord{argsSHA256: "x"}, nil,
		errors.New("the provider timed out")); !errors.As(err, &app) || app.Type() != "AuditUnavailable" || app.NonRetryable() {
		t.Fatalf("got %v, want a retryable AuditUnavailable", err)
	}
	drop()
	if _, err := invoke(g, &agentosv1.Call{Kind: "tool", Name: "test.write", ArgsJson: `[1]`}); !errors.As(err, &app) ||
		app.Type() != "BadArguments" {
		t.Fatalf("the retry: %v", err)
	}
	if p := payloadOf(t, lastRecord(t, l)); p["error_type"] != "BadArguments" {
		t.Errorf("record %v", p)
	}
}

// inActivity runs fn as an attempt of an activity, as Temporal would, and counts the heartbeats it
// sends.
func inActivity(fn func(ctx context.Context) (*agentosv1.Result, error)) (*agentosv1.Result, int64, error) {
	env := quietSuite().NewTestActivityEnvironment()
	var beats atomic.Int64
	env.SetOnActivityHeartbeatListener(func(*activity.Info, converter.EncodedValues) { beats.Add(1) })
	env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: "attempt"})
	v, err := env.ExecuteActivity("attempt")
	if err != nil {
		return nil, beats.Load(), err
	}
	res := &agentosv1.Result{}
	return res, beats.Load(), v.Get(res)
}

// A record that waits for the write turn keeps the attempt alive meanwhile.
func TestARecordWaitingForItsTurnKeepsTheAttemptAlive(t *testing.T) {
	g, l, _ := invokable(t)
	every := keepAliveEvery
	keepAliveEvery = 20 * time.Millisecond
	t.Cleanup(func() { keepAliveEvery = every })
	holding := make(chan struct{})
	go l.Tx(context.Background(), func(*db.Queries) error { close(holding); time.Sleep(300 * time.Millisecond); return nil })
	<-holding
	res, _ := result(StatusDenied, map[string]any{"reason": "no"})
	got, beats, err := inActivity(func(ctx context.Context) (*agentosv1.Result, error) {
		info := activity.GetInfo(ctx)
		info.WorkflowExecution.ID, info.WorkflowExecution.RunID = testWorkflowID, testRunID
		return g.recordCall(ctx, info, noSuchTool, g.Log, &callRecord{argsSHA256: "x"}, res, nil)
	})
	// Its first try, which waited, wrote it: the heartbeat is keepAlive's. (The SDK sends the first
	// heartbeat of an interval at once, and holds back the rest.)
	if err != nil || got.GetStatus() != StatusDenied || beats == 0 {
		t.Fatalf("%v %v, %d heartbeats", got, err, beats)
	}
}

// A record waits for the write turn no longer than appendTurnWait each try, and keeps the attempt
// alive while it waits: then the attempt fails, retryably, rather than wait for ever.
func TestARecordWaitsForItsTurnBoundedAndAlive(t *testing.T) {
	g, l, _ := invokable(t)
	wait, every := appendTurnWait, keepAliveEvery
	appendTurnWait, keepAliveEvery = 50*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { appendTurnWait, keepAliveEvery = wait, every })
	holding, release := make(chan struct{}), make(chan struct{})
	held := make(chan error, 1)
	go func() {
		held <- l.Tx(context.Background(), func(*db.Queries) error { close(holding); <-release; return nil })
	}()
	<-holding
	released := time.AfterFunc(5*time.Second, func() { close(release) })
	start := time.Now()
	res, _ := result(StatusDenied, map[string]any{"reason": "no"})
	_, beats, err := inActivity(func(ctx context.Context) (*agentosv1.Result, error) {
		info := activity.GetInfo(ctx)
		info.WorkflowExecution.ID, info.WorkflowExecution.RunID = testWorkflowID, testRunID
		return g.recordCall(ctx, info, noSuchTool, g.Log, &callRecord{argsSHA256: "x"}, res, nil)
	})
	took := time.Since(start)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != "AuditUnavailable" || took > 3*time.Second || beats == 0 {
		t.Fatalf("after %v, %d heartbeats: %v; want a retryable AuditUnavailable before the turn came free", took, beats, err)
	}
	if released.Stop() {
		close(release)
	}
	if err := <-held; err != nil {
		t.Fatal(err)
	}
}

// A record that cannot be appended at once is tried again before the attempt gives up.
func TestARecordIsTriedAgain(t *testing.T) {
	g, l, w := invokable(t)
	l.UncheckedSchema = true // the table and the trigger are the test's
	if _, err := l.DB.Exec(`CREATE TABLE fail_while (n INTEGER); INSERT INTO fail_while VALUES (1);
		CREATE TRIGGER fail_appends BEFORE INSERT ON audit_log WHEN NEW.type = 'call' AND EXISTS (SELECT 1 FROM fail_while)
		BEGIN SELECT RAISE(ABORT, 'no room for the record'); END`); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(150 * time.Millisecond) // the first tries fail
		if _, err := l.DB.Exec(`DELETE FROM fail_while`); err != nil {
			t.Error(err)
		}
	}()
	// Between its tries, the attempt heartbeats: a record retried for a while does not cost it.
	res, beats, err := inActivity(func(ctx context.Context) (*agentosv1.Result, error) {
		return g.Invoke(ctx,
			&agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{}`})
	})
	if err != nil || !strings.Contains(res.GetValueJson(), `"_audit"`) || beats == 0 {
		t.Fatalf("%v %v, %d heartbeats", res, err, beats)
	}
	checkAudit(t, l)
}

// A call's record is written though the attempt's context has ended.
func TestTheRecordOutlivesTheAttempt(t *testing.T) {
	g, l, _ := invokable(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	info := activity.Info{ActivityID: "1:x", Attempt: 1}
	info.WorkflowExecution.ID, info.WorkflowExecution.RunID = testWorkflowID, testRunID
	res, _ := result(StatusDenied, map[string]any{"reason": "no"})
	got, err := g.recordCall(ctx, info, noSuchTool, g.Log, &callRecord{argsSHA256: "x"}, res, nil)
	if err != nil || !strings.Contains(got.GetValueJson(), `"_audit":{"seq":1,`) {
		t.Fatalf("%v %v", got, err)
	}
	if r := lastRecord(t, l); r.Type != "call" || r.Seq != 1 {
		t.Errorf("record %+v", r)
	}
}

// The loop guard's refusal and its record are one transaction with the count of the call: without
// the record, the call is not counted either.
func TestTheLoopGuardsRefusalIsOneTransaction(t *testing.T) {
	g, l, _ := invokable(t)
	g.Limits.GatewayCalls = 1
	if _, err := l.Q.InsertCall(context.Background(), db.InsertCallParams{WorkflowID: testWorkflowID, ActivityID: "0:x",
		ScheduledAt: ledger.Now(), Kind: "tool", Name: "x", ArgsSha256: "x", CreatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	calls := func() int {
		var n int
		if err := l.DB.QueryRow(`SELECT COUNT(*) FROM calls`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	drop := failAppends(t, l, "call")
	var app *temporal.ApplicationError
	if _, err := invoke(g, noSuchTool); !errors.As(err, &app) || app.Type() != "AuditUnavailable" || app.NonRetryable() {
		t.Fatalf("got %v, want a retryable AuditUnavailable", err)
	}
	if calls() != 1 {
		t.Fatalf("%d calls counted, want only the first", calls())
	}
	drop()
	res, err := invoke(g, noSuchTool)
	if err != nil || res.GetStatus() != StatusDenied || calls() != 2 {
		t.Fatalf("%v %v, %d calls", res, err, calls())
	}
	if p := payloadOf(t, lastRecord(t, l)); !strings.HasPrefix(fmt.Sprint(p["reason"]), "loop guard") ||
		p["result_sha256"] != mustValueSHA(t, map[string]any{"reason": p["reason"]}) {
		t.Errorf("record %v", p)
	}
}

// A claim's record says the version the claim made.
func TestTheClaimsRecordSaysItsVersion(t *testing.T) {
	f := newFixture(t, false)
	claimed := claimedEffect(t, f)
	p := payloadOf(t, lastRecord(t, f.g.Ledger))
	if p["version"] != float64(claimed.Version) || p["by"] != "this-agentd" || claimed.Version != f.status(t).Version {
		t.Errorf("record %v, the claim made version %d", p, claimed.Version)
	}
}

// A change that changed nothing has no record: its UPDATE found the row otherwise than it was read.
func TestNoRecordOfAChangeThatChangedNothing(t *testing.T) {
	ctx := context.Background()
	records := func(f *fixture) int64 {
		r, err := f.g.Ledger.Q.LastAuditRecord(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return 0
		} else if err != nil {
			t.Fatal(err)
		}
		return r.Seq
	}
	t.Run("a settlement of a stale read", func(t *testing.T) {
		f := newFixture(t, false)
		claimed := claimedEffect(t, f)
		if _, err := f.g.settle(ctx, claimed, effectSucceeded, map[string]any{"id": 1}, ""); err != nil {
			t.Fatal(err)
		}
		n := records(f)
		res, err := f.g.settle(ctx, claimed, effectFailed, nil, "too late") // the version moved on
		if err != nil || res.GetStatus() != StatusOK || records(f) != n {
			t.Fatalf("%v %v, records %d -> %d", res, err, n, records(f))
		}
	})
	t.Run("a release of a stale read", func(t *testing.T) {
		f := newFixture(t, false)
		claimed := claimedEffect(t, f)
		if n, err := f.g.giveBack(ctx, claimed, true); err != nil || n != 1 {
			t.Fatal(n, err)
		}
		n := records(f)
		if got, err := f.g.giveBack(ctx, claimed, true); err != nil || got != 0 || records(f) != n {
			t.Fatalf("%d %v, records %d -> %d", got, err, n, records(f))
		}
	})
	t.Run("a held write of a cancelled run at start", func(t *testing.T) {
		f := newFixture(t, false)
		claimedEffect(t, f) // this agentd holds it: recovered by its class, not abandoned at start
		f.cancel(t)
		f.g.Incarnation = "the next agentd"
		n := records(f)
		if err := f.g.RecoverEffects(ctx); err != nil {
			t.Fatal(err)
		}
		f.g.Drain(time.Minute)
		rows, err := f.g.Ledger.Q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: n + 1, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			if r.Type == "effect.settled" && strings.Contains(r.Payload, `"status":"PENDING"`) {
				t.Errorf("a settlement that settled nothing: %+v", r)
			}
		}
	})
	// The approval's write is not as the approval says: the approval changes, and its record is
	// written; the write does not, and has none.
	inconsistent := func(t *testing.T, f *fixture, a db.Approval) {
		t.Helper()
		f.direct = true
		if _, err := f.g.Ledger.DB.Exec(`UPDATE effects SET status = 'FAILED' WHERE effect_id = ?`, a.EffectID); err != nil {
			t.Fatal(err)
		}
	}
	settledOf := func(f *fixture, effectID string, from int64) int {
		rows, err := f.g.Ledger.Q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: from + 1, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		k := 0
		for _, r := range rows {
			if r.Type == "effect.settled" && strings.Contains(r.Payload, effectID) {
				k++
			}
		}
		return k
	}
	t.Run("a rejection", func(t *testing.T) {
		f := newFixture(t, false)
		a := asked(t, f, &writer{class: policy.EffectLookup}, time.Hour)
		inconsistent(t, f, a)
		n := records(f)
		if _, err := f.decide(t, a, Decision{Kind: DecisionReject, Reason: "no"}); err != nil {
			t.Fatal(err)
		}
		if settledOf(f, a.EffectID, n) != 0 || records(f) != n+1 {
			t.Fatalf("records %d -> %d", n, records(f))
		}
	})
	t.Run("an expiry", func(t *testing.T) {
		f := newFixture(t, false)
		clock := useClock(t)
		a := asked(t, f, &writer{class: policy.EffectLookup}, time.Minute)
		inconsistent(t, f, a)
		clock.advance(2 * time.Minute)
		n := records(f)
		if done, err := f.g.expire(ctx, a); err != nil || !done {
			t.Fatal(done, err)
		}
		if settledOf(f, a.EffectID, n) != 0 || records(f) != n+1 {
			t.Fatalf("records %d -> %d", n, records(f))
		}
	})
	t.Run("an edit superseded", func(t *testing.T) {
		f := newFixture(t, false)
		w := &editable{writer{class: policy.EffectLookup}}
		f.g.Tools = tools.Registry{w.Name(): w}
		if _, err := f.ask(t, w, map[string]any{"patch_ref": "sha256:" + strings.Repeat("1", 64)}, 1, time.Hour); err != nil {
			t.Fatal(err)
		}
		a := f.one(t)
		if _, err := f.decide(t, a, Decision{Kind: DecisionEdit, Patch: []byte(editedPatch)}); err != nil {
			t.Fatal(err)
		}
		inconsistent(t, f, a)
		n := records(f)
		edited := map[string]any{"patch_ref": "sha256:" + sha256Hex([]byte(editedPatch))}
		if err := wantStatus(f.ask(t, w, edited, 1, time.Hour))(StatusNeedsApproval); err != nil {
			t.Fatal(err)
		}
		if settledOf(f, a.EffectID, n) != 0 {
			t.Fatal("the superseded write, which was not awaiting approval, has a settlement")
		}
	})
	t.Run("an approval that ran out, of a stale read", func(t *testing.T) {
		f := newFixture(t, false)
		clock := useClock(t)
		w := &writer{class: policy.EffectLookup, unsent: true}
		a := asked(t, f, w, time.Minute)
		if _, err := f.decide(t, a, Decision{Kind: DecisionApprove}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.ask(t, w, map[string]any{"n": 1}, 1, time.Minute); err == nil {
			t.Fatal("the tool sent it")
		}
		clock.advance(2 * time.Minute)
		stale := effectOf(t, f, a)
		stale.Version-- // as read before the last change
		n := records(f)
		if _, err := f.g.expireUnsent(ctx, stale, f.one(t)); !errors.Is(err, errEffectBusy) || records(f) != n {
			t.Fatalf("%v, records %d -> %d", err, n, records(f))
		}
	})
}

// A person's decision and agentd's expiry, one after the other and at once: one of them is the
// approval's, and only its record is in the log. The expiry reads the approval before its
// transaction (ExpireApprovals lists the due ones): a decision may have come between.
func TestADecisionAndAnExpiryAtOnce(t *testing.T) {
	for i := range 30 {
		f := newFixture(t, false)
		useClock(t)
		a := asked(t, f, &writer{class: policy.EffectLookup}, time.Minute)
		decide := func() { f.decide(t, a, Decision{Kind: DecisionApprove}) }
		expire := func() { f.g.expire(context.Background(), a) }
		switch {
		case i < 5:
			decide()
			expire()
		case i < 10:
			expire()
			decide()
		default:
			var wg sync.WaitGroup
			wg.Go(decide)
			wg.Go(expire)
			wg.Wait()
		}
		decided, expiredRecs := 0, 0
		rows, err := f.g.Ledger.Q.ListAuditRecords(context.Background(), db.ListAuditRecordsParams{Seq: 1, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			switch r.Type {
			case "approval.decided":
				decided++
			case "approval.expired":
				expiredRecs++
			}
		}
		got := f.one(t)
		if decided+expiredRecs != 1 || (decided == 1) != (got.Status == approvalApproved) || (expiredRecs == 1) != (got.Status == approvalExpired) {
			t.Fatalf("round %d: %d decided and %d expired records; the approval is %s", i, decided, expiredRecs, got.Status)
		}
	}
}

// differences are the ways the ledger's tables are not what its records say, the effects and
// approvals of rows made since the epoch included.
func differences(t *testing.T, l *ledger.Ledger) []audit.Difference {
	t.Helper()
	var p audit.Projection
	if err := p.Read(context.Background(), l.Q); err != nil {
		t.Fatal(err)
	}
	diffs, err := p.Differences(context.Background(), l.Q, audit.Since{At: time.Unix(0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	return diffs
}

// An effect or an approval changed without its record is found: each field the records speak of.
func TestTheRecordsFindATableChangedWithoutThem(t *testing.T) {
	// An approved write, used, claimed, sent and settled.
	approvedAndSent := func(t *testing.T) (*fixture, db.Approval) {
		f := newFixture(t, false)
		f.direct = true // the test changes the tables itself
		w := &writer{class: policy.EffectLookup}
		a := asked(t, f, w, time.Hour)
		if _, err := f.decide(t, a, Decision{Kind: DecisionApprove}); err != nil {
			t.Fatal(err)
		}
		if err := wantStatus(f.ask(t, w, map[string]any{"n": 1}, 1, time.Hour))(StatusOK); err != nil {
			t.Fatal(err)
		}
		if d := differences(t, f.g.Ledger); len(d) != 0 {
			t.Fatalf("before: %v", d)
		}
		return f, f.one(t)
	}
	for name, c := range map[string]struct {
		sql  string // ?1 is the effect, ?2 the approval
		kind string
		why  string
	}{
		"the effect's status":    {`UPDATE effects SET status = 'FAILED' WHERE effect_id = ?1`, "effect", "is FAILED, and the audit log says SUCCEEDED"},
		"who claimed the effect": {`UPDATE effects SET claimed_by = 'mallory' WHERE effect_id = ?1`, "effect", `claimed by "mallory"`},
		"whether it may have been sent": {`UPDATE effects SET maybe_sent = 0 WHERE effect_id = ?1`, "effect",
			"has maybe_sent 0, and the audit log says true"},
		"the effect gone": {`DELETE FROM approvals WHERE approval_id = ?2; DELETE FROM effects WHERE effect_id = ?1`, "effect",
			"the ledger has no such effect"},
		"the approval gone": {`DELETE FROM approvals WHERE approval_id = ?2`, "approval", "the ledger has no such approval"},
		"the approval's status": {`UPDATE approvals SET status = 'REJECTED', reason = 'no' WHERE approval_id = ?2`, "approval",
			"is REJECTED (decided true), and the audit log says APPROVED"},
		"whether it was used": {`UPDATE approvals SET used_at = NULL WHERE approval_id = ?2`, "approval", "has used false, and the audit log says true"},
		"who decided":         {`UPDATE approvals SET decided_by = 'mallory' WHERE approval_id = ?2`, "approval", `decided by "mallory", and the audit log says "alice"`},
		"the payload held for it, replaced": {`INSERT INTO blobs (sha256, bytes) VALUES ('` + strings.Repeat("a", 64) + `', 'TAMPERED');
			UPDATE effects SET payload_sha256 = '` + strings.Repeat("a", 64) + `', payload_blob = 'sha256:` + strings.Repeat("a", 64) + `'
			WHERE effect_id = ?1`, "effect", "the audit log records its payload as"},
		"its payload blob":     {`UPDATE effects SET payload_blob = 'sha256:00' WHERE effect_id = ?1`, "effect", `payload blob "sha256:00"`},
		"its payload's sha256": {`UPDATE effects SET payload_sha256 = 'x' WHERE effect_id = ?1`, "effect", "has payload_sha256 x and payload blob"},
		"its result":           {`UPDATE effects SET result_blob = 'sha256:00' WHERE effect_id = ?1`, "effect", `has result blob "sha256:00"`},
		"the approval's payload": {`UPDATE approvals SET payload_sha256 = 'x' WHERE approval_id = ?2`, "approval",
			"is of payload sha256 x"},
		"the approval's expiry": {`UPDATE approvals SET expires_at = '2099-01-01T00:00:00.000000000Z' WHERE approval_id = ?2`, "approval",
			"expires at 2099-01-01T00:00:00.000000000Z"},
		"an effect without its record": {`INSERT INTO effects (effect_id, workflow_id, tool, effect_class, payload_sha256, idempotency_key,
			status, created_at, updated_at) VALUES ('e-made-up', 'wf', 'x', 'idempotent', 'p', 'k', 'SUCCEEDED', '2001-01-01T00:00:00Z', '2001-01-01T00:00:00Z')`,
			"effect", "effect e-made-up, made at"},
	} {
		t.Run(name, func(t *testing.T) {
			f, a := approvedAndSent(t)
			if _, err := f.g.Ledger.DB.Exec(c.sql, a.EffectID, a.ApprovalID); err != nil {
				t.Fatal(err)
			}
			d := differences(t, f.g.Ledger)
			if !slices.ContainsFunc(d, func(d audit.Difference) bool { return d.Kind == c.kind && strings.Contains(d.Why, c.why) }) {
				t.Fatalf("no %s difference saying %q: %v", c.kind, c.why, d)
			}
		})
	}
}

// An effect a person was asked to approve, claimed before an approval of it was used, breaks the
// protocol, whatever the tables say.
func TestAClaimBeforeTheApprovalBreaksTheProtocol(t *testing.T) {
	f := newFixture(t, false)
	f.direct = true
	a := asked(t, f, &writer{class: policy.EffectLookup}, time.Hour)
	if _, err := audit.Record(context.Background(), f.g.Ledger, audit.Event{WorkflowID: "wf", Type: "effect.claimed",
		Actor: audit.Agentd, Payload: map[string]any{"effect_id": a.EffectID, "by": "this-agentd", "version": 1}}); err != nil {
		t.Fatal(err)
	}
	d := differences(t, f.g.Ledger)
	if !slices.ContainsFunc(d, func(d audit.Difference) bool { return d.Kind == "protocol" && d.ID == a.EffectID }) {
		t.Fatalf("differences %v", d)
	}
}

// A decision's record is by the operator whose connection sent it, with the uid the kernel gave; the
// name agentctl gave is only part of what it says.
func TestADecisionIsItsOperators(t *testing.T) {
	f := newFixture(t, false)
	a := asked(t, f, &writer{class: policy.EffectLookup}, time.Hour)
	who := audit.Who{Actor: audit.Operator("bob"), PeerUID: sql.NullInt64{Int64: 1001, Valid: true}}
	if _, err := f.decide(t, a, Decision{Kind: DecisionApprove, By: "alice", Who: who}); err != nil {
		t.Fatal(err)
	}
	r := lastRecord(t, f.g.Ledger)
	if p := payloadOf(t, r); r.Type != "approval.decided" || r.Actor != "operator:bob" || r.PeerUid != who.PeerUID ||
		p["decided_by"] != "alice" {
		t.Fatalf("record %+v", r)
	}
}

// A reviewer's edit names its patch by sha256: the approval's patch blob changed without a record is
// found.
func TestTheRecordsFindAnEditedPatchChanged(t *testing.T) {
	f := newFixture(t, false)
	f.direct = true
	w := &editable{writer{class: policy.EffectLookup}}
	f.g.Tools = tools.Registry{w.Name(): w}
	if _, err := f.ask(t, w, map[string]any{"patch_ref": "sha256:" + strings.Repeat("1", 64)}, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	a := f.one(t)
	if _, err := f.decide(t, a, Decision{Kind: DecisionEdit, Patch: []byte(editedPatch)}); err != nil {
		t.Fatal(err)
	}
	if d := differences(t, f.g.Ledger); len(d) != 0 {
		t.Fatalf("before: %v", d)
	}
	if _, err := f.g.Ledger.DB.Exec(`UPDATE approvals SET edited_patch_blob = 'sha256:00' WHERE approval_id = ?`, a.ApprovalID); err != nil {
		t.Fatal(err)
	}
	d := differences(t, f.g.Ledger)
	if !slices.ContainsFunc(d, func(d audit.Difference) bool { return strings.Contains(d.Why, `has the edited patch "sha256:00"`) }) {
		t.Fatalf("differences %v", d)
	}
}

// An approval made and decided before the audit log began, and used after it: its records say only
// that it was used, and that is all that is compared.
// restarted is l's ledger as the next agentd opens it: one that has appended nothing yet.
func restarted(t *testing.T, l *ledger.Ledger) *ledger.Ledger {
	t.Helper()
	var seq int
	var name, file string
	if err := l.DB.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &file); err != nil {
		t.Fatal(err)
	}
	next, err := ledger.Open(context.Background(), file)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { next.Close() })
	return next
}

func TestAnApprovalFromBeforeTheLogUsedAfter(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectLookup}
	a := asked(t, f, w, time.Hour)
	if _, err := f.decide(t, a, Decision{Kind: DecisionApprove}); err != nil {
		t.Fatal(err)
	}
	// The log begins here, after the approval: with the next agentd, whose ledger has no log yet.
	if _, err := f.g.Ledger.DB.Exec(`DELETE FROM audit_log`); err != nil {
		t.Fatal(err)
	}
	f.g.Ledger = restarted(t, f.g.Ledger)
	if err := wantStatus(f.ask(t, w, map[string]any{"n": 1}, 1, time.Hour))(StatusOK); err != nil {
		t.Fatal(err)
	}
	if d := differences(t, f.g.Ledger); len(d) != 0 {
		t.Fatalf("differences %v", d)
	}
}

// A call the policy lets past has its call.started before anything of it leaves agentd; one that
// cannot be written sends nothing, and the attempt fails retryably. A call the policy refuses, or
// one refused before the policy is asked, has none.
func TestNothingLeavesAgentdBeforeItsRecord(t *testing.T) {
	g, l, w := invokable(t)
	drop := failAppends(t, l, "call.started")
	var app *temporal.ApplicationError
	if _, err := invoke(g, &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{}`}); !errors.As(err, &app) ||
		app.Type() != "AuditUnavailable" || app.NonRetryable() {
		t.Fatalf("got %v, want a retryable AuditUnavailable", err)
	}
	if n := w.sent.Load(); n != 0 {
		t.Fatalf("%d requests went out without their record", n)
	}
	var effects int
	if err := l.DB.QueryRow(`SELECT COUNT(*) FROM effects`).Scan(&effects); err != nil || effects != 0 {
		t.Fatalf("%d effects recorded, %v", effects, err)
	}
	if p := payloadOf(t, lastRecord(t, l)); p["status"] != "error" || p["started_seq"] != nil {
		t.Errorf("the attempt's record: %v", p)
	}
	drop()
	g.Policy.Tools["test.denied"] = policy.Tool{Decision: "deny"}
	g.Tools.Add(&namedWriter{writer{class: policy.EffectIdempotent}, "test.denied"})
	for _, c := range []*agentosv1.Call{{Kind: "tool", Name: "test.denied", ArgsJson: `{}`}, noSuchTool} {
		before := lastRecord(t, l).Seq
		if _, err := invoke(g, c); err != nil {
			t.Fatal(err)
		}
		// Its count (call.counted) and its record, and no call.started between.
		if r := lastRecord(t, l); r.Seq != before+2 || r.Type != "call" || payloadOf(t, r)["started_seq"] != nil ||
			recordAt(t, l, before+1).Type != "call.counted" {
			t.Errorf("%s: records %d to %d, the last %+v", c.GetName(), before, r.Seq, r)
		}
	}
}

// namedWriter is a writer under another name.
type namedWriter struct {
	writer
	name string
}

func (w *namedWriter) Name() string { return w.name }
