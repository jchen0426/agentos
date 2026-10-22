package gateway

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tools"
)

// drop plants a trigger that makes writes of table do nothing (RAISE(IGNORE): no row, no error), past
// the ledger's own check of its schema: what agentd decides on the rows its writes change must not
// take a write that did nothing for one that found its work done.
func drop(t *testing.T, l *ledger.Ledger, name, on string) {
	t.Helper()
	l.UncheckedSchema = true // the trigger is the test's; the check finds it otherwise (TestATriggerDoesNotSwitchTheLoopGuardOff)
	if _, err := l.DB.Exec(`CREATE TRIGGER ` + name + ` ` + on + ` BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
}

// One trigger in the ledger used to switch the loop guard off: every count of a call did nothing,
// the guard read it as counted already, wrote no call.counted, and let the calls through. Planted
// while agentd runs, the write transaction finds the schema changed and fails; past that check,
// the guard does not take an insert that added no row for a count either. No call goes through.
func TestATriggerDoesNotSwitchTheLoopGuardOff(t *testing.T) {
	g, l, w := invokable(t)
	g.Limits.GatewayCalls = 1
	if _, err := l.DB.Exec(`CREATE TRIGGER no_count BEFORE INSERT ON calls BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
	for _, unchecked := range []bool{false, true} {
		l.UncheckedSchema = unchecked
		for i := range 3 {
			res, err := invoke(g, &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{"a": 1}`})
			if err == nil {
				t.Fatalf("schema unchecked %v, call %d: %v, want an error", unchecked, i, res)
			}
			// Checked, no write goes through, the call's record neither: the attempt fails, retryably.
			if want := map[bool]string{false: "AuditUnavailable", true: "a write to the ledger changed nothing"}[unchecked]; !strings.Contains(err.Error(), want) {
				t.Fatalf("schema unchecked %v: %v, want one saying %q", unchecked, err, want)
			}
		}
	}
	if n := w.sent.Load(); n != 0 {
		t.Fatalf("%d writes went out", n)
	}
	// Unchecked, each attempt has its record, of a call whose count rolled back: it names no count.
	if d := everywhere(t, l); sayingDiff(d, "call", "names a count the audit log does not have") {
		t.Fatalf("differences %v", d)
	}
}

// A refusal of the loop guard whose record cannot be written rolls back with the count of its call:
// the attempt's record, written after it, is of a call that counted for nothing, and names no count.
func TestARefusalRolledBackNamesNoCount(t *testing.T) {
	g, l, _ := invokable(t)
	g.Limits.GatewayCalls = 1
	if _, err := l.Q.InsertCall(context.Background(), db.InsertCallParams{WorkflowID: testWorkflowID, ActivityID: "0:x",
		ScheduledAt: ledger.Now(), Kind: "tool", Name: "x", ArgsSha256: "x", CreatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	l.UncheckedSchema = true // the trigger is the test's
	if _, err := l.DB.Exec(`CREATE TRIGGER no_refusal BEFORE INSERT ON audit_log WHEN NEW.type = 'call'
		AND NEW.payload LIKE '%"status":"denied"%' BEGIN SELECT RAISE(ABORT, 'no refusal'); END`); err != nil {
		t.Fatal(err)
	}
	if res, err := invoke(g, noSuchTool); err == nil {
		t.Fatalf("a refusal that could not be recorded: %v", res)
	}
	r := lastRecord(t, l)
	if p := payloadOf(t, r); r.Type != "call" || p["error"] == nil || p["scheduled_at"] != nil {
		t.Fatalf("the attempt's record: %s %v", r.Type, p)
	}
	if d := everywhere(t, l); sayingDiff(d, "call", "names a count the audit log does not have") {
		t.Fatalf("differences %v", d)
	}
}

// A write's effect whose row the ledger drops is not taken as one recorded meanwhile.
func TestADroppedEffectIsNotTakenAsRecorded(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectIdempotent}
	f.g.Tools.Add(w)
	drop(t, f.g.Ledger, "no_effects", "BEFORE INSERT ON effects")
	if _, err := f.do(t, w); !errors.Is(err, ledger.ErrWriteDropped) || w.sent.Load() != 0 {
		t.Fatalf("err %v, sent %d", err, w.sent.Load())
	}
}

// An approved write that its effect's update fails to let go is not taken for one of a cancelled
// run, and abandoned: the run is not cancelled, the update was dropped.
func TestAnApprovedWriteDroppedIsNotAbandoned(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectLookup}
	a := asked(t, f, w, time.Hour)
	if _, err := f.decide(t, a, Decision{Kind: DecisionApprove}); err != nil {
		t.Fatal(err)
	}
	drop(t, f.g.Ledger, "no_approve", "BEFORE UPDATE OF status ON effects WHEN OLD.status = 'AWAITING_APPROVAL'")
	if _, err := f.ask(t, w, map[string]any{"n": 1}, 1, time.Hour); !errors.Is(err, ledger.ErrWriteDropped) {
		t.Fatalf("the approved call: %v", err)
	}
	if e := effectOf(t, f, a); e.Status != effectAwaitingApproval || w.sent.Load() != 0 {
		t.Fatalf("effect %s, sent %d", e.Status, w.sent.Load())
	}
}

// A reviewer's patch whose ownership the ledger drops: the edit fails, rather than give the run a
// patch it cannot read.
func TestAnEditWhoseOwnerIsDroppedFails(t *testing.T) {
	f := newFixture(t, false)
	w := &editable{writer{class: policy.EffectLookup}}
	f.g.Tools = tools.Registry{w.Name(): w}
	if _, err := f.ask(t, w, map[string]any{"repo": "acme/payments", "patch_ref": "sha256:" + strings.Repeat("1", 64)}, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	drop(t, f.g.Ledger, "no_owners", "BEFORE INSERT ON blob_owners")
	if _, err := f.decide(t, f.one(t), Decision{Kind: DecisionEdit, Patch: []byte(editedPatch), By: "carol"}); !errors.Is(err, ledger.ErrWriteDropped) {
		t.Fatalf("edit: %v", err)
	}
	if a := f.one(t); a.Status != approvalPending {
		t.Fatalf("approval %s", a.Status)
	}
	_ = context.Background()
}
