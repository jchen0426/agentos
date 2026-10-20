package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tools"
)

// editable is a write a reviewer may edit: the edit replaces its patch_ref.
type editable struct{ writer }

func (e *editable) Name() string    { return "test.edit" }
func (e *editable) EditArg() string { return "patch_ref" }

// A patch agentd takes as one (patch.Parse): what a reviewer's edit must be.
const editedPatch = "--- a/app/db.py\n+++ b/app/db.py\n@@ -1 +1 @@\n-old\n+new\n"

// call is one call of w with args, as callTool makes it: its request and canonical arguments.
func call(t *testing.T, w tools.Writer, args map[string]any) (tools.Request, []byte) {
	t.Helper()
	canon, err := encodeCanonical(args)
	if err != nil {
		t.Fatal(err)
	}
	return tools.Request{WorkflowID: "wf", EffectID: sha256Hex([]byte("wf"), []byte(w.Name()), canon), Args: args}, canon
}

// ask makes a call of w that the policy wants approved within ttl, and allows maxPerRun of.
func (f *fixture) ask(t *testing.T, w tools.Writer, args map[string]any, maxPerRun int, ttl time.Duration) (*agentosv1.Result, error) {
	t.Helper()
	req, canon := call(t, w, args)
	return f.g.effect(context.Background(), f.run, w, w.Class(), maxPerRun, &approvalRule{ttl: ttl}, req, canon, faults.Effect{})
}

// one is the run's only approval.
func (f *fixture) one(t *testing.T) db.Approval {
	t.Helper()
	all := f.approvals(t)
	if len(all) != 1 {
		t.Fatalf("%d approvals, want one", len(all))
	}
	return all[0]
}

func (f *fixture) approvals(t *testing.T) []db.Approval {
	t.Helper()
	a, err := f.g.Ledger.RunApprovals(context.Background(), "wf")
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// decide sends a decision on the approval, as a reviewer who saw its payload.
func (f *fixture) decide(t *testing.T, a db.Approval, d Decision) (db.Approval, error) {
	t.Helper()
	d.ApprovalID = a.ApprovalID
	if d.PayloadSHA256 == "" {
		d.PayloadSHA256 = a.PayloadSha256
	}
	if d.Key == "" {
		d.Key = uuid.NewString()
	}
	if d.By == "" {
		d.By = "alice"
	}
	return f.g.Decide(context.Background(), d)
}

func (f *fixture) doorbells(t *testing.T) int {
	t.Helper()
	var n int
	if err := f.g.Ledger.DB.QueryRow(`SELECT COUNT(*) FROM outbox WHERE workflow_id = 'wf'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// sentThenGivenBack claims an effect, has the tool say a visible request goes out (proceed), and
// gives it back unsettled, as a retry or a recovery does when it cannot tell what happened.
func (f *fixture) sentThenGivenBack(t *testing.T, effectID string) {
	t.Helper()
	ctx := context.Background()
	e, err := f.g.Ledger.Q.GetEffect(ctx, effectID)
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := f.g.claim(ctx, e)
	if !ok || err != nil {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if err := f.g.proceed(claimed)(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.release(ctx, claimed, false); !errors.Is(err, errEffectReleased) {
		t.Fatalf("release: %v", err)
	}
}

func value(t *testing.T, res *agentosv1.Result) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal([]byte(res.GetValueJson()), &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func effectOf(t *testing.T, f *fixture, a db.Approval) db.Effect {
	t.Helper()
	e, err := f.g.Ledger.Q.GetEffect(context.Background(), a.EffectID)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// Section 10: until a person approves it, a write waits, however often it is asked for; the
// approval holds the hash of the payload recorded for it, and only a decision that names that
// hash counts. Approved, the next call sends it, once.
func TestAnApprovalGatesTheWrite(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectLookup}
	args := map[string]any{"patch_ref": "sha256:" + strings.Repeat("1", 64)}
	for range 3 {
		res, err := f.ask(t, w, args, 1, time.Hour)
		if err != nil || res.GetStatus() != StatusNeedsApproval {
			t.Fatalf("got %v %v, want needs_approval", res.GetStatus(), err)
		}
	}
	a := f.one(t)
	if e := effectOf(t, f, a); e.Status != effectAwaitingApproval || e.PayloadSha256 != a.PayloadSha256 || w.sent.Load() != 0 {
		t.Fatalf("effect %s (payload %s, approval holds %s), sent %d", e.Status, e.PayloadSha256, a.PayloadSha256, w.sent.Load())
	}
	payload, _ := json.Marshal(args)
	if a.Status != approvalPending || a.PayloadSha256 != sha256Hex(payload) {
		t.Fatalf("approval %+v", a)
	}

	// A reviewer who saw another payload decides nothing.
	if _, err := f.decide(t, a, Decision{Kind: DecisionApprove, PayloadSHA256: strings.Repeat("0", 64)}); !errors.Is(err, ErrStaleView) {
		t.Fatalf("approving another payload: %v", err)
	}
	if f.doorbells(t) != 0 {
		t.Fatal("a refused decision rang the doorbell")
	}
	got, err := f.decide(t, a, Decision{Kind: DecisionApprove, By: "bob"})
	if err != nil || got.Status != approvalApproved || got.DecidedBy.String != "bob" || !got.DecidedAt.Valid {
		t.Fatalf("approve: %+v %v", got, err)
	}
	if f.doorbells(t) != 1 {
		t.Fatal("the decision did not ring the doorbell in its transaction")
	}
	res, err := f.ask(t, w, args, 1, time.Hour)
	if err != nil || res.GetStatus() != StatusOK || w.sent.Load() != 1 {
		t.Fatalf("after the approval: %v %v, sent %d", res.GetStatus(), err, w.sent.Load())
	}
	if e := effectOf(t, f, a); e.Status != effectSucceeded {
		t.Fatalf("effect %s", e.Status)
	}
	if a = f.one(t); !a.UsedAt.Valid {
		t.Fatal("the approval is not marked used")
	}
	if res, err := f.ask(t, w, args, 1, time.Hour); err != nil || res.GetStatus() != StatusOK || w.sent.Load() != 1 {
		t.Fatalf("again: %v %v, sent %d", res.GetStatus(), err, w.sent.Load())
	}
}

// A rejection needs its reason, which the workflow gets back; the write is abandoned and never
// sent, however often it is asked for again.
func TestARejectionAbandonsTheWrite(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectLookup}
	args := map[string]any{"n": 1}
	if res, err := f.ask(t, w, args, 1, time.Hour); err != nil || res.GetStatus() != StatusNeedsApproval {
		t.Fatalf("%v %v", res.GetStatus(), err)
	}
	a := f.one(t)
	for _, reason := range []string{"", "  ", "bad\x1b[2Jreason"} {
		if _, err := f.decide(t, a, Decision{Kind: DecisionReject, Reason: reason}); !errors.Is(err, ErrBadDecision) {
			t.Fatalf("rejecting with reason %q: %v", reason, err)
		}
	}
	if _, err := f.decide(t, a, Decision{Kind: DecisionReject, Reason: "touches the payment flow; needs a human fix"}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		res, err := f.ask(t, w, args, 1, time.Hour)
		if err != nil || res.GetStatus() != StatusRejected {
			t.Fatalf("%v %v", res.GetStatus(), err)
		}
		if v := value(t, res); v["reason"] != "touches the payment flow; needs a human fix" || v["decided_by"] != "alice" {
			t.Fatalf("value %v", v)
		}
	}
	if e := effectOf(t, f, a); e.Status != effectAbandoned || w.sent.Load() != 0 {
		t.Fatalf("effect %s, sent %d", e.Status, w.sent.Load())
	}
}

// Section 10: the first valid decision wins, decided in a SQLite transaction; the later ones are
// told who decided what, and when. The same decision sent again under its key applies once.
func TestTheFirstDecisionWins(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectLookup}
	if _, err := f.ask(t, w, map[string]any{"n": 1}, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	a := f.one(t)
	type outcome struct {
		d   Decision
		got db.Approval
		err error
	}
	var mu sync.Mutex
	var outcomes []outcome
	var wg sync.WaitGroup
	for i := range 16 {
		d := Decision{Kind: DecisionApprove, By: "approver"}
		if i%2 == 1 {
			d = Decision{Kind: DecisionReject, By: "rejecter", Reason: "no"}
		}
		wg.Go(func() {
			got, err := f.decide(t, a, d)
			mu.Lock()
			outcomes = append(outcomes, outcome{d, got, err})
			mu.Unlock()
		})
	}
	wg.Wait()
	var won *outcome
	for i, o := range outcomes {
		var decided *DecidedError
		switch {
		case o.err == nil:
			if won != nil {
				t.Fatalf("two decisions applied: %+v and %+v", won.got, o.got)
			}
			won = &outcomes[i]
		case !errors.As(o.err, &decided):
			t.Fatalf("a later decision got %v", o.err)
		}
	}
	if won == nil {
		t.Fatal("no decision applied")
	}
	for _, o := range outcomes {
		if o.err != nil && !strings.Contains(o.err.Error(), won.got.Status+" by "+won.d.By+" at "+won.got.DecidedAt.String) {
			t.Fatalf("the later decision is not told the first: %v (first: %+v)", o.err, won.got)
		}
	}
	if f.doorbells(t) != 1 {
		t.Fatalf("%d doorbells, want the winner's", f.doorbells(t))
	}
	// The winner's decision sent again under its key: what it recorded, not a conflict.
	d := won.d
	d.Key = won.got.DecisionKey.String
	if again, err := f.decide(t, a, d); err != nil || again.Status != won.got.Status {
		t.Fatalf("the same decision again: %+v %v", again, err)
	}
	other := Decision{Kind: DecisionReject, Reason: "something else", Key: won.got.DecisionKey.String, By: d.By}
	if won.d.Kind == DecisionReject {
		other = Decision{Kind: DecisionApprove, Key: won.got.DecisionKey.String, By: d.By}
	}
	if _, err := f.decide(t, a, other); !errors.Is(err, ErrBadDecision) {
		t.Fatalf("another decision under the winner's key: %v", err)
	}
	if f.doorbells(t) != 1 {
		t.Fatal("a repeated decision rang again")
	}
}

// Section 10: an approval's time is agentd's, not the workflow's. Undecided when it is up, the
// approval expires as a rejection, with its reason, and rings the run; a decision that comes
// after is told so. Approved but never acted on, it is void too.
func TestApprovalsExpire(t *testing.T) {
	t.Run("nobody decided", func(t *testing.T) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectLookup}
		if _, err := f.ask(t, w, map[string]any{"n": 1}, 1, 300*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		a := f.one(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go f.g.ExpireApprovals(ctx)
		deadline := time.Now().Add(5 * time.Second)
		for a = f.one(t); a.Status == approvalPending && time.Now().Before(deadline); a = f.one(t) {
			time.Sleep(20 * time.Millisecond)
		}
		if a.Status != approvalExpired || a.DecidedBy.String != "agentd" || !strings.Contains(a.Reason.String, "nobody decided") {
			t.Fatalf("approval %+v", a)
		}
		if e := effectOf(t, f, a); e.Status != effectAbandoned || f.doorbells(t) != 1 {
			t.Fatalf("effect %s, %d doorbells", e.Status, f.doorbells(t))
		}
		res, err := f.ask(t, w, map[string]any{"n": 1}, 1, 300*time.Millisecond)
		if err != nil || res.GetStatus() != StatusExpired || !strings.Contains(value(t, res)["reason"].(string), "nobody decided") {
			t.Fatalf("%v %v", res, err)
		}
		var decided *DecidedError
		if _, err := f.decide(t, a, Decision{Kind: DecisionApprove}); !errors.As(err, &decided) || !strings.Contains(err.Error(), "EXPIRED by agentd") {
			t.Fatalf("approving an expired approval: %v", err)
		}
	})
	t.Run("the call finds it expired", func(t *testing.T) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectLookup}
		if _, err := f.ask(t, w, map[string]any{"n": 1}, 1, 50*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		a := f.one(t)
		var decided *DecidedError
		if _, err := f.decide(t, a, Decision{Kind: DecisionApprove}); !errors.As(err, &decided) || decided.Approval.Status != approvalExpired {
			t.Fatalf("approving after the expiry: %v", err)
		}
		res, err := f.ask(t, w, map[string]any{"n": 1}, 1, time.Hour)
		if err != nil || res.GetStatus() != StatusExpired || w.sent.Load() != 0 {
			t.Fatalf("%v %v, sent %d", res.GetStatus(), err, w.sent.Load())
		}
	})
	t.Run("approved but never acted on", func(t *testing.T) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectLookup}
		if _, err := f.ask(t, w, map[string]any{"n": 1}, 1, 300*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		a := f.one(t)
		if _, err := f.decide(t, a, Decision{Kind: DecisionApprove, By: "bob"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(400 * time.Millisecond)
		res, err := f.ask(t, w, map[string]any{"n": 1}, 1, time.Hour)
		if err != nil || res.GetStatus() != StatusExpired || w.sent.Load() != 0 {
			t.Fatalf("%v %v, sent %d", res.GetStatus(), err, w.sent.Load())
		}
		if a = f.one(t); a.Status != approvalExpired || !strings.Contains(a.Reason.String, "approved by bob") {
			t.Fatalf("approval %+v", a)
		}
	})
	t.Run("approved, never acted on, and the policy stopped asking", func(t *testing.T) {
		// The approval moves the write to PENDING only in its time, whatever the policy says now.
		f := newFixture(t, false)
		w := &writer{class: policy.EffectLookup}
		args := map[string]any{"n": 1}
		if _, err := f.ask(t, w, args, 1, 300*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if _, err := f.decide(t, f.one(t), Decision{Kind: DecisionApprove, By: "bob"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(400 * time.Millisecond)
		req, canon := call(t, w, args)
		res, err := f.g.effect(context.Background(), f.run, w, w.Class(), 1, nil, req, canon, faults.Effect{})
		if err != nil || res.GetStatus() != StatusExpired || w.sent.Load() != 0 {
			t.Fatalf("%v %v, sent %d", res.GetStatus(), err, w.sent.Load())
		}
	})
}

// Section 10, T5: what goes out is the payload recorded for the write, which must be the one
// approved, to the byte. A recorded payload changed after the approval is never sent.
func TestAChangedPayloadIsNotSent(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectLookup}
	args := map[string]any{"title": "fix"}
	if _, err := f.ask(t, w, args, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	a := f.one(t)
	if _, err := f.decide(t, a, Decision{Kind: DecisionApprove}); err != nil {
		t.Fatal(err)
	}
	// One byte of the recorded payload changes (the ledger's file was edited, say).
	if _, err := f.g.Ledger.DB.Exec(`UPDATE blobs SET bytes = CAST(replace(CAST(bytes AS TEXT), 'fix', 'fiy') AS BLOB) WHERE sha256 = ?`,
		a.PayloadSha256); err != nil {
		t.Fatal(err)
	}
	res, err := f.ask(t, w, args, 1, time.Hour)
	if err != nil || res.GetStatus() != StatusDenied || !strings.Contains(value(t, res)["reason"].(string), "is not the payload approved") {
		t.Fatalf("%v %v", res, err)
	}
	if e := effectOf(t, f, a); e.Status != effectAbandoned || w.sent.Load() != 0 {
		t.Fatalf("effect %s, sent %d", e.Status, w.sent.Load())
	}
	// Before a decision, too: a reviewer cannot approve a payload that no longer hashes to its own.
	g := newFixture(t, false)
	if _, err := g.ask(t, w, map[string]any{"title": "fix"}, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	b := g.one(t)
	if _, err := g.g.Ledger.DB.Exec(`UPDATE blobs SET bytes = CAST(replace(CAST(bytes AS TEXT), 'fix', 'fiy') AS BLOB) WHERE sha256 = ?`,
		b.PayloadSha256); err != nil {
		t.Fatal(err)
	}
	if _, err := g.decide(t, b, Decision{Kind: DecisionApprove}); !errors.Is(err, ErrStaleView) {
		t.Fatalf("approving a changed payload: %v", err)
	}
}

// Section 10: an edit goes back to the workflow ("edited", with the reviewer's value), which
// checks it and calls again with it; that call needs an approval of its own, which supersedes the
// edited one, whose effect is abandoned: never sent. Only the edited call does that.
func TestAnEditSupersedesTheApproval(t *testing.T) {
	f := newFixture(t, false)
	w := &editable{writer{class: policy.EffectLookup}}
	f.g.Tools = tools.Registry{w.Name(): w}
	original := map[string]any{"repo": "acme/payments", "patch_ref": "sha256:" + strings.Repeat("1", 64)}
	if _, err := f.ask(t, w, original, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	a := f.one(t)
	if _, err := f.decide(t, a, Decision{Kind: DecisionEdit, Patch: []byte("not a patch")}); !errors.Is(err, ErrBadDecision) {
		t.Fatalf("an edit that is no patch: %v", err)
	}
	got, err := f.decide(t, a, Decision{Kind: DecisionEdit, Patch: []byte(editedPatch), By: "carol"})
	if err != nil || got.Status != approvalEdited {
		t.Fatalf("edit: %+v %v", got, err)
	}
	ref := "sha256:" + sha256Hex([]byte(editedPatch))
	if b, err := f.g.Ledger.GetRunBlob(context.Background(), "wf", ref); err != nil || string(b) != editedPatch {
		t.Fatalf("the edited patch is not the run's: %v", err)
	}
	// The original call again: edited, with the reviewer's patch; still not sent.
	res, err := f.ask(t, w, original, 1, time.Hour)
	if err != nil || res.GetStatus() != StatusEdited || value(t, res)["patch_ref"] != ref {
		t.Fatalf("%v %v", res, err)
	}
	// Another patch is not the edit: the run has its one write of the tool waiting already.
	other := map[string]any{"repo": "acme/payments", "patch_ref": "sha256:" + strings.Repeat("2", 64)}
	if res, err := f.ask(t, w, other, 1, time.Hour); err != nil || res.GetStatus() != StatusDenied {
		t.Fatalf("a call that is not the edited one: %v %v", res.GetStatus(), err)
	}
	// The edited call: an approval of its own, which supersedes the first.
	edited := map[string]any{"repo": "acme/payments", "patch_ref": ref}
	if res, err := f.ask(t, w, edited, 1, time.Hour); err != nil || res.GetStatus() != StatusNeedsApproval {
		t.Fatalf("the edited call: %v %v", res.GetStatus(), err)
	}
	all := f.approvals(t)
	if len(all) != 2 {
		t.Fatalf("%d approvals", len(all))
	}
	old, next := all[0], all[1]
	if old.ApprovalID != a.ApprovalID {
		old, next = next, old
	}
	if old.Status != approvalSuperseded || old.SupersededBy.String != next.ApprovalID || next.Supersedes.String != old.ApprovalID ||
		next.Status != approvalPending {
		t.Fatalf("old %+v, next %+v", old, next)
	}
	if e := effectOf(t, f, old); e.Status != effectAbandoned {
		t.Fatalf("the edited effect is %s", e.Status)
	}
	if res, err := f.ask(t, w, original, 1, time.Hour); err != nil || res.GetStatus() != StatusEdited {
		t.Fatalf("the original call after the edit: %v %v", res.GetStatus(), err)
	}
	if _, err := f.decide(t, next, Decision{Kind: DecisionApprove}); err != nil {
		t.Fatal(err)
	}
	if res, err := f.ask(t, w, edited, 1, time.Hour); err != nil || res.GetStatus() != StatusOK || w.sent.Load() != 1 {
		t.Fatalf("the approved edit: %v %v, sent %d", res.GetStatus(), err, w.sent.Load())
	}
	if res, err := f.ask(t, w, original, 1, time.Hour); err != nil || res.GetStatus() != StatusEdited || w.sent.Load() != 1 {
		t.Fatalf("the original call at the end: %v %v, sent %d", res.GetStatus(), err, w.sent.Load())
	}
}

// An edit that changes nothing would send the workflow round in circles; a write that takes no
// edits takes none.
func TestEditsThatCannotBe(t *testing.T) {
	f := newFixture(t, false)
	w := &editable{writer{class: policy.EffectLookup}}
	plain := &writer{class: policy.EffectLookup}
	f.g.Tools = tools.Registry{w.Name(): w, plain.Name(): plain}
	ref := "sha256:" + sha256Hex([]byte(editedPatch))
	if _, err := f.ask(t, w, map[string]any{"patch_ref": ref}, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ask(t, plain, map[string]any{"n": 1}, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	for _, a := range f.approvals(t) {
		if _, err := f.decide(t, a, Decision{Kind: DecisionEdit, Patch: []byte(editedPatch)}); !errors.Is(err, ErrBadDecision) {
			t.Fatalf("%s: %v", a.Tool, err)
		}
	}
}

// A cancelled run's approved write is never sent; nor is a decision on it recorded.
func TestACancelledRunsApprovalIsMoot(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectLookup}
	if _, err := f.ask(t, w, map[string]any{"n": 1}, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ask(t, w, map[string]any{"n": 2}, 2, time.Hour); err != nil {
		t.Fatal(err)
	}
	all := f.approvals(t)
	if _, err := f.decide(t, all[0], Decision{Kind: DecisionApprove}); err != nil {
		t.Fatal(err)
	}
	f.cancel(t)
	if _, err := f.decide(t, all[1], Decision{Kind: DecisionApprove}); !errors.Is(err, ErrRunOver) {
		t.Fatalf("deciding on a cancelled run: %v", err)
	}
	var approved map[string]any
	if all[0].ArgsJson == `{"n":1}` {
		approved = map[string]any{"n": 1}
	} else {
		approved = map[string]any{"n": 2}
	}
	res, err := f.ask(t, w, approved, 2, time.Hour)
	if err != nil || res.GetStatus() != StatusDenied || w.sent.Load() != 0 {
		t.Fatalf("%v %v, sent %d", res.GetStatus(), err, w.sent.Load())
	}
}

// The run's report shows its approvals: RunApprovals lists them, oldest first.
func TestRunApprovalsInOrder(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectLookup}
	for i := range 3 {
		if _, err := f.ask(t, w, map[string]any{"n": i}, 3, time.Hour); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	all := f.approvals(t)
	for i, a := range all {
		if want, _ := encodeCanonical(map[string]any{"n": json.Number(string(rune('0' + i)))}); a.ArgsJson != string(want) {
			t.Fatalf("approval %d is %s", i, a.ArgsJson)
		}
	}
}

// An approval gates an effect, and a read has none: agentd refuses at start a policy that asks
// for approval of a read, and takes one that asks it of a write.
func TestOnlyWritesAskForApproval(t *testing.T) {
	reg := tools.Registry{}
	reg.Add(&tools.ShowDiff{})
	reg.Add(&writer{class: policy.EffectUnsafe})
	read := &policy.Policy{Tools: map[string]policy.Tool{"git.show_diff": {Decision: "require_approval"}}}
	if err := CheckTools(read, reg); err == nil || !strings.Contains(err.Error(), "only reads") {
		t.Fatalf("a read that asks for approval: %v", err)
	}
	write := &policy.Policy{Tools: map[string]policy.Tool{"test.write": {Decision: "require_approval"}}}
	if err := CheckTools(write, reg); err != nil {
		t.Fatalf("a write that asks for approval: %v", err)
	}
}

// A write the policy wants approved is never sent unapproved, whenever it was recorded: one
// recorded PENDING before the policy asked (a policy changed across a restart, or a tool that
// could only dry-run then), never claimed or given back unsent, waits for a person like any other.
func TestAWriteRecordedWithoutAnApprovalWaitsForOne(t *testing.T) {
	for name, prepare := range map[string]func(*fixture, *writer, tools.Request, []byte){
		"recorded, then agentd stopped": func(f *fixture, w *writer, req tools.Request, canon []byte) {
			if res, err := f.g.record(context.Background(), f.run, w, w.Class(), 1, nil, req, canon); res != nil || err != nil {
				t.Fatalf("record: %v %v", res, err)
			}
		},
		"given back unsent": func(f *fixture, w *writer, req tools.Request, canon []byte) {
			w.unsent = true
			if _, err := f.g.effect(context.Background(), f.run, w, w.Class(), 1, nil, req, canon, faults.Effect{}); err == nil {
				t.Fatal("an unsent write did not fail the call")
			}
			w.unsent = false
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, false)
			w := &writer{class: policy.EffectLookup}
			args := map[string]any{"n": 1}
			req, canon := call(t, w, args)
			prepare(f, w, req, canon)
			res, err := f.ask(t, w, args, 1, time.Hour)
			if err != nil || res.GetStatus() != StatusNeedsApproval || w.sent.Load() != 0 {
				t.Fatalf("%v %v, sent %d", res.GetStatus(), err, w.sent.Load())
			}
			a := f.one(t)
			if e := effectOf(t, f, a); e.Status != effectAwaitingApproval || a.Status != approvalPending {
				t.Fatalf("effect %s, approval %s", e.Status, a.Status)
			}
			if _, err := f.decide(t, a, Decision{Kind: DecisionApprove}); err != nil {
				t.Fatal(err)
			}
			if res, err := f.ask(t, w, args, 1, time.Hour); err != nil || res.GetStatus() != StatusOK || w.sent.Load() != 1 {
				t.Fatalf("approved: %v %v, sent %d", res.GetStatus(), err, w.sent.Load())
			}
		})
	}
}

// describer counts the cards it describes.
type describer struct {
	writer
	described atomic.Int64
}

func (d *describer) Describe(context.Context, tools.Request, json.RawMessage) map[string]any {
	d.described.Add(1)
	time.Sleep(20 * time.Millisecond) // as GitHub's answers would take
	return map[string]any{}
}

// What a reviewer is shown is gathered only for a call the run has room for, even for calls that
// come at once, each with other arguments: one card, not one per call.
func TestCallsAtOnceGatherOneCard(t *testing.T) {
	f := newFixture(t, false)
	w := &describer{writer: writer{class: policy.EffectLookup}}
	var wg sync.WaitGroup
	statuses := make([]string, 10)
	for i := range statuses {
		wg.Go(func() {
			res, err := f.ask(t, w, map[string]any{"n": i}, 1, time.Hour)
			if err != nil {
				t.Error(err)
			}
			statuses[i] = res.GetStatus()
		})
	}
	wg.Wait()
	waiting := 0
	for _, s := range statuses {
		if s == StatusNeedsApproval {
			waiting++
		}
	}
	if waiting != 1 || w.described.Load() != 1 {
		t.Fatalf("%d calls wait for approval, %d cards gathered: %v", waiting, w.described.Load(), statuses)
	}
}

// An edit back to a patch the run asked for already would send the workflow round between two
// edited approvals: it is refused. So is an edit that came too late, and its patch is not kept
// for the run.
func TestAnEditBackToAnEarlierPatchIsRefused(t *testing.T) {
	f := newFixture(t, false)
	w := &editable{writer{class: policy.EffectLookup}}
	f.g.Tools = tools.Registry{w.Name(): w}
	first := "--- a/x\n+++ b/x\n@@ -1 +1 @@\n-a\n+b\n"
	firstRef := "sha256:" + sha256Hex([]byte(first))
	if _, err := f.ask(t, w, map[string]any{"patch_ref": firstRef}, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	a := f.one(t)
	if _, err := f.decide(t, a, Decision{Kind: DecisionEdit, Patch: []byte(editedPatch)}); err != nil {
		t.Fatal(err)
	}
	editedRef := "sha256:" + sha256Hex([]byte(editedPatch))
	if res, err := f.ask(t, w, map[string]any{"patch_ref": editedRef}, 1, time.Hour); err != nil || res.GetStatus() != StatusNeedsApproval {
		t.Fatalf("%v %v", res.GetStatus(), err)
	}
	var second db.Approval
	for _, x := range f.approvals(t) {
		if x.ApprovalID != a.ApprovalID {
			second = x
		}
	}
	if _, err := f.decide(t, second, Decision{Kind: DecisionEdit, Patch: []byte(first)}); !errors.Is(err, ErrBadDecision) {
		t.Fatalf("an edit back to the first patch: %v", err)
	}
	// Too late: the first approval was decided. The patch it carried is not the run's.
	late := "--- a/x\n+++ b/x\n@@ -1 +1 @@\n-a\n+c\n"
	var decided *DecidedError
	if _, err := f.decide(t, a, Decision{Kind: DecisionEdit, Patch: []byte(late)}); !errors.As(err, &decided) {
		t.Fatalf("an edit of a decided approval: %v", err)
	}
	if _, err := f.g.Ledger.GetRunBlob(context.Background(), "wf", "sha256:"+sha256Hex([]byte(late))); err == nil {
		t.Fatal("a refused edit's patch was kept as the run's")
	}
}

// An approval approved and then void (the run did not act on it in time) does not stand: the same
// decision sent again is told so, and the expiry keeps who approved it in its reason.
func TestAReplayedApprovalThatExpiredIsTold(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectLookup}
	if _, err := f.ask(t, w, map[string]any{"n": 1}, 1, 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	a := f.one(t)
	if _, err := f.decide(t, a, Decision{Kind: DecisionApprove, By: "bob", Key: "k"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if res, err := f.ask(t, w, map[string]any{"n": 1}, 1, time.Hour); err != nil || res.GetStatus() != StatusExpired {
		t.Fatalf("%v %v", res.GetStatus(), err)
	}
	var decided *DecidedError
	if _, err := f.decide(t, a, Decision{Kind: DecisionApprove, By: "bob", Key: "k"}); !errors.As(err, &decided) ||
		decided.Approval.Status != approvalExpired || !strings.Contains(decided.Approval.Reason.String, "approved by bob") {
		t.Fatalf("the replay: %v", err)
	}
}

// An edit sent again under its key is that edit, also once the workflow has made the edited
// call; a second reviewer's late edit is told the first.
func TestAnEditSentAgainIsTheEdit(t *testing.T) {
	f := newFixture(t, false)
	w := &editable{writer{class: policy.EffectLookup}}
	f.g.Tools = tools.Registry{w.Name(): w}
	if _, err := f.ask(t, w, map[string]any{"patch_ref": "sha256:" + strings.Repeat("1", 64)}, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	a := f.one(t)
	edit := Decision{Kind: DecisionEdit, Patch: []byte(editedPatch), By: "carol", Key: "k"}
	if _, err := f.decide(t, a, edit); err != nil {
		t.Fatal(err)
	}
	if res, err := f.ask(t, w, map[string]any{"patch_ref": "sha256:" + sha256Hex([]byte(editedPatch))}, 1, time.Hour); err != nil ||
		res.GetStatus() != StatusNeedsApproval {
		t.Fatalf("the edited call: %v %v", res.GetStatus(), err)
	}
	if got, err := f.decide(t, a, edit); err != nil || got.Status != approvalSuperseded {
		t.Fatalf("the edit again: %+v %v", got, err)
	}
	var decided *DecidedError
	if _, err := f.decide(t, a, Decision{Kind: DecisionEdit, Patch: []byte(editedPatch), By: "dave"}); !errors.As(err, &decided) {
		t.Fatalf("a second reviewer's edit: %v", err)
	}
}

// One run's slow card holds up no other run's approval, and a call that waits for its own run's
// turn gives up when its context ends.
func TestTheRecordingLockIsTheRunsAlone(t *testing.T) {
	var k keyedLock
	unlock, err := k.lock(context.Background(), "wf\x00tool")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		other, err := k.lock(ctx, fmt.Sprintf("run-%d\x00tool", i))
		cancel()
		if err != nil {
			t.Fatalf("run-%d waited on another run's lock", i)
		}
		other()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := k.lock(ctx, "wf\x00tool"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a waiter that gave up: %v", err)
	}
	unlock()
	again, err := k.lock(context.Background(), "wf\x00tool")
	if err != nil {
		t.Fatal(err)
	}
	again()
	if len(k.keys) != 0 {
		t.Fatalf("%d keys left", len(k.keys))
	}
}

// An approval holds until its write takes effect, not only until the run acts on it: an approved
// write that had not when the approval's time runs out (agentd stopped right after it moved the
// write to PENDING, or the tool sent nothing, or made one request of several and then nothing) is
// not sent, as a rejection. One that may have taken effect is in flight, and is sent again under
// its key whatever the time.
func TestAnApprovedWriteThatHadNotTakenEffectExpires(t *testing.T) {
	approve := func(f *fixture, w tools.Writer, args map[string]any) db.Approval {
		t.Helper()
		if _, err := f.ask(t, w, args, 1, 300*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if _, err := f.decide(t, f.one(t), Decision{Kind: DecisionApprove, By: "bob"}); err != nil {
			t.Fatal(err)
		}
		return f.one(t)
	}
	args := map[string]any{"n": 1}

	// Moved to PENDING, then agentd stopped before it claimed the write.
	f := newFixture(t, false)
	w := &writer{class: policy.EffectLookup}
	a := approve(f, w, args)
	if res, err := f.g.approved(context.Background(), effectOf(t, f, a), a); res != nil || err != nil {
		t.Fatalf("approved: %v %v", res, err)
	}
	time.Sleep(400 * time.Millisecond)
	res, err := f.ask(t, w, args, 1, time.Hour)
	if err != nil || res.GetStatus() != StatusExpired || w.sent.Load() != 0 {
		t.Fatalf("%v %v, sent %d", res.GetStatus(), err, w.sent.Load())
	}
	if a = f.one(t); a.Status != approvalExpired || !strings.Contains(a.Reason.String, "approved by bob") ||
		!strings.Contains(a.Reason.String, "had not taken effect") || strings.Contains(a.Reason.String, "went out") {
		t.Fatalf("approval %+v", a)
	}
	if e := effectOf(t, f, a); e.Status != effectAbandoned {
		t.Fatalf("effect %s", e.Status)
	}

	// The tool sent nothing, and gave it back: nothing went out either.
	g := newFixture(t, false)
	w = &writer{class: policy.EffectLookup, unsent: true}
	approve(g, w, args)
	if _, err := g.ask(t, w, args, 1, time.Hour); err == nil {
		t.Fatal("the unsent write did not fail the call")
	}
	w.unsent = false
	time.Sleep(400 * time.Millisecond)
	if res, err := g.ask(t, w, args, 1, time.Hour); err != nil || res.GetStatus() != StatusExpired || w.sent.Load() != 0 {
		t.Fatalf("given back unsent: %v %v, sent %d", res.GetStatus(), err, w.sent.Load())
	}

	// The tool made one request of several (a branch), and the next did nothing (GitHub refused the
	// pull request for now): the write never took effect, and is not sent past its approval's time.
	p := newFixture(t, false)
	w = &writer{class: policy.EffectLookup, after: &tools.RefusedError{Reason: "rate limited", Retryable: true}}
	approve(p, w, args)
	if _, err := p.ask(t, w, args, 1, time.Hour); err == nil {
		t.Fatal("the refused write did not fail the call")
	}
	w.after = nil
	time.Sleep(400 * time.Millisecond)
	if res, err := p.ask(t, w, args, 1, time.Hour); err != nil || res.GetStatus() != StatusExpired || w.sent.Load() != 0 {
		t.Fatalf("one request of several: %v %v, sent %d", res.GetStatus(), err, w.sent.Load())
	}
	// Its record says what went out: what that request made may remain.
	if a := p.one(t); !strings.Contains(a.Reason.String, "A request of it went out at") {
		t.Fatalf("approval %+v", a)
	}

	// A write that may have taken effect (the tool said a request went out, then it was given back,
	// as a recovery of an idempotent write does): in flight, sent again under its key after the
	// approval's time.
	h := newFixture(t, false)
	iw := &writer{class: policy.EffectIdempotent}
	a = approve(h, iw, args)
	if res, err := h.g.approved(context.Background(), effectOf(t, h, a), a); res != nil || err != nil {
		t.Fatalf("approved: %v %v", res, err)
	}
	h.sentThenGivenBack(t, a.EffectID)
	time.Sleep(400 * time.Millisecond)
	if res, err := h.ask(t, iw, args, 1, time.Hour); err != nil || res.GetStatus() != StatusOK || iw.sent.Load() != 1 {
		t.Fatalf("maybe sent: %v %v, sent %d", res.GetStatus(), err, iw.sent.Load())
	}
}

// A write claimed before the policy asked for approval, and maybe gone out (an idempotent one,
// which cannot be looked up), is neither sent again unapproved nor put up for an approval that
// could reject a write that happened: a person settles it. Found never to have happened, it waits
// for an approval like any other.
func TestAWriteThatMayBeOutWithoutAnApprovalGoesToAPerson(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectIdempotent}
	args := map[string]any{"n": 1}
	req, canon := call(t, w, args)
	if res, err := f.g.record(context.Background(), f.run, w, w.Class(), 1, nil, req, canon); res != nil || err != nil {
		t.Fatalf("record: %v %v", res, err)
	}
	f.sentThenGivenBack(t, req.EffectID)
	res, err := f.ask(t, w, args, 1, time.Hour)
	if err != nil || res.GetStatus() != StatusNeedsReconcile || w.sent.Load() != 0 || len(f.approvals(t)) != 0 {
		t.Fatalf("%v %v, sent %d, %d approvals", res.GetStatus(), err, w.sent.Load(), len(f.approvals(t)))
	}
	if _, _, err := f.g.Reconcile(context.Background(), Resolution{EffectID: req.EffectID, Executed: false, Key: "k"}); err != nil {
		t.Fatal(err)
	}
	if res, err := f.ask(t, w, args, 1, time.Hour); err != nil || res.GetStatus() != StatusNeedsApproval || w.sent.Load() != 0 {
		t.Fatalf("found never to have happened: %v %v, sent %d", res.GetStatus(), err, w.sent.Load())
	}
}

// Recovery never sends again a write the policy wants approved that nobody approved: an effect an
// earlier agentd claimed under a policy that did not ask is only looked up, and given back. One
// none of whose requests went out waits for an approval; one that may be out (an idempotent one
// cannot be looked up) waits for a person to settle it.
func TestRecoveryDoesNotResendAnUnapprovedWrite(t *testing.T) {
	for _, c := range []struct {
		class   string
		visible bool // the earlier agentd's tool said a request went out
		want    string
	}{
		{policy.EffectLookup, false, StatusNeedsApproval},
		{policy.EffectLookup, true, StatusNeedsApproval}, // the lookup found nothing: nothing went out
		{policy.EffectIdempotent, false, StatusNeedsApproval},
		{policy.EffectIdempotent, true, StatusNeedsReconcile},
	} {
		t.Run(fmt.Sprintf("%s visible %v", c.class, c.visible), func(t *testing.T) {
			class, want := c.class, c.want
			f := newFixture(t, false)
			w := &finder{writer: writer{class: class}}
			f.claimElsewhere(t, w)
			if c.visible {
				if _, err := f.g.Ledger.DB.Exec(`UPDATE effects SET maybe_sent = 1 WHERE effect_id = ?`, f.req().EffectID); err != nil {
					t.Fatal(err)
				}
			}
			req := f.req()
			canon, _ := encodeCanonical(req.Args)
			ask := func() (*agentosv1.Result, error) {
				return f.g.effect(context.Background(), f.run, w, w.Class(), 1, &approvalRule{ttl: time.Hour}, req, canon, faults.Effect{})
			}
			res, err := ask()
			if w.sent.Load() != 0 {
				t.Fatalf("sent again without an approval: %v %v", res.GetStatus(), err)
			}
			res, err = ask()
			if err != nil || res.GetStatus() != want || w.sent.Load() != 0 {
				t.Fatalf("given back: %v %v, sent %d", res.GetStatus(), err, w.sent.Load())
			}
		})
	}
}

// A run cancelled while its write was held for approval sends nothing, and waits for nobody.
func TestACancelledRunsWriteIsNotHeld(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectLookup}
	args := map[string]any{"n": 1}
	req, canon := call(t, w, args)
	if res, err := f.g.record(context.Background(), f.run, w, w.Class(), 1, nil, req, canon); res != nil || err != nil {
		t.Fatalf("record: %v %v", res, err)
	}
	f.cancel(t)
	res, err := f.ask(t, w, args, 1, time.Hour)
	if err != nil || res.GetStatus() != StatusDenied || w.sent.Load() != 0 || len(f.approvals(t)) != 0 {
		t.Fatalf("%v %v, sent %d, %d approvals", res.GetStatus(), err, w.sent.Load(), len(f.approvals(t)))
	}
	if e, err := f.g.Ledger.Q.GetEffect(context.Background(), req.EffectID); err != nil || e.Status != effectAbandoned {
		t.Fatalf("effect %s %v", e.Status, err)
	}
}

// approvedThenClaimedElsewhere records the fixture's call of w, which rule wants approved; has a
// reviewer approve it, and the next call move it to PENDING; and has an agentd that is gone claim
// it, and send a request unless it stopped first (sent): what recovery finds. It returns the call.
func (f *fixture) approvedThenClaimedElsewhere(t *testing.T, w tools.Writer, rule *approvalRule, sent bool) func() (*agentosv1.Result, error) {
	t.Helper()
	req := f.req()
	canon, _ := encodeCanonical(req.Args)
	ask := func() (*agentosv1.Result, error) {
		return f.g.effect(context.Background(), f.run, w, w.Class(), 1, rule, req, canon, faults.Effect{})
	}
	if _, err := ask(); err != nil {
		t.Fatal(err)
	}
	a := f.one(t)
	if _, err := f.decide(t, a, Decision{Kind: DecisionApprove}); err != nil {
		t.Fatal(err)
	}
	if res, err := f.g.approved(context.Background(), effectOf(t, f, a), a); res != nil || err != nil {
		t.Fatalf("approved: %v %v", res, err)
	}
	if _, err := f.g.Ledger.DB.Exec(`UPDATE effects SET claimed_by = 'gone', claimed_at = ?, maybe_sent = ?,
		version = version + 1 WHERE effect_id = ?`, ledger.Now(), sent, req.EffectID); err != nil {
		t.Fatal(err)
	}
	return ask
}

// An approved write claimed by an agentd that is gone, which may have gone out: recovery sends a
// lookup write again only within its approval's time (a lookup that finds nothing makes the resend
// a new request), and an idempotent one whatever the time (it cannot be looked up: it is in flight).
func TestRecoveryOfAnApprovedWriteAfterItsApprovalExpired(t *testing.T) {
	t.Run("lookup", func(t *testing.T) {
		f := newFixture(t, false)
		w := &finder{writer: writer{class: policy.EffectLookup}}
		ask := f.approvedThenClaimedElsewhere(t, w, &approvalRule{ttl: 300 * time.Millisecond}, true)
		time.Sleep(400 * time.Millisecond)
		// The lookup finds nothing: the effect goes back, and the next call voids the approval.
		if _, err := ask(); !errors.Is(err, errEffectReleased) || w.sent.Load() != 0 {
			t.Fatalf("recovery: %v, sent %d", err, w.sent.Load())
		}
		if res, err := ask(); err != nil || res.GetStatus() != StatusExpired || w.sent.Load() != 0 {
			t.Fatalf("%v %v, sent %d", res.GetStatus(), err, w.sent.Load())
		}
	})
	t.Run("idempotent", func(t *testing.T) {
		f := newFixture(t, false)
		w := &finder{writer: writer{class: policy.EffectIdempotent}}
		ask := f.approvedThenClaimedElsewhere(t, w, &approvalRule{ttl: 300 * time.Millisecond}, true)
		time.Sleep(400 * time.Millisecond)
		// In flight: recovery sends it again under its key, there and then.
		if res, err := ask(); err != nil || res.GetStatus() != StatusOK || w.sent.Load() != 1 {
			t.Fatalf("%v %v, sent %d", res.GetStatus(), err, w.sent.Load())
		}
	})
	t.Run("idempotent, and the agentd that is gone stopped before it sent anything", func(t *testing.T) {
		// Nothing of it went out: sent now, it would be a new request, past the approval's time.
		f := newFixture(t, false)
		w := &finder{writer: writer{class: policy.EffectIdempotent}}
		ask := f.approvedThenClaimedElsewhere(t, w, &approvalRule{ttl: 300 * time.Millisecond}, false)
		time.Sleep(400 * time.Millisecond)
		if _, err := ask(); !errors.Is(err, errEffectReleased) || w.sent.Load() != 0 {
			t.Fatalf("recovery: %v, sent %d", err, w.sent.Load())
		}
		if res, err := ask(); err != nil || res.GetStatus() != StatusExpired || w.sent.Load() != 0 {
			t.Fatalf("%v %v, sent %d", res.GetStatus(), err, w.sent.Load())
		}
	})
}

// A resend that does nothing leaves the write as it was before it: an idempotent one, which an
// earlier attempt may have made, stays in flight and goes out again under its key past its
// approval's time; a lookup one, which recovery found never took effect before it sent it again,
// never took effect, and expires.
func TestAResendThatDoesNothing(t *testing.T) {
	for class, want := range map[string]string{policy.EffectIdempotent: StatusOK, policy.EffectLookup: StatusExpired} {
		t.Run(class, func(t *testing.T) {
			f := newFixture(t, false)
			w := &finder{writer: writer{class: class, unsent: true}}
			ask := f.approvedThenClaimedElsewhere(t, w, &approvalRule{ttl: 300 * time.Millisecond}, true)
			if _, err := ask(); err == nil || errors.Is(err, errEffectReleased) {
				t.Fatalf("the resend that did nothing: %v", err)
			}
			w.unsent = false
			time.Sleep(400 * time.Millisecond)
			res, err := ask()
			if err != nil || res.GetStatus() != want || (want == StatusOK) != (w.sent.Load() == 1) {
				t.Fatalf("%v %v, sent %d", res.GetStatus(), err, w.sent.Load())
			}
		})
	}
}

// Recovery sends a write a lookup found nothing of again only once the old request can no longer
// take effect. An approval whose time ran out during that wait does not allow the new request:
// the effect goes back, and the next call voids the approval.
func TestAnApprovalThatRunsOutWhileRecoveryWaits(t *testing.T) {
	f := newFixture(t, false)
	w := &finder{writer: writer{class: policy.EffectLookup}, settle: time.Second}
	ask := f.approvedThenClaimedElsewhere(t, w, &approvalRule{ttl: 500 * time.Millisecond}, true)
	if _, err := ask(); !errors.Is(err, errEffectReleased) || w.sent.Load() != 0 {
		t.Fatalf("recovery past the approval's time: %v, sent %d", err, w.sent.Load())
	}
	if res, err := ask(); err != nil || res.GetStatus() != StatusExpired || w.sent.Load() != 0 {
		t.Fatalf("%v %v, sent %d", res.GetStatus(), err, w.sent.Load())
	}
}

// A PENDING effect whose approval is anything but approved and used cannot be: the approval moves
// it to PENDING. Should a ledger hold one, nothing sends it, neither a call nor a recovery: the
// call fails for good.
func TestAnEffectWithABadApprovalIsNotSent(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(fmt.Sprintf("claimed %v", claimed), func(t *testing.T) {
			f := newFixture(t, false)
			w := &finder{writer: writer{class: policy.EffectIdempotent}}
			ask := f.approvedThenClaimedElsewhere(t, w, &approvalRule{ttl: time.Hour}, true)
			if _, err := f.g.Ledger.DB.Exec(`UPDATE approvals SET status = 'REJECTED', reason = 'no'`); err != nil {
				t.Fatal(err)
			}
			if !claimed {
				if _, err := f.g.Ledger.DB.Exec(`UPDATE effects SET claimed_by = NULL, claimed_at = NULL`); err != nil {
					t.Fatal(err)
				}
			}
			var app *temporal.ApplicationError
			res, err := ask()
			if claimed && errors.Is(err, errEffectReleased) { // recovery only gives it back
				res, err = ask()
			}
			if !errors.As(err, &app) || app.Type() != "BadApproval" || !app.NonRetryable() || w.sent.Load() != 0 {
				t.Fatalf("%v %v, sent %d", res.GetStatus(), err, w.sent.Load())
			}
		})
	}
}

// unknownThenUnsent's first attempt makes a visible request and then cannot tell what came of it;
// its second does nothing; later ones send.
type unknownThenUnsent struct {
	writer
	attempts atomic.Int64
}

func (w *unknownThenUnsent) Execute(ctx context.Context, payload json.RawMessage, key string, proceed tools.Proceed) (map[string]any, error) {
	switch w.attempts.Add(1) {
	case 1:
		if err := proceed(ctx); err != nil {
			return nil, err
		}
		return nil, errors.New("connection reset")
	case 2:
		return nil, tools.ErrNotSent
	}
	return w.writer.Execute(ctx, payload, key, proceed)
}

// An idempotent write whose outcome an attempt could not tell is in flight: sent again under its
// key at once, and when that resend does nothing, still in flight, past its approval's time too.
func TestAnAttemptThatMayHaveTakenEffectStaysInFlight(t *testing.T) {
	f := newFixture(t, false)
	w := &unknownThenUnsent{writer: writer{class: policy.EffectIdempotent}}
	args := map[string]any{"n": 1}
	if _, err := f.ask(t, w, args, 1, 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := f.decide(t, f.one(t), Decision{Kind: DecisionApprove}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ask(t, w, args, 1, time.Hour); err == nil || w.attempts.Load() != 2 {
		t.Fatalf("the resend that did nothing: %v, %d attempts", err, w.attempts.Load())
	}
	time.Sleep(400 * time.Millisecond)
	if res, err := f.ask(t, w, args, 1, time.Hour); err != nil || res.GetStatus() != StatusOK || w.sent.Load() != 1 {
		t.Fatalf("%v %v, sent %d", res.GetStatus(), err, w.sent.Load())
	}
}
