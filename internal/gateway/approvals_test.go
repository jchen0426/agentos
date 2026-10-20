package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/faults"
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
