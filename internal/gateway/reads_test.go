package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tools"
)

// reader is a lookup tool whose Find and Describe each send one request through tools.ReadsOf, as
// githubtool's client does. As each request goes out, it notes the ledger's newest record, and it
// counts the requests that went out with nothing to record them.
type reader struct {
	finder
	f          *fixture
	newest     []db.AuditLog
	unrecorded int
	sent       int // requests that went out
}

func (r *reader) request(ctx context.Context, path string) error {
	if reads := tools.ReadsOf(ctx); reads == nil {
		r.unrecorded++
	} else if err := reads(ctx, "GET", path); err != nil {
		return err
	}
	r.sent++
	last, err := r.f.g.Ledger.Q.LastAuditRecord(ctx)
	if err != nil {
		return err
	}
	r.newest = append(r.newest, last)
	return nil
}

func (r *reader) Find(ctx context.Context, payload json.RawMessage) (map[string]any, bool, error) {
	if err := r.request(ctx, "/repos/o/r/commits/abc/comments"); err != nil {
		return nil, false, err
	}
	return r.finder.Find(ctx, payload)
}

func (r *reader) Describe(ctx context.Context, _ tools.Request, _ json.RawMessage) map[string]any {
	if err := r.request(ctx, "/repos/o/r/git/ref/heads/main"); err != nil {
		return map[string]any{"error": err.Error()}
	}
	return map[string]any{}
}

// read checks that each request r sent went out after its agentd.read record.
func (r *reader) read(t *testing.T, purpose, effectID, path string) {
	t.Helper()
	if r.unrecorded != 0 || len(r.newest) == 0 {
		t.Fatalf("%d request(s) went out unrecorded, %d recorded", r.unrecorded, len(r.newest))
	}
	want := map[string]any{"purpose": purpose, "effect_id": effectID, "tool": r.Name(), "method": "GET", "path": path}
	for _, last := range r.newest {
		var p map[string]any
		if err := json.Unmarshal([]byte(last.Payload), &p); err != nil {
			t.Fatal(err)
		}
		if last.Type != "agentd.read" || last.Actor != audit.Agentd || last.WorkflowID != "wf" || !maps.Equal(p, want) {
			t.Fatalf("a request went out after record %d, %s by %s of %s %v; want agentd.read %v",
				last.Seq, last.Type, last.Actor, last.WorkflowID, p, want)
		}
	}
	r.newest = nil
}

// Each request of agentd's own reads goes out after its agentd.read record (tools.Reads): recovery's
// lookup of an effect in doubt, a reconciliation's lookup, and an approval card's requests.
func TestAgentdsOwnReadsAreRecordedFirst(t *testing.T) {
	ctx := context.Background()
	effectID := strings.Repeat("e", 64)
	t.Run("recovery's lookup", func(t *testing.T) {
		f := newFixture(t, false)
		r := &reader{finder: finder{writer: writer{class: policy.EffectLookup}, found: map[string]any{"id": 7}, settle: time.Hour}, f: f}
		f.g.Tools.Add(r)
		f.claimElsewhere(t, r)
		if _, err := f.g.recoverClaimed(ctx, effectID); err != nil {
			t.Fatal(err)
		}
		r.read(t, "lookup", effectID, "/repos/o/r/commits/abc/comments")
		if f.status(t).Status != effectSucceeded {
			t.Fatalf("effect %s", f.status(t).Status)
		}
	})
	t.Run("recovery's lookup again, after the wait", func(t *testing.T) {
		f := newFixture(t, false)
		r := &reader{finder: finder{writer: writer{class: policy.EffectLookup}, settle: 50 * time.Millisecond}, f: f}
		f.g.Tools.Add(r)
		f.claimElsewhere(t, r)
		if _, err := f.g.recoverClaimed(ctx, effectID); !errors.Is(err, errEffectReleased) {
			t.Fatalf("err %v", err)
		}
		if len(r.newest) != 2 {
			t.Fatalf("%d lookups recorded, want the first and the one after the wait", len(r.newest))
		}
		r.read(t, "lookup", effectID, "/repos/o/r/commits/abc/comments")
	})
	t.Run("a reconciliation's lookup", func(t *testing.T) {
		f := newFixture(t, false)
		r := &reader{finder: finder{writer: writer{class: policy.EffectLookup}, findErr: errors.New("rate limited"), settle: time.Hour}, f: f}
		f.g.Tools.Add(r)
		f.claimElsewhere(t, r)
		if status, err := f.do(t, r); err != nil || status != StatusNeedsReconcile {
			t.Fatalf("status %s, err %v", status, err)
		}
		r.read(t, "lookup", effectID, "/repos/o/r/commits/abc/comments")
		r.findErr, r.found = nil, map[string]any{"id": 7}
		if status, _, err := f.g.Reconcile(ctx, Resolution{By: operator, EffectID: effectID, Executed: true, Lookup: true, Key: "a"}); err != nil ||
			status != effectSucceeded {
			t.Fatalf("status %s, err %v", status, err)
		}
		r.read(t, "reconcile", effectID, "/repos/o/r/commits/abc/comments")
	})
	t.Run("an approval card", func(t *testing.T) {
		f := newFixture(t, false)
		r := &reader{finder: finder{writer: writer{class: policy.EffectLookup}}, f: f}
		args := map[string]any{"n": 1}
		req, _ := call(t, r, args)
		if res, err := f.ask(t, r, args, 1, time.Hour); err != nil || res.GetStatus() != StatusNeedsApproval {
			t.Fatalf("%v %v", res, err)
		}
		r.read(t, "approval_card", req.EffectID, "/repos/o/r/git/ref/heads/main")
	})
	t.Run("a read whose record cannot be written does not go out", func(t *testing.T) {
		f := newFixture(t, false)
		r := &reader{finder: finder{writer: writer{class: policy.EffectLookup}, found: map[string]any{"id": 7}, settle: time.Hour}, f: f}
		f.g.Tools.Add(r)
		f.claimElsewhere(t, r)
		failAppends(t, f.g.Ledger, "agentd.read")
		if _, err := f.g.recoverClaimed(ctx, effectID); err != nil {
			t.Fatal(err)
		}
		if r.sent != 0 || r.unrecorded != 0 || f.status(t).Status != effectUnknown {
			t.Fatalf("%d sent, %d unrecorded, effect %s", r.sent, r.unrecorded, f.status(t).Status)
		}
	})
}
