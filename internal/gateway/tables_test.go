package gateway

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/models"
	"github.com/jchen0426/agentos/internal/policy"
)

// everywhere are the differences between the tables and the records, rows made since the epoch
// included, runs and their tables too: as VerifyAudit compares them.
func everywhere(t *testing.T, l *ledger.Ledger) []audit.Difference {
	t.Helper()
	var p audit.Projection
	if err := p.Read(context.Background(), l.Q); err != nil {
		t.Fatal(err)
	}
	diffs, err := p.Differences(context.Background(), l.Q, audit.Since{At: time.Unix(0, 0), Runs: true})
	if err != nil {
		t.Fatal(err)
	}
	return diffs
}

func sayingDiff(d []audit.Difference, kind, why string) bool {
	return slices.ContainsFunc(d, func(d audit.Difference) bool { return d.Kind == kind && strings.Contains(d.Why, why) })
}

// The calls the loop guard counts (calls) are the ones the records count: a row deleted, which
// would let a run past its limits again, is found; so is one changed, or its outcome.
func TestTheRecordsCountTheCalls(t *testing.T) {
	for name, c := range map[string]struct{ sql, why string }{
		"deleted":             {`DELETE FROM calls`, "and the ledger has no such call"},
		"its arguments":       {`UPDATE calls SET args_sha256 = 'x'`, "is counted as \"tool\""},
		"its outcome cleared": {`UPDATE calls SET outcome = NULL`, `has the outcome ""`},
		"one never counted": {`INSERT INTO calls (workflow_id, activity_id, scheduled_at, kind, name, args_sha256, created_at)
			VALUES ('` + testWorkflowID + `', '9:x', 'then', 'tool', 'x', 'a', '2026-01-01T00:00:00Z')`, "is counted (calls) and is not in the audit log"},
	} {
		t.Run(name, func(t *testing.T) {
			g, l, w := invokable(t)
			if _, err := invoke(g, &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{}`}); err != nil {
				t.Fatal(err)
			}
			if d := everywhere(t, l); slices.ContainsFunc(d, func(d audit.Difference) bool { return d.Kind == "call" }) {
				t.Fatalf("before: %v", d)
			}
			if _, err := l.DB.Exec(c.sql); err != nil {
				t.Fatal(err)
			}
			if d := everywhere(t, l); !sayingDiff(d, "call", c.why) {
				t.Fatalf("no call difference saying %q: %v", c.why, d)
			}
		})
	}
}

// A model call's reservation and charge (budget_entries), its request and answer (model_calls), and
// the blobs its answer made the run's (blob_owners) are each the records': changed or removed, each
// is found.
func TestTheRecordsCoverAModelCall(t *testing.T) {
	model := func(t *testing.T) *Gateway {
		g, run, _ := fallbackGateway(t)
		g.Adapters = map[string]models.Adapter{"fake": models.Fake{}, "other": models.Fake{}}
		call := &agentosv1.Call{Kind: "model", Name: "audit_v1", ArgsJson: `{"diff":"x"}`}
		args := map[string]any{"diff": "x"}
		canon, _ := encodeCanonical(args)
		info := activity.Info{ActivityID: "1:audit_v1", Attempt: 1, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 5},
			ScheduledTime: time.Unix(1000, 0)}
		info.WorkflowExecution.ID = run.WorkflowID
		if _, err := g.dispatch(context.Background(), info, run, call, args, canon, faults.Effect{}, &callRecord{argsSHA256: sha256Hex(canon)}); err != nil {
			t.Fatal(err)
		}
		return g
	}
	for name, c := range map[string]struct{ sql, kind, why string }{
		"the reservation's amount": {`UPDATE budget_entries SET amount_micros = amount_micros + 1 WHERE kind = 'RESERVE'`, "budget",
			"the reservation of request"},
		"the charge removed": {`DELETE FROM budget_entries WHERE kind = 'SETTLE'`, "budget", "records the charge of request"},
		"a reservation never recorded": {`INSERT INTO budget_entries (workflow_id, kind, amount_micros, request_id, activity_id, attempt,
			model, price_table_version, created_at) VALUES ('wf', 'RESERVE', 5, 'made-up', '9:x', 1, 'm', 'v', '2026-01-01T00:00:00Z')`,
			"budget", `the reservation of request "made-up", 5 micros`},
		"the request's blob": {`UPDATE model_calls SET request_blob = 'sha256:00'`, "model", "and the audit log says"},
		"the answer's stop":  {`UPDATE model_calls SET stop_reason = 'x'`, "model", "the answer to request 1"},
		"the usage":          {`UPDATE model_calls SET usage_json = '[]'`, "model", "the answer to request 1"},
		"the request gone":   {`DELETE FROM model_calls`, "model", "and the ledger has no such request"},
		"the run's blob":     {`DELETE FROM blob_owners`, "blob", "and the ledger has no such owner"},
	} {
		t.Run(name, func(t *testing.T) {
			g := model(t)
			// The fixture made its run itself, without run.created: only that is not in the log.
			if d := everywhere(t, g.Ledger); len(slices.DeleteFunc(d, func(d audit.Difference) bool { return d.Kind == "run" })) != 0 {
				t.Fatalf("before: %v", d)
			}
			if _, err := g.Ledger.DB.Exec(c.sql); err != nil {
				t.Fatal(err)
			}
			if d := everywhere(t, g.Ledger); !sayingDiff(d, c.kind, c.why) {
				t.Fatalf("no %s difference saying %q: %v", c.kind, c.why, d)
			}
		})
	}
}

// A run's blob owned without its record, for a run the log made: it may read it into a prompt, and
// nothing says so.
func TestARunsBlobWithoutItsRecord(t *testing.T) {
	g, l := startingRun(t, time.Now())
	ctx := context.Background()
	if _, err := audit.Record(ctx, l, audit.Event{WorkflowID: testWorkflowID, Type: "run.created", Actor: audit.Operator("t"),
		Payload: map[string]any{"task": "security-audit", "params": map[string]string{}, "budget_limit_micros": 0, "dry_run": false}}); err != nil {
		t.Fatal(err)
	}
	ref, err := l.PutBlob(ctx, []byte("someone else's"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.DB.Exec(`INSERT INTO blob_owners (workflow_id, sha256) VALUES (?, ?)`, testWorkflowID, strings.TrimPrefix(ref, "sha256:")); err != nil {
		t.Fatal(err)
	}
	_ = g
	if d := everywhere(t, l); !sayingDiff(d, "blob", "and the audit log does not say so") {
		t.Fatalf("differences %v", d)
	}
}

// A person's finding about an effect (effect_resolutions) is the record's.
func TestTheRecordsCoverAFinding(t *testing.T) {
	for name, c := range map[string]struct{ sql, why string }{
		"its status":  {`UPDATE effect_resolutions SET status = 'ABANDONED'`, "the finding"},
		"its removal": {`DELETE FROM effect_resolutions`, "and the ledger has no such finding"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, false)
			f.direct = true
			w := &writer{class: policy.EffectUnsafe}
			f.g.Tools.Add(w)
			f.claimElsewhere(t, w)
			if status, err := f.do(t, w); err != nil || status != StatusNeedsReconcile {
				t.Fatalf("status %s, err %v", status, err)
			}
			if _, _, err := f.g.Reconcile(context.Background(), Resolution{By: operator, EffectID: f.req().EffectID, Executed: true,
				ResultJSON: `{"id": 3}`, Key: "k"}); err != nil {
				t.Fatal(err)
			}
			if _, err := f.g.Ledger.DB.Exec(c.sql); err != nil {
				t.Fatal(err)
			}
			if d := differences(t, f.g.Ledger); !sayingDiff(d, "effect", c.why) {
				t.Fatalf("no effect difference saying %q: %v", c.why, d)
			}
		})
	}
}

// What effect.recorded and approval.created say of a write is compared too: its tool and class
// (the class decides how it is recovered: an idempotent effect cut off in flight is sent again
// under its key, without a lookup, which for a comment is a second comment), its idempotency key,
// and the card its reviewer was shown.
func TestTheRecordsCoverAWritesClassKeyAndCard(t *testing.T) {
	for name, c := range map[string]struct{ sql, kind, why string }{
		"its class":           {`UPDATE effects SET effect_class = 'idempotent'`, "effect", `of class "idempotent", and the audit log says`},
		"its tool":            {`UPDATE effects SET tool = 'github.comment_commit'`, "effect", `a write of "github.comment_commit"`},
		"its key":             {`UPDATE effects SET idempotency_key = 'agentos-other'`, "effect", `has idempotency key "agentos-other"`},
		"the card":            {`UPDATE approvals SET context_blob = 'sha256:00'`, "approval", `shows the reviewer "sha256:00"`},
		"the approval's tool": {`UPDATE approvals SET tool = 'github.comment_commit'`, "approval", `of a write of "github.comment_commit"`},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, false)
			w := &writer{class: policy.EffectLookup}
			asked(t, f, w, time.Hour)
			if d := everywhere(t, f.g.Ledger); slices.ContainsFunc(d, func(d audit.Difference) bool { return d.Kind != "run" }) {
				t.Fatalf("before: %v", d) // the fixture's run has no record of its own
			}
			if _, err := f.g.Ledger.DB.Exec(c.sql); err != nil {
				t.Fatal(err)
			}
			f.direct = true // changed by the test: its end does not check the log
			if d := everywhere(t, f.g.Ledger); !sayingDiff(d, c.kind, c.why) {
				t.Fatalf("no %s difference saying %q: %v", c.kind, c.why, d)
			}
		})
	}
}

// A row says its own date: an effect and an approval dated 2000, added to a run whose creation the
// log records, are not exempt as made before the log began. Only rows of a run made before the
// log, which no record speaks of, are.
func TestRowsOfARunTheLogCreatedNeedRecordsWhateverTheirDate(t *testing.T) {
	f := newFixture(t, false)
	ctx := context.Background()
	l := f.g.Ledger
	if err := l.Tx(ctx, func(q *db.Queries) error {
		return note(ctx, q, "wf", "run.created", audit.Operator("tester"), map[string]any{"task": "security-audit",
			"params": map[string]string{}, "budget_limit_micros": 1, "dry_run": false})
	}); err != nil {
		t.Fatal(err)
	}
	since := audit.Since{At: time.Now().Add(-time.Minute), Runs: false}
	differences := func() []audit.Difference {
		var p audit.Projection
		if err := p.Read(ctx, l.Q); err != nil {
			t.Fatal(err)
		}
		d, err := p.Differences(ctx, l.Q, since)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	if _, err := l.DB.Exec(`INSERT INTO effects (effect_id, workflow_id, tool, effect_class, payload_sha256, idempotency_key, status,
		created_at, updated_at) VALUES ('e-old', 'wf', 'test.write', 'idempotent', 'x', 'k', 'SUCCEEDED', '2000-01-01T00:00:00Z',
		'2000-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := l.DB.Exec(`INSERT INTO approvals (approval_id, effect_id, workflow_id, tool, args_json, payload_sha256,
		context_blob, status, decided_by, decided_at, used_at, created_at, expires_at) VALUES ('a-old', 'e-old', 'wf', 'test.write',
		'{}', 'x', 'sha256:00', 'APPROVED', 'mallory', '2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z',
		'2001-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	d := differences()
	if !sayingDiff(d, "effect", "effect e-old, made at 2000-01-01T00:00:00Z, is not in the audit log") ||
		!sayingDiff(d, "approval", "approval a-old, made at 2000-01-01T00:00:00Z, is not in the audit log") {
		t.Fatalf("differences %v", d)
	}
	// The same rows, of a run made before the log began, which no record speaks of: exempt.
	if _, err := l.DB.Exec(`UPDATE runs SET created_at = '1999-01-01T00:00:00Z'; DELETE FROM audit_log`); err != nil {
		t.Fatal(err)
	}
	f.direct = true // the log is the test's own now
	if err := l.Tx(ctx, func(q *db.Queries) error { return note(ctx, q, "", "agentd.started", audit.Agentd, nil) }); err == nil {
		t.Log("appended after the log was emptied") // the tip refuses this ledger's next append: either is fine here
	}
	if d := differences(); sayingDiff(d, "effect", "e-old") || sayingDiff(d, "approval", "a-old") {
		t.Fatalf("differences %v", d)
	}
}
