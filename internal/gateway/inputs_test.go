package gateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
)

// carried is what a subverted Worker would put in a model's prompt: words of its own.
const carried = "CARRIED_ghp_0123456789abcdef"

// A model call's arguments are its task's inputs, each a blob of the run's own, and no others
// (models.yaml): a call with another argument, without its input, with a ref that is not the run's
// own, or with an input that is no ref is refused for good, as a tool's call with arguments its
// schema refuses: before the policy is asked, and before anything is reserved, counted or sent. Its
// error names the task's own words only.
func TestAModelCallTakesOnlyItsInputs(t *testing.T) {
	ctx := context.Background()
	for name, args := range map[string]func(own, others string) map[string]any{
		"another argument besides": func(own, _ string) map[string]any { return map[string]any{"diff_ref": own, "note": carried} },
		"another ref besides":      func(own, _ string) map[string]any { return map[string]any{"diff_ref": own, carried + "_ref": own} },
		"no argument":              func(string, string) map[string]any { return map[string]any{} },
		"another argument instead": func(own, _ string) map[string]any { return map[string]any{"note_ref": own} },
		"another run's blob":       func(_, others string) map[string]any { return map[string]any{"diff_ref": others} },
		"a blob nobody stored": func(string, string) map[string]any {
			return map[string]any{"diff_ref": "sha256:" + strings.Repeat("c", 64)}
		},
		"no string":              func(string, string) map[string]any { return map[string]any{"diff_ref": 7} },
		"no ref":                 func(string, string) map[string]any { return map[string]any{"diff_ref": carried} },
		"a ref of another shape": func(string, string) map[string]any { return map[string]any{"diff_ref": "sha256:" + carried} },
		// An owned blob's hash without its sha256: prefix: blob_owners holds the bare hex, so the
		// ownership lookup (which trims the prefix) would take it; only the shape check refuses it.
		"the run's own blob, bare hex": func(own, _ string) map[string]any {
			return map[string]any{"diff_ref": strings.TrimPrefix(own, "sha256:")}
		},
	} {
		t.Run(name, func(t *testing.T) {
			g, run, _ := fallbackGateway(t)
			fake := &counted{Adapter: models.Fake{}}
			g.Adapters = map[string]models.Adapter{"fake": fake, "other": fake}
			own, err := audit.PutRunBlob(ctx, g.Ledger, run.WorkflowID, []byte("diff --git a/app.py b/app.py\n"))
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Ledger.Q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "another-run", Task: "security-audit", ParamsJson: "{}",
				BudgetLimitMicros: 1, CreatedAt: ledger.Now()}); err != nil {
				t.Fatal(err)
			}
			others, err := audit.PutRunBlob(ctx, g.Ledger, "another-run", []byte("another repository's code"))
			if err != nil {
				t.Fatal(err)
			}
			a := args(own, others)
			canon, err := encodeCanonical(a)
			if err != nil {
				t.Fatal(err)
			}
			info := activity.Info{ActivityID: "1:audit_v1", Attempt: 1, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 5},
				ScheduledTime: time.Unix(1000, 0)}
			info.WorkflowExecution.ID = run.WorkflowID
			rec := &callRecord{argsSHA256: sha256Hex(canon)}
			call := &agentosv1.Call{Kind: "model", Name: "audit_v1", ArgsJson: string(canon)}
			_, err = g.dispatch(ctx, info, run, call, a, canon, faults.Effect{}, rec)
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || app.Type() != "BadArguments" || !app.NonRetryable() {
				t.Fatalf("%v", err)
			}
			if strings.Contains(err.Error(), carried) || strings.Contains(err.Error(), "note") || strings.Contains(err.Error(), own) {
				t.Errorf("the error quotes the call: %v", err)
			}
			if fake.counts != 0 || fake.calls != 0 || rec.policy != nil || rec.startedSeq != 0 {
				t.Errorf("counted %d, sent %d, policy %v, call.started %d", fake.counts, fake.calls, rec.policy, rec.startedSeq)
			}
			if entries, err := g.Ledger.Q.ListBudgetEntriesAll(ctx); err != nil || len(entries) != 0 {
				t.Errorf("budget entries %v %v", entries, err)
			}
			if rows, err := g.Ledger.Q.ListRunModelCalls(ctx, db.ListRunModelCallsParams{WorkflowID: run.WorkflowID, Limit: 10}); err != nil ||
				len(rows) != 0 {
				t.Errorf("model calls %v %v", rows, err)
			}
		})
	}
	// The call as the Worker makes it goes through.
	g, run, _ := fallbackGateway(t)
	fake := &counted{Adapter: models.Fake{}}
	g.Adapters = map[string]models.Adapter{"fake": fake, "other": fake}
	call, args, canon := diffCall(t, g, run)
	info := activity.Info{ActivityID: "1:audit_v1", Attempt: 1, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 5},
		ScheduledTime: time.Unix(1000, 0)}
	info.WorkflowExecution.ID = run.WorkflowID
	if res, err := g.dispatch(ctx, info, run, call, args, canon, faults.Effect{}, &callRecord{argsSHA256: sha256Hex(canon)}); err != nil ||
		res.GetStatus() != StatusOK || fake.calls != 1 {
		t.Fatalf("%v %v, %d sent", res, err, fake.calls)
	}
}

// A task of two inputs takes two blobs: not one of them twice.
func TestAModelTaskOfTwoInputsTakesTwoBlobs(t *testing.T) {
	g, run, _ := fallbackGateway(t)
	ctx := context.Background()
	put := func(b string) string {
		ref, err := audit.PutRunBlob(ctx, g.Ledger, run.WorkflowID, []byte(b))
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	route := models.Route{Task: "compare_v1", Inputs: []string{"old_ref", "new_ref"}}
	one, two := put("one"), put("two")
	if err := g.inputs(ctx, run, route, map[string]any{"old_ref": one, "new_ref": two}); err != nil {
		t.Fatal(err)
	}
	var app *temporal.ApplicationError
	if err := g.inputs(ctx, run, route, map[string]any{"old_ref": one, "new_ref": one}); !errors.As(err, &app) ||
		app.Type() != "BadArguments" {
		t.Fatalf("one blob twice: %v", err)
	}
}

// Two runs whose model call differs only in what the workflow added to it, besides its input, post
// the same report, byte for byte: the call is refused before the model sees any of it, and the
// report says only that analyze failed.
func TestAReportPostsNothingAWorkerAddsToAModelCall(t *testing.T) {
	schema, err := filepath.Abs("../../config/schemas/audit_report.json")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "models.yaml")
	if err := os.WriteFile(path, []byte("providers:\n  fake: {adapter: fake}\ntasks:\n  audit_v1: {model: fake/audit-fixture, max_tokens: 4096, "+
		"inputs: [diff_ref], output_schema: "+schema+", blobs: [patch]}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mods, err := models.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().Add(time.Hour).Truncate(time.Second)
	var got [2]string
	for i, added := range []string{"a", strings.Repeat(carried+" \"\n", 60)} {
		g, l := reporting(t)
		g.Models = mods
		ref, err := audit.PutRunBlob(context.Background(), l, testWorkflowID, []byte("diff --git a/app.py b/app.py\n"))
		if err != nil {
			t.Fatal(err)
		}
		var h fakeHistory
		h.through(t, base, g, l, &agentosv1.Call{Kind: "model", Name: "audit_v1",
			ArgsJson: `{"diff_ref":"` + ref + `","note":` + quote(added) + `}`})
		got[i] = posted(t, g, l, &h, map[string]any{"repo": "o/r", "commit": strings.Repeat("a", 40), "reason": "done"})
	}
	if got[0] != got[1] || strings.Contains(got[1], carried) || !strings.Contains(got[1], "analyze failed; agentd's log has the details") {
		t.Fatalf("the comments:\n%s\n%s", got[0], got[1])
	}
}

// quote is s as a JSON string.
func quote(s string) string {
	b, err := encodeCanonical(map[string]any{"s": s})
	if err != nil {
		panic(err)
	}
	return strings.TrimSuffix(strings.TrimPrefix(string(b), `{"s":`), "}")
}
