package gateway

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tools"
)

// offline is a write whose tool cannot reach its system: it only dry-runs.
type offline struct{ writer }

func (*offline) Live() bool { return false }

// The report shows an approved write that did not go out as approved as not sent: also one that an
// agentd which could not reach the tool's system took up, and only dry-ran (the approval was decided
// under one that could). Approved alone, it would read as if it went out. A request an earlier
// attempt made of it, which did not take effect, the report names: what it made may remain.
func TestTheReportOfAnApprovedWriteThatOnlyDryRan(t *testing.T) {
	for _, earlier := range []bool{false, true} {
		t.Run(fmt.Sprintf("a request went out before %v", earlier), func(t *testing.T) {
			f := newFixture(t, false)
			ctx := context.Background()
			args := map[string]any{"n": 1}
			if _, err := f.ask(t, &writer{class: policy.EffectLookup}, args, 1, time.Hour); err != nil {
				t.Fatal(err)
			}
			if _, err := f.decide(t, f.one(t), Decision{Kind: DecisionApprove}); err != nil {
				t.Fatal(err)
			}
			if earlier {
				// A request of a write of two (a branch) went out, and the next was refused for now.
				w := &writer{class: policy.EffectLookup, after: &tools.RefusedError{Reason: "rate limited", Retryable: true}}
				if _, err := f.ask(t, w, args, 1, time.Hour); err == nil {
					t.Fatal("the refused write did not fail the call")
				}
			}
			// A dry run asks for no approval (dispatch): the call comes without a rule.
			w := &offline{writer{class: policy.EffectLookup}}
			req, canon := call(t, w, args)
			if res, err := f.g.effect(ctx, f.run, w, w.Class(), 1, nil, req, canon, faults.Effect{}); err != nil || res.GetStatus() != StatusOK {
				t.Fatalf("%v %v", res, err)
			}
			if e := effectOf(t, f, f.one(t)); e.Status != effectDryRun || w.sent.Load() != 0 {
				t.Fatalf("effect %s, sent %d", e.Status, w.sent.Load())
			}
			got, err := RunRecords{f.g.Ledger}.RunApprovals(ctx, "wf")
			if err != nil || len(got) != 1 || got[0].Status != approvalApproved || got[0].Write != "not sent" ||
				!strings.Contains(got[0].WriteReason, "only dry-ran") ||
				strings.Contains(got[0].WriteReason, "A request of it went out at") != earlier {
				t.Fatalf("%+v %v", got, err)
			}
		})
	}
}
