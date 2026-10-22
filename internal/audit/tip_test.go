package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

func exec(t *testing.T, l *ledger.Ledger, stmt string, args ...any) {
	t.Helper()
	if _, err := l.DB.Exec(stmt, args...); err != nil {
		t.Fatal(err)
	}
}

// What this agentd appended last is where the chain ends, and the head it pushed last is in it: a
// log cut under it, emptied, rewritten, or appended to by someone else is not chained onto, nor its
// head pushed, and whoever listens hears of it once.
func TestAppendOnlyOntoTheChainAsThisAgentdLeftIt(t *testing.T) {
	ctx := context.Background()
	for name, change := range map[string]func(t *testing.T, l *ledger.Ledger, pushed Head){
		"its last record cut": func(t *testing.T, l *ledger.Ledger, _ Head) {
			exec(t, l, `DELETE FROM audit_log WHERE seq = (SELECT MAX(seq) FROM audit_log)`)
		},
		"emptied": func(t *testing.T, l *ledger.Ledger, _ Head) { exec(t, l, `DELETE FROM audit_log`) },
		"appended to by someone else": func(t *testing.T, l *ledger.Ledger, _ Head) {
			var seq int
			var name, file string
			if err := l.DB.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &file); err != nil {
				t.Fatal(err)
			}
			other, err := ledger.Open(ctx, file)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			if _, err := Record(ctx, other, Event{Type: "agentd.started", Actor: Agentd}); err != nil {
				t.Fatal(err)
			}
		},
		"the pushed head rewritten": func(t *testing.T, l *ledger.Ledger, pushed Head) {
			exec(t, l, `UPDATE audit_log SET sha256 = ? WHERE seq = ?`, Genesis, pushed.Seq)
		},
	} {
		t.Run(name, func(t *testing.T) {
			l := open(t)
			var heard []error
			OnChainMoved(l, func(err error) { heard = append(heard, err) })
			tc := &signaler{}
			a := &Anchors{Ledger: l, Temporal: tc, Every: time.Hour}
			heads := chain(t, l, 2)
			a.Push(ctx)
			if _, pushed := Tip(l); pushed != heads[1] {
				t.Fatalf("pushed %v, want %v", pushed, heads[1])
			}
			chain(t, l, 1)
			change(t, l, heads[1])
			for range 2 {
				if _, err := Record(ctx, l, Event{Type: "agentd.started", Actor: Agentd}); !errors.Is(err, ErrChainMoved) {
					t.Fatalf("appended after the change: %v", err)
				}
			}
			a.Push(ctx)
			if n := len(tc.got()); n != 1 {
				t.Fatalf("%d pushes, want only the one before the change", n)
			}
			if len(heard) != 1 || !errors.Is(heard[0], ErrChainMoved) {
				t.Fatalf("heard %v", heard)
			}
		})
	}
}

// Appends of one transaction chain onto each other; a transaction rolled back leaves nothing this
// agentd would look for.
func TestAppendsOfOneTransactionAndOfOneRolledBack(t *testing.T) {
	ctx := context.Background()
	l := open(t)
	chain(t, l, 1)
	two := func(q *db.Queries) error {
		for range 2 {
			if _, err := Append(ctx, q, Event{Type: "agentd.started", Actor: Agentd}); err != nil {
				return err
			}
		}
		return nil
	}
	no := errors.New("rolled back")
	if err := l.Tx(ctx, func(q *db.Queries) error { return errors.Join(two(q), no) }); !errors.Is(err, no) {
		t.Fatal(err)
	}
	if err := l.Tx(ctx, two); err != nil {
		t.Fatal(err)
	}
	h, err := Record(ctx, l, Event{Type: "agentd.started", Actor: Agentd})
	if err != nil {
		t.Fatal(err)
	}
	if appended, _ := Tip(l); appended != h || h.Seq != 4 {
		t.Fatalf("appended %v, record %v", appended, h)
	}
}

// history is an anchor workflow's events, for reader.
type history []*historypb.HistoryEvent

type events struct {
	h   history
	err error
}

func (e *events) HasNext() bool { return e.err != nil || len(e.h) > 0 }
func (e *events) Next() (*historypb.HistoryEvent, error) {
	if e.err != nil {
		err := e.err
		e.err = nil
		return nil, err
	}
	next := e.h[0]
	e.h = e.h[1:]
	return next, nil
}

// reader is a Temporal with the histories of workflows, which lists those of listed, or one that
// fails to give any history.
type reader struct {
	workflows map[string]history
	listed    []string
	failing   error         // what every history read fails with
	listErr   error         // what every listing fails with
	narrowErr error         // what a listing narrowed by start time fails with
	slow      time.Duration // each history read takes this long
	queries   []string
	reads     int
}

func (r *reader) ListWorkflow(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	r.queries = append(r.queries, req.GetQuery())
	switch {
	case r.listErr != nil:
		return nil, r.listErr
	case r.narrowErr != nil && strings.Contains(req.GetQuery(), "StartTime"):
		return nil, r.narrowErr
	}
	var out []*workflowpb.WorkflowExecutionInfo
	for _, id := range r.listed {
		out = append(out, &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id}})
	}
	return &workflowservice.ListWorkflowExecutionsResponse{Executions: out}, nil
}

func (r *reader) GetWorkflowHistory(ctx context.Context, workflowID, _ string, _ bool, _ enumspb.HistoryEventFilterType) client.HistoryEventIterator {
	r.reads++
	if r.slow > 0 {
		select {
		case <-time.After(r.slow):
		case <-ctx.Done():
			return &events{err: ctx.Err()}
		}
	}
	h, ok := r.workflows[workflowID]
	switch {
	case r.failing != nil:
		return &events{err: r.failing}
	case !ok:
		return &events{err: serviceerror.NewNotFound("no such workflow")}
	}
	return &events{h: slices.Clone(h)}
}

func pushedAt(t *testing.T, h AnchorHead, at time.Time) *historypb.HistoryEvent {
	t.Helper()
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(h)
	if err != nil {
		t.Fatal(err)
	}
	return &historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, EventTime: timestamppb.New(at),
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: AnchorSignal, Input: payloads}}}
}

// me is the identity of the agentd of these tests; someone another's.
const me, someone = "a1a1a1a1a1a1a1a1", "b2b2b2b2b2b2b2b2"

// As agentd starts, every head its chain's anchor workflows of today and yesterday hold must be a
// record the ledger holds, and none may come after a higher one. What it finds, it reports, and
// starts: a head someone signalled later does not hide a cut (the highest head agentd pushed is
// still there), and one with a seq the ledger never reached does not keep agentd from starting.
func TestAStartChecksEveryHeadPushed(t *testing.T) {
	ctx := context.Background()
	l := open(t)
	heads := chain(t, l, 10)
	key := AnchorChain(records(t, l)[0].Sha256)
	now := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
	today, yesterday := AnchorWorkflowID(key, now), AnchorWorkflowID(key, now.AddDate(0, 0, -1))
	at := func(h Head, at time.Time) *historypb.HistoryEvent {
		return pushedAt(t, AnchorHead{Seq: h.Seq, Sha256: h.Sha256, Agentd: me}, at)
	}
	other := Head{Seq: 99, Sha256: strings.Repeat("0", 64)}
	r := &reader{workflows: map[string]history{
		yesterday: {at(heads[3], now.Add(-25*time.Hour))},
		today:     {at(heads[9], now.Add(-time.Hour))},
		// Another agentd's chain, and this chain's of before yesterday: nothing to this ledger.
		AnchorWorkflowID("other", now):               {pushedAt(t, AnchorHead{Seq: other.Seq, Sha256: other.Sha256, Agentd: someone}, now)},
		AnchorWorkflowID(key, now.AddDate(0, 0, -2)): {at(other, now)},
	}, listed: []string{AnchorWorkflowID("other", now), AnchorWorkflowID(key, now.AddDate(0, 0, -2))}}
	check := func(want ...string) {
		t.Helper()
		found, err := CheckPushed(ctx, r, l, me, key, now)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, f := range found {
			got = append(got, fmt.Sprintf("%d %s: %s", f.Seq, f.WorkflowID, f.Why))
		}
		if len(got) != len(want) {
			t.Fatalf("found\n\t%s\nwant %d", strings.Join(got, "\n\t"), len(want))
		}
		for i, w := range want {
			if !strings.Contains(got[i], w) {
				t.Errorf("finding %q, want one saying %q", got[i], w)
			}
		}
	}
	check()
	// The log cut back to its 7th record while agentd was down, and a head of the 7th signalled after
	// the 10th, newer than any: the 10th, which agentd pushed, is not in the ledger, and the 7th came
	// after it.
	exec(t, l, `DELETE FROM audit_log WHERE seq > 7`)
	r.workflows[today] = append(r.workflows[today], pushedAt(t, AnchorHead{Seq: heads[6].Seq, Sha256: heads[6].Sha256}, now))
	check("10 "+today+": the head pushed to Temporal was record 10, which the audit log does not have",
		"7 "+today+": the head pushed to Temporal was record 7, after record 10 (in "+today+"): the ledger went backwards")
	// A head with a seq the ledger never reached is found, and is no error.
	r.workflows[today] = append(r.workflows[today], at(Head{Seq: 1 << 40, Sha256: heads[0].Sha256}, now.Add(time.Minute)))
	found, err := CheckPushed(ctx, r, l, me, key, now)
	if err != nil || len(found) != 3 || !slices.ContainsFunc(found, func(f Finding) bool {
		return f.Seq == 1<<40 && strings.Contains(f.Why, "which the audit log does not have")
	}) {
		t.Fatalf("found %+v, %v", found, err)
	}
	// One the ledger holds with another hash.
	r.workflows[today] = history{at(Head{Seq: 2, Sha256: heads[1].Sha256}, now)}
	r.workflows[yesterday] = history{at(Head{Seq: 1, Sha256: heads[1].Sha256}, now.Add(-25*time.Hour))}
	check("1 " + yesterday + ": the head pushed to Temporal was record 1 with sha256 " + heads[1].Sha256 + "; the audit log has " + heads[0].Sha256)
}

// The start check reads what anyone with Temporal's API can add to: it is bounded, by time, by
// workflows (this chain's first) and by events, and best-effort. What it could not read, for a cap,
// the time, or Temporal's error, is a finding with how much was not read, never an error that would
// keep agentd from starting.
func TestAStartCheckIsBoundedAndBestEffort(t *testing.T) {
	ctx := context.Background()
	l := open(t)
	heads := chain(t, l, 3)
	key := AnchorChain(records(t, l)[0].Sha256)
	now := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
	today := AnchorWorkflowID(key, now)
	own := history{pushedAt(t, AnchorHead{Seq: heads[2].Seq, Sha256: heads[2].Sha256, Agentd: me}, now)}
	check := func(r *reader, want string, notRead int) []Finding { // notRead < 0: some

		t.Helper()
		found, err := CheckPushed(ctx, r, l, me, key, now)
		if err != nil {
			t.Fatalf("an error, which would stop agentd: %v", err)
		}
		if !slices.ContainsFunc(found, func(f Finding) bool {
			return strings.Contains(f.Why, want) && (f.NotRead == notRead || notRead < 0 && f.NotRead > 0)
		}) {
			t.Fatalf("found %+v, want one saying %q, %d not read", found, want, notRead)
		}
		return found
	}

	// Many anchor workflows of today's: 50 read, this chain's first, and the rest counted.
	junk := &reader{workflows: map[string]history{today: own}}
	for i := range 5000 {
		junk.listed = append(junk.listed, AnchorWorkflowID(fmt.Sprintf("junk%04d", i), now))
	}
	start := time.Now()
	check(junk, "the start reads at most 50 anchor workflows: 4952 more of today's and yesterday's not read", 4952)
	if junk.reads != 50 || time.Since(start) > 5*time.Second || !strings.Contains(junk.queries[0], "StartTime > '2026-10-20T01:00:00Z'") {
		t.Fatalf("%d workflows read in %v, queries %q", junk.reads, time.Since(start), junk.queries)
	}
	// Where Temporal cannot narrow the listing by start time, all of them are listed.
	junk.narrowErr, junk.queries = errors.New("unknown search attribute StartTime"), nil
	check(junk, "4952 more of today's and yesterday's not read", 4952)
	if len(junk.queries) != 2 || strings.Contains(junk.queries[1], "StartTime") {
		t.Fatalf("queries %q", junk.queries)
	}

	// A workflow of more events than the start reads.
	MaxAnchorEvents = 5
	t.Cleanup(func() { MaxAnchorEvents = 10000 })
	long := slices.Repeat(own, 7)
	check(&reader{workflows: map[string]history{today: long}}, "more than 5 events: the rest of the anchor workflow not read", 1)
	MaxAnchorEvents = 10000

	// Temporal's errors, listing or reading.
	failing := &reader{workflows: map[string]history{today: own}, failing: errors.New("the frontend is down")}
	check(failing, "the anchor workflow could not be read: the frontend is down", 1)
	check(&reader{workflows: map[string]history{today: own}, listErr: errors.New("visibility is down")},
		"the anchor workflows could not be listed: visibility is down", 0)

	// Out of time.
	StartCheckTime = 100 * time.Millisecond
	t.Cleanup(func() { StartCheckTime = 10 * time.Second })
	slow := &reader{workflows: map[string]history{}, listed: junk.listed[:40], slow: 30 * time.Millisecond}
	start = time.Now()
	found := check(slow, "anchor workflows not read", -1) // how many, the clock says
	if time.Since(start) > 2*time.Second || slow.reads > 10 {
		t.Fatalf("%d workflows read in %v: %+v", slow.reads, time.Since(start), found)
	}
}

// A ledger replaced while agentd was down has a chain of its own, whose anchor workflows hold
// nothing yet: what shows it is the heads this agentd pushed under the chain before, the highest of
// which is not this chain's. A head of another agentd's, however high, is none of this one's.
func TestAStartFindsALedgerReplacedWhileAgentdWasDown(t *testing.T) {
	ctx := context.Background()
	l := open(t)
	heads := chain(t, l, 3)
	key := AnchorChain(records(t, l)[0].Sha256)
	now := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
	old := AnchorWorkflowID("0123456789abcdef", now.AddDate(0, 0, -1))
	r := &reader{workflows: map[string]history{
		old: {pushedAt(t, AnchorHead{Seq: 500, Sha256: strings.Repeat("5", 64), Agentd: me}, now.Add(-2*time.Hour))},
		AnchorWorkflowID("fedcba9876543210", now): {
			pushedAt(t, AnchorHead{Seq: 900, Sha256: strings.Repeat("9", 64), Agentd: someone}, now)},
	}, listed: []string{old, AnchorWorkflowID("fedcba9876543210", now)}}
	replaced := func(chain string) {
		t.Helper()
		found, err := CheckPushed(ctx, r, l, me, chain, now)
		if err != nil || len(found) != 1 || found[0].Seq != 500 || found[0].WorkflowID != old ||
			!strings.Contains(found[0].Why, `the highest head this agentd pushed since yesterday is record 500 of chain "0123456789abcdef"`) {
			t.Fatalf("chain %q: found %+v, %v", chain, found, err)
		}
	}
	replaced(key) // a new chain
	replaced("")  // an emptied log
	// This chain pushed since, later, but lower: the old chain's is still the highest.
	r.workflows[AnchorWorkflowID(key, now)] = history{pushedAt(t, AnchorHead{Seq: heads[2].Seq, Sha256: heads[2].Sha256, Agentd: me}, now)}
	replaced(key)
	// This chain's pushes as high: nothing found.
	exec(t, l, `UPDATE audit_log SET seq = 500 WHERE seq = 3`) // the seq the old chain had, and this chain's own head
	r.workflows[AnchorWorkflowID(key, now)] = history{pushedAt(t, AnchorHead{Seq: 500, Sha256: heads[2].Sha256, Agentd: me}, now)}
	if found, err := CheckPushed(ctx, r, l, me, key, now); err != nil || len(found) != 0 {
		t.Fatalf("found %+v, %v", found, err)
	}
}

// Signals someone sent that are no head agentd pushes (a seq that is not positive, a sha256 that is
// not 64 lowercase hex characters, an agentd that is no identity, a payload that is no head) are
// findings of their own, which copy nothing of them; what agentd.started holds of a flood of them is
// bounded.
func TestAFloodOfForgedHeadsIsBounded(t *testing.T) {
	ctx := context.Background()
	l := open(t)
	chain(t, l, 2)
	key := AnchorChain(records(t, l)[0].Sha256)
	now := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
	junk := strings.Repeat("\u202e<script>", 400)
	var flood history
	for i := range 100 {
		h := []AnchorHead{{Seq: -1, Sha256: strings.Repeat("0", 64)}, {Seq: 1, Sha256: junk}, {Seq: 1, Sha256: strings.Repeat("A", 64)},
			{Seq: 1, Sha256: strings.Repeat("0", 64), Agentd: junk}}[i%4]
		flood = append(flood, pushedAt(t, h, now))
	}
	notAHead := pushedAt(t, AnchorHead{}, now)
	notAHead.GetWorkflowExecutionSignaledEventAttributes().Input.Payloads[0].Data = []byte(`"` + junk + `"`)
	// Another chain's that cannot be read say nothing of whose they are: not found.
	elsewhere := AnchorWorkflowID("0123456789abcdef", now)
	r := &reader{workflows: map[string]history{AnchorWorkflowID(key, now): append(flood, notAHead), elsewhere: flood},
		listed: []string{elsewhere}}
	found, err := CheckPushed(ctx, r, l, me, key, now)
	if err != nil || len(found) != 101 || slices.ContainsFunc(found, func(f Finding) bool {
		return f.Why != "an unreadable head: not a head agentd pushes" || f.WorkflowID != AnchorWorkflowID(key, now)
	}) {
		t.Fatalf("%d found, %v: %+v", len(found), err, found[:min(len(found), 3)])
	}
	record, more := FindingsRecord(found)
	if len(record) != MaxFindings || more != 101-MaxFindings {
		t.Fatalf("%d in the record, %d more", len(record), more)
	}
	b, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "script") || strings.Contains(string(b), "AAAA") || len(b) > MaxFindings*400 {
		t.Fatalf("the record copies what was signalled: %d bytes, %s", len(b), b[:min(len(b), 300)])
	}
	// A workflow's ID is anyone's to choose too: bounded in the record.
	long := Finding{Pushed: Pushed{WorkflowID: AnchorIDPrefix + strings.Repeat("x", 5000)}, Why: "w"}
	if id := long.Payload()["workflow_id"].(string); len(id) > MaxText {
		t.Fatalf("a workflow ID of %d bytes in the record", len(id))
	}
}
