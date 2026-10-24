package control

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

const workerQueue = "TaskQueue = 'agentos-workflows'"

var anchorQuery = fmt.Sprintf("WorkflowId STARTS_WITH '%s'", audit.AnchorIDPrefix)

// verifyFixture is a Control service whose Temporal lists the runs it started on the Worker's task
// queue, each with a history of the gateway calls agentd recorded for it.
type verifyFixture struct {
	s        *Server
	tc       *fakeTemporal
	allow    []string // the other chains verify allows: another agentd's
	replaced []string // and those of a ledger this agentd's was replaced with
}

func newVerifyFixture(t *testing.T) *verifyFixture {
	t.Helper()
	s, tc := newServer(t)
	s.TaskQueue = "agentos-workflows"
	tc.retention = 24 * time.Hour
	tc.executions = map[string][]string{}
	tc.histories = map[string][]*historypb.HistoryEvent{}
	tc.historyErr = map[string]error{}
	tc.kinds = map[string][2]string{}
	tc.running = map[string]bool{}
	return &verifyFixture{s: s, tc: tc}
}

// chain is the fixture's chain's key in its anchor workflows' IDs.
func (f *verifyFixture) chain(t *testing.T) string {
	t.Helper()
	return audit.AnchorChain(auditRecords(t, f.s)[0].Sha256)
}

// run starts a run, and has it make calls gateway calls: each recorded as agentd records it, and
// completed in its history with the result that names its record.
func (f *verifyFixture) run(t *testing.T, calls int) *agentosv1.Run {
	t.Helper()
	ctx := context.Background()
	run, err := f.s.StartRun(asOperator(ctx), &agentosv1.StartRunRequest{Task: "t"})
	if err != nil {
		t.Fatal(err)
	}
	events := []*historypb.HistoryEvent{{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		EventTime: timestamppb.New(time.Now())}}
	for i := range calls {
		activityID := fmt.Sprintf("%d:git.show_diff", i+1)
		value := map[string]any{"blob": fmt.Sprintf("sha256:%d", i)}
		sum, _ := audit.ValueSHA256(value)
		h, err := audit.Record(ctx, f.s.Ledger, audit.Event{WorkflowID: run.GetRunId(), Type: "call", Actor: audit.Run,
			Payload: map[string]any{"activity_id": activityID, "attempt": 1, "status": "ok", "result_sha256": sum}})
		if err != nil {
			t.Fatal(err)
		}
		value["_audit"] = h
		b, _ := json.Marshal(value)
		events = append(events, completed(t, int64(10*(i+1)), activityID, 1, &agentosv1.Result{Status: "ok", ValueJson: string(b)})...)
	}
	key := run.GetRunId() + "/" + run.GetTemporalRunId()
	f.tc.mu.Lock()
	f.tc.executions[workerQueue] = append(f.tc.executions[workerQueue], key)
	f.tc.histories[key] = events
	f.tc.mu.Unlock()
	return run
}

func (f *verifyFixture) verify(t *testing.T) *agentosv1.AuditReport {
	t.Helper()
	rep, err := f.s.VerifyAudit(context.Background(), &agentosv1.VerifyAuditRequest{AllowChains: f.allow,
		AllowReplacedLedgers: f.replaced})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

// this and another are agentds' identities (audit.Identity): this one's, and another's.
const this, another = "a1a1a1a1a1a1a1a1", "b2b2b2b2b2b2b2b2"

func clean(r *agentosv1.AuditReport) bool {
	return r.GetBrokenAtSeq() == 0 && len(r.GetAnchorsMismatched()) == 0 && len(r.GetRunsMismatched()) == 0 &&
		len(r.GetStateMismatched()) == 0 && len(r.GetRunsUnavailable()) == 0
}

func TestVerifyAuditOfAWholeLog(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 2)
	f.run(t, 1)
	rep := f.verify(t)
	if !clean(rep) || rep.GetAnchorsChecked() != 3 || rep.GetRunsChecked() != 2 || rep.GetRecords() != 7 ||
		rep.GetRetentionSeconds() != 86400 {
		t.Fatalf("report %v", rep)
	}
	// The runs are those on the Worker's task queue, not those of some workflow type.
	if !slices.Contains(f.tc.queries, workerQueue) || !slices.Contains(f.tc.queries, anchorQuery) {
		t.Errorf("queries %q", f.tc.queries)
	}
}

// Temporal lists a page at a time: every page's runs are checked.
func TestVerifyAuditReadsEveryPage(t *testing.T) {
	f := newVerifyFixture(t)
	for range 3 {
		f.run(t, 1)
	}
	f.tc.pageSize = 1
	if rep := f.verify(t); !clean(rep) || rep.GetRunsChecked() != 3 || rep.GetAnchorsChecked() != 3 {
		t.Fatalf("report %v", rep)
	}
	// A run on the second page that the ledger has no row of is found there.
	extra := f.run(t, 1)
	if _, err := f.s.Ledger.DB.Exec(`DELETE FROM runs WHERE workflow_id = ?`, extra.GetRunId()); err != nil {
		t.Fatal(err)
	}
	rep := f.verify(t)
	if !slices.ContainsFunc(rep.GetRunsMismatched(), func(m *agentosv1.RunMismatch) bool { return m.GetRunId() == extra.GetRunId() }) {
		t.Fatalf("report %v", rep)
	}
}

// A history Temporal no longer has is a mismatch while the namespace's retention says it should have
// it; after that, the run is unchecked. Any other failure to read a history fails the verification.
func TestVerifyAuditOfHistoriesTemporalDoesNotGive(t *testing.T) {
	f := newVerifyFixture(t)
	run := f.run(t, 1)
	key := run.GetRunId() + "/" + run.GetTemporalRunId()
	delete(f.tc.histories, key)
	rep := f.verify(t)
	if len(rep.GetRunsMismatched()) != 1 || !strings.Contains(rep.GetRunsMismatched()[0].GetWhy(), "within the namespace's retention") ||
		len(rep.GetRunsUnavailable()) != 0 {
		t.Fatalf("report %v", rep)
	}
	f.tc.retention = time.Nanosecond // the run's last record is older than that
	rep = f.verify(t)
	if len(rep.GetRunsMismatched()) != 0 || len(rep.GetRunsUnavailable()) != 1 || !strings.Contains(rep.GetRunsUnavailable()[0].GetWhy(),
		"past the namespace's retention") {
		t.Fatalf("report %v", rep)
	}
	f.tc.historyErr[key] = serviceerror.NewUnavailable("the frontend is down")
	if _, err := f.s.VerifyAudit(context.Background(), &agentosv1.VerifyAuditRequest{}); status.Code(err) != codes.Unavailable ||
		!strings.Contains(err.Error(), "the frontend is down") {
		t.Fatalf("got %v, want the verification to fail", err)
	}
}

// The result the workflow got must be the one the record holds: its value, to the byte.
func TestVerifyAuditOfAResultChangedInTheHistory(t *testing.T) {
	f := newVerifyFixture(t)
	run := f.run(t, 1)
	key := run.GetRunId() + "/" + run.GetTemporalRunId()
	events := f.tc.histories[key]
	completedEvent := events[len(events)-1].GetActivityTaskCompletedEventAttributes()
	var res agentosv1.Result
	if err := converter.GetDefaultDataConverter().FromPayloads(completedEvent.GetResult(), &res); err != nil {
		t.Fatal(err)
	}
	res.ValueJson = strings.Replace(res.GetValueJson(), "sha256:0", "sha256:9", 1)
	payloads, _ := converter.GetDefaultDataConverter().ToPayloads(&res)
	completedEvent.Result = payloads
	rep := f.verify(t)
	if len(rep.GetAnchorsMismatched()) != 1 || !strings.Contains(rep.GetAnchorsMismatched()[0].GetWhy(), "holds a result with sha256") {
		t.Fatalf("report %v", rep)
	}
}

func signaled(t *testing.T, id int64, h audit.Head) *historypb.HistoryEvent {
	t.Helper()
	return pushedBy(t, id, audit.AnchorHead{Seq: h.Seq, Sha256: h.Sha256}, time.Time{})
}

// pushedBy is the signal of head h, as agentd h.Agentd pushes it, which Temporal took at at.
func pushedBy(t *testing.T, id int64, h audit.AnchorHead, at time.Time) *historypb.HistoryEvent {
	t.Helper()
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(h)
	if err != nil {
		t.Fatal(err)
	}
	e := &historypb.HistoryEvent{EventId: id, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: audit.AnchorSignal, Input: payloads}}}
	if !at.IsZero() {
		e.EventTime = timestamppb.New(at)
	}
	return e
}

// The heads agentd pushed to the anchor workflows are checked against the chain, and the report
// says how many records no anchor outside the ledger follows yet.
func TestVerifyAuditOfTheExternalAnchors(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 2)
	head := f.verify(t)
	day := audit.AnchorWorkflowID(f.chain(t), time.Now())
	anchorAt := func(heads ...audit.Head) {
		var events []*historypb.HistoryEvent
		for i, h := range heads {
			events = append(events, signaled(t, int64(i+2), h))
		}
		f.tc.executions[anchorQuery] = []string{day + "/r1"}
		f.tc.histories[day+"/r1"] = events
	}
	last := audit.Head{Seq: head.GetHeadSeq(), Sha256: head.GetHeadSha256()}
	anchorAt(audit.Head{Seq: 1, Sha256: auditRecords(t, f.s)[0].Sha256}, last)
	rep := f.verify(t)
	if !clean(rep) || rep.GetExternalAnchorsChecked() != 2 || rep.GetLastExternalAnchorSeq() != last.Seq ||
		rep.GetRecordsAfterExternalAnchor() != 0 {
		t.Fatalf("report %v", rep)
	}
	f.run(t, 1) // three more records, which no anchor follows yet
	if rep := f.verify(t); !clean(rep) || rep.GetRecordsAfterExternalAnchor() != 3 {
		t.Fatalf("report %v", rep)
	}
	// An anchor that names another record, or one the chain no longer has, does not match.
	anchorAt(audit.Head{Seq: last.Seq, Sha256: strings.Repeat("0", 64)}, audit.Head{Seq: 99, Sha256: last.Sha256})
	rep = f.verify(t)
	if len(rep.GetAnchorsMismatched()) != 2 || rep.GetLastExternalAnchorSeq() != 0 ||
		!strings.Contains(rep.GetAnchorsMismatched()[0].GetWhy(), "pushed to Temporal") {
		t.Fatalf("report %v", rep)
	}
}

// The ledger's tables must be the state the records say: a row changed, added or removed without
// its record is found, as a state mismatch of its kind.
func TestVerifyAuditComparesTheTablesWithTheRecords(t *testing.T) {
	ctx := context.Background()
	now := func() string { return time.Now().UTC().Format(time.RFC3339Nano) }
	cancel := func(f *verifyFixture, run *agentosv1.Run) {
		if _, err := f.s.CancelRun(asOperator(ctx), &agentosv1.CancelRunRequest{RunId: run.GetRunId()}); err != nil {
			t.Fatal(err)
		}
	}
	extend := func(f *verifyFixture, run *agentosv1.Run) {
		if _, err := f.s.ExtendBudget(asOperator(ctx), &agentosv1.ExtendBudgetRequest{RunId: run.GetRunId(), AmountMicros: 5,
			IdempotencyKey: "k"}); err != nil {
			t.Fatal(err)
		}
	}
	for name, c := range map[string]struct {
		before func(*verifyFixture, *agentosv1.Run)
		sql    string // ? is the run's workflow ID, and ?2 now
		kind   string
		why    string
	}{
		"a budget changed":           {nil, `UPDATE runs SET budget_limit_micros = 999999 WHERE workflow_id = ?1`, "run", "a budget of 999999"},
		"dry run turned on":          {nil, `UPDATE runs SET dry_run = 1 WHERE workflow_id = ?1`, "run", "dry run true"},
		"the params changed":         {nil, `UPDATE runs SET params_json = '{"repo":"elsewhere"}' WHERE workflow_id = ?1`, "run", `params {"repo":"elsewhere"}`},
		"the task changed":           {nil, `UPDATE runs SET task = 'other' WHERE workflow_id = ?1`, "run", "is task other"},
		"another execution":          {nil, `UPDATE runs SET run_id = 'r-other' WHERE workflow_id = ?1`, "run", `has execution "r-other"`},
		"the row gone":               {nil, `DELETE FROM runs WHERE workflow_id = ?1`, "run", "has no row of it"},
		"cancelled without a record": {nil, `UPDATE runs SET cancelled_at = ?2 WHERE workflow_id = ?1`, "run", "records no cancel"},
		"the cancel's time changed": {cancel, `UPDATE runs SET cancelled_at = '2000-01-01T00:00:00Z' WHERE workflow_id = ?1`, "run",
			`was cancelled at "2000-01-01T00:00:00Z"`},
		"a run without its record": {nil, `INSERT INTO runs (workflow_id, task, params_json, budget_limit_micros, created_at)
			VALUES ('made-up', 't', '{}', 1, ?2)`, "run", "run made-up, made at"},
		"an extension changed": {extend, `UPDATE budget_entries SET amount_micros = 500 WHERE kind = 'EXTEND'`, "budget", "is 500 micros"},
		"an extension gone":    {extend, `DELETE FROM budget_entries WHERE kind = 'EXTEND'`, "budget", "has no such extension"},
		"an extension without its record": {nil, `INSERT INTO budget_entries (workflow_id, kind, amount_micros, idempotency_key, created_at)
			VALUES (?1, 'EXTEND', 5, 'k2', ?2)`, "budget", `extension "k2" of 5 micros`},
		"an effect without its record": {nil, `INSERT INTO effects (effect_id, workflow_id, tool, effect_class, payload_sha256,
			idempotency_key, status, created_at, updated_at) VALUES ('e-made-up', ?1, 'x', 'idempotent', 'p', 'k', 'SUCCEEDED', ?2, ?2)`,
			"effect", "effect e-made-up, made at"},
		// A made-at that cannot be read is not "before the log began".
		"an effect made when, nobody can read": {nil, `INSERT INTO effects (effect_id, workflow_id, tool, effect_class, payload_sha256,
			idempotency_key, status, created_at, updated_at) VALUES ('e-garbled', ?1, 'x', 'idempotent', 'p', 'k', 'SUCCEEDED', 'whenever', ?2)`,
			"effect", "effect e-garbled, made at whenever"},
		"an approval without its record": {nil, `INSERT INTO effects (effect_id, workflow_id, tool, effect_class, payload_sha256,
			idempotency_key, status, created_at, updated_at) VALUES ('e-made-up', ?1, 'x', 'idempotent', 'p', 'k', 'AWAITING_APPROVAL', '2000-01-01T00:00:00Z', ?2);
			INSERT INTO approvals (approval_id, effect_id, workflow_id, tool, args_json, payload_sha256, context_blob, status,
			created_at, expires_at) VALUES ('a-made-up', 'e-made-up', ?1, 'x', '{}', 'p', 'c', 'PENDING', ?2, ?2)`,
			"approval", "approval a-made-up, made at"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newVerifyFixture(t)
			run := f.run(t, 1)
			if c.before != nil {
				c.before(f, run)
			}
			if rep := f.verify(t); !clean(rep) {
				t.Fatalf("before: %v", rep)
			}
			if _, err := f.s.Ledger.DB.Exec(c.sql, run.GetRunId(), now()); err != nil {
				t.Fatal(err)
			}
			rep := f.verify(t)
			if !slices.ContainsFunc(rep.GetStateMismatched(), func(m *agentosv1.StateMismatch) bool {
				return m.GetKind() == c.kind && strings.Contains(m.GetWhy(), c.why)
			}) {
				t.Fatalf("no %s mismatch saying %q: %v", c.kind, c.why, rep.GetStateMismatched())
			}
		})
	}
}

// A run's row back after the run was discarded is a mismatch.
func TestVerifyAuditOfADiscardedRunsRow(t *testing.T) {
	f := newVerifyFixture(t)
	f.tc.startErr = serviceerror.NewUnavailable("no")
	if _, err := f.s.StartRun(asOperator(context.Background()), &agentosv1.StartRunRequest{Task: "t"}); err == nil {
		t.Fatal("started")
	}
	var id string
	for _, r := range auditRecords(t, f.s) {
		if r.Type == "run.discarded" {
			id = r.WorkflowID
		}
	}
	if id == "" {
		t.Fatal("no run.discarded")
	}
	if rep := f.verify(t); len(rep.GetStateMismatched()) != 0 {
		t.Fatalf("before: %v", rep)
	}
	if _, err := f.s.Ledger.DB.Exec(`INSERT INTO runs (workflow_id, task, params_json, budget_limit_micros, created_at)
		VALUES (?, 't', '{}', 7, '2000-01-01T00:00:00Z')`, id); err != nil {
		t.Fatal(err)
	}
	rep := f.verify(t)
	if !slices.ContainsFunc(rep.GetStateMismatched(), func(m *agentosv1.StateMismatch) bool {
		return m.GetKind() == "run" && strings.Contains(m.GetWhy(), "as discarded, and the ledger has a row of it")
	}) {
		t.Fatalf("report %v", rep)
	}
}

// anchorWorkflow is a workflow under the chain's anchor prefix (or another's) that holds events.
func (f *verifyFixture) anchorWorkflow(id string, kind [2]string, events ...*historypb.HistoryEvent) {
	f.tc.mu.Lock()
	defer f.tc.mu.Unlock()
	f.tc.executions[anchorQuery] = append(f.tc.executions[anchorQuery], id+"/r1")
	f.tc.histories[id+"/r1"] = events
	if kind != ([2]string{}) {
		f.tc.kinds[id+"/r1"] = kind
	}
}

// A chain's anchor workflows are its own, by their IDs: another chain's in the namespace (another
// agentd's ledger, or an earlier one) are counted, not checked; one of this chain's that is not
// agentd's, and a signal in one that agentd did not send or that cannot be read, are mismatches.
func TestVerifyAuditOfEachChainsAnchorWorkflows(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 1)
	rep := f.verify(t)
	head := audit.Head{Seq: rep.GetHeadSeq(), Sha256: rep.GetHeadSha256()}
	chain := f.chain(t)
	if rep.GetChain() != chain {
		t.Fatalf("chain %q, want %q", rep.GetChain(), chain)
	}
	today := audit.AnchorWorkflowID(chain, time.Now())
	f.anchorWorkflow(today, [2]string{}, pushedBy(t, 2, audit.AnchorHead{Seq: head.Seq, Sha256: head.Sha256}, time.Now().Add(-time.Hour)))
	// Another chain's, with heads this chain does not have, and of another type: none of it counts.
	// Its heads name no agentd, and came before this chain's first push: a ledger replaced, as far
	// as anyone can tell. Another's name another agentd, which pushed after it: one beside this one.
	f.anchorWorkflow(audit.AnchorWorkflowID("fedcba9876543210", time.Now()), [2]string{"other", "q"},
		signaled(t, 2, audit.Head{Seq: 99, Sha256: strings.Repeat("0", 64)}))
	f.anchorWorkflow(audit.AnchorWorkflowID("0123456789abcdef", time.Now()), [2]string{},
		pushedBy(t, 2, audit.AnchorHead{Seq: 98, Sha256: strings.Repeat("0", 64), Agentd: another}, time.Now()))
	// A head that names an agentd no identity is: not a head, which says nothing of whose they are.
	f.anchorWorkflow(audit.AnchorWorkflowID("fedcba9876543210", time.Now().Add(-24*time.Hour)), [2]string{},
		pushedBy(t, 2, audit.AnchorHead{Seq: 97, Sha256: strings.Repeat("0", 64), Agentd: "<script>"}, time.Now()))
	// Unless the request names them, each is a mismatch: the old key of a chain rewritten from its
	// first record looks just so. Only the agentd beside this one is suggested the flag.
	rep = f.verify(t)
	if len(rep.GetAnchorsMismatched()) != 2 || !slices.Equal(rep.GetOtherChains(), []string{"0123456789abcdef", "fedcba9876543210"}) ||
		!strings.Contains(rep.GetAnchorsMismatched()[0].GetWhy(), "--allow-chain 0123456789abcdef") ||
		!strings.Contains(rep.GetAnchorsMismatched()[0].GetWhy(), "another agentd ("+another+")") ||
		strings.Contains(rep.GetAnchorsMismatched()[1].GetWhy(), "--allow") ||
		!strings.Contains(rep.GetAnchorsMismatched()[1].GetWhy(), "an agentd that names itself in none of them") ||
		strings.Contains(rep.GetAnchorsMismatched()[1].GetWhy(), "script") ||
		rep.GetAnchorsMismatched()[0].GetRunId() != "agentos-audit-anchor-0123456789abcdef-*" || rep.GetOtherAnchorWorkflows() != 0 {
		t.Fatalf("report %v", rep)
	}
	f.allow = []string{"0123456789abcdef", "fedcba9876543210"}
	if rep := f.verify(t); len(rep.GetAnchorsMismatched()) != 1 || rep.GetOtherAnchorWorkflows() != 1 ||
		!strings.Contains(rep.GetAnchorsMismatched()[0].GetWhy(), `chain "fedcba9876543210"`) ||
		!strings.Contains(rep.GetAnchorsMismatched()[0].GetWhy(), "--allow-chain accepts only a chain that pushed after") {
		t.Fatalf("report %v", rep)
	}
	// The agentd beside this one is not a ledger this one replaced; the one that stopped before this
	// chain began may be, whatever its heads say, which only the operator knows.
	f.allow, f.replaced = nil, []string{"0123456789abcdef", "fedcba9876543210"}
	if rep := f.verify(t); len(rep.GetAnchorsMismatched()) != 1 || rep.GetOtherAnchorWorkflows() != 2 ||
		rep.GetAnchorsMismatched()[0].GetRunId() != "agentos-audit-anchor-0123456789abcdef-*" {
		t.Fatalf("report %v", rep)
	}
	f.allow, f.replaced = []string{"0123456789abcdef"}, []string{"fedcba9876543210"}
	rep = f.verify(t)
	if !clean(rep) || rep.GetExternalAnchorsChecked() != 1 || rep.GetOtherAnchorWorkflows() != 3 ||
		rep.GetLastExternalAnchorSeq() != head.Seq {
		t.Fatalf("report %v", rep)
	}

	notAgentds := audit.AnchorWorkflowID(chain, time.Now().Add(-24*time.Hour))
	f.anchorWorkflow(notAgentds, [2]string{"someone-elses", audit.AnchorTaskQueue}, signaled(t, 2, head))
	rep = f.verify(t)
	if len(rep.GetAnchorsMismatched()) != 1 || rep.GetAnchorsMismatched()[0].GetRunId() != notAgentds ||
		!strings.Contains(rep.GetAnchorsMismatched()[0].GetWhy(), `is a "someone-elses" on task queue "agentos-audit-anchor"`) ||
		rep.GetExternalAnchorsChecked() != 1 {
		t.Fatalf("report %v", rep)
	}

	garbled := signaled(t, 3, head)
	garbled.GetWorkflowExecutionSignaledEventAttributes().Input.Payloads[0].Data = []byte("{not json")
	other := signaled(t, 4, head)
	other.GetWorkflowExecutionSignaledEventAttributes().SignalName = "doorbell"
	junk := pushedBy(t, 5, audit.AnchorHead{Seq: head.Seq, Sha256: "<script>" + strings.Repeat("x", 3000)}, time.Time{})
	f.tc.histories[today+"/r1"] = append(f.tc.histories[today+"/r1"], garbled, other, junk)
	rep = f.verify(t) // a mismatch each, not a failure to verify
	whys := []string{}
	for _, m := range rep.GetAnchorsMismatched() {
		whys = append(whys, m.GetWhy())
	}
	unreadable := func(w string) bool { return strings.Contains(w, "cannot be read") }
	if len(whys) != 4 || len(slices.DeleteFunc(slices.Clone(whys), func(w string) bool { return !unreadable(w) })) != 2 ||
		!slices.ContainsFunc(whys, func(w string) bool {
			return strings.Contains(w, `event 5 of execution r1 cannot be read: it is not a head agentd pushes`)
		}) ||
		!slices.ContainsFunc(whys, func(w string) bool { return strings.Contains(w, `a signal "doorbell"`) }) ||
		slices.ContainsFunc(whys, func(w string) bool { return strings.Contains(w, "script") }) {
		t.Fatalf("mismatches %q", whys)
	}
}

// The audit log holds this agentd's start, the last record it appended and the last head it
// pushed, as long as it runs: a log cut under it, or emptied, is a mismatch, however whole what is
// left of it. An emptied log implies no rows at all: each one is a difference too.
func TestVerifyAuditOfALogCutUnderAgentd(t *testing.T) {
	for name, c := range map[string]struct {
		cut  string
		want []string
	}{
		"its tail":  {`DELETE FROM audit_log WHERE seq = (SELECT MAX(seq) FROM audit_log)`, []string{"the last record this agentd appended"}},
		"all of it": {`DELETE FROM audit_log`, []string{"this agentd's start (agentd.started)", "the last record this agentd appended"}},
		"its start": {`DELETE FROM audit_log WHERE type = 'agentd.started'`, []string{"this agentd's start (agentd.started)"}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newVerifyFixture(t)
			start, err := audit.Record(context.Background(), f.s.Ledger, audit.Event{Type: "agentd.started", Actor: audit.Agentd})
			if err != nil {
				t.Fatal(err)
			}
			f.s.StartRecord = start
			f.run(t, 1)
			if rep := f.verify(t); !clean(rep) {
				t.Fatalf("report %v", rep)
			}
			if _, err := f.s.Ledger.DB.Exec(c.cut); err != nil {
				t.Fatal(err)
			}
			rep := f.verify(t)
			for _, want := range c.want {
				if !slices.ContainsFunc(rep.GetAnchorsMismatched(), func(m *agentosv1.AnchorMismatch) bool {
					return m.GetRunId() == "" && strings.HasPrefix(m.GetWhy(), want) && strings.Contains(m.GetWhy(), "cut, rewritten or replaced")
				}) {
					t.Errorf("no mismatch saying %q: %v", want, rep.GetAnchorsMismatched())
				}
			}
			if name == "all of it" && !slices.ContainsFunc(rep.GetStateMismatched(), func(m *agentosv1.StateMismatch) bool {
				return m.GetKind() == "run" && strings.Contains(m.GetWhy(), "is not in the audit log")
			}) {
				t.Errorf("the run's row, which no record speaks of, is no difference: %v", rep.GetStateMismatched())
			}
		})
	}
}

// Another chain that stopped before this chain's first push, by Temporal's times, is of a ledger
// this one replaced: a reset, or a rewrite from its first record, which only the operator can tell
// apart, whoever its heads say pushed them (an identity comes from configuration, and respelled,
// it changes). Its mismatch states the facts, and suggests no flag; --allow-chain, for an agentd
// beside this one, does not accept it, --allow-replaced-ledger does. One of this agentd's that
// pushed after this chain's first push is two ledgers of one agentd live at once, which no reset
// makes, and neither flag accepts.
func TestVerifyAuditOfAChainThisAgentdPushedBefore(t *testing.T) {
	f := newVerifyFixture(t)
	f.s.AnchorIdentity = this
	f.run(t, 1)
	rep := f.verify(t)
	head := audit.AnchorHead{Seq: rep.GetHeadSeq(), Sha256: rep.GetHeadSha256(), Agentd: this}
	firstPush := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	f.anchorWorkflow(audit.AnchorWorkflowID(f.chain(t), firstPush), [2]string{}, pushedBy(t, 2, head, firstPush),
		pushedBy(t, 3, head, firstPush.Add(time.Hour)))
	reset, overlapped := "1111111111111111", "2222222222222222"
	f.anchorWorkflow(audit.AnchorWorkflowID(reset, firstPush), [2]string{},
		pushedBy(t, 2, audit.AnchorHead{Seq: 7, Sha256: strings.Repeat("7", 64), Agentd: this}, firstPush.Add(-time.Hour)))
	f.anchorWorkflow(audit.AnchorWorkflowID(overlapped, firstPush), [2]string{},
		pushedBy(t, 2, audit.AnchorHead{Seq: 3, Sha256: strings.Repeat("3", 64), Agentd: another}, firstPush.Add(-time.Hour)),
		pushedBy(t, 3, audit.AnchorHead{Seq: 4, Sha256: strings.Repeat("4", 64), Agentd: this}, firstPush.Add(time.Second)))
	whys := func(rep *agentosv1.AuditReport) map[string]string {
		out := map[string]string{}
		for _, m := range rep.GetAnchorsMismatched() {
			out[strings.TrimSuffix(strings.TrimPrefix(m.GetRunId(), audit.AnchorIDPrefix), "-*")] = m.GetWhy()
		}
		return out
	}
	rep = f.verify(t)
	got := whys(rep)
	if len(got) != 2 || rep.GetOtherAnchorWorkflows() != 0 {
		t.Fatalf("report %v", rep)
	}
	first := fmt.Sprintf("this ledger's chain %q first pushed record %d at 2026-10-21T12:00:00Z", f.chain(t), head.Seq)
	for key, want := range map[string]string{
		reset: "this agentd pushed its heads, the last record 7 (sha256 " + strings.Repeat("7", 64) + ") at 2026-10-21T11:00:00Z; " + first,
		overlapped: "this agentd pushed its heads, the last record 4 (sha256 " + strings.Repeat("4", 64) + ") at 2026-10-21T12:00:01Z, after " +
			first + ": two ledgers of this agentd were live at once, which neither --allow-chain nor --allow-replaced-ledger accepts",
	} {
		if !strings.Contains(got[key], want) || key == reset && strings.Contains(got[key], "--allow") {
			t.Errorf("chain %s: %q, want one saying %q and suggesting no flag", key, got[key], want)
		}
	}
	// --allow-chain is for an agentd beside this one.
	f.allow = []string{reset, overlapped}
	rep = f.verify(t)
	if got := whys(rep); len(got) != 2 || rep.GetOtherAnchorWorkflows() != 0 ||
		!strings.Contains(got[reset], "--allow-chain accepts only a chain that pushed after this chain's first push") {
		t.Fatalf("report %v", rep)
	}
	// --allow-replaced-ledger accepts the reset, not the two ledgers live at once.
	f.allow, f.replaced = nil, []string{reset, overlapped}
	rep = f.verify(t)
	if got := whys(rep); len(got) != 1 || rep.GetOtherAnchorWorkflows() != 1 || !strings.Contains(got[overlapped],
		"two ledgers of this agentd were live at once") {
		t.Fatalf("report %v", rep)
	}
	// Another agentd's heads, whoever it says it is, of a chain that stopped before this one began:
	// a ledger replaced, as far as anyone can tell (a respelled data directory makes this agentd
	// another one), and --allow-chain does not accept it.
	respelled := "3333333333333333"
	f.anchorWorkflow(audit.AnchorWorkflowID(respelled, firstPush), [2]string{},
		pushedBy(t, 2, audit.AnchorHead{Seq: 9, Sha256: strings.Repeat("9", 64), Agentd: another}, firstPush.Add(-time.Minute)))
	f.allow, f.replaced = []string{respelled}, []string{reset}
	rep = f.verify(t)
	if got := whys(rep); len(got) != 2 || strings.Contains(got[respelled], "--allow-chain "+respelled) ||
		!strings.Contains(got[respelled], "another agentd ("+another+") pushed its heads") {
		t.Fatalf("report %v", rep)
	}
	f.allow, f.replaced = nil, []string{reset, respelled}
	if rep := f.verify(t); len(whys(rep)) != 1 || rep.GetOtherAnchorWorkflows() != 2 {
		t.Fatalf("report %v", rep)
	}
}

// verify reads what anyone with Temporal's API can add to, bounded: anchor workflows, and the events
// of each. Past its bounds the report says what it did not read; agentctl exits 3 on such a report
// when nothing it read was a mismatch (TestTheAuditVerifyExitStatus).
func TestVerifyAuditIsBounded(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 1)
	rep := f.verify(t)
	head := audit.AnchorHead{Seq: rep.GetHeadSeq(), Sha256: rep.GetHeadSha256()}
	maxAnchorWorkflows, maxAnchorEvents = 3, 4
	t.Cleanup(func() { maxAnchorWorkflows, maxAnchorEvents = 2000, 20000 })
	today := audit.AnchorWorkflowID(f.chain(t), time.Now())
	f.anchorWorkflow(today, [2]string{}, pushedBy(t, 2, head, time.Time{}))
	if rep := f.verify(t); !clean(rep) || len(rep.GetUnread()) != 0 {
		t.Fatalf("report %v", rep)
	}
	// More events than verify reads of one.
	f.tc.histories[today+"/r1"] = slices.Repeat(f.tc.histories[today+"/r1"], 5)
	rep = f.verify(t)
	if !clean(rep) || len(rep.GetUnread()) != 1 || rep.GetUnread()[0].GetWhat() != fmt.Sprintf("events of anchor workflow %q", today) ||
		!strings.Contains(rep.GetUnread()[0].GetWhy(), "at most 4 events") {
		t.Fatalf("report %v", rep)
	}
	f.tc.histories[today+"/r1"] = f.tc.histories[today+"/r1"][:1]
	// More anchor workflows than verify reads, listed in pages: those past the bound are not read.
	f.tc.pageSize = 2
	for _, key := range []string{"1111111111111111", "2222222222222222", "3333333333333333", "4444444444444444"} {
		f.anchorWorkflow(audit.AnchorWorkflowID(key, time.Now()), [2]string{})
		f.replaced = append(f.replaced, key) // holding no head, they are not beside this one
	}
	rep = f.verify(t)
	if !clean(rep) || len(rep.GetUnread()) != 1 || rep.GetUnread()[0].GetWhat() != "anchor workflows" ||
		rep.GetUnread()[0].GetCount() < 1 || !strings.Contains(rep.GetUnread()[0].GetWhy(), "at most 3 anchor workflows") {
		t.Fatalf("report %v", rep)
	}
}

// agentd pushes the chain's head as it grows: a head pushed after a higher one, by Temporal's times
// and within a workflow by its events, means the ledger went backwards between the two pushes.
func TestVerifyAuditOfHeadsPushedOutOfOrder(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 2)
	rows := auditRecords(t, f.s)
	at := time.Date(2026, 10, 21, 23, 59, 0, 0, time.UTC)
	h := func(i int) audit.AnchorHead { return audit.AnchorHead{Seq: rows[i].Seq, Sha256: rows[i].Sha256} }
	yesterday, today := audit.AnchorWorkflowID(f.chain(t), at), audit.AnchorWorkflowID(f.chain(t), at.Add(time.Hour))
	f.anchorWorkflow(yesterday, [2]string{}, pushedBy(t, 2, h(1), at), pushedBy(t, 3, h(len(rows)-1), at.Add(30*time.Second)))
	f.anchorWorkflow(today, [2]string{}, pushedBy(t, 2, h(2), at.Add(2*time.Minute)), pushedBy(t, 3, h(len(rows)-1), at.Add(3*time.Minute)))
	rep := f.verify(t)
	if len(rep.GetAnchorsMismatched()) != 1 || rep.GetAnchorsMismatched()[0].GetRunId() != today ||
		rep.GetAnchorsMismatched()[0].GetSeq() != rows[2].Seq || !strings.Contains(rep.GetAnchorsMismatched()[0].GetWhy(),
		fmt.Sprintf("after record %d (in %s): the ledger went backwards between the two pushes", rows[len(rows)-1].Seq, yesterday)) {
		t.Fatalf("report %v", rep)
	}
	// In order, all of them, it is clean.
	f.tc.histories[today+"/r1"] = []*historypb.HistoryEvent{pushedBy(t, 2, h(len(rows)-1), at.Add(3*time.Minute))}
	if rep := f.verify(t); !clean(rep) || rep.GetExternalAnchorsChecked() != 3 {
		t.Fatalf("report %v", rep)
	}
}

// Records older than twice the push interval must have a pushed head after them, once this agentd
// has run that long; records older than the retention need not.
func TestVerifyAuditOfRecordsNoPushedHeadCovers(t *testing.T) {
	f := newVerifyFixture(t)
	f.s.AnchorEvery = 30 * time.Second
	f.tc.retention = 7 * 24 * time.Hour
	clock := ledger.Clock
	t.Cleanup(func() { ledger.Clock = clock })
	now := time.Now()
	ledger.Clock = func() time.Time { return now.Add(-10 * time.Minute) }
	f.run(t, 1) // three records, ten minutes old: run.created, run.started, the call
	ledger.Clock = func() time.Time { return now }
	f.s.StartedAt = now.Add(-30 * time.Second) // agentd started only just: its first push is yet to come
	if rep := f.verify(t); !clean(rep) {
		t.Fatalf("report %v", rep)
	}
	f.s.StartedAt = now.Add(-time.Hour)
	rep := f.verify(t)
	if len(rep.GetAnchorsMismatched()) != 1 || rep.GetAnchorsMismatched()[0].GetSeq() != 1 ||
		rep.GetAnchorsMismatched()[0].GetRunId() != audit.AnchorPrefix(f.chain(t))+"*" ||
		!strings.Contains(rep.GetAnchorsMismatched()[0].GetWhy(), "no head pushed to Temporal covers them") {
		t.Fatalf("report %v", rep)
	}
	// Records older than the retention need no head Temporal still has.
	f.tc.retention = 5 * time.Minute
	if rep := f.verify(t); len(rep.GetAnchorsMismatched()) != 0 {
		t.Fatalf("report %v", rep)
	}
	f.tc.retention = 7 * 24 * time.Hour
	// A head after the first two covers them; the call's record, after it, is not.
	all := auditRecords(t, f.s)
	f.anchorWorkflow(audit.AnchorWorkflowID(f.chain(t), now), [2]string{},
		signaled(t, 2, audit.Head{Seq: all[1].Seq, Sha256: all[1].Sha256}))
	if rep := f.verify(t); len(rep.GetAnchorsMismatched()) != 1 || rep.GetAnchorsMismatched()[0].GetSeq() != all[2].Seq {
		t.Fatalf("report %v", rep)
	}
	// A head after all of them: those after it are too young yet.
	f.tc.histories[audit.AnchorWorkflowID(f.chain(t), now)+"/r1"] = append(f.tc.histories[audit.AnchorWorkflowID(f.chain(t), now)+"/r1"],
		signaled(t, 3, audit.Head{Seq: all[2].Seq, Sha256: all[2].Sha256}))
	ledger.Clock = func() time.Time { return now.Add(-10 * time.Second) }
	f.run(t, 0) // younger than a minute
	ledger.Clock = func() time.Time { return now }
	if rep := f.verify(t); len(rep.GetAnchorsMismatched()) != 0 || rep.GetRecordsAfterExternalAnchor() != 2 {
		t.Fatalf("report %v", rep)
	}
	// Ten minutes on, they are not.
	ledger.Clock = func() time.Time { return now.Add(10 * time.Minute) }
	if rep := f.verify(t); len(rep.GetAnchorsMismatched()) != 1 || rep.GetAnchorsMismatched()[0].GetSeq() != all[2].Seq+1 {
		t.Fatalf("report %v", rep)
	}
	ledger.Clock = func() time.Time { return now }
	// Past the retention, nothing is expected to hold their heads any more.
	f.tc.retention = 5 * time.Minute
	if rep := f.verify(t); len(rep.GetAnchorsMismatched()) != 0 {
		t.Fatalf("report %v", rep)
	}
}

// A run's history must be in Temporal while its last live record, its last call's, is within the
// retention: records appended after the run closed (an expiry, a reconciliation) say nothing of it.
func TestVerifyAuditCountsTheRetentionFromTheLastCall(t *testing.T) {
	f := newVerifyFixture(t)
	clock := ledger.Clock
	t.Cleanup(func() { ledger.Clock = clock })
	now := time.Now()
	ledger.Clock = func() time.Time { return now.Add(-3 * time.Hour) }
	run := f.run(t, 1)
	ledger.Clock = func() time.Time { return now.Add(-time.Hour) } // after the close, by agentd
	if _, err := audit.Record(context.Background(), f.s.Ledger, audit.Event{WorkflowID: run.GetRunId(), Type: "approval.expired",
		Actor: audit.Agentd, Payload: map[string]any{"approval_id": "a1", "why": "nobody decided"}}); err != nil {
		t.Fatal(err)
	}
	// A call of an execution agentd did not start, under the run's ID: nobody vouches that the run's
	// execution made it, and it says nothing of when that one was last live.
	if _, err := audit.Record(context.Background(), f.s.Ledger, audit.Event{WorkflowID: run.GetRunId(), Type: "call",
		Actor: audit.UnknownRun, Payload: map[string]any{"activity_id": "1:x", "attempt": 1, "status": "error"}}); err != nil {
		t.Fatal(err)
	}
	ledger.Clock = func() time.Time { return now }
	f.tc.retention = 2 * time.Hour
	delete(f.tc.histories, run.GetRunId()+"/"+run.GetTemporalRunId())
	rep := f.verify(t)
	if len(rep.GetRunsMismatched()) != 0 || len(rep.GetRunsUnavailable()) != 1 {
		t.Fatalf("report %v", rep)
	}
	f.tc.retention = 4 * time.Hour // its last call is within it: the history must be there
	if rep := f.verify(t); len(rep.GetRunsMismatched()) != 1 {
		t.Fatalf("report %v", rep)
	}
}

// A run made before the audit log began, of which the log has no record and Temporal no history, is
// reported apart: nothing is left to check it against, and agentctl does not count it unchecked.
func TestVerifyAuditOfRunsFromBeforeTheLog(t *testing.T) {
	f := newVerifyFixture(t)
	if _, err := f.s.Ledger.DB.Exec(`INSERT INTO runs (workflow_id, run_id, task, params_json, budget_limit_micros, created_at)
		VALUES ('old', 'old-run', 't', '{}', 1, '2000-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	f.run(t, 1) // the log begins here
	rep := f.verify(t)
	if len(rep.GetRunsBeforeLog()) != 1 || rep.GetRunsBeforeLog()[0].GetRunId() != "old" || len(rep.GetRunsUnavailable()) != 0 ||
		!clean(rep) {
		t.Fatalf("report %v", rep)
	}
	// One the log has a record of is unchecked, as any other.
	if _, err := audit.Record(context.Background(), f.s.Ledger, audit.Event{WorkflowID: "old", Type: "approval.expired",
		Actor: audit.Agentd, Payload: map[string]any{"approval_id": "a0", "why": "nobody decided"}}); err != nil {
		t.Fatal(err)
	}
	if rep := f.verify(t); len(rep.GetRunsBeforeLog()) != 0 || len(rep.GetRunsUnavailable()) != 1 {
		t.Fatalf("report %v", rep)
	}
}

// Every blob's bytes must hash to their key, and what effects and approvals refer to must be there.
func TestVerifyAuditOfTheBlobs(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 1)
	// More than a page of them, the one changed among the last.
	var ref string
	for i := range 250 {
		r, err := f.s.Ledger.PutBlob(context.Background(), []byte(fmt.Sprintf("the payload approved %d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if ref == "" || r > ref {
			ref = r
		}
	}
	if rep := f.verify(t); !clean(rep) {
		t.Fatalf("report %v", rep)
	}
	if _, err := f.s.Ledger.DB.Exec(`UPDATE blobs SET bytes = 'TAMPERED' WHERE sha256 = ?`, strings.TrimPrefix(ref, "sha256:")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Ledger.DB.Exec(`INSERT INTO effects (effect_id, workflow_id, tool, effect_class, payload_sha256,
		idempotency_key, status, payload_blob, created_at, updated_at) VALUES ('e-old', 'wf-old', 'x', 'idempotent', 'p', 'k', 'SUCCEEDED',
		'sha256:0000', '2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	// An approval whose card (what its reviewer was shown) is gone.
	if _, err := f.s.Ledger.DB.Exec(`INSERT INTO approvals (approval_id, effect_id, workflow_id, tool, args_json, payload_sha256,
		context_blob, status, created_at, expires_at) SELECT 'a-old', 'e-old', workflow_id, 'x', '{}', 'p', 'sha256:1111', 'PENDING',
		'2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z' FROM runs LIMIT 1`); err != nil {
		t.Fatal(err)
	}
	rep := f.verify(t)
	saying := func(kind, why string) bool {
		return slices.ContainsFunc(rep.GetStateMismatched(), func(m *agentosv1.StateMismatch) bool {
			return m.GetKind() == kind && strings.Contains(m.GetWhy(), why)
		})
	}
	if !saying("blob", "holds bytes with sha256") || !saying("effect", "refers to blob sha256:0000") ||
		!saying("approval", "refers to blob sha256:1111") {
		t.Fatalf("report %v", rep.GetStateMismatched())
	}
}

// A run made with its record, and given an execution without one.
func TestVerifyAuditOfAnExecutionWithoutItsStart(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 1)
	ctx := asOperator(context.Background())
	who, _ := operator(ctx)
	if err := f.s.Ledger.Tx(ctx, func(q *db.Queries) error {
		if err := q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "wf-new", Task: "t", ParamsJson: "{}", BudgetLimitMicros: 7,
			CreatedAt: ledger.Now()}); err != nil {
			return err
		}
		return note(ctx, q, "wf-new", "run.created", who, map[string]any{"task": "t", "params": map[string]string{},
			"budget_limit_micros": 7, "dry_run": false})
	}); err != nil {
		t.Fatal(err)
	}
	if rep := f.verify(t); len(rep.GetStateMismatched()) != 0 {
		t.Fatalf("report %v", rep.GetStateMismatched())
	}
	if _, err := f.s.Ledger.DB.Exec(`UPDATE runs SET run_id = 'r-made-up' WHERE workflow_id = 'wf-new'`); err != nil {
		t.Fatal(err)
	}
	rep := f.verify(t)
	if !slices.ContainsFunc(rep.GetStateMismatched(), func(m *agentosv1.StateMismatch) bool {
		return m.GetId() == "wf-new" && strings.Contains(m.GetWhy(), "records no start of it")
	}) {
		t.Fatalf("report %v", rep.GetStateMismatched())
	}
}

// hookedReads has the ledger's reads run before() ahead of the first statement that contains match,
// in a Read or not: a write lands between two statements of one read. It reports whether it did.
func hookedReads(t *testing.T, l *ledger.Ledger, match string, before func()) (fired func() bool) {
	var once sync.Once
	var did bool
	run := func() { did = true; before() }
	l.ReadHook = func(c db.DBTX) db.DBTX { return &hookDB{DBTX: c, match: match, once: &once, before: run} }
	q := l.Q
	l.Q = db.New(&hookDB{DBTX: l.DB, match: match, once: &once, before: run})
	t.Cleanup(func() { l.ReadHook, l.Q = nil, q })
	return func() bool { return did }
}

type hookDB struct {
	db.DBTX
	match  string
	once   *sync.Once
	before func()
}

func (h *hookDB) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	if strings.Contains(q, h.match) {
		h.once.Do(h.before)
	}
	return h.DBTX.QueryContext(ctx, q, args...)
}

func (h *hookDB) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	if strings.Contains(q, h.match) {
		h.once.Do(h.before)
	}
	return h.DBTX.QueryRowContext(ctx, q, args...)
}

// startAnother records a run as StartRun does first: its row and its run.created, in one transaction.
func startAnother(t *testing.T, f *verifyFixture, workflowID string) func() {
	return func() {
		ctx := asOperator(context.Background())
		who, _ := operator(ctx)
		if err := f.s.Ledger.Tx(ctx, func(q *db.Queries) error {
			if err := q.InsertRun(ctx, db.InsertRunParams{WorkflowID: workflowID, Task: "t", ParamsJson: "{}", BudgetLimitMicros: 7,
				CreatedAt: ledger.Now()}); err != nil {
				return err
			}
			return note(ctx, q, workflowID, "run.created", who, map[string]any{"task": "t", "params": map[string]string{},
				"budget_limit_micros": 7, "dry_run": false})
		}); err != nil {
			t.Error(err)
		}
	}
}

// A run started while VerifyAudit reads the runs table and the records of runs: the two reads are of
// one snapshot, which has neither, and nothing is reported of it.
func TestVerifyAuditReadsTheRunsAsOneSnapshot(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 1)
	fired := hookedReads(t, f.s.Ledger, "'run.created', 'run.started', 'run.discarded'", startAnother(t, f, "wf-mid-read"))
	if rep := f.verify(t); !clean(rep) || !fired() {
		t.Fatalf("report %v (the write between the reads: %v)", rep, fired())
	}
	if rep := f.verify(t); !clean(rep) { // read again, the run is in both
		t.Fatalf("report %v", rep)
	}
}

// A run started while VerifyAudit replays the chain and compares the tables with it: the replay and
// the tables are of one snapshot, which has neither.
func TestVerifyAuditReadsTheChainAsOneSnapshot(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 1)
	fired := hookedReads(t, f.s.Ledger, "kind = 'EXTEND'", startAnother(t, f, "wf-mid-replay"))
	if rep := f.verify(t); !clean(rep) || !fired() {
		t.Fatalf("report %v (the write between the reads: %v)", rep, fired())
	}
}

// An audit log with no records is no agentd's (each start appends one): what Temporal holds of it,
// heads pushed or results anchored, is a mismatch, and nothing counts as from before the log.
func TestVerifyAuditOfAnEmptyLog(t *testing.T) {
	f := newVerifyFixture(t)
	run := f.run(t, 1)
	chain := f.chain(t)
	f.anchorWorkflow(audit.AnchorWorkflowID(chain, time.Now()), [2]string{}, signaled(t, 2, audit.Head{Seq: 3, Sha256: strings.Repeat("0", 64)}))
	if _, err := f.s.Ledger.DB.Exec(`DELETE FROM audit_log`); err != nil {
		t.Fatal(err)
	}
	empty := func(rep *agentosv1.AuditReport) bool {
		return slices.ContainsFunc(rep.GetAnchorsMismatched(), func(m *agentosv1.AnchorMismatch) bool {
			return m.GetWhy() == "Temporal holds heads or anchored results, but the audit log has no records"
		})
	}
	rep := f.verify(t)
	if !empty(rep) || rep.GetChain() != "" || len(rep.GetRunsBeforeLog()) != 0 {
		t.Fatalf("report %v", rep)
	}
	// The anchored results say so on their own, as do the heads on theirs.
	f.allow = []string{chain}
	if rep := f.verify(t); !empty(rep) {
		t.Fatalf("report %v", rep)
	}
	delete(f.tc.histories, run.GetRunId()+"/"+run.GetTemporalRunId())
	f.tc.executions[workerQueue] = nil
	f.allow = nil
	if rep := f.verify(t); !empty(rep) {
		t.Fatalf("report %v", rep)
	}
	// The run's row, made when nothing says the log had begun, is not from before it.
	if rep := f.verify(t); len(rep.GetRunsBeforeLog()) != 0 {
		t.Fatalf("report %v", rep)
	}
}

// A run whose history Temporal still has, started before the log began and with results that name no
// record, is reported as from before the log; one of its results that names a record is checked.
func TestVerifyAuditOfARunFromBeforeTheLogInTemporal(t *testing.T) {
	f := newVerifyFixture(t)
	old := time.Now().Add(-48 * time.Hour)
	if _, err := f.s.Ledger.DB.Exec(`INSERT INTO runs (workflow_id, run_id, task, params_json, budget_limit_micros, created_at)
		VALUES ('old', 'old-run', 't', '{}', 1, ?)`, old.UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	f.run(t, 1) // the log begins here
	events := []*historypb.HistoryEvent{{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		EventTime: timestamppb.New(old)}}
	events = append(events, completed(t, 10, "1:git.show_diff", 1, &agentosv1.Result{Status: "ok", ValueJson: `{"blob":"sha256:ab"}`})...)
	f.tc.executions[workerQueue] = append(f.tc.executions[workerQueue], "old/old-run")
	f.tc.histories["old/old-run"] = events
	rep := f.verify(t)
	if len(rep.GetRunsBeforeLog()) != 1 || rep.GetRunsBeforeLog()[0].GetRunId() != "old" ||
		!strings.Contains(rep.GetRunsBeforeLog()[0].GetWhy(), "results of it name no record") || len(rep.GetAnchorsMismatched()) != 0 {
		t.Fatalf("report %v", rep)
	}
}

// attempt appends an attempt's records as the gateway does: its call.started, unless started is
// false, and its call record, unless ended is "" (with the policy's decision and, when it names
// one, the call.started's seq).
func (f *verifyFixture) record(t *testing.T, workflowID, typ string, p map[string]any) audit.Head {
	t.Helper()
	h, err := audit.Record(context.Background(), f.s.Ledger, audit.Event{WorkflowID: workflowID, Type: typ, Actor: audit.Run, Payload: p})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func callMismatches(rep *agentosv1.AuditReport) []string {
	var out []string
	for _, m := range rep.GetStateMismatched() {
		if m.GetKind() == "call" {
			out = append(out, m.GetWhy())
		}
	}
	return out
}

// Each attempt the policy let past has its call.started, closed by its call record, which names it,
// or by an agentd.started after it, when the attempt was cut short.
func TestVerifyAuditOfTheAttemptsRecords(t *testing.T) {
	allow := map[string]any{"decision": "allow"}
	for name, c := range map[string]struct {
		records func(f *verifyFixture, wf string)
		running bool
		want    string // in a call mismatch; "" for none
		cut     int
	}{
		"started and ended": {func(f *verifyFixture, wf string) {
			h := f.record(t, wf, "call.started", map[string]any{"activity_id": "1:x", "attempt": 1, "policy": allow})
			f.record(t, wf, "call", map[string]any{"activity_id": "1:x", "attempt": 1, "status": "ok", "policy": allow, "started_seq": h.Seq})
		}, false, "", 0},
		"cut short by a restart": {func(f *verifyFixture, wf string) {
			f.record(t, wf, "call.started", map[string]any{"activity_id": "1:x", "attempt": 1, "policy": allow})
			f.record(t, "", "agentd.started", map[string]any{"pid": 1})
			h := f.record(t, wf, "call.started", map[string]any{"activity_id": "1:x", "attempt": 2, "policy": allow})
			f.record(t, wf, "call", map[string]any{"activity_id": "1:x", "attempt": 2, "status": "ok", "policy": allow, "started_seq": h.Seq})
		}, false, "", 1},
		"in flight": {func(f *verifyFixture, wf string) {
			f.record(t, wf, "call.started", map[string]any{"activity_id": "1:x", "attempt": 1, "policy": allow})
		}, true, "", 0},
		"open, its run over": {func(f *verifyFixture, wf string) {
			f.record(t, wf, "call.started", map[string]any{"activity_id": "1:x", "attempt": 1, "policy": allow})
		}, false, "has no call record and no agentd.started", 0},
		// Temporal begins the next attempt while agentd still works on the last (its timeout ran
		// out first): each is in flight while the run runs.
		"open, a later attempt begun": {func(f *verifyFixture, wf string) {
			f.record(t, wf, "call.started", map[string]any{"activity_id": "1:x", "attempt": 1, "policy": allow})
			f.record(t, wf, "call.started", map[string]any{"activity_id": "1:x", "attempt": 2, "policy": allow})
		}, true, "", 0},
		"open, a later attempt ended, its run running": {func(f *verifyFixture, wf string) {
			f.record(t, wf, "call.started", map[string]any{"activity_id": "1:x", "attempt": 1, "policy": allow})
			h := f.record(t, wf, "call.started", map[string]any{"activity_id": "1:x", "attempt": 2, "policy": allow})
			f.record(t, wf, "call", map[string]any{"activity_id": "1:x", "attempt": 2, "status": "ok", "policy": allow, "started_seq": h.Seq})
		}, true, "", 0},
		"open, a later attempt ended, its run over": {func(f *verifyFixture, wf string) {
			f.record(t, wf, "call.started", map[string]any{"activity_id": "1:x", "attempt": 1, "policy": allow})
			h := f.record(t, wf, "call.started", map[string]any{"activity_id": "1:x", "attempt": 2, "policy": allow})
			f.record(t, wf, "call", map[string]any{"activity_id": "1:x", "attempt": 2, "status": "ok", "policy": allow, "started_seq": h.Seq})
		}, false, "has no call record and no agentd.started", 0},
		"another attempt's call.started": {func(f *verifyFixture, wf string) {
			h := f.record(t, wf, "call.started", map[string]any{"activity_id": "1:x", "attempt": 1, "policy": allow})
			f.record(t, wf, "call", map[string]any{"activity_id": "2:y", "attempt": 1, "status": "ok", "policy": allow, "started_seq": h.Seq})
		}, true, "which is not that attempt's call.started", 0},
		"a call.started after it": {func(f *verifyFixture, wf string) {
			f.record(t, wf, "call", map[string]any{"activity_id": "1:x", "attempt": 1, "status": "ok", "policy": allow, "started_seq": 99})
		}, true, "which is not that attempt's call.started", 0},
		"one named twice": {func(f *verifyFixture, wf string) {
			h := f.record(t, wf, "call.started", map[string]any{"activity_id": "1:x", "attempt": 1, "policy": allow})
			for range 2 {
				f.record(t, wf, "call", map[string]any{"activity_id": "1:x", "attempt": 1, "status": "ok", "policy": allow, "started_seq": h.Seq})
			}
		}, false, "which another call record or an agentd.started closed", 0},
		"one cut short, then named": {func(f *verifyFixture, wf string) {
			h := f.record(t, wf, "call.started", map[string]any{"activity_id": "1:x", "attempt": 1, "policy": allow})
			f.record(t, "", "agentd.started", map[string]any{"pid": 1})
			f.record(t, wf, "call", map[string]any{"activity_id": "1:x", "attempt": 1, "status": "ok", "policy": allow, "started_seq": h.Seq})
		}, false, "which another call record or an agentd.started closed", 1},
		"none, though the policy let it past": {func(f *verifyFixture, wf string) {
			f.record(t, wf, "call", map[string]any{"activity_id": "1:x", "attempt": 1, "status": "ok", "policy": allow})
		}, false, "names no call.started", 0},
		// One the policy let past to ask for approval went on too: it records its write and its
		// approval, after its call.started.
		"none, though the policy let it past to an approval": {func(f *verifyFixture, wf string) {
			f.record(t, wf, "call", map[string]any{"activity_id": "1:x", "attempt": 1, "status": "needs_approval",
				"policy": map[string]any{"decision": "require_approval"}})
		}, false, "names no call.started", 0},
		"none, refused by the policy": {func(f *verifyFixture, wf string) {
			f.record(t, wf, "call", map[string]any{"activity_id": "1:x", "attempt": 1, "status": "denied",
				"policy": map[string]any{"decision": "deny"}})
		}, false, "", 0},
		"none, ended with an error before it": {func(f *verifyFixture, wf string) {
			f.record(t, wf, "call", map[string]any{"activity_id": "1:x", "attempt": 1, "status": "error", "policy": allow,
				"error": "agentd could not record the call before making it; retry"})
		}, false, "", 0},
	} {
		t.Run(name, func(t *testing.T) {
			f := newVerifyFixture(t)
			run := f.run(t, 0)
			if c.running {
				f.tc.running[run.GetRunId()+"/"+run.GetTemporalRunId()] = true
			}
			c.records(f, run.GetRunId())
			rep := f.verify(t)
			got := callMismatches(rep)
			switch {
			case c.want == "" && len(got) > 0:
				t.Fatalf("mismatches %q", got)
			case c.want != "" && !slices.ContainsFunc(got, func(w string) bool { return strings.Contains(w, c.want) }):
				t.Fatalf("mismatches %q, want one saying %q", got, c.want)
			case len(rep.GetAttemptsCutShort()) != c.cut:
				t.Fatalf("cut short %v, want %d", rep.GetAttemptsCutShort(), c.cut)
			}
			if c.cut == 1 {
				if a := rep.GetAttemptsCutShort()[0]; a.GetRunId() != run.GetRunId() || a.GetActivityId() != "1:x" || a.GetAttempt() != 1 ||
					a.GetRestartedSeq() != a.GetSeq()+1 {
					t.Fatalf("cut short %v", a)
				}
			}
		})
	}
}

// Every table of the ledger is one VerifyAudit compares with the records, or one it names as
// unchecked with why: a table added without either shows here.
func TestEveryTableIsCheckedOrNamed(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 1)
	rep := f.verify(t)
	var named []string
	for _, u := range rep.GetUncheckedTables() {
		named = append(named, u.GetName())
		if u.GetWhy() == "a table verify does not know of" {
			t.Errorf("table %s is neither compared with the records nor named as unchecked", u.GetName())
		}
	}
	if !slices.Equal(named, []string{"goose_db_version", "migration_files", "outbox"}) {
		t.Fatalf("unchecked tables %v", named)
	}
	// A table of anyone else's is a table no migration makes: the schema's mismatch, past which verify
	// reads nothing (TestVerifyAuditOfASchemaChanged). What verify names as a table it does not know
	// of is one a migration makes and verify was not taught: none of the migrations' tables, above.
	if _, err := f.s.Ledger.DB.Exec(`CREATE TABLE side (x INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if rep := f.verify(t); !slices.ContainsFunc(rep.GetStateMismatched(), func(m *agentosv1.StateMismatch) bool {
		return m.GetKind() == "schema" && strings.Contains(m.GetWhy(), `table "side" on "side", which no migration makes`)
	}) {
		t.Fatalf("report %v", rep.GetStateMismatched())
	}
}

// The ledger's schema is its migrations', nothing more: an index of someone else's (one that would
// make an insert skip its row) is a mismatch, and so is a trigger under a name of SQLite's.
func TestVerifyAuditOfASchemaChanged(t *testing.T) {
	for name, c := range map[string]struct{ change, want string }{
		"an index": {`CREATE UNIQUE INDEX one_call_a_run ON calls (workflow_id)`,
			`index "one_call_a_run" on "calls", which no migration makes`},
		// A name CREATE refuses, written into sqlite_master (schema_version as it was): SQLite loads it.
		"a trigger named sqlite_": {`PRAGMA writable_schema=ON; INSERT INTO sqlite_master (type, name, tbl_name, rootpage, sql)
			VALUES ('trigger', 'sqlite_x', 'calls', 0, 'CREATE TRIGGER sqlite_x BEFORE INSERT ON calls BEGIN SELECT RAISE(IGNORE); END');
			PRAGMA writable_schema=OFF`, `trigger "sqlite_x" on "calls", which no migration makes`},
	} {
		t.Run(name, func(t *testing.T) {
			f := newVerifyFixture(t)
			f.run(t, 1)
			if _, err := f.s.Ledger.DB.Exec(c.change); err != nil {
				t.Fatal(err)
			}
			rep := f.verify(t)
			if !slices.ContainsFunc(rep.GetStateMismatched(), func(m *agentosv1.StateMismatch) bool {
				return m.GetKind() == "schema" && strings.Contains(m.GetWhy(), c.want)
			}) {
				t.Fatalf("report %v", rep.GetStateMismatched())
			}
			// Past it verify reads nothing, and says so: through that schema, a read could return anything.
			if rep.GetRecords() != 0 || len(rep.GetUnread()) != 1 || rep.GetUnread()[0].GetWhat() != "the rest of the audit" {
				t.Fatalf("records %d, unread %v", rep.GetRecords(), rep.GetUnread())
			}
		})
	}
}

// What a comparison of sqlite_master does not see, PRAGMA integrity_check does: two indexes of the
// audit log whose rootpages were swapped, each reading the other's tree, every row of sqlite_master
// the migrations' but for its rootpage. A mismatch naming the first of its lines, quoted; past it
// verify reads nothing.
func TestVerifyAuditOfADamagedLedger(t *testing.T) {
	f := newVerifyFixture(t)
	f.run(t, 3)
	if _, err := f.s.Ledger.DB.Exec(`PRAGMA writable_schema=ON;
		UPDATE sqlite_master SET rootpage = (SELECT sum(rootpage) FROM sqlite_master
			WHERE name IN ('audit_log_by_run', 'sqlite_autoindex_audit_log_1')) - rootpage
		WHERE name IN ('audit_log_by_run', 'sqlite_autoindex_audit_log_1');
		PRAGMA writable_schema=OFF`); err != nil {
		t.Fatal(err)
	}
	rep := f.verify(t)
	var found []string
	for _, m := range rep.GetStateMismatched() {
		if m.GetKind() != "integrity" || m.GetId() != "ledger" {
			t.Fatalf("mismatch %v", m)
		}
		found = append(found, m.GetWhy())
	}
	if len(found) != maxIntegrityLines+1 || found[0] != `PRAGMA integrity_check: "row 1 missing from index audit_log_by_run"` ||
		!strings.Contains(found[maxIntegrityLines], "stops at 10 lines") {
		t.Fatalf("mismatches %q", found)
	}
	if rep.GetRecords() != 0 || len(rep.GetUnread()) != 1 || rep.GetUnread()[0].GetWhat() != "the rest of the audit" {
		t.Fatalf("records %d, unread %v", rep.GetRecords(), rep.GetUnread())
	}
}

// someoneElse is the fixture's ledger as another process with the file opens it: its appends chain
// onto the log as agentd's do, and this agentd's tip knows nothing of them.
func (f *verifyFixture) someoneElse(t *testing.T) *ledger.Ledger {
	t.Helper()
	var seq int
	var name, path string
	if err := f.s.Ledger.DB.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	other, err := ledger.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { other.Close() })
	return other
}

// foreignMismatch is the report's mismatch of records after this agentd's last, if any.
func foreignMismatch(rep *agentosv1.AuditReport) string {
	for _, m := range rep.GetAnchorsMismatched() {
		if strings.HasPrefix(m.GetWhy(), "the audit log goes on past the last record this agentd appended") {
			return m.GetWhy()
		}
	}
	return ""
}

// A record appended by someone else, correctly chained after the last one this agentd appended,
// leaves every record of this agentd's in place: a decision nobody gave, say. verify reports each
// record after this agentd's last as one it did not append; and once agentd has found the chain
// moved (its next append refused), that too.
func TestVerifyAuditOfRecordsAppendedBySomeoneElse(t *testing.T) {
	ctx := context.Background()
	f := newVerifyFixture(t)
	run := f.run(t, 1)
	if rep := f.verify(t); foreignMismatch(rep) != "" || len(rep.GetAnchorsMismatched()) != 0 {
		t.Fatalf("before: %v", rep.GetAnchorsMismatched())
	}
	other := f.someoneElse(t)
	h, err := audit.Record(ctx, other, audit.Event{WorkflowID: run.GetRunId(), Type: "approval.decided",
		Actor: audit.Operator("mallory"), Payload: map[string]any{"approval_id": "a1", "decision": "approve"}})
	if err != nil {
		t.Fatal(err)
	}
	rep := f.verify(t)
	if want := fmt.Sprintf(`with records it did not append: %d ("approval.decided" by "operator:mallory"); someone else`, h.Seq); !strings.Contains(foreignMismatch(rep), want) {
		t.Fatalf("mismatches %v, want one saying %s", rep.GetAnchorsMismatched(), want)
	}
	// agentd's next append finds the chain moved, and refuses: verify says that too.
	if _, err := audit.Record(ctx, f.s.Ledger, audit.Event{Type: "test.next", Actor: audit.Agentd}); !errors.Is(err, audit.ErrChainMoved) {
		t.Fatalf("an append onto someone else's record: %v", err)
	}
	if rep := f.verify(t); !slices.ContainsFunc(rep.GetAnchorsMismatched(), func(m *agentosv1.AnchorMismatch) bool {
		return strings.HasPrefix(m.GetWhy(), "agentd found the audit log changed under it, and has appended nothing to it since")
	}) {
		t.Fatalf("mismatches %v", rep.GetAnchorsMismatched())
	}
	// Named up to ten.
	for range 11 {
		if _, err := audit.Record(ctx, other, audit.Event{Type: "test.more", Actor: audit.Agentd}); err != nil {
			t.Fatal(err)
		}
	}
	if why := foreignMismatch(f.verify(t)); strings.Count(why, `"test.more"`) != 9 || !strings.HasSuffix(why, "and more; someone else wrote them") {
		t.Fatalf("mismatch %q", why)
	}
}

// verify racing this agentd's own appends never takes one of them for someone else's: the
// snapshot and the tip are read under the write turn (ReadAtTurn).
func TestVerifyAuditRacingHonestAppends(t *testing.T) {
	ctx := context.Background()
	f := newVerifyFixture(t)
	f.run(t, 1)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := audit.Record(ctx, f.s.Ledger, audit.Event{Type: "test.flood", Actor: audit.Agentd}); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	for range 100 {
		if rep := f.verify(t); len(rep.GetAnchorsMismatched()) != 0 {
			close(stop)
			wg.Wait()
			t.Fatalf("an honest append taken for someone else's: %v", rep.GetAnchorsMismatched())
		}
	}
	close(stop)
	wg.Wait()
	if n := len(auditRecords(t, f.s)); n < 50 {
		t.Fatalf("only %d records: the appends did not race verify", n)
	}
}

// plantingAttempts is a gateway that, asked whether an attempt is executing, first has a trigger
// planted in the ledger: the schema changed while verify runs, past its first check.
type plantingAttempts struct{ plant func() }

func (a plantingAttempts) Executing(string, string, int64) bool         { a.plant(); return false }
func (a plantingAttempts) Faulted(string, string, int64) (string, bool) { return "", false }

// verify reads the ledger again after its snapshot (an attempt that ended meanwhile), in a read
// transaction of the ledger's: a schema changed by then fails it, naming the change, rather than be
// read through.
func TestVerifyAuditReadsAgainThroughTheCheck(t *testing.T) {
	f := newVerifyFixture(t)
	run := f.run(t, 1)
	f.record(t, run.GetRunId(), "call.started", map[string]any{"activity_id": "9:audit_v1", "attempt": 1,
		"policy": map[string]any{"decision": "allow"}})
	planted := false
	f.s.Attempts = plantingAttempts{plant: func() {
		if !planted {
			planted = true
			if _, err := f.s.Ledger.DB.Exec(`CREATE TRIGGER late BEFORE INSERT ON calls BEGIN SELECT 1; END`); err != nil {
				t.Error(err)
			}
		}
	}}
	_, err := f.s.VerifyAudit(context.Background(), &agentosv1.VerifyAuditRequest{})
	if !planted || !errors.Is(err, ledger.ErrSchemaMismatch) || !strings.Contains(err.Error(), `trigger "late" on "calls"`) {
		t.Fatalf("verify with the schema changed under it (planted %v): %v", planted, err)
	}
}

// fakeAttempts is a gateway's answers about its attempts.
type fakeAttempts struct {
	executing map[[2]string]bool   // (run, activity) of attempt 1
	faulted   map[[2]string]string // why
}

func (a fakeAttempts) Executing(workflowID, activityID string, attempt int64) bool {
	return attempt == 1 && a.executing[[2]string{workflowID, activityID}]
}

func (a fakeAttempts) Faulted(workflowID, activityID string, attempt int64) (string, bool) {
	why, ok := a.faulted[[2]string{workflowID, activityID}]
	return why, ok && attempt == 1
}

// A call.started left open after its run ended: in flight while its attempt is executing in this
// agentd (a run cancelled ends at once, its model call still going), and while its record lands
// after verify's snapshot; reported apart when a fault lost its record; a mismatch otherwise.
func TestVerifyAuditOfAnAttemptGoingOnAfterItsRunEnded(t *testing.T) {
	allow := map[string]any{"decision": "allow"}
	for name, c := range map[string]struct {
		attempts   Attempts
		lateRecord bool
		mismatch   bool
		unrecorded bool
	}{
		"executing":                     {attempts: fakeAttempts{executing: map[[2]string]bool{{"", "9:audit_v1"}: true}}},
		"its record after the snapshot": {lateRecord: true},
		"its record lost to a fault":    {attempts: fakeAttempts{faulted: map[[2]string]string{{"", "9:audit_v1"}: "the disk is full"}}, unrecorded: true},
		"neither":                       {attempts: fakeAttempts{}, mismatch: true},
		"no gateway":                    {mismatch: true},
	} {
		t.Run(name, func(t *testing.T) {
			f := newVerifyFixture(t)
			run := f.run(t, 1)
			wf := run.GetRunId()
			h := f.record(t, wf, "call.started", map[string]any{"activity_id": "9:audit_v1", "attempt": 1, "policy": allow})
			if a, ok := c.attempts.(fakeAttempts); ok { // the fixture's run ID, known only now
				for k, v := range a.executing {
					a.executing[[2]string{wf, k[1]}] = v
				}
				for k, v := range a.faulted {
					a.faulted[[2]string{wf, k[1]}] = v
				}
			}
			f.s.Attempts = c.attempts
			if c.lateRecord { // appended once the snapshot has its records of the run's anchors, which come after the chain
				hookedReads(t, f.s.Ledger, "WHERE workflow_id = ? ORDER BY seq", func() {
					f.record(t, wf, "call", map[string]any{"activity_id": "9:audit_v1", "attempt": 1, "status": "ok",
						"policy": allow, "started_seq": h.Seq})
				})
			}
			rep := f.verify(t)
			calls := callMismatches(rep)
			if (len(calls) == 1) != c.mismatch || len(calls) > 1 || (len(rep.GetAttemptsUnrecorded()) == 1) != c.unrecorded {
				t.Fatalf("call mismatches %q, unrecorded %v", calls, rep.GetAttemptsUnrecorded())
			}
			if c.unrecorded && (rep.GetAttemptsUnrecorded()[0].GetSeq() != h.Seq || rep.GetAttemptsUnrecorded()[0].GetWhy() != "the disk is full") {
				t.Fatalf("unrecorded %v", rep.GetAttemptsUnrecorded())
			}
		})
	}
}

// Each execution Temporal says was terminated, agentd recorded terminating first (it writes
// run.terminating before it asks Temporal, and run.terminated after). One with no run.terminating
// of it was terminated outside agentd, by someone with Temporal's API or by Temporal itself: no
// change to the ledger, so no mismatch. It is listed, with when, and who its caller said it was and
// why, from its history.
func TestVerifyAuditOfTerminatedExecutions(t *testing.T) {
	f := newVerifyFixture(t)
	run := f.run(t, 1)
	key := run.GetRunId() + "/" + run.GetTemporalRunId()
	at := time.Now().Add(-time.Minute).UTC()
	f.tc.mu.Lock()
	f.tc.statuses = map[string]enumspb.WorkflowExecutionStatus{key: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED}
	f.tc.histories[key] = append(f.tc.histories[key], &historypb.HistoryEvent{EventId: 99,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, EventTime: timestamppb.New(at),
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
				Reason: "GrpcMessageTooLarge", Identity: "history-service\nrun r2: fine"}}})
	f.tc.mu.Unlock()
	outside := func(rep *agentosv1.AuditReport) {
		t.Helper()
		want := &agentosv1.TerminatedExecution{RunId: run.GetRunId(), TemporalRunId: run.GetTemporalRunId(),
			At: at.Format(time.RFC3339Nano), ClaimedIdentity: "history-service\\nrun r2: fine", ClaimedReason: "GrpcMessageTooLarge"}
		if len(rep.GetRunsMismatched()) != 0 || len(rep.GetTerminatedOutsideAgentd()) != 1 ||
			!proto.Equal(rep.GetTerminatedOutsideAgentd()[0], want) {
			t.Fatalf("mismatched %v; terminated outside agentd %v, want %v", rep.GetRunsMismatched(), rep.GetTerminatedOutsideAgentd(), want)
		}
	}
	outside(f.verify(t))
	// Neither an operator's run.terminating of it, nor agentd's of another execution, is agentd's of it.
	for _, e := range []audit.Event{
		{WorkflowID: run.GetRunId(), Type: "run.terminating", Actor: audit.Operator("mallory"),
			Payload: map[string]any{"temporal_run_id": run.GetTemporalRunId(), "why": "x"}},
		{WorkflowID: run.GetRunId(), Type: "run.terminating", Actor: audit.Agentd,
			Payload: map[string]any{"temporal_run_id": "another-execution", "why": "x"}},
	} {
		if _, err := audit.Record(context.Background(), f.s.Ledger, e); err != nil {
			t.Fatal(err)
		}
	}
	outside(f.verify(t))
	before := len(auditRecords(t, f.s))
	asked := -1 // how many records there were as agentd asked Temporal
	f.tc.onTerminate = func() { asked = len(auditRecords(t, f.s)) }
	if err := f.s.terminate(context.Background(), run.GetRunId(), run.GetTemporalRunId(), "a test"); err != nil {
		t.Fatal(err)
	}
	rows := auditRecords(t, f.s)[before:]
	if len(rows) != 2 || rows[0].Type != "run.terminating" || rows[1].Type != "run.terminated" || asked != before+1 {
		t.Fatalf("records %+v; Temporal asked after %d records", rows, asked)
	}
	if rep := f.verify(t); len(rep.GetRunsMismatched()) != 0 || len(rep.GetTerminatedOutsideAgentd()) != 0 {
		t.Fatalf("mismatched %v; terminated outside agentd %v", rep.GetRunsMismatched(), rep.GetTerminatedOutsideAgentd())
	}
}

// A termination Temporal did not do is recorded so (run.terminate_failed, with its error): it closes
// agentd's run.terminating, which is then no termination of agentd's. An execution Temporal says was
// terminated after it was terminated by someone else, as far as agentd's records go.
func TestVerifyAuditOfATerminationThatFailed(t *testing.T) {
	f := newVerifyFixture(t)
	run := f.run(t, 1)
	key := run.GetRunId() + "/" + run.GetTemporalRunId()
	f.tc.mu.Lock()
	f.tc.statuses = map[string]enumspb.WorkflowExecutionStatus{key: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED}
	f.tc.histories[key] = append(f.tc.histories[key], &historypb.HistoryEvent{EventId: 99,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, EventTime: timestamppb.Now(),
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
				Reason: "mine now", Identity: "mallory@laptop"}}})
	f.tc.terminateErr = errors.New("rpc error: code = Unavailable desc = connection refused" + strings.Repeat(" and more", 1000))
	f.tc.mu.Unlock()
	before := len(auditRecords(t, f.s))
	if err := f.s.terminate(context.Background(), run.GetRunId(), run.GetTemporalRunId(), "a test"); err == nil {
		t.Fatal("a termination Temporal refused succeeded")
	}
	rows := auditRecords(t, f.s)[before:]
	if len(rows) != 2 || rows[0].Type != "run.terminating" || rows[1].Type != "run.terminate_failed" || rows[1].Actor != audit.Agentd {
		t.Fatalf("records %+v", rows)
	}
	var p struct {
		TemporalRunID string `json:"temporal_run_id"`
		Error         string `json:"error"`
	}
	// The error as agentd got it, bounded as every text of a record is (audit.Text).
	if err := json.Unmarshal([]byte(rows[1].Payload), &p); err != nil || p.TemporalRunID != run.GetTemporalRunId() ||
		!strings.Contains(p.Error, "connection refused") || len(p.Error) > 2100 {
		t.Fatalf("payload %s %v", rows[1].Payload, err)
	}
	rep := f.verify(t)
	if len(rep.GetRunsMismatched()) != 0 || len(rep.GetStateMismatched()) != 0 || len(rep.GetTerminatedOutsideAgentd()) != 1 ||
		rep.GetTerminatedOutsideAgentd()[0].GetClaimedIdentity() != "mallory@laptop" {
		t.Fatalf("report %v", rep)
	}
	// A run.terminated says Temporal did it (alone, as an agentd from before run.terminating wrote it).
	if _, err := audit.Record(context.Background(), f.s.Ledger, audit.Event{WorkflowID: run.GetRunId(), Type: "run.terminated",
		Actor: audit.Agentd, Payload: map[string]any{"temporal_run_id": run.GetTemporalRunId(), "why": "a test"}}); err != nil {
		t.Fatal(err)
	}
	if rep := f.verify(t); len(rep.GetTerminatedOutsideAgentd()) != 0 {
		t.Fatalf("report %v", rep)
	}
}
