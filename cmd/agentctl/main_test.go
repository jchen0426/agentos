package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/budget"
	"github.com/jchen0426/agentos/internal/control"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// A failed run whose failure message the Worker wrote to rewrite the screen and forge a line.
var hostile = &agentosv1.Run{
	RunId: "r1", Task: "security-audit", State: "FAILED", CreatedAt: "2026-10-17T00:00:00Z",
	ResultJson: "{\"x\":\"\u009b2J\u202e\"}",
	Failure:    "boom\x1b]0;title\x07\nstate\tSUCCEEDED",
}

func TestTextOutputShowsWorkerTextInert(t *testing.T) {
	var b bytes.Buffer
	if err := writeRun(&b, hostile); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, r := range out {
		if !unicode.IsPrint(r) && r != '\n' {
			t.Fatalf("raw %U in the output:\n%s", r, out)
		}
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 8 {
		t.Errorf("got %d lines, want 8 (one per field):\n%s", len(lines), out)
	}
	states := 0
	for _, l := range lines {
		if strings.HasPrefix(l, "state ") {
			states++
		}
	}
	if states != 1 {
		t.Errorf("got %d state lines, want 1:\n%s", states, out)
	}
}

func TestJSONOutputShowsWorkerTextInert(t *testing.T) {
	b, err := encodeJSON(hostile)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range string(b) {
		if !unicode.IsPrint(r) {
			t.Fatalf("raw %U in %s", r, b)
		}
	}
	var back agentosv1.Run
	if err := protojson.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.GetFailure() != hostile.GetFailure() || back.GetResultJson() != hostile.GetResultJson() {
		t.Errorf("the JSON no longer decodes to the same run: %s", b)
	}
}

func TestUSD(t *testing.T) {
	for in, want := range map[string]int64{"2.50": 2_500_000, "$1": 1_000_000, "0.000001": 1, "10.123456": 10_123_456} {
		if got, err := parseUSD(in); err != nil || got != want {
			t.Errorf("parseUSD(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "0", "0.00", "-1", "1.2345678", "1,5", "1e3", ".5", "99999999999999999999"} {
		if got, err := parseUSD(in); err == nil {
			t.Errorf("parseUSD(%q) = %d, want an error", in, got)
		}
	}
	for micros, want := range map[int64]string{5_000_000: "$5.00", 4500: "$0.0045", 1: "$0.000001", 0: "$0.00", 12_340_000: "$12.34"} {
		if got := usd(micros); got != want {
			t.Errorf("usd(%d) = %q, want %q", micros, got, want)
		}
	}
}

// An approval card shows text a model, a tool or the outside world wrote: the patch, the
// findings, the payload. On the terminal none of it can rewrite the screen or forge a line of
// the card (a second status, a payload hash), and the patch still reads as written, line by line.
func TestApprovalCardShowsTheirTextInert(t *testing.T) {
	a := &agentosv1.Approval{
		ApprovalId: "a1", RunId: "r1", Tool: "github.open_draft_pr", Status: "PENDING",
		CreatedAt: "2026-10-20T00:00:00.000000000Z", ExpiresAt: "2026-10-21T00:00:00.000000000Z",
		PayloadSha256: strings.Repeat("ab", 32),
		PayloadJson:   `{"title":"fix\u001b[2J","body":"x\nstatus  APPROVED"}`,
		ContextJson: `{"task":{"findings":[{"cwe":"CWE-89","title":"t\u009b2J"}]},` +
			`"write":{"patch":"--- a/x\n+++ b/x\n@@ -1 +1 @@\n-a\n+b\u001b]0;pwned\u0007\nstatus\tAPPROVED\n"}}`,
	}
	var b bytes.Buffer
	if err := writeApproval(&b, a); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, r := range out {
		if !unicode.IsPrint(r) && r != '\n' && r != '\t' {
			t.Fatalf("raw %U in the card:\n%s", r, out)
		}
	}
	statuses := 0
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "status") {
			statuses++
		}
	}
	if statuses != 1 || !strings.Contains(out, "      +++ b/x\n") || !strings.Contains(out, "agentctl approvals approve a1 --payload-sha256 "+a.PayloadSha256) {
		t.Fatalf("card:\n%s", out)
	}
}

// One approval's payload can be far larger than gRPC's default of 4 MiB (a pull request's runs to
// 20 files of 1 MiB, more once escaped). It must neither keep the queue from being listed nor
// keep its own card from being read: without the card, nobody can decide it.
func TestALargeApprovalStaysReadable(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("/tmp", "agentctl-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	led, err := ledger.Open(ctx, filepath.Join(dir, "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { led.Close() })
	if err := led.Q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "wf", Task: "t", ParamsJson: "{}", BudgetLimitMicros: 1,
		CreatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]string{"content": strings.Repeat("\x00", 1<<20)}) // 6 MiB once escaped
	ref, err := led.PutBlob(ctx, payload)
	if err != nil {
		t.Fatal(err)
	}
	sum := strings.TrimPrefix(ref, "sha256:")
	card, _ := led.PutBlob(ctx, []byte(`{}`))
	if _, err := led.Q.InsertEffect(ctx, db.InsertEffectParams{EffectID: "e", WorkflowID: "wf", Tool: "github.open_draft_pr",
		EffectClass: "lookup", PayloadSha256: sum, PayloadBlob: sql.NullString{String: ref, Valid: true}, IdempotencyKey: "k",
		Status: "AWAITING_APPROVAL", CreatedAt: ledger.Now(), UpdatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := led.Q.InsertApproval(ctx, db.InsertApprovalParams{ApprovalID: "a", EffectID: "e", WorkflowID: "wf",
		Tool: "github.open_draft_pr", ArgsJson: "{}", PayloadSha256: sum, ContextBlob: card, CreatedAt: ledger.Stamp(time.Now()),
		ExpiresAt: ledger.Stamp(time.Now().Add(time.Hour))}); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(dir, "c.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	agentosv1.RegisterControlServer(srv, &control.Server{Ledger: led, Budget: &budget.Budget{Ledger: led}})
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("unix://"+sock, dialOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := agentosv1.NewControlClient(conn)
	list, err := c.ListPending(ctx, &agentosv1.ListPendingRequest{})
	if err != nil || len(list.GetItems()) != 1 || list.GetItems()[0].GetPayloadSha256() != sum {
		t.Fatalf("list: %v %v", list, err)
	}
	a, err := c.GetApproval(ctx, &agentosv1.GetApprovalRequest{ApprovalId: "a"})
	if err != nil || len(a.GetPayloadJson()) < 6<<20 {
		t.Fatalf("show: %d bytes, %v", len(a.GetPayloadJson()), err)
	}
}

// The audit report quotes what a Worker named (activity IDs) and what Temporal said: on the
// terminal none of it can rewrite the screen or forge a line of the report, such as a second
// "chain" line that says the chain is whole.
func TestTheAuditReportShowsTheirTextInert(t *testing.T) {
	r := &agentosv1.AuditReport{Records: 4, HeadSeq: 4, HeadSha256: strings.Repeat("ab", 32), BrokenAtSeq: 5,
		BrokenReason: "its sha256 is not the hash of its fields", AnchorsChecked: 3, RunsChecked: 1,
		AnchorsMismatched: []*agentosv1.AnchorMismatch{{RunId: "r1", ActivityId: "1:x\x1b[2J\nchain\twhole: 9 records", Seq: 4,
			Why: "the record\u009b differs"}},
		RunsMismatched:   []*agentosv1.RunMismatch{{RunId: "r2\u202e", Why: "the ledger has no row of the run"}},
		StateMismatched:  []*agentosv1.StateMismatch{{Kind: "effect", Id: "e1\nchain\twhole: 9 records", Why: "is FAILED\x07"}},
		AttemptsCutShort: []*agentosv1.CutShortAttempt{{RunId: "r4", ActivityId: "2:x\nchain\twhole", Attempt: 1, Seq: 3, RestartedSeq: 4}},
		RunsUnavailable:  []*agentosv1.UnavailableRun{{RunId: "r3", Why: "gone\r"}}}
	var b bytes.Buffer
	if err := writeAuditReport(&b, r); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, c := range out {
		if !unicode.IsPrint(c) && c != '\n' {
			t.Fatalf("raw %U in the report:\n%s", c, out)
		}
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	chains := 0
	for _, l := range lines {
		if strings.HasPrefix(l, "chain") {
			chains++
		}
	}
	if chains != 1 || len(lines) != 12 || !strings.Contains(out, "BROKEN at seq 5") || !strings.Contains(out, "1 do not match the ledger") ||
		!strings.Contains(out, "1 differences from what the records say") {
		t.Fatalf("%d lines, %d chain lines:\n%s", len(lines), chains, out)
	}
}

// What a Worker names (activity IDs, and the IDs of workflows it starts on its task queue) and the
// keys of other chains, which anyone in the namespace may make, are quoted in the report: none reads
// as part of the line around it, as an activity ID that says the record matched would.
func TestTheAuditReportQuotesWhatOthersNamed(t *testing.T) {
	r := &agentosv1.AuditReport{Records: 4, HeadSeq: 4,
		AnchorsMismatched: []*agentosv1.AnchorMismatch{{RunId: "r1", ActivityId: "7, seq 9: matches; run r2", Seq: 4, Why: "differs"},
			{Seq: 3, Why: "this agentd's start: gone"}},
		RunsMismatched:   []*agentosv1.RunMismatch{{RunId: "child: the ledger agrees", Why: "the ledger has no row of the run"}},
		StateMismatched:  []*agentosv1.StateMismatch{{Kind: "call", Id: "r3: fine", Why: "differs"}},
		AttemptsCutShort: []*agentosv1.CutShortAttempt{{RunId: "r4", ActivityId: "2:x, attempt 1: fine", Attempt: 1, Seq: 3, RestartedSeq: 4}},
		RunsUnavailable:  []*agentosv1.UnavailableRun{{RunId: "r5: checked", Why: "gone"}},
		OtherChains:      []string{"abc, def"},
		UncheckedTables:  []*agentosv1.UncheckedTable{{Name: "x: checked, ok", Why: "made by someone"}},
		AttemptsUnrecorded: []*agentosv1.UnrecordedAttempt{{RunId: "r6", ActivityId: "3:y, attempt 1: fine", Attempt: 1, Seq: 5,
			Why: "the disk is full"}},
		TerminatedOutsideAgentd: []*agentosv1.TerminatedExecution{{RunId: "r7", TemporalRunId: "e7: by agentd",
			At: "2026-10-22T00:00:00Z", ClaimedIdentity: "agentd\", reason: all fine", ClaimedReason: "a cancel"},
			{RunId: "r8", TemporalRunId: "e8"}}}
	var b bytes.Buffer
	if err := writeAuditReport(&b, r); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{`run "r1", call "7, seq 9: matches; run r2", seq 4: differs`, "seq 3: this agentd's start: gone",
		`run "child: the ledger agrees": the ledger has no row`, `call "r3: fine": differs`,
		`run "r4", call "2:x, attempt 1: fine", attempt 1:`, `run "r5: checked": gone`, `"abc, def"; 0 anchor workflows`,
		`"x: checked, ok": made by someone`, `run "r6", call "3:y, attempt 1: fine", attempt 1: its call.started (seq 5) has no call record`,
		`run "r7", execution "e7: by agentd", at 2026-10-22T00:00:00Z, not by agentd; its caller says it is "agentd\", reason: all fine", and why: "a cancel"`,
		`run "r8", execution "e8", at a time its history does not give, not by agentd; its caller says it is "", and why: ""`} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q in\n%s", want, out)
		}
	}
}

// agentctl audit verify exits 1 when anything does not match, 2 when only some runs could not be
// checked, unless --allow-unavailable accepts that, and 0 otherwise.
func TestTheAuditVerdict(t *testing.T) {
	gone := []*agentosv1.UnavailableRun{{RunId: "r", Why: "gone"}}
	for name, c := range map[string]struct {
		r     *agentosv1.AuditReport
		allow bool
		code  int
	}{
		"all checked and matching": {&agentosv1.AuditReport{Records: 3}, false, 0},
		"a broken chain":           {&agentosv1.AuditReport{BrokenAtSeq: 2}, false, 1},
		"an anchor":                {&agentosv1.AuditReport{AnchorsMismatched: []*agentosv1.AnchorMismatch{{}}}, false, 1},
		"a run":                    {&agentosv1.AuditReport{RunsMismatched: []*agentosv1.RunMismatch{{}}}, false, 1},
		"a table":                  {&agentosv1.AuditReport{StateMismatched: []*agentosv1.StateMismatch{{}}}, true, 1},
		"a run unchecked":          {&agentosv1.AuditReport{RunsUnavailable: gone}, false, 2},
		"a run unchecked, allowed": {&agentosv1.AuditReport{RunsUnavailable: gone}, true, 0},
		"unchecked and a mismatch": {&agentosv1.AuditReport{RunsUnavailable: gone, BrokenAtSeq: 1}, true, 1},
		"before the log":           {&agentosv1.AuditReport{RunsBeforeLog: gone}, true, 2}, // --allow-unavailable is not it
		// Terminated outside agentd: Temporal's doing, or someone's with its API, not the ledger's.
		"terminated outside agentd": {&agentosv1.AuditReport{TerminatedOutsideAgentd: []*agentosv1.TerminatedExecution{{RunId: "r"}}},
			false, 0},
	} {
		err := verdict(c.r, c.allow, false)
		var exit exitError
		switch {
		case c.code == 0 && err != nil:
			t.Errorf("%s: %v, want none", name, err)
		case c.code != 0 && (!errors.As(err, &exit) || exit.code != c.code):
			t.Errorf("%s: %v, want exit %d", name, err, c.code)
		}
	}
}

// verifier is an agentd whose VerifyAudit answers as a test says.
type verifier struct {
	agentosv1.UnimplementedControlServer
	answer func(context.Context, *agentosv1.VerifyAuditRequest) (*agentosv1.AuditReport, error)
}

func (v verifier) VerifyAudit(ctx context.Context, req *agentosv1.VerifyAuditRequest) (*agentosv1.AuditReport, error) {
	return v.answer(ctx, req)
}

// The status agentctl audit verify exits with: 0, 1 and 2 for what the report says, 3 when there is
// no report: agentd is not there, or does not answer in time, or refuses the request.
func TestTheAuditVerifyExitStatus(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "agentctl-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "control.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex // the cases set answer; the server's goroutines call it
	var answer func(context.Context, *agentosv1.VerifyAuditRequest) (*agentosv1.AuditReport, error)
	srv := grpc.NewServer()
	agentosv1.RegisterControlServer(srv, verifier{answer: func(ctx context.Context, req *agentosv1.VerifyAuditRequest) (*agentosv1.AuditReport, error) {
		mu.Lock()
		a := answer
		mu.Unlock()
		return a(ctx, req)
	}})
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)
	exit := func(socket string, args ...string) int {
		t.Helper()
		cmd := auditVerifyCmd(&globals{socket: socket, output: "json"})
		cmd.SetArgs(args)
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		stdout := os.Stdout
		devnull, _ := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		os.Stdout = devnull
		err := cmd.Execute()
		os.Stdout = stdout
		devnull.Close()
		var e exitError
		switch {
		case err == nil:
			return 0
		case errors.As(err, &e):
			return e.code
		}
		t.Fatalf("an error without its status: %v", err)
		return -1
	}
	report := func(r *agentosv1.AuditReport) func(context.Context, *agentosv1.VerifyAuditRequest) (*agentosv1.AuditReport, error) {
		return func(context.Context, *agentosv1.VerifyAuditRequest) (*agentosv1.AuditReport, error) { return r, nil }
	}
	gone := []*agentosv1.UnavailableRun{{RunId: "r", Why: "gone"}}
	for name, c := range map[string]struct {
		answer func(context.Context, *agentosv1.VerifyAuditRequest) (*agentosv1.AuditReport, error)
		args   []string
		want   int
	}{
		"clean":                             {report(&agentosv1.AuditReport{Records: 2}), nil, 0},
		"runs from before the log":          {report(&agentosv1.AuditReport{RunsBeforeLog: gone}), nil, 2},
		"a mismatch":                        {report(&agentosv1.AuditReport{StateMismatched: []*agentosv1.StateMismatch{{}}}), nil, 1},
		"a run unchecked":                   {report(&agentosv1.AuditReport{RunsUnavailable: gone}), nil, 2},
		"runs from before the log, allowed": {report(&agentosv1.AuditReport{RunsBeforeLog: gone}), []string{"--allow-before-log"}, 0},
		"other chains, allowed": {func(_ context.Context, req *agentosv1.VerifyAuditRequest) (*agentosv1.AuditReport, error) {
			if !slices.Equal(req.GetAllowChains(), []string{"0123456789abcdef", "fedcba9876543210"}) {
				return &agentosv1.AuditReport{AnchorsMismatched: []*agentosv1.AnchorMismatch{{}}}, nil
			}
			return &agentosv1.AuditReport{}, nil
		}, []string{"--allow-chain", "0123456789abcdef", "--allow-chain", "fedcba9876543210"}, 0},
		"replaced ledgers, allowed": {func(_ context.Context, req *agentosv1.VerifyAuditRequest) (*agentosv1.AuditReport, error) {
			if !slices.Equal(req.GetAllowReplacedLedgers(), []string{"1111111111111111", "2222222222222222"}) || len(req.GetAllowChains()) != 0 {
				return &agentosv1.AuditReport{AnchorsMismatched: []*agentosv1.AnchorMismatch{{}}}, nil
			}
			return &agentosv1.AuditReport{}, nil
		}, []string{"--allow-replaced-ledger", "1111111111111111", "--allow-replaced-ledger", "2222222222222222"}, 0},
		"a run unchecked, allowed": {report(&agentosv1.AuditReport{RunsUnavailable: gone}), []string{"--allow-unavailable"}, 0},
		// Past its bounds: never 0.
		"a partial read": {report(&agentosv1.AuditReport{Unread: []*agentosv1.Unread{{What: "anchor workflows", Count: 1}}}), nil, 3},
		"a partial read, a mismatch": {report(&agentosv1.AuditReport{Unread: []*agentosv1.Unread{{What: "anchor workflows", Count: 1}},
			StateMismatched: []*agentosv1.StateMismatch{{}}}), nil, 1},
		"a partial read, runs unchecked and allowed": {report(&agentosv1.AuditReport{Unread: []*agentosv1.Unread{{What: "anchor workflows", Count: 1}},
			RunsUnavailable: gone}), []string{"--allow-unavailable"}, 3},
		// Records a fault lost: not checked.
		"a call record lost": {report(&agentosv1.AuditReport{AttemptsUnrecorded: []*agentosv1.UnrecordedAttempt{{}}}), nil, 2},
		"a call record lost, runs unchecked and allowed": {report(&agentosv1.AuditReport{
			AttemptsUnrecorded: []*agentosv1.UnrecordedAttempt{{}}, RunsUnavailable: gone}), []string{"--allow-unavailable"}, 2},
		"a call record lost, a mismatch": {report(&agentosv1.AuditReport{AttemptsUnrecorded: []*agentosv1.UnrecordedAttempt{{}},
			StateMismatched: []*agentosv1.StateMismatch{{}}}), nil, 1},
		"no such run": {func(context.Context, *agentosv1.VerifyAuditRequest) (*agentosv1.AuditReport, error) {
			return nil, status.Error(codes.NotFound, "no run x")
		}, []string{"--run", "x"}, 3},
		"no answer in time": {func(ctx context.Context, _ *agentosv1.VerifyAuditRequest) (*agentosv1.AuditReport, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}, []string{"--timeout", "100ms"}, 3},
	} {
		mu.Lock()
		answer = c.answer
		mu.Unlock()
		start := time.Now()
		if got := exit(socket, c.args...); got != c.want || time.Since(start) > 5*time.Second {
			t.Errorf("%s: exit %d after %v, want %d", name, got, time.Since(start), c.want)
		}
	}
	if got := exit(filepath.Join(dir, "nobody.sock")); got != 3 {
		t.Errorf("agentd not there: exit %d, want 3", got)
	}
}
