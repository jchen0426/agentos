package control

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/tasks"
)

// A start whose parameters are refused is an invalid argument; one whose parameters could not
// be checked (the git tool server failed) is unavailable, and may work later. Either way the
// refusal is recorded, by whoever asked, with what they asked for.
func TestStartRunSaysWhyResolveFailed(t *testing.T) {
	for err, want := range map[error]codes.Code{
		errors.New("commit abc1234 not found in the mirror"):                                codes.InvalidArgument,
		&tasks.Unavailable{Err: errors.New("the git tool server failed on resolve_commit")}: codes.Unavailable,
	} {
		s, _ := newServer(t)
		s.Tasks["t"] = tasks.Task{Name: "t", Resolve: func(context.Context, map[string]string) (any, map[string]string, error) {
			return nil, nil, err
		}}
		_, got := s.StartRun(asOperator(context.Background()), &agentosv1.StartRunRequest{Task: "t",
			Params: map[string]string{"commit": "abc1234"}})
		if status.Code(got) != want || status.Convert(got).Message() != err.Error() {
			t.Errorf("%v: got %v, want %v", err, got, want)
		}
		actor, uid := me(t)
		rows := auditRecords(t, s)
		if len(rows) != 1 || rows[0].Type != "run.refused" || rows[0].Actor != actor || rows[0].PeerUid != uid || rows[0].WorkflowID != "" ||
			!strings.Contains(rows[0].Payload, `"params":{"commit":"abc1234"},"task":"t","why":`) ||
			!strings.Contains(rows[0].Payload, err.Error()) {
			t.Errorf("records %+v", rows)
		}
	}
	// So is a task agentd does not have, and a budget it does not take.
	s, _ := newServer(t)
	for _, req := range []*agentosv1.StartRunRequest{{Task: "nope"}, {Task: "t", BudgetLimitMicros: -1}} {
		if _, err := s.StartRun(asOperator(context.Background()), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%v: %v", req, err)
		}
	}
	if rows := auditRecords(t, s); len(rows) != 2 || rows[0].Type != "run.refused" || rows[1].Type != "run.refused" {
		t.Errorf("records %+v", rows)
	}
}

// asOperator is ctx as a call on the control socket from this process's user carries it.
func asOperator(ctx context.Context) context.Context { return asUID(ctx, int64(os.Getuid())) }

// asUID is ctx as a call on the control socket from a process of uid carries it.
func asUID(ctx context.Context, uid int64) context.Context {
	return peer.NewContext(ctx, &peer.Peer{AuthInfo: Peer{UID: uid}})
}

// A reconciliation whose recorded payload changed since is listed all the same, without the bytes:
// one blob changed under its key does not keep a person from the others.
func TestAReconciliationWhosePayloadChangedIsListed(t *testing.T) {
	s, _ := newServer(t)
	ctx := context.Background()
	if err := s.Ledger.Q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "wf", Task: "t", ParamsJson: "{}", BudgetLimitMicros: 1,
		CreatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	ref, err := s.Ledger.PutBlob(ctx, []byte(`{"title":"fix"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ledger.Q.InsertEffect(ctx, db.InsertEffectParams{EffectID: "e", WorkflowID: "wf", Tool: "github.open_draft_pr",
		EffectClass: "lookup", PayloadSha256: strings.TrimPrefix(ref, "sha256:"), PayloadBlob: sql.NullString{String: ref, Valid: true},
		IdempotencyKey: "k", Status: "UNKNOWN", CreatedAt: ledger.Now(), UpdatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ledger.DB.Exec(`UPDATE blobs SET bytes = 'TAMPERED' WHERE sha256 = ?`, strings.TrimPrefix(ref, "sha256:")); err != nil {
		t.Fatal(err)
	}
	resp, err := s.ListPending(asOperator(ctx), &agentosv1.ListPendingRequest{})
	if err != nil || len(resp.GetItems()) != 1 || resp.GetItems()[0].GetPayloadJson() != "" ||
		!strings.Contains(resp.GetItems()[0].GetReason(), "has changed since") {
		t.Fatalf("%v %v", resp, err)
	}
}

// An approval whose recorded payload's bytes changed under their key is shown as a payload changed,
// not as a failure to read it: the reviewer is told not to approve it.
func TestAnApprovalWhosePayloadChangedUnderItsKey(t *testing.T) {
	s, _ := newServer(t)
	ctx := context.Background()
	if err := s.Ledger.Q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "wf", Task: "t", ParamsJson: "{}", BudgetLimitMicros: 1,
		CreatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	ref, err := s.Ledger.PutBlob(ctx, []byte(`{"title":"security fix"}`))
	if err != nil {
		t.Fatal(err)
	}
	card, err := s.Ledger.PutBlob(ctx, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	sum := strings.TrimPrefix(ref, "sha256:")
	if _, err := s.Ledger.Q.InsertEffect(ctx, db.InsertEffectParams{EffectID: "e", WorkflowID: "wf", Tool: "github.open_draft_pr",
		EffectClass: "lookup", PayloadSha256: sum, PayloadBlob: sql.NullString{String: ref, Valid: true}, IdempotencyKey: "k",
		Status: "AWAITING_APPROVAL", CreatedAt: ledger.Now(), UpdatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := s.Ledger.Q.InsertApproval(ctx, db.InsertApprovalParams{ApprovalID: "a", EffectID: "e", WorkflowID: "wf",
		Tool: "github.open_draft_pr", ArgsJson: "{}", PayloadSha256: sum, ContextBlob: card, CreatedAt: ledger.Now(),
		ExpiresAt: ledger.Stamp(ledger.Clock().Add(time.Hour))}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ledger.DB.Exec(`UPDATE blobs SET bytes = '{"title":"security fiX"}' WHERE sha256 = ?`, sum); err != nil {
		t.Fatal(err)
	}
	_, err = s.GetApproval(asOperator(ctx), &agentosv1.GetApprovalRequest{ApprovalId: "a"})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "does not hash to") {
		t.Fatalf("got %v", err)
	}
}
