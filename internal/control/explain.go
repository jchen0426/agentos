package control

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/explain"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// ExplainRun says why a run is where it is (design doc, section 5, item 9): its history in Temporal,
// read with agentd's ledger (internal/explain). A run agentd did not start is NotFound. The Worker
// shapes the history: whatever it does to explain, a panic is the recovery interceptor's (Recover),
// and agentd, which serves every run's gateway calls, stays up.
func (s *Server) ExplainRun(ctx context.Context, req *agentosv1.ExplainRunRequest) (*agentosv1.Explanation, error) {
	r, err := s.lookup(ctx, req.GetRunId())
	if err != nil {
		return nil, err
	}
	run, err := s.describe(ctx, r)
	if err != nil {
		return nil, err
	}
	in := explain.Input{RunID: r.WorkflowID, Task: s.Tasks[r.Task], Run: r, State: run.GetState(), Now: ledger.Clock()}
	if err := explain.ReadTemporal(ctx, s.Temporal, &in); err != nil {
		return nil, status.Errorf(codes.Unavailable, "describe: %v", err)
	}
	// The ledger once the history is read, as one snapshot.
	e, err := explain.Explain(in, func(want explain.Wanted) (explain.Ledger, error) {
		var l explain.Ledger
		err := s.Ledger.Read(ctx, func(q *db.Queries) error {
			var err error
			l, err = explain.ReadLedger(ctx, q, r, want)
			return err
		})
		return l, err
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "explain run %s: %v", r.WorkflowID, err)
	}
	return e, nil
}
