package gateway

import (
	"context"

	"github.com/jchen0426/agentos/internal/githubtool"
	"github.com/jchen0426/agentos/internal/ledger"
)

// RunRecords is what github.comment_commit reads of agentd's own record of a run: its effects'
// results, and its approvals.
type RunRecords struct{ *ledger.Ledger }

func (r RunRecords) RunApprovals(ctx context.Context, workflowID string) ([]githubtool.Approval, error) {
	rows, err := r.Ledger.RunApprovals(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	out := make([]githubtool.Approval, 0, len(rows))
	for _, a := range rows {
		ga := githubtool.Approval{ID: a.ApprovalID, Tool: a.Tool, Status: a.Status, DecidedBy: a.DecidedBy.String,
			DecidedAt: a.DecidedAt.String, Reason: a.Reason.String, ExpiresAt: a.ExpiresAt}
		if a.Status == approvalApproved {
			// Approved, and refused or lost after: the report must not read as if it went out.
			e, err := r.Ledger.Q.GetEffect(ctx, a.EffectID)
			if err != nil {
				return nil, err
			}
			ga.Write = map[string]string{effectAbandoned: "not sent", effectDenied: "not sent", effectFailed: "failed",
				effectUnknown: "outcome unknown"}[e.Status]
			if ga.Write != "" {
				ga.WriteReason = e.Reason.String
			}
		}
		out = append(out, ga)
	}
	return out, nil
}
