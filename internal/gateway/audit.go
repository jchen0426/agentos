package gateway

import (
	"context"
	"database/sql"
	"strings"

	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/tools"
)

// The gateway's records in the audit log (design doc, section 8). Each is appended through q: the
// transaction of the change it records, so that neither commits without the other. A run's
// call and what it asks for are the run's; a person's decisions and findings are the operator's;
// the steps agentd takes of itself (claims, requests, settlements, expiries) are agentd's.

// note appends one record of the run's through q.
func note(ctx context.Context, q *db.Queries, workflowID, typ, actor string, payload map[string]any) error {
	_, err := audit.Append(ctx, q, audit.Event{WorkflowID: workflowID, Type: typ, Actor: actor, Payload: payload})
	return err
}

// noteSettled is effect.settled: agentd recorded how an effect ended, or that it waits for a person
// (UNKNOWN); with the sha256 of its result, when it has one.
func noteSettled(ctx context.Context, q *db.Queries, workflowID, effectID, status string, reason, result sql.NullString) error {
	p := map[string]any{"effect_id": effectID, "status": status}
	if reason.Valid {
		p["reason"] = audit.Text(reason.String)
	}
	if result.Valid {
		p["result_sha256"] = strings.TrimPrefix(result.String, "sha256:")
	}
	return note(ctx, q, workflowID, "effect.settled", audit.Agentd, p)
}

// noteApproval is approval.created: a write waits for a person's approval, made in the transaction
// that records the write (by the run's call) or holds it back for one (by agentd, gate).
func noteApproval(ctx context.Context, q *db.Queries, a db.InsertApprovalParams, actor string) error {
	p := map[string]any{"approval_id": a.ApprovalID, "effect_id": a.EffectID, "tool": a.Tool,
		"payload_sha256": a.PayloadSha256, "expires_at": a.ExpiresAt, "context_sha256": strings.TrimPrefix(a.ContextBlob, "sha256:")}
	if a.Supersedes.Valid {
		p["supersedes"] = a.Supersedes.String
	}
	return note(ctx, q, a.WorkflowID, "approval.created", actor, p)
}

// reading is ctx for one of agentd's own reads of an effect's target (tools.Reads): a lookup of the
// effect, as agentd recovers it ("lookup") or a person reconciles it ("reconcile"), or the requests
// of its approval card ("approval_card"). Each request goes out after its agentd.read record, which
// names why agentd reads, the effect, the tool, and the request's method and path; what the read
// found is in the records that follow.
func (g *Gateway) reading(ctx context.Context, workflowID, effectID, tool, purpose string) context.Context {
	return tools.WithReads(ctx, func(ctx context.Context, method, path string) error {
		return g.Ledger.Tx(ctx, func(q *db.Queries) error {
			return note(ctx, q, workflowID, "agentd.read", audit.Agentd, map[string]any{"purpose": purpose,
				"effect_id": effectID, "tool": tool, "method": audit.Text(method), "path": audit.Text(path)})
		})
	})
}

// noteExpired is approval.expired: agentd voided an approval, as a rejection.
func noteExpired(ctx context.Context, q *db.Queries, a db.Approval, why string) error {
	return note(ctx, q, a.WorkflowID, "approval.expired", audit.Agentd, map[string]any{"approval_id": a.ApprovalID,
		"why": audit.Text(why)})
}
