-- name: InsertRun :exec
INSERT INTO runs (workflow_id, task, params_json, budget_limit_micros, dry_run, created_at)
VALUES (?, ?, ?, ?, ?, ?);

-- name: SetRunID :execrows
UPDATE runs SET run_id = ? WHERE workflow_id = ? AND run_id IS NULL;

-- name: DeleteRun :execrows
DELETE FROM runs WHERE workflow_id = ? AND run_id IS NULL;

-- name: GetRun :one
SELECT * FROM runs WHERE workflow_id = ?;

-- name: ListRuns :many
SELECT * FROM runs ORDER BY created_at DESC, workflow_id LIMIT ?;

-- name: ListAllRuns :many
SELECT * FROM runs ORDER BY created_at, workflow_id;

-- name: PutBlob :exec
INSERT INTO blobs (sha256, bytes) VALUES (?, ?) ON CONFLICT (sha256) DO NOTHING;

-- name: GetBlob :one
SELECT bytes FROM blobs WHERE sha256 = ?;

-- name: InsertModelCall :exec
INSERT INTO model_calls (
  workflow_id, activity_id, attempt, request_seq, request_blob, provider, model, prompt_version, effort, started_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: FinishModelCall :exec
UPDATE model_calls
SET response_blob = ?, stop_reason = ?, usage_json = ?, finished_at = ?
WHERE workflow_id = ? AND activity_id = ? AND attempt = ? AND request_seq = ?;

-- name: ListFinishedModelCalls :many
-- The answered requests of one model call, in the order they were sent.
SELECT * FROM model_calls
WHERE workflow_id = ? AND activity_id = ? AND response_blob IS NOT NULL
ORDER BY attempt, request_seq;

-- name: InsertEffect :execrows
-- A new effect: PENDING, or AWAITING_APPROVAL when the policy wants a person to approve it first.
INSERT INTO effects (
  effect_id, workflow_id, tool, effect_class, payload_sha256, payload_blob, idempotency_key, status,
  created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (effect_id) DO NOTHING;

-- name: GetEffect :one
SELECT * FROM effects WHERE effect_id = ?;

-- name: ClaimEffect :execrows
-- Lets one sender send a PENDING effect, unless its run was cancelled: the cancel is checked
-- in the same transaction that lets the request go out.
UPDATE effects SET claimed_by = ?, claimed_at = ?, version = version + 1, updated_at = ?
WHERE effect_id = ? AND version = ? AND status = 'PENDING'
  AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.workflow_id = effects.workflow_id AND r.cancelled_at IS NOT NULL);

-- name: TouchEffect :execrows
-- Marks when a claimed effect's visible request goes out, unless its run was cancelled
-- meanwhile: a lookup's wait for a request in flight counts from here.
UPDATE effects SET claimed_at = ?, last_request_at = ?, maybe_sent = 1, updated_at = ?
WHERE effect_id = ? AND version = ? AND claimed_by = ? AND status = 'PENDING'
  AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.workflow_id = effects.workflow_id AND r.cancelled_at IS NOT NULL);

-- name: CountRunEffects :one
-- A run's effects of one tool that went out or may have, or that the tool refused as it was to
-- send them (DENIED: its checks may have cost requests), other than the one asked about.
SELECT CAST(COUNT(*) AS INTEGER) FROM effects
WHERE workflow_id = ? AND tool = ? AND effect_id <> ? AND status NOT IN ('ABANDONED', 'FAILED');

-- name: ReleaseEffect :execrows
-- Returns a claimed effect for a later attempt to send under its key, as it was: one an attempt
-- may have made (maybe_sent) stays in flight.
UPDATE effects SET claimed_by = NULL, claimed_at = NULL, version = version + 1, updated_at = ?
WHERE effect_id = ? AND version = ? AND status = 'PENDING';

-- name: ReleaseUnsentEffect :execrows
-- Returns a claimed effect that never took effect, as far as agentd knows: a lookup found nothing,
-- or the only attempt that could have made it did nothing.
UPDATE effects SET claimed_by = NULL, claimed_at = NULL, maybe_sent = 0, version = version + 1, updated_at = ?
WHERE effect_id = ? AND version = ? AND status = 'PENDING';

-- name: SetEffectStatus :execrows
UPDATE effects SET status = ?, result_blob = ?, reason = ?, version = version + 1, updated_at = ?
WHERE effect_id = ? AND version = ? AND status = 'PENDING';

-- name: ResolveEffect :execrows
-- A person's finding about an effect whose outcome nobody knew.
UPDATE effects
SET status = ?, result_blob = ?, claimed_by = NULL, claimed_at = NULL, maybe_sent = 0, version = version + 1, updated_at = ?
WHERE effect_id = ? AND version = ? AND status = 'UNKNOWN';

-- name: InsertResolution :exec
INSERT INTO effect_resolutions (effect_id, resolve_key, executed, status, resolved_at) VALUES (?, ?, ?, ?, ?);

-- name: GetResolution :one
SELECT * FROM effect_resolutions WHERE effect_id = ? AND resolve_key = ?;

-- name: ListClaimedEffects :many
-- Effects another agentd process claimed and never finished: one data directory has one
-- agentd at a time, so nobody is sending them any more.
SELECT * FROM effects WHERE status = 'PENDING' AND claimed_by IS NOT NULL AND claimed_by <> ?
ORDER BY created_at;

-- name: AbandonUnsentEffects :execrows
-- Effects of cancelled runs that nobody holds, and that no attempt may have made, never took
-- effect, and never will.
UPDATE effects SET status = 'ABANDONED', reason = 'the run was cancelled before the request was sent',
  version = version + 1, updated_at = ?
WHERE status = 'PENDING' AND claimed_by IS NULL AND maybe_sent = 0
  AND workflow_id IN (SELECT workflow_id FROM runs WHERE cancelled_at IS NOT NULL);

-- name: ListPendingEffectsOfCancelledRuns :many
-- What AbandonUnsentEffects and HandOverUnsentEffects may change, read in their transaction: each
-- change is recorded in the audit log. Those an agentd holds stay as they are, for recovery.
SELECT * FROM effects
WHERE status = 'PENDING' AND workflow_id IN (SELECT workflow_id FROM runs WHERE cancelled_at IS NOT NULL)
ORDER BY created_at, effect_id;

-- name: HandOverUnsentEffects :execrows
-- Effects of cancelled runs that nobody holds, but that an attempt may have made: nothing sends
-- them again, and a person settles them.
UPDATE effects SET status = 'UNKNOWN',
  reason = 'the run was cancelled before the request was sent again; but an earlier attempt may have taken effect',
  version = version + 1, updated_at = ?
WHERE status = 'PENDING' AND claimed_by IS NULL AND maybe_sent = 1
  AND workflow_id IN (SELECT workflow_id FROM runs WHERE cancelled_at IS NOT NULL);

-- name: ListEffectsByStatus :many
SELECT * FROM effects WHERE status = ? ORDER BY created_at;

-- name: GetLatestEffect :one
-- The run's latest finished effect of a tool, sent or dry-run.
SELECT * FROM effects WHERE workflow_id = ? AND tool = ? AND status IN ('SUCCEEDED', 'DRY_RUN')
ORDER BY updated_at DESC LIMIT 1;

-- name: ListStartingRuns :many
SELECT * FROM runs WHERE run_id IS NULL;

-- name: MarkRunCancelled :execrows
UPDATE runs SET cancelled_at = ? WHERE workflow_id = ? AND cancelled_at IS NULL;

-- name: ListCancelledRuns :many
SELECT * FROM runs WHERE cancelled_at IS NOT NULL AND run_id IS NOT NULL;

-- name: InsertBudgetEntry :exec
INSERT INTO budget_entries (
  workflow_id, kind, amount_micros, request_id, activity_id, attempt, model, price_table_version, reason, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: InsertBudgetExtension :execrows
INSERT INTO budget_entries (workflow_id, kind, amount_micros, idempotency_key, created_at)
VALUES (?, 'EXTEND', ?, ?, ?)
ON CONFLICT DO NOTHING;

-- name: BudgetExtended :one
-- Whether the extension of that key is there: what an insert that added none must find.
SELECT EXISTS (SELECT 1 FROM budget_entries WHERE workflow_id = ? AND kind = 'EXTEND' AND idempotency_key = ?);

-- name: GetBudgetTotals :one
SELECT
  CAST(r.budget_limit_micros + COALESCE((
    SELECT SUM(e.amount_micros) FROM budget_entries e WHERE e.workflow_id = r.workflow_id AND e.kind = 'EXTEND'
  ), 0) AS INTEGER) AS limit_micros,
  CAST(COALESCE((
    SELECT SUM(e.amount_micros) FROM budget_entries e WHERE e.workflow_id = r.workflow_id AND e.kind = 'SETTLE'
  ), 0) AS INTEGER) AS spent_micros,
  CAST(COALESCE((
    SELECT SUM(e.amount_micros) FROM budget_entries e
    WHERE e.workflow_id = r.workflow_id AND e.kind = 'RESERVE' AND NOT EXISTS (
      SELECT 1 FROM budget_entries c WHERE c.request_id = e.request_id AND c.kind IN ('SETTLE', 'RELEASE'))
  ), 0) AS INTEGER) AS held_micros
FROM runs r WHERE r.workflow_id = ?;

-- name: GetOpenReservation :one
SELECT * FROM budget_entries e
WHERE e.request_id = ? AND e.kind = 'RESERVE' AND NOT EXISTS (
  SELECT 1 FROM budget_entries c WHERE c.request_id = e.request_id AND c.kind IN ('SETTLE', 'RELEASE'));

-- name: ListOpenReservations :many
SELECT * FROM budget_entries e
WHERE e.kind = 'RESERVE' AND NOT EXISTS (
  SELECT 1 FROM budget_entries c WHERE c.request_id = e.request_id AND c.kind IN ('SETTLE', 'RELEASE'))
ORDER BY e.entry_id;

-- name: InsertCall :execrows
-- One row per logical call: a retry finds it there and inserts nothing.
INSERT INTO calls (workflow_id, activity_id, scheduled_at, kind, name, args_sha256, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING;

-- name: CallCounted :one
-- Whether InsertCall's row is there: what an insert that added none must find.
SELECT EXISTS (SELECT 1 FROM calls WHERE workflow_id = ? AND activity_id = ? AND scheduled_at = ?);

-- name: ListSameCalls :many
-- Every call of a run with the same kind, name and arguments: a call that waited is sent
-- again under a new activity.
SELECT * FROM calls WHERE workflow_id = ? AND kind = ? AND name = ? AND args_sha256 = ?;

-- name: SetCallOutcome :exec
UPDATE calls SET outcome = ? WHERE workflow_id = ? AND activity_id = ? AND scheduled_at = ?;

-- name: GetCallCounts :one
-- A call that returned a wait status asked again after the wait: it counts as a step, but
-- neither as a model call nor as a repeat.
SELECT
  CAST(COUNT(*) AS INTEGER) AS gateway_calls,
  CAST(COALESCE(SUM(kind = 'model'
    AND COALESCE(outcome NOT IN ('needs_approval', 'needs_reconcile', 'budget_exhausted'), 1)), 0) AS INTEGER) AS model_calls,
  CAST(COALESCE(SUM(kind = sqlc.arg(kind) AND name = sqlc.arg(name) AND args_sha256 = sqlc.arg(args_sha256)
    AND COALESCE(outcome NOT IN ('needs_approval', 'needs_reconcile', 'budget_exhausted'), 1)), 0) AS INTEGER) AS same_calls
FROM calls WHERE workflow_id = sqlc.arg(workflow_id);

-- name: AddBlobOwner :execrows
INSERT INTO blob_owners (workflow_id, sha256) VALUES (?, ?) ON CONFLICT DO NOTHING;

-- name: IsBlobOwner :one
SELECT EXISTS (SELECT 1 FROM blob_owners WHERE workflow_id = ? AND sha256 = ?);

-- name: ListModelAnswers :many
-- The answered requests of a run's calls of one model task: the run's model answers.
SELECT m.* FROM model_calls m
WHERE m.workflow_id = sqlc.arg(workflow_id) AND m.response_blob IS NOT NULL AND m.activity_id IN (
  SELECT c.activity_id FROM calls c WHERE c.workflow_id = sqlc.arg(workflow_id) AND c.kind = 'model' AND c.name = sqlc.arg(task));

-- name: ApproveEffect :execrows
-- An approved effect goes on to be sent, unless its run was cancelled: from here on it is an
-- effect like any other (claimed, sent, recovered by its class).
UPDATE effects SET status = 'PENDING', version = version + 1, updated_at = ?
WHERE effect_id = ? AND version = ? AND status = 'AWAITING_APPROVAL'
  AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.workflow_id = effects.workflow_id AND r.cancelled_at IS NOT NULL);

-- name: HoldForApproval :execrows
-- A PENDING effect nobody claimed, recorded before the policy asked for approval: it waits for
-- a person now, unless its run was cancelled.
UPDATE effects SET status = 'AWAITING_APPROVAL', version = version + 1, updated_at = ?
WHERE effect_id = ? AND version = ? AND status = 'PENDING' AND claimed_by IS NULL AND maybe_sent = 0
  AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.workflow_id = effects.workflow_id AND r.cancelled_at IS NOT NULL);

-- name: AbandonUnclaimedEffect :execrows
-- A PENDING effect that never took effect is never sent: its approval's time ran out first.
UPDATE effects SET status = 'ABANDONED', reason = ?, version = version + 1, updated_at = ?
WHERE effect_id = ? AND version = ? AND status = 'PENDING' AND claimed_by IS NULL AND maybe_sent = 0;

-- name: AbandonAwaitingEffect :execrows
-- An effect whose approval was rejected, expired or replaced is never sent.
UPDATE effects SET status = 'ABANDONED', reason = ?, version = version + 1, updated_at = ?
WHERE effect_id = ? AND status = 'AWAITING_APPROVAL';

-- name: InsertApproval :exec
INSERT INTO approvals (
  approval_id, effect_id, workflow_id, tool, args_json, payload_sha256, context_blob, status,
  created_at, expires_at, supersedes
) VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?);

-- name: GetApproval :one
SELECT * FROM approvals WHERE approval_id = ?;

-- name: GetApprovalByEffect :one
SELECT * FROM approvals WHERE effect_id = ?;

-- name: GetEditedApproval :one
-- The approval whose edit a call carries: the reviewer's patch in the arguments of the call
-- that was edited.
SELECT * FROM approvals
WHERE workflow_id = ? AND tool = ? AND status = 'EDITED' AND edited_args_sha256 = ?;

-- name: ListPendingApprovals :many
-- What waits for a decision, but for the approvals of cancelled runs: nothing they approve goes out.
SELECT a.* FROM approvals a JOIN runs r ON r.workflow_id = a.workflow_id
WHERE a.status = 'PENDING' AND r.cancelled_at IS NULL
ORDER BY a.created_at, a.approval_id;

-- name: ListRunApprovals :many
SELECT * FROM approvals WHERE workflow_id = ? ORDER BY created_at, approval_id;

-- name: DecideApproval :execrows
-- A person's decision, if the approval still waits for one: the first valid decision wins.
UPDATE approvals
SET status = ?, decided_by = ?, decided_at = ?, decision_key = ?, reason = ?, edited_patch_blob = ?,
  edited_args_sha256 = ?
WHERE approval_id = ? AND status = 'PENDING';

-- name: ExpirePendingApproval :execrows
-- agentd's decision when an approval's time is up and nobody decided: a rejection.
UPDATE approvals SET status = 'EXPIRED', decided_by = 'agentd', decided_at = ?, reason = ?
WHERE approval_id = ? AND status = 'PENDING';

-- name: ExpireUnusedApproval :execrows
-- An approval the run did not act on before its time was up: void as well.
UPDATE approvals SET status = 'EXPIRED', decided_by = 'agentd', decided_at = ?, reason = ?
WHERE approval_id = ? AND status = 'APPROVED' AND used_at IS NULL;

-- name: ExpireUsedApproval :execrows
-- An approval whose write had not taken effect when its time was up: void too, as nothing is in
-- flight.
UPDATE approvals SET status = 'EXPIRED', decided_by = 'agentd', decided_at = ?, reason = ?
WHERE approval_id = ? AND status = 'APPROVED' AND used_at IS NOT NULL;

-- name: UseApproval :execrows
UPDATE approvals SET used_at = ? WHERE approval_id = ? AND status = 'APPROVED' AND used_at IS NULL;

-- name: SupersedeApproval :execrows
UPDATE approvals SET status = 'SUPERSEDED', superseded_by = ? WHERE approval_id = ? AND status = 'EDITED';

-- name: ListExpiredApprovals :many
SELECT * FROM approvals WHERE status = 'PENDING' AND expires_at <= ? ORDER BY expires_at;

-- name: NextApprovalExpiry :one
SELECT CAST(COALESCE(MIN(expires_at), '') AS TEXT) FROM approvals WHERE status = 'PENDING';

-- name: InsertDoorbell :exec
INSERT INTO outbox (workflow_id, kind, why, created_at, next_attempt_at) VALUES (?, 'doorbell', ?, ?, ?);

-- name: ListDueDoorbells :many
SELECT * FROM outbox WHERE delivered_at IS NULL AND next_attempt_at <= ? ORDER BY seq LIMIT 200;

-- name: NextDoorbellDue :one
SELECT CAST(COALESCE(MIN(next_attempt_at), '') AS TEXT) FROM outbox WHERE delivered_at IS NULL;

-- name: MarkDoorbellsDelivered :execrows
-- One signal wakes the run for every doorbell rung for it until then.
UPDATE outbox SET delivered_at = ?, outcome = ?
WHERE workflow_id = ? AND seq <= ? AND delivered_at IS NULL;

-- name: RetryDoorbells :execrows
UPDATE outbox SET attempts = attempts + 1, next_attempt_at = ?, outcome = ?
WHERE workflow_id = ? AND seq <= ? AND delivered_at IS NULL;

-- name: LastAuditRecord :one
SELECT * FROM audit_log ORDER BY seq DESC LIMIT 1;

-- name: InsertAuditRecord :exec
INSERT INTO audit_log (seq, at, workflow_id, type, actor, peer_uid, payload, prev_sha256, sha256)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: ListAuditRecords :many
-- A page of the chain, from seq on.
SELECT * FROM audit_log WHERE seq >= ? ORDER BY seq LIMIT ?;

-- name: ListRunAuditRecords :many
SELECT * FROM audit_log WHERE workflow_id = ? ORDER BY seq;

-- name: ListRunLifeRecords :many
-- Each run's creation, start and discard, as the audit log has them: what the runs table must agree with.
SELECT * FROM audit_log WHERE type IN ('run.created', 'run.started', 'run.discarded') ORDER BY seq;

-- name: ListRunLastRecords :many
-- When each run's execution was last seen live: its last call's record (only a running execution
-- calls), or its start's. Temporal removes a history its retention after the execution closed, and
-- so after this; records appended after the close (an expiry, a reconciliation) say nothing of it.
-- records counts all of each run's records.
SELECT workflow_id,
  CAST(COALESCE(MAX(CASE WHEN (type = 'call' AND actor = 'run') OR type IN ('run.created', 'run.started') THEN at END), '')
    AS TEXT) AS at,
  COUNT(*) AS records
FROM audit_log WHERE workflow_id <> '' GROUP BY workflow_id;

-- name: ListBlobsAfter :many
-- The blobs in the order of their keys, a page at a time: a verification hashes each one's bytes.
SELECT sha256, bytes FROM blobs WHERE sha256 > ? ORDER BY sha256 LIMIT ?;

-- name: ListMissingBlobs :many
-- What effects and approvals refer to that the ledger has no blob of.
SELECT CAST('effect' AS TEXT) AS kind, effect_id AS id, CAST(payload_blob AS TEXT) AS ref FROM effects
WHERE payload_blob IS NOT NULL AND NOT EXISTS (SELECT 1 FROM blobs WHERE blobs.sha256 = substr(effects.payload_blob, 8))
UNION ALL
SELECT CAST('effect' AS TEXT), effect_id, CAST(result_blob AS TEXT) FROM effects
WHERE result_blob IS NOT NULL AND NOT EXISTS (SELECT 1 FROM blobs WHERE blobs.sha256 = substr(effects.result_blob, 8))
UNION ALL
SELECT CAST('approval' AS TEXT), approval_id, CAST(context_blob AS TEXT) FROM approvals
WHERE NOT EXISTS (SELECT 1 FROM blobs WHERE blobs.sha256 = substr(approvals.context_blob, 8))
UNION ALL
SELECT CAST('approval' AS TEXT), approval_id, CAST(edited_patch_blob AS TEXT) FROM approvals
WHERE edited_patch_blob IS NOT NULL AND NOT EXISTS (SELECT 1 FROM blobs WHERE blobs.sha256 = substr(approvals.edited_patch_blob, 8));

-- name: ListEffectsCreated :many
SELECT effect_id, workflow_id, created_at FROM effects ORDER BY effect_id;

-- name: ListApprovalsCreated :many
SELECT approval_id, workflow_id, created_at FROM approvals ORDER BY approval_id;

-- name: ListBudgetExtensions :many
SELECT workflow_id, CAST(COALESCE(idempotency_key, '') AS TEXT) AS idempotency_key, amount_micros, created_at
FROM budget_entries WHERE kind = 'EXTEND' ORDER BY entry_id;

-- name: ListCallsAll :many
-- Every logical call, for a verification to recount against the records.
SELECT workflow_id, activity_id, scheduled_at, kind, name, args_sha256, created_at, outcome FROM calls
ORDER BY workflow_id, activity_id, scheduled_at;

-- name: ListBudgetEntriesAll :many
-- Every budget entry, with the activity and attempt of the reservation it closes, for a
-- verification to compare with the records.
SELECT e.entry_id, e.workflow_id, e.kind, e.amount_micros, CAST(COALESCE(e.request_id, '') AS TEXT) AS request_id,
  CAST(COALESCE(e.model, '') AS TEXT) AS model, CAST(COALESCE(e.price_table_version, '') AS TEXT) AS price_table_version,
  CAST(COALESCE(e.reason, '') AS TEXT) AS reason, CAST(COALESCE(e.idempotency_key, '') AS TEXT) AS idempotency_key, e.created_at,
  CAST(COALESCE(e.activity_id, '') AS TEXT) AS activity_id, CAST(COALESCE(e.attempt, 0) AS INTEGER) AS attempt
FROM budget_entries e ORDER BY e.entry_id;

-- name: ListModelCallsAll :many
SELECT workflow_id, activity_id, attempt, request_seq, request_blob, CAST(COALESCE(response_blob, '') AS TEXT) AS response_blob,
  provider, model, prompt_version, CAST(COALESCE(effort, '') AS TEXT) AS effort, CAST(COALESCE(stop_reason, '') AS TEXT) AS stop_reason,
  CAST(COALESCE(usage_json, '') AS TEXT) AS usage_json, started_at
FROM model_calls ORDER BY workflow_id, activity_id, attempt, request_seq;

-- name: ListBlobOwnersAll :many
SELECT workflow_id, sha256 FROM blob_owners ORDER BY workflow_id, sha256;

-- name: ListResolutionsAll :many
SELECT r.effect_id, r.resolve_key, r.executed, r.status, r.resolved_at, COALESCE(e.workflow_id, '') AS workflow_id
FROM effect_resolutions r LEFT JOIN effects e ON e.effect_id = r.effect_id ORDER BY r.effect_id, r.resolve_key;
