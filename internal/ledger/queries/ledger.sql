-- name: InsertRun :exec
INSERT INTO runs (workflow_id, task, params_json, budget_limit_micros, created_at)
VALUES (?, ?, ?, ?, ?);

-- name: SetRunID :execrows
UPDATE runs SET run_id = ? WHERE workflow_id = ? AND run_id IS NULL;

-- name: DeleteRun :exec
DELETE FROM runs WHERE workflow_id = ? AND run_id IS NULL;

-- name: GetRun :one
SELECT * FROM runs WHERE workflow_id = ?;

-- name: ListRuns :many
SELECT * FROM runs ORDER BY created_at DESC, workflow_id LIMIT ?;

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

-- name: GetFinishedModelCall :one
SELECT * FROM model_calls
WHERE workflow_id = ? AND activity_id = ? AND response_blob IS NOT NULL
  AND (stop_reason IS NULL OR stop_reason <> 'invalid_output')
ORDER BY attempt, request_seq
LIMIT 1;

-- name: InsertEffect :execrows
INSERT INTO effects (
  effect_id, workflow_id, tool, effect_class, payload_sha256, idempotency_key, status, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (effect_id) DO NOTHING;

-- name: GetEffect :one
SELECT * FROM effects WHERE effect_id = ?;

-- name: FinishEffect :execrows
UPDATE effects SET status = ?, result_blob = ?, updated_at = ?
WHERE effect_id = ? AND status = 'PENDING';

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

-- name: InsertCall :exec
INSERT INTO calls (workflow_id, activity_id, scheduled_at, kind, name, args_sha256, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING;

-- name: GetCallCounts :one
SELECT
  CAST(COUNT(*) AS INTEGER) AS gateway_calls,
  CAST(COALESCE(SUM(kind = 'model'), 0) AS INTEGER) AS model_calls,
  CAST(COALESCE(SUM(kind = sqlc.arg(kind) AND name = sqlc.arg(name) AND args_sha256 = sqlc.arg(args_sha256)), 0) AS INTEGER) AS same_calls
FROM calls WHERE workflow_id = sqlc.arg(workflow_id);

-- name: AddBlobOwner :exec
INSERT INTO blob_owners (workflow_id, sha256) VALUES (?, ?) ON CONFLICT DO NOTHING;

-- name: IsBlobOwner :one
SELECT EXISTS (SELECT 1 FROM blob_owners WHERE workflow_id = ? AND sha256 = ?);
