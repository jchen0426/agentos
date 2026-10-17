-- agentd's ledgers. Execution history lives in Temporal; these tables hold what Temporal does not.
-- Opened with journal_mode=WAL and synchronous=FULL (see ledger.go).

-- +goose Up
CREATE TABLE runs (               -- only runs started through the Control socket
  workflow_id TEXT PRIMARY KEY,
  run_id TEXT,                    -- the Temporal execution agentd started; NULL while the start is in flight
  task TEXT NOT NULL,
  params_json TEXT NOT NULL,      -- as resolved by agentd (full commit SHA)
  budget_limit_micros INTEGER NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE model_calls (        -- the exact request and response, for "why did the model say that"
  workflow_id TEXT NOT NULL,
  activity_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  request_blob TEXT NOT NULL,
  response_blob TEXT,
  provider TEXT NOT NULL,         -- from models.yaml: anthropic, openai, local (fake in M1)
  model TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  effort TEXT,
  stop_reason TEXT,               -- normalized; raw values stay in response_blob
  usage_json TEXT,                -- input, output, cache read and cache write tokens
  started_at TEXT NOT NULL,
  finished_at TEXT,
  PRIMARY KEY (workflow_id, activity_id, attempt)
);

CREATE TABLE effects (
  effect_id TEXT PRIMARY KEY,     -- hash of workflow id, tool and canonical payload
  workflow_id TEXT NOT NULL,
  tool TEXT NOT NULL,
  effect_class TEXT NOT NULL,     -- none, idempotent, lookup, unsafe
  payload_sha256 TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  status TEXT NOT NULL,           -- AWAITING_APPROVAL, PENDING, SUCCEEDED, FAILED, UNKNOWN
  result_blob TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX effects_by_workflow ON effects (workflow_id);

CREATE TABLE approvals (          -- M4
  approval_id TEXT PRIMARY KEY,
  effect_id TEXT NOT NULL,
  payload_sha256 TEXT NOT NULL,
  status TEXT NOT NULL,           -- PENDING, APPROVED, REJECTED, EXPIRED, SUPERSEDED
  expires_at TEXT NOT NULL,
  decided_by TEXT,
  reason TEXT,
  edited_patch_blob TEXT
);

CREATE TABLE budget_entries (     -- M2
  entry_id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  kind TEXT NOT NULL,             -- RESERVE, SETTLE, RELEASE, EXTEND
  amount_micros INTEGER NOT NULL,
  activity_id TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE audit_log (          -- M5: every gateway decision, hash-chained
  seq INTEGER PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  type TEXT NOT NULL,
  payload TEXT NOT NULL,
  prev_sha256 TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  at TEXT NOT NULL
);

CREATE TABLE blobs (
  sha256 TEXT PRIMARY KEY,
  bytes BLOB NOT NULL
);

-- +goose Down
DROP TABLE blobs;
DROP TABLE audit_log;
DROP TABLE budget_entries;
DROP TABLE approvals;
DROP TABLE effects;
DROP TABLE model_calls;
DROP TABLE runs;
