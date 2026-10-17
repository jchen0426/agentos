-- Budget (design doc, section 9): every network request that may be billed holds a
-- reservation, closed by a settlement (what was billed) or a release (it never left
-- agentd). Replaces the M1 placeholder, which never held a row.

-- +goose Up
DROP TABLE budget_entries;
CREATE TABLE budget_entries (
  entry_id INTEGER PRIMARY KEY,         -- in the order written
  workflow_id TEXT NOT NULL REFERENCES runs (workflow_id),
  kind TEXT NOT NULL CHECK (kind IN ('RESERVE', 'SETTLE', 'RELEASE', 'EXTEND')),
  amount_micros INTEGER NOT NULL CHECK (amount_micros >= 0),
  request_id TEXT,                      -- RESERVE, SETTLE, RELEASE: one network request
  activity_id TEXT,                     -- RESERVE: the logical call the request serves
  attempt INTEGER,                      -- RESERVE: its activity attempt
  model TEXT,                           -- RESERVE: the model asked; SETTLE: the model that billed it
  price_table_version TEXT,             -- RESERVE, SETTLE
  reason TEXT,                          -- SETTLE: usage, rejected, outcome_unknown, open_at_start; RELEASE: not_sent
  idempotency_key TEXT,                 -- EXTEND
  created_at TEXT NOT NULL,
  CHECK ((kind = 'EXTEND') = (request_id IS NULL))
);
CREATE UNIQUE INDEX budget_entries_reserve ON budget_entries (request_id) WHERE kind = 'RESERVE';
CREATE UNIQUE INDEX budget_entries_close ON budget_entries (request_id) WHERE kind IN ('SETTLE', 'RELEASE');
CREATE UNIQUE INDEX budget_entries_extend ON budget_entries (workflow_id, idempotency_key) WHERE kind = 'EXTEND';
CREATE INDEX budget_entries_run ON budget_entries (workflow_id, kind);

-- +goose Down
DROP TABLE budget_entries;
CREATE TABLE budget_entries (
  entry_id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  amount_micros INTEGER NOT NULL,
  activity_id TEXT,
  created_at TEXT NOT NULL
);
