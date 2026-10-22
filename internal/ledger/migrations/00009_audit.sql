-- M5 (design doc, section 8): agentd's audit log. Every policy decision, side effect and approval
-- is a record, appended in the transaction of the change it records, with who made it. Each record
-- holds the sha256 of the one before it, and its own over all its fields (internal/audit), so that
-- a record changed, removed or put out of order breaks the chain; and the results Invoke returns
-- carry the head of the chain into Temporal's history, which a rewrite of the ledger cannot reach.
-- The audit_log of 00001 never held a row: it is made again, with the actor.

-- +goose Up
DROP TABLE audit_log;
CREATE TABLE audit_log (
  seq INTEGER PRIMARY KEY CHECK (seq > 0),  -- 1, 2, 3, ...: one after the last, in the appending transaction
  at TEXT NOT NULL,                         -- fixed-width (ledger.Stamp)
  workflow_id TEXT NOT NULL,                -- the run's; '' for agentd's own events
  type TEXT NOT NULL,                       -- call, effect.claimed, approval.decided, ...
  actor TEXT NOT NULL,                      -- run, agentd, or operator:<name>
  peer_uid INTEGER,                         -- an operator's: the uid the Control socket's peer credentials gave
  payload TEXT NOT NULL,                    -- a JSON object, as hashed
  prev_sha256 TEXT NOT NULL,                -- the record before it's sha256; 64 zeros for the first
  sha256 TEXT NOT NULL UNIQUE
);
CREATE INDEX audit_log_by_run ON audit_log (workflow_id, seq);
