-- M3 (design doc, section 6): an effect is claimed before its request goes out, recovered by
-- its class after agentd restarts, and reconciled by a person when nobody can tell what
-- happened. Runs may be dry runs. The loop guard learns how each call ended, so that calls
-- that only waited (for an approval, a reconciliation or budget) do not count as repeats.

-- +goose Up
ALTER TABLE effects ADD COLUMN payload_blob TEXT;     -- what the external system receives, as rendered
ALTER TABLE effects ADD COLUMN claimed_by TEXT;       -- the agentd process allowed to send it
ALTER TABLE effects ADD COLUMN claimed_at TEXT;
ALTER TABLE effects ADD COLUMN version INTEGER NOT NULL DEFAULT 0;  -- bumped by every change: compare-and-swap
ALTER TABLE effects ADD COLUMN reason TEXT;           -- why it is FAILED, UNKNOWN or ABANDONED
CREATE INDEX effects_by_status ON effects (status);
CREATE INDEX effects_by_run_tool ON effects (workflow_id, tool);
-- A person's findings about effects nobody could settle, one per idempotency key: the same
-- finding sent again applies once, however the effect has moved on since.
CREATE TABLE effect_resolutions (
  effect_id TEXT NOT NULL REFERENCES effects (effect_id),
  resolve_key TEXT NOT NULL,
  executed INTEGER NOT NULL,          -- the finding: did the write happen?
  status TEXT NOT NULL,               -- what the effect became
  resolved_at TEXT NOT NULL,
  PRIMARY KEY (effect_id, resolve_key)
);
ALTER TABLE runs ADD COLUMN dry_run INTEGER NOT NULL DEFAULT 0;  -- write tools render and record, never send
ALTER TABLE calls ADD COLUMN outcome TEXT;            -- the status the call last returned

-- +goose Down
ALTER TABLE calls DROP COLUMN outcome;
ALTER TABLE runs DROP COLUMN dry_run;
DROP TABLE effect_resolutions;
DROP INDEX effects_by_run_tool;
DROP INDEX effects_by_status;
ALTER TABLE effects DROP COLUMN reason;
ALTER TABLE effects DROP COLUMN version;
ALTER TABLE effects DROP COLUMN claimed_at;
ALTER TABLE effects DROP COLUMN claimed_by;
ALTER TABLE effects DROP COLUMN payload_blob;
