-- M4 (design doc, section 10): approvals, and the outbox that rings a waiting run's doorbell.
-- An approval gates one effect, which waits in AWAITING_APPROVAL until a person approves it;
-- a decision, or agentd's expiry, is written together with the doorbell that wakes the run.
-- The approvals table of 00001 never held a row: it is made again, with what an approval needs.
-- Times that SQL compares (expires_at, next_attempt_at) are written fixed-width (ledger.Stamp),
-- so that they sort as text.

-- +goose Up
DROP TABLE approvals;
CREATE TABLE approvals (
  approval_id TEXT PRIMARY KEY,
  effect_id TEXT NOT NULL UNIQUE REFERENCES effects (effect_id),
  workflow_id TEXT NOT NULL REFERENCES runs (workflow_id),
  tool TEXT NOT NULL,
  args_json TEXT NOT NULL,              -- the call's canonical arguments, scope filled in: an edit applies to them
  payload_sha256 TEXT NOT NULL,         -- the effect's recorded payload, which a decision must name
  context_blob TEXT NOT NULL,           -- what agentd observed for the reviewer when it made the approval
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'EXPIRED', 'EDITED', 'SUPERSEDED')),
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,             -- fixed-width: the approval is void from then on
  decided_by TEXT,                      -- the reviewer's name, as agentctl gave it; agentd for an expiry
  decided_at TEXT,
  decision_key TEXT,                    -- the decision's idempotency key: sent again, it applies once
  reason TEXT,                          -- a rejection's reason, or why the approval expired
  edited_patch_blob TEXT,               -- EDITED, SUPERSEDED: the reviewer's patch, a blob of the run
  edited_args_sha256 TEXT,              -- EDITED, SUPERSEDED: the arguments of the call that carries the edit
  supersedes TEXT REFERENCES approvals (approval_id),  -- the EDITED approval this one replaced
  superseded_by TEXT,
  used_at TEXT,                         -- APPROVED: when its effect went on to be sent
  CHECK ((status = 'PENDING') = (decided_at IS NULL)),
  CHECK (status <> 'REJECTED' OR reason IS NOT NULL),
  CHECK ((status IN ('EDITED', 'SUPERSEDED')) = (edited_patch_blob IS NOT NULL))
);
CREATE INDEX approvals_pending ON approvals (expires_at) WHERE status = 'PENDING';
CREATE INDEX approvals_by_run ON approvals (workflow_id, tool);
-- Whether an effect may have taken effect: set when the tool says a visible request goes out
-- (proceed); cleared when that is known not to be so: a lookup found nothing, the only attempt that
-- could have made it did nothing, or a person found it never happened. An approval's time, and an
-- approval it never had, matter only while it is clear: a write that may have taken effect is in
-- flight, recovered by its class, or settled by a person. A write of several requests may have
-- made some of them without taking effect (a branch, for a pull request never opened).
ALTER TABLE effects ADD COLUMN maybe_sent INTEGER NOT NULL DEFAULT 0;
-- When agentd last made a visible request of an effect (proceed). Unlike maybe_sent, it stays
-- when the effect is found not to have taken effect: a write of several requests may have left
-- what some of them made (a branch, for a pull request never opened), which its record must not deny.
ALTER TABLE effects ADD COLUMN last_request_at TEXT;
-- An earlier agentd gave back an idempotent effect it could not look up, unsettled: one claimed
-- before (its version moved on) and unclaimed now may have taken effect. Anything else it gave
-- back had been found never to have.
UPDATE effects SET maybe_sent = 1
WHERE status = 'PENDING' AND claimed_by IS NULL AND effect_class = 'idempotent' AND version > 0;

-- Doorbells to deliver (design doc, section 10): each written in the same transaction as the
-- change that should wake the run, and signalled until Temporal has it, or the run has ended.
CREATE TABLE outbox (
  seq INTEGER PRIMARY KEY,              -- in the order written
  workflow_id TEXT NOT NULL REFERENCES runs (workflow_id),
  kind TEXT NOT NULL CHECK (kind = 'doorbell'),
  why TEXT NOT NULL,                    -- approval, reconciliation, budget
  created_at TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT NOT NULL,        -- fixed-width
  delivered_at TEXT,
  outcome TEXT                          -- delivered, run_ended; until then, the last failure
);
CREATE INDEX outbox_due ON outbox (next_attempt_at) WHERE delivered_at IS NULL;

-- +goose Down
ALTER TABLE effects DROP COLUMN last_request_at;
ALTER TABLE effects DROP COLUMN maybe_sent;
DROP TABLE outbox;
DROP TABLE approvals;
CREATE TABLE approvals (
  approval_id TEXT PRIMARY KEY,
  effect_id TEXT NOT NULL,
  payload_sha256 TEXT NOT NULL,
  status TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  decided_by TEXT,
  reason TEXT,
  edited_patch_blob TEXT
);
