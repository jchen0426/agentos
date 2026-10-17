-- Loop protection (design doc, section 9): every logical gateway call a run makes, once, so
-- that agentd can cap model calls, all calls, and repeats of one tool with the same arguments.

-- +goose Up
CREATE TABLE calls (
  workflow_id TEXT NOT NULL REFERENCES runs (workflow_id),
  activity_id TEXT NOT NULL,
  -- When Temporal first scheduled the activity: the same for every retry, and set by the
  -- server, not the Worker. With the activity ID it names one logical call, so a Worker that
  -- reuses an activity ID for a new call still has it counted.
  scheduled_at TEXT NOT NULL,
  kind TEXT NOT NULL,             -- model or tool
  name TEXT NOT NULL,
  args_sha256 TEXT NOT NULL,      -- of the canonical arguments
  created_at TEXT NOT NULL,
  PRIMARY KEY (workflow_id, activity_id, scheduled_at)
);

-- +goose Down
DROP TABLE calls;
