-- A model call may now take more than one network request within an attempt: the gateway
-- asks again when an answer breaks the task's output schema (design doc, section 9).
-- SQLite cannot change a primary key in place, so the table is rebuilt.

-- +goose Up
CREATE TABLE model_calls_v2 (
  workflow_id TEXT NOT NULL,
  activity_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  request_seq INTEGER NOT NULL,   -- 1; 2 when the first answer broke the output schema
  request_blob TEXT NOT NULL,
  response_blob TEXT,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  effort TEXT,
  stop_reason TEXT,               -- normalized; invalid_output when the answer broke the schema
  usage_json TEXT,                -- what each attempt within the request was billed for, by model
  started_at TEXT NOT NULL,
  finished_at TEXT,
  PRIMARY KEY (workflow_id, activity_id, attempt, request_seq)
);
INSERT INTO model_calls_v2
SELECT workflow_id, activity_id, attempt, 1, request_blob, response_blob, provider, model,
       prompt_version, effort, stop_reason, usage_json, started_at, finished_at
FROM model_calls;
DROP TABLE model_calls;
ALTER TABLE model_calls_v2 RENAME TO model_calls;

-- +goose Down
CREATE TABLE model_calls_v1 (
  workflow_id TEXT NOT NULL,
  activity_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  request_blob TEXT NOT NULL,
  response_blob TEXT,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  effort TEXT,
  stop_reason TEXT,
  usage_json TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  PRIMARY KEY (workflow_id, activity_id, attempt)
);
INSERT INTO model_calls_v1
SELECT workflow_id, activity_id, attempt, request_blob, response_blob, provider, model,
       prompt_version, effort, stop_reason, usage_json, started_at, finished_at
FROM model_calls WHERE request_seq = 1;
DROP TABLE model_calls;
ALTER TABLE model_calls_v1 RENAME TO model_calls;
