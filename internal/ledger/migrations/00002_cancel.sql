-- Cancellation is recorded by agentd before Temporal is asked: Temporal's cancel is a request
-- the workflow may ignore, agentd's refusal at the gateway is not.

-- +goose Up
ALTER TABLE runs ADD COLUMN cancelled_at TEXT;

-- +goose Down
ALTER TABLE runs DROP COLUMN cancelled_at;
