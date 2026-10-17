-- Which run produced each blob. A model call may only read blobs its own run produced: blobs
-- are content-addressed and shared, and a Worker that learned another run's blob reference
-- must not be able to have it sent to a model under its own run.

-- +goose Up
CREATE TABLE blob_owners (
  workflow_id TEXT NOT NULL REFERENCES runs (workflow_id),
  sha256 TEXT NOT NULL REFERENCES blobs (sha256),
  PRIMARY KEY (workflow_id, sha256)
);

-- +goose Down
DROP TABLE blob_owners;
