package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// PutRunBlob stores b as a blob the run produced, which only that run may have read into a
// model's prompt (ledger.GetRunBlob). The run's ownership of it is a row of blob_owners, and that
// row has its record, blob.owned, in the same transaction: what the run may read is in the log.
func PutRunBlob(ctx context.Context, l *ledger.Ledger, workflowID string, b []byte) (string, error) {
	sum := sha256.Sum256(b)
	h := hex.EncodeToString(sum[:])
	if err := l.Tx(ctx, func(q *db.Queries) error {
		if err := q.PutBlob(ctx, db.PutBlobParams{Sha256: h, Bytes: b}); err != nil {
			return err
		}
		n, err := q.AddBlobOwner(ctx, db.AddBlobOwnerParams{WorkflowID: workflowID, Sha256: h})
		if err != nil {
			return err
		}
		if n == 0 { // owned already, and recorded then: the row is there, or the write was dropped
			return OwnerThere(ctx, q, workflowID, h)
		}
		_, err = Append(ctx, q, Event{WorkflowID: workflowID, Type: "blob.owned", Actor: Agentd, Payload: map[string]any{"sha256": h}})
		return err
	}); err != nil {
		return "", err
	}
	return "sha256:" + h, nil
}

// OwnerThere is what an AddBlobOwner that added no row must find: the run owns the blob already.
// Otherwise the write was dropped (ledger.ErrWriteDropped).
func OwnerThere(ctx context.Context, q *db.Queries, workflowID, sha256 string) error {
	owns, err := q.IsBlobOwner(ctx, db.IsBlobOwnerParams{WorkflowID: workflowID, Sha256: sha256})
	if err == nil && owns == 0 {
		err = fmt.Errorf("%w: run %s as the owner of blob %s", ledger.ErrWriteDropped, workflowID, sha256)
	}
	return err
}

// RunBlobs is the ledger as the tools store and read their runs' blobs (tools.BlobStore), each one
// stored with its record.
type RunBlobs struct{ *ledger.Ledger }

func (r RunBlobs) PutRunBlob(ctx context.Context, workflowID string, b []byte) (string, error) {
	return PutRunBlob(ctx, r.Ledger, workflowID, b)
}
