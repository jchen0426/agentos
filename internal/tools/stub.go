// Package tools executes tool calls for the gateway. M1 ships stubs shaped like the first
// task's tools (git.*, github.*) that touch neither git nor GitHub. Every request a stub
// receives is appended to an external log, the stand-in for the outside world: tests read
// it to count how often an external write really happened.
package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// BlobStore keeps large values out of the activity payloads, owned by the run that produced
// them.
type BlobStore interface {
	PutRunBlob(ctx context.Context, workflowID string, b []byte) (string, error)
}

// ErrNotSent marks a tool failure where the request never reached the outside world, so
// nothing can have happened there. Any other error leaves the outcome unknown.
var ErrNotSent = errors.New("request not sent")

// Request is one tool execution.
type Request struct {
	WorkflowID     string
	Tool           string
	Args           map[string]any
	IdempotencyKey string // empty for effect class "none"
}

type Stubs struct {
	Blobs    BlobStore
	External *ExternalLog

	mu     sync.Mutex
	nextPR int
}

const stubDiff = `diff --git a/app/db.py b/app/db.py
--- a/app/db.py
+++ b/app/db.py
@@ -1,2 +1,2 @@
 def find_user(conn, name):
-    return conn.execute("SELECT * FROM users WHERE name = ?", (name,))
+    return conn.execute("SELECT * FROM users WHERE name = '%s'" % name)
`

func (s *Stubs) Call(ctx context.Context, req Request) (map[string]any, error) {
	if err := s.External.Append(req); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotSent, err)
	}
	switch req.Tool {
	case "git.show_diff":
		ref, err := s.Blobs.PutRunBlob(ctx, req.WorkflowID, []byte(stubDiff))
		if err != nil {
			return nil, err
		}
		return map[string]any{"blob": ref}, nil
	case "git.apply_check":
		return map[string]any{"applies": true}, nil
	case "github.open_draft_pr":
		s.mu.Lock()
		s.nextPR++
		n := s.nextPR
		s.mu.Unlock()
		return map[string]any{"url": fmt.Sprintf("stub://github/%v/pull/%d", req.Args["repo"], n)}, nil
	case "github.comment_commit":
		return map[string]any{"comment": "posted"}, nil
	default:
		return nil, fmt.Errorf("%w: no stub for tool %q", ErrNotSent, req.Tool)
	}
}

// ExternalLog records, one JSON line per request, what the stubs were asked to do.
type ExternalLog struct {
	mu   sync.Mutex
	path string
}

func NewExternalLog(path string) *ExternalLog { return &ExternalLog{path: path} }

func (l *ExternalLog) Append(req Request) error {
	line, err := json.Marshal(map[string]any{
		"at":              time.Now().UTC().Format(time.RFC3339Nano),
		"workflow_id":     req.WorkflowID,
		"tool":            req.Tool,
		"idempotency_key": req.IdempotencyKey,
		"args":            req.Args,
	})
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		f.Close()
		return err
	}
	return f.Close() // a lost record would hide an external write
}
