package models

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/jchen0426/agentos/internal/pricing"
)

// Fake answers model tasks with fixed, schema-shaped output and fixed token counts, priced
// by the price table like any model. A fault script can make it answer 429 or 5xx, lose an
// answer it billed, never receive a request, or answer as a fallback model. Like a real
// provider it keeps its own record of every request it received, independent of agentd's
// ledger, so that tests can hold the ledger against it.
type Fake struct {
	Blobs BlobStore
	Log   *ProviderLog
}

// FakeFault is what a fault script asks of the fake provider for one request.
type FakeFault struct {
	Status      int    // answer with this HTTP status instead: 429 or 5xx, not billed
	LoseAnswer  bool   // bill the request, then lose the answer, like a timeout
	Unreachable bool   // fail before the request leaves agentd
	ServedBy    string // answer as this "<provider>/<model>", like a fallback
}

// fakeOutputTokens is what every fake answer is billed for on the output side.
const fakeOutputTokens = 500

// fakeInputTokens is the fake's token count: a fixed prompt plus the arguments.
func fakeInputTokens(req Request) int64 {
	b, _ := json.Marshal(req.Args)
	return 1200 + int64(len(b))
}

func (f Fake) CountTokens(_ context.Context, req Request) (int64, error) {
	return fakeInputTokens(req), nil
}

// ProviderLog is the fake provider's own record, one JSON line per request it received.
type ProviderLog struct {
	mu   sync.Mutex
	path string
}

func NewProviderLog(path string) *ProviderLog { return &ProviderLog{path: path} }

func (l *ProviderLog) record(req Request, model string, status int, billed pricing.Usage) error {
	if l == nil {
		return nil
	}
	line, err := json.Marshal(map[string]any{
		"at": time.Now().UTC().Format(time.RFC3339Nano), "workflow_id": req.WorkflowID,
		"request_id": req.RequestID, "task": req.Route.Task, "model": model, "status": status, "usage": billed,
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
	return f.Close()
}

const fakePatch = `diff --git a/app/db.py b/app/db.py
--- a/app/db.py
+++ b/app/db.py
@@ -1,2 +1,2 @@
 def find_user(conn, name):
-    return conn.execute("SELECT * FROM users WHERE name = '%s'" % name)
+    return conn.execute("SELECT * FROM users WHERE name = ?", (name,))
`

func (f Fake) Call(ctx context.Context, req Request) (Response, error) {
	if req.Route.Task != "audit_v1" {
		return Response{}, fmt.Errorf("fake adapter has no fixture for task %q: %w", req.Route.Task, ErrNotSent)
	}
	if req.Fault.Unreachable {
		return Response{}, fmt.Errorf("fake provider unreachable: %w", ErrNotSent)
	}
	model := req.Route.Name()
	if s := req.Fault.ServedBy; s != "" {
		if !slices.Contains(req.Route.Candidates, s) {
			return Response{}, fmt.Errorf("fake provider cannot answer as %q, not a fallback of %s: %w", s, model, ErrNotSent)
		}
		model = s
	}
	usage := pricing.Usage{InputTokens: fakeInputTokens(req), OutputTokens: fakeOutputTokens}
	if req.Fault.Status != 0 {
		if err := f.Log.record(req, model, req.Fault.Status, pricing.Usage{}); err != nil {
			return Response{}, fmt.Errorf("%w: %v", ErrNotSent, err)
		}
		return Response{}, &RejectedError{StatusCode: req.Fault.Status, Message: "fault script"}
	}
	// The provider bills a request it served, whether or not its answer gets back.
	if err := f.Log.record(req, model, 200, usage); err != nil {
		return Response{}, fmt.Errorf("%w: %v", ErrNotSent, err)
	}
	if req.Fault.LoseAnswer {
		return Response{}, fmt.Errorf("fake provider: answer lost: %w", context.DeadlineExceeded)
	}
	findings := []map[string]any{{
		"severity": "high",
		"cwe":      "CWE-89",
		"file":     "app/db.py",
		"line":     2,
		"title":    "SQL built with string formatting",
	}}
	report, err := json.Marshal(map[string]any{"diff_ref": req.Args["diff_ref"], "findings": findings})
	if err != nil {
		return Response{}, err
	}
	reportRef, err := f.Blobs.PutBlob(ctx, report)
	if err != nil {
		return Response{}, err
	}
	patchRef, err := f.Blobs.PutBlob(ctx, []byte(fakePatch))
	if err != nil {
		return Response{}, err
	}
	return Response{
		Status:     "ok",
		Model:      model,
		Value:      map[string]any{"report_ref": reportRef, "patch_ref": patchRef, "findings": findings},
		StopReason: "end_turn",
		Usage:      usage,
	}, nil
}
