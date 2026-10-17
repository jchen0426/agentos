package models

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Fake answers model tasks with fixed, schema-shaped output. It costs nothing and never
// refuses; fault scripts add holds and delays around it. Like a real provider it keeps its
// own record of every request it answered, independent of agentd's ledger.
type Fake struct {
	Blobs BlobStore
	Log   *ProviderLog
}

// ProviderLog is the fake provider's own record, one JSON line per request.
type ProviderLog struct {
	mu   sync.Mutex
	path string
}

func NewProviderLog(path string) *ProviderLog { return &ProviderLog{path: path} }

func (l *ProviderLog) record(req Request) error {
	line, err := json.Marshal(map[string]any{
		"at": time.Now().UTC().Format(time.RFC3339Nano), "workflow_id": req.WorkflowID,
		"task": req.Route.Task, "model": req.Route.Model,
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
		return Response{}, fmt.Errorf("fake adapter has no fixture for task %q", req.Route.Task)
	}
	if f.Log != nil {
		if err := f.Log.record(req); err != nil {
			return Response{}, err
		}
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
		Value:      map[string]any{"report_ref": reportRef, "patch_ref": patchRef, "findings": findings},
		StopReason: "end_turn",
		Usage:      map[string]int64{"input_tokens": 0, "output_tokens": 0},
	}, nil
}
