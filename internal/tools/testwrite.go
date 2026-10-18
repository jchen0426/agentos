package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jchen0426/agentos/internal/policy"
)

// TestWrite is test.unsafe_write, registered only when agentd runs with --test-tools: a write
// to a local log that stands in for an external system which neither dedupes requests nor
// can be asked afterwards what happened (T3).
type TestWrite struct{ Log *ExternalLog }

var testWriteArgs = MustSchema(`{
  "type": "object", "additionalProperties": false, "required": ["repo", "note"],
  "properties": {"repo": {"type": "string"}, "note": {"type": "string", "maxLength": 200}}
}`)

var testWriteResult = MustSchema(`{
  "type": "object", "additionalProperties": false, "required": ["written"],
  "properties": {"written": {"type": "boolean"}}
}`)

func (*TestWrite) Name() string    { return "test.unsafe_write" }
func (*TestWrite) Class() string   { return policy.EffectUnsafe }
func (*TestWrite) Args() *Schema   { return testWriteArgs }
func (*TestWrite) Result() *Schema { return testWriteResult }
func (*TestWrite) Live() bool      { return true }

type testWritePayload struct {
	WorkflowID string `json:"workflow_id"`
	Note       string `json:"note"`
}

func (*TestWrite) Prepare(_ context.Context, req Request) (json.RawMessage, error) {
	return json.Marshal(testWritePayload{WorkflowID: req.WorkflowID, Note: String(req.Args, "note")})
}

func (t *TestWrite) Execute(ctx context.Context, payload json.RawMessage, key string, proceed Proceed) (map[string]any, error) {
	var p testWritePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotSent, err)
	}
	if err := proceed(ctx); err != nil {
		return nil, err
	}
	if err := t.Log.Append(map[string]any{
		"workflow_id": p.WorkflowID, "tool": t.Name(), "note": p.Note, "idempotency_key": key,
	}); err != nil {
		return nil, err // the line may or may not be in the log
	}
	return map[string]any{"written": true}, nil
}

func (*TestWrite) DryRun(json.RawMessage) map[string]any {
	return map[string]any{"written": false, "dry_run": true}
}

// ExternalLog is a stand-in for the outside world: one JSON line per request it received.
// Tests read it to count how often an external write really happened.
type ExternalLog struct {
	mu   sync.Mutex
	path string
}

func NewExternalLog(path string) *ExternalLog { return &ExternalLog{path: path} }

func (l *ExternalLog) Append(entry map[string]any) error {
	entry["at"] = time.Now().UTC().Format(time.RFC3339Nano)
	line, err := json.Marshal(entry)
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
