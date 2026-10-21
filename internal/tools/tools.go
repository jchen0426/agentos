// Package tools holds the tools the gateway runs for workflows. Every tool declares its effect
// class (design doc, section 6) and the schema of its arguments. A tool with an external effect
// splits each call in two: Prepare renders exactly what the outside world will receive, and
// Execute sends it. The gateway records the payload, claims it, and after a crash recovers it
// by the tool's class.
package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Request is one call of a tool.
type Request struct {
	WorkflowID   string
	RunCreatedAt time.Time      // when the run was created: the same on every attempt
	EffectID     string         // writes only: names the effect, e.g. in a marker a lookup finds
	Args         map[string]any // after the gateway filled in the run's scope and the schema accepted them
}

// Tool is what every tool declares.
type Tool interface {
	Name() string
	// Class is the tool's effect class: policy.EffectNone for a Reader; for a Writer
	// EffectIdempotent, EffectLookup or EffectUnsafe.
	Class() string
	Args() *Schema
}

// Reader is a tool without external effects: a retry simply reads again.
type Reader interface {
	Tool
	Read(ctx context.Context, req Request) (map[string]any, error)
}

// Writer is a tool with an external effect.
type Writer interface {
	Tool
	// Prepare renders what the external system will receive. It is deterministic: the same
	// request renders the same payload on every attempt.
	Prepare(ctx context.Context, req Request) (json.RawMessage, error)
	// Execute sends the payload, calling proceed before each request whose effect someone
	// could see; when proceed fails, Execute stops and returns its error, unless a request it
	// sent already may have made the write: then it returns an error that says so (a
	// *ConflictError, as github.open_draft_pr does once it pushed its branch). Its error tells
	// what happened outside: ErrNotSent, a *RefusedError and a *DeniedError mean nothing did (a
	// *DeniedError, before anything was sent, refuses the payload for good); any other error
	// leaves the outcome unknown.
	Execute(ctx context.Context, payload json.RawMessage, idempotencyKey string, proceed Proceed) (map[string]any, error)
	// DryRun is what a dry run returns to the workflow instead of Execute's result.
	DryRun(payload json.RawMessage) map[string]any
	// Result checks a result a person reports when reconciling.
	Result() *Schema
	// Live reports whether the tool can reach its external system. One that cannot only
	// dry-runs.
	Live() bool
}

// Describer is a write whose reviewer needs to see more than its payload: what agentd can find
// out about the write before a person approves it (design doc, section 10), such as whether the
// patch applies and where the target stands now. It is best-effort: what it cannot find out, it
// says in its answer rather than failing the call.
type Describer interface {
	Writer
	Describe(ctx context.Context, req Request, payload json.RawMessage) map[string]any
}

// Editable is a write a reviewer may edit before approving it. An edit replaces the value of one
// argument, EditArg, with a blob of the run: for github.open_draft_pr, the patch. The workflow
// gets the new value, checks it again, and calls again with it; that call needs an approval of
// its own.
type Editable interface {
	Writer
	EditArg() string
}

// Proceed is asked before each visible request of a write. It fails with ErrStopped when the
// request must not go out (the run was cancelled meanwhile, or the write's approval ran out while
// the tool prepared the request), and with another error that wraps
// ErrNotSent when agentd cannot record the request; and it marks the moment the request goes
// out: a lookup's wait for a request in flight counts from the last one.
type Proceed func(ctx context.Context) error

// Finder is a Writer whose effect can be looked up afterwards.
type Finder interface {
	Writer
	Find(ctx context.Context, payload json.RawMessage) (value map[string]any, found bool, err error)
	// Settle is how long after its last visible request went out a lookup that finds nothing
	// means the write never took effect: the client's timeout plus the server's own
	// processing limit.
	Settle() time.Duration
}

// ErrNotSent means the request never left agentd, or failed before anything visible
// happened: sending it again is safe.
var ErrNotSent = errors.New("request not sent")

// ErrStopped is what Proceed fails with when the run was cancelled, the write's approval ran out,
// or the effect is no longer this sender's to send: nothing more of the write may go out. It is
// ErrNotSent too. Proceed failing otherwise (agentd could not record the request) may be tried
// again.
var ErrStopped = fmt.Errorf("%w: the run was cancelled, the approval expired, or the effect changed", ErrNotSent)

// DeniedError refuses a call as it stands, before anything is sent: for example a patch that
// touches a file the policy protects, or (from Execute, which then ends the effect) one whose
// files are not what GitHub holds. The workflow gets a "denied" result.
type DeniedError struct{ Reason string }

func (e *DeniedError) Error() string { return "denied: " + e.Reason }

// RefusedError means the external system answered and did nothing.
type RefusedError struct {
	Reason    string
	Retryable bool // e.g. a rate limit: the same request may succeed later
}

func (e *RefusedError) Error() string { return "refused: " + e.Reason }

// ConflictError means the external system already holds something different under the name
// the effect would create. agentd never overwrites it (section 6): a person decides.
type ConflictError struct{ Reason string }

func (e *ConflictError) Error() string { return "conflict: " + e.Reason }

// BlobStore keeps large values out of activity payloads, owned by the run that produced them.
type BlobStore interface {
	PutRunBlob(ctx context.Context, workflowID string, b []byte) (string, error)
	GetRunBlob(ctx context.Context, workflowID, ref string) ([]byte, error)
}

// Registry is the tools agentd offers, by name.
type Registry map[string]Tool

func (r Registry) Add(t Tool) { r[t.Name()] = t }

// Schema is a compiled JSON Schema for a tool's arguments or results.
type Schema struct {
	compiled *jsonschema.Schema
	props    []string
}

// MustSchema compiles a schema written into the code; a broken one is a programming error.
func MustSchema(text string) *Schema {
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(text))
	if err != nil {
		panic(err)
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource("schema.json", doc); err != nil {
		panic(err)
	}
	compiled, err := c.Compile("schema.json")
	if err != nil {
		panic(err)
	}
	s := &Schema{compiled: compiled}
	if obj, ok := doc.(map[string]any); ok {
		if props, ok := obj["properties"].(map[string]any); ok {
			s.props = slices.Sorted(maps.Keys(props))
		}
	}
	return s
}

// Props lists the properties the schema names.
func (s *Schema) Props() []string { return s.props }

// Validate checks v against the schema.
func (s *Schema) Validate(v map[string]any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	if err != nil {
		return err
	}
	if err := s.compiled.Validate(doc); err != nil {
		return fmt.Errorf("%v", err)
	}
	return nil
}

// Common argument schemas. The gateway fills each scoped argument with the run's own value;
// these still check what a workflow passes.
const (
	CommitPattern = `^[0-9a-f]{40}$`
	BlobPattern   = `^sha256:[0-9a-f]{64}$`
)

var branchRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,99}$`)

// ValidBranch says whether name is a branch name agentd takes: git's rule for one (git
// check-ref-format --branch) within a few plain characters. No "..", no part that is empty or
// begins with a dot or ends in ".lock", no dot at the end, not "HEAD". GitHub reads a branch by
// its exact name, but go-github resolves dot segments in the paths it asks for ("a/./b" is
// a/b), and GitHub answers for "a//b" with a/b's commit under the name it was asked for: with
// such a name, open_draft_pr would check one branch and open the pull request against another.
func ValidBranch(name string) bool {
	if !branchRe.MatchString(name) || name == "HEAD" || strings.Contains(name, "..") || strings.HasSuffix(name, ".") {
		return false
	}
	for part := range strings.SplitSeq(name, "/") {
		if part == "" || strings.HasPrefix(part, ".") || strings.HasSuffix(part, ".lock") {
			return false
		}
	}
	return true
}

// String returns args[key] when it is a string.
func String(args map[string]any, key string) string {
	s, _ := args[key].(string)
	return s
}
