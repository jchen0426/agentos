package audit

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jchen0426/agentos/internal/ledger/db"
)

// Projection is the state the audit log's records imply, replayed from them: of the effects, the
// approvals, the runs, the budget's entries, the loop guard's count of calls, the requests to the
// models, the runs' blobs and the people's findings that they record, and the hashes of the bytes
// they name. Compared with the ledger's tables (Differences), it shows a table changed without its
// record, as the chain shows a record changed. A field no record has set (of a row from before the
// audit log, or one a test wrote itself) is not compared: an approval made before the log began
// and decided after it is compared for its decision, not for when it was made.
type Projection struct {
	next       int64
	Effects    map[string]*ProjectedEffect
	Approvals  map[string]*ProjectedApproval
	Runs       map[string]*ProjectedRun
	Extensions map[[2]string]int64 // (workflow ID, idempotency key) -> amount
	// Calls are the logical calls the loop guard counted (calls), by run, activity and when Temporal
	// scheduled them.
	Calls map[[3]string]*ProjectedCall
	// Entries are the budget's reservations and what closed them (budget_entries), by request.
	Entries map[string]*ProjectedRequest
	// Requests are the requests to the models (model_calls), by run, activity, attempt and seq.
	Requests map[ModelRequestKey]*ProjectedModelRequest
	// Owned are the blobs each run produced, which it may have read into a prompt (blob_owners).
	Owned map[string]map[string]bool
	// Resolutions are the people's findings about effects nobody knew the outcome of
	// (effect_resolutions), by effect and the finding's key.
	Resolutions map[[2]string]*ProjectedResolution
	// Violations are the records that break the effect protocol: an effect a person was asked to
	// approve, claimed before an approval of it was used.
	Violations []Difference
}

// ProjectedCall is a logical call as its records say: what it is, and the outcome its last record
// set (Outcome nil while none has).
type ProjectedCall struct {
	Kind, Name, ArgsSHA256 string
	Outcome                *string
}

// ProjectedRequest is one model request's reservation (Reserved -1 when it came before the log
// began), and what closed it (both nil while it is open).
type ProjectedRequest struct {
	WorkflowID, ActivityID, Model, PriceTableVersion string
	Attempt, Reserved                                int64
	Settled                                          *ProjectedSettle
	Released                                         *string // the reason
}

type ProjectedSettle struct {
	Amount                           int64
	Model, PriceTableVersion, Reason string
}

type ModelRequestKey struct {
	WorkflowID, ActivityID string
	Attempt, Seq           int64
}

// ProjectedModelRequest is a request to a model, and its answer once it came.
type ProjectedModelRequest struct {
	RequestSHA256, Provider, Model, PromptVersion, Effort string
	Answered                                              bool
	ResponseSHA256, StopReason, UsageSHA256               string
}

type ProjectedResolution struct {
	Executed bool
	Status   string
}

// A field that is nil, or "" for Status, is one no record has set.
type ProjectedEffect struct {
	Status         string  // "" until a record says
	ClaimedBy      *string // "" for none
	MaybeSent      *bool
	PayloadSHA256  *string // of the payload recorded for it, which its payload_blob names too
	ResultBlob     *string // the reference of its result's blob; "" for none
	Tool, Class    string  // as recorded; "" for none: they decide how it is sent, and recovered
	IdempotencyKey *string // under which it is sent again; nil in records from before they had it
	gated          bool    // an approval of it was asked for
	approved       bool    // and used since
}

type ProjectedApproval struct {
	Status        string  // "" until a record says
	DecidedBy     *string // "" while undecided
	Used          *bool
	PayloadSHA256 *string
	ExpiresAt     *string
	EditedPatch   *string // the reference of the reviewer's patch blob; "" for none
	Tool          string  // as recorded
	Context       *string // the reference of what the reviewer is shown; nil in records from before they had it
}

type ProjectedRun struct {
	Created     bool
	Task        string
	Params      map[string]string
	Budget      int64
	DryRun      bool
	RunID       string // its execution, once started
	CancelledAt string
	Discarded   bool
}

// Difference is a way the ledger's tables, or the records themselves, are not what the records say.
type Difference struct {
	Kind string // effect, approval, run, budget, protocol
	ID   string
	Why  string
}

func ptr[T any](v T) *T { return &v }

// Read applies the records appended since the last Read.
func (p *Projection) Read(ctx context.Context, q *db.Queries) error {
	if p.next == 0 {
		p.next = 1
	}
	for {
		rows, err := q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: p.next, Limit: page})
		if err != nil {
			return err
		}
		for _, r := range rows {
			if err := p.Apply(r); err != nil {
				return err
			}
		}
		if len(rows) < page {
			return nil
		}
	}
}

// Apply applies one record, the one after the last applied.
func (p *Projection) Apply(r db.AuditLog) error {
	if p.Effects == nil {
		p.Effects, p.Approvals, p.Runs = map[string]*ProjectedEffect{}, map[string]*ProjectedApproval{}, map[string]*ProjectedRun{}
		p.Extensions = map[[2]string]int64{}
		p.Calls, p.Entries, p.Requests = map[[3]string]*ProjectedCall{}, map[string]*ProjectedRequest{}, map[ModelRequestKey]*ProjectedModelRequest{}
		p.Owned, p.Resolutions = map[string]map[string]bool{}, map[[2]string]*ProjectedResolution{}
	}
	p.next = r.Seq + 1
	var d struct {
		EffectID          string            `json:"effect_id"`
		ApprovalID        string            `json:"approval_id"`
		Status            string            `json:"status"`
		By                string            `json:"by"`
		Decision          string            `json:"decision"`
		DecidedBy         *string           `json:"decided_by"`
		MaybeSent         bool              `json:"maybe_sent"`
		PayloadSHA256     string            `json:"payload_sha256"`
		ResultSHA256      string            `json:"result_sha256"`
		ExpiresAt         string            `json:"expires_at"`
		EditedPatchSHA256 string            `json:"edited_patch_sha256"`
		Task              string            `json:"task"`
		Params            map[string]string `json:"params"`
		Budget            int64             `json:"budget_limit_micros"`
		DryRun            bool              `json:"dry_run"`
		TemporalRunID     string            `json:"temporal_run_id"`
		CancelledAt       string            `json:"cancelled_at"`
		Amount            int64             `json:"amount_micros"`
		IdempotencyKey    string            `json:"idempotency_key"`
		Tool              string            `json:"tool"`
		Class             string            `json:"class"`
		ContextSHA256     *string           `json:"context_sha256"`
		ActivityID        string            `json:"activity_id"`
		ScheduledAt       string            `json:"scheduled_at"`
		Kind              string            `json:"kind"`
		Name              string            `json:"name"`
		ArgsSHA256        string            `json:"args_sha256"`
		Outcome           *string           `json:"outcome"`
		RequestID         string            `json:"request_id"`
		Attempt           int64             `json:"attempt"`
		Model             string            `json:"model"`
		PriceTableVersion string            `json:"price_table_version"`
		Reason            string            `json:"reason"`
		RequestSeq        int64             `json:"request_seq"`
		RequestSHA256     string            `json:"request_sha256"`
		Provider          string            `json:"provider"`
		PromptVersion     string            `json:"prompt_version"`
		Effort            string            `json:"effort"`
		ResponseSHA256    string            `json:"response_sha256"`
		StopReason        string            `json:"stop_reason"`
		UsageSHA256       string            `json:"usage_sha256"`
		SHA256            string            `json:"sha256"`
		Executed          bool              `json:"executed"`
		ResolveKey        *string           `json:"resolve_key"`
	}
	if err := json.Unmarshal([]byte(r.Payload), &d); err != nil {
		return fmt.Errorf("record %d: %w", r.Seq, err)
	}
	effect := func(id string) *ProjectedEffect {
		if p.Effects[id] == nil {
			p.Effects[id] = &ProjectedEffect{}
		}
		return p.Effects[id]
	}
	approval := func() *ProjectedApproval {
		if p.Approvals[d.ApprovalID] == nil {
			p.Approvals[d.ApprovalID] = &ProjectedApproval{}
		}
		return p.Approvals[d.ApprovalID]
	}
	run := func() *ProjectedRun {
		if p.Runs[r.WorkflowID] == nil {
			p.Runs[r.WorkflowID] = &ProjectedRun{}
		}
		return p.Runs[r.WorkflowID]
	}
	owned := func(workflowID, sha string) {
		if p.Owned[workflowID] == nil {
			p.Owned[workflowID] = map[string]bool{}
		}
		p.Owned[workflowID][sha] = true
	}
	ref := func(sha string) *string { // a blob reference, as the tables hold it
		if sha == "" {
			return ptr("")
		}
		return ptr("sha256:" + sha)
	}
	switch r.Type {
	case "effect.recorded":
		e := &ProjectedEffect{Status: d.Status, ClaimedBy: ptr(""), MaybeSent: ptr(false),
			PayloadSHA256: ptr(d.PayloadSHA256), ResultBlob: ptr(""), Tool: d.Tool, Class: d.Class}
		if d.IdempotencyKey != "" {
			e.IdempotencyKey = ptr(d.IdempotencyKey)
		}
		p.Effects[d.EffectID] = e
	case "effect.claimed":
		e := effect(d.EffectID)
		if e.gated && !e.approved {
			p.Violations = append(p.Violations, Difference{"protocol", d.EffectID, fmt.Sprintf(
				"effect %s was claimed (record %d) after a person was asked to approve it, and before an approval was used",
				d.EffectID, r.Seq)})
		}
		e.ClaimedBy = ptr(d.By)
	case "effect.sending":
		effect(d.EffectID).MaybeSent = ptr(true)
	case "effect.released":
		e := effect(d.EffectID)
		e.ClaimedBy, e.MaybeSent = ptr(""), ptr(d.MaybeSent)
	case "effect.settled":
		e := effect(d.EffectID)
		e.Status, e.ResultBlob = d.Status, ref(d.ResultSHA256)
	case "effect.reconciled":
		e := effect(d.EffectID)
		e.Status, e.ClaimedBy, e.MaybeSent, e.ResultBlob = d.Status, ptr(""), ptr(false), ref(d.ResultSHA256)
		if d.ResolveKey != nil {
			p.Resolutions[[2]string{d.EffectID, *d.ResolveKey}] = &ProjectedResolution{Executed: d.Executed, Status: d.Status}
		}
	case "approval.created":
		p.Approvals[d.ApprovalID] = &ProjectedApproval{Status: "PENDING", DecidedBy: ptr(""), Used: ptr(false),
			PayloadSHA256: ptr(d.PayloadSHA256), ExpiresAt: ptr(d.ExpiresAt), EditedPatch: ptr(""), Tool: d.Tool}
		if d.ContextSHA256 != nil {
			p.Approvals[d.ApprovalID].Context = ref(*d.ContextSHA256)
		}
		e := effect(d.EffectID)
		e.Status, e.gated, e.approved = "AWAITING_APPROVAL", true, false
	case "approval.decided":
		a := approval()
		a.Status = map[string]string{"approve": "APPROVED", "reject": "REJECTED", "edit": "EDITED"}[d.Decision]
		a.DecidedBy = d.DecidedBy // nil, unknown, in a record from before the decider was written in it
		if d.PayloadSHA256 != "" {
			a.PayloadSHA256 = ptr(d.PayloadSHA256)
		}
		if d.Decision == "edit" {
			a.EditedPatch = ref(d.EditedPatchSHA256)
			owned(r.WorkflowID, d.EditedPatchSHA256) // the run reads the patch it was given as its own
		}
	case "approval.expired":
		a := approval()
		a.Status, a.DecidedBy = "EXPIRED", ptr("agentd")
	case "approval.superseded":
		approval().Status = "SUPERSEDED"
	case "approval.used":
		approval().Used = ptr(true)
		e := effect(d.EffectID)
		e.Status, e.approved = "PENDING", true
	case "run.created":
		ru := run()
		ru.Created, ru.Task, ru.Params, ru.Budget, ru.DryRun = true, d.Task, d.Params, d.Budget, d.DryRun
	case "run.started":
		run().RunID = d.TemporalRunID
	case "run.cancelled":
		run().CancelledAt = d.CancelledAt
	case "run.discarded":
		run().Discarded = true
	case "budget.extended":
		p.Extensions[[2]string{r.WorkflowID, d.IdempotencyKey}] = d.Amount
	case "call.counted":
		p.Calls[[3]string{r.WorkflowID, d.ActivityID, d.ScheduledAt}] = &ProjectedCall{Kind: d.Kind, Name: d.Name, ArgsSHA256: d.ArgsSHA256}
	case "call":
		if d.ScheduledAt == "" {
			break // refused before it counted, or from before calls were recorded as counted
		}
		c := p.Calls[[3]string{r.WorkflowID, d.ActivityID, d.ScheduledAt}]
		switch {
		case c == nil: // agentd counted the call before it went on: the count has its record
			p.Violations = append(p.Violations, Difference{"call", r.WorkflowID, fmt.Sprintf("record %d, the call %q scheduled "+
				"at %s, names a count the audit log does not have (no call.counted before it)", r.Seq, d.ActivityID, d.ScheduledAt)})
		case d.Outcome != nil:
			c.Outcome = d.Outcome
		}
	case "budget.reserved":
		p.Entries[d.RequestID] = &ProjectedRequest{WorkflowID: r.WorkflowID, ActivityID: d.ActivityID, Attempt: d.Attempt,
			Model: d.Model, PriceTableVersion: d.PriceTableVersion, Reserved: d.Amount}
	case "budget.settled", "budget.released":
		e := p.Entries[d.RequestID]
		if e == nil {
			e = &ProjectedRequest{WorkflowID: r.WorkflowID, Reserved: -1} // reserved before the log began
			p.Entries[d.RequestID] = e
		}
		if r.Type == "budget.settled" {
			e.Settled = &ProjectedSettle{Amount: d.Amount, Model: d.Model, PriceTableVersion: d.PriceTableVersion, Reason: d.Reason}
		} else {
			e.Released = ptr(d.Reason)
		}
	case "model.requested":
		p.Requests[ModelRequestKey{r.WorkflowID, d.ActivityID, d.Attempt, d.RequestSeq}] = &ProjectedModelRequest{
			RequestSHA256: d.RequestSHA256, Provider: d.Provider, Model: d.Model, PromptVersion: d.PromptVersion, Effort: d.Effort}
	case "model.answered":
		if m := p.Requests[ModelRequestKey{r.WorkflowID, d.ActivityID, d.Attempt, d.RequestSeq}]; m != nil {
			m.Answered, m.ResponseSHA256, m.StopReason, m.UsageSHA256 = true, d.ResponseSHA256, d.StopReason, d.UsageSHA256
		}
	case "blob.owned":
		owned(r.WorkflowID, d.SHA256)
	}
	return nil
}

// Since is when rows began to need records: rows created at or after it that no record speaks of are
// differences too. Zero checks no such rows.
type Since struct {
	At   time.Time
	Runs bool // runs and budget extensions as well as effects and approvals
}

// Differences says where the ledger's tables are not what the records say, and which records break
// the protocol, in a stable order.
func (p *Projection) Differences(ctx context.Context, q *db.Queries, since Since) ([]Difference, error) {
	out := slices.Clone(p.Violations)
	add := func(kind, id, format string, args ...any) {
		out = append(out, Difference{kind, id, fmt.Sprintf(format, args...)})
	}
	for _, id := range slices.Sorted(maps.Keys(p.Effects)) {
		s := p.Effects[id]
		e, err := q.GetEffect(ctx, id)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			add("effect", id, "the audit log records effect %s, and the ledger has no such effect", id)
			continue
		case err != nil:
			return nil, err
		}
		if s.Status != "" && s.Status != e.Status {
			add("effect", id, "effect %s is %s, and the audit log says %s", id, e.Status, s.Status)
		}
		if s.ClaimedBy != nil && *s.ClaimedBy != e.ClaimedBy.String {
			add("effect", id, "effect %s is claimed by %q, and the audit log says %q", id, e.ClaimedBy.String, *s.ClaimedBy)
		}
		if s.MaybeSent != nil && *s.MaybeSent != (e.MaybeSent != 0) {
			add("effect", id, "effect %s has maybe_sent %d, and the audit log says %v", id, e.MaybeSent, *s.MaybeSent)
		}
		if s.PayloadSHA256 != nil && (e.PayloadSha256 != *s.PayloadSHA256 || e.PayloadBlob.String != "sha256:"+*s.PayloadSHA256) {
			add("effect", id, "effect %s has payload_sha256 %s and payload blob %q, and the audit log records its payload as %s",
				id, e.PayloadSha256, e.PayloadBlob.String, *s.PayloadSHA256)
		}
		if s.ResultBlob != nil && e.ResultBlob.String != *s.ResultBlob {
			add("effect", id, "effect %s has result blob %q, and the audit log says %q", id, e.ResultBlob.String, *s.ResultBlob)
		}
		if s.Tool != "" && e.Tool != s.Tool || s.Class != "" && e.EffectClass != s.Class {
			add("effect", id, "effect %s is a write of %q, of class %q, and the audit log says %q, %q", id, e.Tool, e.EffectClass,
				s.Tool, s.Class)
		}
		if s.IdempotencyKey != nil && e.IdempotencyKey != *s.IdempotencyKey {
			add("effect", id, "effect %s has idempotency key %q, and the audit log says %q", id, e.IdempotencyKey, *s.IdempotencyKey)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(p.Approvals)) {
		s := p.Approvals[id]
		a, err := q.GetApproval(ctx, id)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			add("approval", id, "the audit log records approval %s, and the ledger has no such approval", id)
			continue
		case err != nil:
			return nil, err
		}
		if s.Status != "" && (a.Status != s.Status || a.DecidedAt.Valid != (s.Status != "PENDING")) {
			add("approval", id, "approval %s is %s (decided %v), and the audit log says %s", id, a.Status, a.DecidedAt.Valid, s.Status)
		}
		if s.Used != nil && a.UsedAt.Valid != *s.Used {
			add("approval", id, "approval %s has used %v, and the audit log says %v", id, a.UsedAt.Valid, *s.Used)
		}
		if s.DecidedBy != nil && *s.DecidedBy != a.DecidedBy.String {
			add("approval", id, "approval %s was decided by %q, and the audit log says %q", id, a.DecidedBy.String, *s.DecidedBy)
		}
		if s.PayloadSHA256 != nil && a.PayloadSha256 != *s.PayloadSHA256 {
			add("approval", id, "approval %s is of payload sha256 %s, and the audit log says %s", id, a.PayloadSha256, *s.PayloadSHA256)
		}
		if s.ExpiresAt != nil && a.ExpiresAt != *s.ExpiresAt {
			add("approval", id, "approval %s expires at %s, and the audit log says %s", id, a.ExpiresAt, *s.ExpiresAt)
		}
		if s.EditedPatch != nil && a.EditedPatchBlob.String != *s.EditedPatch {
			add("approval", id, "approval %s has the edited patch %q, and the audit log says %q", id, a.EditedPatchBlob.String,
				*s.EditedPatch)
		}
		if s.Tool != "" && a.Tool != s.Tool {
			add("approval", id, "approval %s is of a write of %q, and the audit log says %q", id, a.Tool, s.Tool)
		}
		if s.Context != nil && a.ContextBlob != *s.Context {
			add("approval", id, "approval %s shows the reviewer %q, and the audit log says %q", id, a.ContextBlob, *s.Context)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(p.Runs)) {
		s := p.Runs[id]
		r, err := q.GetRun(ctx, id)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if !s.Discarded {
				add("run", id, "the audit log records run %s, and the ledger has no row of it", id)
			}
			continue
		case err != nil:
			return nil, err
		case s.Discarded:
			add("run", id, "the audit log records run %s as discarded, and the ledger has a row of it", id)
			continue
		}
		var params map[string]string
		if err := json.Unmarshal([]byte(r.ParamsJson), &params); err != nil {
			params = nil
		}
		switch {
		case s.Created && (r.Task != s.Task || r.BudgetLimitMicros != s.Budget || (r.DryRun != 0) != s.DryRun ||
			!maps.Equal(params, s.Params)):
			add("run", id, "run %s is task %s with params %s, a budget of %d and dry run %v; the audit log says %s, %v, %d, %v",
				id, r.Task, r.ParamsJson, r.BudgetLimitMicros, r.DryRun != 0, s.Task, s.Params, s.Budget, s.DryRun)
		case s.RunID != "" && r.RunID.String != s.RunID:
			add("run", id, "run %s has execution %q, and the audit log says %q", id, r.RunID.String, s.RunID)
		case s.Created && s.RunID == "" && r.RunID.Valid:
			add("run", id, "run %s has execution %q, and the audit log records no start of it", id, r.RunID.String)
		case s.CancelledAt != "" && r.CancelledAt.String != s.CancelledAt:
			add("run", id, "run %s was cancelled at %q, and the audit log says %q", id, r.CancelledAt.String, s.CancelledAt)
		case s.Created && s.CancelledAt == "" && r.CancelledAt.Valid:
			add("run", id, "run %s was cancelled at %s, and the audit log records no cancel", id, r.CancelledAt.String)
		}
	}
	extensions, err := q.ListBudgetExtensions(ctx)
	if err != nil {
		return nil, err
	}
	seen := map[[2]string]bool{}
	for _, x := range extensions {
		k := [2]string{x.WorkflowID, x.IdempotencyKey}
		seen[k] = true
		amount, recorded := p.Extensions[k]
		switch {
		case recorded && amount != x.AmountMicros:
			add("budget", x.WorkflowID, "run %s's extension %q is %d micros, and the audit log says %d", x.WorkflowID,
				x.IdempotencyKey, x.AmountMicros, amount)
		case !recorded && since.Runs && after(x.CreatedAt, since.At):
			add("budget", x.WorkflowID, "run %s's extension %q of %d micros, made at %s, is not in the audit log", x.WorkflowID,
				x.IdempotencyKey, x.AmountMicros, x.CreatedAt)
		}
	}
	for _, k := range slices.SortedFunc(maps.Keys(p.Extensions), func(a, b [2]string) int { return slices.Compare(a[:], b[:]) }) {
		if !seen[k] {
			add("budget", k[0], "the audit log records run %s's extension %q, and the ledger has no such extension", k[0], k[1])
		}
	}
	runs, err := q.ListAllRuns(ctx)
	if err != nil {
		return nil, err
	}
	needsRecord := p.needsRecord(runs, since)
	if err := p.otherTables(ctx, q, since, needsRecord, add); err != nil {
		return nil, err
	}
	if since.At.IsZero() {
		return out, nil
	}
	effects, err := q.ListEffectsCreated(ctx)
	if err != nil {
		return nil, err
	}
	for _, e := range effects {
		if p.Effects[e.EffectID] == nil && needsRecord(e.WorkflowID, e.CreatedAt) {
			add("effect", e.EffectID, "effect %s, made at %s, is not in the audit log (run %s)", e.EffectID, e.CreatedAt, e.WorkflowID)
		}
	}
	approvals, err := q.ListApprovalsCreated(ctx)
	if err != nil {
		return nil, err
	}
	for _, a := range approvals {
		if p.Approvals[a.ApprovalID] == nil && needsRecord(a.WorkflowID, a.CreatedAt) {
			add("approval", a.ApprovalID, "approval %s, made at %s, is not in the audit log (run %s)", a.ApprovalID, a.CreatedAt,
				a.WorkflowID)
		}
	}
	if since.Runs {
		for _, r := range runs {
			if (p.Runs[r.WorkflowID] == nil || !p.Runs[r.WorkflowID].Created) && after(r.CreatedAt, since.At) {
				add("run", r.WorkflowID, "run %s, made at %s, is not in the audit log", r.WorkflowID, r.CreatedAt)
			}
		}
	}
	return out, nil
}

// needsRecord says whether a row of run workflowID, made at at (by the row's own word), must have
// its record. Every row of a run whose creation the log records, whatever its date: a row says its
// own date, and one dated before the log began would be exempt otherwise. Of a run made before the
// log began, which no record speaks of (it counts as before the log: control's RunsBeforeLog), the
// rows made since it began. Of any other run (one with no row, or one made since the log began that
// no record speaks of), every row. With a zero since.At, none.
func (p *Projection) needsRecord(runs []db.Run, since Since) func(workflowID, at string) bool {
	beforeLog := map[string]bool{}
	for _, r := range runs {
		if (p.Runs[r.WorkflowID] == nil || !p.Runs[r.WorkflowID].Created) && !since.At.IsZero() && !after(r.CreatedAt, since.At) {
			beforeLog[r.WorkflowID] = true
		}
	}
	return func(workflowID, at string) bool {
		switch {
		case since.At.IsZero():
			return false
		case p.Runs[workflowID] != nil && p.Runs[workflowID].Created:
			return true
		case beforeLog[workflowID]:
			return after(at, since.At)
		}
		return true
	}
}

// after reports whether ledger time s is at or after t; a time that cannot be read is.
func after(s string, t time.Time) bool {
	at, err := time.Parse(time.RFC3339Nano, s)
	return err != nil || !at.Before(t)
}

// otherTables compares what the loop guard (calls), the budget (budget_entries, but its
// extensions), the requests to the models (model_calls), the runs' blobs (blob_owners) and the
// people's findings (effect_resolutions) hold with their records: a recorded row must be there as
// recorded, and a row made since since.At (with since.Runs) must have its record. A run's blobs
// carry no time: those of a run the log records the creation of must all have theirs.
func (p *Projection) otherTables(ctx context.Context, q *db.Queries, since Since, needsRecord func(workflowID, at string) bool,
	add func(kind, id, format string, args ...any)) error {
	unrecorded := func(workflowID, at string) bool { return since.Runs && needsRecord(workflowID, at) }
	calls, err := q.ListCallsAll(ctx)
	if err != nil {
		return err
	}
	seenCalls := map[[3]string]bool{}
	for _, c := range calls {
		k := [3]string{c.WorkflowID, Text(c.ActivityID), c.ScheduledAt}
		seenCalls[k] = true
		pc := p.Calls[k]
		switch {
		case pc == nil:
			if unrecorded(c.WorkflowID, c.CreatedAt) {
				add("call", c.WorkflowID, "call %q of run %s, scheduled at %s, is counted (calls) and is not in the audit log",
					c.ActivityID, c.WorkflowID, c.ScheduledAt)
			}
		case pc.Kind != Text(c.Kind) || pc.Name != Text(c.Name) || pc.ArgsSHA256 != c.ArgsSha256:
			add("call", c.WorkflowID, "call %q of run %s is counted as %q %q with arguments %s, and the audit log says %q %q with %s",
				c.ActivityID, c.WorkflowID, c.Kind, c.Name, c.ArgsSha256, pc.Kind, pc.Name, pc.ArgsSHA256)
		case (pc.Outcome != nil) != c.Outcome.Valid || (pc.Outcome != nil && *pc.Outcome != c.Outcome.String):
			recorded := "none"
			if pc.Outcome != nil {
				recorded = strconv.Quote(*pc.Outcome)
			}
			add("call", c.WorkflowID, "call %q of run %s has the outcome %q (valid %v), and the audit log says %s", c.ActivityID,
				c.WorkflowID, c.Outcome.String, c.Outcome.Valid, recorded)
		}
	}
	for _, k := range slices.SortedFunc(maps.Keys(p.Calls), func(a, b [3]string) int { return slices.Compare(a[:], b[:]) }) {
		if !seenCalls[k] {
			add("call", k[0], "the audit log counts call %q of run %s, scheduled at %s, and the ledger has no such call: the loop "+
				"guard counts what calls holds", k[1], k[0], k[2])
		}
	}

	entries, err := q.ListBudgetEntriesAll(ctx)
	if err != nil {
		return err
	}
	type kinds struct{ reserve, settle, release bool }
	seen := map[string]*kinds{}
	for _, e := range entries {
		if e.Kind == "EXTEND" {
			continue // compared with budget.extended above
		}
		id := Text(e.RequestID)
		if seen[id] == nil {
			seen[id] = &kinds{}
		}
		pe := p.Entries[id]
		switch e.Kind {
		case "RESERVE":
			seen[id].reserve = true
			switch {
			case pe == nil || pe.Reserved < 0:
				if unrecorded(e.WorkflowID, e.CreatedAt) {
					add("budget", e.WorkflowID, "the reservation of request %q, %d micros at %s, is not in the audit log", e.RequestID,
						e.AmountMicros, e.CreatedAt)
				}
			case pe.WorkflowID != e.WorkflowID || pe.Reserved != e.AmountMicros || pe.ActivityID != Text(e.ActivityID) ||
				pe.Attempt != e.Attempt || pe.Model != e.Model || pe.PriceTableVersion != e.PriceTableVersion:
				add("budget", e.WorkflowID, "the reservation of request %q is %d micros for %s (run %s, call %q, attempt %d, prices %s), "+
					"and the audit log says %d micros for %s (run %s, call %q, attempt %d, prices %s)", e.RequestID, e.AmountMicros,
					e.Model, e.WorkflowID, e.ActivityID, e.Attempt, e.PriceTableVersion, pe.Reserved, pe.Model, pe.WorkflowID,
					pe.ActivityID, pe.Attempt, pe.PriceTableVersion)
			}
		case "SETTLE":
			seen[id].settle = true
			switch {
			case pe == nil || pe.Settled == nil:
				if unrecorded(e.WorkflowID, e.CreatedAt) {
					add("budget", e.WorkflowID, "the charge of request %q, %d micros at %s, is not in the audit log", e.RequestID,
						e.AmountMicros, e.CreatedAt)
				}
			case pe.Settled.Amount != e.AmountMicros || pe.Settled.Model != e.Model || pe.Settled.PriceTableVersion != e.PriceTableVersion ||
				pe.Settled.Reason != e.Reason:
				add("budget", e.WorkflowID, "request %q was charged %d micros for %s (%s, prices %s), and the audit log says %d micros "+
					"for %s (%s, prices %s)", e.RequestID, e.AmountMicros, e.Model, e.Reason, e.PriceTableVersion, pe.Settled.Amount,
					pe.Settled.Model, pe.Settled.Reason, pe.Settled.PriceTableVersion)
			}
		case "RELEASE":
			seen[id].release = true
			switch {
			case pe == nil || pe.Released == nil:
				if unrecorded(e.WorkflowID, e.CreatedAt) {
					add("budget", e.WorkflowID, "the release of request %q at %s is not in the audit log", e.RequestID, e.CreatedAt)
				}
			case *pe.Released != e.Reason:
				add("budget", e.WorkflowID, "request %q was released (%s), and the audit log says %s", e.RequestID, e.Reason, *pe.Released)
			}
		}
	}
	for _, id := range slices.Sorted(maps.Keys(p.Entries)) {
		pe, k := p.Entries[id], seen[id]
		if k == nil {
			k = &kinds{}
		}
		switch {
		case pe.Reserved >= 0 && !k.reserve:
			add("budget", pe.WorkflowID, "the audit log records the reservation of request %q, and the ledger has no such entry", id)
		case pe.Settled != nil && !k.settle:
			add("budget", pe.WorkflowID, "the audit log records the charge of request %q, and the ledger has no such entry", id)
		case pe.Released != nil && !k.release:
			add("budget", pe.WorkflowID, "the audit log records the release of request %q, and the ledger has no such entry", id)
		}
	}

	requests, err := q.ListModelCallsAll(ctx)
	if err != nil {
		return err
	}
	seenRequests := map[ModelRequestKey]bool{}
	for _, m := range requests {
		k := ModelRequestKey{m.WorkflowID, Text(m.ActivityID), m.Attempt, m.RequestSeq}
		seenRequests[k] = true
		pm := p.Requests[k]
		usage := sha256.Sum256([]byte(m.UsageJson))
		switch {
		case pm == nil:
			if unrecorded(m.WorkflowID, m.StartedAt) {
				add("model", m.WorkflowID, "request %d of call %q, attempt %d, of run %s, made at %s, is not in the audit log",
					m.RequestSeq, m.ActivityID, m.Attempt, m.WorkflowID, m.StartedAt)
			}
		case strings.TrimPrefix(m.RequestBlob, "sha256:") != pm.RequestSHA256 || m.Provider != pm.Provider || m.Model != pm.Model ||
			m.PromptVersion != pm.PromptVersion || m.Effort != pm.Effort:
			add("model", m.WorkflowID, "request %d of call %q, attempt %d, of run %s is %s to %s/%s (prompt %s, effort %q), and the "+
				"audit log says %s to %s/%s (prompt %s, effort %q)", m.RequestSeq, m.ActivityID, m.Attempt, m.WorkflowID, m.RequestBlob,
				m.Provider, m.Model, m.PromptVersion, m.Effort, pm.RequestSHA256, pm.Provider, pm.Model, pm.PromptVersion, pm.Effort)
		case pm.Answered != (m.ResponseBlob != ""):
			add("model", m.WorkflowID, "request %d of call %q, attempt %d, of run %s has the answer %q, and the audit log says it "+
				"was answered: %v", m.RequestSeq, m.ActivityID, m.Attempt, m.WorkflowID, m.ResponseBlob, pm.Answered)
		case pm.Answered && (strings.TrimPrefix(m.ResponseBlob, "sha256:") != pm.ResponseSHA256 || m.StopReason != pm.StopReason ||
			hex.EncodeToString(usage[:]) != pm.UsageSHA256):
			add("model", m.WorkflowID, "the answer to request %d of call %q, attempt %d, of run %s is %s (stopped %q, usage sha256 %s), "+
				"and the audit log says %s (stopped %q, usage sha256 %s)", m.RequestSeq, m.ActivityID, m.Attempt, m.WorkflowID,
				m.ResponseBlob, m.StopReason, hex.EncodeToString(usage[:]), pm.ResponseSHA256, pm.StopReason, pm.UsageSHA256)
		}
	}
	for _, k := range slices.SortedFunc(maps.Keys(p.Requests), func(a, b ModelRequestKey) int {
		return cmp.Or(strings.Compare(a.WorkflowID, b.WorkflowID), strings.Compare(a.ActivityID, b.ActivityID),
			cmp.Compare(a.Attempt, b.Attempt), cmp.Compare(a.Seq, b.Seq))
	}) {
		if !seenRequests[k] {
			add("model", k.WorkflowID, "the audit log records request %d of call %q, attempt %d, of run %s, and the ledger has no such "+
				"request", k.Seq, k.ActivityID, k.Attempt, k.WorkflowID)
		}
	}

	owners, err := q.ListBlobOwnersAll(ctx)
	if err != nil {
		return err
	}
	have := map[[2]string]bool{}
	for _, o := range owners {
		have[[2]string{o.WorkflowID, o.Sha256}] = true
		if run := p.Runs[o.WorkflowID]; !p.Owned[o.WorkflowID][o.Sha256] && since.Runs && run != nil && run.Created {
			add("blob", o.WorkflowID, "run %s owns blob %s, which it may read into a prompt, and the audit log does not say so",
				o.WorkflowID, o.Sha256)
		}
	}
	for _, workflowID := range slices.Sorted(maps.Keys(p.Owned)) {
		for _, sha := range slices.Sorted(maps.Keys(p.Owned[workflowID])) {
			if !have[[2]string{workflowID, sha}] {
				add("blob", workflowID, "the audit log records run %s as the owner of blob %s, and the ledger has no such owner",
					workflowID, sha)
			}
		}
	}

	findings, err := q.ListResolutionsAll(ctx)
	if err != nil {
		return err
	}
	seenFindings := map[[2]string]bool{}
	for _, f := range findings {
		k := [2]string{f.EffectID, Text(f.ResolveKey)}
		seenFindings[k] = true
		pf := p.Resolutions[k]
		switch {
		case pf == nil:
			if needsRecord(f.WorkflowID, f.ResolvedAt) {
				add("effect", f.EffectID, "the finding %q about effect %s, made at %s, is not in the audit log", f.ResolveKey, f.EffectID,
					f.ResolvedAt)
			}
		case pf.Executed != (f.Executed != 0) || pf.Status != f.Status:
			add("effect", f.EffectID, "the finding %q about effect %s is executed %v, %s, and the audit log says executed %v, %s",
				f.ResolveKey, f.EffectID, f.Executed != 0, f.Status, pf.Executed, pf.Status)
		}
	}
	for _, k := range slices.SortedFunc(maps.Keys(p.Resolutions), func(a, b [2]string) int { return slices.Compare(a[:], b[:]) }) {
		if !seenFindings[k] {
			add("effect", k[0], "the audit log records the finding %q about effect %s, and the ledger has no such finding", k[1], k[0])
		}
	}
	return nil
}
