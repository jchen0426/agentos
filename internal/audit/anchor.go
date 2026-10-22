package audit

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// External anchors: the chain's head, pushed out of the ledger into Temporal, where whoever can
// write the ledger cannot reach it. The results of calls anchor the records of calls; these anchor
// the rest, agentd's own records at the end of the chain among them, which nothing else follows.
// Each UTC day has a workflow of the chain's own (AnchorWorkflowID) that agentd starts with its
// first signal, and that nobody polls: no Worker may poll its task queue or signal it (the Runtime
// proxy's rules). It times out after AnchorTimeout, and the namespace keeps it for its retention
// after that. Its ID names the chain, by its first record: another agentd's ledger, or an earlier
// one, in the same namespace has workflows of its own.
const (
	AnchorWorkflowType = "agentos-audit-anchor"
	AnchorTaskQueue    = "agentos-audit-anchor"
	AnchorSignal       = "anchor"
	AnchorTimeout      = 8 * 24 * time.Hour
	// AnchorIDPrefix begins the ID of every chain's anchor workflows.
	AnchorIDPrefix = "agentos-audit-anchor-"
)

// AnchorChain is a chain's key in its anchor workflows' IDs: the first 16 hex characters of the
// sha256 of its first record.
func AnchorChain(firstSHA256 string) string { return firstSHA256[:min(16, len(firstSHA256))] }

// AnchorPrefix begins the IDs of chain's anchor workflows.
func AnchorPrefix(chain string) string { return AnchorIDPrefix + chain + "-" }

// AnchorChainOf is the key of the chain an anchor workflow's ID names: what it has between
// AnchorIDPrefix and the day. An ID without a day is all key.
func AnchorChainOf(workflowID string) string {
	rest := strings.TrimPrefix(workflowID, AnchorIDPrefix)
	if i := len(rest) - len("-2006-01-02"); i > 0 && rest[i] == '-' {
		if _, err := time.Parse("2006-01-02", rest[i+1:]); err == nil {
			return rest[:i]
		}
	}
	return rest
}

// AnchorWorkflowID is the workflow that holds chain's anchors pushed on t's UTC day.
func AnchorWorkflowID(chain string, t time.Time) string {
	return AnchorPrefix(chain) + t.UTC().Format("2006-01-02")
}

// AnchorReader is the part of Temporal's client agentd reads anchor workflows with as it starts: it
// lists them by their IDs, and reads their histories.
type AnchorReader interface {
	ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error)
	GetWorkflowHistory(ctx context.Context, workflowID, runID string, isLongPoll bool,
		filterType enumspb.HistoryEventFilterType) client.HistoryEventIterator
}

// Pushed is a head an anchor workflow holds: which, where (the workflow and the event), and when
// Temporal took it.
type Pushed struct {
	AnchorHead
	WorkflowID string
	EventID    int64
	At         time.Time
}

// Valid reports whether h can be a head agentd pushed: a positive seq, a sha256 of 64 lowercase hex
// characters, and no agentd or an identity's 16 (Identity). Anything else someone else signalled,
// and nothing of it is copied anywhere.
func (h AnchorHead) Valid() bool {
	return h.Seq > 0 && lowerHex(h.Sha256, 64) && (h.Agentd == "" || lowerHex(h.Agentd, 16))
}

func lowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range []byte(s) {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// ReadPushed reads the heads the anchor workflows ids hold, at most maxEvents events of each, and
// the signals named as heads that are none (unreadable, or not a head agentd pushes: Valid), as
// findings that copy nothing of them. A workflow Temporal does not have holds none; a signal of
// another name is not a head. What it could not read (a workflow past maxEvents, one whose history
// Temporal would not give, those left when ctx ended) is a finding too, with how much was not read:
// none of it is an error, since anyone with Temporal's API can make it happen.
func ReadPushed(ctx context.Context, h AnchorReader, ids []string, maxEvents int) (heads []Pushed, unreadable, unread []Finding) {
	for i, id := range ids {
		if ctx.Err() != nil {
			unread = append(unread, Finding{Why: fmt.Sprintf("out of time: %d anchor workflows not read", len(ids)-i),
				NotRead: len(ids) - i})
			return heads, unreadable, unread
		}
		it := h.GetWorkflowHistory(ctx, id, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for n := 0; it.HasNext(); n++ {
			if n == maxEvents {
				unread = append(unread, Finding{Pushed: Pushed{WorkflowID: id},
					Why: fmt.Sprintf("more than %d events: the rest of the anchor workflow not read", maxEvents), NotRead: 1})
				break
			}
			e, err := it.Next()
			var notFound *serviceerror.NotFound
			if errors.As(err, &notFound) {
				break
			}
			if err != nil {
				unread = append(unread, Finding{Pushed: Pushed{WorkflowID: id},
					Why: "the anchor workflow could not be read: " + Text(err.Error()), NotRead: 1})
				break
			}
			attrs := e.GetWorkflowExecutionSignaledEventAttributes()
			if e.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED || attrs.GetSignalName() != AnchorSignal {
				continue
			}
			p := Pushed{WorkflowID: id, EventID: e.GetEventId(), At: EventTime(e)}
			if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &p.AnchorHead); err != nil || !p.Valid() {
				unreadable = append(unreadable, Finding{Pushed: Pushed{WorkflowID: id, EventID: p.EventID, At: p.At},
					Why: "an unreadable head: not a head agentd pushes"})
				continue
			}
			heads = append(heads, p)
		}
	}
	return heads, unreadable, unread
}

// EventTime is when Temporal took e; the zero time when it says not.
func EventTime(e *historypb.HistoryEvent) time.Time {
	if e.GetEventTime() == nil {
		return time.Time{}
	}
	return e.GetEventTime().AsTime()
}

// Finding is a head pushed to Temporal that the ledger does not bear out, or what the start check
// could not read (NotRead: how many workflows, at least).
type Finding struct {
	Pushed
	Why     string
	NotRead int
}

// Payload is f as agentd.started records it. An unreadable head's has no seq or sha256.
func (f Finding) Payload() map[string]any {
	p := map[string]any{"why": Text(f.Why)}
	if f.WorkflowID != "" {
		p["workflow_id"] = Text(f.WorkflowID)
	}
	if f.EventID != 0 {
		p["event_id"] = f.EventID
	}
	if f.Seq > 0 {
		p["seq"], p["sha256"] = f.Seq, f.Sha256
	}
	if f.NotRead > 0 {
		p["not_read"] = f.NotRead
	}
	return p
}

// MaxFindings bounds what agentd.started and agentd's log say of the start check's findings: forged
// signals are as many as anyone sends.
const MaxFindings = 20

// FindingsRecord is findings as agentd.started holds them: the first MaxFindings, and how many
// more there were.
func FindingsRecord(findings []Finding) ([]map[string]any, int) {
	out := make([]map[string]any, 0, min(len(findings), MaxFindings))
	for _, f := range findings[:min(len(findings), MaxFindings)] {
		out = append(out, f.Payload())
	}
	return out, len(findings) - len(out)
}

// NotHeld are the heads q's ledger does not hold, by their seq and sha256. Every head is checked, not
// the newest: anyone who can signal Temporal can add a head, and a cut shows in the highest one
// that agentd pushed, which a head added later does not take away.
func NotHeld(ctx context.Context, q *db.Queries, heads []Pushed) ([]Finding, error) {
	var out []Finding
	for _, h := range heads {
		rows, err := q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: h.Seq, Limit: 1})
		switch {
		case err != nil:
			return nil, err
		case len(rows) == 0 || rows[0].Seq != h.Seq:
			out = append(out, Finding{Pushed: h, Why: fmt.Sprintf("the head pushed to Temporal was record %d, which the audit log does not have", h.Seq)})
		case rows[0].Sha256 != h.Sha256:
			out = append(out, Finding{Pushed: h, Why: fmt.Sprintf("the head pushed to Temporal was record %d with sha256 %s; the audit log has %s",
				h.Seq, h.Sha256, rows[0].Sha256)})
		}
	}
	return out, nil
}

// OutOfOrder are the heads pushed after one with a higher seq, in the order Temporal took them:
// agentd pushes the chain's head as it grows, so a lower one means the ledger went backwards
// between the two pushes. The order is Temporal's time, and within a workflow its events'.
func OutOfOrder(heads []Pushed) []Finding {
	sorted := slices.Clone(heads)
	slices.SortStableFunc(sorted, func(a, b Pushed) int {
		return cmp.Or(a.At.Compare(b.At), strings.Compare(a.WorkflowID, b.WorkflowID), cmp.Compare(a.EventID, b.EventID))
	})
	var out []Finding
	var highest *Pushed
	for i, h := range sorted {
		if highest != nil && h.Seq < highest.Seq {
			out = append(out, Finding{Pushed: h, Why: fmt.Sprintf("the head pushed to Temporal was record %d, after record %d (in %s): "+
				"the ledger went backwards between the two pushes", h.Seq, highest.Seq, highest.WorkflowID)})
		}
		if highest == nil || h.Seq > highest.Seq {
			highest = &sorted[i]
		}
	}
	return out
}

// The start check's bounds: it reads what anyone with Temporal's API can add to. What it does not get
// to read, it says it did not (Finding.NotRead); verify, which reads it all, is the authority.
var (
	StartCheckTime      = 10 * time.Second // for all of it
	StartCheckWorkflows = 50               // anchor workflows read, this chain's first
	MaxAnchorEvents     = 10000            // events read of each (agentd pushes at most 2880 a day)
)

// CheckPushed is what agentd checks as it starts, before it appends anything, in the anchor
// workflows of now's UTC day and the day before. Its chain's (chain is the ledger's, by its first
// record; "" for an empty log, which has none): each head must be a record the ledger holds, none
// may come after a higher one, and a signal that is no head is a finding too. And the heads this
// agentd pushed (identity), under any chain: the highest must be this chain's, or the ledger was
// replaced while agentd was down. It is bounded (StartCheckTime, StartCheckWorkflows,
// MaxAnchorEvents) and best-effort: what it could not read is a finding, with how much, not an
// error. What it finds does not stop agentd: Temporal's signals are not authenticated, the identity
// in one no more than the head, and a head anyone added must not keep agentd from starting. agentd
// records the findings in its start's record, what it could not read first; verify finds the same,
// by itself. Only a ledger that cannot be read is an error.
func CheckPushed(ctx context.Context, c AnchorReader, l *ledger.Ledger, identity, chain string, now time.Time) ([]Finding, error) {
	ctx, cancel := context.WithTimeout(ctx, StartCheckTime)
	defer cancel()
	days := []string{now.UTC().AddDate(0, 0, -1).Format("2006-01-02"), now.UTC().Format("2006-01-02")}
	ours := func(id string) bool { return chain != "" && strings.HasPrefix(id, AnchorPrefix(chain)) }
	var ids []string
	if chain != "" {
		for _, day := range days {
			ids = append(ids, AnchorPrefix(chain)+day) // by ID, and first: a listing lags behind
		}
	}
	var unread []Finding
	listed, more, err := listAnchors(ctx, c, now.Add(-48*time.Hour))
	if err != nil {
		unread = append(unread, Finding{Why: "the anchor workflows could not be listed: " + Text(err.Error())})
	}
	var others []string
	for _, id := range listed {
		if !ours(id) && slices.Contains(days, AnchorDay(id)) {
			others = append(others, id)
		}
	}
	if room := max(0, StartCheckWorkflows-len(ids)); len(others) > room || more {
		left := len(others) - min(room, len(others))
		why := fmt.Sprintf("the start reads at most %d anchor workflows: %d more of today's and yesterday's not read",
			StartCheckWorkflows, left)
		if more {
			why += ", and more not listed"
		}
		unread = append(unread, Finding{Why: why, NotRead: max(1, left)})
		others = others[:min(room, len(others))]
	}
	heads, unreadable, notRead := ReadPushed(ctx, c, append(ids, others...), MaxAnchorEvents)
	unread = append(unread, notRead...)
	var mine []Pushed   // this chain's
	var highest *Pushed // of this agentd's, under any chain: this chain's are read first, and keep a tie
	for i, h := range heads {
		if ours(h.WorkflowID) {
			mine = append(mine, h)
		}
		if identity != "" && h.Agentd == identity && (highest == nil || h.Seq > highest.Seq) {
			highest = &heads[i]
		}
	}
	found := unread
	if highest != nil && !ours(highest.WorkflowID) {
		this := chain
		if this == "" {
			this = "(none: the audit log has no records)"
		}
		found = append(found, Finding{Pushed: *highest, Why: fmt.Sprintf("the highest head this agentd pushed since yesterday "+
			"is record %d of chain %q, not of this ledger's chain %q: the ledger was replaced while agentd was down, or "+
			"someone signalled that head", highest.Seq, Text(AnchorChainOf(highest.WorkflowID)), this)})
	}
	err = l.Read(ctx, func(q *db.Queries) error {
		notHeld, err := NotHeld(ctx, q, mine)
		found = append(found, notHeld...)
		return err
	})
	if err != nil && ctx.Err() == nil {
		return nil, err
	} else if err != nil {
		found = append(found, Finding{Why: "out of time: the heads of this chain not checked against the ledger", NotRead: 1})
	}
	found = append(found, OutOfOrder(mine)...)
	for _, f := range unreadable {
		if ours(f.WorkflowID) { // another chain's says nothing of whose it is
			found = append(found, f)
		}
	}
	return found, nil
}

// listAnchors lists the anchor workflows started since since, one page, where Temporal can narrow by
// start time, or all of them where it cannot; more says the page was not the last.
func listAnchors(ctx context.Context, c AnchorReader, since time.Time) ([]string, bool, error) {
	prefix := fmt.Sprintf("WorkflowId STARTS_WITH '%s'", AnchorIDPrefix)
	resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{PageSize: 1000,
		Query: prefix + fmt.Sprintf(" AND StartTime > '%s'", since.UTC().Format(time.RFC3339))})
	if err != nil {
		if resp, err = c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{PageSize: 1000, Query: prefix}); err != nil {
			return nil, false, err
		}
	}
	ids := make([]string, 0, len(resp.GetExecutions()))
	for _, e := range resp.GetExecutions() {
		ids = append(ids, e.GetExecution().GetWorkflowId())
	}
	return ids, len(resp.GetNextPageToken()) > 0, nil
}

// AnchorDay is the UTC day an anchor workflow's ID names, as 2006-01-02; "" for an ID without one.
func AnchorDay(workflowID string) string {
	if i := len(workflowID) - len("2006-01-02"); i > len(AnchorIDPrefix) && workflowID[i-1] == '-' {
		if _, err := time.Parse("2006-01-02", workflowID[i:]); err == nil {
			return workflowID[i:]
		}
	}
	return ""
}

// Head is the record h names.
func (h AnchorHead) Head() Head { return Head{Seq: h.Seq, Sha256: h.Sha256} }

// Signaler is the part of Temporal's client the anchors use.
type Signaler interface {
	SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, signalArg any,
		options client.StartWorkflowOptions, workflow any, workflowArgs ...any) (client.WorkflowRun, error)
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
}

// AnchorHead is what agentd signals to an anchor workflow: the chain's head, and the agentd that
// pushed it (Identity). Who that is, the ledger does not say: a ledger rewritten from its first
// record, or replaced, has a chain with a new key, and the old chain's workflows still hold heads
// this agentd pushed. Signals from before agentd named itself have no agentd.
type AnchorHead struct {
	Seq    int64  `json:"seq"`
	Sha256 string `json:"sha256"`
	Agentd string `json:"agentd,omitempty"`
}

// Identity names an agentd in the heads it pushes: the first 16 hex characters of the sha256 of its
// Worker's task queue and its data directory, from its configuration. Two agentds on one namespace
// have task queues of their own (their Workers would take each other's runs otherwise), and two on
// one host data directories of their own; a ledger put in another's place keeps the agentd's.
func Identity(taskQueue, dataDir string) string {
	sum := sha256.Sum256([]byte("agentos-agentd/1\x00" + taskQueue + "\x00" + dataDir))
	return hex.EncodeToString(sum[:8])
}

// Anchors pushes the chain's head to Temporal whenever it has moved: at most every Every, and once
// more at shutdown, after the last records (Push).
type Anchors struct {
	Ledger   *ledger.Ledger
	Temporal Signaler
	Identity string // this agentd's (Identity), in each head it pushes
	Every    time.Duration
	OnError  func(error) // a push that failed: the next tries again
	// OnMisplaced hears of a day's anchor workflow that is not agentd's: of another type or task
	// queue, made by someone else before agentd's first push of the day, or continued as another.
	// The heads go to it all the same, and agentctl audit verify reports it.
	OnMisplaced func(error)

	mu        sync.Mutex
	pushed    Head
	chain     string
	described string // the anchor workflow whose type and task queue were looked at
}

// Run pushes the head every Every, if it has moved, until ctx ends.
func (a *Anchors) Run(ctx context.Context) {
	t := time.NewTicker(a.Every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.Push(ctx)
		}
	}
}

// Push pushes the chain's head, if it has moved since the last push: only while the chain ends
// where this process left it (tip.go).
func (a *Anchors) Push(ctx context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()
	t := tipOf(a.Ledger)
	head, err := t.head(ctx, a.Ledger)
	switch {
	case err != nil:
		a.fail(err)
		return
	case head == Head{} || head == a.pushed:
		return
	}
	if a.chain == "" {
		first, err := a.Ledger.Q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: 1, Limit: 1})
		if err != nil || len(first) == 0 {
			a.fail(cmp.Or(err, errors.New("the audit log has no first record")))
			return
		}
		a.chain = AnchorChain(first[0].Sha256)
	}
	id := AnchorWorkflowID(a.chain, ledger.Clock())
	signal := AnchorHead{Seq: head.Seq, Sha256: head.Sha256, Agentd: a.Identity}
	_, err = a.Temporal.SignalWithStartWorkflow(ctx, id, AnchorSignal, signal, client.StartWorkflowOptions{
		ID: id, TaskQueue: AnchorTaskQueue, WorkflowExecutionTimeout: AnchorTimeout,
	}, AnchorWorkflowType)
	if err != nil {
		a.fail(err)
		return
	}
	a.pushed = head
	t.mu.Lock()
	t.pushed = head
	t.mu.Unlock()
	if a.described != id {
		a.describe(ctx, id)
	}
}

// describe looks, once a day, at what the day's anchor workflow is: SignalWithStart signals the
// workflow with its ID whatever its type, and one made before agentd's first push, or continued as
// another since, holds the heads where verify does not read them.
func (a *Anchors) describe(ctx context.Context, id string) {
	d, err := a.Temporal.DescribeWorkflowExecution(ctx, id, "")
	if err != nil {
		a.fail(err) // looked at again with the next push
		return
	}
	a.described = id
	info := d.GetWorkflowExecutionInfo()
	if typ, queue := info.GetType().GetName(), info.GetTaskQueue(); typ != AnchorWorkflowType || queue != AnchorTaskQueue {
		if a.OnMisplaced != nil {
			a.OnMisplaced(fmt.Errorf("the anchor workflow %s is a %q on task queue %q, not agentd's: the heads pushed to it "+
				"are not where agentctl audit verify reads them", id, typ, queue))
		}
	}
}

func (a *Anchors) fail(err error) {
	if a.OnError != nil {
		a.OnError(err)
	}
}
