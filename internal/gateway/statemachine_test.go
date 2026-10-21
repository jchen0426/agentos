package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/githubtool"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tools"
)

// The effect protocol as a state machine (design doc, sections 6 and 10). Each seed walks a few
// writes through what can happen to them, in an order it draws: calls with and without an
// approval rule; approvals, rejections, edits and the clock running out; claims, and what the
// tools do around their proceed (a request that takes effect, a refusal, a denial, ErrNotSent, no
// answer, a write of two requests whose second does nothing); an agentd that dies holding a
// claim, recovery at start and on a call; lookups, before and after Settle; a person's
// reconciliation; cancels. A fake outside world (statemachine_world_test.go) records which
// requests went out and which took effect, and after every step the walk checks agentd's ledger
// against it:
//
//  1. No request of a write the policy wants approved goes out without an approval a person
//     approved, which moved the write on (APPROVED and used); and what goes out of a write with an
//     approval is the payload that person saw.
//  2. No new request of such a write goes out at or after its approval's expires_at: only a
//     resend, under the same key, of a write that may have taken effect (one in flight).
//  3. A write the ledger says never happened (ABANDONED, DRY_RUN, DENIED, FAILED, or its approval
//     EXPIRED or REJECTED) never took effect, nor may have, as far as agentd was told; one it says
//     SUCCEEDED did.
//  4. maybe_sent is set while a request of the write went out and neither a tool's answer, a
//     lookup nor a person has shown that the write did not take effect; a write that may have is
//     never held for approval.
//  5. A write takes effect at most once: for an idempotent tool, once per key.
//  6. The report's approval lines are the ledger's: each approval as it ended, an approved write
//     as it went; and an approval that expired after a request of its write went out says so.
//
// An approval is also only ever used (it moves its write to PENDING) before it expires; and no
// request of a run's write goes out once the run is cancelled (proceed checks it in the same
// statement that lets the request go out).
//
// A failing seed prints its walk. Walk one alone, with its log, with
//
//	go test ./internal/gateway -run TestEffectProtocolStateMachine -statemachine.seed=N -v
//
// and once the bug is fixed, keep the seed in regressionSeeds.
var (
	smSeedFlag  = flag.Uint64("statemachine.seed", 0, "walk only this seed of TestEffectProtocolStateMachine, and log it")
	smSeedsFlag = flag.Int("statemachine.seeds", 0, "walk seeds 1 to N (and regressionSeeds)")
)

// regressionSeeds failed once, beyond the seeds walked by default: they are walked on every run,
// with seeds 1 to smSeeds. A seed names a walk of this file's code as it is: change what a walk
// draws, and find them again (docs/statemachine-notes.md says how).
var regressionSeeds = []uint64{
	1189, 1596, // agentd, starting, abandoned a cancelled run's write that may have taken effect
}

const (
	smSeeds = 200 // seeds walked by default
	smSteps = 100 // a walk's steps, besides those taken while a tool waits
	smDepth = 2   // how deep steps nest in steps taken while a tool waits
)

func TestEffectProtocolStateMachine(t *testing.T) {
	seeds := slices.Clone(regressionSeeds)
	n := smSeeds
	if *smSeedsFlag > 0 {
		n = *smSeedsFlag
	}
	for s := range n {
		seeds = append(seeds, uint64(s+1))
	}
	if *smSeedFlag != 0 {
		seeds = []uint64{*smSeedFlag}
	}
	clock := ledger.Clock
	t.Cleanup(func() { ledger.Clock = clock })
	dir := t.TempDir()
	failed := 0
	for _, seed := range seeds {
		m := newMachine(t, seed, filepath.Join(dir, fmt.Sprintf("ledger-%d.db", seed)))
		m.walk()
		m.stop()
		switch {
		case m.violation != "":
			t.Errorf("seed %d: %s\n\nThe walk:\n%s\nThe ledger:\n%s", seed, m.violation, m.log.String(), m.dump)
			if failed++; failed == 3 {
				t.Fatal("stopping after three failing seeds")
			}
		case *smSeedFlag != 0:
			t.Logf("seed %d:\n%s", seed, m.log.String())
		}
	}
}

// mix is how often a walk does what: drawn for each walk, so that walks differ in what they do much
// of (swarm testing): one dies often, another changes its policy, another has tools that cannot
// tell what came of their requests.
type mix struct {
	steps, nested []int // weights of act's steps: at the top, and while a tool waits
	attempts      []int // weights of the attempt kinds
	waits         int   // percent: a tool's wait is a pause for steps
	recovers      int   // percent: an agentd that starts recovers what the last one claimed
	newPolicy     int   // percent: an agentd that starts has a tool's policy changed
	flipLive      int   // percent: an agentd that starts can reach the outside world when the last could not, or the other way
}

// drawMix draws a walk's mix: each weight the usual one, none, or more.
func (m *machine) drawMix() mix {
	scale := func(base ...int) []int {
		out := make([]int, len(base))
		for i, w := range base {
			out[i] = w * []int{0, 1, 1, 3}[m.rng.IntN(4)]
		}
		return out
	}
	x := mix{steps: scale(40, 16, 12, 4, 8, 2, 3, 1, 3), nested: scale(20, 14, 20, 4, 4, 7, 7, 0, 0),
		attempts: scale(30, 24, 6, 8, 12, 5, 5, 3)}
	// Every walk calls, decides and moves the clock; its tools send something.
	x.steps[0], x.steps[1], x.steps[2] = max(x.steps[0], 40), max(x.steps[1], 16), max(x.steps[2], 12)
	x.nested[2] = max(x.nested[2], 20)
	x.attempts[0], x.attempts[1] = max(x.attempts[0], 30), max(x.attempts[1], 24)
	x.waits = []int{20, 35, 60}[m.rng.IntN(3)]
	x.recovers = []int{50, 80, 100}[m.rng.IntN(3)]
	x.newPolicy = []int{10, 30, 60}[m.rng.IntN(3)]
	x.flipLive = []int{0, 10, 30}[m.rng.IntN(3)]
	return x
}

// machine is one walk.
type machine struct {
	mix
	world // guarded by mu, as are the effects' models

	t         *testing.T
	ctx       context.Context
	seed      uint64
	rng       *rand.Rand
	path      string // the ledger every agentd opens
	runs      []*runModel
	toolNames []string // the writes this walk calls
	ns        []int    // the arguments it calls them with
	idemFinds bool     // the idempotent tool can be looked up

	cur        *incarnation // the agentd that is up
	incs       []*incarnation
	fx         map[string]*effectModel          // by effect ID
	decisions  map[string]Decision              // the decision a person got recorded first, by approval ID
	decided    map[string][]Decision            // decisions that went through, by approval ID: replayed as they were
	resolved   map[string][]Resolution          // findings that went through, by effect ID
	calling    map[*incarnation]map[string]bool // effects an agentd has a call of, which a nested call would join
	recovering bool                             // recovery at start runs, in goroutines of its own: nothing may draw
	depth      int
	step       int
	keys       int

	logMu     sync.Mutex
	log       strings.Builder
	names     map[string]string // effects, approvals and patches, as the log names them
	named     map[string]int    // how many approvals and patches the log has named
	violation string            // the first broken invariant
	dump      string            // the ledger when it broke
}

// incarnation is one agentd process: its own connection to the ledger, closed when it dies.
type incarnation struct {
	name   string
	led    *ledger.Ledger
	g      *Gateway
	tools  map[string]tools.Writer
	policy map[string]toolPolicy
	live   bool // its tools can reach the outside world
	dead   bool
}

// toolPolicy is what the policy says of one write.
type toolPolicy struct {
	require bool // require_approval
	ttl     time.Duration
	max     int    // max_per_run
	stated  string // the class it states: the tool's own, or unsafe
}

type runModel struct {
	id        string
	dryRun    bool
	cancelled bool
	ended     bool // its workflow ended: nobody calls for it any more
}

// effectModel is what the walk knows of an effect: what the outside world and the tools told
// agentd, rather than what agentd made of it.
type effectModel struct {
	tool   string
	run    *runModel
	dedupe bool // its tool's outside world makes one thing per key
	finder bool // its tool can look it up
	// inFlight: a request of it went out, and nothing since showed that the write did not and will
	// not take effect: neither the answer of the attempt that sent it, nor a lookup that found
	// nothing once nothing sent could take effect any more, nor a person. What maybe_sent must say
	// at least; and what makes a request that goes out again under the same key no new one.
	inFlight  bool
	lastFinal time.Time // when the last request that could make the write went out
}

func newMachine(t *testing.T, seed uint64, path string) *machine {
	m := &machine{
		t: t, ctx: context.Background(), seed: seed, rng: rand.New(rand.NewPCG(seed, 0x5eed)), path: path,
		fx: map[string]*effectModel{}, decisions: map[string]Decision{}, decided: map[string][]Decision{},
		resolved: map[string][]Resolution{}, calling: map[*incarnation]map[string]bool{}, names: map[string]string{},
		named: map[string]int{},
	}
	m.now = time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	ledger.Clock = m.Now
	m.mix = m.drawMix()
	// A walk calls few writes, so that each goes far: one to three tools, one or two sets of
	// arguments each, in a run; another run starts now and then (newRun).
	all := []string{"sm.idempotent", "sm.lookup", "sm.unsafe"}
	m.rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	m.toolNames = all[:1+m.rng.IntN(3)]
	slices.Sort(m.toolNames)
	m.ns = []int{1}
	if m.rng.IntN(100) < 30 {
		m.ns = append(m.ns, 2)
	}
	m.idemFinds = m.rng.IntN(2) == 0
	pol := map[string]toolPolicy{}
	for _, name := range all {
		pol[name] = m.drawPolicy()
	}
	m.cur = m.start(pol, m.rng.IntN(100) < 92)
	m.logf("seed %d: tools %v with arguments %v; %s, live %v, policy %s", seed, m.toolNames, m.ns,
		m.cur.name, m.cur.live, m.policyText(m.cur.policy))
	m.newRun()
	return m
}

// newRun starts a run (now and then a dry run), up to five.
func (m *machine) newRun() {
	if len(m.runs) == 5 {
		m.logf("no more runs start")
		return
	}
	r := &runModel{id: fmt.Sprintf("wf%d", len(m.runs)+1), dryRun: m.rng.IntN(100) < 8}
	var dry int64
	if r.dryRun {
		dry = 1
	}
	if err := m.cur.led.Q.InsertRun(m.ctx, db.InsertRunParams{WorkflowID: r.id, Task: "security-audit", ParamsJson: "{}",
		BudgetLimitMicros: 1, DryRun: dry, CreatedAt: ledger.Now()}); err != nil {
		m.fatal(err)
	}
	m.runs = append(m.runs, r)
	m.logf("%s starts%s", r.id, map[bool]string{true: ", a dry run"}[r.dryRun])
}

func (m *machine) drawPolicy() toolPolicy {
	p := toolPolicy{require: m.rng.IntN(100) < 75, max: 1}
	p.ttl = []time.Duration{2 * time.Minute, 10 * time.Minute, time.Hour, 24 * time.Hour}[m.choose(3, 3, 3, 1)]
	if m.rng.IntN(100) < 20 {
		p.max = 2
	}
	if m.rng.IntN(100) < 12 {
		p.stated = policy.EffectUnsafe
	}
	return p
}

// start starts an agentd: a new incarnation, on the same ledger.
func (m *machine) start(pol map[string]toolPolicy, live bool) *incarnation {
	inc := &incarnation{name: fmt.Sprintf("agentd%d", len(m.incs)+1), policy: pol, live: live}
	led, err := ledger.Open(m.ctx, m.path)
	if err != nil {
		m.fatal(err)
	}
	inc.led, inc.tools = led, m.smTools(inc)
	reg := tools.Registry{}
	for _, w := range inc.tools {
		reg.Add(w)
	}
	inc.g = &Gateway{Ledger: led, Tools: reg, Incarnation: inc.name, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		wait: m.waitFor(inc)}
	m.incs = append(m.incs, inc)
	m.calling[inc] = map[string]bool{}
	return inc
}

func (m *machine) stop() {
	for _, inc := range m.incs {
		if !inc.dead {
			inc.led.Close()
		}
	}
}

// walk takes the walk's steps, checking after each, and then lets whatever may still take
// effect take effect, or never.
func (m *machine) walk() {
	for m.step = 1; m.step <= smSteps && m.violation == ""; m.step++ {
		m.act(false)
		m.check()
	}
	if m.violation == "" {
		m.advance(smSettle)
		m.logf("the end, %s later", smSettle)
		m.check()
	}
}

// act takes one step. Nested, it is taken while a tool waits: another call, a decision, the
// clock, a cancel, agentd dying.
func (m *machine) act(nested bool) {
	weights := m.steps
	if nested {
		weights = m.nested
	}
	switch m.choose(weights...) {
	case 0:
		m.call()
	case 1:
		m.decide()
	case 2:
		d := []time.Duration{5 * time.Second, 45 * time.Second, 2 * time.Minute, 4 * time.Minute, 11 * time.Minute,
			70 * time.Minute, 26 * time.Hour}[m.choose(16, 16, 18, 16, 14, 6, 2)]
		m.advance(d)
		m.logf("%s pass", d)
	case 3:
		m.sweep()
	case 4:
		m.reconcile()
	case 5:
		m.cancel()
	case 6:
		m.restart()
	case 7:
		m.end()
	case 8:
		m.newRun()
	}
}

// choose draws an index of weights, by weight.
func (m *machine) choose(weights ...int) int {
	total := 0
	for _, w := range weights {
		total += w
	}
	n := m.rng.IntN(total)
	for i, w := range weights {
		if n < w {
			return i
		}
		n -= w
	}
	panic("unreachable")
}

// pause is where a tool waits on the outside world: the walk may take other steps there, or
// have the tool's agentd die. Not during recovery at start, which runs in goroutines of its own.
func (m *machine) pause(inc *incarnation, where string) {
	if m.recovering || m.violation != "" || m.depth >= smDepth || m.rng.IntN(100) >= m.waits {
		return
	}
	m.depth++
	defer func() { m.depth-- }()
	m.logf("%s pauses %s", inc.name, where)
	for range 1 + m.rng.IntN(2) {
		if inc.dead || m.violation != "" {
			return
		}
		m.act(true)
		m.check()
	}
}

// callSpec is a call a workflow may make.
type callSpec struct {
	run  *runModel
	tool string
	args map[string]any
	name string
}

// call makes a call of a write, as a workflow does: one of the walk's, or the call an edit
// asks for (the edited approval's call with the reviewer's patch). Mostly of a write that is
// not over yet: one waiting for approval, being sent, or for a person; or not made yet.
func (m *machine) call() {
	var specs []callSpec
	for _, r := range m.runs {
		if r.ended {
			continue
		}
		for _, tool := range m.toolNames {
			for _, n := range m.ns {
				specs = append(specs, callSpec{run: r, tool: tool, name: fmt.Sprintf("%s/%s/%d", r.id, tool[3:], n),
					args: map[string]any{"n": n, "patch_ref": "sha256:" + sha256Hex(fmt.Appendf(nil, "patch %d", n))}})
			}
		}
	}
	for _, a := range m.approvals() {
		if (a.Status != approvalEdited && a.Status != approvalSuperseded) || m.run(a.WorkflowID).ended {
			continue
		}
		args, err := ledger.DecodeObject([]byte(a.ArgsJson))
		if err != nil {
			m.fatal(err)
		}
		args["patch_ref"] = a.EditedPatchBlob.String
		specs = append(specs, callSpec{run: m.run(a.WorkflowID), tool: a.Tool, args: args,
			name: m.name(a.EffectID) + "+" + m.name(a.EditedPatchBlob.String)})
	}
	if len(specs) == 0 {
		m.logf("no run calls any more")
		m.newRun()
		return
	}
	weights := make([]int, len(specs))
	for i, s := range specs {
		canon, err := encodeCanonical(s.args)
		if err != nil {
			m.fatal(err)
		}
		switch e, err := m.cur.led.Q.GetEffect(m.ctx, sha256Hex([]byte(s.run.id), []byte(s.tool), canon)); {
		case errors.Is(err, sql.ErrNoRows):
			weights[i] = 4
		case err != nil:
			m.fatal(err)
		case e.Status == effectPending, e.Status == effectAwaitingApproval, e.Status == effectUnknown:
			weights[i] = 12
		default:
			weights[i] = 1
		}
	}
	spec := specs[m.choose(weights...)]
	inc := m.cur
	row, err := inc.led.Q.GetRun(m.ctx, spec.run.id)
	if err != nil {
		m.fatal(err)
	}
	t := inc.tools[spec.tool]
	pol := inc.policy[spec.tool]
	class, err := effectClass(t, pol.stated)
	if err != nil {
		m.fatal(err)
	}
	var rule *approvalRule
	if pol.require && row.DryRun == 0 && t.Live() {
		rule = &approvalRule{ttl: pol.ttl}
	}
	canon, err := encodeCanonical(spec.args)
	if err != nil {
		m.fatal(err)
	}
	id := sha256Hex([]byte(spec.run.id), []byte(spec.tool), canon)
	m.model(id, spec)
	if m.calling[inc][id] {
		m.logf("call %s: %s has a call of it already, which the call would join", m.name(id), inc.name)
		return
	}
	m.calling[inc][id] = true
	defer delete(m.calling[inc], id)
	res, err := inc.g.callTool(m.ctx, activity.Info{}, row, t, class, pol.max, rule, spec.args, canon, faults.Effect{})
	m.logf("call %s on %s (class %s, approval %v) → %s", m.name(id), inc.name, class, rule != nil, m.outcome(res, err))
}

// model is the walk's model of the effect of a call, which it names.
func (m *machine) model(id string, spec callSpec) *effectModel {
	m.mu.Lock()
	defer m.mu.Unlock()
	if fx, ok := m.fx[id]; ok {
		return fx
	}
	t := m.cur.tools[spec.tool]
	_, finder := t.(tools.Finder)
	fx := &effectModel{tool: spec.tool, run: spec.run, dedupe: spec.tool == "sm.idempotent", finder: finder}
	m.fx[id] = fx
	m.logMu.Lock()
	m.names[id] = spec.name
	m.logMu.Unlock()
	return fx
}

func (m *machine) effect(id string) *effectModel {
	m.mu.Lock()
	defer m.mu.Unlock()
	fx, ok := m.fx[id]
	if !ok {
		panic("an effect the walk never asked for: " + id)
	}
	return fx
}

func (m *machine) run(id string) *runModel {
	for _, r := range m.runs {
		if r.id == id {
			return r
		}
	}
	panic("no run " + id)
}

// execute is what the walk's tools do to send a write: it draws what this attempt does outside.
func (m *machine) execute(ctx context.Context, t *smTool, payload json.RawMessage, key string, proceed tools.Proceed) (map[string]any, error) {
	inc, id := t.inc, payloadEffect(payload)
	if m.recovering {
		m.fail("recovery at start sent a request of %s: it only looks", m.name(id))
		return nil, tools.ErrNotSent
	}
	fx := m.effect(id)
	m.mu.Lock()
	before := fx.inFlight
	m.mu.Unlock()
	// The attempt did nothing outside: what agentd knew before it still stands.
	nothing := func() {
		if !inc.dead {
			m.mu.Lock()
			fx.inFlight = before
			m.mu.Unlock()
		}
	}
	kind := attemptKind(m.choose(m.attempts...))
	two := kind != attemptUnknownBefore && kind != attemptNotSent && kind != attemptDenied && m.rng.IntN(100) < 30
	m.logf("%s sends %s: %s%s", inc.name, m.name(id), attemptNames[kind], map[bool]string{true: ", after a request before it"}[two])
	m.pause(inc, "before its first request")
	if inc.dead {
		return nil, errDied
	}
	switch kind {
	case attemptDenied:
		nothing()
		return nil, &tools.DeniedError{Reason: "the walk denies it"}
	case attemptNotSent:
		nothing()
		return nil, tools.ErrNotSent
	case attemptUnknownBefore:
		return nil, errors.New("timed out before sending anything")
	}
	if two {
		if err := proceed(ctx); err != nil {
			m.logf("%s: proceed: %v", m.name(id), err)
			nothing()
			return nil, err
		}
		m.send(inc, t, id, key, payload, false, 0, true)
		m.pause(inc, "between its requests")
		if inc.dead {
			return nil, errDied
		}
	}
	if err := proceed(ctx); err != nil {
		m.logf("%s: proceed: %v", m.name(id), err)
		nothing()
		return nil, err
	}
	switch kind {
	case attemptOK:
		m.send(inc, t, id, key, payload, true, 0, true)
	case attemptUnknown:
		switch m.choose(30, 40, 30) {
		case 0:
			m.send(inc, t, id, key, payload, true, 0, true)
		case 1:
			m.send(inc, t, id, key, payload, true, []time.Duration{30 * time.Second, 90 * time.Second, smLate}[m.rng.IntN(3)], true)
		default:
			m.send(inc, t, id, key, payload, true, 0, false)
		}
	default:
		m.send(inc, t, id, key, payload, true, 0, false)
	}
	m.pause(inc, "for the answer")
	if inc.dead {
		return nil, errDied
	}
	if t.dedupe && kind != attemptOK && kind != attemptUnknown && m.keyLanded(key) {
		// The outside world answers a key it acted on with what it made.
		kind = attemptOK
		m.logf("%s: its key took effect already: ok", m.name(id))
	}
	switch kind {
	case attemptOK:
		return map[string]any{"id": 7}, nil
	case attemptUnknown:
		return nil, errors.New("connection reset")
	case attemptRetryable:
		nothing()
		return nil, &tools.RefusedError{Reason: "rate limited", Retryable: true}
	case attemptRefused:
		nothing()
		return nil, &tools.RefusedError{Reason: "unprocessable"}
	}
	nothing()
	return nil, &tools.ConflictError{Reason: "something else holds its name"}
}

func (m *machine) keyLanded(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.ContainsFunc(m.sent, func(r *sentRequest) bool { return r.key == key && r.final && r.landed(m.now) })
}

// send sends one request of an effect, after checking that it may go out (invariants 1 and 2).
func (m *machine) send(inc *incarnation, t *smTool, id, key string, payload json.RawMessage, final bool, late time.Duration, lands bool) {
	now := m.Now()
	m.checkRequest(inc, t, id, key, payload, now)
	r := &sentRequest{effectID: id, key: key, final: final, at: now}
	if lands {
		r.landsAt = now.Add(late)
	}
	fx := m.effect(id)
	m.mu.Lock()
	m.sent = append(m.sent, r)
	fx.inFlight = true
	if final {
		fx.lastFinal = now
	}
	m.mu.Unlock()
	what := "the request that makes it"
	if !final {
		what = "a request before it"
	}
	switch {
	case !lands:
		m.logf("%s: %s goes out, and never takes effect", m.name(id), what)
	case late == 0:
		m.logf("%s: %s goes out, and takes effect", m.name(id), what)
	default:
		m.logf("%s: %s goes out, and takes effect in %s", m.name(id), what, late)
	}
}

// checkRequest checks invariants 1 and 2 for a request of an effect about to go out.
func (m *machine) checkRequest(inc *incarnation, t *smTool, id, key string, payload json.RawMessage, now time.Time) {
	fx := m.effect(id)
	if fx.run.cancelled {
		m.fail("a request of %s went out after its run was cancelled", m.name(id))
	}
	pol := inc.policy[t.name]
	rule := pol.require && inc.live && !fx.run.dryRun
	a, err := inc.led.Q.GetApprovalByEffect(m.ctx, id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if rule {
			m.fail("1: a request of %s went out without an approval, under a policy that wants one", m.name(id))
		}
		return
	case err != nil:
		m.fatal(err)
	}
	d, decided := m.decisions[a.ApprovalID]
	switch {
	case a.Status != approvalApproved || !a.UsedAt.Valid:
		m.fail("1: a request of %s went out while its approval %s is %s (used: %v)", m.name(id), m.name(a.ApprovalID), a.Status, a.UsedAt.Valid)
	case !decided || d.Kind != DecisionApprove:
		m.fail("1: a request of %s went out with approval %s, which no person approved", m.name(id), m.name(a.ApprovalID))
	case sha256Hex(payload) != a.PayloadSha256 || d.PayloadSHA256 != a.PayloadSha256:
		m.fail("1: the payload of %s that went out (sha256 %s) is not the one approved (%s), which the person saw as %s",
			m.name(id), sha256Hex(payload), a.PayloadSha256, d.PayloadSHA256)
	}
	if !rule {
		return
	}
	e, err := inc.led.Q.GetEffect(m.ctx, id)
	if err != nil {
		m.fatal(err)
	}
	expires, err := time.Parse(time.RFC3339Nano, a.ExpiresAt)
	if err != nil {
		m.fatal(err)
	}
	m.mu.Lock()
	resend := t.dedupe && key == e.IdempotencyKey && fx.inFlight
	m.mu.Unlock()
	if !now.Before(expires) && !resend {
		m.fail("2: a new request of %s went out at %s, though its approval %s expired at %s", m.name(id),
			now.Format(time.RFC3339Nano), m.name(a.ApprovalID), a.ExpiresAt)
	}
}

// find is the lookup of the walk's tools: it finds what the effect's requests made by now. A
// lookup that finds nothing once nothing sent can take effect any more shows the write never
// took effect.
func (m *machine) find(t *smTool, payload json.RawMessage) (map[string]any, bool, error) {
	inc, id := t.inc, payloadEffect(payload)
	m.pause(inc, "looking it up")
	if inc.dead {
		return nil, false, errDied
	}
	if !m.recovering && m.rng.IntN(100) < 5 {
		m.logf("%s: the lookup fails", m.name(id))
		return nil, false, errors.New("the lookup failed")
	}
	m.mu.Lock()
	fx := m.fx[id]
	found := m.made(id, t.dedupe) > 0
	settled := !m.now.Before(fx.lastFinal.Add(smLate))
	if !found && settled {
		fx.inFlight = false
	}
	m.mu.Unlock()
	m.logf("%s: %s looks it up: found %v (nothing sent can take effect any more: %v)", m.name(id), inc.name, found, settled)
	if found {
		return map[string]any{"id": 7}, true, nil
	}
	return nil, false, nil
}

// waitFor is how an agentd's recovery waits for a request that may still take effect: the walk
// may take steps meanwhile, and then the clock moves on.
func (m *machine) waitFor(inc *incarnation) func(ctx context.Context, d time.Duration) error {
	return func(ctx context.Context, d time.Duration) error {
		m.pause(inc, fmt.Sprintf("waiting %s for what may still take effect", d))
		if inc.dead {
			return errDied
		}
		m.advance(d)
		m.logf("%s waited %s", inc.name, d)
		return nil
	}
}

// decide has a person decide an approval: mostly one that waits, sometimes one decided already;
// sometimes having seen another payload; sometimes sending again a decision that went through.
func (m *machine) decide() {
	all := m.approvals()
	var waiting []db.Approval
	for _, a := range all {
		if a.Status == approvalPending {
			waiting = append(waiting, a)
		}
	}
	if len(all) == 0 || len(waiting) == 0 && m.rng.IntN(100) < 85 {
		m.logf("decide: nothing waits for a decision")
		return
	}
	a := all[m.rng.IntN(len(all))]
	if len(waiting) > 0 && m.rng.IntN(100) < 90 {
		a = waiting[m.rng.IntN(len(waiting))]
	}
	d := Decision{ApprovalID: a.ApprovalID, PayloadSHA256: a.PayloadSha256, Key: m.key(), By: []string{"alice", "bob"}[m.rng.IntN(2)]}
	switch m.choose(55, 25, 20) {
	case 0:
		d.Kind = DecisionApprove
	case 1:
		d.Kind, d.Reason = DecisionReject, "not this one"
	default:
		d.Kind, d.Patch = DecisionEdit, fmt.Appendf(nil, "--- a/app/db.py\n+++ b/app/db.py\n@@ -1 +1 @@\n-old\n+new %d\n", 1+m.rng.IntN(3))
	}
	if m.rng.IntN(100) < 8 {
		d.PayloadSHA256 = sha256Hex([]byte("another payload"))
	}
	if prev := m.decided[a.ApprovalID]; len(prev) > 0 && m.rng.IntN(100) < 15 {
		d = prev[m.rng.IntN(len(prev))]
	}
	d.RunClosed = m.run(a.WorkflowID).ended
	got, err := m.cur.g.Decide(m.ctx, d)
	what := d.Kind
	if d.PayloadSHA256 != a.PayloadSha256 {
		what += ", having seen another payload"
	}
	if err != nil {
		m.logf("%s %s %s (%s): %v", d.By, what, m.name(a.ApprovalID), m.name(a.EffectID), m.redact(err.Error()))
		return
	}
	m.logf("%s %s %s (%s): it is %s", d.By, what, m.name(a.ApprovalID), m.name(a.EffectID), got.Status)
	if _, ok := m.decisions[a.ApprovalID]; !ok {
		m.decisions[a.ApprovalID] = d
	}
	if !slices.ContainsFunc(m.decided[a.ApprovalID], func(p Decision) bool { return p.Key == d.Key }) {
		m.decided[a.ApprovalID] = append(m.decided[a.ApprovalID], d)
	}
}

// reconcile has a person settle an effect nobody could: by what the outside world holds now.
// One whose tool cannot look it up the person settles only once nothing sent can take effect any
// more; for one it can, agentd says when it is too early.
func (m *machine) reconcile() {
	var unknown []db.Effect
	for _, id := range m.effectIDs() {
		if e, err := m.cur.led.Q.GetEffect(m.ctx, id); err == nil && e.Status == effectUnknown {
			unknown = append(unknown, e)
		}
	}
	if len(unknown) == 0 {
		m.logf("reconcile: nothing to reconcile")
		return
	}
	e := unknown[m.rng.IntN(len(unknown))]
	fx := m.effect(e.EffectID)
	m.mu.Lock()
	made, pending := m.made(e.EffectID, fx.dedupe) > 0, m.pending(e.EffectID)
	m.mu.Unlock()
	r := Resolution{EffectID: e.EffectID, Key: m.key(), Executed: made}
	switch {
	case made && fx.finder && m.rng.IntN(2) == 0:
		r.Lookup = true
	case made:
		r.ResultJSON = `{"id": 7}`
	case pending && !fx.finder:
		m.logf("reconcile %s: the person cannot tell yet", m.name(e.EffectID))
		return
	}
	replay := false
	if prev := m.resolved[e.EffectID]; len(prev) > 0 && m.rng.IntN(100) < 15 {
		r, replay = prev[m.rng.IntN(len(prev))], true
	}
	r.RunClosed = fx.run.ended
	status, _, err := m.cur.g.Reconcile(m.ctx, r)
	if err != nil {
		m.logf("a person finds %s executed %v: %v", m.name(e.EffectID), r.Executed, m.redact(err.Error()))
		return
	}
	m.logf("a person finds %s executed %v%s: it is %s", m.name(e.EffectID), r.Executed, map[bool]string{true: " (again)"}[replay], status)
	if !replay {
		m.resolved[e.EffectID] = append(m.resolved[e.EffectID], r)
		if !r.Executed {
			m.mu.Lock()
			fx.inFlight = false
			m.mu.Unlock()
		}
	}
}

// sweep is ExpireApprovals' round: the approvals nobody decided in time expire.
func (m *machine) sweep() {
	due, err := m.cur.led.Q.ListExpiredApprovals(m.ctx, ledger.Stamp(ledger.Clock()))
	if err != nil {
		m.fatal(err)
	}
	for _, a := range due {
		done, err := m.cur.g.expire(m.ctx, a)
		m.logf("%s expires %s (%s): %v %v", m.cur.name, m.name(a.ApprovalID), m.name(a.EffectID), done, err)
	}
	if len(due) == 0 {
		m.logf("%s: no approval is due to expire", m.cur.name)
	}
}

func (m *machine) cancel() {
	r := m.runs[m.rng.IntN(len(m.runs))]
	n, err := m.cur.led.Q.MarkRunCancelled(m.ctx, db.MarkRunCancelledParams{CancelledAt: valid(ledger.Now()), WorkflowID: r.id})
	if err != nil {
		m.fatal(err)
	}
	r.cancelled = true
	m.logf("%s is cancelled (%d)", r.id, n)
}

// end ends a run's workflow: nobody calls for it any more, and decisions and findings about it
// say so.
func (m *machine) end() {
	r := m.runs[m.rng.IntN(len(m.runs))]
	r.ended = true
	m.logf("%s's workflow ends", r.id)
}

// restart has the agentd that is up die, wherever it is, and another start on the same ledger:
// mostly with the same policy and tools, and mostly recovering at start (as the walk's mix says).
func (m *machine) restart() {
	old := m.cur
	old.dead = true
	if err := old.led.Close(); err != nil {
		m.fatal(err)
	}
	pol := maps.Clone(old.policy)
	live := old.live
	if m.rng.IntN(100) < m.newPolicy {
		name := m.toolNames[m.rng.IntN(len(m.toolNames))]
		pol[name] = m.drawPolicy()
	}
	if m.rng.IntN(100) < m.flipLive {
		live = !live
	}
	m.cur = m.start(pol, live)
	atStart := m.rng.IntN(100) < m.recovers
	m.logf("%s dies; %s starts, live %v, policy %s, recovering at start: %v", old.name, m.cur.name, live, m.policyText(pol), atStart)
	if !atStart {
		return
	}
	m.recovering = true
	err := m.cur.g.RecoverEffects(m.ctx)
	drained := m.cur.g.Drain(time.Minute)
	m.recovering = false
	if err != nil || !drained {
		m.t.Fatalf("recovery at start: %v, drained %v", err, drained)
	}
}

// check checks the invariants after a step.
func (m *machine) check() {
	if m.violation != "" {
		return
	}
	led := m.cur.led
	for _, id := range m.effectIDs() {
		e, err := led.Q.GetEffect(m.ctx, id)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		} else if err != nil {
			m.fatal(err)
		}
		a, err := led.Q.GetApprovalByEffect(m.ctx, id)
		hasApproval := err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			m.fatal(err)
		}
		fx := m.effect(id)
		m.mu.Lock()
		made, inFlight, wentOut := m.made(id, fx.dedupe), fx.inFlight, m.wentOut(id)
		m.mu.Unlock()
		never := slices.Contains([]string{effectAbandoned, effectDryRun, effectDenied, effectFailed}, e.Status) ||
			hasApproval && (a.Status == approvalExpired || a.Status == approvalRejected)
		switch {
		case made > 1:
			m.fail("5: %s took effect %d times", m.name(id), made)
		case never && made > 0:
			m.fail("3: %s took effect, but it is %s (approval: %s)", m.name(id), e.Status, m.approvalText(hasApproval, a))
		case never && inFlight:
			m.fail("3: %s may have taken effect, but it is %s (approval: %s)", m.name(id), e.Status, m.approvalText(hasApproval, a))
		case e.Status == effectSucceeded && made == 0:
			m.fail("3: %s is SUCCEEDED, but it never took effect", m.name(id))
		case e.Status == effectPending && inFlight && e.MaybeSent == 0:
			m.fail("4: %s may have taken effect, but maybe_sent is 0", m.name(id))
		case e.Status == effectAwaitingApproval && inFlight:
			m.fail("4: %s may have taken effect, but it awaits approval", m.name(id))
		case hasApproval && a.UsedAt.Valid && !before(a.UsedAt.String, a.ExpiresAt):
			m.fail("approval %s of %s was used at %s, at or after it expired at %s", m.name(a.ApprovalID), m.name(id), a.UsedAt.String, a.ExpiresAt)
		case hasApproval && a.Status == approvalExpired && a.UsedAt.Valid && wentOut && !strings.Contains(a.Reason.String, "A request of it went out at"):
			m.fail("6: approval %s of %s expired after a request went out, and its reason does not say so: %q", m.name(a.ApprovalID), m.name(id), a.Reason.String)
		}
		if m.violation != "" {
			m.dumpLedger()
			return
		}
	}
	for _, r := range m.runs {
		m.checkReport(r)
		if m.violation != "" {
			m.dumpLedger()
			return
		}
	}
}

// checkReport checks invariant 6: github.comment_commit's report shows each of the run's
// approvals as the ledger has it.
func (m *machine) checkReport(r *runModel) {
	led := m.cur.led
	cc := &githubtool.CommentCommit{Repos: map[string]githubtool.Repo{"o/r": {Owner: "o", Name: "r"}}, Blobs: led, Effects: RunRecords{led}}
	b, err := cc.Prepare(m.ctx, tools.Request{WorkflowID: r.id, EffectID: strings.Repeat("c", 64),
		Args: map[string]any{"repo": "o/r", "commit": strings.Repeat("a", 40), "outcome": "done"}})
	if err != nil {
		m.fatal(err)
	}
	var p struct{ Body string }
	if err := json.Unmarshal(b, &p); err != nil {
		m.fatal(err)
	}
	var got, want []string
	for line := range strings.SplitSeq(p.Body, "\n") {
		if strings.HasPrefix(line, "Approval of ") {
			got = append(got, line)
		}
	}
	all, err := led.RunApprovals(m.ctx, r.id)
	if err != nil {
		m.fatal(err)
	}
	for _, a := range all[:min(len(all), maxApprovalLines)] {
		e, err := led.Q.GetEffect(m.ctx, a.EffectID)
		if err != nil {
			m.fatal(err)
		}
		status := strings.ToLower(a.Status)
		if a.Status == approvalApproved {
			switch e.Status {
			case effectSucceeded, effectPending, effectAwaitingApproval: // sent, or on its way
			case effectFailed:
				status += ", failed"
			case effectUnknown:
				status += ", outcome unknown"
			default:
				status += ", not sent"
			}
		}
		want = append(want, fmt.Sprintf("Approval of %s: %s", a.Tool, status))
		if line := fmt.Sprintf("\n- %s, approval %s: %s", a.Tool, a.ApprovalID, a.Status); !strings.Contains(p.Body, line) {
			m.fail("6: the report of %s has no line %q", r.id, line)
		}
	}
	if !slices.Equal(got, want) {
		m.fail("6: the report of %s says\n\t%s\nbut the ledger has\n\t%s", r.id, strings.Join(got, "\n\t"), strings.Join(want, "\n\t"))
	}
}

// before reports whether ledger time a is before ledger time b.
func before(a, b string) bool {
	ta, erra := time.Parse(time.RFC3339Nano, a)
	tb, errb := time.Parse(time.RFC3339Nano, b)
	return erra == nil && errb == nil && ta.Before(tb)
}

// maxApprovalLines is githubtool's: the report names at most that many approvals outside its fence.
const maxApprovalLines = 20

// approvals lists the walk's approvals, in the order they were made.
func (m *machine) approvals() []db.Approval {
	var out []db.Approval
	for _, r := range m.runs {
		all, err := m.cur.led.RunApprovals(m.ctx, r.id)
		if err != nil {
			m.fatal(err)
		}
		out = append(out, all...)
	}
	slices.SortFunc(out, func(a, b db.Approval) int { return strings.Compare(a.CreatedAt, b.CreatedAt) })
	for _, a := range out {
		m.name(a.ApprovalID)
	}
	return out
}

func (m *machine) effectIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Sorted(maps.Keys(m.fx))
}

func (m *machine) key() string {
	m.keys++
	return fmt.Sprintf("key-%d", m.keys)
}

// fatal stops the test on an error of the walk's own, with the walk so far.
func (m *machine) fatal(err error) {
	m.t.Helper()
	m.t.Fatalf("seed %d: %v\n\nThe walk:\n%s", m.seed, err, m.log.String())
}

// fail records the first broken invariant: the walk stops after the step.
func (m *machine) fail(format string, args ...any) {
	if m.violation == "" {
		m.violation = fmt.Sprintf(format, args...)
		m.logf("BROKEN: %s", m.violation)
	}
}

func (m *machine) dumpLedger() {
	var b strings.Builder
	for _, id := range m.effectIDs() {
		e, err := m.cur.led.Q.GetEffect(m.ctx, id)
		if err != nil {
			continue
		}
		a, aerr := m.cur.led.Q.GetApprovalByEffect(m.ctx, id)
		m.mu.Lock()
		made, wentOut := m.made(id, m.fx[id].dedupe), m.wentOut(id)
		m.mu.Unlock()
		fmt.Fprintf(&b, "  %s: %s class %s maybe_sent %d claimed_by %q claimed_at %s last_request_at %s reason %q; approval %s; outside: went out %v, made %d\n",
			m.name(id), e.Status, e.EffectClass, e.MaybeSent, e.ClaimedBy.String, e.ClaimedAt.String, e.LastRequestAt.String,
			m.redact(e.Reason.String), m.approvalText(aerr == nil, a), wentOut, made)
	}
	m.dump = b.String()
}

func (m *machine) approvalText(ok bool, a db.Approval) string {
	if !ok {
		return "none"
	}
	return fmt.Sprintf("%s %s, expires %s, used %s, reason %q", m.name(a.ApprovalID), a.Status, a.ExpiresAt, a.UsedAt.String, m.redact(a.Reason.String))
}

func (m *machine) outcome(res *agentosv1.Result, err error) string {
	if err != nil {
		return "error: " + m.redact(err.Error())
	}
	return res.GetStatus()
}

func (m *machine) runNames() string {
	var names []string
	for _, r := range m.runs {
		names = append(names, map[bool]string{true: r.id + " (dry run)", false: r.id}[r.dryRun])
	}
	return strings.Join(names, ", ")
}

func (m *machine) policyText(pol map[string]toolPolicy) string {
	var parts []string
	for _, name := range m.toolNames {
		p := pol[name]
		s := name + ": allow"
		if p.require {
			s = fmt.Sprintf("%s: require_approval ttl %s", name, p.ttl)
		}
		if p.max > 1 {
			s += fmt.Sprintf(", max %d", p.max)
		}
		if p.stated != "" {
			s += ", effect " + p.stated
		}
		parts = append(parts, s)
	}
	return "{" + strings.Join(parts, "; ") + "}"
}

// name is how the log names an effect, an approval or a patch: by the call, or in the order the
// walk met it. Approval IDs are random, and the log of a seed is the same on every run.
func (m *machine) name(id string) string {
	m.logMu.Lock()
	defer m.logMu.Unlock()
	if n, ok := m.names[id]; ok {
		return n
	}
	kind := "A"
	if strings.HasPrefix(id, "sha256:") {
		kind = "edit"
	}
	m.named[kind]++
	m.names[id] = fmt.Sprintf("%s%d", kind, m.named[kind])
	return m.names[id]
}

var uuidRe = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

// redact names the approvals a message mentions as the log does.
func (m *machine) redact(s string) string {
	return uuidRe.ReplaceAllStringFunc(s, m.name)
}

func (m *machine) logf(format string, args ...any) {
	m.logMu.Lock()
	defer m.logMu.Unlock()
	fmt.Fprintf(&m.log, "%3d %s%s\n", m.step, strings.Repeat("    ", m.depth), fmt.Sprintf(format, args...))
}
