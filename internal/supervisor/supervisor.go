// Package supervisor runs the Python Worker for agentd (M5 C2): through its sandbox launcher, in a
// session of its own, started again whenever it exits, and stopped before agentd closes the
// Runtime entry. Each run of the Worker is a generation. There is no liveness probe: the tests stop
// a Worker on purpose (SIGSTOP), and a stopped Worker is left so.
//
// Each generation's stdin is its stop channel: a pipe whose write end only this process holds
// (close-on-exec) and never writes. Closing it asks the Worker to stop (agentos_worker's
// --stop-on-stdin-eof): it finishes the workflow tasks it holds and reports them through the
// Runtime entry, which still serves. Signals cannot do that through a launcher: srt drops every
// connection it proxies, the Worker's included, before it passes SIGTERM on, and bwrap dies of
// SIGTERM, the sandbox with it. The channel also closes when agentd dies, however it ends: an
// honest Worker then stops by itself, on every platform.
package supervisor

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/statedir"
)

// ExitedType is the type of the audit record of a Worker's exit that agentd did not cause.
const ExitedType = "worker.exited"

// StateKind names the Workers' state directories (statedir): /tmp/agentos-worker.<agentd pid>.*.
const StateKind = "agentos-worker"

// readyMark is what the Worker writes once it polls its task queue.
var readyMark = []byte("worker ready")

// Timing is how long the supervisor waits for what. The zero value of a field is its default.
type Timing struct {
	ReadyWithin time.Duration // the first generation's time to say it is ready: 60 s
	BackoffMin  time.Duration // the first restart's delay, doubled for each next: 1 s
	BackoffMax  time.Duration // and at most this: 60 s
	ResetAfter  time.Duration // a generation up this long starts the delays over: 60 s
	StopGrace   time.Duration // between closing the stop channel and SIGTERM: 5 s
	TermGrace   time.Duration // between SIGTERM and SIGKILL: 1 s
	RecordEvery time.Duration // at most one record of an exit this often: 1 min
	Drain       time.Duration // for the output's end, once the process group is gone: 5 s
	// Liveness (Options.NewTally): a generation is stopped and started again once it has had no poll
	// open for QuietFor, or has left a task it was handed unanswered for HoldFor, as the supervisor
	// finds every CheckEvery. The first is checked once it is ready (ReadyWithin covers it till then),
	// the rest from their start.
	QuietFor   time.Duration // 60 s
	HoldFor    time.Duration // 60 s
	CheckEvery time.Duration // 5 s
}

// WithDefaults is t with each field that is zero (or less) at its default.
func (t Timing) WithDefaults() Timing {
	for _, f := range []struct {
		v   *time.Duration
		def time.Duration
	}{{&t.ReadyWithin, 60 * time.Second}, {&t.BackoffMin, time.Second}, {&t.BackoffMax, 60 * time.Second},
		{&t.ResetAfter, 60 * time.Second}, {&t.StopGrace, 5 * time.Second}, {&t.TermGrace, time.Second},
		{&t.RecordEvery, time.Minute}, {&t.Drain, 5 * time.Second}, {&t.QuietFor, 60 * time.Second},
		{&t.HoldFor, 60 * time.Second}, {&t.CheckEvery, 5 * time.Second}} {
		if *f.v <= 0 {
			*f.v = f.def
		}
	}
	return t
}

// Output limits: the Worker writes what it likes, and agentd's log holds what agentd says. Every
// line is read as it comes, so that the Worker waits no longer than agentd takes to read it (it
// writes as if its pipe were in blocking mode, whatever srt makes of it), and what is logged is cut.
const (
	maxLine        = 2 << 10 // bytes of a line logged; the rest is cut
	linesPerSecond = 20      // lines logged, in bursts of as many
	tailLines      = 20      // of each generation's last lines, kept for its exit
)

// Options say what the supervisor runs, and how.
type Options struct {
	Argv []string // the launcher and its arguments, or the Worker's own command
	Env  []string // the whole environment it starts with
	Dir  string   // its working directory
	// StateDirs gives each generation a directory of its own, AGENTOS_SANDBOX_STATE: the macOS
	// launcher keeps srt's settings and sockets in it, and the Worker's private directory. It goes
	// once the generation's process group is gone.
	StateDirs bool
	// Lifeline gives each generation fd 3 (AGENTOS_SANDBOX_LIFELINE=3), the read end of a pipe whose
	// write end only this process holds and never writes: it reads EOF once agentd is gone, however
	// agentd ended. The macOS launcher kills the Worker then: macOS has no parent-death signal.
	Lifeline bool
	// PIDFile records the running generation's launcher, for the next agentd on the data directory
	// to stop it if this one dies without stopping it (Sweep).
	PIDFile string
	Log     *slog.Logger
	// Record takes the audit record of each exit agentd did not cause, at most one a minute: it must
	// not wait (audit.Queue.Add).
	Record func(audit.Event)
	// Hold is for tests only (agentd's fault script): while it reports true, no generation after the
	// first starts, though the delay before it is over: a window with no Worker running.
	Hold func() bool
	// NewTally, if set, begins what agentd's Runtime proxy counts of a generation's calls (a
	// runtimeproxy.Tally): the supervisor calls it just before each generation starts, and checks the
	// generation's liveness by it (Timing.QuietFor, HoldFor). Unset, nothing is checked.
	NewTally func() Tally
	// Code is the root of the code the Worker runs (agentd's snapshot of it), logged with each start.
	Code string
	// Limits, if set, hold each generation to the Worker's resource limits (Linux, M5 C3a): its
	// launcher gets what Limits.Env gives it, and runs it in a systemd user scope of its own, which the
	// supervisor checks the launcher is in once the generation says it is ready (a launcher that is not
	// fails the supervisor: Failed). Once a generation has exited without agentd's doing, the supervisor
	// asks whether the kernel's out-of-memory killer killed it: its exit's record says so (oom_kill).
	Limits Limits
	Timing Timing
}

// Limits are how a generation's launcher holds it to the Worker's resource limits: a *scope.Set.
type Limits interface {
	// Env is what generation n's launcher gets: the name of its scope ("" for none), and the variables
	// that say what holds it, or that the limits are off.
	Env(n int64) (unit string, env []string)
	// InScope checks that the launcher, process pid, is in unit; nil for no scope.
	InScope(pid int, unit string) error
	// OOMKilled reports, once what ran in unit has exited, whether the kernel's out-of-memory killer
	// killed it; its error says it could not be told.
	OOMKilled(unit string) (bool, error)
	// Memory is the memory limit, as the log names it.
	Memory() string
}

// Tally is what the Runtime proxy saw of one generation's calls: the polls it has open, since when it
// has had none (or since the tally began), and since when a task handed out to it has waited for its
// answer (zero for none).
type Tally interface {
	Liveness() (polls int, quietSince, heldSince time.Time)
}

// Supervisor runs the Worker, a generation after another. Make one with New, and Start it once.
type Supervisor struct {
	o     Options
	t     Timing
	log   *slog.Logger
	lines *rate.Limiter

	stopOnce sync.Once
	stop     chan struct{} // closed by Stop, under mu: launch starts no generation once it is
	done     chan struct{} // closed once the loop has returned
	ready    chan struct{} // closed once the first generation is ready
	failed   chan error    // why the first generation never was

	mu      sync.Mutex
	started bool
	gen     int
	pid     int // the running generation's launcher; 0 between generations

	beforeStart func(n int)           // tests: runs just before generation n's launcher is started
	startCmd    func(*exec.Cmd) error // starts a launcher: (*exec.Cmd).Start, but in tests
	terminating func()                // tests: runs as a generation's stop begins

	outMu     sync.Mutex
	dropped   int         // lines not logged since the last one logged
	droppedOf int         // their generation
	dropTimer *time.Timer // says how many, a second after the first of them, if no line has
}

// New makes a supervisor of what o says.
func New(o Options) *Supervisor {
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Supervisor{o: o, t: o.Timing.WithDefaults(), log: log, lines: rate.NewLimiter(linesPerSecond, linesPerSecond),
		stop: make(chan struct{}), done: make(chan struct{}), ready: make(chan struct{}), failed: make(chan error, 1),
		startCmd: (*exec.Cmd).Start}
}

// Start starts the first generation, and the rest as each exits, until Stop.
func (s *Supervisor) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return
	}
	s.started = true
	go s.loop()
}

// Ready is closed once the first generation says it is ready.
func (s *Supervisor) Ready() <-chan struct{} { return s.ready }

// Failed has the reason the first generation never got ready, with its last output: it exited
// first, or did not say it was ready in time and was stopped; or why a generation's launcher, ready,
// was not in the generation's scope (Options.Limits), and was stopped. No generation follows either.
func (s *Supervisor) Failed() <-chan error { return s.failed }

// Current is the last generation started, and its launcher's pid while it runs: 0 for none yet, and
// 0 between generations.
func (s *Supervisor) Current() (generation, pid int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gen, s.pid
}

// Stop stops the supervisor, before agentd closes the Runtime entry: no generation starts after it,
// and the running one is asked to stop (its stop channel closes); after the grace it gets SIGTERM,
// then SIGKILL with its whole process group. Then its state directory goes, and its pid file. It
// returns once all of that is done. A nil Supervisor stops at once.
func (s *Supervisor) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		s.mu.Lock()
		close(s.stop)
		s.mu.Unlock()
	})
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if started {
		<-s.done
	}
}

// generation is one run of the Worker.
type generation struct {
	n        int
	cmd      *exec.Cmd
	pid      int
	started  time.Time
	state    string   // its state directory, if any
	lifeline *os.File // the lifeline's write end, if any
	stopper  *os.File // the stop channel's write end; nil once closed
	out      *os.File // the read end of its stdout and stderr

	ready      chan struct{} // closed by the reader at the ready mark
	readerDone chan struct{}
	exited     chan struct{} // closed once Wait has returned: ended is set then
	ended      time.Time
	tail       tail
	tally      Tally  // what the Runtime proxy counts of its calls; nil for no checks
	unit       string // its systemd user scope (Options.Limits); "" for none
}

func (s *Supervisor) loop() {
	defer close(s.done)
	rec := &records{every: s.t.RecordEvery, record: s.o.Record}
	defer rec.flush()
	var backoff time.Duration
	for n := 1; ; n++ {
		if n > 1 && !s.pause(backoff, rec) {
			return
		}
		g, err := s.launch(n)
		if errors.Is(err, errStopped) {
			return
		}
		if err != nil {
			at := time.Now()
			if n == 1 {
				s.log.Error("could not start the Worker", "generation", n, "error", err)
				rec.exit(exit{generation: n, startError: err.Error(), at: at})
				s.failed <- fmt.Errorf("could not start the Worker: %w", err)
				return
			}
			backoff = s.next(backoff, 0)
			s.log.Error("could not start the Worker", "generation", n, "error", err, "restart_in", backoff.String())
			rec.exit(exit{generation: n, startError: err.Error(), at: at, restartIn: backoff})
			continue
		}
		if s.watch(g, rec, &backoff) {
			return
		}
	}
}

// watch follows a generation until it is over, and reports whether the supervisor stops with it.
func (s *Supervisor) watch(g *generation, rec *records, backoff *time.Duration) (stop bool) {
	first := g.n == 1
	var deadline <-chan time.Time
	if first {
		t := time.NewTimer(s.t.ReadyWithin)
		defer t.Stop()
		deadline = t.C
	}
	// The liveness checks: the first generation's from its readiness, the others' from their start. The
	// first's quiet counts from its readiness at the earliest: it may have polled nothing before.
	var check <-chan time.Time
	var ticker *time.Ticker
	var quietFrom time.Time
	checking := func() {
		if g.tally != nil && ticker == nil {
			ticker = time.NewTicker(s.t.CheckEvery)
			check = ticker.C
		}
	}
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	if !first {
		checking()
	}
	ready := g.ready
	sawReady := func() {
		ready = nil
		s.log.Info("worker ready", "generation", g.n, "pid", g.pid, "after", time.Since(g.started).Round(time.Millisecond).String())
		if first {
			close(s.ready)
			deadline = nil
			quietFrom = time.Now()
			checking()
		}
	}
	// exited handles the generation's exit, without agentd's doing, and reports whether the supervisor stops.
	exited := func() bool {
		s.cleanup(g) // its output read to the end: a ready mark in it is seen
		wasReady := closed(g.ready)
		if wasReady && ready != nil {
			sawReady()
		}
		ran := g.ended.Sub(g.started)
		e := exit{generation: g.n, pid: g.pid, ran: ran, ready: wasReady, at: g.ended, oomKill: s.oomKilled(g)}
		e.code, e.signal = exitStatus(g.cmd.ProcessState)
		if first && !wasReady {
			s.logExit(g, e)
			rec.exit(e)
			over := ""
			if e.oomKill {
				over = "; the kernel's out-of-memory killer killed it, under a memory limit of " + s.o.Limits.Memory()
			}
			s.failed <- fmt.Errorf("the Worker exited before it said it was ready (%s%s); its last output:%s", status(g), over, g.tail.text())
			return true
		}
		*backoff = s.next(*backoff, ran)
		e.restartIn = *backoff
		s.logExit(g, e)
		rec.exit(e)
		return false
	}
	for {
		select {
		case <-ready:
			// Before its readiness counts (the first's is agentd's): a launcher that does not source
			// limits.sh runs the Worker in no scope, with no limits. (A wrapper that runs one without exec
			// leaves the process agentd started outside the scope too, and agentd cannot tell them apart.)
			if err := s.inScope(g); err != nil {
				if s.reaped(g) {
					// It exited since it said it was ready, and the pid it had tells nothing any more: this is
					// its exit, which sees the ready mark that came before it.
					<-g.exited
					return exited()
				}
				s.terminate(g)
				s.cleanup(g)
				s.log.Error("the Worker's launcher did not run it in its systemd user scope, which holds its limits: agentd "+
					"stopped it, and starts no other", "generation", g.n, "pid", g.pid, "error", err)
				s.failed <- fmt.Errorf("the Worker's launcher did not run it in its systemd user scope, which holds its limits, and "+
					"agentd stopped it: %w", err)
				return true
			}
			sawReady()
		case now := <-check:
			rule, d := s.unresponsive(g, now, quietFrom)
			if rule == "" {
				continue
			}
			s.log.Error("the Worker is not serving", "generation", g.n, "pid", g.pid, "rule", rule,
				"for", d.Round(time.Millisecond).String())
			s.terminate(g)
			s.cleanup(g)
			ran := g.ended.Sub(g.started)
			e := exit{generation: g.n, pid: g.pid, ran: ran, ready: closed(g.ready), at: g.ended, unresponsive: rule, unresponsiveFor: d}
			e.code, e.signal = exitStatus(g.cmd.ProcessState)
			stopping := closed(s.stop) // a Stop came meanwhile: no generation follows
			if !stopping {
				*backoff = s.next(*backoff, ran)
				e.restartIn = *backoff
			}
			s.logExit(g, e)
			rec.exit(e)
			return stopping
		case <-deadline:
			s.terminate(g)
			s.cleanup(g)
			s.log.Error("worker not ready", "generation", g.n, "pid", g.pid, "within", s.t.ReadyWithin.String(),
				"status", status(g), "output", g.tail.lines())
			s.failed <- fmt.Errorf("the Worker did not say it was ready within %s, and agentd stopped it; its last output:%s",
				s.t.ReadyWithin, g.tail.text())
			return true
		case <-g.exited:
			return exited()
		case <-s.stop:
			s.terminate(g)
			s.cleanup(g)
			s.log.Info("worker stopped", "generation", g.n, "pid", g.pid, "status", status(g),
				"ran", g.ended.Sub(g.started).Round(time.Millisecond).String())
			return true
		case <-rec.due():
			rec.fire()
		}
	}
}

// reaped reports whether generation g's launcher has exited and been reaped (the reader of its exit
// reaps it at once): then its pid tells nothing of where it ran. A zombie, not reaped yet, is still
// there. Once reaped the process answers ErrProcessDone, through its pidfd where Linux has one, and so
// it does once g.exited is closed.
func (s *Supervisor) reaped(g *generation) bool {
	return errors.Is(g.cmd.Process.Signal(syscall.Signal(0)), os.ErrProcessDone)
}

// inScope checks that generation g's launcher is in its scope (Options.Limits): nil without limits.
func (s *Supervisor) inScope(g *generation) error {
	if s.o.Limits == nil {
		return nil
	}
	return s.o.Limits.InScope(g.pid, g.unit)
}

// oomKilled reports whether the kernel's out-of-memory killer killed generation g, once g has exited
// without agentd's doing, and logs it, naming the Worker's memory limit. systemd says it, as the
// generation's scope ended (Options.Limits), and does not say why the killer struck: past that limit,
// as a rule, or memory running out higher up (a slice's limit, the host's memory, systemd-oomd). A
// generation agentd stopped is not asked about.
func (s *Supervisor) oomKilled(g *generation) bool {
	if g.unit == "" {
		return false
	}
	killed, err := s.o.Limits.OOMKilled(g.unit)
	if err != nil {
		s.log.Warn("could not ask systemd how the Worker's scope ended: its exit's record does not say whether the kernel's "+
			"out-of-memory killer killed it", "generation", g.n, "pid", g.pid, "scope", g.unit, "error", err)
		return false
	}
	if killed {
		s.log.Error("the kernel's out-of-memory killer killed the Worker", "generation", g.n, "pid", g.pid,
			"memory_limit", s.o.Limits.Memory(), "scope", g.unit)
	}
	return killed
}

// unresponsive is the liveness rule the generation breaks at now, if any, and how long it has: a
// task it was handed left unanswered for HoldFor (not_answering, which goes first when both hold),
// or no poll open for QuietFor (not_polling), its quiet counted from quietFrom at the earliest.
func (s *Supervisor) unresponsive(g *generation, now, quietFrom time.Time) (rule string, d time.Duration) {
	polls, quietSince, heldSince := g.tally.Liveness()
	if !heldSince.IsZero() && now.Sub(heldSince) >= s.t.HoldFor {
		return "not_answering", now.Sub(heldSince)
	}
	if quietSince.Before(quietFrom) {
		quietSince = quietFrom
	}
	if polls == 0 && now.Sub(quietSince) >= s.t.QuietFor {
		return "not_polling", now.Sub(quietSince)
	}
	return "", 0
}

// pause waits d before the next generation, and on while the hold lasts (Options.Hold); it reports
// false if the supervisor stops meanwhile.
func (s *Supervisor) pause(d time.Duration, rec *records) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	held := false
	for {
		select {
		case <-t.C:
			if s.o.Hold == nil || !s.o.Hold() {
				return true
			}
			if !held {
				held = true
				s.log.Warn("the fault script holds the Worker's next start")
			}
			t.Reset(50 * time.Millisecond)
		case <-s.stop:
			return false
		case <-rec.due():
			rec.fire()
		}
	}
}

// next is the delay before the generation after one that ran for ran: the delays double from the
// least to the most, and start over after a generation that ran long enough.
func (s *Supervisor) next(prev, ran time.Duration) time.Duration {
	if prev == 0 || ran >= s.t.ResetAfter {
		return s.t.BackoffMin
	}
	return min(2*prev, s.t.BackoffMax)
}

// errStopped is launch's answer once Stop has been called: it starts no generation then.
var errStopped = errors.New("the supervisor stopped")

// launch starts generation n.
func (s *Supervisor) launch(n int) (*generation, error) {
	if len(s.o.Argv) == 0 {
		return nil, errors.New("no command for the Worker")
	}
	g := &generation{n: n, ready: make(chan struct{}), readerDone: make(chan struct{}), exited: make(chan struct{})}
	env := slices.Clone(s.o.Env)
	if s.o.StateDirs {
		dir, err := statedir.Make(StateKind)
		if err != nil {
			return nil, fmt.Errorf("the Worker's state directory: %w", err)
		}
		g.state = dir
		env = append(env, "AGENTOS_SANDBOX_STATE="+dir)
	}
	out, w, err := os.Pipe()
	if err != nil {
		s.discard(g)
		return nil, err
	}
	g.out = out
	stdin, stopper, err := os.Pipe() // the stop channel: agentd holds the write end, close-on-exec
	if err != nil {
		w.Close()
		s.discard(g)
		return nil, err
	}
	g.stopper = stopper
	if s.o.Limits != nil {
		var limits []string
		g.unit, limits = s.o.Limits.Env(int64(n))
		env = append(env, limits...)
	}
	cmd := exec.Command(s.o.Argv[0], s.o.Argv[1:]...)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = s.o.Dir, stdin, w, w
	// A session of its own, and so a process group: the group is killed whole, and the session has
	// no controlling terminal, which sandbox-runtime would let the Worker ioctl (TIOCSTI). The
	// terminal agentd runs on does not send it Ctrl-C either: agentd stops it, in order.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	var lifeline *os.File
	if s.o.Lifeline {
		if lifeline, g.lifeline, err = os.Pipe(); err != nil {
			w.Close()
			stdin.Close()
			s.discard(g)
			return nil, err
		}
		cmd.ExtraFiles = []*os.File{lifeline} // fd 3; the write end is close-on-exec, and agentd's alone
		env = append(env, "AGENTOS_SANDBOX_LIFELINE=3")
	}
	cmd.Env = env
	g.cmd = cmd
	started := make(chan error, 1)
	go func() {
		// bwrap's --die-with-parent is PR_SET_PDEATHSIG, which fires when the thread that started it
		// ends, not the process (go#27505): this goroutine keeps its thread until the launcher has.
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		if s.beforeStart != nil {
			s.beforeStart(n)
		}
		// No generation starts once Stop has been called, though the delay before it ended first: Stop
		// closes the channel under the same lock.
		s.mu.Lock()
		if closed(s.stop) {
			s.mu.Unlock()
			started <- errStopped
			return
		}
		if s.o.NewTally != nil { // its connections count from here on
			g.tally = s.o.NewTally()
		}
		err := s.startCmd(cmd)
		s.mu.Unlock()
		if err != nil {
			started <- err
			return
		}
		g.pid, g.started = cmd.Process.Pid, time.Now()
		// Before Wait reaps it: its pid is not anyone else's while it is read.
		if s.o.PIDFile != "" {
			if err := writePIDFile(s.o.PIDFile, g.pid, n); err != nil && !errors.Is(err, errGone) {
				s.log.Error("could not record the Worker's pid: if agentd is killed, the next agentd will not stop it",
					"path", s.o.PIDFile, "error", err)
			}
		}
		started <- nil
		_ = cmd.Wait()
		g.ended = time.Now()
		close(g.exited)
	}()
	err = <-started
	w.Close()
	stdin.Close()
	if lifeline != nil {
		lifeline.Close()
	}
	if err != nil {
		s.discard(g)
		return nil, err
	}
	s.mu.Lock()
	s.gen, s.pid = n, g.pid
	s.mu.Unlock()
	// The environment is safe to log, and the log says what the Worker was given: agentd builds it
	// from scratch (its own PATH and HOME, LANG, and the AGENTOS_SANDBOX_* variables of its own:
	// cmd/agentd's workerLaunch, whose test pins the keys; on Linux the limits' AGENTOS_LIMIT* and
	// XDG_RUNTIME_DIR, internal/scope's), inherits nothing else of its own, and never hands a Worker a
	// secret. A secret here would be one the Worker has, which is the bug to fix, not the log.
	attrs := []any{"generation", n, "pid", g.pid, "argv", s.o.Argv, "env", env}
	if s.o.Code != "" {
		attrs = append(attrs, "code_sha256", s.o.Code)
	}
	s.log.Info("worker started", attrs...)
	go s.read(g)
	return g, nil
}

// discard undoes what launch made of a generation that did not start.
func (s *Supervisor) discard(g *generation) {
	for _, f := range []*os.File{g.out, g.lifeline, g.stopper} {
		if f != nil {
			f.Close()
		}
	}
	if g.state != "" {
		_ = statedir.Remove(g.state)
	}
}

// terminate ends a generation: it closes the stop channel, and the Worker stops by itself; after the
// grace, SIGTERM to its launcher; after a little more, SIGKILL to its whole process group. It returns
// once the launcher has been reaped.
func (s *Supervisor) terminate(g *generation) {
	if closed(g.exited) {
		return
	}
	if s.terminating != nil {
		s.terminating()
	}
	g.closeStopper()
	if g.exitsWithin(s.t.StopGrace) {
		return
	}
	s.log.Warn("the Worker did not stop when asked: SIGTERM", "generation", g.n, "pid", g.pid, "grace", s.t.StopGrace.String())
	_ = g.cmd.Process.Signal(syscall.SIGTERM)
	if g.exitsWithin(s.t.TermGrace) {
		return
	}
	s.log.Warn("the Worker did not stop on SIGTERM: killing its process group", "generation", g.n, "pid", g.pid,
		"grace", s.t.TermGrace.String())
	_ = syscall.Kill(-g.pid, syscall.SIGKILL)
	_ = g.cmd.Process.Kill()
	<-g.exited
}

// exitsWithin reports whether the generation's launcher exits within d.
func (g *generation) exitsWithin(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-g.exited:
		return true
	case <-t.C:
		return false
	}
}

// closeStopper closes the stop channel's write end, once.
func (g *generation) closeStopper() {
	if g.stopper != nil {
		g.stopper.Close()
		g.stopper = nil
	}
}

// cleanup ends what is left of a generation whose launcher has exited: its process group, its
// output, its stop channel and lifeline, its state directory and its pid file.
func (s *Supervisor) cleanup(g *generation) {
	// Anything of the group still running: the macOS launcher's watcher, a child of the Worker's.
	// (On Linux the sandbox is a session of its own, which bwrap's --die-with-parent ends.)
	_ = syscall.Kill(-g.pid, syscall.SIGKILL)
	t := time.NewTimer(s.t.Drain)
	defer t.Stop()
	select {
	case <-g.readerDone:
	case <-t.C:
		// Something outside the group holds the output open (a process that started a session of
		// its own): stop reading it.
		s.log.Warn("the Worker's output is still open after its process group ended", "generation", g.n, "pid", g.pid)
		g.out.Close()
		<-g.readerDone
	}
	g.out.Close()
	s.notLogged()
	g.closeStopper()
	if g.lifeline != nil {
		g.lifeline.Close()
	}
	s.mu.Lock()
	s.pid = 0
	s.mu.Unlock()
	if g.state != "" {
		if err := statedir.Remove(g.state); err != nil {
			s.log.Warn("could not remove the Worker's state directory; the next agentd tries again", "dir", g.state, "error", err)
		}
	}
	if s.o.PIDFile != "" {
		removePIDFile(s.o.PIDFile)
	}
}

func (s *Supervisor) logExit(g *generation, e exit) {
	attrs := []any{"generation", g.n, "pid", g.pid, "status", status(g), "ran", e.ran.Round(time.Millisecond).String(),
		"ready", e.ready}
	if e.restartIn > 0 {
		attrs = append(attrs, "restart_in", e.restartIn.String())
	} else {
		attrs = append(attrs, "restart", false)
	}
	if e.unresponsive != "" {
		attrs = append(attrs, "unresponsive", e.unresponsive)
	}
	if e.oomKill {
		attrs = append(attrs, "oom_kill", true)
	}
	s.log.Warn("worker exited", append(attrs, "output", g.tail.lines())...)
}

// read reads a generation's output to its end: each line is looked at for the ready mark, kept in
// the tail, and logged within the limits.
func (s *Supervisor) read(g *generation) {
	defer close(g.readerDone)
	r := bufio.NewReaderSize(g.out, 4<<10)
	seen := false
	for {
		line, n, err := nextLine(r, maxLine)
		if err == nil || n > 0 {
			if !seen && bytes.Contains(line, readyMark) {
				seen = true
				close(g.ready)
			}
			s.output(g, line, n)
		}
		if err != nil {
			return
		}
	}
}

// output keeps a line in the tail and logs it, unless the Worker writes faster than agentd logs:
// then it counts it, and the next line logged says how many were not.
func (s *Supervisor) output(g *generation, line []byte, n int) {
	text := string(line)
	if n > len(line) {
		text = string(cutRune(line)) + "…"
	}
	g.tail.add(text)
	s.outMu.Lock()
	if !s.lines.Allow() {
		s.dropped++
		s.droppedOf = g.n
		if s.dropTimer == nil {
			s.dropTimer = time.AfterFunc(time.Second, s.notLogged)
		}
		s.outMu.Unlock()
		return
	}
	s.outMu.Unlock()
	s.notLogged()
	attrs := []any{"generation", g.n, "line", text}
	if n > len(line) {
		attrs = append(attrs, "bytes", n)
	}
	s.log.Info("worker output", attrs...)
}

// notLogged says how many lines were not logged since the last one that was, if any.
func (s *Supervisor) notLogged() {
	s.outMu.Lock()
	n, gen := s.dropped, s.droppedOf
	s.dropped = 0
	if s.dropTimer != nil {
		s.dropTimer.Stop()
		s.dropTimer = nil
	}
	s.outMu.Unlock()
	if n > 0 {
		s.log.Warn("worker output not logged", "generation", gen, "lines", n)
	}
}

// nextLine reads a line, and returns its first max bytes without the newline, and how long it
// was: a longer line is read to its end all the same. err is that of the read that ended it: nil
// at a newline, io.EOF at the end of what has none.
func nextLine(r *bufio.Reader, max int) ([]byte, int, error) {
	var line []byte
	n := 0
	for {
		chunk, err := r.ReadSlice('\n')
		if err == nil {
			chunk = chunk[:len(chunk)-1]
		}
		n += len(chunk)
		if room := max - len(line); room > 0 {
			line = append(line, chunk[:min(room, len(chunk))]...)
		}
		if err != bufio.ErrBufferFull {
			return line, n, err
		}
	}
}

// cutRune drops a rune the cut left incomplete at the end of b.
func cutRune(b []byte) []byte {
	for i := len(b) - 1; i >= 0 && i >= len(b)-4; i-- {
		if c := b[i]; c&0xc0 != 0x80 { // the last rune's first byte
			if size := runeLen(c); size > len(b)-i {
				return b[:i]
			}
			return b
		}
	}
	return b
}

// runeLen is how long UTF-8 says the rune that begins with c is: 1 for a byte that begins none.
func runeLen(c byte) int {
	switch {
	case c&0xe0 == 0xc0:
		return 2
	case c&0xf0 == 0xe0:
		return 3
	case c&0xf8 == 0xf0:
		return 4
	}
	return 1
}

// tail is a generation's last lines.
type tail struct {
	mu sync.Mutex
	l  []string
}

func (t *tail) add(line string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.l) == tailLines {
		t.l = t.l[1:]
	}
	t.l = append(t.l, line)
}

func (t *tail) lines() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.l)
}

// text is the tail as an error quotes it, a line each, indented.
func (t *tail) text() string {
	l := t.lines()
	if len(l) == 0 {
		return " none"
	}
	return "\n  " + strings.Join(l, "\n  ")
}

func closed(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

func status(g *generation) string {
	if g.cmd.ProcessState == nil {
		return "running"
	}
	return g.cmd.ProcessState.String()
}

// exitStatus is how a process ended: its exit code, or the signal that killed it.
func exitStatus(ps *os.ProcessState) (code int, signal string) {
	if ps == nil {
		return -1, ""
	}
	if ws, ok := ps.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return -1, ws.Signal().String()
	}
	return ps.ExitCode(), ""
}
