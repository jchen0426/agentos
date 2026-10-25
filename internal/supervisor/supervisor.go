// Package supervisor runs the Python Worker for agentd (M5 C2): through its sandbox launcher, in a
// session of its own, started again whenever it exits, and stopped before agentd closes the
// Runtime entry. Each run of the Worker is a generation. There is no liveness probe: the tests stop
// a Worker on purpose (SIGSTOP), and a stopped Worker is left so.
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
	StopGrace   time.Duration // between SIGTERM and SIGKILL: 5 s
	RecordEvery time.Duration // at most one record of an exit this often: 1 min
	Drain       time.Duration // for the output's end, once the process group is gone: 5 s
}

func (t Timing) withDefaults() Timing {
	for _, f := range []struct {
		v   *time.Duration
		def time.Duration
	}{{&t.ReadyWithin, 60 * time.Second}, {&t.BackoffMin, time.Second}, {&t.BackoffMax, 60 * time.Second},
		{&t.ResetAfter, 60 * time.Second}, {&t.StopGrace, 5 * time.Second}, {&t.RecordEvery, time.Minute},
		{&t.Drain, 5 * time.Second}} {
		if *f.v <= 0 {
			*f.v = f.def
		}
	}
	return t
}

// Output limits: the Worker writes what it likes, and agentd's log holds what agentd says. Every
// line is read (a Worker never waits for agentd to read its output), and what is logged is cut.
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
	Timing Timing
}

// Supervisor runs the Worker, a generation after another. Make one with New, and Start it once.
type Supervisor struct {
	o     Options
	t     Timing
	log   *slog.Logger
	lines *rate.Limiter

	stopOnce sync.Once
	stop     chan struct{} // closed by Stop
	done     chan struct{} // closed once the loop has returned
	ready    chan struct{} // closed once the first generation is ready
	failed   chan error    // why the first generation never was

	mu      sync.Mutex
	started bool
	gen     int
	pid     int

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
	return &Supervisor{o: o, t: o.Timing.withDefaults(), log: log, lines: rate.NewLimiter(linesPerSecond, linesPerSecond),
		stop: make(chan struct{}), done: make(chan struct{}), ready: make(chan struct{}), failed: make(chan error, 1)}
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
// first, or did not say it was ready in time and was stopped. No generation follows it.
func (s *Supervisor) Failed() <-chan error { return s.failed }

// Current is the running generation and its launcher's pid: 0 for none yet.
func (s *Supervisor) Current() (generation, pid int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gen, s.pid
}

// Stop stops the supervisor, before agentd closes the Runtime entry: no generation starts after it,
// and the running one gets SIGTERM, then after the grace SIGKILL with its whole process group.
// Then its state directory goes, and its pid file. It returns once all of that is done. A nil
// Supervisor stops at once.
func (s *Supervisor) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() { close(s.stop) })
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
	out      *os.File // the read end of its stdout and stderr

	ready      chan struct{} // closed by the reader at the ready mark
	readerDone chan struct{}
	exited     chan struct{} // closed once Wait has returned: ended is set then
	ended      time.Time
	tail       tail
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
	ready := g.ready
	sawReady := func() {
		ready = nil
		s.log.Info("worker ready", "generation", g.n, "pid", g.pid, "after", time.Since(g.started).Round(time.Millisecond).String())
		if first {
			close(s.ready)
			deadline = nil
		}
	}
	for {
		select {
		case <-ready:
			sawReady()
		case <-deadline:
			s.terminate(g)
			s.cleanup(g)
			s.log.Error("worker not ready", "generation", g.n, "pid", g.pid, "within", s.t.ReadyWithin.String(),
				"status", status(g), "output", g.tail.lines())
			s.failed <- fmt.Errorf("the Worker did not say it was ready within %s, and agentd stopped it; its last output:%s",
				s.t.ReadyWithin, g.tail.text())
			return true
		case <-g.exited:
			s.cleanup(g) // its output read to the end: a ready mark in it is seen
			wasReady := closed(g.ready)
			if wasReady && ready != nil {
				sawReady()
			}
			ran := g.ended.Sub(g.started)
			e := exit{generation: g.n, pid: g.pid, ran: ran, ready: wasReady, at: g.ended}
			e.code, e.signal = exitStatus(g.cmd.ProcessState)
			if first && !wasReady {
				s.logExit(g, e)
				rec.exit(e)
				s.failed <- fmt.Errorf("the Worker exited before it said it was ready (%s); its last output:%s", status(g), g.tail.text())
				return true
			}
			*backoff = s.next(*backoff, ran)
			e.restartIn = *backoff
			s.logExit(g, e)
			rec.exit(e)
			return false
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

// pause waits d before the next generation, and reports false if the supervisor stops meanwhile.
func (s *Supervisor) pause(d time.Duration, rec *records) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			return true
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
	cmd := exec.Command(s.o.Argv[0], s.o.Argv[1:]...)
	cmd.Dir, cmd.Stdout, cmd.Stderr = s.o.Dir, w, w
	// A session of its own, and so a process group: the group is killed whole, and the session has
	// no controlling terminal, which sandbox-runtime would let the Worker ioctl (TIOCSTI). The
	// terminal agentd runs on does not send it Ctrl-C either: agentd stops it, in order.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	var lifeline *os.File
	if s.o.Lifeline {
		if lifeline, g.lifeline, err = os.Pipe(); err != nil {
			w.Close()
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
		if err := cmd.Start(); err != nil {
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
	// from scratch (PATH, HOME, LANG and the AGENTOS_SANDBOX_* variables of its own: cmd/agentd's
	// workerLaunch, whose test pins the keys), inherits none of its own, and never hands a Worker a
	// secret. A secret here would be one the Worker has, which is the bug to fix, not the log.
	s.log.Info("worker started", "generation", n, "pid", g.pid, "argv", s.o.Argv, "env", env)
	go s.read(g)
	return g, nil
}

// discard undoes what launch made of a generation that did not start.
func (s *Supervisor) discard(g *generation) {
	for _, f := range []*os.File{g.out, g.lifeline} {
		if f != nil {
			f.Close()
		}
	}
	if g.state != "" {
		_ = statedir.Remove(g.state)
	}
}

// terminate ends a generation: SIGTERM to its launcher, which passes it on; after the grace,
// SIGKILL to its whole process group. It returns once the launcher has been reaped.
func (s *Supervisor) terminate(g *generation) {
	if closed(g.exited) {
		return
	}
	_ = g.cmd.Process.Signal(syscall.SIGTERM)
	t := time.NewTimer(s.t.StopGrace)
	defer t.Stop()
	select {
	case <-g.exited:
	case <-t.C:
		s.log.Warn("the Worker did not stop on SIGTERM: killing its process group", "generation", g.n, "pid", g.pid,
			"grace", s.t.StopGrace.String())
		_ = syscall.Kill(-g.pid, syscall.SIGKILL)
		_ = g.cmd.Process.Kill()
		<-g.exited
	}
}

// cleanup ends what is left of a generation whose launcher has exited: its process group, its
// output, its lifeline, its state directory and its pid file.
func (s *Supervisor) cleanup(g *generation) {
	// Anything of the group still running: the launcher's watcher, socat, a child of the Worker's.
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
	if g.lifeline != nil {
		g.lifeline.Close()
	}
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
