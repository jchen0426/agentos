package supervisor

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"golang.org/x/sys/unix"

	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/statedir"
)

// helperEnv makes this test binary a Worker that behaves as its value says (helper), or, for
// "supervise", an agentd that supervises one (supervise).
const helperEnv = "AGENTOS_TEST_WORKER"

func TestMain(m *testing.M) {
	if mode := os.Getenv(helperEnv); mode != "" {
		helper(mode)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// helper is a Worker: it writes its output to stdout and stderr as a Worker does, and ends as the
// mode says. AGENTOS_TEST_FILE is where it reports.
func helper(mode string) {
	file := os.Getenv("AGENTOS_TEST_FILE")
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM)
	ready := func() {
		fmt.Fprintln(os.Stderr, "2026-10-25 12:00:00,000 INFO root worker ready: endpoint=x task_queue=y")
	}
	hold := func() {
		<-term
		os.Exit(0)
	}
	switch mode {
	case "ready":
		ready()
		hold()
	case "exit":
		fmt.Println("starting")
		fmt.Fprintln(os.Stderr, "ModuleNotFoundError: No module named 'x'")
		os.Exit(3)
	case "never":
		fmt.Println("connecting")
		hold()
	case "stubborn": // ready, deaf to SIGTERM, with a child in its process group
		signal.Ignore(syscall.SIGTERM)
		child := exec.Command("/bin/sleep", "60") // a Worker has no PATH here
		if err := child.Start(); err != nil {
			os.Exit(1)
		}
		_ = os.WriteFile(file, []byte(strconv.Itoa(child.Process.Pid)), 0o600)
		ready()
		time.Sleep(time.Hour)
	case "leaves-child": // ready, then exits, leaving a child in its process group
		child := exec.Command("/bin/sleep", "60")
		if err := child.Start(); err != nil {
			os.Exit(1)
		}
		_ = os.WriteFile(file, []byte(strconv.Itoa(child.Process.Pid)), 0o600)
		ready()
		time.Sleep(200 * time.Millisecond)
		os.Exit(1)
	case "flood": // a long line first, then more lines than agentd logs, then the ready mark
		fmt.Println(strings.Repeat("x", 2047) + strings.Repeat("é", 5000))
		w := bufio.NewWriter(os.Stdout)
		for i := range 3000 {
			fmt.Fprintf(w, "line %d %s\n", i, strings.Repeat("y", 100))
		}
		w.Flush()
		ready()
		fmt.Print("a last line with no newline")
		hold()
	case "report": // where it runs, and what it was given
		sid, _ := unix.Getsid(0)
		cwd, _ := os.Getwd()
		tty := "none"
		if f, err := os.Open("/dev/tty"); err == nil {
			tty, _ = "one", f.Close()
		}
		var st unix.Stat_t
		fd3 := "closed"
		if unix.Fstat(3, &st) == nil {
			fd3 = "open"
			if st.Mode&unix.S_IFMT == unix.S_IFIFO {
				fd3 = "pipe"
			}
		}
		state := os.Getenv("AGENTOS_SANDBOX_STATE")
		if state != "" {
			_ = os.WriteFile(filepath.Join(state, "mine"), []byte("x"), 0o600)
		}
		b, _ := json.Marshal(map[string]any{"pid": os.Getpid(), "sid": sid, "pgid": syscall.Getpgrp(), "tty": tty, "cwd": cwd,
			"env": os.Environ(), "fd3": fd3})
		_ = os.WriteFile(file, b, 0o600)
		ready()
		hold()
	case "lifeline": // as the macOS launcher's watcher: ends when fd 3 reads EOF
		ready()
		_, _ = io.Copy(io.Discard, os.NewFile(3, "lifeline"))
		_ = os.WriteFile(file, []byte("eof"), 0o600)
	case "planned": // each start is one step of AGENTOS_TEST_PLAN, in turn
		f, _ := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
		b, _ := io.ReadAll(f)
		step := strings.Count(string(b), "\n")
		fmt.Fprintln(f, time.Now().UnixNano())
		f.Close()
		plan := strings.Split(os.Getenv("AGENTOS_TEST_PLAN"), ",")
		switch plan[min(step, len(plan)-1)] {
		case "re": // ready, then exits
			ready()
			os.Exit(1)
		case "long": // ready, up a while, then exits
			ready()
			time.Sleep(500 * time.Millisecond)
			os.Exit(1)
		default:
			ready()
			hold()
		}
	case "supervise":
		supervise()
	}
}

// supervise is an agentd that runs a supervisor of the Worker AGENTOS_TEST_CHILD names, with a
// lifeline if AGENTOS_TEST_LIFELINE says so, and AGENTOS_TEST_PIDFILE: it writes "ready" to
// AGENTOS_TEST_FILE once its Worker is, and runs until killed.
func supervise() {
	file := os.Getenv("AGENTOS_TEST_FILE")
	child := os.Getenv("AGENTOS_TEST_CHILD")
	bin, _ := os.Executable()
	s := New(Options{Argv: []string{bin}, Env: []string{helperEnv + "=" + child, "AGENTOS_TEST_FILE=" + file + ".worker"},
		Lifeline: os.Getenv("AGENTOS_TEST_LIFELINE") == "1", PIDFile: os.Getenv("AGENTOS_TEST_PIDFILE")})
	s.Start()
	<-s.Ready()
	_ = os.WriteFile(file, []byte("ready"), 0o600)
	time.Sleep(time.Hour)
}

// logs collects what a supervisor logs, as JSON lines.
type logs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logs) entries(msg string) []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	for line := range strings.SplitSeq(l.buf.String(), "\n") {
		var e map[string]any
		if json.Unmarshal([]byte(line), &e) == nil && (msg == "" || e["msg"] == msg) {
			out = append(out, e)
		}
	}
	return out
}

// recorded collects the records a supervisor writes, with when it wrote each.
type recorded struct {
	mu     sync.Mutex
	events []audit.Event
	at     []time.Time
}

func (r *recorded) add(e audit.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events, r.at = append(r.events, e), append(r.at, time.Now())
}

func (r *recorded) all() ([]audit.Event, []time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events), slices.Clone(r.at)
}

// worker is a supervisor of this test binary as a Worker in mode, and its logs.
func worker(t *testing.T, mode string, timing Timing, env ...string) (*Supervisor, *logs, *recorded, string) {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "report")
	var l logs
	var r recorded
	s := New(Options{Argv: []string{bin}, Env: append([]string{helperEnv + "=" + mode, "AGENTOS_TEST_FILE=" + file}, env...),
		Dir: t.TempDir(), Log: slog.New(slog.NewJSONHandler(&l, nil)), Record: r.add, Timing: timing})
	t.Cleanup(s.Stop)
	return s, &l, &r, file
}

func wait(t *testing.T, what string, timeout time.Duration, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(timeout); !ok(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
	}
}

func readied(t *testing.T, s *Supervisor) {
	t.Helper()
	select {
	case <-s.Ready():
	case err := <-s.Failed():
		t.Fatalf("the first Worker failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the first Worker was not ready in 10 s")
	}
}

func gone(pid int) bool {
	_, err := startTime(pid)
	return errors.Is(err, errGone)
}

// Each restart waits twice as long as the one before, up to the most; a Worker that was up long
// enough starts the delays over. Every exit, however it ended, is followed by a restart.
func TestBackoff(t *testing.T) {
	s, l, _, file := worker(t, "planned", Timing{BackoffMin: 50 * time.Millisecond, BackoffMax: 200 * time.Millisecond,
		ResetAfter: 400 * time.Millisecond}, "AGENTOS_TEST_PLAN=re,re,re,re,long,re,ready")
	s.Start()
	readied(t, s)
	wait(t, "the seventh Worker", 20*time.Second, func() bool { return len(l.entries("worker ready")) == 7 })
	var restarts []string
	exits := l.entries("worker exited")
	for _, e := range exits {
		restarts = append(restarts, fmt.Sprint(e["restart_in"]))
	}
	if want := []string{"50ms", "100ms", "200ms", "200ms", "50ms", "100ms"}; !slices.Equal(restarts, want) {
		t.Fatalf("restarts %q, want %q", restarts, want)
	}
	// And it waited each delay, from the exit to the next start.
	starts := l.entries("worker started")
	for i, e := range exits {
		exited, _ := time.Parse(time.RFC3339Nano, e["time"].(string))
		next, _ := time.Parse(time.RFC3339Nano, starts[i+1]["time"].(string))
		d, _ := time.ParseDuration(restarts[i])
		if gap := next.Sub(exited); gap < d {
			t.Errorf("restart %d after %s, before its %s", i+1, gap, d)
		}
	}
	if b, _ := os.ReadFile(file); strings.Count(string(b), "\n") != 7 {
		t.Errorf("%d starts", strings.Count(string(b), "\n"))
	}
}

// The first Worker must say it is ready: one that exits first, or does not say so in time (it is
// stopped), fails the supervisor with its last output, and none follows it.
func TestTheFirstWorker(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		s, l, r, _ := worker(t, "ready", Timing{})
		s.Start()
		readied(t, s)
		if gen, pid := s.Current(); gen != 1 || pid == 0 || len(l.entries("worker ready")) != 1 {
			t.Errorf("current %d %d, logs %v", gen, pid, l.entries(""))
		}
		s.Stop()
		if ev, _ := r.all(); len(ev) != 0 {
			t.Errorf("records of a Worker agentd stopped: %v", ev)
		}
	})
	t.Run("exits", func(t *testing.T) {
		s, l, r, _ := worker(t, "exit", Timing{BackoffMin: 10 * time.Millisecond})
		s.Start()
		var err error
		select {
		case err = <-s.Failed():
		case <-time.After(10 * time.Second):
			t.Fatal("no failure")
		}
		for _, want := range []string{"exited before it said it was ready", "exit status 3", "ModuleNotFoundError: No module named 'x'",
			"starting"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%v: no %q", err, want)
			}
		}
		time.Sleep(100 * time.Millisecond)
		if n := len(l.entries("worker started")); n != 1 || closed(s.Ready()) {
			t.Errorf("%d starts", n)
		}
		ev, _ := r.all()
		if len(ev) != 1 || ev[0].Payload.(map[string]any)["exit_code"] != 3 || ev[0].Payload.(map[string]any)["restart_in_ms"] != nil {
			t.Errorf("records %v", ev)
		}
	})
	t.Run("never ready", func(t *testing.T) {
		s, l, r, _ := worker(t, "never", Timing{ReadyWithin: 300 * time.Millisecond, BackoffMin: 10 * time.Millisecond})
		start := time.Now()
		s.Start()
		var err error
		select {
		case err = <-s.Failed():
		case <-time.After(10 * time.Second):
			t.Fatal("no failure")
		}
		if took := time.Since(start); took < 300*time.Millisecond {
			t.Errorf("failed after %s", took)
		}
		if !strings.Contains(err.Error(), "did not say it was ready within 300ms") || !strings.Contains(err.Error(), "connecting") {
			t.Errorf("%v", err)
		}
		_, pid := s.Current()
		time.Sleep(100 * time.Millisecond)
		if !gone(pid) || len(l.entries("worker started")) != 1 {
			t.Errorf("the Worker %d is there, or another started: %v", pid, l.entries(""))
		}
		if ev, _ := r.all(); len(ev) != 0 { // agentd stopped it
			t.Errorf("records %v", ev)
		}
	})
	t.Run("does not start", func(t *testing.T) {
		var l logs
		s := New(Options{Argv: []string{filepath.Join(t.TempDir(), "no-launcher")}, Log: slog.New(slog.NewJSONHandler(&l, nil))})
		s.Start()
		select {
		case err := <-s.Failed():
			if !strings.Contains(err.Error(), "could not start the Worker") {
				t.Errorf("%v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no failure")
		}
		s.Stop()
	})
}

// Stop: no Worker after it; the running one gets SIGTERM, then after the grace SIGKILL with its whole
// process group. A stop while the supervisor waits to restart returns at once.
func TestStop(t *testing.T) {
	t.Run("on SIGTERM", func(t *testing.T) {
		s, l, _, _ := worker(t, "ready", Timing{StopGrace: 3 * time.Second})
		s.Start()
		readied(t, s)
		_, pid := s.Current()
		start := time.Now()
		s.Stop()
		if took := time.Since(start); took > 2*time.Second {
			t.Errorf("Stop took %s", took)
		}
		stopped := l.entries("worker stopped")
		if len(stopped) != 1 || stopped[0]["status"] != "exit status 0" || !gone(pid) || len(l.entries("worker exited")) != 0 {
			t.Errorf("logs %v", l.entries(""))
		}
		time.Sleep(100 * time.Millisecond)
		if len(l.entries("worker started")) != 1 {
			t.Error("a Worker started after Stop")
		}
	})
	t.Run("deaf to SIGTERM", func(t *testing.T) {
		s, l, _, file := worker(t, "stubborn", Timing{StopGrace: 300 * time.Millisecond})
		s.Start()
		readied(t, s)
		_, pid := s.Current()
		var child int
		wait(t, "the child's pid", 5*time.Second, func() bool {
			b, _ := os.ReadFile(file)
			child, _ = strconv.Atoi(string(b))
			return child > 0
		})
		start := time.Now()
		s.Stop()
		if took := time.Since(start); took < 300*time.Millisecond || took > 5*time.Second {
			t.Errorf("Stop took %s", took)
		}
		if !gone(pid) {
			t.Errorf("the Worker %d is there", pid)
		}
		wait(t, "the Worker's child to end with its group", 5*time.Second, func() bool { return gone(child) })
		if l.entries("worker stopped")[0]["status"] != "signal: killed" {
			t.Errorf("logs %v", l.entries(""))
		}
	})
	t.Run("an exit ends its process group", func(t *testing.T) {
		s, l, _, file := worker(t, "leaves-child", Timing{BackoffMin: 30 * time.Second})
		s.Start()
		readied(t, s)
		wait(t, "the exit", 5*time.Second, func() bool { return len(l.entries("worker exited")) == 1 })
		b, _ := os.ReadFile(file)
		child, _ := strconv.Atoi(string(b))
		if child == 0 {
			t.Fatalf("no child: %q", b)
		}
		wait(t, "the Worker's child to end with its group", 5*time.Second, func() bool { return gone(child) })
	})
	t.Run("while it waits to restart", func(t *testing.T) {
		s, l, _, _ := worker(t, "planned", Timing{BackoffMin: 30 * time.Second}, "AGENTOS_TEST_PLAN=re")
		s.Start()
		readied(t, s)
		wait(t, "the exit", 5*time.Second, func() bool { return len(l.entries("worker exited")) == 1 })
		start := time.Now()
		s.Stop()
		if took := time.Since(start); took > time.Second {
			t.Errorf("Stop took %s", took)
		}
		if len(l.entries("worker started")) != 1 {
			t.Error("a Worker started after Stop")
		}
	})
	t.Run("never started", func(t *testing.T) {
		New(Options{}).Stop()
		var s *Supervisor
		s.Stop()
	})
}

// Every line the Worker writes is read, however fast it writes, and looked at for the ready mark;
// agentd's log takes at most 20 a second, each cut at 2 KiB, and says how many it left out.
func TestOutput(t *testing.T) {
	s, l, _, _ := worker(t, "flood", Timing{})
	start := time.Now()
	s.Start()
	readied(t, s) // its ready mark comes after 3,001 lines
	elapsed := time.Since(start)
	lines := l.entries("worker output")
	if limit := linesPerSecond + int(linesPerSecond*elapsed.Seconds()) + 1; len(lines) > limit {
		t.Errorf("%d lines logged in %s", len(lines), elapsed)
	}
	for _, e := range lines {
		if strings.Contains(e["line"].(string), "worker ready") {
			t.Error("the ready mark was logged: the test did not find it among lines left out")
		}
	}
	long := lines[0]
	text := long["line"].(string)
	if long["bytes"] != float64(2047+2*5000) || !strings.HasSuffix(text, "x…") || len(text) > maxLine+len("…") || !utf8.ValidString(text) {
		t.Errorf("the long line: %d bytes, %v, %.40q…%q", len(text), long["bytes"], text, text[max(0, len(text)-8):])
	}
	// What was left out is counted within a second, though the Worker writes no more.
	wait(t, "the count of lines not logged", 5*time.Second, func() bool { return len(l.entries("worker output not logged")) > 0 })
	s.Stop()
	// The last line, which has no newline, is read too: when the output ends.
	n := 0
	for _, e := range l.entries("worker output not logged") {
		n += int(e["lines"].(float64))
	}
	if total := len(l.entries("worker output")) + n; total != 3003 {
		t.Errorf("%d lines read, want 3003", total)
	}
}

func TestNextLine(t *testing.T) {
	r := bufio.NewReaderSize(strings.NewReader("short\n"+strings.Repeat("a", 10)+"\n\nlast"), 16)
	for _, want := range []struct {
		line string
		n    int
		err  error
	}{{"short", 5, nil}, {"aaaaaaaa", 10, nil}, {"", 0, nil}, {"last", 4, io.EOF}, {"", 0, io.EOF}} {
		line, n, err := nextLine(r, 8)
		if string(line) != want.line || n != want.n || err != want.err {
			t.Errorf("%q %d %v, want %q %d %v", line, n, err, want.line, want.n, want.err)
		}
	}
	for in, want := range map[string]string{"abc": "abc", "ab\xc3": "ab", "a\xe2\x82": "a", "a\xe2\x82\xac": "a\xe2\x82\xac",
		"\xf0\x9f\x98": "", "x\x80\x80": "x\x80\x80"} {
		if got := string(cutRune([]byte(in))); got != want {
			t.Errorf("cutRune(%q) = %q, want %q", in, got, want)
		}
	}
}

// One record of an exit agentd did not cause, at most one in each window: the first at once, the
// rest held, and when the window is up one record describes the latest and counts the others. Every
// exit is either described or counted; the stop's own is neither.
func TestRecords(t *testing.T) {
	s, l, r, _ := worker(t, "planned", Timing{BackoffMin: 20 * time.Millisecond, BackoffMax: 20 * time.Millisecond,
		RecordEvery: 400 * time.Millisecond}, "AGENTOS_TEST_PLAN=re")
	s.Start()
	readied(t, s)
	wait(t, "a dozen exits", 20*time.Second, func() bool { return len(l.entries("worker exited")) >= 12 })
	time.Sleep(450 * time.Millisecond)
	// More than a window has passed since the first record: the held exits were recorded when it was up.
	if before, _ := r.all(); len(before) < 2 {
		t.Fatalf("%d records before the stop", len(before))
	}
	s.Stop()
	exits := len(l.entries("worker exited"))
	ev, at := r.all()
	if len(ev) < 2 || ev[0].Payload.(map[string]any)["generation"] != 1 {
		t.Fatalf("records %v", ev)
	}
	counted := 0
	for i, e := range ev {
		p := e.Payload.(map[string]any)
		if e.Type != ExitedType || e.Actor != audit.Agentd || e.WorkflowID != "" {
			t.Errorf("record %d: %+v", i, e)
		}
		keys := slices.Sorted(maps.Keys(p))
		if want := []string{"exit_code", "exited_at", "generation", "pid", "ran_ms", "ready", "restart_in_ms", "unrecorded"}; !slices.Equal(keys, want) {
			t.Errorf("record %d: keys %q", i, keys)
		}
		counted += 1 + p["unrecorded"].(int)
		if i > 0 && at[i].Sub(at[i-1]) < 390*time.Millisecond && i < len(ev)-1 { // the last may be the stop's flush
			t.Errorf("records %d and %d %s apart", i-1, i, at[i].Sub(at[i-1]))
		}
	}
	if counted != exits {
		t.Errorf("%d exits, %d described or counted", exits, counted)
	}
}

// Exits held for a window are recorded once it is up, though no exit comes after them to write them;
// and those held as the supervisor stops, then.
func TestRecordsOfHeldExits(t *testing.T) {
	t.Run("once the window is up", func(t *testing.T) {
		s, l, r, _ := worker(t, "planned", Timing{BackoffMin: 20 * time.Millisecond, BackoffMax: 20 * time.Millisecond,
			RecordEvery: 400 * time.Millisecond}, "AGENTOS_TEST_PLAN=re,re,re,ready")
		s.Start()
		readied(t, s)
		wait(t, "the fourth Worker", 10*time.Second, func() bool { return len(l.entries("worker ready")) == 4 })
		wait(t, "the held exits' record", 5*time.Second, func() bool { ev, _ := r.all(); return len(ev) == 2 })
		ev, _ := r.all()
		if p := ev[1].Payload.(map[string]any); p["generation"] != 3 || p["unrecorded"] != 1 {
			t.Errorf("the second record: %v", p)
		}
	})
	t.Run("as the supervisor stops", func(t *testing.T) {
		s, l, r, _ := worker(t, "planned", Timing{BackoffMin: 20 * time.Millisecond, BackoffMax: 20 * time.Millisecond,
			RecordEvery: time.Hour}, "AGENTOS_TEST_PLAN=re,re,ready")
		s.Start()
		readied(t, s)
		wait(t, "the third Worker", 10*time.Second, func() bool { return len(l.entries("worker ready")) == 3 })
		if ev, _ := r.all(); len(ev) != 1 {
			t.Fatalf("records before the stop: %v", ev)
		}
		s.Stop()
		ev, _ := r.all()
		if len(ev) != 2 || ev[1].Payload.(map[string]any)["generation"] != 2 || ev[1].Payload.(map[string]any)["unrecorded"] != 0 {
			t.Errorf("records %v", ev)
		}
	})
}

// A Worker runs in a session of its own, with no controlling terminal, in its working directory,
// with the environment it is given and no more: on macOS a state directory of its own and the
// lifeline as fd 3. Its launcher is in the pid file while it runs; both go when it does.
func TestWhatTheWorkerGets(t *testing.T) {
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	file, pidFile := filepath.Join(dir, "report"), filepath.Join(dir, "worker.pid")
	env := []string{helperEnv + "=report", "AGENTOS_TEST_FILE=" + file, "LANG=C.UTF-8"}
	s := New(Options{Argv: []string{bin}, Env: env, Dir: dir, StateDirs: true, Lifeline: true, PIDFile: pidFile})
	t.Cleanup(s.Stop)
	s.Start()
	readied(t, s)
	var got struct {
		PID, SID, PGID int
		TTY, CWD, FD3  string
		Env            []string
	}
	b, err := os.ReadFile(file)
	if err != nil || json.Unmarshal(b, &got) != nil {
		t.Fatalf("%q: %v", b, err)
	}
	if got.SID != got.PID || got.PGID != got.PID || got.TTY != "none" || got.FD3 != "pipe" {
		t.Errorf("session %d, group %d of pid %d, %s controlling terminal, fd 3 %s", got.SID, got.PGID, got.PID, got.TTY, got.FD3)
	}
	if real, _ := filepath.EvalSymlinks(dir); got.CWD != real && got.CWD != dir {
		t.Errorf("cwd %s", got.CWD)
	}
	var state string
	var rest []string
	for _, kv := range got.Env {
		if v, ok := strings.CutPrefix(kv, "AGENTOS_SANDBOX_STATE="); ok {
			state = v
		} else {
			rest = append(rest, kv)
		}
	}
	if want := append(slices.Clone(env), "AGENTOS_SANDBOX_LIFELINE=3"); !slices.Equal(slices.Sorted(slices.Values(rest)), slices.Sorted(slices.Values(want))) {
		t.Errorf("env %q, want %q and a state directory", got.Env, want)
	}
	if !strings.HasPrefix(state, filepath.Join(statedir.Root, statedir.Prefix(StateKind))) {
		t.Errorf("state directory %q", state)
	}
	if _, err := os.Stat(filepath.Join(state, "mine")); err != nil {
		t.Errorf("the Worker's file in its state directory: %v", err)
	}
	var rec pidRecord
	if b, err := os.ReadFile(pidFile); err != nil || json.Unmarshal(b, &rec) != nil {
		t.Fatalf("pid file %q: %v", b, err)
	}
	if start, err := startTime(got.PID); rec.PID != got.PID || rec.Start != start || rec.Generation != 1 || err != nil {
		t.Errorf("pid file %+v, the Worker %d started %s (%v)", rec, got.PID, start, err)
	}
	s.Stop()
	for _, p := range []string{state, pidFile} {
		if _, err := os.Lstat(p); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s is still there: %v", p, err)
		}
	}
}

// agentd is a process of this test binary that supervises a Worker, which this test kills -9.
func agentd(t *testing.T, child string, lifeline bool) (cmd *exec.Cmd, report, pidFile string) {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	report, pidFile = filepath.Join(dir, "report"), filepath.Join(dir, "worker.pid")
	cmd = exec.Command(bin)
	cmd.Env = []string{helperEnv + "=supervise", "AGENTOS_TEST_CHILD=" + child, "AGENTOS_TEST_FILE=" + report,
		"AGENTOS_TEST_PIDFILE=" + pidFile, "AGENTOS_TEST_LIFELINE=" + map[bool]string{true: "1", false: "0"}[lifeline]}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	wait(t, "agentd's Worker", 10*time.Second, func() bool { b, _ := os.ReadFile(report); return string(b) == "ready" })
	return cmd, report, pidFile
}

// agentd killed -9: the lifeline reads EOF, however agentd ended, and nothing else holds its write
// end (not the Worker it passes the read end to).
func TestTheLifelineEndsWithAgentd(t *testing.T) {
	cmd, report, _ := agentd(t, "lifeline", true)
	time.Sleep(200 * time.Millisecond)
	if _, err := os.Stat(report + ".worker"); err == nil {
		t.Fatal("the lifeline read EOF while agentd runs")
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	wait(t, "the Worker to read EOF on its lifeline", 5*time.Second, func() bool {
		b, _ := os.ReadFile(report + ".worker")
		return string(b) == "eof"
	})
}

// A Worker the last agentd left running when it was killed -9 (it has no lifeline, or did not heed
// it) is stopped by Sweep, with its whole process group, and its pid file goes.
func TestSweepStopsTheWorkerTheLastAgentdLeft(t *testing.T) {
	cmd, report, pidFile := agentd(t, "stubborn", false)
	var rec pidRecord
	if b, err := os.ReadFile(pidFile); err != nil || json.Unmarshal(b, &rec) != nil {
		t.Fatalf("pid file %q: %v", b, err)
	}
	var child int
	wait(t, "the Worker's child", 5*time.Second, func() bool {
		b, _ := os.ReadFile(report + ".worker")
		child, _ = strconv.Atoi(string(b))
		return child > 0
	})
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	time.Sleep(200 * time.Millisecond)
	if gone(rec.PID) || gone(child) {
		t.Fatal("the Worker ended with agentd: the test shows nothing")
	}
	var l logs
	if err := Sweep(pidFile, slog.New(slog.NewJSONHandler(&l, nil))); err != nil {
		t.Fatal(err)
	}
	if !gone(rec.PID) {
		t.Errorf("the Worker %d still runs", rec.PID)
	}
	wait(t, "the Worker's child", 5*time.Second, func() bool { return gone(child) })
	if _, err := os.Stat(pidFile); !errors.Is(err, os.ErrNotExist) || len(l.entries("stopped the Worker the last agentd on this data directory left running")) != 1 {
		t.Errorf("pid file %v, logs %v", err, l.entries(""))
	}
}

// Sweep kills nothing but the Worker its pid file names: a pid that is gone, or that another process
// has now, only has the file removed; so does a file it cannot read.
func TestSweepLeavesOtherProcesses(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "worker.pid")
	var l logs
	log := slog.New(slog.NewJSONHandler(&l, nil))
	if err := Sweep(path, log); err != nil {
		t.Fatalf("no file: %v", err)
	}
	other := exec.Command("sleep", "60")
	other.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := other.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Process.Kill(); _ = other.Wait() })
	ended := exec.Command("true")
	if err := ended.Run(); err != nil {
		t.Fatal(err)
	}
	start, err := startTime(other.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"garbage":            "{not json",
		"no start":           fmt.Sprintf(`{"pid": %d}`, other.Process.Pid),
		"a pid that is gone": fmt.Sprintf(`{"pid": %d, "start": "1.000000"}`, ended.Process.Pid),
		"a pid another has":  fmt.Sprintf(`{"pid": %d, "start": "%s0"}`, other.Process.Pid, start),
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := Sweep(path, log); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s: the file stays", name)
		}
		if gone(other.Process.Pid) {
			t.Fatalf("%s: killed another process", name)
		}
	}
}

// startTime tells processes apart: the same process gives the same answer, another a different one,
// and a process that has ended (a zombie not reaped yet included) none.
func TestStartTime(t *testing.T) {
	me, err := startTime(os.Getpid())
	if again, err2 := startTime(os.Getpid()); err != nil || err2 != nil || again != me {
		t.Fatalf("%q, %q: %v %v", me, again, err, err2)
	}
	zombie := exec.Command("true")
	if err := zombie.Start(); err != nil {
		t.Fatal(err)
	}
	wait(t, "a zombie", 5*time.Second, func() bool { return gone(zombie.Process.Pid) }) // not reaped
	if err := syscall.Kill(zombie.Process.Pid, 0); err != nil {
		t.Errorf("the zombie is not there to be told apart: %v", err)
	}
	_ = zombie.Wait()
	child := exec.Command("sleep", "60")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	if theirs, err := startTime(child.Process.Pid); err != nil || theirs == me {
		t.Errorf("a child's start %q, %v; mine %q", theirs, err, me)
	}
}

// A Worker's exit record is one agentctl audit verify reads past: whatever the exit, its payload
// decodes into the fields audit.Projection reads of every record (its keys are none of them), and
// holds no number that is not an integer. The rest of verify skips the type (internal/control's
// TestVerifyAuditOfTheWorkersExits), and runs explain's starts are agentd.started only.
func TestTheExitRecord(t *testing.T) {
	at := time.Date(2026, 10, 25, 12, 0, 0, 123456789, time.FixedZone("x", 3600))
	for name, e := range map[string]exit{
		"an exit code":            {generation: 3, pid: 42, code: 1, ran: 1500 * time.Millisecond, ready: true, at: at, restartIn: 2 * time.Second},
		"a signal":                {generation: 4, pid: 43, signal: "killed", ran: time.Minute, ready: true, at: at, restartIn: time.Second},
		"never started":           {generation: 5, startError: "fork/exec /x: no such file or directory", at: at, restartIn: 4 * time.Second},
		"the first, agentd stops": {generation: 1, pid: 44, code: 2, at: at},
	} {
		ev := e.Event(7)
		if ev.Type != ExitedType || ev.Actor != audit.Agentd || ev.WorkflowID != "" {
			t.Errorf("%s: %+v", name, ev)
		}
		b, err := json.Marshal(ev.Payload)
		if err != nil {
			t.Fatal(err)
		}
		var p audit.Projection
		if err := p.Apply(db.AuditLog{Seq: 1, Type: ev.Type, Actor: ev.Actor, Payload: string(b)}); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		var fields map[string]any
		if err := dec.Decode(&fields); err != nil {
			t.Fatal(err)
		}
		for k, v := range fields {
			if n, ok := v.(json.Number); ok {
				if _, err := n.Int64(); err != nil {
					t.Errorf("%s: %s is %s", name, k, n)
				}
			}
		}
		if fields["unrecorded"] != json.Number("7") || fields["exited_at"] != "2026-10-25T11:00:00.123456789Z" ||
			(e.restartIn == 0) != (fields["restart_in_ms"] == nil) {
			t.Errorf("%s: %s", name, b)
		}
	}
}
