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
	"sync/atomic"
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

// rootEnv names the test binary's pid to the helpers it starts: each ends when that process does, so
// that a test that fails or times out (or a mutant's) leaves none behind. They run in sessions of
// their own, where nothing else would end them.
const rootEnv = "AGENTOS_TEST_ROOT"

// helper is a Worker: it writes its output to stdout and stderr as a Worker does, and ends as the
// mode says; as the real Worker does, it stops when its stdin (the stop channel) ends or on
// SIGTERM, unless its mode is deaf to them. AGENTOS_TEST_FILE is where it reports.
func helper(mode string) {
	if root, _ := strconv.Atoi(os.Getenv(rootEnv)); root > 0 {
		go func() {
			for {
				time.Sleep(200 * time.Millisecond)
				if errors.Is(syscall.Kill(root, 0), syscall.ESRCH) {
					os.Exit(4)
				}
			}
		}()
	}
	file := os.Getenv("AGENTOS_TEST_FILE")
	ready := func() {
		fmt.Fprintln(os.Stderr, "2026-10-25 12:00:00,000 INFO root worker ready: endpoint=x task_queue=y")
	}
	// hold runs until the stop channel ends (if heeded) or SIGTERM (if heeded), then exits 0.
	hold := func(stdin, term bool) {
		if stdin {
			go func() {
				_, _ = io.Copy(io.Discard, os.Stdin)
				os.Exit(0)
			}()
		}
		if term {
			c := make(chan os.Signal, 1)
			signal.Notify(c, syscall.SIGTERM)
			<-c
			os.Exit(0)
		}
		signal.Ignore(syscall.SIGTERM)
		time.Sleep(time.Hour)
	}
	child := func() int { // a child in this process group, reported
		c := exec.Command("/bin/sleep", "30") // a Worker has no PATH here
		if err := c.Start(); err != nil {
			os.Exit(1)
		}
		_ = os.WriteFile(file, []byte(strconv.Itoa(c.Process.Pid)), 0o600)
		return c.Process.Pid
	}
	switch mode {
	case "ready":
		ready()
		hold(true, true)
	case "exit":
		fmt.Println("starting")
		fmt.Fprintln(os.Stderr, "ModuleNotFoundError: No module named 'x'")
		os.Exit(3)
	case "never":
		fmt.Println("connecting")
		hold(true, true)
	case "stdin-only": // ready; stops when its stdin ends, deaf to SIGTERM
		ready()
		hold(true, false)
	case "deaf-stdin": // ready; does not read its stdin, stops on SIGTERM
		ready()
		hold(false, true)
	case "stubborn": // ready, deaf to its stop channel and to SIGTERM, with a child in its process group
		child()
		ready()
		hold(false, false)
	case "leaves-child": // ready, then exits, leaving a child in its process group
		child()
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
		hold(true, true)
	case "report":
		report(file, false)
		ready()
		hold(true, true)
	case "lifeline": // as the macOS launcher's watcher: ends when fd 3 reads EOF
		ready()
		_, _ = io.Copy(io.Discard, os.NewFile(3, "lifeline"))
		_ = os.WriteFile(file, []byte("eof"), 0o600)
	case "holds-output": // a process of its own session that keeps the output open, at most a while
		time.Sleep(30 * time.Second)
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
		case "early": // exits before it says it is ready
			fmt.Println("starting")
			os.Exit(1)
		case "never": // never says it is ready
			fmt.Println("connecting")
			hold(true, true)
		case "long": // ready, up a while, then exits
			ready()
			time.Sleep(500 * time.Millisecond)
			os.Exit(1)
		case "slow": // ready only after 600 ms, then stays
			time.Sleep(600 * time.Millisecond)
			ready()
			hold(true, true)
		case "late-long": // ready late, up long enough only counted from its start, then exits
			time.Sleep(300 * time.Millisecond)
			ready()
			time.Sleep(250 * time.Millisecond)
			os.Exit(1)
		case "report-re": // reports where it runs, ready, then exits
			report(file+".gen", true)
			ready()
			os.Exit(1)
		case "report": // reports where it runs, and stays
			report(file+".gen", true)
			ready()
			hold(true, true)
		case "escapes": // leaves a process of its own session behind with the output, ready, then exits
			bin, _ := os.Executable()
			c := exec.Command(bin)
			c.Env = []string{helperEnv + "=holds-output", rootEnv + "=" + os.Getenv(rootEnv)}
			c.Stdout, c.Stderr = os.Stdout, os.Stderr
			c.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
			if err := c.Start(); err != nil {
				os.Exit(1)
			}
			_ = os.WriteFile(file+".escaped", []byte(strconv.Itoa(c.Process.Pid)), 0o600)
			ready()
			os.Exit(1)
		default:
			ready()
			hold(true, true)
		}
	case "supervise":
		supervise()
	}
}

// report writes where the helper runs, and what it was given, to path: as the whole file, or as one
// more line of it.
func report(path string, appendLine bool) {
	sid, _ := unix.Getsid(0)
	cwd, _ := os.Getwd()
	tty := "none"
	if f, err := os.Open("/dev/tty"); err == nil {
		tty, _ = "one", f.Close()
	}
	fd := func(n int) string {
		var st unix.Stat_t
		if unix.Fstat(n, &st) != nil {
			return "closed"
		}
		if st.Mode&unix.S_IFMT == unix.S_IFIFO {
			return "pipe"
		}
		return "open"
	}
	state := os.Getenv("AGENTOS_SANDBOX_STATE")
	if state != "" {
		_ = os.WriteFile(filepath.Join(state, "mine"), []byte("x"), 0o600)
	}
	b, _ := json.Marshal(map[string]any{"pid": os.Getpid(), "sid": sid, "pgid": syscall.Getpgrp(), "tty": tty, "cwd": cwd,
		"env": os.Environ(), "fd0": fd(0), "fd3": fd(3), "state": state})
	if !appendLine {
		_ = os.WriteFile(path, b, 0o600)
		return
	}
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	fmt.Fprintln(f, string(b))
	f.Close()
}

// supervise is an agentd that runs a supervisor of the Worker AGENTOS_TEST_CHILD names, with a
// lifeline if AGENTOS_TEST_LIFELINE says so, and AGENTOS_TEST_PIDFILE: it writes "ready" to
// AGENTOS_TEST_FILE once its Worker is, and runs until killed.
func supervise() {
	file := os.Getenv("AGENTOS_TEST_FILE")
	child := os.Getenv("AGENTOS_TEST_CHILD")
	bin, _ := os.Executable()
	s := New(Options{Argv: []string{bin}, Env: []string{helperEnv + "=" + child, "AGENTOS_TEST_FILE=" + file + ".worker",
		rootEnv + "=" + os.Getenv(rootEnv)}, Lifeline: os.Getenv("AGENTOS_TEST_LIFELINE") == "1",
		PIDFile: os.Getenv("AGENTOS_TEST_PIDFILE")})
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
	s := New(Options{Argv: []string{bin}, Env: append([]string{helperEnv + "=" + mode, "AGENTOS_TEST_FILE=" + file, root()}, env...),
		Dir: t.TempDir(), Log: slog.New(slog.NewJSONHandler(&l, nil)), Record: r.add, Timing: timing})
	t.Cleanup(s.Stop)
	return s, &l, &r, file
}

// root names this test binary to a helper: it ends when the test binary does.
func root() string { return rootEnv + "=" + strconv.Itoa(os.Getpid()) }

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
		pid := int(l.entries("worker started")[0]["pid"].(float64))
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

// Stop: no Worker after it. The running one is asked to stop (its stop channel, stdin, ends); after
// the grace it gets SIGTERM; after a little more SIGKILL, with its whole process group. A stop while
// the supervisor waits to restart returns at once.
func TestStop(t *testing.T) {
	stops := func(t *testing.T, mode string, timing Timing, status string, least, most time.Duration) *logs {
		t.Helper()
		s, l, _, _ := worker(t, mode, timing)
		s.Start()
		readied(t, s)
		_, pid := s.Current()
		start := time.Now()
		s.Stop()
		if took := time.Since(start); took < least || took > most {
			t.Errorf("Stop took %s, want %s to %s", took, least, most)
		}
		stopped := l.entries("worker stopped")
		if len(stopped) != 1 || stopped[0]["status"] != status || !gone(pid) || len(l.entries("worker exited")) != 0 {
			t.Errorf("logs %v", l.entries(""))
		}
		if _, now := s.Current(); now != 0 {
			t.Errorf("a pid %d once it is gone", now)
		}
		time.Sleep(100 * time.Millisecond)
		if len(l.entries("worker started")) != 1 {
			t.Error("a Worker started after Stop")
		}
		return l
	}
	t.Run("when asked", func(t *testing.T) { // its stop channel: no signal, which it would not heed
		l := stops(t, "stdin-only", Timing{StopGrace: 3 * time.Second}, "exit status 0", 0, 2*time.Second)
		if len(l.entries("the Worker did not stop when asked: SIGTERM")) != 0 {
			t.Error("SIGTERM for a Worker that stopped when asked")
		}
	})
	t.Run("on SIGTERM, deaf to its stop channel", func(t *testing.T) {
		stops(t, "deaf-stdin", Timing{StopGrace: 300 * time.Millisecond, TermGrace: 3 * time.Second}, "exit status 0",
			300*time.Millisecond, 2*time.Second)
	})
	t.Run("deaf to both", func(t *testing.T) {
		s, l, _, file := worker(t, "stubborn", Timing{StopGrace: 200 * time.Millisecond, TermGrace: 200 * time.Millisecond})
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
		stopped := make(chan struct{})
		go func() {
			s.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second): // the graces are 400 ms in all
			// Killed here, the Worker's group lets Stop return, and the test's cleanup after it.
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			<-stopped
			t.Fatal("Stop did not return within 5 s: the Worker deaf to its stop channel and SIGTERM was not killed")
		}
		if took := time.Since(start); took < 400*time.Millisecond {
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

// The supervisor never gives up after the first generation: one that exits before it says it is
// ready is followed by the next, and so is one whose launcher cannot start; and only the first must
// say it is ready in time.
func TestLaterGenerations(t *testing.T) {
	t.Run("exit before they are ready", func(t *testing.T) {
		s, l, _, _ := worker(t, "planned", Timing{BackoffMin: 10 * time.Millisecond, BackoffMax: 10 * time.Millisecond},
			"AGENTOS_TEST_PLAN=re,early,early,ready")
		s.Start()
		readied(t, s)
		wait(t, "the fourth generation, ready", 10*time.Second, func() bool { return len(l.entries("worker ready")) == 2 })
		if gen, _ := s.Current(); gen != 4 || len(l.entries("worker exited")) != 3 {
			t.Errorf("generation %d, logs %v", gen, l.entries(""))
		}
		select {
		case err := <-s.Failed():
			t.Errorf("a later generation failed the supervisor: %v", err)
		default:
		}
	})
	t.Run("cannot start", func(t *testing.T) {
		bin, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		launcher := filepath.Join(dir, "launcher")
		if err := os.Symlink(bin, launcher); err != nil {
			t.Fatal(err)
		}
		var l logs
		s := New(Options{Argv: []string{launcher}, Env: []string{helperEnv + "=ready", root()}, Log: slog.New(slog.NewJSONHandler(&l, nil)),
			Timing: Timing{BackoffMin: 20 * time.Millisecond, BackoffMax: 80 * time.Millisecond}})
		t.Cleanup(s.Stop)
		s.Start()
		readied(t, s)
		_, pid := s.Current()
		if err := os.Remove(launcher); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		wait(t, "starts that fail", 10*time.Second, func() bool { return len(l.entries("could not start the Worker")) >= 4 })
		var delays []string
		for _, e := range l.entries("could not start the Worker")[:4] {
			delays = append(delays, fmt.Sprint(e["restart_in"]))
		}
		if want := []string{"40ms", "80ms", "80ms", "80ms"}; !slices.Equal(delays, want) { // after the exit's own 20ms
			t.Errorf("delays %q, want %q", delays, want)
		}
		if err := os.Symlink(bin, launcher); err != nil {
			t.Fatal(err)
		}
		wait(t, "a Worker again", 10*time.Second, func() bool { return len(l.entries("worker ready")) == 2 })
		select {
		case err := <-s.Failed():
			t.Errorf("a later generation failed the supervisor: %v", err)
		default:
		}
	})
	t.Run("are never ready", func(t *testing.T) {
		s, l, _, _ := worker(t, "planned", Timing{ReadyWithin: 200 * time.Millisecond, BackoffMin: 10 * time.Millisecond},
			"AGENTOS_TEST_PLAN=re,never")
		s.Start()
		readied(t, s)
		wait(t, "the second generation", 5*time.Second, func() bool { return len(l.entries("worker started")) == 2 })
		time.Sleep(600 * time.Millisecond)
		if _, pid := s.Current(); pid == 0 || len(l.entries("worker not ready")) != 0 {
			t.Errorf("the second generation was stopped: %v", l.entries(""))
		}
		select {
		case err := <-s.Failed():
			t.Errorf("a later generation failed the supervisor: %v", err)
		default:
		}
	})
}

// A generation whose output a process outside its group keeps open (one that started a session of
// its own) does not hold the supervisor up: after the drain, the output is closed, the next
// generation starts, and Stop returns.
func TestTheDrainEnds(t *testing.T) {
	s, l, _, file := worker(t, "planned", Timing{Drain: 300 * time.Millisecond, BackoffMin: 10 * time.Millisecond},
		"AGENTOS_TEST_PLAN=escapes,ready")
	t.Cleanup(func() {
		if b, err := os.ReadFile(file + ".escaped"); err == nil {
			if pid, _ := strconv.Atoi(string(b)); pid > 0 {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})
	s.Start()
	readied(t, s)
	wait(t, "the second generation", 10*time.Second, func() bool { return len(l.entries("worker ready")) == 2 })
	if len(l.entries("the Worker's output is still open after its process group ended")) != 1 {
		t.Errorf("logs %v", l.entries(""))
	}
	start := time.Now()
	s.Stop()
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("Stop took %s", took)
	}
}

// No generation starts once Stop has been called, however close the next start was: launch looks at
// the stop channel under the lock that Stop closes it under, and then starts the launcher. The test
// stops the supervisor in the window a timing rarely hits: once the delay before the second
// generation is over (so pause has gone by), before its launcher starts.
func TestNoGenerationAfterStop(t *testing.T) {
	s, l, r, _ := worker(t, "planned", Timing{BackoffMin: 10 * time.Millisecond}, "AGENTOS_TEST_PLAN=re")
	stopped := make(chan struct{})
	s.beforeStart = func(n int) {
		if n != 2 {
			return
		}
		go func() {
			s.Stop()
			close(stopped)
		}()
		for deadline := time.Now().Add(5 * time.Second); !closed(s.stop) && time.Now().Before(deadline); {
			time.Sleep(time.Millisecond)
		}
	}
	s.Start()
	readied(t, s)
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return")
	}
	if n := len(l.entries("worker started")); n != 1 {
		t.Errorf("%d generations started, the last after Stop: %v", n, l.entries(""))
	}
	// Nor is the start that Stop prevented taken for one that failed: one record, of the first's exit.
	if events, _ := r.all(); len(events) != 1 || len(l.entries("could not start the Worker")) != 0 {
		t.Errorf("records %v, logs %v", events, l.entries(""))
	}
}

// While the hold (agentd's fault script) lasts, no generation after the first starts, however short
// the delay: a window with no Worker running. The next starts once the hold is over; a stop during
// it starts none.
func TestTheHold(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(fmt.Sprintf("stopped during it %v", stop), func(t *testing.T) {
			var held atomic.Bool
			held.Store(true)
			s, l, _, _ := worker(t, "planned", Timing{BackoffMin: 10 * time.Millisecond}, "AGENTOS_TEST_PLAN=re,ready")
			s.o.Hold = held.Load
			s.Start()
			readied(t, s)
			wait(t, "the first generation's exit", 5*time.Second, func() bool { return len(l.entries("worker exited")) == 1 })
			time.Sleep(300 * time.Millisecond)
			if n := len(l.entries("worker started")); n != 1 || len(l.entries("the fault script holds the Worker's next start")) != 1 {
				t.Fatalf("%d generations started during the hold: %v", n, l.entries(""))
			}
			if stop {
				start := time.Now()
				s.Stop()
				if took := time.Since(start); took > time.Second || len(l.entries("worker started")) != 1 {
					t.Errorf("Stop took %s; logs %v", took, l.entries(""))
				}
				return
			}
			held.Store(false)
			wait(t, "the second generation", 5*time.Second, func() bool { return len(l.entries("worker ready")) == 2 })
		})
	}
}

// fakeTally is a generation's tally as a test sets it (the Runtime proxy's, in agentd).
type fakeTally struct {
	mu                    sync.Mutex
	polls                 int
	quietSince, heldSince time.Time
}

func (f *fakeTally) Liveness() (int, time.Time, time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.polls, f.quietSince, f.heldSince
}

// tallies makes each generation's tally: quiet from its start, with the polls polls gives for the
// generation (1-based), and a task held from its start if held says so.
type tallies struct {
	mu    sync.Mutex
	n     int
	polls func(n int) int
	held  bool
}

func (ts *tallies) new() Tally {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.n++
	f := &fakeTally{quietSince: time.Now()}
	if ts.polls != nil {
		f.polls = ts.polls(ts.n)
	}
	if ts.held {
		f.heldSince = time.Now()
	}
	return f
}

// livenessTiming is the liveness thresholds at 300 ms, checked every 50 ms.
var livenessTiming = Timing{QuietFor: 300 * time.Millisecond, HoldFor: 300 * time.Millisecond, CheckEvery: 50 * time.Millisecond,
	BackoffMin: 10 * time.Millisecond, BackoffMax: 10 * time.Millisecond}

// A generation that has had no poll open for QuietFor, or has left a task it was handed unanswered
// for HoldFor, is stopped, logged, recorded, and started again.
func TestAnUnresponsiveWorkerIsStartedAgain(t *testing.T) {
	for _, c := range []struct {
		rule string
		ts   *tallies
	}{
		{"not_polling", &tallies{}},
		{"not_answering", &tallies{polls: func(int) int { return 1 }, held: true}},
	} {
		t.Run(c.rule, func(t *testing.T) {
			s, l, r, _ := worker(t, "ready", livenessTiming)
			s.o.NewTally = c.ts.new
			s.Start()
			readied(t, s)
			wait(t, "the second generation", 10*time.Second, func() bool { return len(l.entries("worker started")) == 2 })
			found := l.entries("the Worker is not serving")
			if len(found) == 0 || found[0]["rule"] != c.rule || found[0]["generation"] != float64(1) {
				t.Fatalf("logs %v", l.entries(""))
			}
			if d, err := time.ParseDuration(found[0]["for"].(string)); err != nil || d < 300*time.Millisecond {
				t.Errorf("for %v (%v), want 300ms or more", found[0]["for"], err)
			}
			exited := l.entries("worker exited")
			if len(exited) == 0 || exited[0]["unresponsive"] != c.rule || exited[0]["restart_in"] == nil {
				t.Errorf("exits %v", exited)
			}
			wait(t, "the record", 5*time.Second, func() bool { events, _ := r.all(); return len(events) > 0 })
			events, _ := r.all()
			if p, _ := events[0].Payload.(map[string]any); p["unresponsive"] != c.rule || p["unresponsive_ms"].(int64) < 300 || p["generation"] != 1 {
				t.Errorf("record %v", p)
			}
		})
	}
}

// A generation that keeps a poll open is alive, however long it makes no new call: a long poll lasts.
func TestOpenPollsAreAlive(t *testing.T) {
	ts := &tallies{polls: func(int) int { return 1 }}
	s, l, _, _ := worker(t, "ready", livenessTiming)
	s.o.NewTally = ts.new
	s.Start()
	readied(t, s)
	time.Sleep(2 * livenessTiming.QuietFor)
	if len(l.entries("worker started")) != 1 || len(l.entries("the Worker is not serving")) != 0 {
		t.Errorf("logs %v", l.entries(""))
	}
}

// The first generation is checked only once it is ready, ReadyWithin covering it till then: one that
// is quiet for longer than QuietFor before it is ready is not stopped for that then.
func TestTheFirstIsCheckedOnceReady(t *testing.T) {
	ts := &tallies{}
	s, l, _, _ := worker(t, "planned", livenessTiming, "AGENTOS_TEST_PLAN=slow")
	s.o.NewTally = ts.new
	s.Start()
	readied(t, s)
	wait(t, "a check after it was ready", 5*time.Second, func() bool { return len(l.entries("the Worker is not serving")) > 0 })
	var msgs []string
	var times []time.Time
	for _, e := range l.entries("") {
		if m := e["msg"].(string); m == "worker ready" || m == "the Worker is not serving" {
			msgs = append(msgs, m)
			at, _ := time.Parse(time.RFC3339Nano, e["time"].(string))
			times = append(times, at)
		}
	}
	if msgs[0] != "worker ready" {
		t.Errorf("checked before it was ready: %v", msgs)
	}
	// Its quiet counts from its readiness: quiet since its start, it is not stopped at the first check.
	if gap := times[1].Sub(times[0]); gap < livenessTiming.QuietFor {
		t.Errorf("stopped %s after it was ready, want %s or more", gap, livenessTiming.QuietFor)
	}
}

// When both rules hold, the one reported is not_answering: the Worker was handed a task it holds.
func TestBothRulesHold(t *testing.T) {
	ts := &tallies{held: true} // no poll open, and a task held, both since its start
	s, l, _, _ := worker(t, "ready", livenessTiming)
	s.o.NewTally = ts.new
	s.Start()
	readied(t, s)
	wait(t, "the check", 5*time.Second, func() bool { return len(l.entries("the Worker is not serving")) > 0 })
	if found := l.entries("the Worker is not serving"); found[0]["rule"] != "not_answering" {
		t.Errorf("rule %v, want not_answering", found[0]["rule"])
	}
}

// Each generation's tally is made before its launcher starts: a connection it makes counts in it.
func TestTheTallyComesBeforeTheStart(t *testing.T) {
	var mu sync.Mutex
	started, seen := 0, []int{}
	s, l, _, _ := worker(t, "planned", Timing{BackoffMin: 10 * time.Millisecond}, "AGENTOS_TEST_PLAN=re,re,ready")
	s.startCmd = func(c *exec.Cmd) error { mu.Lock(); started++; mu.Unlock(); return c.Start() }
	s.o.NewTally = func() Tally {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, started) // the generations started when this one's tally was made
		return &fakeTally{polls: 1, quietSince: time.Now()}
	}
	s.Start()
	readied(t, s)
	wait(t, "the third generation", 10*time.Second, func() bool { return len(l.entries("worker ready")) == 3 })
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(seen[:3], []int{0, 1, 2}) {
		t.Errorf("tallies made with %v generations started, want [0 1 2]", seen)
	}
}

// A later generation that never polls is stopped and started again: only the first fails agentd.
func TestALaterGenerationThatNeverPolls(t *testing.T) {
	ts := &tallies{polls: func(n int) int { return map[bool]int{true: 1}[n == 1] }} // the first polls, the rest never
	s, l, _, _ := worker(t, "planned", livenessTiming, "AGENTOS_TEST_PLAN=re,ready")
	s.o.NewTally = ts.new
	s.Start()
	readied(t, s)
	wait(t, "the third generation", 10*time.Second, func() bool { return len(l.entries("worker started")) >= 3 })
	found := l.entries("the Worker is not serving")
	if len(found) == 0 || found[0]["generation"] != float64(2) || found[0]["rule"] != "not_polling" {
		t.Errorf("logs %v", l.entries(""))
	}
	select {
	case err := <-s.Failed():
		t.Errorf("failed: %v", err)
	default:
	}
}

// A Stop that comes while the supervisor stops an unresponsive generation starts no other.
func TestAStopDuringACheckStartsNothing(t *testing.T) {
	ts := &tallies{}
	timing := livenessTiming
	timing.StopGrace = 300 * time.Millisecond // it does not read its stop channel: SIGTERM after this
	s, l, r, _ := worker(t, "deaf-stdin", timing)
	s.o.NewTally = ts.new
	var once sync.Once
	s.terminating = func() { // the Stop comes as the check's stop begins, however loaded the machine
		once.Do(func() {
			go s.Stop()
			for !closed(s.stop) {
				time.Sleep(time.Millisecond)
			}
		})
	}
	s.Start()
	readied(t, s)
	wait(t, "the check", 5*time.Second, func() bool { return len(l.entries("the Worker is not serving")) > 0 })
	s.Stop() // returns once the supervisor has
	time.Sleep(100 * time.Millisecond)
	if n := len(l.entries("worker started")); n != 1 {
		t.Errorf("%d generations started: %v", n, l.entries(""))
	}
	// Its record says why it was stopped, and no restart, which there was none of.
	events, _ := r.all()
	if p, _ := events[0].Payload.(map[string]any); len(events) != 1 || p["unresponsive"] != "not_polling" || p["restart_in_ms"] != nil {
		t.Errorf("records %v", events)
	}
}

// The defaults are the ones Timing's comments, and README.md, give.
func TestTheDefaults(t *testing.T) {
	want := Timing{ReadyWithin: time.Minute, BackoffMin: time.Second, BackoffMax: time.Minute, ResetAfter: time.Minute,
		StopGrace: 5 * time.Second, TermGrace: time.Second, RecordEvery: time.Minute, Drain: 5 * time.Second,
		QuietFor: time.Minute, HoldFor: time.Minute, CheckEvery: 5 * time.Second}
	if got := (Timing{}).WithDefaults(); got != want {
		t.Errorf("%+v, want %+v", got, want)
	}
}

// listFDs, in sh, prints the shell's open file descriptors of 0 to 40, without opening one.
const listFDs = `for fd in $(seq 0 40); do [ -e /dev/fd/$fd ] && printf "%s " $fd; done; echo`

// The Worker gets stdin (its stop channel), stdout and stderr, and fd 3 with a lifeline: nothing else
// of agentd's. A child agentd starts beside it gets none of either pipe's ends, so the stop channel
// ends for the Worker while that child still runs.
func TestTheWorkerGetsNoOtherFiles(t *testing.T) {
	for _, lifeline := range []bool{false, true} {
		t.Run(fmt.Sprintf("lifeline %v", lifeline), func(t *testing.T) {
			var l logs
			s := New(Options{Argv: []string{"/bin/sh", "-c", "printf 'fds: '; " + listFDs + "; echo 'worker ready'; exec cat >/dev/null"},
				Env: []string{"PATH=/usr/bin:/bin"}, Dir: t.TempDir(), Lifeline: lifeline,
				Log: slog.New(slog.NewJSONHandler(&l, nil)), Timing: Timing{StopGrace: 3 * time.Second}})
			t.Cleanup(s.Stop)
			s.Start()
			readied(t, s)
			// A child of agentd's own, started while the generation runs, as a git tool server is.
			sibling := exec.Command("/bin/sh", "-c", "printf 'fds: '; "+listFDs+"; exec sleep 30")
			sibling.Env = []string{"PATH=/usr/bin:/bin"}
			out, err := sibling.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := sibling.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = sibling.Process.Kill()
				_ = sibling.Wait()
			})
			theirs, err := bufio.NewReader(out).ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			s.Stop() // while the sibling runs
			took := time.Since(start)
			want := "fds: 0 1 2"
			if lifeline {
				want += " 3"
			}
			var mine string
			for _, e := range l.entries("worker output") {
				if line, _ := e["line"].(string); strings.HasPrefix(line, "fds:") {
					mine = strings.TrimSpace(line)
				}
			}
			if mine != want {
				t.Errorf("the Worker's files: %q, want %q", mine, want)
			}
			if theirs = strings.TrimSpace(theirs); theirs != "fds: 0 1 2" {
				t.Errorf("the sibling's files: %q, want %q", theirs, "fds: 0 1 2")
			}
			if took > 2*time.Second || len(l.entries("the Worker did not stop when asked: SIGTERM")) != 0 {
				t.Errorf("the stop channel did not end for the Worker while the sibling ran: Stop took %s", took)
			}
		})
	}
}

// openFiles counts this process's open file descriptors, without opening one to count them.
func openFiles() int {
	n := 0
	for fd := range 4096 {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err == nil {
			n++
		}
	}
	return n
}

// Each generation's files are closed once it is over, whether it exited or was stopped: generations
// that crash, one after another, leave agentd no more files open than it had before them.
func TestEachGenerationClosesItsFiles(t *testing.T) {
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	var l logs
	s := New(Options{Argv: []string{bin}, Env: []string{helperEnv + "=planned", "AGENTOS_TEST_FILE=" + filepath.Join(dir, "report"), root(),
		"AGENTOS_TEST_PLAN=re,re,re,re,re,ready"}, Lifeline: true, PIDFile: filepath.Join(dir, "worker.pid"),
		Log: slog.New(slog.NewJSONHandler(&l, nil)), Timing: Timing{BackoffMin: 10 * time.Millisecond, BackoffMax: 10 * time.Millisecond}})
	t.Cleanup(s.Stop)
	if r, w, err := os.Pipe(); err == nil { // the runtime's poller opens a descriptor of its own with the first pipe
		r.Close()
		w.Close()
	}
	before := openFiles()
	s.Start()
	readied(t, s)
	wait(t, "the sixth generation", 10*time.Second, func() bool { return len(l.entries("worker ready")) == 6 })
	s.Stop()
	if after := openFiles(); after > before {
		t.Errorf("%d files open after five generations exited and a sixth was stopped, %d before", after, before)
	}
}

// Each generation has its own pid file, state directory and lifeline, whichever it is.
func TestEachGenerationHasItsOwn(t *testing.T) {
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	file, pidFile := filepath.Join(dir, "report"), filepath.Join(dir, "worker.pid")
	var l logs
	s := New(Options{Argv: []string{bin}, Env: []string{helperEnv + "=planned", "AGENTOS_TEST_FILE=" + file, root(),
		"AGENTOS_TEST_PLAN=report-re,report"}, StateDirs: true, Lifeline: true, PIDFile: pidFile,
		Log: slog.New(slog.NewJSONHandler(&l, nil)), Timing: Timing{BackoffMin: 10 * time.Millisecond}})
	t.Cleanup(s.Stop)
	s.Start()
	readied(t, s)
	wait(t, "the second generation", 10*time.Second, func() bool { return len(l.entries("worker ready")) == 2 })
	b, err := os.ReadFile(file + ".gen")
	if err != nil {
		t.Fatal(err)
	}
	var gens []struct {
		PID      int
		FD0, FD3 string
		State    string
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
		var g struct {
			PID      int
			FD0, FD3 string
			State    string
		}
		if err := json.Unmarshal([]byte(line), &g); err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		gens = append(gens, g)
	}
	if len(gens) != 2 {
		t.Fatalf("reports %+v", gens)
	}
	second := gens[1]
	if second.FD0 != "pipe" || second.FD3 != "pipe" || second.State == "" || second.State == gens[0].State {
		t.Errorf("the second generation %+v, the first %+v", second, gens[0])
	}
	if _, err := os.Lstat(gens[0].State); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the first generation's state directory %s stays: %v", gens[0].State, err)
	}
	var rec pidRecord
	if b, err := os.ReadFile(pidFile); err != nil || json.Unmarshal(b, &rec) != nil {
		t.Fatalf("pid file %q: %v", b, err)
	}
	if start, err := startTime(second.PID); err != nil || rec.PID != second.PID || rec.Generation != 2 || rec.Start != start {
		t.Errorf("pid file %+v, the second generation %d started %s (%v)", rec, second.PID, start, err)
	}
}

// How long a generation was up, for the delays to start over, counts from its start, not from when it
// said it was ready: a launcher's own start (srt's, bwrap's) is part of the Worker's.
func TestTheDelaysStartOverCountedFromTheStart(t *testing.T) {
	s, l, _, _ := worker(t, "planned", Timing{BackoffMin: 20 * time.Millisecond, BackoffMax: time.Second,
		ResetAfter: 400 * time.Millisecond}, "AGENTOS_TEST_PLAN=re,late-long,re,ready")
	s.Start()
	readied(t, s)
	wait(t, "the fourth generation", 10*time.Second, func() bool { return len(l.entries("worker ready")) == 4 })
	var restarts []string
	for _, e := range l.entries("worker exited") {
		restarts = append(restarts, fmt.Sprint(e["restart_in"]))
	}
	// up 550 ms from its start, 250 ms from its readiness: the delays start over
	if want := []string{"20ms", "20ms", "40ms"}; !slices.Equal(restarts, want) {
		t.Errorf("restarts %q, want %q", restarts, want)
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
		FD0            string
		Env            []string
	}
	b, err := os.ReadFile(file)
	if err != nil || json.Unmarshal(b, &got) != nil {
		t.Fatalf("%q: %v", b, err)
	}
	if got.SID != got.PID || got.PGID != got.PID || got.TTY != "none" || got.FD3 != "pipe" || got.FD0 != "pipe" {
		t.Errorf("session %d, group %d of pid %d, %s controlling terminal, fd 3 %s, stdin %s", got.SID, got.PGID, got.PID,
			got.TTY, got.FD3, got.FD0)
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
		"AGENTOS_TEST_PIDFILE=" + pidFile, "AGENTOS_TEST_LIFELINE=" + map[bool]string{true: "1", false: "0"}[lifeline], root()}
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
		time.Sleep(200 * time.Millisecond) // a SIGKILL takes a moment to land
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
		"unresponsive": {generation: 2, pid: 45, signal: "killed", ran: 2 * time.Minute, ready: true, at: at, restartIn: time.Second,
			unresponsive: "not_answering", unresponsiveFor: 61500 * time.Millisecond},
		"over its memory": {generation: 6, pid: 46, signal: "terminated", ran: time.Minute, ready: true, at: at, restartIn: time.Second,
			oomKill: true},
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
		if e.unresponsive == "" && (fields["unresponsive"] != nil || fields["unresponsive_ms"] != nil) ||
			e.unresponsive != "" && (fields["unresponsive"] != e.unresponsive || fields["unresponsive_ms"] != json.Number("61500")) {
			t.Errorf("%s: %s", name, b)
		}
		if o, ok := fields["oom_kill"]; e.oomKill != ok || ok && o != true {
			t.Errorf("%s: %s", name, b)
		}
	}
}

// limits are the Worker's resource limits as a test sets them: each generation's scope is named after
// it, and the kernel killed the generations killed names (OOMKilled answers err for every one, if set).
type limits struct {
	mu      sync.Mutex
	killed  map[int64]bool
	err     error
	asked   []string       // the scopes OOMKilled was asked about
	outside map[int64]bool // the generations whose launcher is not in its scope
	checked []int          // the pids InScope was asked about
	// vanishing reads a launcher's cgroup as /proc does once its pid is reaped: it waits for that, a
	// while, and fails if it came; a launcher that stays is in its scope.
	vanishing bool
}

func (l *limits) InScope(pid int, unit string) error {
	if l.vanishing {
		for deadline := time.Now().Add(500 * time.Millisecond); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
				return fmt.Errorf("cannot tell the cgroup of the process agentd started for the worker (pid %d): open /proc/%d/cgroup: %w",
					pid, pid, os.ErrNotExist)
			}
		}
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.checked = append(l.checked, pid)
	var n int64
	fmt.Sscanf(unit, "agentos-test-worker-%d.scope", &n)
	if l.outside[n] {
		return fmt.Errorf("the process agentd started for the worker (pid %d) is in the cgroup /elsewhere, not in its scope %s", pid, unit)
	}
	return nil
}

func (l *limits) Env(n int64) (string, []string) {
	unit := fmt.Sprintf("agentos-test-worker-%d.scope", n)
	return unit, []string{"AGENTOS_LIMIT_UNIT=" + unit, "AGENTOS_LIMIT_MEMORY=1073741824"}
}

func (l *limits) OOMKilled(unit string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.asked = append(l.asked, unit)
	var n int64
	fmt.Sscanf(unit, "agentos-test-worker-%d.scope", &n)
	return l.killed[n], l.err
}

func (l *limits) Memory() string { return "1GiB" }

func (l *limits) scopes() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.asked)
}

// Each generation's launcher gets its limits, its scope of its own named among them (M5 C3a). Once a
// generation has exited without agentd's doing, the supervisor asks whether the kernel's out-of-memory
// killer killed it: one it did is logged so, naming the Worker's limit, and recorded so (oom_kill); it
// starts again as any other. A generation agentd stopped is not asked about, nor is one without a scope.
func TestAWorkerOverItsMemory(t *testing.T) {
	l := &limits{killed: map[int64]bool{1: true}}
	s, logged, r, file := worker(t, "planned", Timing{BackoffMin: 20 * time.Millisecond, BackoffMax: 20 * time.Millisecond},
		"AGENTOS_TEST_PLAN=report-re,report-re,report")
	s.o.Limits = l
	s.Start()
	readied(t, s)
	wait(t, "the third generation", 10*time.Second, func() bool { return len(logged.entries("worker ready")) == 3 })
	s.Stop()
	b, err := os.ReadFile(file + ".gen")
	if err != nil {
		t.Fatal(err)
	}
	for i, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var got struct{ Env []string }
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatal(err)
		}
		if unit := fmt.Sprintf("AGENTOS_LIMIT_UNIT=agentos-test-worker-%d.scope", i+1); !slices.Contains(got.Env, unit) ||
			!slices.Contains(got.Env, "AGENTOS_LIMIT_MEMORY=1073741824") {
			t.Errorf("generation %d's environment: %q, want %s", i+1, got.Env, unit)
		}
	}
	if asked := l.scopes(); !slices.Equal(asked, []string{"agentos-test-worker-1.scope", "agentos-test-worker-2.scope"}) {
		t.Errorf("asked about %q", asked)
	}
	over := logged.entries("the kernel's out-of-memory killer killed the Worker")
	if len(over) != 1 || over[0]["generation"] != 1.0 || over[0]["memory_limit"] != "1GiB" || over[0]["scope"] != "agentos-test-worker-1.scope" {
		t.Errorf("logged %v", over)
	}
	exits := logged.entries("worker exited")
	if len(exits) != 2 || exits[0]["oom_kill"] != true || exits[1]["oom_kill"] != nil || exits[0]["restart_in"] != "20ms" {
		t.Errorf("exits %v", exits)
	}
	ev, _ := r.all()
	if len(ev) != 2 || ev[0].Payload.(map[string]any)["oom_kill"] != true || ev[1].Payload.(map[string]any)["oom_kill"] != nil {
		t.Errorf("records %v", ev)
	}
}

// A first generation the kernel killed before it was ready fails agentd, saying so; and a generation
// systemd cannot be asked about is recorded without oom_kill, the log saying why.
func TestTheFirstWorkerOverItsMemory(t *testing.T) {
	s, _, r, _ := worker(t, "exit", Timing{})
	s.o.Limits = &limits{killed: map[int64]bool{1: true}}
	s.Start()
	select {
	case err := <-s.Failed():
		if !strings.Contains(err.Error(), "the Worker exited before it said it was ready (exit status 3; the kernel's out-of-memory killer "+
			"killed it, under a memory limit of 1GiB)") {
			t.Errorf("%v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the first Worker did not fail")
	}
	if ev, _ := r.all(); len(ev) != 1 || ev[0].Payload.(map[string]any)["oom_kill"] != true {
		t.Errorf("records %v", ev)
	}

	s, logged, r, _ := worker(t, "planned", Timing{BackoffMin: 20 * time.Millisecond}, "AGENTOS_TEST_PLAN=re,ready")
	s.o.Limits = &limits{killed: map[int64]bool{1: true}, err: errors.New("systemctl: exit status 1")}
	s.Start()
	readied(t, s)
	wait(t, "the second generation", 10*time.Second, func() bool { return len(logged.entries("worker ready")) == 2 })
	if w := logged.entries("could not ask systemd how the Worker's scope ended: its exit's record does not say whether the kernel's " +
		"out-of-memory killer killed it"); len(w) != 1 || w[0]["error"] != "systemctl: exit status 1" ||
		len(logged.entries("the kernel's out-of-memory killer killed the Worker")) != 0 {
		t.Errorf("logged %v", w)
	}
	if ev, _ := r.all(); len(ev) != 1 || ev[0].Payload.(map[string]any)["oom_kill"] != nil {
		t.Errorf("records %v", ev)
	}
}

// A generation whose launcher, ready, is not in its scope (one that does not source limits.sh) runs with
// no limits: the supervisor stops it, before its readiness counts, starts no other, and fails, saying
// why. The first's agentd never gets ready; a later one's stops (agentd's run reads Failed). The one
// outside stays, as a Worker does: one that has exited by the check is its exit (see
// TestAWorkerThatExitsAsItSaysItIsReady). The one before it is up a while, checked, then exits.
func TestAWorkerOutsideItsScope(t *testing.T) {
	for _, c := range []struct {
		outside int64
		plan    string
	}{{1, "ready"}, {2, "long,ready"}} {
		outside := c.outside
		t.Run(fmt.Sprintf("generation %d", outside), func(t *testing.T) {
			l := &limits{outside: map[int64]bool{outside: true}}
			s, logged, _, _ := worker(t, "planned", Timing{BackoffMin: 20 * time.Millisecond}, "AGENTOS_TEST_PLAN="+c.plan)
			s.o.Limits = l
			s.Start()
			var err error
			select {
			case err = <-s.Failed():
			case <-time.After(10 * time.Second):
				t.Fatal("the supervisor did not fail")
			}
			if !strings.Contains(err.Error(), "the Worker's launcher did not run it in its systemd user scope, which holds its limits, "+
				"and agentd stopped it") || !strings.Contains(err.Error(), fmt.Sprintf("not in its scope agentos-test-worker-%d.scope", outside)) {
				t.Errorf("%v", err)
			}
			select {
			case <-s.done:
			case <-time.After(10 * time.Second):
				t.Fatal("the supervisor went on")
			}
			if ready := logged.entries("worker ready"); len(ready) != int(outside)-1 {
				t.Errorf("ready %v", ready)
			}
			if started := logged.entries("worker started"); len(started) != int(outside) {
				t.Errorf("started %v", started)
			}
			if _, pid := s.Current(); pid != 0 {
				t.Errorf("generation %d's launcher still runs: %d", outside, pid)
			}
			if closed(s.ready) != (outside > 1) {
				t.Errorf("ready closed: %t", closed(s.ready))
			}
			l.mu.Lock()
			defer l.mu.Unlock()
			if len(l.checked) != int(outside) {
				t.Errorf("checked %v", l.checked)
			}
		})
	}
}

// A generation that exits as soon as it says it is ready is reaped before its launcher's cgroup can be
// read, and /proc no longer has its pid: that is its exit, not a launcher outside its scope. It is
// started again, as before the scope was checked, and its readiness counts; nothing goes on Failed.
func TestAWorkerThatExitsAsItSaysItIsReady(t *testing.T) {
	s, logged, r, _ := worker(t, "planned", Timing{BackoffMin: 20 * time.Millisecond, BackoffMax: 20 * time.Millisecond},
		"AGENTOS_TEST_PLAN=re,re,ready")
	s.o.Limits = &limits{vanishing: true}
	s.Start()
	readied(t, s)
	wait(t, "the third generation", 10*time.Second, func() bool { return len(logged.entries("worker ready")) == 3 })
	select {
	case err := <-s.Failed():
		t.Fatalf("failed: %v", err)
	default:
	}
	exits := logged.entries("worker exited")
	if len(exits) != 2 || exits[0]["ready"] != true || exits[1]["ready"] != true || exits[0]["restart_in"] != "20ms" {
		t.Errorf("exits %v", exits)
	}
	if ev, _ := r.all(); len(ev) != 1 || ev[0].Payload.(map[string]any)["generation"] != 1 {
		t.Errorf("records %v", ev)
	}
	if out := logged.entries("the Worker's launcher did not run it in its systemd user scope, which holds its limits: agentd " +
		"stopped it, and starts no other"); len(out) != 0 {
		t.Errorf("logged %v", out)
	}
}
