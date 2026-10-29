package scope

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// systemd answers systemctl and systemd-run as a test says, and records what it was asked.
type systemd struct {
	mu    sync.Mutex
	calls [][]string
	envs  [][]string
	// probe is systemd-run's answer for the probe scope unit, run with argv.
	probe func(unit string, argv []string) (string, error)
	// shown are systemctl show's answers, in turn: the last one again once they run out.
	shown []string
	err   error // systemctl's, for every call
}

func (s *systemd) run(_ context.Context, env []string, argv ...string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls, s.envs = append(s.calls, slices.Clone(argv)), append(s.envs, slices.Clone(env))
	switch {
	case argv[0] == "systemd-run":
		unit := ""
		for _, a := range argv {
			if u, ok := strings.CutPrefix(a, "--unit="); ok {
				unit = u
			}
		}
		out, err := s.probe(unit, argv)
		return []byte(out), err
	case s.err != nil:
		return nil, s.err
	case slices.Contains(argv, "show"):
		if len(s.shown) == 0 {
			return nil, errors.New("nothing to show")
		}
		out := s.shown[0]
		if len(s.shown) > 1 {
			s.shown = s.shown[1:]
		}
		return []byte(out), nil
	}
	return nil, nil // reset-failed
}

func (s *systemd) asked(verb string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if slices.Contains(c, verb) {
			n++
		}
	}
	return n
}

// managed is a directory where a user manager listens: its systemd/private is a socket. Under /tmp:
// a socket's path takes 104 bytes at most on macOS.
func managed(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "scope-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	if err := os.Mkdir(filepath.Join(dir, "systemd"), 0o700); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", filepath.Join(dir, "systemd", "private"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return dir
}

// held is what a probe scope reports where the user manager holds what it asks: its argv's limits.
func held(unit string, argv []string) (string, error) {
	var memory, tasks string
	for _, a := range argv {
		if v, ok := strings.CutPrefix(a, "--property=MemoryMax="); ok {
			memory = v
		}
		if v, ok := strings.CutPrefix(a, "--property=TasksMax="); ok {
			tasks = v
		}
	}
	return fmt.Sprintf("cgroup 0::/user.slice/user-501.slice/user@501.service/app.slice/%s\nmemory.max %s\nmemory.swap.max 0\npids.max %s\n",
		unit, memory, tasks), nil
}

var (
	workerLimits = Limits{Memory: 1 << 30, Tasks: 512}
	toolLimits   = Limits{Memory: 512 << 20, Tasks: 128}
)

// A launcher gets the name of its run's scope, of agentd's start and the run's own, its limits, and
// where the user manager listens; or, with the limits off, AGENTOS_LIMITS=off alone.
func TestEnv(t *testing.T) {
	s := New("/run/user/501", (&systemd{}).run)
	unit, env := s.On("worker", workerLimits).Env(3)
	if !regexp.MustCompile(`^agentos-[0-9a-f]{16}-worker-3\.scope$`).MatchString(unit) || !strings.HasPrefix(unit, strings.TrimSuffix(s.Names(), "*.scope")) {
		t.Errorf("unit %q, names %q", unit, s.Names())
	}
	if want := []string{"AGENTOS_LIMIT_MEMORY=1073741824", "AGENTOS_LIMIT_TASKS=512", "AGENTOS_LIMIT_UNIT=" + unit,
		"XDG_RUNTIME_DIR=/run/user/501"}; !slices.Equal(env, want) {
		t.Errorf("env %q, want %q", env, want)
	}
	tool, _ := s.On("tool", toolLimits).Env(3)
	if tool == unit || !strings.HasSuffix(tool, "-tool-3.scope") {
		t.Errorf("a tool call's unit %q, the Worker's %q", tool, unit)
	}
	other, _ := New("/run/user/501", nil).On("worker", workerLimits).Env(3)
	if other == unit {
		t.Errorf("two starts name their scopes alike: %s", unit)
	}
	if unit, env := Off("worker", workerLimits).Env(3); unit != "" || !slices.Equal(env, []string{"AGENTOS_LIMITS=off"}) {
		t.Errorf("off: %q %q", unit, env)
	}
	for l, want := range map[Limits]string{workerLimits: "1GiB", toolLimits: "512MiB", {Memory: 3 << 29}: "1536MiB", {Memory: 1<<20 + 1}: "1048577 bytes"} {
		if got := s.On("worker", l).Memory(); got != want {
			t.Errorf("%d bytes: %s, want %s", l.Memory, got, want)
		}
	}
}

// agentd's own XDG_RUNTIME_DIR, when it is an absolute path, else where systemd-logind makes one.
func TestRuntimeDir(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1234")
	if d := RuntimeDir(); d != "/run/user/1234" {
		t.Errorf("%s", d)
	}
	want := fmt.Sprintf("/run/user/%d", os.Getuid())
	for _, v := range []string{"", "run/user/1234"} {
		t.Setenv("XDG_RUNTIME_DIR", v)
		if d := RuntimeDir(); d != want {
			t.Errorf("XDG_RUNTIME_DIR=%q: %s, want %s", v, d, want)
		}
	}
}

// The start check: a probe scope of each set's limits, as its launcher would start it, in which they
// read back as asked. It tells no user manager (no socket where it listens, or none answering) from
// limits that do not take (no cgroup v2 alone, not the scope's cgroup, a controller not delegated, a
// value not as asked), and says how to make either right.
func TestProbe(t *testing.T) {
	dir := managed(t)
	t.Setenv("PATH", "/usr/local/bin:/usr/bin:/bin")
	t.Setenv("DBUS_SESSION_BUS_ADDRESS", "unix:path=/run/user/501/bus")
	sd := &systemd{probe: held}
	s := New(dir, sd.run)
	if err := s.Probe(context.Background(), s.On("worker", workerLimits), Off("tool", toolLimits), s.On("tool", toolLimits)); err != nil {
		t.Fatal(err)
	}
	// One probe for each set the limits hold, sized as the set, collected; none for one that is off. Its
	// environment is the launchers': not the session bus, which would answer where they could not.
	if len(sd.calls) != 2 {
		t.Fatalf("%d calls: %q", len(sd.calls), sd.calls)
	}
	for i, want := range []struct {
		kind          string
		memory, tasks string
	}{{"worker", "1073741824", "512"}, {"tool", "536870912", "128"}} {
		c := sd.calls[i]
		if args := strings.Join(c, " "); !strings.HasPrefix(args, "systemd-run --user --scope --quiet --collect --unit=agentos-"+s.token+"-probe-"+want.kind+".scope "+
			"--property=MemoryMax="+want.memory+" --property=MemorySwapMax=0 --property=TasksMax="+want.tasks+" --property=OOMPolicy=stop -- /bin/sh -c ") {
			t.Errorf("probe %d: %s", i, args)
		}
		if env := sd.envs[i]; !slices.Equal(env, []string{"PATH=/usr/local/bin:/usr/bin:/bin", "LANG=C.UTF-8", "XDG_RUNTIME_DIR=" + dir}) {
			t.Errorf("probe %d's environment: %q", i, env)
		}
	}

	report := func(lines ...string) func(string, []string) (string, error) {
		return func(unit string, _ []string) (string, error) {
			return strings.ReplaceAll(strings.Join(lines, "\n"), "UNIT", unit), nil
		}
	}
	const good = "cgroup 0::/user.slice/user-501.slice/user@501.service/app.slice/UNIT"
	for name, c := range map[string]struct {
		dir     string
		probe   func(string, []string) (string, error)
		is      error
		message []string
	}{
		"no socket": {dir: t.TempDir(), probe: held, is: ErrNoManager,
			message: []string{"no systemd user manager for uid", "systemd/private is not there", "loginctl enable-linger", "--insecure-dev"}},
		"no manager answers": {dir: dir, probe: func(string, []string) (string, error) {
			return "", errors.New("systemd-run: exit status 1: Failed to connect to bus: Connection refused")
		}, is: ErrNoManager, message: []string{"Failed to connect to bus: Connection refused", "loginctl enable-linger"}},
		"a scope it cannot start": {dir: dir, probe: func(string, []string) (string, error) {
			return "", errors.New("systemd-run: exit status 1: Failed to start transient scope unit: Access denied")
		}, message: []string{"could not start a systemd user scope", "Access denied"}},
		"no memory controller": {dir: dir, probe: report(good, "memory.max missing", "memory.swap.max missing", "pids.max 512"), is: ErrNotHeld,
			message: []string{"has no memory.max", "delegated", "cgroup.controllers", "--insecure-dev"}},
		"no swap limit": {dir: dir, probe: report(good, "memory.max 1073741824", "memory.swap.max missing", "pids.max 512"), is: ErrNotHeld,
			message: []string{"has no memory.swap.max"}},
		"no memory limit": {dir: dir, probe: report(good, "memory.max max", "memory.swap.max 0", "pids.max 512"), is: ErrNotHeld,
			message: []string{"memory.max is max, not 1073741824"}},
		"swap": {dir: dir, probe: report(good, "memory.max 1073741824", "memory.swap.max max", "pids.max 512"), is: ErrNotHeld,
			message: []string{"memory.swap.max is max, not 0"}},
		"no task limit": {dir: dir, probe: report(good, "memory.max 1073741824", "memory.swap.max 0", "pids.max missing"), is: ErrNotHeld,
			message: []string{"has no pids.max"}},
		"another task limit": {dir: dir, probe: report(good, "memory.max 1073741824", "memory.swap.max 0", "pids.max 511"), is: ErrNotHeld,
			message: []string{"pids.max is 511, not 512"}},
		"cgroup v1 too": {dir: dir, probe: report("cgroup 12:pids:/user.slice", good, "memory.max 1073741824", "memory.swap.max 0", "pids.max 512"),
			is: ErrNotHeld, message: []string{"not in one of cgroup v2's"}},
		"another cgroup": {dir: dir, probe: report("cgroup 0::/user.slice/user-501.slice/session-6.scope", "memory.max 1073741824",
			"memory.swap.max 0", "pids.max 512"), is: ErrNotHeld, message: []string{"not in its scope's"}},
	} {
		s := New(c.dir, (&systemd{probe: c.probe}).run)
		err := s.Probe(context.Background(), s.On("worker", workerLimits))
		if err == nil || c.is != nil && !errors.Is(err, c.is) || c.is == nil && (errors.Is(err, ErrNoManager) || errors.Is(err, ErrNotHeld)) {
			t.Errorf("%s: %v, want %v", name, err, c.is)
			continue
		}
		for _, m := range c.message {
			if !strings.Contains(err.Error(), m) {
				t.Errorf("%s: %v, without %q", name, err, m)
			}
		}
	}
}

// A run's end, as systemd says it once the run's processes are gone: the kernel killed it at its
// memory limit if its scope's Result is oom-kill. OOMKilled waits while systemd is not yet done with
// the scope, and clears a failed one; a clean end it leaves as systemd does, gone.
func TestOOMKilled(t *testing.T) {
	settleEvery = time.Millisecond
	defer func() { settleEvery = 10 * time.Millisecond }()
	const active, killed, gone = "Result=success\nActiveState=active\n", "Result=oom-kill\nActiveState=failed\n",
		"Result=success\nActiveState=inactive\n"
	for name, c := range map[string]struct {
		shown        []string
		killed       bool
		resets, asks int
	}{
		"killed":                     {shown: []string{killed}, killed: true, resets: 1, asks: 1},
		"killed, once systemd heard": {shown: []string{active, active, "ActiveState=deactivating\nResult=oom-kill\n", killed}, killed: true, resets: 1, asks: 4},
		"exited":                     {shown: []string{active, active, gone}, asks: 3},
		"another failure":            {shown: []string{"Result=timeout\nActiveState=failed\n"}, resets: 1, asks: 1},
	} {
		sd := &systemd{shown: c.shown}
		s := New("/run/user/501", sd.run)
		got, err := s.On("tool", toolLimits).OOMKilled("agentos-x-tool-1.scope")
		if err != nil || got != c.killed || sd.asked("reset-failed") != c.resets || sd.asked("show") != c.asks {
			t.Errorf("%s: %t, %v; %d reset-failed, %d show: %q", name, got, err, sd.asked("reset-failed"), sd.asked("show"), sd.calls)
		}
		for i, call := range sd.calls {
			if call[len(call)-1] != "agentos-x-tool-1.scope" || call[len(call)-2] != "--" || !slices.Equal(sd.envs[i],
				[]string{"PATH=" + os.Getenv("PATH"), "LANG=C.UTF-8", "XDG_RUNTIME_DIR=/run/user/501"}) {
				t.Errorf("%s: %q with %q", name, call, sd.envs[i])
			}
		}
	}
	// A scope systemd is never done with, within the wait: its Result as it stands.
	settleWithin = 50 * time.Millisecond
	defer func() { settleWithin = 3 * time.Second }()
	for shown, want := range map[string]bool{active: false, "ActiveState=deactivating\nResult=oom-kill\n": true} {
		sd := &systemd{shown: []string{shown}}
		if got, err := New("/run/user/501", sd.run).On("tool", toolLimits).OOMKilled("u.scope"); err != nil || got != want ||
			sd.asked("reset-failed") != 0 {
			t.Errorf("%q, never done: %t, %v", shown, got, err)
		}
	}
	// systemd could not be asked; the limits are off, or the run had no scope: nothing to ask.
	sd := &systemd{err: errors.New("systemctl: exit status 1: Failed to connect to bus")}
	if _, err := New("/run/user/501", sd.run).On("tool", toolLimits).OOMKilled("u.scope"); err == nil {
		t.Error("no error from a systemctl that failed")
	}
	if got, err := Off("tool", toolLimits).OOMKilled("u.scope"); got || err != nil {
		t.Errorf("off: %t, %v", got, err)
	}
	sd = &systemd{}
	if got, err := New("/run/user/501", sd.run).On("tool", toolLimits).OOMKilled(""); got || err != nil || len(sd.calls) != 0 {
		t.Errorf("no scope: %t, %v, %q", got, err, sd.calls)
	}
}
