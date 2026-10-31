package scope

import (
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The launchers' half (sandbox/linux/limits.sh): what Env gives a launcher, it takes, and starts its
// scope with, as named and sized, OOMPolicy=stop and no swap; then, in the scope, it checks that its
// cgroup is the scope's, and refuses to go on where it is not (here a systemd-run that only pretends
// says nothing true). AGENTOS_LIMITS=off it runs without a scope. Anything else it refuses before it
// starts one: no limits, malformed ones, both, no user manager listening, or a claim to be in the scope
// already. It fails closed: no launcher runs without its limits by mistake. Where a user manager runs
// (Linux: the VM, CI), the launcher runs in a real scope too: in it, what it starts has the
// oom_score_adj it gave itself, OOMScoreAdj; off, it leaves its own as it was.
func TestTheLaunchersTakeWhatEnvGives(t *testing.T) {
	limitsSh, err := filepath.Abs("../../sandbox/linux/limits.sh")
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("/tmp", "launcher-") // a socket's path takes 104 bytes at most on macOS
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	launcher := filepath.Join(dir, "run-test.sh")
	if err := os.WriteFile(launcher, []byte("#!/bin/bash\nset -euo pipefail\n. "+strconv.Quote(limitsSh)+"\necho \"the launcher goes on: $*\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	// systemd-run as the launcher calls it: it writes down its arguments, and execs the command after --.
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	ran := filepath.Join(dir, "systemd-run.args")
	fake := "#!/bin/sh\nprintf '%s\\n' \"$@\" >" + strconv.Quote(ran) + "\nwhile [ \"$1\" != -- ]; do shift; done\nshift\nexec \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, "systemd-run"), []byte(fake), 0o700); err != nil {
		t.Fatal(err)
	}
	manager := filepath.Join(dir, "run")
	if err := os.MkdirAll(filepath.Join(manager, "systemd"), 0o700); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", filepath.Join(manager, "systemd", "private"))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	launch := func(env []string) (string, string, bool, []string) {
		t.Helper()
		_ = os.Remove(ran)
		cmd := exec.Command(launcher, "a", "b c")
		cmd.Env = append([]string{"PATH=" + bin + ":/usr/bin:/bin"}, env...)
		var stdout, stderr strings.Builder
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		var args []string
		if b, err := os.ReadFile(ran); err == nil {
			args = strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
		}
		return stdout.String(), stderr.String(), err == nil, args
	}

	s := New(manager, nil, nil)
	unit, env := s.On("tool", Limits{Memory: 64 << 20, Tasks: 32}).Env(7)
	stdout, stderr, ok, args := launch(env)
	if want := []string{"--user", "--scope", "--quiet", "--unit=" + unit, "--property=MemoryMax=67108864", "--property=MemorySwapMax=0",
		"--property=TasksMax=32", "--property=OOMPolicy=stop", "--", launcher, "a", "b c"}; !slices.Equal(args, want) {
		t.Errorf("systemd-run %q, want %q", args, want)
	}
	if ok || strings.Contains(stdout, "the launcher goes on") {
		t.Errorf("a launcher outside its scope went on: %s%s", stdout, stderr)
	}
	if runtime.GOOS == "linux" && !strings.Contains(stderr, "not in its scope "+unit) {
		t.Errorf("outside its scope: %s", stderr)
	}

	// As much memory as agentd's configuration may ask (999999999GiB, 19 digits).
	_, most := s.On("tool", Limits{Memory: 999999999 << 30, Tasks: 32}).Env(8)
	if _, _, _, args := launch(most); len(args) < 5 || args[4] != "--property=MemoryMax=1073741822926258176" {
		t.Errorf("the most memory: systemd-run %q", args)
	}

	_, off := Off("tool", Limits{Memory: 64 << 20, Tasks: 32}).Env(7)
	if stdout, stderr, ok, args := launch(off); !ok || stdout != "the launcher goes on: a b c\n" || args != nil {
		t.Errorf("off: %t %q %q, systemd-run %q", ok, stdout, stderr, args)
	}

	without := func(drop string, add ...string) []string {
		var out []string
		for _, kv := range env {
			if !strings.HasPrefix(kv, drop+"=") {
				out = append(out, kv)
			}
		}
		return append(out, add...)
	}
	for name, c := range map[string]struct {
		env     []string
		refusal string
	}{
		"nothing":                 {nil, "AGENTOS_LIMIT_MEMORY= is not a number of bytes"},
		"limits on":               {[]string{"AGENTOS_LIMITS=on"}, "AGENTOS_LIMITS=on: only off turns the limits off"},
		"limits empty":            {[]string{"AGENTOS_LIMITS="}, "AGENTOS_LIMITS=: only off turns the limits off"},
		"off, and limits":         {append(slices.Clone(env), OffEnv), "AGENTOS_LIMITS=off, and limits too"},
		"memory in GiB":           {without("AGENTOS_LIMIT_MEMORY", "AGENTOS_LIMIT_MEMORY=1GiB"), "is not a number of bytes"},
		"memory of 20 digits":     {without("AGENTOS_LIMIT_MEMORY", "AGENTOS_LIMIT_MEMORY=10000000000000000000"), "is not a number of bytes"},
		"no memory":               {without("AGENTOS_LIMIT_MEMORY"), "is not a number of bytes"},
		"no tasks":                {without("AGENTOS_LIMIT_TASKS", "AGENTOS_LIMIT_TASKS=0"), "is not a number of tasks"},
		"a path for a unit":       {without("AGENTOS_LIMIT_UNIT", "AGENTOS_LIMIT_UNIT=../x.scope"), "is not the name of a scope"},
		"a service for a unit":    {without("AGENTOS_LIMIT_UNIT", "AGENTOS_LIMIT_UNIT=x.service"), "is not the name of a scope"},
		"no XDG_RUNTIME_DIR":      {without("XDG_RUNTIME_DIR"), "no systemd user manager listens"},
		"no user manager there":   {without("XDG_RUNTIME_DIR", "XDG_RUNTIME_DIR="+dir), "no systemd user manager listens at XDG_RUNTIME_DIR=" + dir},
		"a relative runtime dir":  {without("XDG_RUNTIME_DIR", "XDG_RUNTIME_DIR=run"), "no systemd user manager listens"},
		"in its scope, it claims": {append(slices.Clone(env), "AGENTOS_LIMIT_IN_SCOPE=1"), ""},
	} {
		stdout, stderr, ok, args := launch(c.env)
		if ok || strings.Contains(stdout, "the launcher goes on") || args != nil || !strings.Contains(stderr, c.refusal) {
			t.Errorf("%s: went on %t, %q %q, systemd-run %q; want %q", name, ok, stdout, stderr, args, c.refusal)
		}
	}

	if runtime.GOOS != "linux" {
		return
	}
	userManager, _ := RuntimeDir()
	if fi, err := os.Stat(filepath.Join(userManager, "systemd", "private")); err != nil || fi.Mode().Type() != fs.ModeSocket {
		t.Logf("no user manager at %s: the launcher did not run in a real scope", userManager)
		return
	}
	scoped := filepath.Join(dir, "run-scoped.sh")
	if err := os.WriteFile(scoped, []byte("#!/bin/bash\nset -euo pipefail\n. "+strconv.Quote(limitsSh)+
		"\necho \"the launcher goes on; what it starts is at $(cat /proc/self/oom_score_adj)\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	own, err := os.ReadFile("/proc/self/oom_score_adj")
	if err != nil {
		t.Fatal(err)
	}
	_, held := New(userManager, nil, nil).On("tool", Limits{Memory: 64 << 20, Tasks: 32}).Env(1)
	for name, c := range map[string]struct {
		env []string
		adj string
	}{"in its scope": {held, strconv.Itoa(OOMScoreAdj)}, "off": {off, strings.TrimSpace(string(own))}} {
		cmd := exec.Command(scoped)
		cmd.Env = append([]string{"PATH=/usr/bin:/bin"}, c.env...)
		out, err := cmd.CombinedOutput()
		if want := "the launcher goes on; what it starts is at " + c.adj + "\n"; err != nil || string(out) != want {
			t.Errorf("%s: %v, %q; want %q", name, err, out, want)
		}
	}
}
