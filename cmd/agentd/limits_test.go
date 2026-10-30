package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jchen0426/agentos/internal/config"
	"github.com/jchen0426/agentos/internal/gitmcp"
	"github.com/jchen0426/agentos/internal/models"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/pricing"
	"github.com/jchen0426/agentos/internal/scope"
)

// probes answers agentd's probe scopes as a user manager that holds their limits would, and records
// which it was asked for, and what systemctl was asked.
type probes struct {
	units     []string
	systemctl [][]string
}

func (p *probes) run(_ context.Context, _ []string, argv ...string) ([]byte, error) {
	if argv[0] == "systemctl" {
		p.systemctl = append(p.systemctl, slices.Clone(argv))
		return nil, nil
	}
	if argv[0] != "systemd-run" {
		return nil, fmt.Errorf("%q", argv)
	}
	var unit, memory, tasks string
	for _, a := range argv {
		if v, ok := strings.CutPrefix(a, "--unit="); ok {
			unit = v
		}
		if v, ok := strings.CutPrefix(a, "--property=MemoryMax="); ok {
			memory = v
		}
		if v, ok := strings.CutPrefix(a, "--property=TasksMax="); ok {
			tasks = v
		}
	}
	p.units = append(p.units, unit)
	return fmt.Appendf(nil, "cgroup 0::/user.slice/user-501.slice/user@501.service/app.slice/%s\nmemory.max %s\nmemory.swap.max 0\npids.max %s\n",
		unit, memory, tasks), nil
}

// proc reads /proc as a kernel would whose command line leaves the cgroups' memory as it is.
func proc(path string) ([]byte, error) {
	if path == "/proc/cmdline" {
		return []byte("BOOT_IMAGE=/vmlinuz root=LABEL=cloudimg-rootfs ro\n"), nil
	}
	return nil, fmt.Errorf("open %s: %w", path, os.ErrNotExist)
}

// userManager makes XDG_RUNTIME_DIR a directory where a user manager listens.
func userManager(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "agentd-")
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
	t.Setenv("XDG_RUNTIME_DIR", dir)
	return dir
}

// On Linux each launcher the configuration names gets the limits of its runs, and agentd probes each
// set before it changes anything: without --no-worker the Worker's too. --insecure-dev turns them off,
// probing nothing: a launcher gets AGENTOS_LIMITS=off. macOS holds none, and refuses a configuration
// that names any.
func TestResourceLimits(t *testing.T) {
	dir := userManager(t)
	lingerDir = t.TempDir() // lingering on, for this test's user: TestLingering
	t.Cleanup(func() { lingerDir = "/var/lib/systemd/linger" })
	if u, err := user.Current(); err == nil {
		if err := os.WriteFile(filepath.Join(lingerDir, u.Username), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	both := workerConfig()
	both.GitTools.Sandbox = []string{"/opt/agentos/run-tool.sh"}
	both.Worker.Limits, both.GitTools.Limits = config.Limits{Memory: 768 << 20, Tasks: 300}, config.Limits{Memory: 64 << 20, Tasks: 32}
	unsandboxed := workerConfig()
	unsandboxed.Worker.Sandbox = nil
	for name, c := range map[string]struct {
		goos                string
		cfg                 *config.Config
		insecureDev, noWork bool
		enforcement         string
		worker, tools       string // what each launcher gets: "" for nothing, "off", or the memory it is held to
		probes              []string
	}{
		"Linux":                       {"linux", both, false, false, "systemd_user_scope", "768MiB", "64MiB", []string{"worker", "tool"}},
		"Linux, --no-worker":          {"linux", both, false, true, "systemd_user_scope", "", "64MiB", []string{"tool"}},
		"Linux, --insecure-dev":       {"linux", both, true, false, "off", "off", "off", nil},
		"Linux, nothing sandboxed":    {"linux", unsandboxed, true, false, "off", "", "", nil},
		"macOS":                       {"darwin", workerConfig(), false, false, "none", "", "", nil},
		"macOS, --insecure-dev":       {"darwin", workerConfig(), true, false, "none", "", "", nil},
		"macOS, nothing sandboxed":    {"darwin", unsandboxed, true, true, "none", "", "", nil},
		"Linux, no git tool launcher": {"linux", workerConfig(), false, false, "systemd_user_scope", "1GiB", "", []string{"worker"}},
	} {
		if c.cfg.Worker.Limits.Memory == 0 { // as config.Load leaves it
			c.cfg.Worker.Limits, c.cfg.GitTools.Limits = config.DefaultWorkerLimits, config.DefaultToolLimits
		}
		var p probes
		l, err := resourceLimits(context.Background(), c.goos, c.cfg, c.insecureDev, c.noWork, p.run, proc)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if l.enforcement != c.enforcement {
			t.Errorf("%s: enforcement %q, want %q", name, l.enforcement, c.enforcement)
		}
		for _, set := range []struct {
			what string
			set  *scope.Set
			want string
		}{{"the Worker's", l.worker, c.worker}, {"the tools'", l.tools, c.tools}} {
			got := ""
			if set.set != nil {
				got = set.set.Memory()
				if unit, env := set.set.Env(1); unit == "" {
					got = "off"
					if !slices.Equal(env, []string{"AGENTOS_LIMITS=off"}) {
						t.Errorf("%s: %s launcher gets %q", name, set.what, env)
					}
				} else if !slices.Contains(env, "XDG_RUNTIME_DIR="+dir) {
					t.Errorf("%s: %s launcher gets %q", name, set.what, env)
				}
			}
			if got != set.want {
				t.Errorf("%s: %s limits %q, want %q", name, set.what, got, set.want)
			}
		}
		var probed []string
		for _, u := range p.units {
			probed = append(probed, u[strings.LastIndex(u, "-")+1:len(u)-len(".scope")])
		}
		if !slices.Equal(probed, c.probes) {
			t.Errorf("%s: probed %q, want %q", name, probed, c.probes)
		}
	}

	// A configuration that names limits is refused where nothing would hold them.
	named := workerConfig()
	named.Worker.Limits = config.Limits{Memory: 1 << 30, Tasks: 64, Named: true}
	if _, err := resourceLimits(context.Background(), "darwin", named, false, false, (&probes{}).run, proc); err == nil ||
		!strings.Contains(err.Error(), "worker.limits: agentd sets resource limits on Linux only") {
		t.Errorf("named limits on macOS: %v", err)
	}
	// No user manager: agentd refuses to start, saying so.
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	if _, err := resourceLimits(context.Background(), "linux", both, false, false, (&probes{}).run, proc); !errors.Is(err, scope.ErrNoManager) {
		t.Errorf("no user manager: %v", err)
	}
	if _, err := resourceLimits(context.Background(), "linux", both, true, false, (&probes{}).run, proc); err != nil {
		t.Errorf("no user manager, --insecure-dev: %v", err)
	}
}

// Where the limits hold, logind must keep the user manager without a session (lingering): agentd says
// so at start when it would not, and how to make it. As agentd stops, the failed scopes of its start go.
func TestLingering(t *testing.T) {
	userManager(t)
	u, err := user.Current()
	if err != nil {
		t.Skip(err)
	}
	lingerDir = t.TempDir()
	t.Cleanup(func() { lingerDir = "/var/lib/systemd/linger" })
	cfg := workerConfig()
	cfg.GitTools.Sandbox = []string{"/opt/agentos/run-tool.sh"}
	cfg.Worker.Limits, cfg.GitTools.Limits = config.DefaultWorkerLimits, config.DefaultToolLimits
	var p probes
	l, err := resourceLimits(context.Background(), "linux", cfg, false, false, p.run, proc)
	if err != nil {
		t.Fatal(err)
	}
	var log strings.Builder
	l.say(cfg, slog.New(slog.NewTextHandler(&log, nil)))
	if l.noLinger != u.Username || !strings.Contains(log.String(), "lingering is off for "+u.Username) ||
		!strings.Contains(log.String(), "sudo loginctl enable-linger "+u.Username) {
		t.Errorf("no linger: %q; %s", l.noLinger, log.String())
	}
	l.clear(slog.New(slog.DiscardHandler))
	if want := [][]string{{"systemctl", "--user", "reset-failed", "--", strings.TrimSuffix(l.scopes.Names(), ".scope")}}; !slices.EqualFunc(p.systemctl, want, slices.Equal) {
		t.Errorf("at the end: %q, want %q", p.systemctl, want)
	}
	if err := os.WriteFile(filepath.Join(lingerDir, u.Username), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	l, err = resourceLimits(context.Background(), "linux", cfg, false, false, p.run, proc)
	if err != nil || l.noLinger != "" {
		t.Errorf("lingering: %v, %q", err, l.noLinger)
	}
	// Where the limits are off or none, nothing is said of it, and nothing cleared.
	for _, c := range []struct {
		goos        string
		insecureDev bool
	}{{"linux", true}, {"darwin", false}} {
		_ = os.Remove(filepath.Join(lingerDir, u.Username))
		var p probes
		l, err := resourceLimits(context.Background(), c.goos, cfg, c.insecureDev, false, p.run, proc)
		l.clear(slog.New(slog.DiscardHandler))
		if err != nil || l.noLinger != "" || len(p.systemctl) != 0 {
			t.Errorf("%s, --insecure-dev %t: %v, %q, %q", c.goos, c.insecureDev, err, l.noLinger, p.systemctl)
		}
	}
}

// procWith reads /proc and /sys/fs/cgroup as proc does, with files besides.
func procWith(files map[string]string) func(string) ([]byte, error) {
	return func(path string) ([]byte, error) {
		if b, ok := files[path]; ok {
			return []byte(b), nil
		}
		return proc(path)
	}
}

// The limits' worst case: the Worker's memory limit, and the git tool calls' for each slot (the
// client's 4 and the run starts' 2), counting only what agentd runs; against the machine's memory, or
// less where a cgroup above the scopes holds less ("max" is no limit). Over half of it, strictly,
// agentd warns at every start, saying what adds up to what, against what, and how to lower it; at
// half or under it says nothing. agentd.started records it either way; where /proc/meminfo cannot be
// read, agentd says so, and the record says why. Where the limits are off or none, nothing of it.
func TestTheLimitsWorstCase(t *testing.T) {
	userManager(t)
	lingerDir = t.TempDir()
	t.Cleanup(func() { lingerDir = "/var/lib/systemd/linger" })
	const vm = 6052052 // kB: the Lima VM's MemTotal, about 5.8GiB
	meminfo := func(kb int64) string { return fmt.Sprintf("MemTotal:       %d kB\nMemFree:          524288 kB\n", kb) }
	cgroup := func(dir string) string { return "/sys/fs/cgroup/user.slice" + dir + "/memory.max" }
	const app, manager, user = "/user-501.slice/user@501.service/app.slice", "/user-501.slice/user@501.service", "/user-501.slice"
	both := workerConfig()
	both.GitTools.Sandbox = []string{"/opt/agentos/run-tool.sh"}
	both.Worker.Limits, both.GitTools.Limits = config.DefaultWorkerLimits, config.DefaultToolLimits
	worker := workerConfig()
	worker.Worker.Limits, worker.GitTools.Limits = config.DefaultWorkerLimits, config.DefaultToolLimits
	big := workerConfig()
	big.Worker.Limits, big.GitTools.Limits = config.Limits{Memory: 4 << 30, Tasks: 512}, config.DefaultToolLimits
	record := func(worker, tool, calls int64, with string, withBytes int64, warned bool) map[string]any {
		return map[string]any{"memory_bytes": worker + tool*calls, "worker_memory_bytes": worker, "tool_memory_bytes": tool,
			"tool_calls": calls, "compared_with": with, "compared_with_bytes": withBytes, "warned": warned}
	}
	for name, c := range map[string]struct {
		cfg     *config.Config
		noWork  bool
		files   map[string]string
		record  map[string]any
		warning string // what agentd warns of it; "" for nothing
	}{
		"16GiB": {both, false, map[string]string{"/proc/meminfo": meminfo(16 << 20)},
			record(1<<30, 512<<20, 6, "MemTotal", 16<<30, false), ""},
		"exactly half": {both, false, map[string]string{"/proc/meminfo": meminfo(8 << 20)},
			record(1<<30, 512<<20, 6, "MemTotal", 8<<30, false), ""},
		"a kB over half": {both, false, map[string]string{"/proc/meminfo": meminfo(8<<20 - 1)},
			record(1<<30, 512<<20, 6, "MemTotal", (8<<20-1)<<10, true), "come to 4GiB"},
		"the VM's": {both, false, map[string]string{"/proc/meminfo": meminfo(vm)}, record(1<<30, 512<<20, 6, "MemTotal", vm<<10, true),
			"the resource limits' worst case is over half of the machine's memory: the Worker's 1GiB and 6 git tool calls at " +
				"512MiB each come to 4GiB, of 5.8GiB (MemTotal in /proc/meminfo). Each scope holds its own limit, and nothing " +
				"limits their sum: past what the machine has, the kernel's out-of-memory killer picks its victim from the whole " +
				"machine, not only from the scopes. worker.limits.memory and git_tools.limits.memory lower it"},
		"--no-worker": {both, true, map[string]string{"/proc/meminfo": meminfo(vm)}, record(0, 512<<20, 6, "MemTotal", vm<<10, true),
			"over half of the machine's memory: 6 git tool calls at 512MiB each come to 3GiB, of 5.8GiB"},
		"no git tool launcher": {worker, false, map[string]string{"/proc/meminfo": meminfo(vm)},
			record(1<<30, 0, 0, "MemTotal", vm<<10, false), ""},
		"the Worker alone": {big, false, map[string]string{"/proc/meminfo": meminfo(vm)}, record(4<<30, 0, 0, "MemTotal", vm<<10, true),
			"over half of the machine's memory: the Worker's 4GiB alone, of 5.8GiB"},
		"a slice that holds less": {both, false, map[string]string{"/proc/meminfo": meminfo(16 << 20), cgroup(app): "6442450944\n"},
			record(1<<30, 512<<20, 6, "/user.slice"+app, 6<<30, true),
			"over half of what the cgroup /user.slice" + app + " may use: the Worker's 1GiB and 6 git tool calls at 512MiB each " +
				"come to 4GiB, of 6GiB (its memory.max). Each scope holds its own limit, and nothing limits their sum: past what " +
				"that cgroup may use, the kernel's out-of-memory killer picks its victim from all of that cgroup, not only from the scopes"},
		"max, then less higher up": {both, false, map[string]string{"/proc/meminfo": meminfo(16 << 20), cgroup(app): "max\n",
			cgroup(manager): "max\n", cgroup(user): "12884901888\n", cgroup(""): "max\n"},
			record(1<<30, 512<<20, 6, "/user.slice"+user, 12<<30, false), ""},
		"the lowest of them": {both, false, map[string]string{"/proc/meminfo": meminfo(16 << 20), cgroup(app): "7516192768\n",
			cgroup(""): "6442450944\n"}, record(1<<30, 512<<20, 6, "/user.slice", 6<<30, true), "of 6GiB (its memory.max)"},
		"a slice above the machine": {both, false, map[string]string{"/proc/meminfo": meminfo(vm), cgroup(""): "34359738368\n"},
			record(1<<30, 512<<20, 6, "MemTotal", vm<<10, true), "of 5.8GiB (MemTotal in /proc/meminfo)"},
		"no /proc/meminfo": {both, false, nil, map[string]any{"memory_bytes": int64(4 << 30), "worker_memory_bytes": int64(1 << 30),
			"tool_memory_bytes": int64(512 << 20), "tool_calls": int64(6), "warned": false,
			"compared_with_error": "cannot read /proc/meminfo: open /proc/meminfo: file does not exist"},
			"agentd cannot tell whether the resource limits' worst case is over half of the memory the scopes can have"},
	} {
		var p probes
		l, err := resourceLimits(context.Background(), "linux", c.cfg, false, c.noWork, p.run, procWith(c.files))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if r := l.worstCaseRecord(); !maps.Equal(r, c.record) {
			t.Errorf("%s: the record %v, want %v", name, r, c.record)
		}
		var log strings.Builder
		l.say(c.cfg, slog.New(slog.NewTextHandler(&log, nil)))
		if said := strings.Contains(log.String(), "worst case"); said != (c.warning != "") ||
			c.warning != "" && !strings.Contains(log.String(), strconv.Quote(c.warning)[1:len(strconv.Quote(c.warning))-1]) {
			t.Errorf("%s: the log %s, want %q", name, log.String(), c.warning)
		}
	}
	// Off and none: no worst case, said or recorded.
	for _, c := range []struct {
		goos        string
		insecureDev bool
	}{{"linux", true}, {"darwin", false}} {
		l, err := resourceLimits(context.Background(), c.goos, both, c.insecureDev, false, (&probes{}).run,
			procWith(map[string]string{"/proc/meminfo": meminfo(vm)}))
		var log strings.Builder
		l.say(both, slog.New(slog.NewTextHandler(&log, nil)))
		if err != nil || l.worstCaseRecord() != nil || strings.Contains(log.String(), "worst case") {
			t.Errorf("%s, --insecure-dev %t: %v, %v, %s", c.goos, c.insecureDev, err, l.worstCaseRecord(), log.String())
		}
	}
}

// agentd.started records the limits of the Worker (TestWorkerRecord) and of the git tool calls: where
// they hold, their memory and tasks, no swap, and what holds them; where they do not, only that.
func TestStartRecordsTheToolLimits(t *testing.T) {
	cfg := workerConfig()
	cfg.GitTools.Limits = config.Limits{Memory: 64 << 20, Tasks: 32}
	for enforcement, want := range map[string]map[string]any{
		enforcedByScope: {"memory_bytes": int64(64 << 20), "tasks": int64(32), "swap_bytes": 0, "enforcement": enforcedByScope},
		enforcedOff:     {"enforcement": "off"},
		enforcedByNone:  {"enforcement": "none"},
	} {
		p := startRecord("agentd.yaml", cfg, &policy.Policy{}, &models.Config{}, &pricing.Table{}, "i", "", false, false, enforcement)
		if l, ok := p["git_tools_limits"].(map[string]any); !ok || !maps.Equal(l, want) {
			t.Errorf("%s: %v", enforcement, p["git_tools_limits"])
		}
	}
}

// Each git tool call's launcher gets the calls' limits, a scope of its own named among them; or, with
// them off, AGENTOS_LIMITS=off; or, where nothing holds them, none of it.
func TestTheGitToolCallsGetTheirLimits(t *testing.T) {
	dir := t.TempDir()
	server, launcher := filepath.Join(dir, "agentos-mcp-git"), filepath.Join(dir, "run-tool.sh")
	if err := os.WriteFile(server, nil, 0o700); err != nil {
		t.Fatal(err)
	}
	// It writes down what it was given of the limits, and fails, as a launcher that cannot run.
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\nenv | grep -E '^(AGENTOS_LIMIT|XDG_RUNTIME_DIR=)' | sort >\"$0.env\"\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	gone := func(context.Context, []string, ...string) ([]byte, error) {
		return []byte("ActiveState=inactive\nResult=success\n"), nil
	}
	on := scope.New("/run/user/501", gone, nil).On("tool", scope.Limits{Memory: 64 << 20, Tasks: 32})
	unit, _ := on.Env(1)
	for name, k := range map[string]struct {
		limits *scope.Set
		want   []string
	}{
		"held": {on, []string{"AGENTOS_LIMIT_MEMORY=67108864", "AGENTOS_LIMIT_TASKS=32", "AGENTOS_LIMIT_UNIT=" + unit,
			"XDG_RUNTIME_DIR=/run/user/501"}},
		"off":  {scope.Off("tool", scope.Limits{Memory: 64 << 20, Tasks: 32}), []string{"AGENTOS_LIMITS=off"}},
		"none": {nil, nil},
	} {
		cfg := workerConfig()
		cfg.GitTools = config.GitTools{Server: server, Sandbox: []string{launcher}, Timeout: 10 * time.Second}
		c, err := gitServer(cfg, false, k.limits, slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Call(context.Background(), dir, "resolve_commit", gitmcp.ResolveCommitIn{Commit: "abcdef0"}, &gitmcp.ResolveCommitOut{}); !gitmcp.IsServerFailure(err) {
			t.Errorf("%s: %v", name, err)
		}
		b, err := os.ReadFile(launcher + ".env")
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Fields(string(b)); !slices.Equal(got, k.want) {
			t.Errorf("%s: the launcher got %q, want %q", name, got, k.want)
		}
	}
}
