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
