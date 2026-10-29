package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"os"
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
// which it was asked for.
type probes struct{ units []string }

func (p *probes) run(_ context.Context, _ []string, argv ...string) ([]byte, error) {
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
		l, err := resourceLimits(context.Background(), c.goos, c.cfg, c.insecureDev, c.noWork, p.run)
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
	if _, err := resourceLimits(context.Background(), "darwin", named, false, false, (&probes{}).run); err == nil ||
		!strings.Contains(err.Error(), "worker.limits: agentd sets resource limits on Linux only") {
		t.Errorf("named limits on macOS: %v", err)
	}
	// No user manager: agentd refuses to start, saying so.
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	if _, err := resourceLimits(context.Background(), "linux", both, false, false, (&probes{}).run); !errors.Is(err, scope.ErrNoManager) {
		t.Errorf("no user manager: %v", err)
	}
	if _, err := resourceLimits(context.Background(), "linux", both, true, false, (&probes{}).run); err != nil {
		t.Errorf("no user manager, --insecure-dev: %v", err)
	}
}

// agentd.started records the limits of the Worker (TestWorkerRecord) and of the git tool calls: their
// memory and tasks, no swap, and what holds them.
func TestStartRecordsTheToolLimits(t *testing.T) {
	cfg := workerConfig()
	cfg.GitTools.Limits = config.Limits{Memory: 64 << 20, Tasks: 32}
	for _, enforcement := range []string{enforcedByScope, enforcedOff, enforcedByNone} {
		p := startRecord("agentd.yaml", cfg, &policy.Policy{}, &models.Config{}, &pricing.Table{}, "i", "", false, false, enforcement)
		if l, ok := p["git_tools_limits"].(map[string]any); !ok || !maps.Equal(l, map[string]any{"memory_bytes": int64(64 << 20),
			"tasks": int64(32), "swap_bytes": 0, "enforcement": enforcement}) {
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
	on := scope.New("/run/user/501", gone).On("tool", scope.Limits{Memory: 64 << 20, Tasks: 32})
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
