package main

import (
	"encoding/json"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/jchen0426/agentos/internal/config"
)

func workerConfig() *config.Config {
	return &config.Config{DataDir: "/srv/agentos/data", ControlSocket: "/run/agentos/control/control.sock",
		RuntimeListen: "tcp://127.0.0.1:7244", Temporal: config.Temporal{Namespace: "ns"},
		TaskQueues: config.TaskQueues{Workflows: "wq"}, Repos: map[string]config.Repo{"a/b": {Mirror: "/srv/mirrors/b"}},
		Worker: config.Worker{Sandbox: []string{"/opt/agentos/run-worker.sh"}, Python: "/opt/venv", Code: "/opt/code",
			Workflows: "agentos_worker.workflows:WORKFLOWS", DenyRead: []string{"/srv/more"}}}
}

var port = &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7244}

// agentd runs the Worker through its platform's launcher, which takes the Runtime entry as that
// launcher reaches it (macOS a port, Linux the socket), the Python environment and the code
// directory; or, without a launcher, alone. Its command is the same either way, with the
// configuration's namespace, task queue and workflows.
func TestWorkerLaunch(t *testing.T) {
	worker := []string{"/opt/venv/bin/python", "-m", "agentos_worker"}
	args := []string{"--namespace", "ns", "--task-queue", "wq", "--workflows", "agentos_worker.workflows:WORKFLOWS"}
	cfg := workerConfig()
	o, err := workerLaunch("darwin", cfg, port)
	if want := slices.Concat([]string{"/opt/agentos/run-worker.sh", "7244", "/opt/venv", "/opt/code", "--"}, worker,
		[]string{"--endpoint", "127.0.0.1:7244", "--http-connect-proxy", "env"}, args); err != nil || !slices.Equal(o.Argv, want) ||
		!o.StateDirs || !o.Lifeline || o.Dir != "/opt/code" {
		t.Errorf("macOS: %v\n got %q\nwant %q, %+v", err, o.Argv, want, o)
	}
	cfg.RuntimeListen = "unix:///run/agentos/runtime/runtime.sock"
	o, err = workerLaunch("linux", cfg, &net.UnixAddr{Name: "/run/agentos/runtime/runtime.sock", Net: "unix"})
	if want := slices.Concat([]string{"/opt/agentos/run-worker.sh", "/run/agentos/runtime/runtime.sock", "/opt/venv", "/opt/code", "--"},
		worker, []string{"--endpoint", "127.0.0.1:7233"}, args); err != nil || !slices.Equal(o.Argv, want) || o.StateDirs || o.Lifeline {
		t.Errorf("Linux: %v\n got %q\nwant %q, %+v", err, o.Argv, want, o)
	}
	cfg = workerConfig()
	cfg.Worker.Sandbox = nil
	for _, goos := range []string{"darwin", "linux"} {
		o, err = workerLaunch(goos, cfg, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 50123})
		if want := slices.Concat(worker, []string{"--endpoint", "127.0.0.1:50123"}, args); err != nil || !slices.Equal(o.Argv, want) ||
			o.StateDirs || o.Lifeline {
			t.Errorf("%s without a sandbox: %v\n got %q\nwant %q", goos, err, o.Argv, want)
		}
	}
	if _, err := workerLaunch("freebsd", workerConfig(), port); err == nil {
		t.Error("a launcher for freebsd")
	}
	if _, err := workerLaunch("darwin", workerConfig(), &net.UnixAddr{Name: "/r.sock", Net: "unix"}); err == nil {
		t.Error("the macOS launcher given a socket")
	}
}

// The Worker's environment is agentd's own making, from scratch, the same on every platform,
// sandboxed or not: these keys and no others, whatever agentd's own environment holds. So no
// secret reaches the Worker that way, nor the log of its start, which shows it. (The supervisor
// adds AGENTOS_SANDBOX_STATE and AGENTOS_SANDBOX_LIFELINE where StateDirs and Lifeline say:
// internal/supervisor's TestWhatTheWorkerGets.)
func TestTheWorkersEnvironment(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "sk-ant-canary")
	t.Setenv("OPENAI_API_KEY", "sk-canary")
	t.Setenv("GITHUB_TOKEN", "ghp_canary")
	t.Setenv("HTTPS_PROXY", "http://user:canary@proxy:8080")
	t.Setenv("HOME", "/home/someone")
	t.Setenv("PATH", "/usr/local/bin:/usr/bin:/bin")
	want := []string{"AGENTOS_SANDBOX_DENY_READ", "HOME", "LANG", "PATH"}
	for name, c := range map[string]struct {
		goos, listen string
		sandboxed    bool
	}{
		"macOS":                   {"darwin", "tcp://127.0.0.1:7244", true},
		"macOS, unsandboxed":      {"darwin", "tcp://127.0.0.1:7244", false},
		"Linux":                   {"linux", "unix:///run/agentos/runtime/runtime.sock", true},
		"Linux, unsandboxed":      {"linux", "tcp://127.0.0.1:7244", false},
		"Linux, unsandboxed, ::1": {"linux", "tcp://[::1]:7244", false},
	} {
		cfg := workerConfig()
		cfg.RuntimeListen = c.listen
		if !c.sandboxed {
			cfg.Worker.Sandbox = nil
		}
		var addr net.Addr = port
		if path, ok := strings.CutPrefix(c.listen, "unix://"); ok {
			addr = &net.UnixAddr{Name: path, Net: "unix"}
		}
		o, err := workerLaunch(c.goos, cfg, addr)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		env := map[string]string{}
		for _, kv := range o.Env {
			k, v, _ := strings.Cut(kv, "=")
			if _, twice := env[k]; twice {
				t.Errorf("%s: %s twice", name, k)
			}
			env[k] = v
			if strings.Contains(v, "canary") {
				t.Errorf("%s: %s is agentd's own: %q", name, k, kv)
			}
		}
		if keys := slices.Sorted(maps.Keys(env)); !slices.Equal(keys, want) {
			t.Errorf("%s: the keys %q, want %q", name, keys, want)
		}
		var deny []string
		if err := json.Unmarshal([]byte(env["AGENTOS_SANDBOX_DENY_READ"]), &deny); err != nil ||
			!slices.Equal(deny, append(toolDenyRead(cfg), "/srv/more")) {
			t.Errorf("%s: deny %q, %v", name, deny, err)
		}
		if env["HOME"] != "/home/someone" || env["PATH"] != "/usr/local/bin:/usr/bin:/bin" || env["LANG"] != "C.UTF-8" {
			t.Errorf("%s: env %v", name, env)
		}
	}
}

// Without a launcher, agentd runs a Worker only with --insecure-dev; and the configuration's checks
// apply either way.
func TestCheckWorker(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"venv/bin", "code"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "venv/bin/python"), nil, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{DataDir: filepath.Join(dir, "data"), ControlSocket: filepath.Join(dir, "c/control.sock"),
		RuntimeListen: "tcp://127.0.0.1:7244",
		Worker:        config.Worker{Python: filepath.Join(dir, "venv"), Code: filepath.Join(dir, "code"), Workflows: config.DefaultWorkflows}}
	if err := checkWorker("darwin", cfg, false); err == nil || !strings.Contains(err.Error(), "worker.sandbox is required") ||
		!strings.Contains(err.Error(), "--no-worker") {
		t.Errorf("no sandbox: %v", err)
	}
	if err := checkWorker("darwin", cfg, true); err != nil {
		t.Errorf("no sandbox, --insecure-dev: %v", err)
	}
	cfg.Worker.Code = cfg.DataDir
	if err := checkWorker("darwin", cfg, true); err == nil {
		t.Error("the data directory as the code directory")
	}
}

// agentd.started says how the Worker runs: its launcher, what it may read, what it serves, and its
// limits, none yet.
func TestWorkerRecord(t *testing.T) {
	r := workerRecord(workerConfig())
	if r["sandbox"] != "/opt/agentos/run-worker.sh" || r["python"] != "/opt/venv" || r["code"] != "/opt/code" ||
		r["workflows"] != "agentos_worker.workflows:WORKFLOWS" || len(r["limits"].(map[string]any)) != 0 || len(r) != 5 {
		t.Errorf("%v", r)
	}
	cfg := workerConfig()
	cfg.Worker.Sandbox = nil
	if r := workerRecord(cfg); r["sandbox"] != "" {
		t.Errorf("unsandboxed: %v", r)
	}
}
