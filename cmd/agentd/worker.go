package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/jchen0426/agentos/internal/config"
	"github.com/jchen0426/agentos/internal/supervisor"
)

// checkWorker refuses, before agentd changes anything, a Worker it cannot run as the configuration
// says on goos (unless --no-worker): without a launcher only with --insecure-dev, and as
// config.CheckWorker says.
func checkWorker(goos string, cfg *config.Config, insecureDev bool) error {
	sandboxed := len(cfg.Worker.Sandbox) > 0
	if !sandboxed && !insecureDev {
		return errors.New("worker.sandbox is required: the launcher in sandbox/macos or sandbox/linux (or start agentd " +
			"with --insecure-dev, for development only, or with --no-worker and a Worker you start yourself)")
	}
	return cfg.CheckWorker(goos, sandboxed)
}

// workerLaunch is how agentd runs the Worker on goos, once the Runtime entry listens at runtime:
// through its launcher, which takes the Runtime entry (macOS a port, Linux the socket), the Python
// environment and the code directory, then the Worker's command; or, with no launcher
// (--insecure-dev), the Worker's command alone. The environment is agentd's own making, from
// scratch: what the launchers need (PATH for node, bwrap and socat; HOME, which the macOS launcher
// denies), and what agentd keeps from the Worker. None of agentd's own environment, and no secret:
// a Worker never gets one.
func workerLaunch(goos string, cfg *config.Config, runtime net.Addr) (supervisor.Options, error) {
	w := cfg.Worker
	python := filepath.Join(w.Python, "bin", "python")
	args := []string{"--namespace", cfg.Temporal.Namespace, "--task-queue", cfg.TaskQueues.Workflows, "--workflows", w.Workflows}
	deny, err := json.Marshal(slices.Concat(toolDenyRead(cfg), w.DenyRead))
	if err != nil {
		return supervisor.Options{}, err
	}
	path := os.Getenv("PATH")
	if path == "" {
		path = "/usr/bin:/bin"
	}
	env := []string{"PATH=" + path}
	if home, err := os.UserHomeDir(); err == nil {
		env = append(env, "HOME="+home)
	}
	env = append(env, "LANG=C.UTF-8", "AGENTOS_SANDBOX_DENY_READ="+string(deny))
	o := supervisor.Options{Env: env, Dir: w.Code}
	command := []string{python, "-m", "agentos_worker"}
	switch {
	case len(w.Sandbox) == 0:
		o.Argv = slices.Concat(command, []string{"--endpoint", runtime.String()}, args)
	case goos == "darwin":
		tcp, ok := runtime.(*net.TCPAddr)
		if !ok {
			return supervisor.Options{}, fmt.Errorf("the macOS launcher takes a port, not %s", runtime)
		}
		port := strconv.Itoa(tcp.Port)
		o.Argv = slices.Concat(w.Sandbox, []string{port, w.Python, w.Code, "--"}, command,
			[]string{"--endpoint", "127.0.0.1:" + port, "--http-connect-proxy", "env"}, args)
		// A state directory of each launch's own, and the lifeline: macOS has no parent-death signal.
		o.StateDirs, o.Lifeline = true, true
	case goos == "linux":
		socket, ok := strings.CutPrefix(cfg.RuntimeListen, "unix://")
		if !ok {
			return supervisor.Options{}, fmt.Errorf("the Linux launcher takes a socket, not %s", cfg.RuntimeListen)
		}
		// Inside the sandbox socat listens on 127.0.0.1:7233 and forwards to the socket. bwrap's
		// --die-with-parent ends the sandbox with agentd; the Worker writes nothing.
		o.Argv = slices.Concat(w.Sandbox, []string{socket, w.Python, w.Code, "--"}, command,
			[]string{"--endpoint", "127.0.0.1:7233"}, args)
	default:
		return supervisor.Options{}, fmt.Errorf("agentd has no Worker launcher for %s", goos)
	}
	return o, nil
}

// workerRecord is the Worker in agentd.started: its launcher ("" for none), what it may read, what
// it serves, and its limits, of which this agentd sets none.
func workerRecord(cfg *config.Config) map[string]any {
	w := cfg.Worker
	sandbox := ""
	if len(w.Sandbox) > 0 {
		sandbox = w.Sandbox[0]
	}
	return map[string]any{"sandbox": sandbox, "python": w.Python, "code": w.Code, "workflows": w.Workflows,
		"limits": map[string]any{}}
}
