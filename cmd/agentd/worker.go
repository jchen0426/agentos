package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/jchen0426/agentos/internal/codesnap"
	"github.com/jchen0426/agentos/internal/config"
	"github.com/jchen0426/agentos/internal/models"
	"github.com/jchen0426/agentos/internal/scope"
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

// workerLaunch is how agentd runs the Worker on goos, once the Runtime entry listens at runtime, from
// snap, agentd's copy of the Python environment and the code directory: through its launcher, which
// takes the Runtime entry (macOS a port, Linux the socket), the copies of the two directories, then the
// Worker's command; or, with no launcher (--insecure-dev), the Worker's command alone. The live
// directories are no part of it. The environment is agentd's own making, from scratch: what the
// launchers need (agentd's own PATH, for node, bwrap and socat, and HOME, which the macOS launcher
// denies), and what agentd keeps from the Worker. Nothing else of agentd's own environment, and no
// secret: a Worker never gets one. limits are what the launcher gets of each generation's resource
// limits (the supervisor adds them: Options.Limits); nil for none.
func workerLaunch(goos string, cfg *config.Config, snap *codesnap.Snapshot, runtime net.Addr, limits *scope.Set) (supervisor.Options, error) {
	w := cfg.Worker
	python := filepath.Join(snap.Python, "bin", "python")
	// The Worker stops, gracefully, when its stdin ends: the supervisor's stop channel.
	args := []string{"--namespace", cfg.Temporal.Namespace, "--task-queue", cfg.TaskQueues.Workflows, "--workflows", w.Workflows,
		"--stop-on-stdin-eof"}
	deny, err := json.Marshal(workerPrivate(cfg))
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
	o := supervisor.Options{Env: env, Dir: snap.Code, Code: snap.Root}
	if limits != nil { // a nil *scope.Set in the interface would not be nil
		o.Limits = limits
	}
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
		o.Argv = slices.Concat(w.Sandbox, []string{port, snap.Python, snap.Code, "--"}, command,
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
		o.Argv = slices.Concat(w.Sandbox, []string{socket, snap.Python, snap.Code, "--"}, command,
			[]string{"--endpoint", "127.0.0.1:7233"}, args)
	default:
		return supervisor.Options{}, fmt.Errorf("agentd has no Worker launcher for %s", goos)
	}
	return o, nil
}

// workerPrivate is what the Worker must not read: what the tools must not (agentd's data, its sockets'
// directories, the Temporal client's files, the mirrors), and the deny list. Its launcher denies it
// (AGENTOS_SANDBOX_DENY_READ), and the copy of its code holds none of it (codesnap.Make).
func workerPrivate(cfg *config.Config) []string {
	return slices.Concat(toolDenyRead(cfg), cfg.Worker.DenyRead)
}

// snapOptions are what the copy of the Worker's code on goos may not hold: what the Worker must not read
// (workerPrivate), and what its launcher does not let it read (workerVisible).
func snapOptions(goos string, cfg *config.Config) codesnap.Options {
	return codesnap.Options{Private: workerPrivate(cfg), Visible: workerVisible(goos, len(cfg.Worker.Sandbox) > 0)}
}

// workerVisible is, for codesnap.Make, whether the Worker's launcher on goos lets it read a path
// outside its Python environment and its code directory (which every launcher lets it read, wherever
// they are): a directory a .pth names there goes into the copy of its code only if so. nil without a
// launcher (sandboxed false): the Worker reads everything. It mirrors the launchers, and changes with
// them:
//   - sandbox/macos/run-worker.sh (srt's denyRead: its denied, through both): nothing in a home
//     directory, $HOME's or the user database's, or its .agentos, nor in a shared temporary
//     directory, /tmp, /var/tmp or /var/folders, or /private's; by the path and by its real path, as
//     the launcher lists both, whatever the case, as APFS compares names, and by the data volume's
//     names for them (/System/Volumes/Data/Users/...), as Seatbelt resolves them (darwinHidden).
//   - sandbox/linux/run-worker.sh (bwrap's --ro-bind /usr /usr, and its links into it: /bin, /sbin,
//     /lib, and /lib64 where /usr/lib64 is): only what is in /usr, by its real path; and by the path,
//     whose first name the sandbox looks up in its own root, which holds nothing else of the host's
//     (bwrapShown).
//
// The deny list both launchers take (AGENTOS_SANDBOX_DENY_READ, workerPrivate) is not in it:
// codesnap refuses to copy from in or around any of it.
func workerVisible(goos string, sandboxed bool) func(string) bool {
	if !sandboxed {
		return nil
	}
	switch goos {
	case "darwin":
		var homes []string
		if home, err := os.UserHomeDir(); err == nil {
			homes = append(homes, home)
		}
		if u, err := user.Current(); err == nil {
			homes = append(homes, u.HomeDir)
		}
		return hiddenIn(darwinHidden(homes))
	case "linux":
		return bwrapShown("/")
	}
	return func(string) bool { return false } // a launcher agentd does not know: nothing
}

// darwinHidden is what the macOS launcher denies the Worker with homes, its home directories, but the
// deny list: its denied, the homes and their .agentos among them (whose real path may be elsewhere);
// and the data volume's names for them all, which Seatbelt hides as well: /Users and /private are
// firmlinks into /System/Volumes/Data, which no link resolves.
func darwinHidden(homes []string) []string {
	hidden := []string{"/tmp", "/private/tmp", "/var/tmp", "/private/var/tmp", "/var/folders", "/private/var/folders"}
	for _, h := range homes {
		hidden = append(hidden, h, filepath.Join(h, ".agentos"))
	}
	for _, h := range slices.Clone(hidden) {
		if filepath.IsAbs(h) {
			hidden = append(hidden, filepath.Join("/System/Volumes/Data", realPath(h)))
		}
	}
	return hidden
}

// hiddenIn shows a path unless it is in one of dirs, by its name or its real path, compared with
// theirs, whatever the case.
func hiddenIn(dirs []string) func(string) bool {
	var folded []string
	for _, d := range dirs {
		if filepath.IsAbs(d) {
			folded = append(folded, strings.ToLower(filepath.Clean(d)), strings.ToLower(realPath(d)))
		}
	}
	return func(path string) bool {
		for _, p := range []string{filepath.Clean(path), realPath(path)} {
			for _, d := range folded {
				if inside(strings.ToLower(p), d) {
					return false
				}
			}
		}
		return true
	}
}

// bwrapShown is what the Linux launcher's sandbox shows of the host's files under root ("/", but in
// tests): /usr, and the links to it its root has, /bin, /sbin and /lib, and /lib64 only where
// /usr/lib64 is ([[ -e /usr/lib64 ]] && --symlink usr/lib64 /lib64).
func bwrapShown(root string) func(string) bool {
	names := []string{"usr", "bin", "sbin", "lib"}
	if _, err := os.Stat(filepath.Join(root, "usr", "lib64")); err == nil {
		names = append(names, "lib64")
	}
	for i, n := range names {
		names[i] = filepath.Join(root, n)
	}
	return shownIn(realPath(filepath.Join(root, "usr")), names)
}

// shownIn shows a path only if its real path is in root, and its name in one of names: root, and the
// links to it the sandbox makes in its own root.
func shownIn(root string, names []string) func(string) bool {
	return func(path string) bool {
		if !inside(realPath(path), root) {
			return false
		}
		for _, n := range names {
			if inside(filepath.Clean(path), n) {
				return true
			}
		}
		return false
	}
}

// realPath is path's real path, as the macOS launcher's real makes it: where path does not resolve,
// the real path of its directory, and its name.
func realPath(path string) string {
	if r, err := filepath.EvalSymlinks(path); err == nil {
		return r
	}
	dir := filepath.Dir(path)
	if dir == path {
		return path
	}
	return filepath.Join(realPath(dir), filepath.Base(path))
}

// inside reports whether path is dir or in it, by their names.
func inside(path, dir string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// workerRecord is the Worker in agentd.started: its launcher ("" for none), the directories it runs a
// copy of, what it serves, and its resource limits with what holds them (enforcement, limitsRecord); the
// copy (snap): its root, which addresses the manifest's blob in the ledger, its size, the distributions
// in the environment and the interpreter; and the liveness thresholds, t's.
func workerRecord(cfg *config.Config, snap *codesnap.Snapshot, t supervisor.Timing, enforcement string) map[string]any {
	w := cfg.Worker
	sandbox := ""
	if len(w.Sandbox) > 0 {
		sandbox = w.Sandbox[0]
	}
	return map[string]any{"sandbox": sandbox, "python": w.Python, "code": w.Code, "workflows": w.Workflows,
		"limits": limitsRecord(w.Limits, enforcement), "code_sha256": snap.Root, "code_files": snap.Files, "code_bytes": snap.Bytes,
		"distributions": slices.Clone(snap.Distributions), "interpreter": snap.Interpreter,
		"liveness": map[string]any{"quiet_ms": t.QuietFor.Milliseconds(), "hold_ms": t.HoldFor.Milliseconds()}}
}

// checkUnsandboxedSecrets refuses to start agentd's Worker without a sandbox (--insecure-dev, no
// worker.sandbox) while the configuration names a real secret: the Worker would run as the user,
// and could read it itself, from the keychain or its file. The way out is --no-worker, with a Worker
// started by hand: then it is the user's own doing. It guards against a mistake; it contains nothing:
// an unsandboxed Worker can read whatever its user can, secrets the configuration does not name too.
func checkUnsandboxedSecrets(cfg *config.Config, mods *models.Config, insecureDev bool) error {
	if len(cfg.Worker.Sandbox) > 0 || !insecureDev {
		return nil
	}
	named := secretsIn(cfg, mods)
	if len(named) == 0 {
		return nil
	}
	return fmt.Errorf("%s names a real secret, and agentd's Worker would run without a sandbox (--insecure-dev, no "+
		"worker.sandbox), as you, able to read it: give the Worker its sandbox (worker.sandbox), or start "+
		"agentd with --no-worker and a Worker of your own", strings.Join(named, ", "))
}

// secretsIn is every setting of the configuration that names a real secret: each model provider's
// key, whether a task reaches the provider or not (the keychain holds it all the same), the GitHub
// token, and the Temporal client key, with which a Worker could reach Temporal as agentd, past the
// Runtime proxy.
func secretsIn(cfg *config.Config, mods *models.Config) []string {
	var named []string
	for _, name := range slices.Sorted(maps.Keys(mods.Providers)) {
		if mods.Providers[name].Key != "" {
			named = append(named, fmt.Sprintf("models.yaml's providers.%s.key", name))
		}
	}
	if g := cfg.GitHub; g != nil && g.Token != "" {
		named = append(named, "agentd.yaml's github.token")
	}
	if t := cfg.Temporal.TLS; t != nil && t.KeyFile != "" {
		named = append(named, "agentd.yaml's temporal.tls.key_file")
	}
	return named
}
