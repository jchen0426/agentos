package main

import (
	"encoding/json"
	"log/slog"
	"maps"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/jchen0426/agentos/internal/codesnap"
	"github.com/jchen0426/agentos/internal/config"
	"github.com/jchen0426/agentos/internal/models"
	"github.com/jchen0426/agentos/internal/scope"
	"github.com/jchen0426/agentos/internal/supervisor"
)

func workerConfig() *config.Config {
	return &config.Config{DataDir: "/srv/agentos/data", ControlSocket: "/run/agentos/control/control.sock",
		RuntimeListen: "tcp://127.0.0.1:7244", Temporal: config.Temporal{Namespace: "ns"},
		TaskQueues: config.TaskQueues{Workflows: "wq"}, Repos: map[string]config.Repo{"a/b": {Mirror: "/srv/mirrors/b"}},
		Worker: config.Worker{Sandbox: []string{"/opt/agentos/run-worker.sh"}, Python: "/opt/venv", Code: "/opt/code",
			Workflows: "agentos_worker.workflows:WORKFLOWS", DenyRead: []string{"/srv/more"}}}
}

var port = &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7244}

// snap is agentd's copy of the Worker's code, as codesnap makes it.
var snap = &codesnap.Snapshot{Dir: "/tmp/agentos-code.1.x", Python: "/tmp/agentos-code.1.x/python", Code: "/tmp/agentos-code.1.x/code",
	Root: "c0de", Files: 3, Bytes: 42, Distributions: []string{"temporalio==1.34.0"}, Interpreter: "/usr/bin/python3.12"}

// agentd runs the Worker through its platform's launcher, which takes the Runtime entry as that
// launcher reaches it (macOS a port, Linux the socket), the Python environment and the code
// directory; or, without a launcher, alone. Its command is the same either way, with the
// configuration's namespace, task queue and workflows. On Linux the supervisor gives the launcher
// the Worker's resource limits too (M5 C3a).
func TestWorkerLaunch(t *testing.T) {
	// The copy's, never the live directories (/opt/venv, /opt/code).
	worker := []string{"/tmp/agentos-code.1.x/python/bin/python", "-m", "agentos_worker"}
	args := []string{"--namespace", "ns", "--task-queue", "wq", "--workflows", "agentos_worker.workflows:WORKFLOWS", "--stop-on-stdin-eof"}
	cfg := workerConfig()
	o, err := workerLaunch("darwin", cfg, snap, port, nil)
	if want := slices.Concat([]string{"/opt/agentos/run-worker.sh", "7244", snap.Python, snap.Code, "--"}, worker,
		[]string{"--endpoint", "127.0.0.1:7244", "--http-connect-proxy", "env"}, args); err != nil || !slices.Equal(o.Argv, want) ||
		!o.StateDirs || !o.Lifeline || o.Dir != snap.Code || o.Code != "c0de" {
		t.Errorf("macOS: %v\n got %q\nwant %q, %+v", err, o.Argv, want, o)
	}
	if o.Limits != nil {
		t.Errorf("macOS: limits %v", o.Limits)
	}
	cfg.RuntimeListen = "unix:///run/agentos/runtime/runtime.sock"
	limits := scope.New("/run/user/501", nil, nil).On("worker", scope.Limits{Memory: 1 << 30, Tasks: 512})
	o, err = workerLaunch("linux", cfg, snap, &net.UnixAddr{Name: "/run/agentos/runtime/runtime.sock", Net: "unix"}, limits)
	if want := slices.Concat([]string{"/opt/agentos/run-worker.sh", "/run/agentos/runtime/runtime.sock", snap.Python, snap.Code, "--"},
		worker, []string{"--endpoint", "127.0.0.1:7233"}, args); err != nil || !slices.Equal(o.Argv, want) || o.StateDirs || o.Lifeline ||
		o.Dir != snap.Code || o.Limits != supervisor.Limits(limits) {
		t.Errorf("Linux: %v\n got %q\nwant %q, %+v", err, o.Argv, want, o)
	}
	cfg = workerConfig()
	cfg.Worker.Sandbox = nil
	for _, goos := range []string{"darwin", "linux"} {
		o, err = workerLaunch(goos, cfg, snap, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 50123}, nil)
		if want := slices.Concat(worker, []string{"--endpoint", "127.0.0.1:50123"}, args); err != nil || !slices.Equal(o.Argv, want) ||
			o.StateDirs || o.Lifeline || o.Dir != snap.Code {
			t.Errorf("%s without a sandbox: %v\n got %q\nwant %q", goos, err, o.Argv, want)
		}
	}
	if _, err := workerLaunch("freebsd", workerConfig(), snap, port, nil); err == nil {
		t.Error("a launcher for freebsd")
	}
	if _, err := workerLaunch("darwin", workerConfig(), snap, &net.UnixAddr{Name: "/r.sock", Net: "unix"}, nil); err == nil {
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
		o, err := workerLaunch(c.goos, cfg, snap, addr, nil)
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

// What the Worker must not read, which its launcher denies and the copy of its code may not hold:
// agentd's data, its sockets' directories, the Temporal client's files, the mirrors, and the deny list.
func TestWorkerPrivate(t *testing.T) {
	cfg := workerConfig()
	cfg.RuntimeListen = "unix:///run/agentos/runtime/runtime.sock"
	cfg.Temporal.TLS = &config.TLS{CertFile: "/etc/agentos/client.crt", KeyFile: "/etc/agentos/client.key"}
	want := []string{"/srv/agentos/data", "/run/agentos/control", "/run/agentos/runtime", "/etc/agentos/client.crt",
		"/etc/agentos/client.key", "/srv/mirrors/b", "/srv/more"}
	if got := workerPrivate(cfg); !slices.Equal(got, want) {
		t.Errorf("%q, want %q", got, want)
	}
}

// agentd.started says how the Worker runs: its launcher, what it may read, what it serves, and its
// limits, with what holds them.
func TestWorkerRecord(t *testing.T) {
	cfg := workerConfig()
	cfg.Worker.Limits = config.Limits{Memory: 768 << 20, Tasks: 300}
	r := workerRecord(cfg, snap, supervisor.Timing{}.WithDefaults(), enforcedByScope)
	if r["sandbox"] != "/opt/agentos/run-worker.sh" || r["python"] != "/opt/venv" || r["code"] != "/opt/code" ||
		r["workflows"] != "agentos_worker.workflows:WORKFLOWS" {
		t.Errorf("%v", r)
	}
	// Its limits, with no swap, and what holds them (M5 C3a).
	if l := r["limits"].(map[string]any); !maps.Equal(l, map[string]any{"memory_bytes": int64(768 << 20), "tasks": int64(300),
		"swap_bytes": 0, "enforcement": "systemd_user_scope"}) {
		t.Errorf("limits %v", l)
	}
	// The copy it runs, and the thresholds of its liveness.
	if r["code_sha256"] != "c0de" || r["code_files"] != 3 || r["code_bytes"] != int64(42) ||
		!slices.Equal(r["distributions"].([]string), []string{"temporalio==1.34.0"}) || r["interpreter"] != "/usr/bin/python3.12" ||
		!maps.Equal(r["liveness"].(map[string]any), map[string]any{"quiet_ms": int64(60000), "hold_ms": int64(60000)}) || len(r) != 11 {
		t.Errorf("%v", r)
	}
	cfg = workerConfig()
	cfg.Worker.Sandbox = nil
	// With the limits off (or none), the record says only that.
	if r := workerRecord(cfg, snap, supervisor.Timing{}, enforcedOff); r["sandbox"] != "" ||
		!maps.Equal(r["limits"].(map[string]any), map[string]any{"enforcement": "off"}) {
		t.Errorf("unsandboxed: %v", r)
	}
}

// agentd starts no Worker of its own without a sandbox while its configuration names a real secret
// (the user's decision): the Worker would run as the user, and could read it from the keychain. Each
// such setting is named, and the way out. A sandboxed Worker, or none (--no-worker), is no matter.
func TestNoUnsandboxedWorkerNextToASecret(t *testing.T) {
	mods := &models.Config{Providers: map[string]models.Provider{"fake": {Adapter: "fake"},
		"spare": {Adapter: "anthropic", Key: "keyring:agentos-test"}, "local": {Adapter: "openai_compat", BaseURL: "http://127.0.0.1:1/v1"}}}
	cfg := workerConfig()
	cfg.Worker.Sandbox = nil
	cfg.GitHub = &config.GitHub{Token: "keyring:agentos-github-token"}
	err := checkUnsandboxedSecrets(cfg, mods, true)
	if err == nil || !strings.Contains(err.Error(), "models.yaml's providers.spare.key, agentd.yaml's github.token names a real secret") ||
		!strings.Contains(err.Error(), "--no-worker") || strings.Contains(err.Error(), "providers.local") {
		t.Errorf("unsandboxed, next to two secrets: %v", err)
	}
	cfg.GitHub = &config.GitHub{APIURL: "http://127.0.0.1:1/"} // a simulator: no token
	if err := checkUnsandboxedSecrets(cfg, mods, true); err == nil || strings.Contains(err.Error(), "github") {
		t.Errorf("unsandboxed, next to a key: %v", err)
	}
	noKeys := &models.Config{Providers: map[string]models.Provider{"fake": {Adapter: "fake"}}}
	if err := checkUnsandboxedSecrets(cfg, noKeys, true); err != nil {
		t.Errorf("unsandboxed, no secret: %v", err)
	}
	// The Temporal client key: with it a Worker would reach Temporal as agentd, past the Runtime proxy.
	cfg.Temporal.TLS = &config.TLS{CertFile: "/etc/agentos/agentd.crt", KeyFile: "/etc/agentos/agentd.key"}
	if err := checkUnsandboxedSecrets(cfg, noKeys, true); err == nil || !strings.Contains(err.Error(), "agentd.yaml's temporal.tls.key_file names") {
		t.Errorf("unsandboxed, next to the Temporal client key: %v", err)
	}
	cfg.Temporal.TLS = nil
	if err := checkUnsandboxedSecrets(workerConfig(), mods, true); err != nil {
		t.Errorf("sandboxed: %v", err)
	}
	if err := checkUnsandboxedSecrets(cfg, mods, false); err != nil { // checkWorker refuses that already
		t.Errorf("no --insecure-dev: %v", err)
	}
}

// stopping records the order in which agentd stops what it serves.
type stopping struct {
	name  string
	order *[]string
}

func (s stopping) Stop()         { *s.order = append(*s.order, s.name) }
func (s stopping) GracefulStop() { *s.order = append(*s.order, s.name) }

// On SIGTERM agentd stops its Worker first, then the Control server, then the Runtime entry: the
// Worker reports what it holds through the Runtime entry, and a supervisor still running once it is
// closed would start a Worker that cannot reach it. (On an error, run's deferred Stop of the
// supervisor is its latest defer, and runs before the servers': no test here.)
func TestTheWorkerStopsFirst(t *testing.T) {
	var order []string
	stopInOrder(slog.New(slog.DiscardHandler), stopping{"worker", &order}, stopping{"control", &order}, stopping{"runtime", &order})
	if want := []string{"worker", "control", "runtime"}; !slices.Equal(order, want) {
		t.Fatalf("stopped %q, want %q", order, want)
	}
}

// agentd copies the Worker's code with what the Worker must not read, and, sandboxed, what its launcher
// does not show it.
func TestSnapOptions(t *testing.T) {
	cfg := workerConfig()
	o := snapOptions("darwin", cfg)
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(o.Private, workerPrivate(cfg)) || o.Visible == nil || o.Visible(filepath.Join(home, "proj")) || !o.Visible("/usr/lib") {
		t.Errorf("macOS: %q, shows the home %v", o.Private, o.Visible != nil && o.Visible(filepath.Join(home, "proj")))
	}
	if o := snapOptions("linux", cfg); o.Visible == nil || o.Visible("/opt/proj") {
		t.Error("Linux: shows /opt")
	}
	cfg.Worker.Sandbox = nil
	if o := snapOptions("darwin", cfg); o.Visible != nil || !slices.Equal(o.Private, workerPrivate(cfg)) {
		t.Errorf("unsandboxed: %+v", o)
	}
}

// The copy of the Worker's code takes a directory a .pth names outside its Python environment and its
// code directory only where the Worker's launcher lets it read it: without one, everywhere; on macOS,
// outside the homes and the shared temporary directories; on Linux, in /usr.
func TestWorkerVisible(t *testing.T) {
	for _, goos := range []string{"darwin", "linux"} {
		if workerVisible(goos, false) != nil {
			t.Errorf("%s, unsandboxed: not everything", goos)
		}
	}
	if workerVisible("plan9", true)("/usr/lib") {
		t.Error("a launcher agentd does not know shows something")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	darwin := workerVisible("darwin", true)
	for path, shown := range map[string]bool{
		filepath.Join(home, "proj"): false, strings.ToUpper(filepath.Join(home, "proj")): false,
		"/tmp/x": false, "/private/tmp/x": false, "/var/tmp/x": false, "/private/var/tmp/x": false,
		"/var/folders/ab/T": false, "/private/var/folders/ab/T": false,
		"/usr/lib/python3.12": true, "/opt/homebrew/lib/python3.12/site-packages": true, "/Library/Python": true,
		// The data volume's names for the same directories, which Seatbelt hides too.
		filepath.Join("/System/Volumes/Data", realPath(home), "proj"): false, "/System/Volumes/Data/private/tmp/x": false,
		"/system/volumes/data/private/var/folders/ab": false, "/System/Volumes/Data/Library": true,
	} {
		if darwin(path) != shown {
			t.Errorf("macOS: %s shown %v, want %v", path, !shown, shown)
		}
	}
	if u, err := user.Current(); err == nil { // the user database's home, whatever $HOME says
		t.Setenv("HOME", t.TempDir())
		if workerVisible("darwin", true)(filepath.Join(u.HomeDir, "proj")) {
			t.Errorf("macOS: %s shown", u.HomeDir)
		}
	}
	linux := workerVisible("linux", true)
	for path, shown := range map[string]bool{
		"/usr/lib/python3/dist-packages": true, "/usr/local/lib/python3.12/site-packages": true,
		"/home/u/proj": false, "/opt/proj": false, "/tmp/x": false, "/usrx": false,
	} {
		if linux(path) != shown {
			t.Errorf("Linux: %s shown %v, want %v", path, !shown, shown)
		}
	}
}

// The macOS launcher denies each home's .agentos wherever it really is, and the data volume's names
// for what it denies.
func TestDarwinHidden(t *testing.T) {
	hidden := darwinHidden([]string{"/Users/u"})
	for _, want := range []string{"/Users/u", "/Users/u/.agentos", "/tmp", "/System/Volumes/Data/Users/u", "/System/Volumes/Data/Users/u/.agentos"} {
		if !slices.Contains(hidden, want) {
			t.Errorf("%s not hidden: %q", want, hidden)
		}
	}
}

// macOS's sandbox judges a path by its real path, and APFS's names regardless of case: so does the
// copy, for the directories it hides and for the path.
func TestHiddenIn(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"Home/sub", "other"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for link, target := range map[string]string{"home-link": "Home", "sneaky": "Home/sub"} {
		if err := os.Symlink(target, filepath.Join(dir, link)); err != nil {
			t.Fatal(err)
		}
	}
	shown := hiddenIn([]string{filepath.Join(dir, "home-link"), "relative"})
	for path, want := range map[string]bool{
		"home-link/x": false, "Home/sub": false, "Home/not-yet/x": false, "HOME/sub": false, "sneaky": false,
		"other": true, "other/x": true, "Homes": true,
	} {
		if got := shown(filepath.Join(dir, path)); got != want {
			t.Errorf("%s shown %v, want %v", path, got, want)
		}
	}
}

// Linux's sandbox has only /usr of the host's, and its links to it: the copy takes a path whose real
// path is in /usr only if its first name is one of those, as the sandbox looks it up; /lib64 only
// where /usr/lib64 is, as the launcher makes it only then.
func TestBwrapShown(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"usr/lib/py", "other"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for link, target := range map[string]string{"lib": "usr/lib", "lib64": "usr/lib", "opt": "usr/lib", "etc": "/nowhere",
		"usr/lib/out": "../../other"} {
		if err := os.Symlink(target, filepath.Join(dir, link)); err != nil {
			t.Fatal(err)
		}
	}
	if bwrapShown(dir)(filepath.Join(dir, "lib64", "py")) {
		t.Error("lib64/py shown, with no usr/lib64")
	}
	if err := os.Mkdir(filepath.Join(dir, "usr", "lib64"), 0o755); err != nil {
		t.Fatal(err)
	}
	shown := bwrapShown(dir)
	if !shown(filepath.Join(dir, "lib64", "py")) {
		t.Error("lib64/py not shown, with usr/lib64")
	}
	for path, want := range map[string]bool{
		"usr/lib/py": true, "lib/py": true, "usr/lib/not-yet": true, "etc": false, "other": false,
		"opt/py":      false, // in /usr by its real path, but the sandbox has no /opt
		"usr/lib/out": false, // in /usr by its name, but it leads out
	} {
		if got := shown(filepath.Join(dir, path)); got != want {
			t.Errorf("%s shown %v, want %v", path, got, want)
		}
	}
}
