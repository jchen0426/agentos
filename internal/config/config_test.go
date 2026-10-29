package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jchen0426/agentos/internal/models"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/pricing"
)

// priced checks that every model each task may reach has a price, as agentd does at start.
func priced(t *testing.T, m *models.Config, pricesFile string) {
	t.Helper()
	prices, err := pricing.Load(pricesFile)
	if err != nil {
		t.Fatal(err)
	}
	for name := range m.Tasks {
		r, err := m.Route(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := prices.Bound(r.Candidates, 0, r.MaxTokens); err != nil {
			t.Errorf("task %s: %v", name, err)
		}
	}
}

func TestDevConfigLoads(t *testing.T) {
	c, err := Load("../../config/dev/agentd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{c.DataDir, c.ControlSocket, c.PolicyFile, c.ModelsFile, c.PricesFile, c.Repos["acme/payments"].Mirror} {
		if !filepath.IsAbs(p) {
			t.Errorf("%q is not absolute", p)
		}
	}
	if c.TaskQueues.Gateway != "gateway" || c.TaskQueues.Workflows != "agentos-workflows" {
		t.Errorf("task queues: %+v", c.TaskQueues)
	}
	if len(c.GitTools.Sandbox) != 1 {
		t.Fatalf("git_tools.sandbox: %v", c.GitTools.Sandbox)
	}
	if _, err := os.Stat(c.GitTools.Sandbox[0]); err != nil {
		t.Errorf("the git tool launcher: %v", err)
	}
	// agentd starts the Worker under its launcher, with the repository's environment and code.
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if w := c.Worker; len(w.Sandbox) != 1 || w.Sandbox[0] != filepath.Join(repo, "sandbox/macos/run-worker.sh") ||
		w.Python != filepath.Join(repo, ".venv") || w.Code != filepath.Join(repo, "python") || w.Workflows != DefaultWorkflows {
		t.Errorf("worker: %+v", w)
	}
	// The dev Worker as agentd checks it on macOS, where the check applies: to a .venv (made by uv sync)
	// whose Python is outside the home directory, as python.org's and Homebrew's are. A Python under
	// $HOME (uv's own) is refused on macOS, as it should be; elsewhere the check is not macOS's.
	t.Run("the dev Worker", func(t *testing.T) {
		python := filepath.Join(repo, ".venv/bin/python")
		real, err := filepath.EvalSymlinks(python)
		if err != nil {
			t.Skipf("no dev Worker to check: %v", err)
		}
		home, _ := os.UserHomeDir()
		realHome, _ := filepath.EvalSymlinks(home)
		if runtime.GOOS != "darwin" || home != "" && (within(real, home) || within(real, realHome)) {
			t.Skipf("%s leads to %s: the macOS check applies on macOS, to a Python outside $HOME (%s)", python, real, home)
		}
		if err := c.CheckWorker("darwin", true); err != nil {
			t.Errorf("the dev Worker: %v", err)
		}
	})
	if _, err := policy.Load(c.PolicyFile); err != nil {
		t.Error(err)
	}
	m, err := models.Load(c.ModelsFile)
	if err != nil {
		t.Fatal(err)
	}
	priced(t, m, c.PricesFile)
}

func TestDevClaudeConfigLoads(t *testing.T) {
	c, err := Load("../../config/dev/agentd-claude.yaml")
	if err != nil {
		t.Fatal(err)
	}
	m, err := models.Load(c.ModelsFile)
	if err != nil {
		t.Fatal(err)
	}
	priced(t, m, c.PricesFile)
	pol, err := policy.Load(c.PolicyFile)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := m.Route("audit_v1")
	if v := pol.EvaluateModel("audit_v1", r.Provider); v.Decision != policy.Allow {
		t.Errorf("the dev policy refuses %s: %s", r.Provider, v.Reason)
	}
	// A real model and real GitHub writes never meet without a person in between: injected text
	// in a diff could steer the model into a pull request. The dev policy has a person approve
	// each one (section 10); this config, without a github section, only dry-runs.
	if v := pol.EvaluateTool("github.open_draft_pr", map[string]any{"repo": "acme/payments"}, nil); v.Decision != policy.RequireApproval {
		t.Errorf("the dev policy opens pull requests without approval: %v", v.Decision)
	}
	if c.GitHub != nil {
		t.Error("agentd-claude.yaml configures GitHub: the dev configs only dry-run")
	}
	// It holds a real key: its Worker runs in the sandbox, which keeps the keychain from it.
	if len(c.Worker.Sandbox) != 1 {
		t.Errorf("worker: %+v", c.Worker)
	}
}

// The tests and the README give a Worker the repository as its code directory, and its sandbox
// lets it read all of that directory (and, on Linux, connect to any socket in it). So the dev
// configs keep agentd's data, its sockets and the mirror out of the repository, in the home
// directory, which the macOS sandbox cannot read and the Linux one does not mount.
func TestDevConfigsKeepDataOutOfTheRepository(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"agentd.yaml", "agentd-claude.yaml"} {
		c, err := Load(filepath.Join(repo, "config/dev", name))
		if err != nil {
			t.Fatal(err)
		}
		paths := []string{c.DataDir, c.ControlSocket}
		if p, ok := strings.CutPrefix(c.RuntimeListen, "unix://"); ok {
			paths = append(paths, p)
		}
		for _, r := range c.Repos {
			paths = append(paths, r.Mirror)
		}
		for _, p := range paths {
			if within(p, repo) || !within(p, home) {
				t.Errorf("%s: %s is in the repository, or outside the home directory", name, p)
			}
		}
	}
}

// On Linux the git tool server's launcher mounts all of a call's mirror, and a read-only mount
// stops neither reads nor connect(): a mirror around the data directory would hand the server
// the ledger and the Control socket. So no mirror may hold anything else of agentd's, or another
// repo's mirror (design doc, section 8), whatever name a config gives it.
func TestMirrorsHoldNothingElse(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", filepath.Join(dir, "home"))
	for _, d := range []string{"home", "real/made", "up/x", "elsewhere/a/b"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// link is another name for the mirror real; inner leads into it, away out of it; soon leads
	// nowhere yet, and soon2 too, by an absolute path that ends in /. sub leads deep into
	// elsewhere, and lat through sub/.. to elsewhere/a/later, which is not there yet: the kernel
	// takes a .. after the symlink before it.
	for name, target := range map[string]string{"link": "real", "inner": "real/made", "away": "up/x", "soon": "up/soon",
		"soon2": filepath.Join(dir, "up", "soon2") + "/", "sub": "elsewhere/a/b", "lat": "sub/../later"} {
		if err := os.Symlink(target, filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	load := func(body string) (*Config, error) {
		t.Helper()
		path := filepath.Join(dir, "agentd.yaml")
		if err := os.WriteFile(path, []byte("policy_file: p\nmodels_file: m\nprices_file: x\n"+body), 0o600); err != nil {
			t.Fatal(err)
		}
		return Load(path)
	}
	// Sockets out of the data directory, where they are by default: then the data directory is
	// all the mirror holds.
	const apart = "control_socket: c/control.sock\nruntime_listen: tcp://127.0.0.1:7244\n"
	// Each refusal names what the mirror holds, as the config gives it.
	type refusal struct{ body, held string }
	refused := map[string]refusal{
		"the data directory":              {"data_dir: m/data\n" + apart + "repos: {a/b: m}\n", "m/data"},
		"the data directory itself":       {"data_dir: m\n" + apart + "repos: {a/b: m}\n", "m"},
		"the Control socket's directory":  {"data_dir: d\ncontrol_socket: m/control/control.sock\nrepos: {a/b: m}\n", "m/control"},
		"the Runtime socket's directory":  {"data_dir: d\nruntime_listen: unix://m/runtime/runtime.sock\nrepos: {a/b: m}\n", "m/runtime"},
		"the Temporal client certificate": {"data_dir: d\ntemporal: {tls: {cert_file: m/agentd.crt, key_file: agentd.key}}\nrepos: {a/b: m}\n", "m/agentd.crt"},
		"the Temporal client key":         {"data_dir: d\ntemporal: {tls: {cert_file: agentd.crt, key_file: m/agentd.key}}\nrepos: {a/b: m}\n", "m/agentd.key"},
		"another repo's mirror":           {"data_dir: d\nrepos: {a/b: m, c/d: m/d}\n", "m/d"},
		"another repo's mirror, reversed": {"data_dir: d\nrepos: {a/b: m/b, c/d: m}\n", "m/b"},
		"another repo's mirror, the same": {"data_dir: d\nrepos: {a/b: m, c/d: m}\n", "m"},
		// All else by default: the data directory is ~/.agentos.
		"the home directory":                  {"repos: {a/b: \"~/\"}\n", "home/.agentos"},
		"a mirror through a symlink":          {"data_dir: real/data\n" + apart + "repos: {a/b: link}\n", "real/data"},
		"a mirror not made yet":               {"data_dir: real/later/data\n" + apart + "repos: {a/b: link/later}\n", "real/later/data"},
		"a data directory through a symlink":  {"data_dir: inner\n" + apart + "repos: {a/b: real}\n", "inner"},
		"a data directory not made yet":       {"data_dir: inner/data\n" + apart + "repos: {a/b: real}\n", "inner/data"},
		"a Runtime directory not made yet":    {"data_dir: d\nruntime_listen: unix://inner/runtime/runtime.sock\nrepos: {a/b: real}\n", "inner/runtime"},
		"a socket's directory through a link": {"data_dir: d\ncontrol_socket: link/control.sock\nrepos: {a/b: real}\n", "link"},
		// agentd makes up/soon when it makes the data directory: then soon is the mirror around it.
		"a mirror through a symlink to nothing yet":                        {"data_dir: up/soon/data\n" + apart + "repos: {a/b: soon}\n", "up/soon/data"},
		"a mirror through a symlink to nothing yet, by a path ending in /": {"data_dir: up/soon2/data\n" + apart + "repos: {a/b: soon2}\n", "up/soon2/data"},
		"a mirror through a symlink to nothing yet, by .. after a symlink": {"data_dir: elsewhere/a/later/data\n" + apart + "repos: {a/b: lat}\n", "elsewhere/a/later/data"},
		"a repo with an empty id":                                          {"data_dir: m/data\n" + apart + "repos: {\"\": {mirror: m, github: o/n}}\n", "m/data"},
		// The kernel takes away/.. to be up; agentd's ledger goes to the name without the .., inner.
		"a data directory with .. after a symlink": {"data_dir: " + dir + "/away/../inner\n" + apart + "repos: {a/b: real}\n", "inner"},
	}
	// Another name for the same directory, which only the file system knows: on macOS, a
	// firmlink, or another case. (On Linux a bind mount, which a test cannot make.)
	realDir, err := filepath.EvalSymlinks(filepath.Join(dir, "real"))
	if err != nil {
		t.Fatal(err)
	}
	realInfo, err := os.Stat(realDir)
	if err != nil {
		t.Fatal(err)
	}
	for name, alias := range map[string]string{"firmlink": "/System/Volumes/Data" + realDir, "case": strings.ToUpper(realDir)} {
		if a, err := os.Stat(alias); err != nil || !os.SameFile(a, realInfo) {
			t.Logf("%s: %s has no such name here", name, realDir)
			continue
		}
		if err := os.Symlink(filepath.Join(alias, "made"), filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
		refused["the mirror by another name: "+name] = refusal{"data_dir: real/data\n" + apart + "repos: {a/b: \"" + alias + "\"}\n", "real/data"}
		// The symlink leads into the mirror by the other name: where the data directory will be
		// is known only from the real path.
		refused["a symlink into the mirror by another name: "+name] = refusal{"data_dir: " + name + "/data\n" + apart + "repos: {a/b: real}\n", name + "/data"}
		refused["the data directory is the mirror by another name: "+name] = refusal{"data_dir: \"" + alias + "\"\n" + apart + "repos: {a/b: real}\n", alias}
		// The mirror is a symlink to the other name: only the directory it leads to is the one.
		if err := os.Symlink(alias, filepath.Join(dir, name+"-mirror")); err != nil {
			t.Fatal(err)
		}
		refused["a symlink to the mirror by another name: "+name] = refusal{"data_dir: real/data\n" + apart + "repos: {a/b: " + name + "-mirror}\n", "real/data"}
	}
	for name, c := range refused {
		held := c.held
		if !filepath.IsAbs(held) {
			held = filepath.Join(dir, held)
		}
		// The path itself, not just one inside it.
		if _, err := load(c.body); err == nil || !regexp.MustCompile(regexp.QuoteMeta(held)+`($|[^/])`).MatchString(err.Error()) {
			t.Errorf("%s: %v", name, err)
		}
	}

	for name, body := range map[string]string{
		"a mirror in the data directory": "data_dir: d\nrepos: {a/b: d/mirror}\n", // the dev configs' layout
		"names that share a prefix":      "data_dir: m-data\ncontrol_socket: m-control/control.sock\nrepos: {a/b: m, c/d: m2}\n",
		// lat leads to elsewhere/a/later, not to later
		"a link's .. taken after the symlink before it": "data_dir: later/data\n" + apart + "repos: {a/b: lat}\n",
	} {
		if _, err := load(body); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	// Only absolute paths are compared. A Runtime endpoint without a path names no directory,
	// whatever directory agentd starts in.
	t.Chdir(filepath.Join(dir, "real"))
	if _, err := load("data_dir: d\nruntime_listen: \"unix://\"\nrepos: {a/b: real}\n"); err != nil {
		t.Errorf("unix:// started in the mirror: %v", err)
	}
}

// resolve takes a path as the kernel does where it exists, as filepath.EvalSymlinks does, and
// keeps the names that do not exist yet.
func TestResolveAsTheKernelDoes(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "a/b/c"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, target := range map[string]string{
		"rel": "a/b", "abs": filepath.Join(dir, "a"), "chain": "rel", "dots": "a/b/../b/c", "deep": "a/b/c",
	} {
		if err := os.Symlink(target, filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	// Chains of 32 links, as many as macOS follows (Linux 40): to a place, and to nowhere yet.
	for i := 1; i <= 32; i++ {
		to, nowhere := fmt.Sprintf("c%d", i+1), fmt.Sprintf("n%d", i+1)
		if i == 32 {
			to, nowhere = "a/b", "a/nowhere"
		}
		if err := os.Symlink(to, filepath.Join(dir, fmt.Sprintf("c%d", i))); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(nowhere, filepath.Join(dir, fmt.Sprintf("n%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []string{dir + "/a/b/c", dir + "/rel/c", dir + "/abs/b", dir + "/chain/c", dir + "/dots",
		dir + "/deep/..", dir + "/rel/../b", dir + "/c1/c", "/tmp", "/var"} {
		want, err := filepath.EvalSymlinks(p)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := resolve(p); got != want || err != nil {
			t.Errorf("resolve(%s) = %s, %v; want %s", p, got, err, want)
		}
		if got, err := resolve(p + "/not/yet"); got != want+"/not/yet" || err != nil {
			t.Errorf("resolve(%s/not/yet) = %s, %v; want %s/not/yet", p, got, err, want)
		}
	}
	a, err := filepath.EvalSymlinks(dir + "/a")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := resolve(dir + "/n1/c"); got != a+"/nowhere/c" || err != nil {
		t.Errorf("resolve(n1/c) = %s, %v; want %s/nowhere/c", got, err, a)
	}
	// A loop of symlinks ends, with nowhere to tell of: the kernel cannot take it either.
	for name, target := range map[string]string{"loop1": "loop2", "loop2": "loop1"} {
		if err := os.Symlink(target, filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := resolve(dir + "/loop1/x"); err == nil {
		t.Errorf("resolve(loop1/x) = %s", got)
	}
}

// Where resolve cannot follow a path, Load refuses it rather than guess. Here the real path of
// the data directory is longer than the system takes, though the kernel reaches it by a short
// one through symlinks: it would make the data directory inside the mirror.
func TestLoadRefusesWhatItCannotFollow(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "mirror"), 0o700); err != nil {
		t.Fatal(err)
	}
	// Each s leads 500 bytes deeper. They are made from inside: no name of the deepest
	// directory is short enough to make it from outside.
	t.Chdir(dir)
	short, long := dir, strings.Repeat("D", 250)
	for i := range 10 {
		step := fmt.Sprintf("%s%da/%s%db", long, i, long, i)
		if err := os.MkdirAll(step, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(step, "s"); err != nil {
			t.Fatal(err)
		}
		if err := os.Chdir("s"); err != nil {
			t.Fatal(err)
		}
		short += "/s"
	}
	if err := os.Symlink(filepath.Join(dir, "mirror"), "x"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "agentd.yaml")
	if err := os.WriteFile(path, []byte("policy_file: p\nmodels_file: m\nprices_file: x\ndata_dir: "+short+"/x/data\n"+
		"control_socket: c/control.sock\nruntime_listen: tcp://127.0.0.1:7244\nrepos: {a/b: mirror}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if c, err := Load(path); err == nil {
		t.Errorf("loaded, data_dir %s", c.DataDir)
	}
}

func TestPathsInTheHomeDirectory(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "agentd.yaml")
	if err := os.WriteFile(path, []byte("data_dir: ~/agentos-data\npolicy_file: p\nmodels_file: ~/m.yaml\nprices_file: x\n"+
		"repos:\n  a/b: ~/mirrors/b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range [][2]string{
		{c.DataDir, filepath.Join(home, "agentos-data")},
		{c.ControlSocket, filepath.Join(home, "agentos-data", "control", "control.sock")},
		{c.ModelsFile, filepath.Join(home, "m.yaml")},
		{c.Repos["a/b"].Mirror, filepath.Join(home, "mirrors", "b")},
		{c.PolicyFile, filepath.Join(dir, "p")}, // relative: next to the config
	} {
		if p[0] != p[1] {
			t.Errorf("got %s, want %s", p[0], p[1])
		}
	}

	// Only ~/ is expanded: another ~ would otherwise name a directory next to the config.
	for _, dataDir := range []string{`"~"`, "~someone/agentos"} {
		other := filepath.Join(dir, "other.yaml")
		if err := os.WriteFile(other, []byte("data_dir: "+dataDir+"\npolicy_file: p\nmodels_file: m\nprices_file: x\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if c, err := Load(other); err == nil {
			t.Errorf("data_dir %s: loaded as %s", dataDir, c.DataDir)
		}
	}

	// Without an absolute home directory, a ~/ path is an error: not a directory named ~, nor a
	// data directory relative to wherever agentd runs while its sockets are next to the config.
	for _, h := range []string{"", "relative/home"} {
		t.Setenv("HOME", h)
		if c, err := Load(path); err == nil {
			t.Errorf("HOME=%q: loaded, data_dir %s", h, c.DataDir)
		}
	}
	if err := os.WriteFile(path, []byte("data_dir: d\npolicy_file: p\nmodels_file: m\nprices_file: x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err != nil {
		t.Errorf("a config without ~/ paths needs no home directory: %v", err)
	}
}

func TestReposAndGitHub(t *testing.T) {
	dir := t.TempDir()
	// A directory whose name ends in a newline, reached by a symlink without one.
	if err := os.Mkdir(filepath.Join(dir, "nl\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("nl\n", filepath.Join(dir, "nl-link")); err != nil {
		t.Fatal(err)
	}
	write := func(body string) (*Config, error) {
		path := filepath.Join(dir, "agentd.yaml")
		if err := os.WriteFile(path, []byte("policy_file: p\nmodels_file: m\nprices_file: x\n"+body), 0o600); err != nil {
			t.Fatal(err)
		}
		return Load(path)
	}
	c, err := write("repos:\n  acme/payments: mirror\n  acme/ledger: {mirror: m2, github: jchen0426/audit-target}\n" +
		"github: {api_url: \"http://127.0.0.1:9/\"}\n")
	if err != nil {
		t.Fatal(err)
	}
	if r := c.Repos["acme/payments"]; r.Mirror != filepath.Join(dir, "mirror") || r.GitHub != "acme/payments" {
		t.Errorf("acme/payments: %+v", r)
	}
	if r := c.Repos["acme/ledger"]; r.GitHub != "jchen0426/audit-target" {
		t.Errorf("acme/ledger: %+v", r)
	}
	if g := c.GitHub; g.RequestTimeout != 30*time.Second || g.Settle != time.Minute || g.AuthorEmail != "agentos@agentos.invalid" {
		t.Errorf("github defaults: %+v", g)
	}
	for name, body := range map[string]string{
		"no token for GitHub itself": "github: {api_url: \"https://api.github.com/\"}\n",
		"github not owner/name":      "repos:\n  a/b: {mirror: m, github: nope}\n",
		"no mirror":                  "repos:\n  a/b: {github: o/n}\n",
		// The Linux launcher's shell would drop the newline, and mount m: not what Load checked.
		"a mirror with a control character": "repos:\n  a/b: \"m\\n\"\n",
		"a mirror whose real path has one":  "repos:\n  a/b: nl-link\n",
	} {
		if _, err := write(body); err == nil {
			t.Errorf("%s: loaded", name)
		}
	}
}

func TestDesignDocModelsLoad(t *testing.T) {
	m, err := models.Load("../../config/models.yaml")
	if err != nil {
		t.Fatal(err)
	}
	r, err := m.Route("audit_v1")
	if err != nil {
		t.Fatal(err)
	}
	if r.Provider != "anthropic" || r.Adapter != "anthropic" || r.Model != "claude-opus-5-5" || r.Effort != "high" || r.MaxTokens <= 0 {
		t.Errorf("route: %+v", r)
	}
	priced(t, m, "../../config/prices.yaml")
}

// The worker section's paths are the config's, like every other: relative to the file, ~/ in the
// home directory. A Worker serves the product's workflows unless the section says otherwise, within
// the default limits unless it names others (TestLimits).
func TestWorkerSection(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", filepath.Join(dir, "home"))
	load := func(body string) (*Config, error) {
		t.Helper()
		path := filepath.Join(dir, "agentd.yaml")
		if err := os.WriteFile(path, []byte("policy_file: p\nmodels_file: m\nprices_file: x\n"+body), 0o600); err != nil {
			t.Fatal(err)
		}
		return Load(path)
	}
	c, err := load("worker: {sandbox: [launch/run-worker.sh, --flag], python: venv, code: ~/code, deny_read: [secret, /abs/x]}\n")
	if err != nil {
		t.Fatal(err)
	}
	w := c.Worker
	if !slices.Equal(w.Sandbox, []string{filepath.Join(dir, "launch/run-worker.sh"), "--flag"}) || w.Python != filepath.Join(dir, "venv") ||
		w.Code != filepath.Join(dir, "home/code") || !slices.Equal(w.DenyRead, []string{filepath.Join(dir, "secret"), "/abs/x"}) ||
		w.Workflows != DefaultWorkflows || w.Limits != DefaultWorkerLimits {
		t.Errorf("worker %+v", w)
	}
	if c, err := load("worker: {workflows: \"tests.rogue_workflows:StepsOutOfLine\", limits: {}}\n"); err != nil ||
		c.Worker.Workflows != "tests.rogue_workflows:StepsOutOfLine" || c.Worker.Limits != DefaultWorkerLimits {
		t.Errorf("workflows: %v, %+v", err, c)
	}
	if _, err := load("worker: {pyhton: venv}\n"); err == nil {
		t.Error("a misspelt field: loaded")
	}
}

// Each sandbox's resource limits: the Worker's and each git tool call's, memory in MiB or GiB and
// tasks, each at its default where left out, and at least the least. Nothing else is taken: no
// swap, no bare number of bytes, no misspelt key. Only Linux holds them: a configuration that names
// any is refused on another platform, where nothing would.
func TestLimits(t *testing.T) {
	dir := t.TempDir()
	load := func(body string) (*Config, error) {
		t.Helper()
		path := filepath.Join(dir, "agentd.yaml")
		if err := os.WriteFile(path, []byte("policy_file: p\nmodels_file: m\nprices_file: x\n"+body), 0o600); err != nil {
			t.Fatal(err)
		}
		return Load(path)
	}
	c, err := load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Worker.Limits != (Limits{Memory: 1 << 30, Tasks: 512}) || c.GitTools.Limits != (Limits{Memory: 512 << 20, Tasks: 128}) {
		t.Errorf("the defaults: %+v, %+v", c.Worker.Limits, c.GitTools.Limits)
	}
	for body, want := range map[string][2]Limits{
		"worker: {limits: {memory: 2GiB, tasks: 1000}}\n":   {{Memory: 2 << 30, Tasks: 1000, Named: true}, DefaultToolLimits},
		"worker: {limits: {memory: 768MiB}}\n":              {{Memory: 768 << 20, Tasks: 512, Named: true}, DefaultToolLimits},
		"git_tools: {limits: {tasks: 64}}\n":                {DefaultWorkerLimits, {Memory: 512 << 20, Tasks: 64, Named: true}},
		"git_tools: {limits: {memory: 16MiB, tasks: 16}}\n": {DefaultWorkerLimits, {Memory: 16 << 20, Tasks: 16, Named: true}},
		"worker: {limits: {tasks: 4194304}}\n":              {{Memory: 1 << 30, Tasks: 4194304, Named: true}, DefaultToolLimits},
		"worker: {limits: {memory: 999999999GiB}}\n":        {{Memory: 999999999 << 30, Tasks: 512, Named: true}, DefaultToolLimits},
		"worker: {limits: {}}\ngit_tools: {limits: {}}\n":   {DefaultWorkerLimits, DefaultToolLimits},
	} {
		c, err := load(body)
		if err != nil || c.Worker.Limits != want[0] || c.GitTools.Limits != want[1] {
			t.Errorf("%q: %v, %+v, %+v", body, err, c.Worker.Limits, c.GitTools.Limits)
		}
	}
	for body, refusal := range map[string]string{
		"worker: {limits: {memory: 1073741824}}\n":    "write a whole number of MiB or GiB",
		"worker: {limits: {memory: 536870912}}\n":     "write a whole number of MiB or GiB",
		"worker: {limits: {memory: 1G}}\n":            "write a whole number of MiB or GiB",
		"worker: {limits: {memory: 1.5GiB}}\n":        "write a whole number of MiB or GiB",
		"worker: {limits: {memory: 0MiB}}\n":          "write a whole number of MiB or GiB",
		"worker: {limits: {memory: -1GiB}}\n":         "write a whole number of MiB or GiB",
		"worker: {limits: {memory: 512KiB}}\n":        "write a whole number of MiB or GiB",
		"worker: {limits: {memory: 1000000000GiB}}\n": "write a whole number of MiB or GiB",
		"worker: {limits: {memory: 15MiB}}\n":         "worker.limits.memory: 15MiB is below the least, 16MiB",
		"git_tools: {limits: {memory: 8MiB}}\n":       "git_tools.limits.memory: 8MiB is below the least, 16MiB",
		"worker: {limits: {tasks: 0}}\n":              "write a whole number, at least 1",
		"worker: {limits: {tasks: -3}}\n":             "write a whole number, at least 1",
		"worker: {limits: {tasks: 15}}\n":             "worker.limits.tasks: 15 is below the least, 16",
		"git_tools: {limits: {tasks: 4194305}}\n":     "git_tools.limits.tasks: 4194305 is above the most, 4194304",
		"git_tools: {limits: {tasks: 1.5}}\n":         "write a whole number, at least 1",
		"git_tools: {limits: {tasks: 0x40}}\n":        "write a whole number, at least 1",
		"git_tools: {limits: {tasks: [64]}}\n":        "write a whole number, at least 1",
		"worker: {limits: {swap: 0MiB}}\n":            "field swap not found",
		"git_tools: {limits: {memroy: 1GiB}}\n":       "field memroy not found",
		"worker: {limits: {named: true}}\n":           "field named not found",
	} {
		if _, err := load(body); err == nil || !strings.Contains(err.Error(), refusal) {
			t.Errorf("%q: %v, want %q", body, err, refusal)
		}
	}
	for _, b := range []struct {
		b    Bytes
		text string
	}{{1 << 30, "1GiB"}, {3 << 29, "1536MiB"}, {512 << 20, "512MiB"}, {1<<20 + 1, "1048577 bytes"}, {0, "0 bytes"}} {
		if b.b.String() != b.text {
			t.Errorf("%d bytes: %s, want %s", int64(b.b), b.b, b.text)
		}
	}
	// Only Linux holds them.
	named, err := load("worker: {limits: {tasks: 64}}\n")
	if err != nil {
		t.Fatal(err)
	}
	tools, err := load("git_tools: {limits: {memory: 1GiB}}\n")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		cfg     *Config
		goos    string
		refusal string
	}{{named, "linux", ""}, {tools, "linux", ""}, {c, "darwin", ""},
		{named, "darwin", "worker.limits: agentd sets resource limits on Linux only"},
		{tools, "darwin", "git_tools.limits: agentd sets resource limits on Linux only"},
		{tools, "freebsd", "freebsd's sandbox sets none"}} {
		err := c.cfg.CheckLimits(c.goos)
		if (c.refusal == "") != (err == nil) || err != nil && !strings.Contains(err.Error(), c.refusal) {
			t.Errorf("%s, %+v %+v: %v, want %q", c.goos, c.cfg.Worker.Limits, c.cfg.GitTools.Limits, err, c.refusal)
		}
	}
}

// A Worker can read all of its Python environment and its code directory: agentd refuses either
// that holds what it keeps from the Worker, by name or through a symlink. And it refuses a Runtime
// entry the platform's launcher cannot hand the Worker.
func TestCheckWorker(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"venv/bin", "code", "data", "c", "s", "launch", "outer/data", "tls"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"venv/bin/python", "launch/run-worker.sh"} {
		if err := os.WriteFile(filepath.Join(dir, f), nil, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(dir, "outer"), filepath.Join(dir, "around")); err != nil {
		t.Fatal(err)
	}
	// A home directory with a Python in it, as uv keeps its own; a venv elsewhere whose python leads
	// there; and a code directory at home with a venv inside it.
	t.Setenv("HOME", filepath.Join(dir, "home"))
	for _, d := range []string{"home/.local/python/bin", "homevenv/bin", "home/code/venv/bin", "home/code/runtime/bin", "codevenv/bin"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"home/.local/python/bin/python3.12", "home/code/venv/bin/python3.12", "home/code/runtime/bin/python3.12"} {
		if err := os.WriteFile(filepath.Join(dir, f), nil, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(dir, "home/.local/python/bin/python3.12"), filepath.Join(dir, "homevenv/bin/python")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("python3.12", filepath.Join(dir, "home/code/venv/bin/python")); err != nil {
		t.Fatal(err)
	}
	// A venv outside the home directory whose python leads into the code directory, at home.
	if err := os.Symlink(filepath.Join(dir, "home/code/runtime/bin/python3.12"), filepath.Join(dir, "codevenv/bin/python")); err != nil {
		t.Fatal(err)
	}
	load := func(body string) *Config {
		t.Helper()
		path := filepath.Join(dir, "agentd.yaml")
		if err := os.WriteFile(path, []byte("policy_file: p\nmodels_file: m\nprices_file: x\ncontrol_socket: c/control.sock\n"+body), 0o600); err != nil {
			t.Fatal(err)
		}
		c, err := Load(path)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	const sandbox = "sandbox: [launch/run-worker.sh], "
	const tcp, sock = "runtime_listen: tcp://127.0.0.1:7244\n", "runtime_listen: unix://s/runtime.sock\n"
	type check struct {
		body, goos string
		sandboxed  bool
		refusal    string // "" for none
	}
	for name, c := range map[string]check{
		"macOS":                 {"data_dir: data\n" + tcp + "worker: {" + sandbox + "python: venv, code: code}\n", "darwin", true, ""},
		"Linux":                 {"data_dir: data\n" + sock + "worker: {" + sandbox + "python: venv, code: code}\n", "linux", true, ""},
		"unsandboxed":           {"data_dir: data\n" + tcp + "worker: {python: venv, code: code}\n", "linux", false, ""},
		"code in the data dir":  {"data_dir: outer\n" + tcp + "worker: {python: venv, code: outer/data}\n", "darwin", false, ""},
		"no python":             {"data_dir: data\n" + tcp + "worker: {code: code}\n", "darwin", false, "worker.python and worker.code are required"},
		"no code":               {"data_dir: data\n" + tcp + "worker: {python: venv}\n", "darwin", false, "worker.python and worker.code are required"},
		"bad workflows":         {"data_dir: data\n" + tcp + "worker: {python: venv, code: code, workflows: \"a b:c\"}\n", "darwin", false, "is not module:attribute"},
		"code holds the data":   {"data_dir: outer/data\n" + tcp + "worker: {python: venv, code: outer}\n", "darwin", false, "holds the data directory " + dir + "/outer/data"},
		"code is the data":      {"data_dir: data\n" + tcp + "worker: {python: venv, code: data}\n", "darwin", false, "holds the data directory"},
		"code holds it, linked": {"data_dir: outer/data\n" + tcp + "worker: {python: venv, code: around}\n", "darwin", false, "(real paths: "},
		"code holds control":    {"data_dir: data\n" + tcp + "worker: {python: venv, code: c}\n", "darwin", false, "holds the Control socket's directory"},
		"code holds runtime":    {"data_dir: data\n" + sock + "worker: {" + sandbox + "python: venv, code: s}\n", "linux", true, "holds the Runtime socket's directory"},
		"python holds a mirror": {"data_dir: data\n" + tcp + "repos: {a/b: outer/data}\nworker: {python: outer, code: code}\n", "darwin", false, "holds repo a/b's mirror"},
		"code holds the key":    {"data_dir: data\n" + tcp + "temporal: {tls: {cert_file: x.crt, key_file: tls/k.key}}\nworker: {python: venv, code: tls}\n", "darwin", false, "holds the Temporal client key"},
		"code holds the cert":   {"data_dir: data\n" + tcp + "temporal: {tls: {cert_file: tls/x.crt, key_file: k.key}}\nworker: {python: venv, code: tls}\n", "darwin", false, "holds the Temporal client certificate"},
		"no bin/python":         {"data_dir: data\n" + tcp + "worker: {python: code, code: code}\n", "darwin", false, "does not run the Worker"},
		"code not there":        {"data_dir: data\n" + tcp + "worker: {python: venv, code: gone}\n", "darwin", false, "is not a directory"},
		"no launcher":           {"data_dir: data\n" + tcp + "worker: {sandbox: [launch/gone.sh], python: venv, code: code}\n", "darwin", true, "worker.sandbox"},
		"unsandboxed on a socket": {"data_dir: data\n" + sock + "worker: {python: venv, code: code}\n", "linux", false,
			"a Worker without a sandbox reaches the Runtime entry by host:port only"},
		"macOS on a socket":      {"data_dir: data\n" + sock + "worker: {" + sandbox + "python: venv, code: code}\n", "darwin", true, "the macOS launcher lets the Worker reach 127.0.0.1:<port> only"},
		"macOS on ::1":           {"data_dir: data\nruntime_listen: \"tcp://[::1]:7244\"\nworker: {" + sandbox + "python: venv, code: code}\n", "darwin", true, "127.0.0.1:<port> only"},
		"Linux on a port":        {"data_dir: data\n" + tcp + "worker: {" + sandbox + "python: venv, code: code}\n", "linux", true, "give it unix:///<path>"},
		"Linux socket with data": {"data_dir: data\nruntime_listen: unix://data/runtime.sock\nworker: {" + sandbox + "python: venv, code: code}\n", "linux", true, "holds the data directory"},
		"Linux socket with control": {"data_dir: data\nruntime_listen: unix://c/runtime.sock\nworker: {" + sandbox + "python: venv, code: code}\n", "linux", true,
			"holds the Control socket's directory"},
		"Linux socket around data": {"data_dir: outer/data\nruntime_listen: unix://outer/runtime.sock\nworker: {" + sandbox + "python: venv, code: code}\n", "linux", true,
			"holds the data directory"},
		"another platform": {"data_dir: data\n" + tcp + "worker: {" + sandbox + "python: venv, code: code}\n", "freebsd", true, "no Worker launcher for freebsd"},
		// A Python in the home directory, which the macOS sandbox cannot read (uv's): refused there only.
		"macOS, a Python at home": {"data_dir: data\n" + tcp + "worker: {" + sandbox + "python: homevenv, code: code}\n", "darwin", true,
			"in the home directory, which the macOS sandbox cannot read"},
		"macOS, a Python at home, unsandboxed":  {"data_dir: data\n" + tcp + "worker: {python: homevenv, code: code}\n", "darwin", false, ""},
		"Linux, a Python at home":               {"data_dir: data\n" + sock + "worker: {" + sandbox + "python: homevenv, code: code}\n", "linux", true, ""},
		"macOS, a Python in the code directory": {"data_dir: data\n" + tcp + "worker: {" + sandbox + "python: home/code/venv, code: home/code}\n", "darwin", true, ""},
		// Its interpreter in the code directory, which agentd's Worker runs a copy of the *.py and *.so of:
		// refused on every platform, sandboxed or not (M5 C2c).
		"macOS, a Python leading into the code directory": {"data_dir: data\n" + tcp + "worker: {" + sandbox + "python: codevenv, code: home/code}\n",
			"darwin", true, "in the code directory"},
		"Linux, a Python leading into the code directory": {"data_dir: data\n" + sock + "worker: {" + sandbox + "python: codevenv, code: home/code}\n",
			"linux", true, "in the code directory"},
		"unsandboxed, a Python leading into the code directory": {"data_dir: data\n" + tcp + "worker: {python: codevenv, code: home/code}\n",
			"darwin", false, "in the code directory"},
	} {
		err := load(c.body).CheckWorker(c.goos, c.sandboxed)
		if (c.refusal == "") != (err == nil) || err != nil && !strings.Contains(err.Error(), c.refusal) {
			t.Errorf("%s: %v, want %q", name, err, c.refusal)
		}
	}
}
