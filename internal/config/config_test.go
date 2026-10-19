package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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
	// Until approvals arrive (M4), a real model and real GitHub writes without approval never
	// meet: injected text in a diff could open a pull request. This config only dry-runs.
	if c.GitHub != nil {
		t.Error("agentd-claude.yaml configures GitHub: with the dev policy, nothing would ask before writing")
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
