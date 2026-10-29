// Package config loads agentd.yaml. Relative paths are resolved against the directory of
// the config file; a path that starts with ~/ is in the home directory. Every path is cleaned
// (filepath.Clean): a .. drops the name before it, a symlink's too.
package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"
	"unicode"

	"go.yaml.in/yaml/v3"
)

type Config struct {
	DataDir       string          `yaml:"data_dir"`       // ledger.db and the stub external log; created 0700
	ControlSocket string          `yaml:"control_socket"` // its directory is created 0700
	RuntimeListen string          `yaml:"runtime_listen"` // unix:///path (Linux) or tcp://127.0.0.1:port (macOS)
	Temporal      Temporal        `yaml:"temporal"`
	TaskQueues    TaskQueues      `yaml:"task_queues"`
	PolicyFile    string          `yaml:"policy_file"`
	ModelsFile    string          `yaml:"models_file"`
	PricesFile    string          `yaml:"prices_file"` // versioned price table (design doc, section 9)
	Repos         map[string]Repo `yaml:"repos"`       // by logical repo id
	// GitHub is how the GitHub tools reach GitHub. Without it they only dry-run: nothing is
	// written anywhere.
	GitHub   *GitHub  `yaml:"github"`
	GitTools GitTools `yaml:"git_tools"`
	// Worker is how agentd runs the Python Worker, unless it is started with --no-worker.
	Worker Worker `yaml:"worker"`
	// How long a cancelled run may keep running before agentd terminates it: Temporal's
	// cancel is a request the workflow can ignore.
	CancelGrace time.Duration `yaml:"cancel_grace"`
	LoopLimits  LoopLimits    `yaml:"loop_limits"`

	SHA256 string `yaml:"-"` // of the file as Load read it: agentd records it at start
}

// LoopLimits cap what one run may ask of the gateway (design doc, section 9). A call's
// retries count once.
type LoopLimits struct {
	ModelCalls    int64 `yaml:"model_calls"`     // model calls per run
	GatewayCalls  int64 `yaml:"gateway_calls"`   // calls of any kind per run
	SameToolCalls int64 `yaml:"same_tool_calls"` // calls of one tool with the same arguments
	// Attempts of one call. The Worker writes the activity's retry policy, so the gateway
	// keeps its own cap.
	Attempts int64 `yaml:"attempts"`
}

// Repo is a logical repository: its read-only mirror, and its name on GitHub. In YAML it is
// either the mirror's path alone, or {mirror: ..., github: owner/name}; the GitHub name
// defaults to the logical id.
type Repo struct {
	Mirror string `yaml:"mirror"`
	GitHub string `yaml:"github"`
}

func (r *Repo) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		return n.Decode(&r.Mirror)
	}
	type plain Repo
	return n.Decode((*plain)(r))
}

type GitHub struct {
	APIURL string `yaml:"api_url"` // default https://api.github.com/
	// Token is a secret reference (keyring:<service>[/<account>]); it may be left out only
	// for a simulator on a loopback address.
	Token          string        `yaml:"token"`
	RequestTimeout time.Duration `yaml:"request_timeout"` // per request; default 30s
	// Settle is how long after a request was sent a lookup that finds nothing means it never
	// took effect: the request timeout, GitHub's own 10s limit, and a margin. Default: the
	// request timeout plus 30s.
	Settle      time.Duration `yaml:"settle"`
	AuthorName  string        `yaml:"author_name"`  // who the patch commits are by; default agentos
	AuthorEmail string        `yaml:"author_email"` // default agentos@agentos.invalid, which is nobody's
}

// GitTools is how agentd runs the read-only git MCP server: once per call, through a sandbox
// launcher.
type GitTools struct {
	Server string `yaml:"server"` // agentos-mcp-git; default: next to agentd's own binary
	// Sandbox is the launcher and its leading arguments: ../sandbox/macos/run-tool.sh or
	// ../sandbox/linux/run-tool.sh. Leaving it out needs agentd's --insecure-dev.
	Sandbox []string      `yaml:"sandbox"`
	Timeout time.Duration `yaml:"timeout"` // per call; default 60s
}

// DefaultWorkflows is what a Worker serves when the configuration does not say.
const DefaultWorkflows = "agentos_worker.workflows:WORKFLOWS"

// Worker is how agentd runs the Python Worker: through its sandbox launcher, once the Runtime
// entry serves, and again whenever it exits.
type Worker struct {
	// Sandbox is the launcher and its leading arguments: ../sandbox/macos/run-worker.sh or
	// ../sandbox/linux/run-worker.sh. Leaving it out needs agentd's --insecure-dev.
	Sandbox []string `yaml:"sandbox"`
	// Python is the Worker's Python environment, a virtual environment: <python>/bin/python runs
	// the Worker. Code is its code directory, its working directory and where it imports from. The
	// Worker can read all of both.
	Python string `yaml:"python"`
	Code   string `yaml:"code"`
	// Workflows is what the Worker serves, module:attribute (a list, or one workflow class);
	// DefaultWorkflows when left out.
	Workflows string `yaml:"workflows"`
	// DenyRead lists more paths the macOS sandbox keeps from the Worker, on top of the launcher's
	// own rules and what agentd denies of itself. Linux mounts only what the Worker is given.
	DenyRead []string `yaml:"deny_read"`
	// Limits is kept for the Worker's resource limits, which this agentd does not set yet: a
	// configuration that names any is refused rather than taken for enforced.
	Limits map[string]any `yaml:"limits"`
}

type Temporal struct {
	Address   string `yaml:"address"`
	Namespace string `yaml:"namespace"`
	TLS       *TLS   `yaml:"tls"` // mTLS to Temporal; agentd's certificate
}

type TLS struct {
	CertFile   string `yaml:"cert_file"`
	KeyFile    string `yaml:"key_file"`
	CAFile     string `yaml:"ca_file"`
	ServerName string `yaml:"server_name"`
}

type TaskQueues struct {
	Gateway   string `yaml:"gateway"`   // where agentd serves Invoke
	Workflows string `yaml:"workflows"` // where the Python Worker polls
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	sum := sha256.Sum256(b)
	c.SHA256 = hex.EncodeToString(sum[:])
	base, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	home, homeErr := os.UserHomeDir()
	if homeErr == nil && !filepath.IsAbs(home) {
		homeErr = fmt.Errorf("the home directory %q is not absolute", home)
	}
	var tildeErr error // a ~ path that names no directory
	abs := func(p string) string {
		if strings.HasPrefix(p, "~") {
			rest, ok := strings.CutPrefix(p, "~/")
			switch {
			case !ok:
				tildeErr = fmt.Errorf("%q: only ~/ is expanded", p)
			case homeErr != nil:
				tildeErr = fmt.Errorf("%q: %w", p, homeErr)
			}
			return filepath.Join(home, rest)
		}
		if p == "" {
			return p
		}
		if filepath.IsAbs(p) {
			// Clean, as filepath.Join makes the others: agentd names its files by joining onto
			// these paths, which takes a .. by name, where the kernel takes it after a symlink.
			return filepath.Clean(p)
		}
		return filepath.Join(base, p)
	}
	if c.DataDir == "" {
		c.DataDir = "~/.agentos"
	}
	c.DataDir = abs(c.DataDir)
	if c.ControlSocket == "" {
		c.ControlSocket = filepath.Join(c.DataDir, "control", "control.sock")
	}
	c.ControlSocket = abs(c.ControlSocket)
	if c.RuntimeListen == "" {
		c.RuntimeListen = "unix://" + filepath.Join(c.DataDir, "runtime", "runtime.sock")
	}
	if p, ok := strings.CutPrefix(c.RuntimeListen, "unix://"); ok {
		c.RuntimeListen = "unix://" + abs(p)
	}
	if c.Temporal.Address == "" {
		c.Temporal.Address = "127.0.0.1:7233"
	}
	if c.Temporal.Namespace == "" {
		c.Temporal.Namespace = "default"
	}
	if t := c.Temporal.TLS; t != nil {
		t.CertFile, t.KeyFile, t.CAFile = abs(t.CertFile), abs(t.KeyFile), abs(t.CAFile)
	}
	if c.TaskQueues.Gateway == "" {
		c.TaskQueues.Gateway = "gateway"
	}
	if c.TaskQueues.Workflows == "" {
		c.TaskQueues.Workflows = "agentos-workflows"
	}
	if c.CancelGrace <= 0 {
		c.CancelGrace = 2 * time.Minute
	}
	// An audit makes one model call and a handful of tool calls; these leave room for retries
	// after a pause and stop a workflow that loops.
	if c.LoopLimits.ModelCalls <= 0 {
		c.LoopLimits.ModelCalls = 20
	}
	if c.LoopLimits.GatewayCalls <= 0 {
		c.LoopLimits.GatewayCalls = 100
	}
	if c.LoopLimits.SameToolCalls <= 0 {
		c.LoopLimits.SameToolCalls = 3
	}
	if c.LoopLimits.Attempts <= 0 {
		c.LoopLimits.Attempts = 10
	}
	c.PolicyFile, c.ModelsFile, c.PricesFile = abs(c.PolicyFile), abs(c.ModelsFile), abs(c.PricesFile)
	if c.PolicyFile == "" || c.ModelsFile == "" || c.PricesFile == "" {
		return nil, fmt.Errorf("config %s: policy_file, models_file and prices_file are required", path)
	}
	for id, r := range c.Repos {
		if r.Mirror == "" {
			return nil, fmt.Errorf("config %s: repo %s: mirror is required", path, id)
		}
		r.Mirror = abs(r.Mirror)
		if r.GitHub == "" {
			r.GitHub = id
		}
		if owner, name, ok := strings.Cut(r.GitHub, "/"); !ok || owner == "" || name == "" || strings.Contains(name, "/") {
			return nil, fmt.Errorf("config %s: repo %s: github must be owner/name, not %q", path, id, r.GitHub)
		}
		c.Repos[id] = r
	}
	if c.GitTools.Server != "" {
		c.GitTools.Server = abs(c.GitTools.Server)
	}
	if len(c.GitTools.Sandbox) > 0 {
		c.GitTools.Sandbox[0] = abs(c.GitTools.Sandbox[0])
	}
	w := &c.Worker
	if len(w.Sandbox) > 0 {
		w.Sandbox[0] = abs(w.Sandbox[0])
	}
	w.Python, w.Code = abs(w.Python), abs(w.Code)
	for i, p := range w.DenyRead {
		w.DenyRead[i] = abs(p)
	}
	if w.Workflows == "" {
		w.Workflows = DefaultWorkflows
	}
	if len(w.Limits) > 0 {
		return nil, fmt.Errorf("config %s: worker.limits: this agentd sets no limits on the Worker yet; leave it out", path)
	}
	if tildeErr != nil {
		return nil, fmt.Errorf("config %s: %w", path, tildeErr)
	}
	if err := c.checkMirrors(); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	if c.GitTools.Timeout <= 0 {
		c.GitTools.Timeout = time.Minute
	}
	if g := c.GitHub; g != nil {
		if g.APIURL == "" {
			g.APIURL = "https://api.github.com/"
		}
		if g.RequestTimeout <= 0 {
			g.RequestTimeout = 30 * time.Second
		}
		if g.Settle <= 0 {
			g.Settle = g.RequestTimeout + 30*time.Second
		}
		if g.AuthorName == "" {
			g.AuthorName = "agentos"
		}
		if g.AuthorEmail == "" {
			g.AuthorEmail = "agentos@agentos.invalid"
		}
		if g.Token == "" && !Loopback(g.APIURL) {
			return nil, fmt.Errorf("config %s: github.token is required for %s", path, g.APIURL)
		}
		// A lookup that finds nothing proves a write never took effect only once a request
		// still in flight could no longer: past the client's timeout and GitHub's own 10s
		// limit. A simulator on this machine applies a write as it receives it.
		least := g.RequestTimeout + 10*time.Second
		if Loopback(g.APIURL) {
			least = g.RequestTimeout
		}
		if g.Settle < least {
			return nil, fmt.Errorf("config %s: github.settle must be at least %s", path, least)
		}
	}
	return &c, nil
}

// checkMirrors refuses a mirror that holds anything else the git tool server must not read,
// whatever name the config gives either: agentd's data directory, its sockets' directories, the
// Temporal client certificate and key (what agentd's toolDenyRead denies), or another repo's
// mirror. On Linux the server's launcher mounts all of the call's mirror, and a read-only mount
// stops neither reads nor connect(); the server reads no keys, ledger or other mirrors (design
// doc, section 8). A mirror may be inside one of them: the dev configs keep theirs in the data
// directory.
func (c *Config) checkMirrors() error {
	paths := c.private()
	for _, p := range paths {
		if p.err != nil && len(c.Repos) > 0 {
			return fmt.Errorf("cannot tell where %s %s leads, to check it against the mirrors: %w", p.what, p.path, p.err)
		}
	}
	for i, m := range paths {
		if !m.mirror {
			continue
		}
		// The Linux launcher takes the mirror's real path in shell, which drops a trailing newline:
		// it would mount another directory than the one checked here.
		if strings.ContainsFunc(m.real, unicode.IsControl) {
			return fmt.Errorf("repo %s: the mirror %q has a control character in its real path %q", m.repo, m.path, m.real)
		}
		for j, p := range paths {
			if i != j && m.holds(p.place) {
				via := ""
				if !within(p.path, m.path) {
					via = fmt.Sprintf(" (real paths: %s, %s)", m.real, p.real)
				}
				return fmt.Errorf("repo %s: the mirror %s holds %s %s%s, and the git tool server may read all of its mirror",
					m.repo, m.path, p.what, p.path, via)
			}
		}
	}
	return nil
}

// named is one of the paths agentd keeps from the sandboxes, and what it is.
type named struct {
	what   string
	mirror bool   // a repo's mirror
	repo   string // whose mirror it is
	place
}

// private is what the git tool server and the Worker must not read, located: agentd's data
// directory, its sockets' directories, the Temporal client certificate and key (what agentd's
// toolDenyRead denies), and every repo's mirror.
func (c *Config) private() []named {
	paths := []named{{what: "the data directory", place: locate(c.DataDir)},
		{what: "the Control socket's directory", place: locate(filepath.Dir(c.ControlSocket))}}
	if p, ok := strings.CutPrefix(c.RuntimeListen, "unix://"); ok {
		paths = append(paths, named{what: "the Runtime socket's directory", place: locate(filepath.Dir(p))})
	}
	if t := c.Temporal.TLS; t != nil {
		paths = append(paths, named{what: "the Temporal client certificate", place: locate(t.CertFile)},
			named{what: "the Temporal client key", place: locate(t.KeyFile)})
	}
	for _, id := range slices.Sorted(maps.Keys(c.Repos)) {
		paths = append(paths, named{what: "repo " + id + "'s mirror", mirror: true, repo: id, place: locate(c.Repos[id].Mirror)})
	}
	return paths
}

// interpreterOutsideCode refuses an environment whose bin/python leads into the code directory, but
// for the environment's own: agentd runs a copy of the code directory, with its *.py and *.so files
// only, so the copy's bin/python would lead to the live interpreter, which the sandbox no longer opens.
func interpreterOutsideCode(w Worker) error {
	python := filepath.Join(w.Python, "bin", "python")
	real, err := filepath.EvalSymlinks(python)
	if err != nil {
		return fmt.Errorf("worker.python: %s: %w", python, err)
	}
	in := func(dir string) bool {
		r, err := filepath.EvalSymlinks(dir)
		return within(real, dir) || err == nil && within(real, r)
	}
	if in(w.Code) && !in(w.Python) {
		return fmt.Errorf("worker.python: %s leads to %s, in the code directory, of which agentd's Worker runs a copy "+
			"of the *.py and *.so files only: give the Worker a Python environment whose interpreter is elsewhere", python, real)
	}
	return nil
}

// outsideHome refuses, for the macOS sandbox, a Python environment whose bin/python leads into the
// home directory outside the Python environment and the code directory: the sandbox cannot read it,
// and the Worker would fail only once its launcher ran. uv's own Pythons live there.
func outsideHome(w Worker) error {
	python := filepath.Join(w.Python, "bin", "python")
	real, err := filepath.EvalSymlinks(python)
	if err != nil {
		return fmt.Errorf("worker.python: %s: %w", python, err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil // the launcher denies $HOME: nothing to compare with here
	}
	readable := func(p string) bool {
		for _, d := range []string{w.Python, w.Code} {
			if within(p, d) {
				return true
			}
			if r, err := filepath.EvalSymlinks(d); err == nil && within(p, r) {
				return true
			}
		}
		return false
	}
	realHome := home
	if r, err := filepath.EvalSymlinks(home); err == nil {
		realHome = r
	}
	if (within(real, home) || within(real, realHome)) && !readable(real) {
		return fmt.Errorf("worker.python: %s leads to %s, in the home directory, which the macOS sandbox cannot read: "+
			"give the Worker a Python environment made from a Python outside it (python.org's or Homebrew's; "+
			"uv's own Pythons are in the home directory)", python, real)
	}
	return nil
}

// workflowsSpec is module:attribute, as the Worker's --workflows takes it.
var workflowsSpec = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*$`)

// CheckWorker refuses a Worker agentd cannot run as the configuration says, on the platform goos,
// sandboxed (worker.sandbox is set) or not; agentd checks it unless it runs no Worker. The Worker can
// read all of its Python environment and code directory: neither may hold anything agentd keeps
// from it (private), by whatever name. And its launcher reaches the Runtime entry only one way: on
// macOS through a loopback port on 127.0.0.1, on Linux through the socket, whose directory it mounts.
func (c *Config) CheckWorker(goos string, sandboxed bool) error {
	w := c.Worker
	if w.Python == "" || w.Code == "" {
		return errors.New("worker.python and worker.code are required (or start agentd with --no-worker and run a Worker yourself)")
	}
	if !workflowsSpec.MatchString(w.Workflows) {
		return fmt.Errorf("worker.workflows: %q is not module:attribute", w.Workflows)
	}
	held := c.private()
	for _, d := range []struct{ what, path string }{{"worker.python", w.Python}, {"worker.code", w.Code}} {
		dir := locate(d.path)
		if dir.err != nil {
			return fmt.Errorf("%s: cannot tell where %s leads: %w", d.what, d.path, dir.err)
		}
		for _, p := range held {
			if p.err != nil {
				return fmt.Errorf("cannot tell where %s %s leads, to check it against %s: %w", p.what, p.path, d.what, p.err)
			}
			if dir.holds(p.place) {
				via := ""
				if !within(p.path, dir.path) {
					via = fmt.Sprintf(" (real paths: %s, %s)", dir.real, p.real)
				}
				return fmt.Errorf("%s %s holds %s %s%s, and the Worker may read all of it", d.what, d.path, p.what, p.path, via)
			}
		}
	}
	if fi, err := os.Stat(filepath.Join(w.Python, "bin", "python")); err != nil || fi.IsDir() {
		return fmt.Errorf("worker.python: %s/bin/python does not run the Worker: %v", w.Python, err)
	}
	if err := interpreterOutsideCode(w); err != nil {
		return err
	}
	if sandboxed && goos == "darwin" {
		if err := outsideHome(w); err != nil {
			return err
		}
	}
	if fi, err := os.Stat(w.Code); err != nil || !fi.IsDir() {
		return fmt.Errorf("worker.code: %s is not a directory: %v", w.Code, err)
	}
	if sandboxed {
		if _, err := os.Stat(w.Sandbox[0]); err != nil {
			return fmt.Errorf("worker.sandbox: %w", err)
		}
	}
	socket, unixListen := strings.CutPrefix(c.RuntimeListen, "unix://")
	hostport, tcpListen := strings.CutPrefix(c.RuntimeListen, "tcp://")
	host, _, _ := net.SplitHostPort(hostport)
	switch {
	case !sandboxed && !tcpListen:
		return fmt.Errorf("runtime_listen %s: a Worker without a sandbox reaches the Runtime entry by host:port only: "+
			"give it tcp://127.0.0.1:<port>", c.RuntimeListen)
	case !sandboxed:
	case goos == "darwin" && (!tcpListen || host != "127.0.0.1"):
		return fmt.Errorf("runtime_listen %s: the macOS launcher lets the Worker reach 127.0.0.1:<port> only: "+
			"give it tcp://127.0.0.1:<port>", c.RuntimeListen)
	case goos == "linux" && !unixListen:
		return fmt.Errorf("runtime_listen %s: the Linux sandbox has no network, and its launcher forwards to a socket: "+
			"give it unix:///<path> (or start agentd with --no-worker and run a Worker yourself)", c.RuntimeListen)
	case goos == "linux":
		// The launcher mounts the socket's directory into the sandbox, all of it.
		dir := locate(filepath.Dir(socket))
		for _, p := range held[:2] { // the data directory and the Control socket's
			if dir.holds(p.place) {
				return fmt.Errorf("runtime_listen %s: the Linux launcher mounts the socket's directory into the Worker's "+
					"sandbox, and it holds %s %s: give the socket a directory of its own", c.RuntimeListen, p.what, p.path)
			}
		}
	case goos != "darwin":
		return fmt.Errorf("worker.sandbox: agentd has no Worker launcher for %s", goos)
	}
	return nil
}

// place is a path from the config, and where it is on the file system: each path is looked up
// once, however many mirrors there are. Only an absolute path is looked up; another (an empty
// one) names nothing, and holds nothing.
type place struct {
	path string
	real string        // path, its symlinks resolved as far as it exists
	err  error         // why real could not be told
	info os.FileInfo   // path itself, if it exists
	dirs []os.FileInfo // real and each directory above it, as far as they exist
}

// locate looks path up on the file system.
func locate(path string) place {
	p := place{path: path}
	if !filepath.IsAbs(path) {
		return p
	}
	if p.real, p.err = resolve(path); p.err != nil {
		return p
	}
	if fi, err := os.Stat(path); err == nil {
		p.info = fi
	}
	for d := p.real; ; d = filepath.Dir(d) {
		if fi, err := os.Stat(d); err == nil {
			p.dirs = append(p.dirs, fi)
		}
		if d == filepath.Dir(d) {
			return p
		}
	}
}

// holds reports whether the directory d holds p, or is it: by their names, by their real paths,
// or because one of the directories p is in is d by another name, which only the file system
// knows (on macOS a firmlink or another case; on Linux d may be a bind mount of one of them).
// A mount inside d, or p mounted from inside d, goes unseen.
func (d place) holds(p place) bool {
	return within(p.path, d.path) || within(p.real, d.real) ||
		d.info != nil && slices.ContainsFunc(p.dirs, func(fi os.FileInfo) bool { return os.SameFile(fi, d.info) })
}

// resolve is path, an absolute one, with its symlinks resolved a name at a time as the kernel
// does, as far as the names exist: what does not exist yet will be made under the names given,
// where the part that does leads, and a symlink to nothing leads where it points. Where it
// cannot tell (a name it cannot look at, a real path longer than the system takes), it fails.
func resolve(path string) (string, error) {
	sep := string(filepath.Separator)
	dest, names := sep, strings.Split(path, sep)
	for links := 0; len(names) > 0; {
		name := names[0]
		names = names[1:]
		switch name {
		case "", ".":
			continue
		case "..":
			dest = filepath.Dir(dest) // dest holds no symlink: its parent is the one by name
			continue
		}
		next := filepath.Join(dest, name)
		target, err := os.Readlink(next)
		if err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, syscall.ENOTDIR) {
			return "", err
		}
		if err != nil || links == 40 { // not a symlink, or not there; Linux follows 40 at most
			dest = next
			continue
		}
		links++
		if filepath.IsAbs(target) {
			dest = sep
		}
		names = append(strings.Split(target, sep), names...)
	}
	return dest, nil
}

// within reports whether path is dir or inside it, by their names.
func within(path, dir string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// Mirrors maps each logical repo id to its read-only mirror.
func (c *Config) Mirrors() map[string]string {
	out := make(map[string]string, len(c.Repos))
	for id, r := range c.Repos {
		out[id] = r.Mirror
	}
	return out
}

// Loopback reports whether a URL points at this machine: a simulator, or a local model server.
func Loopback(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	ip := net.ParseIP(u.Hostname())
	return ip != nil && ip.IsLoopback()
}
