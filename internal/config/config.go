// Package config loads agentd.yaml. Relative paths are resolved against the directory of
// the config file; a path that starts with ~/ is in the home directory.
package config

import (
	"bytes"
	"fmt"
	"maps"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

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
	// How long a cancelled run may keep running before agentd terminates it: Temporal's
	// cancel is a request the workflow can ignore.
	CancelGrace time.Duration `yaml:"cancel_grace"`
	LoopLimits  LoopLimits    `yaml:"loop_limits"`
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
		if p == "" || filepath.IsAbs(p) {
			return p
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
	type named struct{ repo, what, path string } // repo: whose mirror it is
	paths := []named{{"", "the data directory", c.DataDir}, {"", "the Control socket's directory", filepath.Dir(c.ControlSocket)}}
	if p, ok := strings.CutPrefix(c.RuntimeListen, "unix://"); ok {
		paths = append(paths, named{"", "the Runtime socket's directory", filepath.Dir(p)})
	}
	if t := c.Temporal.TLS; t != nil {
		paths = append(paths, named{"", "the Temporal client certificate", t.CertFile}, named{"", "the Temporal client key", t.KeyFile})
	}
	ids := slices.Sorted(maps.Keys(c.Repos))
	for _, id := range ids {
		paths = append(paths, named{id, "repo " + id + "'s mirror", c.Repos[id].Mirror})
	}
	for _, id := range ids {
		mirror := c.Repos[id].Mirror
		for _, p := range paths {
			if p.repo != id && p.path != "" && holds(mirror, p.path) {
				return fmt.Errorf("repo %s: the mirror %s holds %s %s, and the git tool server may read all of its mirror",
					id, mirror, p.what, p.path)
			}
		}
	}
	return nil
}

// holds reports whether the directory dir holds path, or is it: by their names, by their real
// paths, or because one of the directories path is in is dir by another name, which only the
// file system knows (on macOS a firmlink or another case, on Linux a bind mount of dir).
func holds(dir, path string) bool {
	real := resolve(path)
	if within(path, dir) || within(real, resolve(dir)) {
		return true
	}
	d, err := os.Stat(dir)
	if err != nil {
		return false
	}
	for p := real; ; p = filepath.Dir(p) {
		if fi, err := os.Stat(p); err == nil && os.SameFile(fi, d) {
			return true
		}
		if p == filepath.Dir(p) {
			return false
		}
	}
}

// resolve is path with its symlinks resolved as far as it exists: what does not exist yet will
// be made where the part that does leads.
func resolve(path string) string {
	if real, err := filepath.EvalSymlinks(path); err == nil {
		return real
	}
	parent := filepath.Dir(path)
	if parent == path {
		return path
	}
	return filepath.Join(resolve(parent), filepath.Base(path))
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
