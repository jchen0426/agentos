// Package config loads agentd.yaml. Relative paths are resolved against the directory of
// the config file.
package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

type Config struct {
	DataDir       string            `yaml:"data_dir"`       // ledger.db and the stub external log; created 0700
	ControlSocket string            `yaml:"control_socket"` // its directory is created 0700
	RuntimeListen string            `yaml:"runtime_listen"` // unix:///path (Linux) or tcp://127.0.0.1:port (macOS)
	Temporal      Temporal          `yaml:"temporal"`
	TaskQueues    TaskQueues        `yaml:"task_queues"`
	PolicyFile    string            `yaml:"policy_file"`
	ModelsFile    string            `yaml:"models_file"`
	PricesFile    string            `yaml:"prices_file"` // versioned price table (design doc, section 9)
	Repos         map[string]string `yaml:"repos"`       // logical repo id -> read-only mirror
	// How long a cancelled run may keep running before agentd terminates it: Temporal's
	// cancel is a request the workflow can ignore.
	CancelGrace time.Duration `yaml:"cancel_grace"`
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
	abs := func(p string) string {
		if p == "" || filepath.IsAbs(p) {
			return p
		}
		return filepath.Join(base, p)
	}
	if c.DataDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		c.DataDir = filepath.Join(home, ".agentos")
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
	c.PolicyFile, c.ModelsFile, c.PricesFile = abs(c.PolicyFile), abs(c.ModelsFile), abs(c.PricesFile)
	if c.PolicyFile == "" || c.ModelsFile == "" || c.PricesFile == "" {
		return nil, fmt.Errorf("config %s: policy_file, models_file and prices_file are required", path)
	}
	for id, mirror := range c.Repos {
		c.Repos[id] = abs(mirror)
	}
	return &c, nil
}
