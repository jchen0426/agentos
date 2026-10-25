package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/zalando/go-keyring"

	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/config"
	"github.com/jchen0426/agentos/internal/models"
	"github.com/jchen0426/agentos/internal/runtimeproxy"
	"github.com/jchen0426/agentos/internal/secrets"
)

// The git tool server's launcher gets everything of agentd's it must keep out of reach.
func TestToolDenyRead(t *testing.T) {
	cfg := &config.Config{
		DataDir:       "/srv/agentos/data",
		ControlSocket: "/run/agentos/control/control.sock",
		RuntimeListen: "unix:///run/agentos/runtime/runtime.sock",
		Temporal:      config.Temporal{TLS: &config.TLS{CertFile: "/etc/agentos/agentd.crt", KeyFile: "/etc/agentos/agentd.key"}},
		Repos: map[string]config.Repo{
			"acme/payments": {Mirror: "/srv/mirrors/payments"},
			"acme/ledger":   {Mirror: "/srv/mirrors/acme:ledger"},
		},
	}
	got := toolDenyRead(cfg)
	want := []string{"/srv/agentos/data", "/run/agentos/control", "/run/agentos/runtime", "/etc/agentos/agentd.crt",
		"/etc/agentos/agentd.key", "/srv/mirrors/acme:ledger", "/srv/mirrors/payments"}
	if !slices.Equal(got, want) {
		t.Errorf("toolDenyRead:\n got %q\nwant %q", got, want)
	}
	// A Runtime endpoint on TCP has no file to deny; the sandbox has no network.
	cfg.RuntimeListen = "tcp://127.0.0.1:7233"
	if slices.Contains(toolDenyRead(cfg), "127.0.0.1:7233") || len(toolDenyRead(cfg)) != len(want)-1 {
		t.Errorf("a TCP Runtime endpoint: %q", toolDenyRead(cfg))
	}
}

// On Linux the launcher mounts all of the call's mirror: whatever else toolDenyRead keeps from the
// git tool server, a config that makes it a repo's mirror does not load.
func TestNoMirrorIsWhatToolsMustNotRead(t *testing.T) {
	dir := t.TempDir()
	repos := map[string]string{"acme/payments": filepath.Join(dir, "payments"), "acme/ledger": filepath.Join(dir, "ledger")}
	load := func() (*config.Config, error) {
		t.Helper()
		b, err := json.Marshal(map[string]any{ // JSON is YAML
			"data_dir":       filepath.Join(dir, "data"),
			"control_socket": filepath.Join(dir, "control", "control.sock"),
			"runtime_listen": "unix://" + filepath.Join(dir, "runtime", "runtime.sock"),
			"temporal": map[string]any{"tls": map[string]string{
				"cert_file": filepath.Join(dir, "tls", "agentd.crt"), "key_file": filepath.Join(dir, "tls", "agentd.key")}},
			"policy_file": "p", "models_file": "m", "prices_file": "x",
			"repos": repos,
		})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "agentd.yaml")
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return config.Load(path)
	}
	cfg, err := load()
	if err != nil {
		t.Fatal(err)
	}
	own := cfg.Repos["acme/payments"].Mirror
	for _, p := range toolDenyRead(cfg) {
		if p == own {
			continue
		}
		repos["acme/payments"] = p
		if _, err := load(); err == nil {
			t.Errorf("acme/payments with the mirror %s: loaded", p)
		}
	}
}

// agentd makes its files where Load looked. The kernel takes a .. after the symlink before it,
// while Load and filepath.Join take it by name: so Load cleans the paths agentd uses.
func TestAgentdMakesNothingInAMirror(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "aos-") // short: a socket's path is at most 103 bytes
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	mirror := filepath.Join(dir, "mirror")
	if err := os.MkdirAll(filepath.Join(mirror, "refs"), 0o700); err != nil {
		t.Fatal(err)
	}
	// in leads into the mirror: to the kernel, in/.. is the mirror; by name, it is dir.
	if err := os.Symlink("mirror/refs", filepath.Join(dir, "in")); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(map[string]any{ // JSON is YAML
		"data_dir":       dir + "/in/../data",
		"control_socket": dir + "/in/../control.sock",
		"runtime_listen": "unix://" + dir + "/in/../runtime.sock",
		"policy_file":    "p", "models_file": "m", "prices_file": "x",
		"repos": map[string]string{"acme/payments": mirror},
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "agentd.yaml")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := mkdirPrivate(cfg.DataDir); err != nil {
		t.Fatal(err)
	}
	for _, addr := range []string{"unix://" + cfg.ControlSocket, cfg.RuntimeListen} {
		ln, err := listen(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
	}
	entries, err := os.ReadDir(mirror)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "refs" {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("agentd made files in the mirror: %q", names)
	}
	// The ledger goes into the data directory agentd made.
	lock, err := lockDataDir(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	lock.Close()
}

// A provider elsewhere is reached only over https; a key for a Chat Completions server is read
// only when models.yaml names it.
func TestModelAdapterKeys(t *testing.T) {
	load := func(body string) *models.Config {
		t.Helper()
		path := filepath.Join(t.TempDir(), "models.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		m, err := models.Load(path)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	task := "tasks: {audit_v1: {model: local/m, max_tokens: 10, inputs: [diff_ref]}}\n"
	for name, c := range map[string]struct {
		provider, wantErr string
	}{
		"key over http":       {`{adapter: openai_compat, api: chat, base_url: "http://10.0.0.5:8000/v1", context_tokens: 4096, key: "keyring:k"}`, "only over https"},
		"no key, http vLLM":   {`{adapter: openai_compat, api: chat, base_url: "http://10.0.0.5:8000/v1", context_tokens: 4096}`, "only over https"},
		"no key, https vLLM":  {`{adapter: openai_compat, api: chat, base_url: "https://gpu.lan:8000/v1", context_tokens: 4096}`, ""},
		"no key, Ollama here": {`{adapter: openai_compat, api: responses, base_url: "http://127.0.0.1:11434/v1"}`, ""},
		"Claude over http":    {`{adapter: anthropic, base_url: "http://proxy.lan:8080", key: "keyring:k"}`, "only over https"},
	} {
		m := load("providers:\n  local: " + c.provider + "\n" + task)
		_, err := modelAdapters(m, nil, t.TempDir(), slog.New(slog.DiscardHandler))
		if (c.wantErr == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), c.wantErr)) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// A key exactly as long as security's prompt keeps was likely cut there: agentd says so at
// start, without showing the key.
func TestCutKeyWarning(t *testing.T) {
	keyring.MockInit() // an in-memory keyring for this process
	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "models.yaml")
	body := `providers:
  openai: {adapter: openai_compat, api: responses, base_url: "https://api.openai.com/v1", key: "keyring:agentos-test-cut"}
tasks: {audit_v1: {model: openai/m, max_tokens: 10, inputs: [diff_ref]}}
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := models.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	for length, warns := range map[int]bool{secrets.PromptLimit: true, secrets.PromptLimit - 1: false, 164: false} {
		key := "sk-proj-" + strings.Repeat("k", length-len("sk-proj-"))
		if err := keyring.Set("agentos-test-cut", me.Username, key); err != nil {
			t.Fatal(err)
		}
		var logs bytes.Buffer
		if _, err := modelAdapters(m, nil, t.TempDir(), slog.New(slog.NewTextHandler(&logs, nil))); err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(logs.String(), "security's prompt"); got != warns {
			t.Errorf("a %d-character key: warned %v, want %v: %s", length, got, warns, logs.String())
		}
		if strings.Contains(logs.String(), key) {
			t.Errorf("a %d-character key: the log shows it", length)
		}
	}
}

// The safety identifier is random, made once and kept.
func TestSafetyIdentifier(t *testing.T) {
	dir := t.TempDir()
	first, err := safetyIdentifier(dir)
	if err != nil || !strings.HasPrefix(first, "agentos-") || len(first) < 30 {
		t.Fatalf("safetyIdentifier = %q, %v", first, err)
	}
	if again, _ := safetyIdentifier(dir); again != first {
		t.Errorf("not kept: %q, then %q", first, again)
	}
	if other, _ := safetyIdentifier(t.TempDir()); other == first {
		t.Error("two data directories, one identifier")
	}
	if info, err := os.Stat(filepath.Join(dir, "safety-identifier")); err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("kept as %v, %v", info.Mode(), err)
	}
}

// A refusal's record says what the refusal log says: the method, the command and the reason of a
// refusal logged in full, and how many were refused within how long for a count.
func TestARefusalsRecord(t *testing.T) {
	e := refusalRecord(runtimeproxy.Refused{Method: "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted",
		Command: "COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION", Reason: "a Worker signals no workflow", Count: 1})
	p := e.Payload.(map[string]any)
	if e.Type != "runtime.refused" || e.Actor != audit.Agentd || e.WorkflowID != "" || p["count"] != int64(1) ||
		p["method"] != "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted" ||
		p["command"] != "COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION" || p["reason"] != "a Worker signals no workflow" ||
		p["within"] != nil {
		t.Fatalf("record %+v", e)
	}
	p = refusalRecord(runtimeproxy.Refused{Count: 40, Within: time.Minute}).Payload.(map[string]any)
	if p["count"] != int64(40) || p["within"] != "1m0s" || p["method"] != nil || p["command"] != nil || p["reason"] != nil {
		t.Fatalf("record %v", p)
	}
}

// An agentd's identity names its data directory as the file system resolves it: the same directory
// reached through a symbolic link, or a relative path, is the same agentd; another directory, or
// another task queue, is another.
func TestTheIdentityNamesTheDataDirectoryItself(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "data")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(dir, "other")
	if err := os.Mkdir(other, 0o700); err != nil {
		t.Fatal(err)
	}
	id := func(queue, path string) string {
		t.Helper()
		s, err := agentdIdentity(queue, path)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	t.Chdir(dir)
	same := id("agentos-workflows", real)
	if id("agentos-workflows", link) != same || id("agentos-workflows", "data") != same || id("agentos-workflows", "link/") != same {
		t.Fatal("the same directory, spelled otherwise, is another agentd")
	}
	if id("agentos-workflows", other) == same || id("other-workflows", real) == same {
		t.Fatal("another directory or task queue is the same agentd")
	}
	if _, err := agentdIdentity("q", filepath.Join(dir, "missing")); err == nil {
		t.Fatal("an identity for a data directory that is not there")
	}
}
