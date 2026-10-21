// agentd is the Agent OS daemon. It holds the keys and the ledgers and is the only way out:
// it serves the gateway activity, the Control socket for agentctl, and the Runtime socket
// through which the sandboxed Worker reaches Temporal.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/budget"
	"github.com/jchen0426/agentos/internal/config"
	"github.com/jchen0426/agentos/internal/control"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/gateway"
	"github.com/jchen0426/agentos/internal/githubtool"
	"github.com/jchen0426/agentos/internal/gitmcp"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/models"
	"github.com/jchen0426/agentos/internal/outbound"
	"github.com/jchen0426/agentos/internal/outbox"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/pricing"
	"github.com/jchen0426/agentos/internal/runtimeproxy"
	"github.com/jchen0426/agentos/internal/safetext"
	"github.com/jchen0426/agentos/internal/secrets"
	"github.com/jchen0426/agentos/internal/tasks"
	"github.com/jchen0426/agentos/internal/tools"
)

// drainTimeout bounds how long a shutdown waits for gateway work in flight.
const drainTimeout = 30 * time.Second

func main() {
	configPath := flag.String("config", "agentd.yaml", "path to agentd.yaml")
	faultScript := flag.String("fault-script", "", "TESTS ONLY: fault injection script for the tools, the fake model and run starts")
	testTools := flag.Bool("test-tools", false, "TESTS ONLY: also offer test.unsafe_write, a write to a local log")
	insecureDev := flag.Bool("insecure-dev", false, "DEVELOPMENT ONLY: run the git tool server without a sandbox when git_tools.sandbox is unset")
	logLevel := flag.String("log-level", "info", "debug, info, warn or error")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintln(os.Stderr, "agentd:", err)
		os.Exit(2)
	}
	// The log carries values the Worker chose (activity IDs, call names): no raw controls.
	log := slog.New(slog.NewJSONHandler(safetext.JSONWriter(os.Stderr), &slog.HandlerOptions{Level: level}))
	if err := run(*configPath, *faultScript, *testTools, *insecureDev, log); err != nil {
		log.Error("agentd stopped", "error", err)
		os.Exit(1)
	}
}

func run(configPath, faultScript string, testTools, insecureDev bool, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Before any client is made: where agentd's requests go is its configuration's to say.
	set, err := outbound.IgnoreEnvProxies()
	if err != nil {
		return err
	}
	if len(set) > 0 {
		log.Warn("agentd ignores the proxy settings in its environment: its requests go straight to their hosts, "+
			"and fail where only a proxy reaches them", "variables", set)
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if err := mkdirPrivate(cfg.DataDir); err != nil {
		return err
	}
	lock, err := lockDataDir(cfg.DataDir)
	if err != nil {
		return err
	}
	defer lock.Close()
	pol, err := policy.Load(cfg.PolicyFile)
	if err != nil {
		return err
	}
	mods, err := models.Load(cfg.ModelsFile)
	if err != nil {
		return err
	}
	prices, err := pricing.Load(cfg.PricesFile)
	if err != nil {
		return err
	}
	for name := range mods.Tasks {
		route, err := mods.Route(name)
		if err != nil {
			return err
		}
		for _, hop := range append([]models.Route{route}, route.Next...) {
			// Every model a task may reach must be priced before agentd will start.
			if _, err := prices.Bound(hop.Candidates, 0, hop.MaxTokens); err != nil {
				return fmt.Errorf("model task %s: %w", name, err)
			}
			if mods.Providers[hop.Provider].API != "chat" {
				continue
			}
			// Chat Completions servers count no tokens before a request: its reservation is
			// an estimate, and a bound only when nothing is charged (section 9).
			if free, err := prices.Free(hop.Name()); err != nil || !free {
				return fmt.Errorf("model task %s: %s takes api chat, whose input is only estimated: "+
					"its prices must be 0 (a model server of your own)", name, hop.Name())
			}
		}
	}
	var script *faults.Script
	if faultScript != "" {
		if script, err = faults.Load(faultScript); err != nil {
			return err
		}
		log.Warn("FAULT INJECTION ENABLED: never run this outside tests", "script", faultScript)
	}
	led, err := ledger.Open(ctx, filepath.Join(cfg.DataDir, "ledger.db"))
	if err != nil {
		return err
	}
	defer led.Close()
	bud := &budget.Budget{Ledger: led}
	// Before any request is served: a reservation still open belongs to a request agentd
	// lost track of when it stopped, and the provider may have billed it.
	if n, err := bud.SettleOpen(ctx); err != nil {
		return fmt.Errorf("settle open reservations: %w", err)
	} else if n > 0 {
		log.Warn("charged reservations left open by the last run of agentd in full", "count", n)
	}

	tlsConfig, err := temporalTLS(cfg.Temporal.TLS)
	if err != nil {
		return err
	}
	tc, err := client.DialContext(ctx, client.Options{
		HostPort:          cfg.Temporal.Address,
		Namespace:         cfg.Temporal.Namespace,
		Logger:            tlog.NewStructuredLogger(log.With("component", "temporal-sdk")),
		ConnectionOptions: client.ConnectionOptions{TLS: tlsConfig, DialOptions: []grpc.DialOption{grpc.WithNoProxy()}},
	})
	if err != nil {
		return fmt.Errorf("connect to Temporal at %s: %w", cfg.Temporal.Address, err)
	}
	defer tc.Close()

	adapters, err := modelAdapters(mods, led, cfg.DataDir)
	if err != nil {
		return err
	}
	for name := range mods.Tasks {
		route, err := mods.Route(name)
		if err != nil {
			return err
		}
		for _, hop := range append([]models.Route{route}, route.Next...) {
			if c, ok := adapters[hop.Provider].(models.RouteChecker); ok {
				if err := c.CheckRoute(hop); err != nil {
					return fmt.Errorf("model task %s: %s: %w", name, hop.Name(), err)
				}
			}
		}
	}
	git, err := gitServer(cfg, insecureDev, log)
	if err != nil {
		return err
	}
	// Real calls through the launcher before any run: a sandbox that cannot work stops agentd
	// here, instead of failing every run's first tool call.
	if err := git.SelfCheck(ctx); err != nil {
		return fmt.Errorf("the git tool server does not work: %w", err)
	}
	// A run's commit is resolved through the server too: agentd itself runs no git on a mirror.
	// Starts take slots of their own, so that slow tool calls of running runs never hold up
	// another run's start.
	registry := tasks.Registry{"security-audit": tasks.SecurityAudit(pol, cfg.Mirrors(), git.WithOwnSlots(2))}
	toolset, err := buildTools(cfg, pol, led, git, testTools, log)
	if err != nil {
		return err
	}
	if err := gateway.CheckTools(pol, toolset); err != nil {
		return err
	}
	if err := gateway.CheckModels(pol, mods); err != nil {
		return err
	}
	gw := &gateway.Gateway{
		Ledger:   led,
		Policy:   pol,
		Tasks:    registry,
		Models:   mods,
		Prices:   prices,
		Budget:   bud,
		Adapters: adapters,
		Limiters: limiters(mods),
		Limits: gateway.LoopLimits{ModelCalls: cfg.LoopLimits.ModelCalls, GatewayCalls: cfg.LoopLimits.GatewayCalls,
			SameToolCalls: cfg.LoopLimits.SameToolCalls, Attempts: cfg.LoopLimits.Attempts},
		Tools:       toolset,
		Faults:      script,
		Log:         log.With("component", "gateway"),
		Incarnation: uuid.NewString(),
	}
	// Before any call is served: effects the last agentd process claimed are recovered by
	// their class, in the background; the calls that ask for them join the recovery.
	if err := gw.RecoverEffects(ctx); err != nil {
		return fmt.Errorf("recover effects: %w", err)
	}
	// Doorbells wake the runs waiting for a person or for budget: those the last agentd process
	// wrote and never delivered go out now, and every new one as it is written. Approvals nobody
	// decided in time expire, as rejections, from now on.
	doorbells := &outbox.Outbox{Ledger: led, Temporal: tc, Hold: script.HoldDoorbells, Log: log.With("component", "outbox")}
	gw.Doorbells = doorbells
	go doorbells.Run(ctx)
	go gw.ExpireApprovals(ctx)
	// On shutdown, wait for running Invokes, then for the executions they started to record
	// their results: an effect left PENDING needs a person to reconcile it.
	defer func() {
		if !gw.Drain(drainTimeout) {
			log.Error("shut down with gateway executions still running; their effects stay PENDING")
		}
	}()
	w := worker.New(tc, cfg.TaskQueues.Gateway, worker.Options{WorkerStopTimeout: drainTimeout})
	w.RegisterActivityWithOptions(gw.Invoke, activity.RegisterOptions{Name: gateway.ActivityName})
	if err := w.Start(); err != nil {
		return fmt.Errorf("start gateway worker: %w", err)
	}
	defer w.Stop()

	// Runtime socket: the Worker's only way to Temporal.
	creds := insecure.NewCredentials()
	if tlsConfig != nil {
		creds = credentials.NewTLS(tlsConfig)
	}
	backend, err := grpc.NewClient(cfg.Temporal.Address,
		append(runtimeproxy.DialOptions(), grpc.WithTransportCredentials(creds), grpc.WithNoProxy())...)
	if err != nil {
		return err
	}
	defer backend.Close()
	runtimeLn, err := listen(cfg.RuntimeListen)
	if err != nil {
		return fmt.Errorf("runtime socket: %w", err)
	}
	runtimeSrv := runtimeproxy.NewServer(backend, runtimeproxy.Rules{
		Namespace:     cfg.Temporal.Namespace,
		WorkflowQueue: cfg.TaskQueues.Workflows,
		GatewayQueue:  cfg.TaskQueues.Gateway,
		ActivityType:  gateway.ActivityName,
		// AgentState, and the system attribute workflow.patched() sets on the workflow.
		SearchAttributes: []string{control.AgentState.GetName(), "TemporalChangeVersion"},
	}, log.With("component", "runtime-proxy"))
	defer runtimeSrv.Stop()

	// Control socket: agentctl only.
	controlLn, err := listen("unix://" + cfg.ControlSocket)
	if err != nil {
		return fmt.Errorf("control socket: %w", err)
	}
	ctl := &control.Server{
		Ledger:      led,
		Budget:      bud,
		Effects:     gw,
		Approvals:   gw,
		Doorbells:   doorbells,
		Temporal:    tc,
		Tasks:       registry,
		TaskQueue:   cfg.TaskQueues.Workflows,
		CancelGrace: cfg.CancelGrace,
		Faults:      script,
		Log:         log.With("component", "control"),
	}
	if err := ctl.RecoverStarts(ctx); err != nil {
		return fmt.Errorf("recover interrupted starts: %w", err)
	}
	if err := ctl.RecoverCancels(ctx); err != nil {
		return fmt.Errorf("recover cancels: %w", err)
	}
	controlSrv := grpc.NewServer()
	agentosv1.RegisterControlServer(controlSrv, ctl)
	defer controlSrv.Stop()

	errs := make(chan error, 2)
	go func() { errs <- runtimeSrv.Serve(runtimeLn) }()
	go func() { errs <- controlSrv.Serve(controlLn) }()
	log.Info("agentd ready",
		"runtime", cfg.RuntimeListen, "control", cfg.ControlSocket, "temporal", cfg.Temporal.Address,
		"gateway_queue", cfg.TaskQueues.Gateway, "workflow_queue", cfg.TaskQueues.Workflows)

	select {
	case <-ctx.Done():
		log.Info("shutting down")
		controlSrv.GracefulStop()
		runtimeSrv.Stop()
		return nil
	case err := <-errs:
		return err
	}
}

// listen opens "unix:///path" (in a 0700 directory, socket 0600) or "tcp://host:port".
func listen(addr string) (net.Listener, error) {
	if path, ok := strings.CutPrefix(addr, "unix://"); ok {
		// sun_path holds 104 bytes on macOS and 108 on Linux, including the terminating NUL.
		if len(path) > 103 {
			return nil, fmt.Errorf("unix socket path %s is %d bytes; keep it under 104 (choose a shorter path)", path, len(path))
		}
		if err := mkdirPrivate(filepath.Dir(path)); err != nil {
			return nil, err
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		ln, err := net.Listen("unix", path)
		if err != nil {
			return nil, err
		}
		return ln, os.Chmod(path, 0o600)
	}
	if hostport, ok := strings.CutPrefix(addr, "tcp://"); ok {
		host, _, err := net.SplitHostPort(hostport)
		if err != nil {
			return nil, err
		}
		if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			return nil, fmt.Errorf("%s: the runtime entry must be a loopback address", addr)
		}
		return net.Listen("tcp", hostport)
	}
	return nil, fmt.Errorf("%s: want unix:///path or tcp://127.0.0.1:port", addr)
}

// lockDataDir makes this process the only agentd on the data directory, before anything
// touches the ledger or the sockets. A second agentd would take the sockets over, charge the
// first one's open reservations in full, and recover effects the first one is still sending.
// The lock goes with the process, however it ends.
func lockDataDir(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, "agentd.lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("another agentd is using the data directory %s", dir)
		}
		return nil, fmt.Errorf("lock the data directory %s: %w", dir, err)
	}
	return f, nil
}

// buildTools makes the tools agentd offers. The GitHub tools need the github section of the
// config and a token; without them they only dry-run.
func buildTools(cfg *config.Config, pol *policy.Policy, led *ledger.Ledger, git *gitmcp.Client, testTools bool, log *slog.Logger) (tools.Registry, error) {
	var gh *githubtool.Client
	if g := cfg.GitHub; g != nil {
		var token string
		if g.Token != "" {
			var err error
			if token, err = secrets.Resolve(g.Token); err != nil {
				return nil, fmt.Errorf("github token: %w", err)
			}
		}
		var err error
		gh, err = githubtool.New(githubtool.Config{APIURL: g.APIURL, Token: token, RequestTimeout: g.RequestTimeout,
			Settle: g.Settle, Author: githubtool.Author{Name: g.AuthorName, Email: g.AuthorEmail}})
		if err != nil {
			return nil, fmt.Errorf("github: %w", err)
		}
	} else {
		log.Warn("GitHub is not configured: the github.* tools only dry-run, and nothing is written")
	}
	repos := map[string]githubtool.Repo{}
	for id, r := range cfg.Repos {
		owner, name, _ := strings.Cut(r.GitHub, "/")
		repos[id] = githubtool.Repo{Mirror: r.Mirror, Owner: owner, Name: name}
	}
	reg := tools.Registry{}
	reg.Add(&tools.ShowDiff{Blobs: led, Mirrors: cfg.Mirrors(), Git: git})
	// git.apply_check refuses what github.open_draft_pr would: a patch it passes is one that tool
	// takes, whatever the two tools' deny_paths say.
	applyDeny := slices.Concat(githubtool.ProtectedPaths, pol.Tools["git.apply_check"].DenyPaths,
		pol.Tools["github.open_draft_pr"].DenyPaths)
	reg.Add(&tools.ApplyCheck{Blobs: led, Mirrors: cfg.Mirrors(), DenyPaths: applyDeny, Git: git})
	reg.Add(&githubtool.OpenDraftPR{Client: gh, Repos: repos, Blobs: led, DenyPaths: pol.Tools["github.open_draft_pr"].DenyPaths, Git: git})
	reg.Add(&githubtool.CommentCommit{Client: gh, Repos: repos, Blobs: led, Effects: gateway.RunRecords{Ledger: led}})
	if testTools {
		log.Warn("TEST TOOLS ENABLED: never run this outside tests")
		reg.Add(&tools.TestWrite{Log: tools.NewExternalLog(filepath.Join(cfg.DataDir, "test-external.jsonl"))})
	}
	return reg, nil
}

// gitServer is how agentd runs the read-only git MCP server. Its sandbox launcher is required:
// the server reads repositories, which are untrusted. --insecure-dev lets a developer run it
// without one, and every start says so.
func gitServer(cfg *config.Config, insecureDev bool, log *slog.Logger) (*gitmcp.Client, error) {
	g := cfg.GitTools
	if g.Server == "" {
		self, err := os.Executable()
		if err != nil {
			return nil, err
		}
		g.Server = filepath.Join(filepath.Dir(self), "agentos-mcp-git")
	}
	if _, err := os.Stat(g.Server); err != nil {
		return nil, fmt.Errorf("git tool server: %w (build it with make build, or set git_tools.server)", err)
	}
	switch {
	case len(g.Sandbox) > 0:
		if _, err := os.Stat(g.Sandbox[0]); err != nil {
			return nil, fmt.Errorf("git_tools.sandbox: %w", err)
		}
	case insecureDev:
		log.Warn("THE GIT TOOL SERVER RUNS WITHOUT A SANDBOX (--insecure-dev): development only")
	default:
		return nil, errors.New("git_tools.sandbox is required: the launcher in sandbox/macos or sandbox/linux " +
			"(or start agentd with --insecure-dev, for development only)")
	}
	return gitmcp.NewClient(gitmcp.Options{Server: g.Server, Sandbox: g.Sandbox, DenyRead: toolDenyRead(cfg),
		Timeout: g.Timeout, Log: log.With("component", "git-tools")})
}

// toolDenyRead is what the git tool server must not read, whatever its launcher allows:
// agentd's data, its control socket and Runtime socket, the Temporal client certificate and
// key, and every mirror (the launcher lets the server read the call's own). config.Load
// refuses a mirror that holds any of the rest: the Linux launcher mounts all of the call's
// mirror.
func toolDenyRead(cfg *config.Config) []string {
	deny := []string{cfg.DataDir, filepath.Dir(cfg.ControlSocket)}
	if p, ok := strings.CutPrefix(cfg.RuntimeListen, "unix://"); ok {
		deny = append(deny, filepath.Dir(p))
	}
	if t := cfg.Temporal.TLS; t != nil {
		deny = append(deny, t.CertFile, t.KeyFile)
	}
	for _, id := range slices.Sorted(maps.Keys(cfg.Repos)) {
		deny = append(deny, cfg.Repos[id].Mirror)
	}
	return deny
}

func mkdirPrivate(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.Chmod(dir, 0o700)
}

// temporalTLS builds the mTLS client configuration agentd presents to Temporal.
func temporalTLS(c *config.TLS) (*tls.Config, error) {
	if c == nil {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("temporal tls: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, ServerName: c.ServerName, MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("temporal tls: no certificates in %s", c.CAFile)
		}
	}
	return cfg, nil
}

// modelAdapters builds one adapter for each provider a task may reach, by provider name. A
// provider's key is read from the OS keyring now, so that a missing key stops agentd at start
// rather than failing the first run. Claude and OpenAI take none only from a simulator on this
// machine; a Chat Completions server (Ollama, vLLM) needs one only when models.yaml names it.
// A provider elsewhere is reached over https only: the requests carry the task's data, and
// the key.
func modelAdapters(mods *models.Config, blobs models.BlobStore, dataDir string) (map[string]models.Adapter, error) {
	adapters := map[string]models.Adapter{}
	for name, p := range mods.Providers {
		if !mods.Uses(name) {
			continue
		}
		local := config.Loopback(p.BaseURL)
		if p.BaseURL != "" && !local && !strings.HasPrefix(p.BaseURL, "https://") {
			return nil, fmt.Errorf("provider %s: %s is not on this machine: the task's data goes there only over https", name, p.BaseURL)
		}
		key := ""
		if p.Key != "" || (!local && (p.Adapter == "anthropic" || p.API == "responses")) {
			var err error
			if key, err = secrets.Resolve(p.Key); err != nil {
				return nil, fmt.Errorf("provider %s: %w", name, err)
			}
		}
		switch p.Adapter {
		case "fake":
			adapters[name] = models.Fake{Log: models.NewProviderLog(filepath.Join(dataDir, "fake-provider.jsonl"))}
		case "anthropic":
			adapters[name] = models.NewAnthropic(name, key, p.BaseURL, blobs)
		case "openai_compat":
			id, err := safetyIdentifier(dataDir)
			if err != nil {
				return nil, err
			}
			a, err := models.NewOpenAICompat(name, p, key, id, blobs)
			if err != nil {
				return nil, fmt.Errorf("provider %s: %w", name, err)
			}
			adapters[name] = a
		default:
			return nil, fmt.Errorf("provider %s: adapter %q is not in this build", name, p.Adapter)
		}
	}
	return adapters, nil
}

// safetyIdentifier names this agentd to OpenAI's safeguards, and nothing else about it: a
// flagged request holds it to account, not the whole organization (design doc, section 9).
// It is random, made the first time and kept in the data directory.
func safetyIdentifier(dataDir string) (string, error) {
	path := filepath.Join(dataDir, "safety-identifier")
	if b, err := os.ReadFile(path); err == nil && len(strings.TrimSpace(string(b))) > 0 {
		return strings.TrimSpace(string(b)), nil
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("safety identifier: %w", err)
	}
	id := "agentos-" + rand.Text()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(id+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("safety identifier: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", fmt.Errorf("safety identifier: %w", err)
	}
	return id, nil
}

// limiters builds a token bucket for each provider with a requests_per_minute: a quarter of a
// minute's requests may go out at once, the rest at the steady rate.
func limiters(mods *models.Config) map[string]*rate.Limiter {
	out := map[string]*rate.Limiter{}
	for name, p := range mods.Providers {
		if p.RequestsPerMinute > 0 && mods.Uses(name) {
			out[name] = rate.NewLimiter(rate.Limit(float64(p.RequestsPerMinute)/60), max(1, p.RequestsPerMinute/4))
		}
	}
	return out
}
