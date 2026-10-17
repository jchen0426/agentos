// agentd is the Agent OS daemon. It holds the keys and the ledgers and is the only way out:
// it serves the gateway activity, the Control socket for agentctl, and the Runtime socket
// through which the sandboxed Worker reaches Temporal.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

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
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/models"
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
	faultScript := flag.String("fault-script", "", "TESTS ONLY: fault injection script for the stub tools and the fake model")
	logLevel := flag.String("log-level", "info", "debug, info, warn or error")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintln(os.Stderr, "agentd:", err)
		os.Exit(2)
	}
	// The log carries values the Worker chose (activity IDs, call names): no raw controls.
	log := slog.New(slog.NewJSONHandler(safetext.JSONWriter(os.Stderr), &slog.HandlerOptions{Level: level}))
	if err := run(*configPath, *faultScript, log); err != nil {
		log.Error("agentd stopped", "error", err)
		os.Exit(1)
	}
}

func run(configPath, faultScript string, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if err := mkdirPrivate(cfg.DataDir); err != nil {
		return err
	}
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
		// Every model a task may reach must be priced before agentd will start.
		route, err := mods.Route(name)
		if err != nil {
			return err
		}
		if _, err := prices.Bound(route.Candidates, 0, route.MaxTokens); err != nil {
			return fmt.Errorf("model task %s: %w", name, err)
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
		ConnectionOptions: client.ConnectionOptions{TLS: tlsConfig},
	})
	if err != nil {
		return fmt.Errorf("connect to Temporal at %s: %w", cfg.Temporal.Address, err)
	}
	defer tc.Close()

	registry := tasks.Registry{"security-audit": tasks.SecurityAudit(pol, cfg.Repos)}
	adapters, err := modelAdapters(mods, led, cfg.DataDir)
	if err != nil {
		return err
	}
	for name := range mods.Tasks {
		route, err := mods.Route(name)
		if err != nil {
			return err
		}
		if c, ok := adapters[route.Provider].(models.RouteChecker); ok {
			if err := c.CheckRoute(route); err != nil {
				return fmt.Errorf("model task %s: %w", name, err)
			}
		}
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
		Tools:  &tools.Stubs{Blobs: led, External: tools.NewExternalLog(filepath.Join(cfg.DataDir, "stub-external.jsonl"))},
		Faults: script,
		Log:    log.With("component", "gateway"),
	}
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
		append(runtimeproxy.DialOptions(), grpc.WithTransportCredentials(creds))...)
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
		Temporal:    tc,
		Tasks:       registry,
		TaskQueue:   cfg.TaskQueues.Workflows,
		CancelGrace: cfg.CancelGrace,
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
// rather than failing the first run.
func modelAdapters(mods *models.Config, blobs models.BlobStore, dataDir string) (map[string]models.Adapter, error) {
	adapters := map[string]models.Adapter{}
	for name, p := range mods.Providers {
		if !mods.Uses(name) {
			continue
		}
		switch p.Adapter {
		case "fake":
			adapters[name] = models.Fake{Log: models.NewProviderLog(filepath.Join(dataDir, "fake-provider.jsonl"))}
		case "anthropic":
			key, err := secrets.Resolve(p.Key)
			if err != nil {
				return nil, fmt.Errorf("provider %s: %w", name, err)
			}
			adapters[name] = models.NewAnthropic(name, key, p.BaseURL, blobs)
		default:
			return nil, fmt.Errorf("provider %s: adapter %q is not in this build", name, p.Adapter)
		}
	}
	return adapters, nil
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
