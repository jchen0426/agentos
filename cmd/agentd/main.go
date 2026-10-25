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
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/budget"
	"github.com/jchen0426/agentos/internal/config"
	"github.com/jchen0426/agentos/internal/control"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/gateway"
	"github.com/jchen0426/agentos/internal/githubtool"
	"github.com/jchen0426/agentos/internal/gitmcp"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/models"
	"github.com/jchen0426/agentos/internal/outbound"
	"github.com/jchen0426/agentos/internal/outbox"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/pricing"
	"github.com/jchen0426/agentos/internal/runtimeproxy"
	"github.com/jchen0426/agentos/internal/safetext"
	"github.com/jchen0426/agentos/internal/secrets"
	"github.com/jchen0426/agentos/internal/statedir"
	"github.com/jchen0426/agentos/internal/supervisor"
	"github.com/jchen0426/agentos/internal/tasks"
	"github.com/jchen0426/agentos/internal/tools"
)

// drainTimeout bounds how long a shutdown waits for gateway work in flight.
const drainTimeout = 30 * time.Second

const (
	anchorEvery     = 30 * time.Second   // how often, at most, the audit log's head goes out to Temporal
	minRetention    = 7 * 24 * time.Hour // a shorter retention loses the anchors of runs sooner: agentd warns
	refusalQueue    = 256                // refused Runtime calls waiting for their records
	workerExitQueue = 64                 // the Worker's exits waiting for theirs
	maxGatewayCalls = 200                // gateway calls agentd executes at once
)

func main() {
	configPath := flag.String("config", "agentd.yaml", "path to agentd.yaml")
	faultScript := flag.String("fault-script", "", "TESTS ONLY: fault injection script for the tools, the fake model and run starts")
	testTools := flag.Bool("test-tools", false, "TESTS ONLY: also offer test.unsafe_write, a write to a local log")
	insecureDev := flag.Bool("insecure-dev", false, "DEVELOPMENT ONLY: run the git tool server without a sandbox when git_tools.sandbox is unset, "+
		"and the Worker when worker.sandbox is")
	noWorker := flag.Bool("no-worker", false, "start no Worker: run one yourself (maintenance, a staged migration, the evaluation)")
	logLevel := flag.String("log-level", "info", "debug, info, warn or error")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintln(os.Stderr, "agentd:", err)
		os.Exit(2)
	}
	// The log carries values the Worker chose (activity IDs, call names): no raw controls.
	log := slog.New(slog.NewJSONHandler(safetext.JSONWriter(os.Stderr), &slog.HandlerOptions{Level: level}))
	if err := run(*configPath, *faultScript, *testTools, *insecureDev, *noWorker, log); err != nil {
		log.Error("agentd stopped", "error", err)
		os.Exit(1)
	}
}

func run(configPath, faultScript string, testTools, insecureDev, noWorker bool, log *slog.Logger) error {
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
	// Before agentd changes anything: a Worker it could not run as the configuration says.
	if !noWorker {
		if err := checkWorker(runtime.GOOS, cfg, insecureDev); err != nil {
			return err
		}
	}
	if err := mkdirPrivate(cfg.DataDir); err != nil {
		return err
	}
	lock, err := lockDataDir(cfg.DataDir)
	if err != nil {
		return err
	}
	defer lock.Close()
	// Only now, holding the lock, and before anything listens: a Worker the last agentd on this data
	// directory left running when it was killed goes, whether this agentd runs one or not. Another
	// agentd on the data directory never got past the lock, and its Worker is left alone.
	workerLog := log.With("component", "worker")
	if err := supervisor.Sweep(filepath.Join(cfg.DataDir, "worker.pid"), workerLog); err != nil {
		return err
	}
	statedir.Sweep(supervisor.StateKind)
	pol, err := policy.Load(cfg.PolicyFile)
	if err != nil {
		return err
	}
	mods, err := models.Load(cfg.ModelsFile)
	if err != nil {
		return err
	}
	// The user's decision (M5 C2a): no Worker of agentd's own runs unsandboxed next to a real secret.
	if !noWorker {
		if err := checkUnsandboxedSecrets(cfg, mods, insecureDev); err != nil {
			return err
		}
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
	// The anchors of the audit log in Temporal last as long as the namespace keeps histories.
	ns, err := tc.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: cfg.Temporal.Namespace})
	if err != nil {
		return fmt.Errorf("describe the namespace %s: %w", cfg.Temporal.Namespace, err)
	}
	retention := ns.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration()
	if retention < minRetention {
		log.Warn("the namespace keeps closed runs' histories for a short time: the audit log's anchors in them go with them, "+
			"and agentctl audit verify cannot check a run's records after that", "namespace", cfg.Temporal.Namespace,
			"retention", retention.String(), "want_at_least", minRetention.String())
	}

	// First in the audit log, before this process changes anything: one data directory has one
	// agentd at a time (lockDataDir), so every record of agentd's after this one is this process's,
	// until the next start.
	incarnation := uuid.NewString()
	startedAt := time.Now()
	start := startRecord(configPath, cfg, pol, mods, prices, incarnation, faultScript, testTools, insecureDev)
	start["retention_seconds"] = int64(retention / time.Second)
	start["no_worker"] = noWorker
	if !noWorker {
		start["worker"] = workerRecord(cfg)
	}
	// The chain its anchor workflows are named by: its first record's. A new ledger's first record is
	// this one, which cannot name its own sha256.
	chain := ""
	if first, err := led.Q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: 1, Limit: 1}); err != nil {
		return fmt.Errorf("audit log: %w", err)
	} else if len(first) == 1 {
		chain = audit.AnchorChain(first[0].Sha256)
		start["chain"] = chain
	}
	identity, err := agentdIdentity(cfg.TaskQueues.Workflows, cfg.DataDir)
	if err != nil {
		return err
	}
	start["agentd"] = identity
	// The heads of the chain pushed since yesterday are records the ledger holds, in their order, and
	// the highest this agentd pushed under any chain is this chain's, unless the ledger lost records
	// while agentd was down: its log cut, or the ledger put back to an older copy, or replaced. What
	// agentd finds does not stop it (Temporal's signals are not authenticated, the identity in one no
	// more than its head: anyone who can signal could keep it from starting); its start's record says
	// it, at most audit.MaxFindings of it, and verify finds it too.
	findings, err := audit.CheckPushed(ctx, tc, led, identity, chain, time.Now())
	if err != nil {
		return fmt.Errorf("read the audit log's anchors in Temporal: %w", err)
	}
	if len(findings) > 0 {
		found, more := audit.FindingsRecord(findings)
		for _, f := range findings[:len(found)] {
			msg := "THE LEDGER DOES NOT BEAR OUT A HEAD OF ITS AUDIT LOG IN TEMPORAL: its log was cut, or the ledger put " +
				"back to an older copy or replaced, while agentd was down; or someone signalled an anchor workflow"
			if f.NotRead > 0 || f.Seq == 0 && f.EventID == 0 {
				msg = "THE START CHECK DID NOT READ ALL OF THE AUDIT LOG'S HEADS IN TEMPORAL: agentctl audit verify reads them"
			}
			log.Error(msg, "anchor_workflow", audit.Text(f.WorkflowID), "event_id", f.EventID, "seq", f.Seq, "sha256", f.Sha256,
				"why", f.Why, "not_read", f.NotRead)
		}
		start["anchor_findings"] = found
		if more > 0 {
			log.Error("THE LEDGER DOES NOT BEAR OUT MORE HEADS OF ITS AUDIT LOG IN TEMPORAL", "more", more)
			start["anchor_findings_more"] = more
		}
	}
	audit.OnChainMoved(led, func(err error) {
		log.Error("THE AUDIT LOG CHANGED UNDER AGENTD: agentd appends no more records, and pushes no more heads", "error", err)
	})
	startHead, err := audit.Record(ctx, led, audit.Event{Type: "agentd.started", Actor: audit.Agentd, Payload: start})
	if err != nil {
		return fmt.Errorf("audit log: %w", err)
	}
	// The chain's head goes out of the ledger, to Temporal, whenever it moved; and once more at
	// shutdown, after the last records (after the refusals' queue, after the gateway's drain: both
	// are deferred after this, so they end before it).
	anchors := &audit.Anchors{Ledger: led, Temporal: tc, Every: anchorEvery, Identity: identity,
		OnError:     func(err error) { log.Warn("could not push the audit log's head to Temporal", "error", err) },
		OnMisplaced: func(err error) { log.Error("the audit log's anchor workflow is not agentd's", "error", err) }}
	go anchors.Run(ctx)
	defer func() {
		push, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		anchors.Push(push)
	}()
	// Refused Runtime calls are recorded off the Worker's requests, through a bounded queue.
	refusals := audit.NewQueue(led, refusalQueue, func(err error) {
		log.Error("could not record a refused runtime call in the audit log", "error", err)
	})
	refusalsCtx, stopRefusals := context.WithCancel(context.Background())
	refusalsDone := make(chan struct{})
	go func() { refusals.Run(refusalsCtx); close(refusalsDone) }()
	defer func() { stopRefusals(); <-refusalsDone }()
	// The Worker's exits have a queue of their own: a queue's next record counts what that queue
	// dropped (dropped), and a worker.exited must not carry the refusals' count, as if exits were lost.
	exits := audit.NewQueue(led, workerExitQueue, func(err error) {
		log.Error("could not record the Worker's exit in the audit log", "error", err)
	})
	exitsCtx, stopExits := context.WithCancel(context.Background())
	exitsDone := make(chan struct{})
	go func() { exits.Run(exitsCtx); close(exitsDone) }()
	defer func() { stopExits(); <-exitsDone }()

	bud := &budget.Budget{Ledger: led}
	// Before any request is served: a reservation still open belongs to a request agentd
	// lost track of when it stopped, and the provider may have billed it.
	if n, err := bud.SettleOpen(ctx); err != nil {
		return fmt.Errorf("settle open reservations: %w", err)
	} else if n > 0 {
		log.Warn("charged reservations left open by the last run of agentd in full", "count", n)
	}

	adapters, err := modelAdapters(mods, led, cfg.DataDir, log)
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
	toolset, err := buildTools(cfg, pol, led, tc, git, registry, testTools, log)
	if err != nil {
		return err
	}
	if err := gateway.CheckTools(pol, toolset); err != nil {
		return err
	}
	if err := gateway.CheckModels(pol, mods); err != nil {
		return err
	}
	if err := gateway.CheckRequired(registry, mods, toolset); err != nil {
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
		Incarnation: incarnation,
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
	// A Worker schedules as many gateway calls as it likes; each waits for the ledger, whose writes
	// take turns. 200 at once is four times T7's 50, and bounds what one Worker makes agentd hold.
	w := worker.New(tc, cfg.TaskQueues.Gateway, worker.Options{WorkerStopTimeout: drainTimeout,
		MaxConcurrentActivityExecutionSize: maxGatewayCalls})
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
	var sup *supervisor.Supervisor
	if !noWorker {
		o, err := workerLaunch(runtime.GOOS, cfg, runtimeLn.Addr())
		if err != nil {
			return err
		}
		if len(cfg.Worker.Sandbox) == 0 {
			workerLog.Warn("THE WORKER RUNS WITHOUT A SANDBOX (--insecure-dev): development only")
		}
		// Its exits' records go through their own queue: Add never waits for the ledger.
		o.PIDFile, o.Log, o.Record = filepath.Join(cfg.DataDir, "worker.pid"), workerLog, exits.Add
		sup = supervisor.New(o)
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
	// The refusals the proxy logs go into the audit log too, as often as it logs them: the first of
	// each kind, about one a second beyond those, and a count of the rest. Not on the Worker's
	// request: the queue writes them.
	runtimeSrv.OnRefused(func(r runtimeproxy.Refused) { refusals.Add(refusalRecord(r)) })

	// Control socket: agentctl only.
	controlLn, err := listen("unix://" + cfg.ControlSocket)
	if err != nil {
		return fmt.Errorf("control socket: %w", err)
	}
	ctl := &control.Server{
		Namespace:      cfg.Temporal.Namespace,
		AnchorEvery:    anchorEvery,
		StartedAt:      startedAt,
		AnchorIdentity: identity,
		StartRecord:    startHead,
		Attempts:       gw,
		Ledger:         led,
		Budget:         bud,
		Effects:        gw,
		Approvals:      gw,
		Doorbells:      doorbells,
		Temporal:       tc,
		Tasks:          registry,
		TaskQueue:      cfg.TaskQueues.Workflows,
		CancelGrace:    cfg.CancelGrace,
		Faults:         script,
		Log:            log.With("component", "control"),
		SnapshotDir:    cfg.DataDir,
	}
	// An export's snapshot left by an agentd that stopped mid-export: nobody reads it any more.
	if n, err := control.RemoveSnapshots(cfg.DataDir); err != nil {
		return fmt.Errorf("remove stale export snapshots: %w", err)
	} else if n > 0 {
		log.Info("removed export snapshots a stopped agentd left", "count", n)
	}
	if err := ctl.RecoverStarts(ctx); err != nil {
		return fmt.Errorf("recover interrupted starts: %w", err)
	}
	if err := ctl.RecoverCancels(ctx); err != nil {
		return fmt.Errorf("recover cancels: %w", err)
	}
	// Every call carries the uid of the process at the other end of the socket, which the audit log
	// records with each change it makes.
	// A panic in any call is logged and fails the call, not agentd (control.RecoverUnary).
	controlSrv := grpc.NewServer(control.ServerOptions(log.With("component", "control"))...)
	agentosv1.RegisterControlServer(controlSrv, ctl)
	defer controlSrv.Stop()

	errs := make(chan error, 2)
	go func() { errs <- runtimeSrv.Serve(runtimeLn) }()
	go func() { errs <- controlSrv.Serve(controlLn) }()
	// The Worker stops first: this branch closes the Runtime entry before any deferred cleanup runs,
	// and a supervisor still running would start a Worker that cannot reach it. On an error, the
	// deferred Stop below is the latest defer, and runs before the servers' own.
	shutdown := func() error {
		log.Info("shutting down")
		sup.Stop()
		controlSrv.GracefulStop()
		runtimeSrv.Stop()
		log.Info("the Runtime entry is closed")
		return nil
	}
	if sup != nil {
		// The Runtime entry serves: the first Worker must say it is ready before agentd does.
		sup.Start()
		defer sup.Stop()
		select {
		case <-sup.Ready():
		case err := <-sup.Failed():
			return err
		case <-ctx.Done():
			return shutdown()
		case err := <-errs:
			return err
		}
	}
	theWorker := "none (--no-worker)"
	if sup != nil {
		_, pid := sup.Current()
		theWorker = fmt.Sprintf("pid %d", pid)
	}
	log.Info("agentd ready",
		"runtime", cfg.RuntimeListen, "control", cfg.ControlSocket, "temporal", cfg.Temporal.Address,
		"gateway_queue", cfg.TaskQueues.Gateway, "workflow_queue", cfg.TaskQueues.Workflows, "worker", theWorker)

	select {
	case <-ctx.Done():
		return shutdown()
	case err := <-errs:
		return err
	}
}

// agentdIdentity is who this agentd is in the heads it pushes (audit.Identity): from its
// configuration, not from the ledger; its data directory as the file system resolves it, whatever
// path the configuration spells (a symbolic link, a relative path).
func agentdIdentity(taskQueue, dataDir string) (string, error) {
	dir, err := filepath.Abs(dataDir)
	if err == nil {
		dir, err = filepath.EvalSymlinks(dir)
	}
	if err != nil {
		return "", err
	}
	return audit.Identity(taskQueue, dir), nil
}

// refusalRecord is the record of a line of the Runtime proxy's refusal log.
func refusalRecord(r runtimeproxy.Refused) audit.Event {
	p := map[string]any{"count": r.Count}
	if r.Method != "" {
		p["method"], p["reason"] = audit.Text(r.Method), audit.Text(r.Reason)
	}
	if r.Command != "" {
		p["command"] = audit.Text(r.Command)
	}
	if r.Within > 0 {
		p["within"] = r.Within.String()
	}
	return audit.Event{Type: "runtime.refused", Actor: audit.Agentd, Payload: p}
}

// startRecord is the payload of agentd.started: this process, how it was started, and what it was
// started with, each configuration file by the sha256 of the bytes it was loaded from.
func startRecord(configPath string, cfg *config.Config, pol *policy.Policy, mods *models.Config, prices *pricing.Table,
	incarnation, faultScript string, testTools, insecureDev bool) map[string]any {
	sandbox := ""
	if len(cfg.GitTools.Sandbox) > 0 {
		sandbox = cfg.GitTools.Sandbox[0]
	}
	if abs, err := filepath.Abs(configPath); err == nil {
		configPath = abs
	}
	file := func(path, sum string) map[string]string { return map[string]string{"path": path, "sha256": sum} }
	p := map[string]any{
		"pid": os.Getpid(), "incarnation": incarnation, "insecure_dev": insecureDev, "test_tools": testTools,
		"fault_script": faultScript != "", "git_tools_sandbox": sandbox,
		"config": map[string]any{
			"agentd": file(configPath, cfg.SHA256), "policy": file(cfg.PolicyFile, pol.SHA256),
			"models": file(cfg.ModelsFile, mods.SHA256), "prices": file(cfg.PricesFile, prices.SHA256),
		},
	}
	// The commit this agentd was built from, when the build recorded it.
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				p["revision"] = s.Value
			case "vcs.modified":
				p["modified"] = s.Value == "true"
			}
		}
	}
	return p
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
func buildTools(cfg *config.Config, pol *policy.Policy, led *ledger.Ledger, tc client.Client, git *gitmcp.Client,
	registry tasks.Registry, testTools bool, log *slog.Logger) (tools.Registry, error) {
	var gh *githubtool.Client
	if g := cfg.GitHub; g != nil {
		var token string
		if g.Token != "" {
			var err error
			if token, err = secrets.Resolve(g.Token); err != nil {
				return nil, fmt.Errorf("github token: %w", err)
			}
			warnIfCut(log, token, "github token")
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
	reg.Add(&tools.ShowDiff{Blobs: audit.RunBlobs{Ledger: led}, Mirrors: cfg.Mirrors(), Git: git})
	// git.apply_check refuses what github.open_draft_pr would: a patch it passes is one that tool
	// takes, whatever the two tools' deny_paths say.
	applyDeny := slices.Concat(githubtool.ProtectedPaths, pol.Tools["git.apply_check"].DenyPaths,
		pol.Tools["github.open_draft_pr"].DenyPaths)
	reg.Add(&tools.ApplyCheck{Blobs: audit.RunBlobs{Ledger: led}, Mirrors: cfg.Mirrors(), DenyPaths: applyDeny, Git: git})
	reg.Add(&githubtool.OpenDraftPR{Client: gh, Repos: repos, Blobs: audit.RunBlobs{Ledger: led}, DenyPaths: pol.Tools["github.open_draft_pr"].DenyPaths, Git: git})
	// The report says what the run's history and agentd's records say of it, by runs explain's rules.
	reg.Add(&githubtool.CommentCommit{Client: gh, Repos: repos, Blobs: audit.RunBlobs{Ledger: led}, Effects: gateway.RunRecords{Ledger: led},
		Verdicts: gateway.ReportVerdicts{Ledger: led, Temporal: tc, Tasks: registry, Log: log.With("component", "report")},
		Task:     tasks.AuditModel})
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
func modelAdapters(mods *models.Config, blobs models.BlobStore, dataDir string, log *slog.Logger) (map[string]models.Adapter, error) {
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
			warnIfCut(log, key, "provider "+name)
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

// warnIfCut warns when the secret of what (a provider, the GitHub token) may have been cut as
// it was stored (secrets.MayBeCut): nothing else says so before its first 401.
func warnIfCut(log *slog.Logger, secret, what string) {
	if secrets.MayBeCut(secret) {
		log.Warn(fmt.Sprintf("the keychain item holds exactly %d characters, as many as security's prompt keeps: "+
			"a longer key stored through the prompt was cut there, and will be refused; store it again as the README says",
			secrets.PromptLimit), "secret", what)
	}
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
