package main

import (
	"context"
	"log/slog"

	"github.com/jchen0426/agentos/internal/config"
	"github.com/jchen0426/agentos/internal/scope"
)

// What holds the limits, as agentd.started records it (limitsRecord).
const (
	enforcedByScope = "systemd_user_scope" // Linux: a systemd user scope of each run's own
	enforcedOff     = "off"                // Linux, --insecure-dev: no scope
	enforcedByNone  = "none"               // macOS: its sandbox sets none
)

// sandboxLimits are the resource limits of the Worker's and the git tool calls' launchers (M5 C3a),
// and what holds them.
type sandboxLimits struct {
	// worker and tools are what each launcher gets of them, nil for nothing: none on this platform, no
	// launcher, or (worker) no Worker of agentd's own.
	worker, tools *scope.Set
	enforcement   string
	scopes        *scope.Scopes // nil unless the limits are held
}

// resourceLimits are the Worker's and the git tool calls' limits on goos, before agentd changes
// anything. On Linux, without --insecure-dev, each run of a launcher the configuration names runs in a
// systemd user scope of its own, and a probe scope of each set shows now that the user manager holds
// them: else agentd refuses to start, saying why. --insecure-dev turns them off: a launcher gets
// AGENTOS_LIMITS=off. macOS holds none, and refuses a configuration that names any (CheckLimits).
func resourceLimits(ctx context.Context, goos string, cfg *config.Config, insecureDev, noWorker bool, run scope.Runner) (sandboxLimits, error) {
	if err := cfg.CheckLimits(goos); err != nil {
		return sandboxLimits{}, err
	}
	worker, tools := scopeLimits(cfg.Worker.Limits), scopeLimits(cfg.GitTools.Limits)
	workerLauncher, toolLauncher := !noWorker && len(cfg.Worker.Sandbox) > 0, len(cfg.GitTools.Sandbox) > 0
	var l sandboxLimits
	switch {
	case goos != "linux":
		l.enforcement = enforcedByNone
	case insecureDev:
		l.enforcement = enforcedOff
		if workerLauncher {
			l.worker = scope.Off("worker", worker)
		}
		if toolLauncher {
			l.tools = scope.Off("tool", tools)
		}
	default:
		l.enforcement, l.scopes = enforcedByScope, scope.New(scope.RuntimeDir(), run)
		var sets []*scope.Set
		if workerLauncher {
			l.worker = l.scopes.On("worker", worker)
			sets = append(sets, l.worker)
		}
		if toolLauncher {
			l.tools = l.scopes.On("tool", tools)
			sets = append(sets, l.tools)
		}
		if err := l.scopes.Probe(ctx, sets...); err != nil {
			return sandboxLimits{}, err
		}
	}
	return l, nil
}

func scopeLimits(l config.Limits) scope.Limits {
	return scope.Limits{Memory: int64(l.Memory), Tasks: int64(l.Tasks)}
}

// say logs how the limits are held, once agentd has checked them: a warning when they are off.
func (l sandboxLimits) say(cfg *config.Config, log *slog.Logger) {
	w, t := cfg.Worker.Limits, cfg.GitTools.Limits
	switch l.enforcement {
	case enforcedOff:
		log.Warn("THE RESOURCE LIMITS ARE OFF (--insecure-dev): the Worker and the git tool calls may use any memory and "+
			"tasks; development only", "worker_memory", w.Memory.String(), "worker_tasks", int64(w.Tasks),
			"tool_memory", t.Memory.String(), "tool_tasks", int64(t.Tasks))
	case enforcedByScope:
		log.Info("the Worker's generations and the git tool calls each run in a systemd user scope, which holds their limits",
			"worker_memory", w.Memory.String(), "worker_tasks", int64(w.Tasks), "tool_memory", t.Memory.String(),
			"tool_tasks", int64(t.Tasks), "scopes", l.scopes.Names())
	}
}

// limitsRecord is a set of limits in agentd.started: memory and tasks, and swap, which is always none;
// and what holds them: a systemd user scope of each run's own (Linux), off (--insecure-dev), or none
// (macOS).
func limitsRecord(l config.Limits, enforcement string) map[string]any {
	return map[string]any{"memory_bytes": int64(l.Memory), "tasks": int64(l.Tasks), "swap_bytes": 0, "enforcement": enforcement}
}
