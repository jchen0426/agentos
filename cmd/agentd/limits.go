package main

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"os/user"
	"path/filepath"

	"github.com/jchen0426/agentos/internal/config"
	"github.com/jchen0426/agentos/internal/scope"
)

// lingerDir is where systemd-logind keeps whose user managers outlive their sessions: a file of each
// user's name. A variable for the tests.
var lingerDir = "/var/lib/systemd/linger"

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
	noLinger      string        // the user, when the limits are held and logind does not keep their manager
}

// resourceLimits are the Worker's and the git tool calls' limits on goos, before agentd changes
// anything; systemd's commands run through run, and read reads /proc's files. On Linux, without --insecure-dev, each run of a launcher the configuration names runs in a
// systemd user scope of its own, and a probe scope of each set shows now that the user manager holds
// them: else agentd refuses to start, saying why. --insecure-dev turns them off: a launcher gets
// AGENTOS_LIMITS=off. macOS holds none, and refuses a configuration that names any (CheckLimits).
func resourceLimits(ctx context.Context, goos string, cfg *config.Config, insecureDev, noWorker bool, run scope.Runner,
	read func(string) ([]byte, error)) (sandboxLimits, error) {
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
		dir, passedOver := scope.RuntimeDir()
		l.enforcement, l.scopes = enforcedByScope, scope.New(dir, run, read).PassedOver(passedOver)
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
		if u, err := user.Current(); err == nil {
			if _, err := os.Stat(filepath.Join(lingerDir, u.Username)); errors.Is(err, fs.ErrNotExist) {
				l.noLinger = u.Username
			}
		}
	}
	return l, nil
}

func scopeLimits(l config.Limits) scope.Limits {
	return scope.Limits{Memory: int64(l.Memory), Tasks: int64(l.Tasks)}
}

// say logs how the limits are held, once agentd has checked them: a warning when they are off, and one
// when logind would stop the user manager, and with it every scope, at the user's last logout.
func (l sandboxLimits) say(cfg *config.Config, log *slog.Logger) {
	w, t := cfg.Worker.Limits, cfg.GitTools.Limits
	switch l.enforcement {
	case enforcedOff:
		log.Warn("THE RESOURCE LIMITS ARE OFF (--insecure-dev): the Worker and the git tool calls may use any memory and " +
			"tasks; development only")
	case enforcedByScope:
		log.Info("the Worker's generations and the git tool calls each run in a systemd user scope, which holds their limits",
			"worker_memory", w.Memory.String(), "worker_tasks", int64(w.Tasks), "tool_memory", t.Memory.String(),
			"tool_tasks", int64(t.Tasks), "scopes", l.scopes.Names())
	}
	if l.noLinger != "" {
		log.Warn("lingering is off for "+l.noLinger+": when the user's last session ends, logind stops their systemd user "+
			"manager, and with it the Worker and every git tool call in its scopes, and the launchers refuse to run until the "+
			"user logs in again; to keep the manager, sudo loginctl enable-linger "+l.noLinger, "user", l.noLinger)
	}
}

// clear clears what systemd keeps of this start's scopes that ended failed and were not cleared as
// they ended (scope.Scopes.ResetFailed), as agentd stops.
func (l sandboxLimits) clear(log *slog.Logger) {
	if l.scopes == nil {
		return
	}
	if err := l.scopes.ResetFailed(); err != nil {
		log.Warn("could not clear the failed scopes of this agentd's start: systemctl --user --failed lists them", "scopes",
			l.scopes.Names(), "error", err)
	}
}

// limitsRecord is a set of limits in agentd.started: what holds them, a systemd user scope of each run's
// own (Linux), off (--insecure-dev) or none (macOS); and, where they are held, memory and tasks, and
// swap, which is always none.
func limitsRecord(l config.Limits, enforcement string) map[string]any {
	if enforcement != enforcedByScope {
		return map[string]any{"enforcement": enforcement}
	}
	return map[string]any{"memory_bytes": int64(l.Memory), "tasks": int64(l.Tasks), "swap_bytes": 0, "enforcement": enforcement}
}
