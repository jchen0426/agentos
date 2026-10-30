package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/user"
	"path/filepath"
	"strings"

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

// The git tool calls agentd runs at once, at most: the runs' calls in the git tool client's slots
// (gitServer), and the run starts' resolve_commit in slots of their own (run). The limits' worst case
// counts both.
const (
	toolCallSlots  = 4
	startCallSlots = 2
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
	worst         *worstCase    // nil unless the limits are held
}

// worstCase is the most memory the start's scopes can hold at once: each scope holds its own limit,
// and nothing holds their sum. It counts what agentd runs: the Worker's generation, and a git tool
// call in each slot (a second attempt runs in its call's slot once the first's scope has ended, and
// adds nothing). room is what the scopes can have between them, unless roomErr says why it is not
// known.
type worstCase struct {
	worker      int64 // the Worker's memory limit; 0 without a Worker of agentd's own
	tool, calls int64 // each git tool call's, and how many run at once; 0 calls without a launcher
	room        scope.Room
	roomErr     error
}

func (w *worstCase) bytes() int64 { return w.worker + w.tool*w.calls }

// over reports whether the worst case is over half the room: more than what is left of it.
func (w *worstCase) over() bool { return w.roomErr == nil && w.bytes() > w.room.Bytes-w.bytes() }

// warning is what agentd says of a worst case over half the room: what adds up to what, against what,
// what that means, and how to lower it.
func (w *worstCase) warning() string {
	var parts []string
	if w.worker > 0 {
		parts = append(parts, "the Worker's "+config.Bytes(w.worker).String())
	}
	if w.calls > 0 {
		parts = append(parts, fmt.Sprintf("%d git tool calls at %s each", w.calls, config.Bytes(w.tool)))
	}
	sum := strings.Join(parts, " and ") + " come to " + config.Bytes(w.bytes()).String()
	if len(parts) == 1 && w.calls == 0 {
		sum = parts[0] + " alone"
	}
	of, past, from := "the machine's memory", "what the machine has", "the whole machine"
	held := approx(w.room.Bytes) + " (MemTotal in /proc/meminfo)"
	if w.room.From != "MemTotal" {
		of, past, from = "what the cgroup "+w.room.From+" may use", "what that cgroup may use", "all of that cgroup"
		held = approx(w.room.Bytes) + " (its memory.max)"
	}
	return fmt.Sprintf("the resource limits' worst case is over half of %s: %s, of %s. Each scope holds its own limit, and "+
		"nothing limits their sum: past %s, the kernel's out-of-memory killer picks its victim from %s, not only from the "+
		"scopes. worker.limits.memory and git_tools.limits.memory lower it", of, sum, held, past, from)
}

// approx writes b as a whole number of MiB or GiB where it is one, else in GiB to a tenth (MiB below 1GiB).
func approx(b int64) string {
	switch {
	case b > 0 && b%(1<<20) == 0:
		return config.Bytes(b).String()
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGiB", float64(b)/(1<<30))
	}
	return fmt.Sprintf("%.1fMiB", float64(b)/(1<<20))
}

// resourceLimits are the Worker's and the git tool calls' limits on goos, before agentd changes
// anything; systemd's commands run through run, and read reads /proc's and /sys/fs/cgroup's files. On
// Linux, without --insecure-dev, each run of a launcher the configuration names runs in a systemd user
// scope of its own, and a probe scope of each set shows now that the user manager holds them: else
// agentd refuses to start, saying why; their worst case (worstCase) is held against the memory the
// scopes can have. --insecure-dev turns them off: a launcher gets
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
		l.worst = &worstCase{}
		if workerLauncher {
			l.worst.worker = worker.Memory
		}
		if toolLauncher {
			l.worst.tool, l.worst.calls = tools.Memory, toolCallSlots+startCallSlots
		}
		l.worst.room, l.worst.roomErr = l.scopes.Room()
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

// say logs how the limits are held, once agentd has checked them: a warning when they are off, one when
// their worst case is over half of the memory the scopes can have (or agentd cannot tell), and one when
// logind would stop the user manager, and with it every scope, at the user's last logout.
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
		switch worst := l.worst; {
		case worst == nil:
		case worst.roomErr != nil:
			log.Warn("agentd cannot tell whether the resource limits' worst case is over half of the memory the scopes can have",
				"worst_case", config.Bytes(worst.bytes()).String(), "error", worst.roomErr)
		case worst.over():
			log.Warn(worst.warning(), "worst_case_bytes", worst.bytes(), "memory_bytes", worst.room.Bytes, "memory_from", worst.room.From)
		}
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

// worstCaseRecord is the limits' worst case in agentd.started, where they hold (worstCase): its bytes and
// what they are made of, the memory they were compared with and where it came from (or why it is not
// known), and whether agentd warned. nil where the limits are off or none.
func (l sandboxLimits) worstCaseRecord() map[string]any {
	w := l.worst
	if w == nil {
		return nil
	}
	r := map[string]any{"memory_bytes": w.bytes(), "worker_memory_bytes": w.worker, "tool_memory_bytes": w.tool,
		"tool_calls": w.calls, "warned": w.over()}
	if w.roomErr != nil {
		r["compared_with_error"] = w.roomErr.Error()
	} else {
		r["compared_with_bytes"], r["compared_with"] = w.room.Bytes, w.room.From
	}
	return r
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
