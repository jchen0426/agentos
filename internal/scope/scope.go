// Package scope holds the Worker and each git tool call to their resource limits on Linux (M5 C3a):
// each runs in a systemd user scope of its own, a transient unit of the user's systemd manager whose
// cgroup (v2) holds what no process's own limit can: all the memory its processes use, the kernel's
// for them among it (page tables, socket and pipe buffers, memfd, tmpfs), with no swap; and its
// tasks, processes and threads together. The launchers start the scope themselves
// (sandbox/linux/limits.sh), named and sized as agentd says (Set.Env). agentd checks, as it starts,
// that the user manager holds such limits at all (Scopes.Probe); and after a run that failed it asks
// systemd how the run's scope ended (Set.OOMKilled): past its memory limit the kernel kills one of
// its processes, and systemd stops the whole scope (OOMPolicy=stop) and keeps it, failed, with the
// Result oom-kill, until reset-failed. The cgroup itself is gone by then: systemd removes it as it
// stops the scope.
package scope

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// OffEnv is what a launcher gets with agentd's --insecure-dev: no scope and no limits. A launcher given
// neither it nor the limits refuses to run.
const OffEnv = "AGENTOS_LIMITS=off"

// Limits are a run's resource limits; its swap is always none.
type Limits struct {
	Memory int64 // bytes, a whole number of MiB (so of pages, as the kernel holds memory.max)
	Tasks  int64 // processes and threads, together
}

// Runner runs one of systemd's commands, argv, with env as its whole environment, and returns what
// it wrote to its standard output; its error says what it wrote to its standard error. Exec runs the
// command; a test's Runner answers as systemd would.
type Runner func(ctx context.Context, env []string, argv ...string) ([]byte, error)

// Exec is the Runner that runs the command.
func Exec(ctx context.Context, env []string, argv ...string) ([]byte, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Env, cmd.Stderr = env, &stderr
	out, err := cmd.Output()
	if err != nil {
		return out, fmt.Errorf("%s: %w: %s", argv[0], err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// RuntimeDir is where agentd finds the user manager: its own XDG_RUNTIME_DIR, an absolute path, else
// /run/user/<uid>, where systemd-logind makes it.
func RuntimeDir() string {
	if d := os.Getenv("XDG_RUNTIME_DIR"); filepath.IsAbs(d) {
		return d
	}
	return fmt.Sprintf("/run/user/%d", os.Getuid())
}

// Scopes are the scopes of one agentd start. Each is named agentos-<token>-<kind>-<n>.scope, with a
// token of the start's own: no name is one systemd still keeps from another agentd (a failed scope
// nobody cleared stays until reset-failed, and systemd refuses a second unit of the same name).
type Scopes struct {
	dir   string // XDG_RUNTIME_DIR, which systemctl and systemd-run find the user manager by
	token string
	run   Runner
}

// New makes the scopes of an agentd start, whose user manager listens in dir, asked through run.
func New(dir string, run Runner) *Scopes {
	b := make([]byte, 8)
	_, _ = rand.Read(b) // never fails (crypto/rand)
	return &Scopes{dir: dir, token: hex.EncodeToString(b), run: run}
}

// Names is the pattern of the names of the start's scopes, for its log.
func (s *Scopes) Names() string { return "agentos-" + s.token + "-*.scope" }

// env is the environment of systemctl and systemd-run: where the user manager listens, and nothing of
// agentd's own but PATH. Not DBUS_SESSION_BUS_ADDRESS, as the launchers get none: systemd-run falls
// back to the bus it names when systemd/private does not answer, and the probe would pass where the
// launchers fail.
func (s *Scopes) env() []string {
	path := os.Getenv("PATH")
	if path == "" {
		path = "/usr/bin:/bin"
	}
	return []string{"PATH=" + path, "LANG=C.UTF-8", "XDG_RUNTIME_DIR=" + s.dir}
}

// On is the scopes of kind's runs (worker, tool), each holding l.
func (s *Scopes) On(kind string, l Limits) *Set { return &Set{kind: kind, limits: l, scopes: s} }

// Off is kind's runs with their limits off (--insecure-dev): no scope, and AGENTOS_LIMITS=off for
// their launcher, which refuses to run without either.
func Off(kind string, l Limits) *Set { return &Set{kind: kind, limits: l} }

// Set is the scopes of one kind of run: the Worker's generations, or the git tool calls.
type Set struct {
	kind   string
	limits Limits
	scopes *Scopes // nil: off
}

// Limits are the set's limits, held or not.
func (t *Set) Limits() Limits { return t.limits }

// Memory is the memory limit, as agentd.yaml writes it (512MiB, 1GiB) and the log and a refusal name it.
func (t *Set) Memory() string {
	switch m := t.limits.Memory; {
	case m > 0 && m%(1<<30) == 0:
		return fmt.Sprintf("%dGiB", m>>30)
	case m > 0 && m%(1<<20) == 0:
		return fmt.Sprintf("%dMiB", m>>20)
	default:
		return fmt.Sprintf("%d bytes", m)
	}
}

// Env is what the launcher of run n (a generation's number, a call's) gets of the limits: the name of
// the run's scope ("" when they are off), and the variables limits.sh takes, which name it, size it
// and say where the user manager listens; or AGENTOS_LIMITS=off.
func (t *Set) Env(n int64) (unit string, env []string) {
	if t.scopes == nil {
		return "", []string{OffEnv}
	}
	unit = fmt.Sprintf("agentos-%s-%s-%d.scope", t.scopes.token, t.kind, n)
	return unit, []string{"AGENTOS_LIMIT_MEMORY=" + strconv.FormatInt(t.limits.Memory, 10),
		"AGENTOS_LIMIT_TASKS=" + strconv.FormatInt(t.limits.Tasks, 10), "AGENTOS_LIMIT_UNIT=" + unit,
		"XDG_RUNTIME_DIR=" + t.scopes.dir}
}

// How long OOMKilled waits for systemd to be done with a scope, and how often it asks meanwhile. A
// scope whose processes have all exited is still active until systemd hears that its cgroup is empty,
// some 10 ms later in the Lima VM; one the kernel killed in is failed, with its Result, at once.
// Variables for the tests only.
var (
	settleWithin = 3 * time.Second
	settleEvery  = 10 * time.Millisecond
)

// OOMKilled reports, once what ran in unit has exited, whether the kernel killed it at its memory
// limit: whether systemd says the scope ended with the Result oom-kill. It waits for systemd to be
// done with the scope, at most settleWithin, and clears what systemd keeps of a failed one
// (reset-failed, whose error it ignores: a scope left is harmless, and its name no other run's). Its
// error says systemd could not be asked.
func (t *Set) OOMKilled(unit string) (bool, error) {
	if t.scopes == nil || unit == "" {
		return false, nil
	}
	s := t.scopes
	ctx, cancel := context.WithTimeout(context.Background(), settleWithin)
	defer cancel()
	for {
		out, err := s.run(ctx, s.env(), "systemctl", "--user", "show", "--property=ActiveState,Result", "--", unit)
		if err != nil {
			return false, err
		}
		p := properties(out)
		switch state := p["ActiveState"]; state {
		case "active", "activating", "deactivating", "reloading", "refreshing":
			select {
			case <-ctx.Done():
				// Still running something, or stopping it: the Result may be the kill's already.
				return p["Result"] == "oom-kill", nil
			case <-time.After(settleEvery):
			}
			continue
		case "failed":
			_, _ = s.run(ctx, s.env(), "systemctl", "--user", "reset-failed", "--", unit)
		}
		return p["Result"] == "oom-kill", nil
	}
}

// properties are systemctl show's Name=value lines.
func properties(out []byte) map[string]string {
	p := map[string]string{}
	for line := range strings.SplitSeq(string(out), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			p[k] = v
		}
	}
	return p
}

// ErrNoManager is Probe's error when there is no systemd user manager to run the scopes.
var ErrNoManager = errors.New("no systemd user manager")

// ErrNotHeld is Probe's error when a scope does not hold its limits.
var ErrNotHeld = errors.New("a systemd user scope does not hold its limits")

// probeScript runs in a probe scope, and says where it runs and what holds it: /proc/self/cgroup,
// then each limit's file in its cgroup, as "<file> <value>", "missing" for one it cannot read.
const probeScript = `while IFS= read -r line; do echo "cgroup $line"; done < /proc/self/cgroup
d=/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/self/cgroup)
for f in memory.max memory.swap.max pids.max; do
  if v=$(cat "$d/$f" 2>/dev/null); then echo "$f $v"; else echo "$f missing"; fi
done`

// Probe checks, before agentd serves anything, that the user manager runs scopes that hold each set's
// limits: in a scope of each set's own sizes, like the one the set's launcher starts, the scope's
// cgroup is the process's (cgroup v2, nothing else) and memory.max, memory.swap.max and pids.max read
// back as asked. Its error tells a missing user manager (ErrNoManager: no login session, no linger)
// apart from limits that do not take (ErrNotHeld: the controllers not delegated to it), and says how
// to make either right.
func (s *Scopes) Probe(ctx context.Context, sets ...*Set) error {
	private := filepath.Join(s.dir, "systemd", "private")
	if fi, err := os.Stat(private); err != nil || fi.Mode().Type() != fs.ModeSocket {
		why := "it is not a socket"
		if err != nil {
			why = err.Error()
		}
		return s.noManager(fmt.Sprintf("its socket %s is not there (%s)", private, why))
	}
	for _, t := range sets {
		if t.scopes != s {
			continue // off, or another start's
		}
		unit := fmt.Sprintf("agentos-%s-probe-%s.scope", s.token, t.kind)
		probe, cancel := context.WithTimeout(ctx, 30*time.Second)
		out, err := s.run(probe, s.env(), "systemd-run", "--user", "--scope", "--quiet", "--collect", "--unit="+unit,
			"--property=MemoryMax="+strconv.FormatInt(t.limits.Memory, 10), "--property=MemorySwapMax=0",
			"--property=TasksMax="+strconv.FormatInt(t.limits.Tasks, 10), "--property=OOMPolicy=stop",
			"--", "/bin/sh", "-c", probeScript)
		cancel()
		switch {
		case err != nil && strings.Contains(err.Error(), "Failed to connect to bus"):
			return s.noManager(err.Error())
		case err != nil:
			return fmt.Errorf("agentd could not start a systemd user scope to check that it holds the %s limits: %w", t.kind, err)
		}
		if err := t.heldIn(unit, out); err != nil {
			return fmt.Errorf("%w: %w; systemd delegates the memory and pids controllers to user@<uid>.service by default "+
				"(Delegate=): /sys/fs/cgroup/user.slice/user-%d.slice/user@%d.service/cgroup.controllers lists those it has; "+
				"or start agentd with --insecure-dev, which turns the limits off (development only)", ErrNotHeld, err, os.Getuid(), os.Getuid())
		}
	}
	return nil
}

// noManager is ErrNoManager, with why and how to start one.
func (s *Scopes) noManager(why string) error {
	name := "<user>"
	if u, err := user.Current(); err == nil {
		name = u.Username
	}
	return fmt.Errorf("%w for uid %d at XDG_RUNTIME_DIR %s: %s. agentd runs the Worker and each git tool call in a systemd "+
		"user scope, which holds their memory and task limits, and the user's manager runs the scopes. A login session "+
		"starts one; to keep one without a session, enable lingering (sudo loginctl enable-linger %s). Or start agentd "+
		"with --insecure-dev, which turns the limits off (development only)", ErrNoManager, os.Getuid(), s.dir, why, name)
}

// heldIn checks a probe's report of unit: one cgroup, the unit's, and each limit as t asks.
func (t *Set) heldIn(unit string, out []byte) error {
	var cgroups []string
	got := map[string]string{}
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		k, v, _ := strings.Cut(line, " ")
		if k == "cgroup" {
			cgroups = append(cgroups, v)
		} else {
			got[k] = v
		}
	}
	if len(cgroups) != 1 || !strings.HasPrefix(cgroups[0], "0::/") {
		return fmt.Errorf("the probe scope's process is in the cgroups %q, not in one of cgroup v2's (the unified hierarchy)", cgroups)
	}
	if !strings.HasSuffix(cgroups[0], "/"+unit) {
		return fmt.Errorf("the probe's process is in the cgroup %s, not in its scope's (%s)", strings.TrimPrefix(cgroups[0], "0::"), unit)
	}
	for _, f := range []struct{ file, want string }{{"memory.max", strconv.FormatInt(t.limits.Memory, 10)}, {"memory.swap.max", "0"},
		{"pids.max", strconv.FormatInt(t.limits.Tasks, 10)}} {
		switch v, ok := got[f.file]; {
		case !ok || v == "missing":
			return fmt.Errorf("the probe scope has no %s (is that controller delegated to the user manager?)", f.file)
		case v != f.want:
			return fmt.Errorf("the probe scope's %s is %s, not %s", f.file, v, f.want)
		}
	}
	return nil
}
