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
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// OffEnv is what a launcher gets with agentd's --insecure-dev: no scope and no limits. A launcher given
// neither it nor the limits refuses to run.
const OffEnv = "AGENTOS_LIMITS=off"

// OOMScoreAdj is the oom_score_adj a launcher gives its sandbox, in its scope (limits.sh, which writes
// the same number, and TestTheLaunchersTakeWhatEnvGives holds it to this one): the sandbox's processes
// are the kernel's out-of-memory killer's first pick when the machine, or a cgroup above the scopes,
// runs out of memory. A process may lower its own as far as it inherited (0, as a rule), so a subverted
// sandbox can undo it.
const OOMScoreAdj = 1000

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
// /run/user/<uid>, where systemd-logind makes it. An XDG_RUNTIME_DIR another uid owns is not agentd's
// (su without a login keeps the caller's): it is passed over for /run/user/<uid>, and passedOver says
// so, for an error that would otherwise name only the directory agentd looked in (Scopes.PassedOver).
func RuntimeDir() (dir, passedOver string) {
	d := os.Getenv("XDG_RUNTIME_DIR")
	fi, err := os.Stat(d)
	if filepath.IsAbs(d) && (err != nil || owner(fi) == os.Getuid()) {
		return d, ""
	}
	if filepath.IsAbs(d) {
		passedOver = fmt.Sprintf("agentd passed over XDG_RUNTIME_DIR=%s, which uid %d owns, not agentd's uid %d", d, owner(fi), os.Getuid())
	}
	return fmt.Sprintf("/run/user/%d", os.Getuid()), passedOver
}

// owner is the uid that owns the file; -1 where the file system does not say.
func owner(fi fs.FileInfo) int {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return int(st.Uid)
	}
	return -1
}

// Scopes are the scopes of one agentd start. Each is named agentos-<token>-<kind>-<n>.scope, with a
// token of the start's own: no name is one systemd still keeps from another agentd (a failed scope
// nobody cleared stays until reset-failed, and systemd refuses a second unit of the same name).
type Scopes struct {
	dir        string // XDG_RUNTIME_DIR, which systemctl and systemd-run find the user manager by
	passedOver string // an XDG_RUNTIME_DIR agentd did not take, and why (RuntimeDir)
	token      string
	run        Runner
	readFile   func(string) ([]byte, error) // reads /proc's and /sys/fs/cgroup's files
	probed     string                       // the cgroup of the last probe scope, a sibling of the runs' scopes
}

// New makes the scopes of an agentd start, whose user manager listens in dir, asked through run; read
// reads /proc's and /sys/fs/cgroup's files (the kernel's command line, the machine's memory, a
// launcher's cgroup and its limits): os.ReadFile, but in tests.
func New(dir string, run Runner, read func(string) ([]byte, error)) *Scopes {
	b := make([]byte, 8)
	_, _ = rand.Read(b) // never fails (crypto/rand)
	return &Scopes{dir: dir, token: hex.EncodeToString(b), run: run, readFile: read}
}

// PassedOver records why agentd did not take its own XDG_RUNTIME_DIR (RuntimeDir), for an error that
// finds no user manager where it looked instead.
func (s *Scopes) PassedOver(why string) *Scopes {
	s.passedOver = why
	return s
}

// Names is the pattern of the names of the start's scopes, for its log.
func (s *Scopes) Names() string { return "agentos-" + s.token + "-*.scope" }

// ResetFailed clears what systemd keeps of the start's scopes that ended failed, all at once: those
// OOMKilled had no word of in time. agentd calls it as it stops, once none of them runs. Its error
// says systemctl failed; a pattern that matches nothing is no error.
func (s *Scopes) ResetFailed() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := s.run(ctx, s.env(), "systemctl", "--user", "reset-failed", "--", "agentos-"+s.token+"-*")
	return err
}

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
				// Still running something, or stopping it: the Result may be the kill's already. Clear the
				// scope all the same, should it be failed by now (or not: Scopes.ResetFailed, at the end).
				reset, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, _ = s.run(reset, s.env(), "systemctl", "--user", "reset-failed", "--", unit)
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

// InScope checks that process pid, the launcher of one of the set's runs, is in unit, the run's scope,
// as limits.sh puts it there, and that the scope holds the set's limits. A launcher that does not
// source limits.sh (an older copy of run-*.sh, one of its own, a wrapper that drops AGENTOS_LIMIT_*)
// runs its sandbox in no scope and with no limits, while agentd.started says they hold; one that makes
// a scope of that name with other limits holds those. A scope that has ended holds nothing any more,
// and is no error: its directory is gone (a cgroup with a live process in it cannot be removed), and
// a process of it not reaped yet reads "<path> (deleted)". That mark is the kernel's only where no
// cgroup has that path: a launcher with a delegated subtree can make a cgroup so named, and move into
// it. nil when the limits are off, or the run has no scope.
func (t *Set) InScope(pid int, unit string) error {
	if t.scopes == nil || unit == "" {
		return nil
	}
	b, err := t.scopes.readFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return fmt.Errorf("cannot tell the cgroup of the process agentd started for the %s (pid %d): %w", t.kind, pid, err)
	}
	cgroup, err := unified(strings.Split(strings.TrimSpace(string(b)), "\n"))
	if err != nil {
		return fmt.Errorf("the process agentd started for the %s (pid %d): %w", t.kind, pid, err)
	}
	if ended, ok := strings.CutSuffix(cgroup, " (deleted)"); ok && t.gone("/sys/fs/cgroup"+cgroup) {
		cgroup = ended
	}
	if !strings.HasSuffix(cgroup, "/"+unit) {
		return fmt.Errorf("the process agentd started for the %s (pid %d) is in the cgroup %s, not in its scope %s: its launcher "+
			"does not source sandbox/linux/limits.sh, or a wrapper runs the launcher without exec", t.kind, pid, cgroup, unit)
	}
	dir := "/sys/fs/cgroup" + cgroup
	got := map[string]string{}
	for _, f := range limitFiles {
		if b, err := t.scopes.readFile(dir + "/" + f); err == nil {
			got[f] = strings.TrimSpace(string(b))
		}
	}
	// After its limits: a directory gone by now has ended, meanwhile perhaps, and lacks no limit.
	if t.gone(dir) {
		return nil
	}
	return t.held("the scope "+unit, got)
}

// gone reports whether the cgroup whose directory is dir has been removed, by its cgroup.procs: a read
// that finds no such file, or one that kernfs answers ENODEV, as it does an open or a read racing the
// removal (kernfs_get_active fails), which errors.Is(err, fs.ErrNotExist) does not count.
func (t *Set) gone(dir string) bool {
	_, err := t.scopes.readFile(dir + "/cgroup.procs")
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENODEV)
}

// unified is a process's cgroup on cgroup v2, from the lines of its /proc/<pid>/cgroup: the path of its
// 0:: line, which there must be. A cgroup v1 hierarchy that holds the memory or the pids controller
// takes them from v2, and the limits would not hold: refused. Other v1 hierarchies are no matter (some
// VPNs mount net_cls beside v2).
func unified(lines []string) (string, error) {
	path, found := "", 0
	for _, line := range lines {
		if p, ok := strings.CutPrefix(line, "0::"); ok {
			path, found = p, found+1
			continue
		}
		if f := strings.SplitN(line, ":", 3); len(f) == 3 {
			for c := range strings.SplitSeq(f[1], ",") {
				if c == "memory" || c == "pids" {
					return "", fmt.Errorf("a cgroup v1 hierarchy holds the %s controller (%q), which the limits need on cgroup v2", c, line)
				}
			}
		}
	}
	if found != 1 || !strings.HasPrefix(path, "/") {
		return "", fmt.Errorf("not in one cgroup of cgroup v2 (the unified hierarchy): %q", lines)
	}
	return path, nil
}

// kernelCharges refuses a kernel command line that keeps kernel memory, or sockets' buffers, from the
// cgroups' memory (cgroup.memory=nokmem, nosocket): the limits would not hold the memory charged to no
// process, which is what they are for.
func kernelCharges(cmdline string) error {
	for _, arg := range strings.Fields(cmdline) {
		v, ok := strings.CutPrefix(arg, "cgroup.memory=")
		if !ok {
			continue
		}
		for opt := range strings.SplitSeq(v, ",") {
			if opt == "nokmem" || opt == "nosocket" {
				return fmt.Errorf("the kernel's command line has cgroup.memory=%s: the cgroups' memory leaves out the kernel's (page "+
					"tables, pipe and socket buffers), and the limits would not hold it", v)
			}
		}
	}
	return nil
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
	cmdline, err := s.readFile("/proc/cmdline")
	if err != nil {
		return fmt.Errorf("%w: cannot read the kernel's command line: %w", ErrNotHeld, err)
	}
	if err := kernelCharges(string(cmdline)); err != nil {
		return fmt.Errorf("%w: %w%s", ErrNotHeld, err, orInsecureDev)
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
		case err != nil && strings.Contains(err.Error(), "OOMPolicy"):
			return fmt.Errorf("agentd could not start a systemd user scope to check that it holds the %s limits (it needs "+
				"systemd 253 or later, for OOMPolicy= on a scope): %w", t.kind, err)
		case err != nil:
			return fmt.Errorf("agentd could not start a systemd user scope to check that it holds the %s limits: %w", t.kind, err)
		}
		cgroup, err := t.heldIn(unit, out)
		if err != nil {
			return fmt.Errorf("%w: %w%s", ErrNotHeld, err, orInsecureDev)
		}
		s.probed = cgroup
	}
	return nil
}

// Room is the most memory the start's scopes can have between them: the machine's, or less where a
// cgroup above them holds less.
type Room struct {
	Bytes int64
	// From is where Bytes comes from: "MemTotal" (/proc/meminfo), or the cgroup above the scopes whose
	// memory.max it is.
	From string
}

// Room reads what the start's scopes can have between them: the machine's memory (MemTotal in
// /proc/meminfo), or the lowest memory.max of the cgroups above the last probe scope, up to the root,
// where that is less ("max" is no limit, nor is a cgroup without the file). Without a probe, the
// machine's. Its error says /proc/meminfo could not be read: Room does not guess.
func (s *Scopes) Room() (Room, error) {
	b, err := s.readFile("/proc/meminfo")
	if err != nil {
		return Room{}, fmt.Errorf("cannot read /proc/meminfo: %w", err)
	}
	r := Room{From: "MemTotal"}
	for line := range strings.SplitSeq(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "MemTotal:"); ok {
			if kb, err := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(v), "kB")), 10, 64); err == nil {
				r.Bytes = kb << 10
			}
		}
	}
	if r.Bytes <= 0 {
		return Room{}, errors.New("/proc/meminfo has no MemTotal agentd can read")
	}
	for dir := path.Dir(s.probed); s.probed != "" && dir != "/"; dir = path.Dir(dir) {
		b, err := s.readFile("/sys/fs/cgroup" + dir + "/memory.max")
		if err != nil {
			continue
		}
		if n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); err == nil && n < r.Bytes {
			r = Room{Bytes: n, From: dir}
		}
	}
	return r, nil
}

// orInsecureDev ends the refusals of limits that do not hold.
const orInsecureDev = "; or start agentd with --insecure-dev, which turns the limits off (development only)"

// noManager is ErrNoManager, with why and how to start one.
func (s *Scopes) noManager(why string) error {
	name := "<user>"
	if u, err := user.Current(); err == nil {
		name = u.Username
	}
	if s.passedOver != "" {
		why += "; " + s.passedOver
	}
	return fmt.Errorf("%w for uid %d at XDG_RUNTIME_DIR %s: %s. agentd runs the Worker and each git tool call in a systemd "+
		"user scope, which holds their memory and task limits, and the user's manager runs the scopes. A login session "+
		"starts one; to keep one without a session, enable lingering (sudo loginctl enable-linger %s). Or start agentd "+
		"with --insecure-dev, which turns the limits off (development only)", ErrNoManager, os.Getuid(), s.dir, why, name)
}

// heldIn checks a probe's report of unit: its cgroup on v2, the unit's, and each limit as t asks. It
// returns the cgroup.
func (t *Set) heldIn(unit string, out []byte) (string, error) {
	var cgroups []string
	got := map[string]string{}
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		k, v, _ := strings.Cut(line, " ")
		if k == "cgroup" {
			cgroups = append(cgroups, v)
		} else if v != "missing" {
			got[k] = v
		}
	}
	cgroup, err := unified(cgroups)
	if err != nil {
		return "", fmt.Errorf("the probe scope's process: %w", err)
	}
	if !strings.HasSuffix(cgroup, "/"+unit) {
		return "", fmt.Errorf("the probe's process is in the cgroup %s, not in its scope's (%s)", cgroup, unit)
	}
	return cgroup, t.held("the probe scope", got)
}

// limitFiles are a scope's files of the limits, under its cgroup's directory.
var limitFiles = []string{"memory.max", "memory.swap.max", "pids.max"}

// held checks what, a scope, by its limit files as read (got: none for a file not there): each as t asks.
func (t *Set) held(what string, got map[string]string) error {
	want := map[string]string{"memory.max": strconv.FormatInt(t.limits.Memory, 10), "memory.swap.max": "0",
		"pids.max": strconv.FormatInt(t.limits.Tasks, 10)}
	for _, f := range limitFiles {
		switch v, ok := got[f]; {
		case !ok:
			return fmt.Errorf("%s has no %s: is that controller delegated to the user manager? systemd delegates memory and "+
				"pids to user@<uid>.service by default (Delegate=), and "+
				"/sys/fs/cgroup/user.slice/user-%d.slice/user@%d.service/cgroup.controllers lists those it has", what, f, os.Getuid(), os.Getuid())
		case v == "max":
			return fmt.Errorf("%s's %s is max, not %s: the scope was not made as asked, or systemd could not set it (journalctl "+
				"--user)", what, f, want[f])
		case v != want[f]:
			return fmt.Errorf("%s's %s is %s, not %s", what, f, v, want[f])
		}
	}
	return nil
}
