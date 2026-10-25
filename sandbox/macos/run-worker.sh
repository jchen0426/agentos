#!/usr/bin/env bash
# Runs a command (the Worker) under sandbox-runtime (Seatbelt) on macOS, as section 8 of
# the design doc asks. macOS has no network namespaces, and allowLocalBinding would open
# every loopback port, so agentd serves the Runtime entry on a loopback port and the only
# destination the sandbox's proxy lets through is that one port. The Worker dials it with
# the SDK's HTTP CONNECT proxy support (--http-connect-proxy env).
#   - network: allowedDomains = ["127.0.0.1:<runtime port>"], allowLocalBinding = false
#   - files: the home directory (both $HOME and the one the user database names), the shared
#     temporary directories (/private/tmp, where every launch and every tool call keeps its state
#     and other programs their files, /private/var/tmp and /private/var/folders), and the paths in
#     AGENTOS_SANDBOX_DENY_READ, a JSON array, are unreadable except the Python environment, the
#     task code and the Worker's private directory. Everything else can be read, so agentd's
#     data directory and mirrors belong under the home directory or in AGENTOS_SANDBOX_DENY_READ.
#     Seatbelt matches real paths, and srt keeps a symlink's spelling when it leads elsewhere, so
#     each path goes in as given and as its real path; agentd's default data directory,
#     ~/.agentos, is denied wherever it really is (as resolved when the Worker starts). The
#     launcher refuses a Python environment or code directory that holds dev data, which the
#     Worker could read (agentd's ledger, a .agentos directory, Temporal's dev database), or a
#     FIFO, which it could read from. It looks when it starts the Worker. Unix sockets there
#     need no such check: srt lets it connect to none
#   - writes: only to a private directory, the Worker's HOME and TMPDIR. Not to /tmp/claude,
#     which srt otherwise always allows and hands out as TMPDIR, to everything it runs
#   - state: each launch has a directory for srt's settings, srt's own sockets and the Worker's
#     private directory. A Worker can read none of them but its own private directory, as it
#     reads nothing else in /tmp. agentd makes it, /tmp/agentos-worker.<agentd's pid>.*, passes it
#     as AGENTOS_SANDBOX_STATE, and removes it once the launch's whole process group is gone,
#     however it ended (the next agentd sweeps what a killed one left). Without it, this script
#     makes /tmp/agentos-sandbox.*, which goes when srt does, however srt ends (kill -9
#     included), with one exception: a kill of srt's whole process group (or SIGKILL/SIGSTOP)
#     takes the remover too, so the directory stays, still out of every Worker's reach. The
#     remover (rm-state.py) walks fd-relative, never following a symlink, so untrusted task code
#     cannot turn it into a delete of anything outside the state directory
#   - lifeline: agentd passes fd 3, the read end of a pipe whose write end only agentd holds, and
#     AGENTOS_SANDBOX_LIFELINE=3. macOS has no parent-death signal: a Worker that outlived a killed
#     agentd would reach the next agentd on the same port, a second Worker beside its own. A watcher
#     outside the sandbox, in this process group (srt lets the Worker signal nothing outside its
#     sandbox), reads fd 3; at EOF, agentd is gone, however it ended, and the watcher kills the group.
#     That, and agentd's sweep of worker.pid (by group too), clean up after an honest Worker; they do
#     not contain a subverted one, which can fork a child that leaves the group (setsid, or a double
#     fork): still sandboxed, it could reach the next agentd's port. Not fixed here: C3b's ulimit -u
#     (no fork), or a per-generation credential at the Runtime entry, would close it
#   - stdin: agentd's stop channel, a pipe the Worker reads (--stop-on-stdin-eof) and stops at its end.
#     This script and srt hand it on as it is (srt's own SIGTERM handling would drop the Worker's
#     connections before it passed the signal on)
#   - controlling terminal: none. srt lets what it runs ioctl /dev/tty, so a Worker in a session
#     with a terminal could type into it (TIOCSTI). This script refuses to run with one: agentd
#     starts it in a session of its own; by hand, see the README
#   - environment: cleared, except what srt adds (its proxy variables, TMPDIR) and
#     CLAUDE_CODE_TMPDIR, which srt hands out as TMPDIR
#
# Usage: run-worker.sh <runtime-port> <python-env-dir> <code-dir> -- <command> [args...]
set -euo pipefail

# A caller that started us with stdin, stdout or stderr closed would have bash hand that low
# descriptor to the control pipe below, and the sandboxed Worker would inherit it. Keep 0-2 open.
{ : <&0; } 2>/dev/null || exec 0</dev/null
{ : >&1; } 2>/dev/null || exec 1>/dev/null
{ : >&2; } 2>/dev/null || exec 2>/dev/null

if [[ $# -lt 5 || $4 != "--" ]]; then
  echo "usage: $0 <runtime-port> <python-env-dir> <code-dir> -- <command> [args...]" >&2
  exit 2
fi
port=$1 pyenv=$2 code=$3
shift 4
[[ $port =~ ^[0-9]{1,5}$ ]] || { echo "$0: bad port $port" >&2; exit 2; }
# Opening /dev/tty succeeds only for a process with a controlling terminal.
if { : </dev/tty; } 2>/dev/null; then
  echo "$0: this has a controlling terminal, which the sandboxed Worker could type into (TIOCSTI):" \
    "run the Worker through agentd, or by hand in a session of its own (see the README)" >&2
  exit 2
fi
# -H: a directory given as a symlink is looked into. A scan fails when a directory goes away
# under it (npm ci in the code directory, say): try again.
scan() {
  find -H "$1" \( -type p -o -name 'ledger.db*' -o -name 'temporal.db*' -o -name .agentos \) -print -quit
}
for dir in "$pyenv" "$code"; do
  found=$(scan "$dir" 2>/dev/null) || found=$(scan "$dir" 2>/dev/null) || found=$(scan "$dir") ||
    { echo "$0: cannot look through $dir" >&2; exit 1; }
  if [[ -p $found ]]; then
    echo "$0: $dir holds $found, a FIFO the Worker could read from" >&2
    exit 1
  elif [[ -n $found ]]; then
    echo "$0: $dir holds $found, agentd's or Temporal's data, which the Worker could read" >&2
    exit 1
  fi
done
here=$(cd "$(dirname "$0")" && pwd)
srt="$here/node_modules/.bin/srt"
[[ -x $srt ]] || { echo "$0: run 'npm ci' in $here first" >&2; exit 1; }
node_dir=$(dirname "$(command -v node)")
# The Python that runs rm-state.py when this launch ends. The pyenv's own python is the
# Worker's, so it is always present; fall back to one on PATH. rm-state.py needs only os.open
# with dir_fd and O_NOFOLLOW, which every 3.x has — that is what makes the removal symlink-safe.
# With neither python the state directory is left in place (read-denied to every Worker) rather
# than removed unsafely.
rmstate="$pyenv/bin/python3"
[[ -x $rmstate ]] || rmstate=$(command -v python3 2>/dev/null || true)

if [[ -n ${AGENTOS_SANDBOX_STATE:-} ]]; then
  # agentd's: made for this launch, and removed by agentd once the launch's process group is gone.
  state=$AGENTOS_SANDBOX_STATE
  [[ -d $state && ! -L $state ]] || { echo "$0: AGENTOS_SANDBOX_STATE is not a directory: $state" >&2; exit 2; }
else
  state=$(mktemp -d /tmp/agentos-sandbox.XXXXXX)
  # Until srt is exec'd there is no sandboxed process yet, so a plain rm is safe here; this guards
  # the window before the reader below is in charge (a signal, or a failure building the settings).
  # exec'ing srt replaces this shell, so the trap does not fire once srt is running.
  trap 'rm -rf "$state"' EXIT
  trap 'exit 1' HUP INT QUIT TERM
  # srt holds fd 9 (the write end of a pipe) until it ends, whatever ends it, and hands it to
  # nothing it starts (libuv spawns with POSIX_SPAWN_CLOEXEC_DEFAULT on macOS). The reader runs
  # outside the Worker's process tree (whoever stops a Worker kills that tree) and, at EOF, removes
  # the state directory with rm-state.py (fd-relative, never follows a symlink). It ignores the
  # job-control and terminal signals it can; a kill of its whole process group still takes it.
  exec 9> >( (trap '' INT QUIT TSTP TTIN TTOU TERM HUP USR1 USR2
    while read -r _; do :; done
    [[ -n $rmstate ]] && "$rmstate" -I "$here/rm-state.py" "$state") <&0 >/dev/null 2>&1 & )
fi
mkdir "$state/work"
# Built with node (which srt needs anyway) so that paths are JSON-escaped.
node -e '
  const fs = require("fs"), path = require("path");
  const [port, pyenv, code, home, state, deny] = process.argv.slice(1);
  const work = path.join(state, "work");
  const real = p => {
    try { return fs.realpathSync(p); } catch {
      const dir = path.dirname(p);
      return dir === p ? p : path.join(real(dir), path.basename(p));
    }
  };
  const homes = [home, require("os").userInfo().homedir];
  const listed = JSON.parse(deny || "[]");
  if (!Array.isArray(listed) || !listed.every(p => typeof p === "string")) {
    throw new Error("AGENTOS_SANDBOX_DENY_READ must be a JSON array of paths");
  }
  // The shared temporary directories hold the state of every launch, this one included:
  // allowRead wins for what is inside, so the Worker reads its own private directory there and
  // nothing else.
  const denied = ["/private/tmp", "/private/var/tmp", "/private/var/folders", ...homes,
    ...homes.map(h => path.join(h, ".agentos")), ...listed];
  const both = paths => [...new Set(paths.flatMap(p => [p, real(p)]))];
  console.log(JSON.stringify({
    network: { allowedDomains: [`127.0.0.1:${port}`], deniedDomains: [], allowLocalBinding: false },
    filesystem: {
      denyRead: both(denied),
      allowRead: both([pyenv, code, work]),
      allowWrite: [work],
      denyWrite: ["/tmp/claude", "/private/tmp/claude"],
    },
  }, null, 2));
' "$port" "$pyenv" "$code" "$HOME" "$state" "${AGENTOS_SANDBOX_DENY_READ:-}" > "$state/srt-settings.json"

if [[ -n ${AGENTOS_SANDBOX_LIFELINE:-} ]]; then
  [[ $AGENTOS_SANDBOX_LIFELINE == 3 ]] && { : <&3; } 2>/dev/null ||
    { echo "$0: AGENTOS_SANDBOX_LIFELINE must be 3, an open descriptor" >&2; exit 2; }
  # This shell becomes srt, and leads the process group agentd made it (a session's): at EOF the
  # watcher kills it all, srt, the Worker and itself. It ignores the signals it can, as the remover.
  group=$$
  (trap '' INT QUIT TSTP TTIN TTOU TERM HUP USR1 USR2
    cat >/dev/null
    kill -KILL -- "-$group") <&3 >/dev/null 2>&1 &
  exec 3<&- # srt and the Worker hold nothing of it
fi
# srt keeps its own temporary files (its sockets) in the state directory, out of the Worker's
# reach, and hands the Worker CLAUDE_CODE_TMPDIR as TMPDIR.
exec env -i PATH="/usr/bin:/bin:$node_dir" HOME="$state/work" TMPDIR="$state" CLAUDE_CODE_TMPDIR="$state/work" \
  LANG=en_US.UTF-8 PYTHONDONTWRITEBYTECODE=1 "$srt" --settings "$state/srt-settings.json" -- "$@"
