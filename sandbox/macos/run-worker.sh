#!/usr/bin/env bash
# Runs a command (the Worker) under sandbox-runtime (Seatbelt) on macOS, as section 8 of
# the design doc asks. macOS has no network namespaces, and allowLocalBinding would open
# every loopback port, so agentd serves the Runtime entry on a loopback port and the only
# destination the sandbox's proxy lets through is that one port. The Worker dials it with
# the SDK's HTTP CONNECT proxy support (--http-connect-proxy env).
#   - network: allowedDomains = ["127.0.0.1:<runtime port>"], allowLocalBinding = false
#   - files: the home directory (both $HOME and the one the user database names), and any
#     colon-separated paths in AGENTOS_SANDBOX_DENY_READ, are unreadable except the Python
#     environment and the task code; writes only to a private temporary directory. Everything
#     else can be read, so agentd's data directory and mirrors belong under the home directory
#     or in AGENTOS_SANDBOX_DENY_READ. Seatbelt matches real paths, and srt keeps a symlink's
#     spelling when it leads elsewhere, so each denied path goes in as given and as its real
#     path; agentd's default data directory, ~/.agentos, is denied wherever it really is (as
#     resolved when the Worker starts). The launcher refuses a Python environment or code
#     directory that holds dev data, which the Worker could read (agentd's ledger, a .agentos
#     directory, Temporal's dev database), or a FIFO, which it could read from. It looks when
#     it starts the Worker. Unix sockets there need no such check: srt lets it connect to none
#   - environment: cleared, except what srt adds (its proxy variables)
#
# Usage: run-worker.sh <runtime-port> <python-env-dir> <code-dir> -- <command> [args...]
set -euo pipefail

if [[ $# -lt 5 || $4 != "--" ]]; then
  echo "usage: $0 <runtime-port> <python-env-dir> <code-dir> -- <command> [args...]" >&2
  exit 2
fi
port=$1 pyenv=$2 code=$3
shift 4
[[ $port =~ ^[0-9]{1,5}$ ]] || { echo "$0: bad port $port" >&2; exit 2; }
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

state=$(mktemp -d /tmp/agentos-sandbox.XXXXXX)
mkdir "$state/work"
# Built with node (which srt needs anyway) so that paths are JSON-escaped.
node -e '
  const fs = require("fs"), path = require("path");
  const [port, pyenv, code, home, work, deny] = process.argv.slice(1);
  const real = p => {
    try { return fs.realpathSync(p); } catch {
      const dir = path.dirname(p);
      return dir === p ? p : path.join(real(dir), path.basename(p));
    }
  };
  const homes = [home, require("os").userInfo().homedir];
  const denied = [...homes, ...homes.map(h => path.join(h, ".agentos")), ...deny.split(":").filter(Boolean)];
  console.log(JSON.stringify({
    network: { allowedDomains: [`127.0.0.1:${port}`], deniedDomains: [], allowLocalBinding: false },
    filesystem: {
      denyRead: [...new Set(denied.flatMap(p => [p, real(p)]))],
      allowRead: [pyenv, code],
      allowWrite: [work],
      denyWrite: [],
    },
  }, null, 2));
' "$port" "$pyenv" "$code" "$HOME" "$state/work" "${AGENTOS_SANDBOX_DENY_READ:-}" > "$state/srt-settings.json"

exec env -i PATH="/usr/bin:/bin:$node_dir" HOME="$state/work" TMPDIR="$state/work" LANG=en_US.UTF-8 \
  PYTHONDONTWRITEBYTECODE=1 "$srt" --settings "$state/srt-settings.json" -- "$@"
