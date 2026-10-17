#!/usr/bin/env bash
# Runs a command (the Worker) under sandbox-runtime (Seatbelt) on macOS, as section 8 of
# the design doc asks. macOS has no network namespaces, and allowLocalBinding would open
# every loopback port, so agentd serves the Runtime entry on a loopback port and the only
# destination the sandbox's proxy lets through is that one port. The Worker dials it with
# the SDK's HTTP CONNECT proxy support (--http-connect-proxy env).
#   - network: allowedDomains = ["127.0.0.1:<runtime port>"], allowLocalBinding = false
#   - files: the home directory, and any colon-separated paths in AGENTOS_SANDBOX_DENY_READ,
#     are unreadable except the Python environment and the task code; writes only to a
#     private temporary directory
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
here=$(cd "$(dirname "$0")" && pwd)
srt="$here/node_modules/.bin/srt"
[[ -x $srt ]] || { echo "$0: run 'npm ci' in $here first" >&2; exit 1; }
node_dir=$(dirname "$(command -v node)")

state=$(mktemp -d /tmp/agentos-sandbox.XXXXXX)
mkdir "$state/work"
# Built with node (which srt needs anyway) so that paths are JSON-escaped.
node -e '
  const [port, pyenv, code, home, work, deny] = process.argv.slice(1);
  console.log(JSON.stringify({
    network: { allowedDomains: [`127.0.0.1:${port}`], deniedDomains: [], allowLocalBinding: false },
    filesystem: {
      denyRead: [home, ...deny.split(":").filter(Boolean)],
      allowRead: [pyenv, code],
      allowWrite: [work],
      denyWrite: [],
    },
  }, null, 2));
' "$port" "$pyenv" "$code" "$HOME" "$state/work" "${AGENTOS_SANDBOX_DENY_READ:-}" > "$state/srt-settings.json"

exec env -i PATH="/usr/bin:/bin:$node_dir" HOME="$state/work" TMPDIR="$state/work" LANG=en_US.UTF-8 \
  PYTHONDONTWRITEBYTECODE=1 "$srt" --settings "$state/srt-settings.json" -- "$@"
