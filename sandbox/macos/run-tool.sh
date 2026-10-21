#!/usr/bin/env bash
# Runs a tool server (agentos-mcp-git) under sandbox-runtime (Seatbelt) on macOS, once per
# call, as section 8 of the design doc asks:
#   - network: none. No destination is allowed and nothing may listen.
#   - files: the home directory, the shared temporary directories (/private/tmp, where other
#     calls and the Worker keep their state, /private/var/tmp and /private/var/folders), and the paths in
#     AGENTOS_SANDBOX_DENY_READ, a JSON array (agentd passes its data directory, its sockets'
#     directories, the Temporal client certificate and key, and every mirror), are unreadable,
#     except the call's mirror, the server binary and the call's work directory. (Not the
#     server's whole directory: srt lets a deny inside an allowed directory win, so a mirror
#     next to the binary would be unreadable even to its own call.) Writes go only to the
#     work directory: not even to /tmp/claude, which srt otherwise always allows.
#   - environment: cleared
#   - limits: 30 s of CPU per process, so that a commit too costly to diff is refused the same
#     way every time (git dies of SIGXCPU; 40 s is the hard limit, for a process that ignores
#     the signal), and 64 MiB per file written. Nothing caps memory on macOS.
# agentd makes a directory for each call, passes it as AGENTOS_SANDBOX_STATE, and removes it
# after the call however the server ended.
#
# Usage: run-tool.sh <mirror> -- <server> [args...]
set -euo pipefail

if [[ $# -lt 3 || $2 != "--" ]]; then
  echo "usage: $0 <mirror> -- <server> [args...]" >&2
  exit 2
fi
[[ -d $1 ]] || { echo "$0: $1 is not a directory" >&2; exit 2; }
mirror=$(cd "$1" && pwd -P) # Seatbelt matches the real path
shift 2
state=${AGENTOS_SANDBOX_STATE:?agentd sets AGENTOS_SANDBOX_STATE}
here=$(cd "$(dirname "$0")" && pwd)
srt="$here/node_modules/.bin/srt"
[[ -x $srt ]] || { echo "$0: run 'npm ci' in $here first" >&2; exit 1; }
node_dir=$(dirname "$(command -v node)")
# The real git, not the /usr/bin shim: the shim's xcrun writes a cache outside the sandbox.
git_dir=$(dirname "$(xcrun --find git 2>/dev/null || command -v git)")

mkdir -p "$state/work"
work=$(cd "$state/work" && pwd -P)
# Built with node (which srt needs anyway) so that paths are JSON-escaped. The home directory
# comes from the user database, not $HOME, which agentd points at the call's own directory.
# Seatbelt matches real paths, so each denied path goes in as given and as its real path too
# (/tmp/x is /private/tmp/x), resolved as far as it exists.
node -e '
  const fs = require("fs"), path = require("path");
  const [mirror, server, work, deny] = process.argv.slice(1);
  const denied = [require("os").userInfo().homedir, ...JSON.parse(deny || "[]")];
  const real = p => {
    try { return fs.realpathSync(p); } catch {
      const dir = path.dirname(p);
      return dir === p ? p : path.join(real(dir), path.basename(p));
    }
  };
  console.log(JSON.stringify({
    network: { allowedDomains: [], deniedDomains: [], allowLocalBinding: false },
    filesystem: {
      denyRead: [...new Set(["/private/tmp", "/private/var/tmp", "/private/var/folders",
        ...denied.flatMap(p => [p, real(p)])])],
      allowRead: [...new Set([mirror, server, real(server), work])],
      allowWrite: [work],
      denyWrite: ["/tmp/claude", "/private/tmp/claude"],
    },
  }, null, 2));
' "$mirror" "$1" "$work" "${AGENTOS_SANDBOX_DENY_READ:-}" > "$state/srt-settings.json"

ulimit -S -t 30 && ulimit -H -t 40 # soft first: it may not exceed the hard limit
ulimit -f $((64 * 1024))            # KiB
# srt sets TMPDIR to a directory of its own; AGENTOS_TMPDIR names the one the server may write.
exec env -i PATH="$git_dir:/usr/bin:/bin:$node_dir" HOME="$work" TMPDIR="$work" \
  AGENTOS_TMPDIR="$work" LANG=en_US.UTF-8 "$srt" --settings "$state/srt-settings.json" -- "$@"
