#!/usr/bin/env bash
# Runs a tool server (agentos-mcp-git) in a bubblewrap sandbox, once per call, as section 8 of
# the design doc asks:
#   - an empty network namespace (--unshare-net), with nothing forwarded into it: no network
#     at all;
#   - allowlisted mounts only: /usr read-only (git and the C library), the server binary and
#     the one mirror the call needs, read-only; a private /tmp of at most 64 MiB. The home
#     directory, /run and agentd's data are never mounted. The root (a tmpfs bwrap makes, as
#     large as half the memory) is read-only: /tmp is the only place to write. /dev holds five
#     device nodes, null, zero, full, random and urandom: no terminal and no pseudo-terminals,
#     whose number this machine's sandboxes share;
#   - --clearenv: no inherited environment variables;
#   - no further user namespaces (--disable-userns);
#   - per process: at most 4 GiB of address space (a small crafted repository can make git ask
#     for a lot of memory), 30 s of CPU (a commit too costly to diff is refused the same way
#     every time: git dies of SIGXCPU; 40 s is the hard limit, where Linux sends SIGKILL
#     instead) and 64 MiB per file written;
#   - for the call as a whole (limits.sh, sourced first): a systemd user scope of its own, a
#     cgroup that holds all the memory its processes use, the kernel's for them and the private
#     /tmp among it, with no swap, and its tasks, processes and threads together, as
#     git_tools.limits says (512 MiB and 128 by default). Past the memory the kernel kills the
#     call's largest process (git, as a rule) and systemd the rest; agentd runs the call once more,
#     and refuses it if that is killed too, as it refuses one past the CPU time. The server gets
#     GOMAXPROCS=4: Go starts a thread for each CPU it may use, and the task limit counts threads (a
#     show_diff took 9 tasks with 4, 134 with GOMAXPROCS=512, past the 128 it may have).
#
# Usage: run-tool.sh <mirror> -- <server> [args...], with the limits in the environment
# (limits.sh says which), or AGENTOS_LIMITS=off.
# Ubuntu 23.10+ also needs an AppArmor profile that grants bwrap user namespaces
# (see vm/lima/agentos.yaml). The limits need a systemd user manager: a login session, or lingering.
set -euo pipefail

if [[ $# -lt 3 || $2 != "--" ]]; then
  echo "usage: $0 <mirror> -- <server> [args...]" >&2
  exit 2
fi
# shellcheck source=limits.sh
. "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/limits.sh" # in its scope from here on
# Both are mounted at the paths they are named by, whatever symlinks those go through.
mirror=$1
shift 2
[[ -d $mirror ]] || { echo "$0: $mirror is not a directory" >&2; exit 2; }
server=$1
[[ -x $server ]] || { echo "$0: $server is not executable" >&2; exit 2; }
shift

args=(
  --unshare-all --unshare-user --disable-userns --die-with-parent --new-session --clearenv
  --setenv PATH /usr/bin:/bin --setenv HOME /tmp --setenv LANG C.UTF-8 --setenv AGENTOS_TMPDIR /tmp
  --setenv GOMAXPROCS 4
  --ro-bind /usr /usr --symlink usr/bin /bin --symlink usr/sbin /sbin --symlink usr/lib /lib
  --proc /proc --size 67108864 --tmpfs /tmp
  --ro-bind "$(readlink -f "$mirror")" "$mirror"
  --ro-bind "$(readlink -f "$server")" "$server"
)
for node in null zero full random urandom; do
  args+=(--dev-bind "/dev/$node" "/dev/$node")
done
[[ -e /usr/lib64 ]] && args+=(--symlink usr/lib64 /lib64)
args+=(--remount-ro /) # last: the mounts above need their mount points made first

ulimit -v $((4 * 1024 * 1024)) # KiB
ulimit -S -t 30 && ulimit -H -t 40 # soft first: it may not exceed the hard limit
ulimit -f $((64 * 1024))            # KiB
exec bwrap "${args[@]}" -- "$server" "$@"
