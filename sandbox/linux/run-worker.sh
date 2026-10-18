#!/usr/bin/env bash
# Runs a command (the Worker) in a bubblewrap sandbox, as section 8 of the design doc asks:
#   - an empty network namespace (--unshare-net): only the sandbox's own loopback exists,
#     so neither the host's Temporal port nor anything else on the network is reachable;
#   - allowlisted mounts only: /usr read-only, the Python environment and the task code
#     read-only. The home directory and the host's /run (D-Bus session bus, ssh-agent,
#     docker.sock) are never mounted. The Worker sees all of the two directories it is given,
#     so the launcher refuses either if it holds a Unix socket, which a read-only mount does not
#     stop the Worker from connecting to, or agentd's data directory (its ledger, and by default
#     its Control socket). It looks when it starts the Worker; what appears there later, the
#     Worker sees too;
#   - no file can be created anywhere: the Worker writes none (docs/m1-notes.md). The root and
#     /dev, tmpfs mounts bwrap makes as large as half the memory, are read-only; /tmp and /run
#     are only directories in the root. A tmpfs's data, inodes and extended attributes are
#     charged to no process, so the OOM killer would kill other processes, not a Worker that
#     filled one (--size bounds the data only). Without a writable /dev/shm, POSIX semaphores
#     and shared memory (multiprocessing) fail with EROFS;
#   - no further user namespaces (--disable-userns): in one, the Worker could mount a tmpfs of
#     its own;
#   - --clearenv: no inherited environment variables;
#   - the Runtime socket is the only way out: socat inside the sandbox forwards
#     127.0.0.1:7233 to it, because the Temporal SDK only dials host:port. Its directory is
#     mounted rather than the socket file: agentd re-creates the socket when it restarts,
#     and a mounted file would keep pointing at the old one. So that directory must hold
#     nothing but the socket.
# Unlike run-tool.sh, no ulimit. -v bounds one process, and the Worker can fork; its address
# space also grows with the CPU count (glibc gives the SDK's threads malloc arenas). -f would
# kill the Worker once a log file its output goes to grew past the limit. That output is the
# one thing it can write without bound: give it a pipe or journald, not a plain file. Memory
# charged to no process remains (memfd, SysV IPC, pipe and socket buffers); bounding it takes
# a cgroup (docs/m1-notes.md).
#
# Usage: run-worker.sh <runtime-socket> <python-env-dir> <code-dir> -- <command> [args...]
# Ubuntu 23.10+ also needs an AppArmor profile that grants bwrap user namespaces
# (see vm/lima/agentos.yaml).
set -euo pipefail

if [[ $# -lt 5 || $4 != "--" ]]; then
  echo "usage: $0 <runtime-socket> <python-env-dir> <code-dir> -- <command> [args...]" >&2
  exit 2
fi
runtime_sock=$1 pyenv=$2 code=$3
shift 4
[[ -S $runtime_sock ]] || { echo "$0: $runtime_sock is not a socket" >&2; exit 1; }
command -v socat >/dev/null || { echo "$0: socat is not installed" >&2; exit 1; }
runtime_dir=$(dirname "$runtime_sock")
sock_name=$(basename "$runtime_sock")
extra=$(find "$runtime_dir" -mindepth 1 -maxdepth 1 ! -name "$sock_name" -print -quit)
[[ -z $extra ]] || { echo "$0: $runtime_dir must hold only the Runtime socket, found $extra" >&2; exit 1; }
for dir in "$pyenv" "$code"; do
  found=$(find -H "$dir" \( -type s -o -name ledger.db \) -print -quit) ||
    { echo "$0: cannot look through $dir" >&2; exit 1; }
  if [[ -S $found ]]; then
    echo "$0: $dir holds the Unix socket $found, which the Worker could connect to" >&2
    exit 1
  elif [[ -n $found ]]; then
    echo "$0: $dir holds agentd's data directory, whose ledger ($found) the Worker could read" >&2
    exit 1
  fi
done

inner_sock=/run/agentos/$sock_name
args=(
  --unshare-all --unshare-user --disable-userns --die-with-parent --new-session --clearenv
  --setenv PATH /usr/bin:/bin --setenv HOME /tmp --setenv LANG C.UTF-8
  --setenv PYTHONDONTWRITEBYTECODE 1
  --ro-bind /usr /usr --symlink usr/bin /bin --symlink usr/sbin /sbin --symlink usr/lib /lib
  --proc /proc --dev /dev --dir /tmp
  --ro-bind "$pyenv" "$pyenv"
  --ro-bind "$code" "$code" --chdir "$code"
  --ro-bind "$runtime_dir" /run/agentos
)
[[ -e /usr/lib64 ]] && args+=(--symlink usr/lib64 /lib64)
args+=(--remount-ro /dev --remount-ro /) # last: the mounts above need their mount points made first

# Inside: start socat, wait until it listens (127.0.0.1:7233 is 0100007F:1C41 in
# /proc/net/tcp, state 0A is LISTEN), then hand over to the command.
exec bwrap "${args[@]}" -- /bin/sh -c '
  socat TCP-LISTEN:7233,bind=127.0.0.1,reuseaddr,fork UNIX-CONNECT:'"$inner_sock"' &
  until grep -q " 0100007F:1C41 00000000:0000 0A " /proc/net/tcp; do sleep 0.05; done
  exec "$@"' sh "$@"
