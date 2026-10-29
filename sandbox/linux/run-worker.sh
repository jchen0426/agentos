#!/usr/bin/env bash
# Runs a command (the Worker) in a bubblewrap sandbox, as section 8 of the design doc asks:
#   - an empty network namespace (--unshare-net): only the sandbox's own loopback exists,
#     so neither the host's Temporal port nor anything else on the network is reachable;
#   - allowlisted mounts only: /usr read-only, the Python environment and the task code
#     read-only. The home directory and the host's /run (D-Bus session bus, ssh-agent,
#     docker.sock) are never mounted. The Worker sees all of the two directories it is given,
#     so the launcher refuses either if it holds a way out, a Unix socket or a FIFO (a read-only
#     mount stops neither connect() nor a write to a FIFO), or dev data: agentd's ledger, and by
#     default its Control socket, a .agentos directory, Temporal's dev database. It looks when it
#     starts the Worker; what appears there later, the Worker sees too;
#   - no file can be created anywhere: the Worker writes none (docs/m1-notes.md). The root, a
#     tmpfs bwrap makes as large as half the memory, is read-only; /tmp, /run and /dev are only
#     directories in it. A tmpfs's data, inodes and extended attributes are charged to no
#     process (only to the cgroup, below), so the OOM killer would kill other processes, not a
#     Worker that filled one (--size bounds the data only). Without a writable /dev/shm, POSIX
#     semaphores and shared memory (multiprocessing) fail with EROFS;
#   - /dev holds five device nodes, null, zero, full, random and urandom: no terminal and no
#     pseudo-terminals, whose number this machine's sandboxes share;
#   - no further user namespaces (--disable-userns): in one, the Worker could mount a tmpfs of
#     its own;
#   - --clearenv: no inherited environment variables;
#   - the Runtime socket is the only way out: socat inside the sandbox forwards
#     127.0.0.1:7233 to it, because the Temporal SDK only dials host:port. Its directory is
#     mounted rather than the socket file: agentd re-creates the socket when it restarts,
#     and a mounted file would keep pointing at the old one. So that directory must hold
#     nothing but the socket, also when it is reached through a symlink;
#   - resource limits (limits.sh, sourced first): each generation runs in a systemd user scope of
#     its own, a cgroup that holds all the memory its processes use, the memory charged to no
#     process among it (memfd, SysV IPC, pipe and socket buffers, tmpfs: docs/m1-notes.md), with
#     no swap, and its tasks, processes and threads together, as worker.limits says (1 GiB and
#     512 by default). Past the memory the kernel kills the Worker; agentd records that, and
#     starts it again. The Worker gets TOKIO_WORKER_THREADS=4: the Temporal SDK's core starts a
#     thread for each CPU, and the task limit counts threads (the Worker had at most 23 on 4 CPUs
#     over the bwrap suite; on some 390 CPUs it would go past 512).
# Unlike run-tool.sh, no ulimit. -v bounds one process, and the Worker can fork; its address
# space also grows with the CPU count (glibc gives the SDK's threads malloc arenas): the cgroup
# bounds what it uses instead. -f would kill the Worker once a log file its output goes to grew
# past the limit. That output is the one thing it can write without bound: give it a pipe or
# journald, not a plain file.
#
# agentd starts this itself (agentd.yaml's worker section), in a session of its own, and starts it
# again whenever it exits. Its stdin is agentd's stop channel, which bwrap hands the Worker as it is:
# the Worker stops at its end (bwrap would die of SIGTERM, the sandbox with it). bwrap's
# --die-with-parent ends the sandbox when agentd dies, however it ends; --new-session keeps the Worker
# off any terminal. AGENTOS_SANDBOX_DENY_READ means nothing here:
# the sandbox mounts only what it is given.
#
# Usage: run-worker.sh <runtime-socket> <python-env-dir> <code-dir> -- <command> [args...], with the
# limits in the environment (limits.sh says which), or AGENTOS_LIMITS=off.
# Ubuntu 23.10+ also needs an AppArmor profile that grants bwrap user namespaces
# (see vm/lima/agentos.yaml). The limits need a systemd user manager: a login session, or lingering.
set -euo pipefail

if [[ $# -lt 5 || $4 != "--" ]]; then
  echo "usage: $0 <runtime-socket> <python-env-dir> <code-dir> -- <command> [args...]" >&2
  exit 2
fi
# shellcheck source=limits.sh
. "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/limits.sh" # in its scope from here on
runtime_sock=$1 pyenv=$2 code=$3
shift 4
[[ -S $runtime_sock ]] || { echo "$0: $runtime_sock is not a socket" >&2; exit 1; }
command -v socat >/dev/null || { echo "$0: socat is not installed" >&2; exit 1; }
runtime_dir=$(dirname "$runtime_sock")
sock_name=$(basename "$runtime_sock")
extra=$(find -H "$runtime_dir" -mindepth 1 -maxdepth 1 ! -name "$sock_name" -print -quit)
[[ -z $extra ]] || { echo "$0: $runtime_dir must hold only the Runtime socket, found $extra" >&2; exit 1; }
# -H: a directory given as a symlink is looked into, as bwrap mounts what it points to. A scan
# fails when a directory goes away under it (npm ci in the code directory, say): try again.
scan() {
  find -H "$1" \( -type s -o -type p -o -name 'ledger.db*' -o -name 'temporal.db*' -o -name .agentos \) -print -quit
}
for dir in "$pyenv" "$code"; do
  found=$(scan "$dir" 2>/dev/null) || found=$(scan "$dir" 2>/dev/null) || found=$(scan "$dir") ||
    { echo "$0: cannot look through $dir" >&2; exit 1; }
  if [[ -S $found || -p $found ]]; then
    echo "$0: $dir holds $found, a way out of the sandbox that a read-only mount does not close" >&2
    exit 1
  elif [[ -n $found ]]; then
    echo "$0: $dir holds $found, agentd's or Temporal's data, which the Worker could read" >&2
    exit 1
  fi
done

inner_sock=/run/agentos/$sock_name
# Of the host's files the sandbox has only /usr (and the links to it), the Python environment, the
# code directory and the Runtime socket's directory. agentd mirrors the /usr lines in workerVisible
# (cmd/agentd/worker.go), to copy only what the Worker can read: change them together.
args=(
  --unshare-all --unshare-user --disable-userns --die-with-parent --new-session --clearenv
  --setenv PATH /usr/bin:/bin --setenv HOME /tmp --setenv LANG C.UTF-8
  --setenv PYTHONDONTWRITEBYTECODE 1 --setenv TOKIO_WORKER_THREADS 4
  --ro-bind /usr /usr --symlink usr/bin /bin --symlink usr/sbin /sbin --symlink usr/lib /lib
  --proc /proc --dir /tmp --dir /dev/shm
)
for node in null zero full random urandom; do
  args+=(--dev-bind "/dev/$node" "/dev/$node")
done
args+=(
  --ro-bind "$pyenv" "$pyenv"
  --ro-bind "$code" "$code" --chdir "$code"
  --ro-bind "$runtime_dir" /run/agentos
)
[[ -e /usr/lib64 ]] && args+=(--symlink usr/lib64 /lib64)
args+=(--remount-ro /) # last: the mounts above need their mount points made first

# Inside: start socat, wait until it listens (127.0.0.1:7233 is 0100007F:1C41 in
# /proc/net/tcp, state 0A is LISTEN), then hand over to the command.
exec bwrap "${args[@]}" -- /bin/sh -c '
  socat TCP-LISTEN:7233,bind=127.0.0.1,reuseaddr,fork UNIX-CONNECT:'"$inner_sock"' &
  until grep -q " 0100007F:1C41 00000000:0000 0A " /proc/net/tcp; do sleep 0.05; done
  exec "$@"' sh "$@"
