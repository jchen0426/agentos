#!/usr/bin/env bash
# Runs a command (the Worker) in a bubblewrap sandbox, as section 8 of the design doc asks:
#   - an empty network namespace (--unshare-net): only the sandbox's own loopback exists,
#     so neither the host's Temporal port nor anything else on the network is reachable;
#   - allowlisted mounts only: /usr read-only, the Python environment and the task code
#     read-only, a private /tmp. The home directory and /run (D-Bus session bus,
#     ssh-agent, docker.sock) are never mounted;
#   - --clearenv: no inherited environment variables;
#   - the Runtime socket is the only way out: socat inside the sandbox forwards
#     127.0.0.1:7233 to it, because the Temporal SDK only dials host:port. Its directory is
#     mounted rather than the socket file: agentd re-creates the socket when it restarts,
#     and a mounted file would keep pointing at the old one. So that directory must hold
#     nothing but the socket.
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

inner_sock=/run/agentos/$sock_name
args=(
  --unshare-all --die-with-parent --new-session --clearenv
  --setenv PATH /usr/bin:/bin --setenv HOME /tmp --setenv LANG C.UTF-8
  --setenv PYTHONDONTWRITEBYTECODE 1
  --ro-bind /usr /usr --symlink usr/bin /bin --symlink usr/sbin /sbin --symlink usr/lib /lib
  --proc /proc --dev /dev --tmpfs /tmp --tmpfs /run
  --ro-bind "$pyenv" "$pyenv"
  --ro-bind "$code" "$code" --chdir "$code"
  --ro-bind "$runtime_dir" /run/agentos
)
[[ -e /usr/lib64 ]] && args+=(--symlink usr/lib64 /lib64)

# Inside: start socat, wait until it listens (127.0.0.1:7233 is 0100007F:1C41 in
# /proc/net/tcp, state 0A is LISTEN), then hand over to the command.
exec bwrap "${args[@]}" -- /bin/sh -c '
  socat TCP-LISTEN:7233,bind=127.0.0.1,reuseaddr,fork UNIX-CONNECT:'"$inner_sock"' &
  until grep -q " 0100007F:1C41 00000000:0000 0A " /proc/net/tcp; do sleep 0.05; done
  exec "$@"' sh "$@"
