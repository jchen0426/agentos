# shellcheck shell=bash
# Sourced by run-worker.sh and run-tool.sh, before they go on: puts the launcher, and with it the
# sandbox it starts, in a systemd user scope of its own, which holds the resource limits agentd sets
# (M5 C3a); then checks them. A scope is a cgroup (v2) of the user's systemd manager:
#   - memory.max: all the memory the sandbox's processes use at once, the kernel's for them among it
#     (page tables, socket and pipe buffers, memfd, tmpfs), which no process's own limit counts. Past
#     it the kernel kills one of them, and systemd stops the whole scope (OOMPolicy=stop) and keeps it,
#     failed, with the Result oom-kill: agentd asks for that once the launcher has exited;
#   - memory.swap.max 0: none of it goes to swap;
#   - pids.max: processes and threads together.
# The sandbox has no /sys and cannot reach the user manager: what runs in it cannot raise them.
# Then the launcher raises its oom_score_adj to 1000, which every process of the sandbox inherits:
# when the machine (or a cgroup above the scope) runs out of memory, the kernel's out-of-memory
# killer picks one of them first, rather than the user's other programs, Temporal or agentd. A
# process may lower its own again as far as it was (0, as a rule): a subverted sandbox can undo it.
#
# agentd gives the launcher, in the environment it makes for it:
#   AGENTOS_LIMIT_MEMORY  bytes, a whole number of pages (up to 19 digits, as agentd's int64 holds)
#   AGENTOS_LIMIT_TASKS   processes and threads
#   AGENTOS_LIMIT_UNIT    the scope's name, agentos-<agentd's token>-<worker|tool>-<n>.scope
#   XDG_RUNTIME_DIR       where the user manager listens (systemd/private)
# or, with --insecure-dev, AGENTOS_LIMITS=off: no scope, and no limits. Anything else, none of them
# included, refuses: a launcher that finds no limits does not run without them.
#
# systemd-run --scope makes the scope of its own process and then execs the launcher again: the
# launcher keeps its pid, its parent (agentd), its stdin and its session, so bwrap's
# --die-with-parent still ends the sandbox with agentd. The scope is named and not --collect'ed: a
# clean end removes it, as ever; one the kernel ended stays, for agentd to read its Result.
#
# Usage: . limits.sh, from a launcher whose "$@" are still its arguments.

limits_fail() {
  echo "$0: $*" >&2
  exit 1
}

if [[ ${AGENTOS_LIMITS+set} ]]; then
  [[ $AGENTOS_LIMITS == off ]] || limits_fail "AGENTOS_LIMITS=$AGENTOS_LIMITS: only off turns the limits off"
  [[ -z ${AGENTOS_LIMIT_MEMORY-}${AGENTOS_LIMIT_TASKS-}${AGENTOS_LIMIT_UNIT-} ]] ||
    limits_fail "AGENTOS_LIMITS=off, and limits too: one or the other"
else
  [[ ${AGENTOS_LIMIT_MEMORY-} =~ ^[1-9][0-9]{0,18}$ ]] ||
    limits_fail "AGENTOS_LIMIT_MEMORY=${AGENTOS_LIMIT_MEMORY-} is not a number of bytes: agentd gives the launcher its limits" \
      "(or AGENTOS_LIMITS=off, its --insecure-dev)"
  [[ ${AGENTOS_LIMIT_TASKS-} =~ ^[1-9][0-9]{0,17}$ ]] ||
    limits_fail "AGENTOS_LIMIT_TASKS=${AGENTOS_LIMIT_TASKS-} is not a number of tasks"
  [[ ${AGENTOS_LIMIT_UNIT-} =~ ^[A-Za-z0-9_-]+\.scope$ ]] ||
    limits_fail "AGENTOS_LIMIT_UNIT=${AGENTOS_LIMIT_UNIT-} is not the name of a scope"
  [[ ${XDG_RUNTIME_DIR-} == /* && -S $XDG_RUNTIME_DIR/systemd/private ]] ||
    limits_fail "no systemd user manager listens at XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR-}: a login session starts one," \
      "and lingering keeps one (loginctl enable-linger)"
  if [[ -z ${AGENTOS_LIMIT_IN_SCOPE-} ]]; then
    # The launcher again, in its scope. AGENTOS_LIMIT_IN_SCOPE says it is meant to be there: it is
    # checked, not believed. bwrap's --clearenv keeps all of these from the sandbox.
    export AGENTOS_LIMIT_IN_SCOPE=1
    exec systemd-run --user --scope --quiet --unit="$AGENTOS_LIMIT_UNIT" \
      --property=MemoryMax="$AGENTOS_LIMIT_MEMORY" --property=MemorySwapMax=0 \
      --property=TasksMax="$AGENTOS_LIMIT_TASKS" --property=OOMPolicy=stop -- "$0" "$@"
  fi
  # In its scope: its cgroup is the scope's, on cgroup v2, and holds the limits asked. A cgroup v1
  # hierarchy that holds the memory or the pids controller would take it from v2; another (a VPN's
  # net_cls) is no matter.
  mapfile -t limits_cgroups </proc/self/cgroup
  limits_cgroup=
  for limits_line in "${limits_cgroups[@]}"; do
    if [[ $limits_line == 0::* ]]; then
      [[ -z $limits_cgroup ]] || limits_fail "two cgroup v2 lines in /proc/self/cgroup: ${limits_cgroups[*]}"
      limits_cgroup=${limits_line#0::}
    elif IFS=: read -r _ limits_controllers _ <<<"$limits_line" &&
      [[ ,$limits_controllers, == *,memory,* || ,$limits_controllers, == *,pids,* ]]; then
      limits_fail "a cgroup v1 hierarchy holds memory or pids ($limits_line): the limits need them on cgroup v2"
    fi
  done
  [[ $limits_cgroup == /* ]] || limits_fail "not on cgroup v2: /proc/self/cgroup is ${limits_cgroups[*]}"
  [[ $limits_cgroup == */"$AGENTOS_LIMIT_UNIT" ]] ||
    limits_fail "not in its scope $AGENTOS_LIMIT_UNIT: its cgroup is $limits_cgroup"
  limits_check() { # <file> <value asked>
    local got
    { read -r got <"/sys/fs/cgroup$limits_cgroup/$1"; } 2>/dev/null ||
      limits_fail "its scope has no $1 (is that controller delegated to the user manager?)"
    [[ $got != max || $2 == max ]] ||
      limits_fail "its scope's $1 is max, not $2: the scope was not made as asked, or systemd could not set it (journalctl --user)"
    [[ $got == "$2" ]] || limits_fail "its scope's $1 is $got, not $2"
  }
  limits_check memory.max "$AGENTOS_LIMIT_MEMORY"
  limits_check memory.swap.max 0
  limits_check pids.max "$AGENTOS_LIMIT_TASKS"
  # First in line for the out-of-memory killer (agentd records 1000: scope.OOMScoreAdj).
  { echo 1000 >/proc/self/oom_score_adj; } 2>/dev/null || limits_fail "cannot raise its oom_score_adj to 1000"
  { read -r limits_oom </proc/self/oom_score_adj; } 2>/dev/null || limits_fail "cannot read its oom_score_adj back"
  [[ $limits_oom == 1000 ]] || limits_fail "its oom_score_adj is $limits_oom, not 1000"
fi
