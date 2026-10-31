"""The resource limits on Linux (M5 C3a): agentd runs each generation of its Worker and each git tool
call in a systemd user scope of its own, a cgroup that holds its memory, with no swap, and its tasks,
which its launcher starts and checks before it starts bwrap. Past its memory limit the kernel kills
the Worker, and agentd records that (worker.exited's oom_kill) and starts it again; a git tool call
past its memory limit is refused, naming the limit, and not tried again. agentd warns as it starts when
the limits' worst case is over half of the memory, and records it. Without a user manager agentd
refuses to start, saying so; --insecure-dev turns the limits off, and says so.

Only under bwrap: the macOS sandbox sets no limits (agentd.started says none: test_worker_supervision.py).
"""

import json
import os
import random
import re
import secrets
import shlex
import signal
import subprocess
from pathlib import Path

import pytest

from tests.harness import SANDBOX, TERMINAL, TOOL_LAUNCHER, WORKER_LAUNCHER, Stack, command_of, descendants, wait_for
from tests.limits_workflows import MARK
from tests.test_t12_explain import end_event, points, record_seq, report_agrees

pytestmark = pytest.mark.skipif(SANDBOX != "bwrap", reason="the Linux launchers' systemd user scopes (M5 C3a)")

WORKFLOWS = "tests.limits_workflows:WORKFLOWS"
HOLDS = "the Worker's generations and the git tool calls each run in a systemd user scope, which holds their limits"
SCOPE = re.compile(r"/user\.slice/user-\d+\.slice/user@\d+\.service/app\.slice/agentos-([0-9a-f]{16})-(worker|tool)-(\d+)\.scope")


def limited(stack: Stack, workflows: str | None = None, worker: dict | None = None, tools: dict | None = None) -> dict:
    """agentd.yaml's worker and git_tools sections, as the harness has them, with these limits."""
    return {"worker": {**stack.worker_config(workflows), **({"limits": worker} if worker else {})},
            "git_tools": {"sandbox": [str(TOOL_LAUNCHER[SANDBOX])], **({"limits": tools} if tools else {})}}


def cgroup_of(pid: int) -> str:
    """The process's cgroup (v2), from the host."""
    [line] = Path(f"/proc/{pid}/cgroup").read_text().splitlines()
    assert line.startswith("0::"), line
    return line.removeprefix("0::")


def limits_of(cgroup: str) -> dict[str, str]:
    return {f: (Path("/sys/fs/cgroup" + cgroup) / f).read_text().strip() for f in ("memory.max", "memory.swap.max", "pids.max")}


def unit_state(unit: str) -> dict[str, str]:
    """What the user manager says of a unit: its ActiveState, Result and LoadState."""
    out = subprocess.run(["systemctl", "--user", "show", "--property=ActiveState,Result,LoadState", "--", unit],
                         capture_output=True, text=True, check=True).stdout
    return dict(line.split("=", 1) for line in out.splitlines() if "=" in line)


def failed_units(token: str) -> list[str]:
    """The agentd start's scopes the user manager keeps, failed."""
    out = subprocess.run(["systemctl", "--user", "list-units", "--all", "--failed", "--plain", "--no-legend", f"agentos-{token}-*"],
                         capture_output=True, text=True, check=True).stdout
    return [line.split()[0] for line in out.splitlines() if line.strip()]


def last_start(stack: Stack) -> dict:
    [r] = stack.ledger("SELECT payload FROM audit_log WHERE type = 'agentd.started' ORDER BY seq DESC LIMIT 1")
    return json.loads(r["payload"])


def exit_records(stack: Stack) -> list[dict]:
    return [json.loads(r["payload"]) for r in
            stack.ledger("SELECT payload FROM audit_log WHERE type = 'worker.exited' ORDER BY seq")]


def said(stack: Stack, nonce: str) -> list[str]:
    """What the Worker wrote of what limits_workflows did for nonce, as agentd logged it."""
    return [line.split(nonce, 1)[1].strip() for line in stack.worker().output() if line.startswith(f"{MARK} {nonce} ")]


def mcp_servers(agentd: int) -> list[int]:
    return [p for p in descendants(agentd) if Path((command_of(p).split() or [""])[0]).name == "agentos-mcp-git"]


def oom_score_adj(pid: int) -> str:
    """The process's oom_score_adj, as the kernel's out-of-memory killer weighs it (from the host: the
    same number as the process reads in its sandbox, of /proc/self)."""
    return Path(f"/proc/{pid}/oom_score_adj").read_text().strip()


def test_each_run_has_a_scope_of_its_own_that_holds_its_limits(stack: Stack) -> None:
    """The Worker's generation and a git tool call, each in a scope of its own, named after agentd's
    start and the run, whose cgroup holds the limits the configuration names, with no swap; their
    sandboxes' processes have an oom_score_adj of 1000, first for the out-of-memory killer; agentd.started
    records them, and what holds them. A scope whose run ended without the kernel killing it is gone."""
    hold = stack.tmp / "hold-in-tool"
    hold.touch()
    stack.start_agentd(worker=True, extra=limited(stack, worker={"memory": "768MiB", "tasks": 300},
                                                  tools={"memory": "384MiB", "tasks": 100}),
                       faults=[{"name": "git.show_diff", "attempt": 1, "hold_in_tool_while_exists": str(hold)}])
    started = last_start(stack)
    assert started["worker"]["limits"] == {"memory_bytes": 768 << 20, "tasks": 300, "swap_bytes": 0, "oom_score_adj": 1000,
                                           "enforcement": "systemd_user_scope"}, started
    assert started["git_tools_limits"] == {"memory_bytes": 384 << 20, "tasks": 100, "swap_bytes": 0, "oom_score_adj": 1000,
                                           "enforcement": "systemd_user_scope"}, started
    [holds] = [e for e in stack.agentd_log() if e.get("msg") == HOLDS]
    assert (holds["worker_memory"], holds["worker_tasks"], holds["tool_memory"], holds["tool_tasks"]) == ("768MiB", 300, "384MiB", 100)
    token = re.fullmatch(r"agentos-([0-9a-f]{16})-\*\.scope", holds["scopes"]).group(1)

    w = stack.worker()
    pid = w.wait_ready(1)
    first = cgroup_of(pid)
    assert SCOPE.fullmatch(first) and SCOPE.fullmatch(first).groups() == (token, "worker", "1"), first
    assert limits_of(first) == {"memory.max": str(768 << 20), "memory.swap.max": "0", "pids.max": "300"}
    assert oom_score_adj(pid) == "1000" and oom_score_adj(stack.agentd.popen.pid) != "1000", pid
    [launch] = w.log("worker started")
    assert f"AGENTOS_LIMIT_UNIT=agentos-{token}-worker-1.scope" in launch["env"], launch["env"]

    # A run's git.show_diff, held with its server up: the server's scope, the call's own.
    run = stack.start_run()
    [server] = wait_for(lambda: mcp_servers(stack.agentd.popen.pid), "the git tool server of the run's git.show_diff", timeout=60)
    call = cgroup_of(server)
    m = SCOPE.fullmatch(call)
    assert m and m.groups()[:2] == (token, "tool"), call  # agentd's self-check at start made the first calls
    assert limits_of(call) == {"memory.max": str(384 << 20), "memory.swap.max": "0", "pids.max": "100"}
    assert oom_score_adj(server) == "1000", server
    hold.unlink()
    assert stack.wait_state(run, TERMINAL, timeout=120)["state"] == "SUCCEEDED"
    assert not Path("/sys/fs/cgroup" + call).exists()

    # The next generation, a scope of its own; the last one's is gone, not kept failed: the kernel did
    # not kill it, and its exit's record says nothing of memory.
    os.kill(pid, signal.SIGKILL)
    second = cgroup_of(w.wait_ready(2))
    assert SCOPE.fullmatch(second).groups() == (token, "worker", "2"), second
    assert not Path("/sys/fs/cgroup" + first).exists()
    assert unit_state(f"agentos-{token}-worker-1.scope")["LoadState"] == "not-found"
    [record] = exit_records(stack)
    assert record["generation"] == 1 and "oom_kill" not in record, record
    assert failed_units(token) == []


WORST = "the resource limits' worst case is over half of "


def machine_memory() -> int:
    """The machine's memory, in bytes: /proc/meminfo's MemTotal (in kB)."""
    [kb] = [line.split()[1] for line in Path("/proc/meminfo").read_text().splitlines() if line.startswith("MemTotal:")]
    return int(kb) << 10


def test_a_worst_case_over_half_the_memory_is_warned_of_and_recorded(stack: Stack) -> None:
    """Each scope holds its own limit, and nothing holds their sum: the Worker's 64GiB and six git tool
    calls at 512MiB each, far over half of the machine's memory, make agentd warn as it starts, saying
    what adds up to what, against what, and how to lower it; agentd.started records it."""
    stack.start_agentd(worker=True, extra=limited(stack, worker={"memory": "64GiB"}))
    worst = last_start(stack)["limits_worst_case"]
    made = {k: worst[k] for k in ("memory_bytes", "worker_memory_bytes", "tool_memory_bytes", "tool_calls", "warned")}
    assert made == {"memory_bytes": (64 << 30) + 6 * (512 << 20), "worker_memory_bytes": 64 << 30, "tool_memory_bytes": 512 << 20,
                    "tool_calls": 6, "warned": True}, worst
    # The machine's, or less where a cgroup above the scopes holds less.
    assert (worst["compared_with"] == "MemTotal" and worst["compared_with_bytes"] == machine_memory() or
            worst["compared_with"].startswith("/user.slice") and worst["compared_with_bytes"] < machine_memory()), worst
    [warned] = [e for e in stack.agentd_log() if e.get("msg", "").startswith(WORST)]
    assert warned["level"] == "WARN", warned
    assert "the Worker's 64GiB and 6 git tool calls at 512MiB each come to 67GiB, of " in warned["msg"], warned
    assert ("nothing limits their sum" in warned["msg"] and "the kernel's out-of-memory killer picks its victim from " in warned["msg"]
            and warned["msg"].endswith("worker.limits.memory and git_tools.limits.memory lower it")), warned
    assert (warned["worst_case_bytes"], warned["memory_bytes"], warned["memory_from"]) == (
        worst["memory_bytes"], worst["compared_with_bytes"], worst["compared_with"]), (warned, worst)


def test_a_worst_case_under_half_the_memory_is_only_recorded(stack: Stack) -> None:
    """Small limits, well under half of the machine's memory: agentd warns of nothing, and agentd.started
    records the worst case, and that it did not warn."""
    stack.start_agentd(worker=True, extra=limited(stack, worker={"memory": "256MiB"}, tools={"memory": "64MiB"}))
    worst = last_start(stack)["limits_worst_case"]
    assert worst == {"memory_bytes": (256 << 20) + 6 * (64 << 20), "worker_memory_bytes": 256 << 20, "tool_memory_bytes": 64 << 20,
                     "tool_calls": 6, "compared_with": worst["compared_with"], "compared_with_bytes": worst["compared_with_bytes"],
                     "warned": False}, worst
    assert worst["compared_with_bytes"] >= 2 * worst["memory_bytes"], worst
    assert not any(e.get("msg", "").startswith(WORST) for e in stack.agentd_log())


def test_a_worker_over_its_memory_limit_is_killed_recorded_and_started_again(stack: Stack) -> None:
    """A Worker that holds more than its memory limit: the kernel's out-of-memory killer kills it, systemd
    stops its scope, and agentd, told so by systemd, logs it with the limit, records it (oom_kill) and
    starts the next generation, which carries the run on to its end. The failed scope is cleared."""
    stack.start_agentd(worker=True, workflows=WORKFLOWS, extra=limited(stack, WORKFLOWS, worker={"memory": "192MiB"}))
    w = stack.worker()
    pid = w.wait_ready(1)
    token, _, _ = SCOPE.fullmatch(cgroup_of(pid)).groups()
    run = stack.start_run()
    nonce = secrets.token_hex(4)
    stack.signal(run, "go", {"nonce": nonce, "allocate_mib": 1024})
    wait_for(lambda: exit_records(stack), "the Worker's exit's record", timeout=90)
    [record] = exit_records(stack)
    assert record["generation"] == 1 and record["oom_kill"] is True and record["ready"] is True, record
    [over] = w.log("the kernel's out-of-memory killer killed the Worker")
    assert over["generation"] == 1 and over["memory_limit"] == "192MiB" and over["scope"] == f"agentos-{token}-worker-1.scope", over
    [exited] = w.log("worker exited")
    assert exited["oom_kill"] is True and exited["restart_in"] == "1s", exited
    assert said(stack, nonce) == ["allocating 1024 MiB"], said(stack, nonce)  # it never held them
    assert failed_units(token) == []

    w.wait_ready(2)
    stack.signal(run, "finish")
    assert stack.wait_state(run, TERMINAL, timeout=120)["state"] == "SUCCEEDED"
    assert said(stack, nonce) == ["allocating 1024 MiB"]  # the next generation replayed the go signal
    assert len(exit_records(stack)) == 1


def test_threads_in_the_worker_stop_at_its_task_limit(stack: Stack) -> None:
    """Threads the Worker starts, with the processes and threads it has, stop at its task limit: the
    next one cannot start. The Worker serves on."""
    stack.start_agentd(worker=True, workflows=WORKFLOWS, extra=limited(stack, WORKFLOWS, worker={"tasks": 64}))
    w = stack.worker()
    cgroup = cgroup_of(w.wait_ready(1))
    run = stack.start_run()
    nonce = secrets.token_hex(4)
    stack.signal(run, "go", {"nonce": nonce, "threads": 1000})
    [line] = wait_for(lambda: said(stack, nonce), "the Worker's threads", timeout=60)
    m = re.fullmatch(r"started (\d+) threads; the next: (.*)", line)
    assert m and 0 < int(m.group(1)) < 64 and "can't start new thread" in m.group(2), line
    scope = Path("/sys/fs/cgroup" + cgroup)
    assert (scope / "pids.max").read_text().strip() == "64" and (scope / "pids.peak").read_text().strip() == "64"
    refused = dict(line.split() for line in (scope / "pids.events").read_text().splitlines())
    assert int(refused["max"]) >= 1, refused
    stack.signal(run, "finish")
    assert stack.wait_state(run, TERMINAL, timeout=120)["state"] == "SUCCEEDED"
    assert exit_records(stack) == [] and cgroup_of(w.pid()) == cgroup  # the same generation served on


def git(repo: Path, *args: str, stdin: str | None = None) -> str:
    env = {**os.environ, "GIT_AUTHOR_NAME": "agentos", "GIT_AUTHOR_EMAIL": "agentos@agentos.invalid",
           "GIT_COMMITTER_NAME": "agentos", "GIT_COMMITTER_EMAIL": "agentos@agentos.invalid",
           "GIT_AUTHOR_DATE": "2026-10-03T00:00:00Z", "GIT_COMMITTER_DATE": "2026-10-03T00:00:00Z",
           "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"}
    return subprocess.run(["git", "-C", str(repo), *args], input=stdin, capture_output=True, text=True, check=True,
                          env=env).stdout.strip()


def costly_commit(mirror: Path) -> str:
    """A commit whose diff git cannot take within a small memory limit: 1.5 million random short lines
    (3 MiB, under show_diff's 4 MiB a file) in place of as many others. git holds both sides, with some
    60 bytes more for every line (internal/gitmcp's server)."""
    trees = []
    for seed in (1, 2):
        lines = "\n".join(random.Random(seed).choices("ab", k=1_500_000)) + "\n"
        blob = git(mirror, "hash-object", "-w", "--stdin", stdin=lines)
        trees.append(git(mirror, "mktree", stdin=f"100644 blob {blob}\tlines.txt\n"))
    old = git(mirror, "commit-tree", trees[0], "-p", "HEAD", "-m", "lines")
    new = git(mirror, "commit-tree", trees[1], "-p", old, "-m", "other lines")
    git(mirror, "update-ref", "refs/heads/costly", new)
    return new


def test_a_git_tool_call_killed_for_memory_twice_is_refused_in_one_attempt(stack: Stack) -> None:
    """A commit too large to diff within the git tool calls' memory limit: the kernel's out-of-memory
    killer kills git, and systemd the rest of the call's scope. systemd does not say whether the call
    went over its own limit or memory ran out higher up, so agentd runs the call once more, in a new
    scope, which is killed too; then it refuses the call for good, as it does one past the CPU time
    limit, saying both: one gateway attempt, a tool's error (no error_cause, which would say the server
    failed), which runs explain and the report quote. The failed scopes are cleared. agentd's own calls
    at start fit within the limit."""
    stack.start_agentd(extra={"git_tools": {"sandbox": [str(TOOL_LAUNCHER[SANDBOX])], "limits": {"memory": "64MiB"}}})
    stack.start_worker()
    stack.commit = costly_commit(stack.tmp / "mirror")
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL, timeout=120)["state"] == "SUCCEEDED"  # reported, as every end path is
    refusal = ("tool git.show_diff: the kernel's out-of-memory killer killed the git tool server in both attempts at the call, "
               "under a memory limit of 64MiB: the commit is too large to read within it, or the host ran out of memory")
    calls = [json.loads(r["payload"]) for r in stack.ledger("SELECT payload FROM audit_log WHERE workflow_id = ? AND type = 'call'",
                                                             run["run_id"])]
    diffs = [c for c in calls if c["name"] == "git.show_diff"]
    assert len(diffs) == 1, diffs
    [diff] = diffs
    assert (diff["attempt"], diff["status"], diff["error_type"]) == (1, "error", "ToolFailed"), diff
    assert diff["error"] == refusal + " (type: ToolFailed, retryable: false)" and "error_cause" not in diff, diff
    killed = [e for e in stack.agentd_log() if e.get("msg") == "the kernel's out-of-memory killer killed the git tool server"
              and e.get("tool") == "show_diff"]
    assert len(killed) == 2 and all(k["memory_limit"] == "64MiB" for k in killed), killed
    first, second = (re.fullmatch(r"agentos-([0-9a-f]{16})-tool-(\d+)\.scope", k["scope"]).groups() for k in killed)
    assert first[0] == second[0] and int(second[1]) == int(first[1]) + 1, killed  # two scopes, one after the other
    assert any(e.get("msg", "").startswith("the git tool call runs once more, in a new scope") for e in stack.agentd_log())
    assert failed_units(first[0]) == []
    e = stack.explain(run)
    points(e, "step_failed", "tool_error", "fetch_diff", "1:git.show_diff", 1,
           seq=record_seq(stack, run, "1:git.show_diff", 1, "error"), event=end_event(stack, run, "1:git.show_diff"),
           says=("out-of-memory killer killed the git tool server in both attempts", "memory limit of 64MiB",
                 "or the host ran out of memory"))
    assert "in both attempts at the call, under a memory limit of 64MiB" in report_agrees(stack, run, e)


# A systemd-run in front of the real one that starts the launcher's scope otherwise than it asked, as a
# user manager might that does not hold what it is asked, or a launcher run in a scope not its own: the
# launcher reads its scope back, and refuses to go on (sed's edit of systemd-run's arguments, and why).
SCOPES_OTHERWISE = {
    "another unit": ("s/^--unit=\\(.*\\)\\.scope$/--unit=\\1-other.scope/", "not in its scope"),
    "more memory": ("s/^--property=MemoryMax=.*/--property=MemoryMax=134217728/", "its scope's memory.max is 134217728, not 67108864"),
    "swap": ("s/^--property=MemorySwapMax=0$/--property=MemorySwapMax=infinity/",
             "its scope's memory.swap.max is max, not 0: the scope was not made as asked, or systemd could not set it"),
    "more tasks": ("s/^--property=TasksMax=.*/--property=TasksMax=64/", "its scope's pids.max is 64, not 32"),
    "as asked": ("", ""),
}


@pytest.mark.parametrize("scope", SCOPES_OTHERWISE)
def test_the_launchers_check_their_scope(tmp_path: Path, scope: str) -> None:
    """The launcher reads back, in its scope, the scope's cgroup and its memory.max, memory.swap.max and
    pids.max, and refuses to go on unless they are what it asked; in a scope as asked it goes on."""
    edit, refusal = SCOPES_OTHERWISE[scope]
    bin = tmp_path / "bin"
    bin.mkdir()
    (bin / "systemd-run").write_text("#!/bin/bash\n"
                                      f"mapfile -t args < <(printf '%s\\n' \"$@\" | sed -e {shlex.quote(edit)})\n"
                                      'exec /usr/bin/systemd-run "${args[@]}"\n')
    (bin / "systemd-run").chmod(0o755)
    env = {"PATH": f"{bin}:/usr/bin:/bin", "LANG": "C.UTF-8", "AGENTOS_LIMIT_MEMORY": str(64 << 20), "AGENTOS_LIMIT_TASKS": "32",
           "AGENTOS_LIMIT_UNIT": f"agentos-test-{secrets.token_hex(8)}-tool-1.scope",
           "XDG_RUNTIME_DIR": os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}"}
    r = subprocess.run([str(TOOL_LAUNCHER[SANDBOX]), str(tmp_path), "--", "/bin/sh", "-c", "echo inside"],
                       capture_output=True, text=True, timeout=60, env=env)
    if not refusal:
        assert (r.returncode, r.stdout) == (0, "inside\n"), r.stderr
    else:
        assert r.returncode == 1 and "inside" not in r.stdout and refusal in r.stderr, (r.stdout, r.stderr)


def unscoped(original: Path, copy: Path) -> Path:
    """copy: original without the line that sources limits.sh."""
    lines = original.read_text().splitlines(keepends=True)
    kept = [line for line in lines if "limits.sh\"" not in line]
    assert len(kept) == len(lines) - 1, "the launcher sources limits.sh in one line"
    copy.write_text("".join(kept))
    copy.chmod(0o755)
    return copy


@pytest.mark.audit_verify(skip="agentd refuses to start by design: a launcher runs its sandbox in no scope")
@pytest.mark.parametrize("launcher", ["worker", "tool"])
def test_a_launcher_that_does_not_source_limits_sh_is_refused(stack: Stack, launcher: str) -> None:
    """A launcher that does not source limits.sh (an older copy of run-*.sh, one of its own) runs the
    sandbox in no scope, with no limits: agentd finds its launcher outside the run's scope once the
    Worker says it is ready, or a tool call's server answers (its self-check at start), and refuses to
    start, saying why."""
    original = {"worker": WORKER_LAUNCHER, "tool": TOOL_LAUNCHER}[launcher][SANDBOX]
    copy = unscoped(original, stack.tmp / original.name)
    extra = limited(stack)
    extra["worker" if launcher == "worker" else "git_tools"]["sandbox"] = [str(copy)]
    with pytest.raises(AssertionError):
        stack.start_agentd(worker=True, extra=extra)
    assert stack.agentd is not None and stack.agentd.popen.wait(timeout=60) == 1
    log = stack.agentd_log()
    [stopped] = [e for e in log if e.get("msg") == "agentd stopped"]
    assert not any(e.get("msg") == "agentd ready" for e in log), log
    outside = (r"the process agentd started for the " + launcher + r" \(pid \d+\) is in the cgroup /\S+, not in its scope agentos-"
               r"[0-9a-f]{16}-" + launcher + r"-1\.scope: its launcher does not source sandbox/linux/limits\.sh, or a wrapper runs the "
               r"launcher without exec")
    if launcher == "worker":
        assert re.search("the Worker's launcher did not run it in a systemd user scope that holds its limits, and agentd stopped it: "
                         + outside, stopped["error"]), stopped
    else:  # the self-check's: a call whose server failed, the log says why
        assert stopped["error"].startswith("the git tool server does not work: the git tool server failed on"), stopped
        assert any(e.get("msg") == "the git tool server failed" and re.search(outside, e.get("error", "")) for e in log), log


@pytest.mark.audit_verify(skip="agentd stops by design: its Worker's launcher runs it in no scope")
def test_a_later_worker_out_of_its_scope_stops_agentd(stack: Stack) -> None:
    """The Worker's launcher that runs the first generation in its scope, and the next in none (a launcher
    changed under a running agentd): agentd, ready by then, stops that generation once it says it is
    ready, starts no other, and stops, saying why."""
    copy = unscoped(WORKER_LAUNCHER[SANDBOX], stack.tmp / "run-worker-unscoped.sh")
    marker = stack.tmp / "first-launched"
    wrapper = stack.tmp / "run-worker.sh"
    wrapper.write_text(f"#!/bin/sh\nif [ -e {shlex.quote(str(marker))} ]; then exec {shlex.quote(str(copy))} \"$@\"; fi\n"
                       f"touch {shlex.quote(str(marker))}\nexec {shlex.quote(str(WORKER_LAUNCHER[SANDBOX]))} \"$@\"\n")
    wrapper.chmod(0o755)
    extra = limited(stack)
    extra["worker"]["sandbox"] = [str(wrapper)]
    stack.start_agentd(worker=True, extra=extra)
    w = stack.worker()
    os.kill(w.wait_ready(1), signal.SIGKILL)
    assert stack.agentd is not None and stack.agentd.popen.wait(timeout=90) == 1
    log = stack.agentd_log()
    [stopped] = [e for e in log if e.get("msg") == "agentd stopped"]
    assert re.search(r"agentd stopped it: the process agentd started for the worker \(pid \d+\) is in the cgroup /\S+, not in its "
                     r"scope agentos-[0-9a-f]{16}-worker-2\.scope", stopped["error"]), stopped
    assert len(w.log("worker started")) == 2 and len(w.log("worker ready")) == 1, w.log()


@pytest.mark.audit_verify(skip="agentd refuses to start by design: there is no user manager")
def test_without_a_user_manager_agentd_refuses_to_start(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    """No systemd user manager where agentd looks for one (XDG_RUNTIME_DIR): agentd refuses to start
    before it changes anything, and says why, and how to start one."""
    empty = stack.tmp / "no-user-manager"
    empty.mkdir()
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(empty))
    with pytest.raises(AssertionError):
        stack.start_agentd(worker=True)
    assert stack.agentd is not None and stack.agentd.popen.wait(timeout=30) == 1
    [stopped] = [e for e in stack.agentd_log() if e.get("msg") == "agentd stopped"]
    for words in ("no systemd user manager", f"{empty}/systemd/private is not there", "loginctl enable-linger", "--insecure-dev"):
        assert words in stopped["error"], (words, stopped)
    assert not stack.data_dir.exists()


def test_insecure_dev_turns_the_limits_off(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    """--insecure-dev with the bwrap launchers: the Worker and the git tool calls run sandboxed but in no
    scope, without limits (and so without a user manager); agentd says so as it starts, and its start's
    record."""
    empty = stack.tmp / "no-user-manager"
    empty.mkdir()
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(empty))
    stack.start_agentd(worker=True, insecure_dev=True)
    [warned] = [e for e in stack.agentd_log() if e.get("msg", "").startswith("THE RESOURCE LIMITS ARE OFF (--insecure-dev)")]
    assert warned["level"] == "WARN", warned
    started = last_start(stack)
    assert started["insecure_dev"] is True, started
    assert started["worker"]["limits"] == {"enforcement": "off"} and started["git_tools_limits"] == {"enforcement": "off"}, started
    assert "limits_worst_case" not in started, started
    w = stack.worker()
    pid = w.wait_ready(1)
    assert "agentos-" not in cgroup_of(pid), cgroup_of(pid)
    assert oom_score_adj(pid) == oom_score_adj(stack.agentd.popen.pid) != "1000", pid  # agentd's, as it was
    [launch] = w.log("worker started")
    assert "AGENTOS_LIMITS=off" in launch["env"] and not any(v.startswith("AGENTOS_LIMIT_") for v in launch["env"]), launch["env"]
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL, timeout=120)["state"] == "SUCCEEDED"
