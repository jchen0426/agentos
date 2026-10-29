"""The resource limits on Linux (M5 C3a): agentd runs each generation of its Worker and each git tool
call in a systemd user scope of its own, a cgroup that holds its memory, with no swap, and its tasks,
which its launcher starts and checks before it starts bwrap. Past its memory limit the kernel kills
the Worker, and agentd records that (worker.exited's oom_kill) and starts it again; a git tool call
past its memory limit is refused, naming the limit, and not tried again. Without a user manager agentd
refuses to start, saying so; --insecure-dev turns the limits off, and says so.

Only under bwrap: the macOS sandbox sets no limits (agentd.started says none: test_worker_supervision.py).
"""

import json
import os
import random
import re
import secrets
import signal
import subprocess
from pathlib import Path

import pytest

from tests.harness import SANDBOX, TERMINAL, TOOL_LAUNCHER, Stack, descendants, command_of, wait_for
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


def test_each_run_has_a_scope_of_its_own_that_holds_its_limits(stack: Stack) -> None:
    """The Worker's generation and a git tool call, each in a scope of its own, named after agentd's
    start and the run, whose cgroup holds the limits the configuration names, with no swap; agentd.started
    records them, and what holds them. A scope whose run ended without the kernel killing it is gone."""
    hold = stack.tmp / "hold-in-tool"
    hold.touch()
    stack.start_agentd(worker=True, extra=limited(stack, worker={"memory": "768MiB", "tasks": 300},
                                                  tools={"memory": "384MiB", "tasks": 100}),
                       faults=[{"name": "git.show_diff", "attempt": 1, "hold_in_tool_while_exists": str(hold)}])
    started = last_start(stack)
    assert started["worker"]["limits"] == {"memory_bytes": 768 << 20, "tasks": 300, "swap_bytes": 0,
                                           "enforcement": "systemd_user_scope"}, started
    assert started["git_tools_limits"] == {"memory_bytes": 384 << 20, "tasks": 100, "swap_bytes": 0,
                                           "enforcement": "systemd_user_scope"}, started
    [holds] = [e for e in stack.agentd_log() if e.get("msg") == HOLDS]
    assert (holds["worker_memory"], holds["worker_tasks"], holds["tool_memory"], holds["tool_tasks"]) == ("768MiB", 300, "384MiB", 100)
    token = re.fullmatch(r"agentos-([0-9a-f]{16})-\*\.scope", holds["scopes"]).group(1)

    w = stack.worker()
    pid = w.wait_ready(1)
    first = cgroup_of(pid)
    assert SCOPE.fullmatch(first) and SCOPE.fullmatch(first).groups() == (token, "worker", "1"), first
    assert limits_of(first) == {"memory.max": str(768 << 20), "memory.swap.max": "0", "pids.max": "300"}
    [launch] = w.log("worker started")
    assert f"AGENTOS_LIMIT_UNIT=agentos-{token}-worker-1.scope" in launch["env"], launch["env"]

    # A run's git.show_diff, held with its server up: the server's scope, the call's own.
    run = stack.start_run()
    [server] = wait_for(lambda: mcp_servers(stack.agentd.popen.pid), "the git tool server of the run's git.show_diff", timeout=60)
    call = cgroup_of(server)
    m = SCOPE.fullmatch(call)
    assert m and m.groups()[:2] == (token, "tool"), call  # agentd's self-check at start made the first calls
    assert limits_of(call) == {"memory.max": str(384 << 20), "memory.swap.max": "0", "pids.max": "100"}
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


def test_a_worker_over_its_memory_limit_is_killed_recorded_and_started_again(stack: Stack) -> None:
    """A Worker that holds more than its memory limit: the kernel kills it, systemd stops its scope, and
    agentd, told so by systemd, logs it with the limit, records it (oom_kill) and starts the next
    generation, which carries the run on to its end. The failed scope is cleared."""
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
    [over] = w.log("the Worker went over its memory limit, and the kernel killed it")
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


def test_a_git_tool_call_over_its_memory_limit_is_refused_once(stack: Stack) -> None:
    """A commit too large to diff within the git tool calls' memory limit: the kernel kills git, systemd
    the rest of the call's scope; agentd, told so by systemd, refuses the call for good, naming the
    limit, as it does one past the CPU time limit: one attempt, a tool's error (no error_cause, which
    would say the server failed), which runs explain and the report quote. The failed scope is cleared.
    agentd's own calls at start fit within the limit."""
    stack.start_agentd(extra={"git_tools": {"sandbox": [str(TOOL_LAUNCHER[SANDBOX])], "limits": {"memory": "64MiB"}}})
    stack.start_worker()
    stack.commit = costly_commit(stack.tmp / "mirror")
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL, timeout=120)["state"] == "SUCCEEDED"  # reported, as every end path is
    refusal = "tool git.show_diff: the commit is too large for the git tool server to read within its memory limit of 64MiB"
    calls = [json.loads(r["payload"]) for r in stack.ledger("SELECT payload FROM audit_log WHERE workflow_id = ? AND type = 'call'",
                                                             run["run_id"])]
    diffs = [c for c in calls if c["name"] == "git.show_diff"]
    assert len(diffs) == 1, diffs
    [diff] = diffs
    assert (diff["attempt"], diff["status"], diff["error_type"]) == (1, "error", "ToolFailed"), diff
    assert diff["error"] == refusal + " (type: ToolFailed, retryable: false)" and "error_cause" not in diff, diff
    [killed] = [e for e in stack.agentd_log() if e.get("msg") == "the git tool call went over its memory limit, and the kernel killed it: refused"]
    assert killed["tool"] == "show_diff" and killed["memory_limit"] == "64MiB", killed
    token = re.fullmatch(r"agentos-([0-9a-f]{16})-tool-\d+\.scope", killed["scope"]).group(1)
    assert failed_units(token) == []
    e = stack.explain(run)
    points(e, "step_failed", "tool_error", "fetch_diff", "1:git.show_diff", 1,
           seq=record_seq(stack, run, "1:git.show_diff", 1, "error"), event=end_event(stack, run, "1:git.show_diff"),
           says=("memory limit of 64MiB",))
    assert "within its memory limit of 64MiB" in report_agrees(stack, run, e)


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
    assert started["worker"]["limits"]["enforcement"] == "off" and started["git_tools_limits"]["enforcement"] == "off", started
    w = stack.worker()
    pid = w.wait_ready(1)
    assert "agentos-" not in cgroup_of(pid), cgroup_of(pid)
    [launch] = w.log("worker started")
    assert "AGENTOS_LIMITS=off" in launch["env"] and not any(v.startswith("AGENTOS_LIMIT_") for v in launch["env"]), launch["env"]
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL, timeout=120)["state"] == "SUCCEEDED"
