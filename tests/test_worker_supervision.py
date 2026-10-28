"""agentd starts and supervises the Worker (M5 C2a): through its sandbox launcher once the Runtime
entry serves; the first must say it is ready before agentd does; every exit is followed by a
restart, after a delay that doubles to 60 s and starts over after a minute up; each exit agentd did
not cause has an audit record, at most one a minute; on SIGTERM agentd stops it first, through its
stop channel (stdin), so that the Worker reports what it holds. A Worker a killed agentd left is
gone before the next agentd listens (it stops by itself when its stop channel ends; on macOS the
launcher's lifeline kills it, on Linux bwrap; else worker.pid), and never one another agentd still
runs. Neither the Worker nor the git tool server has a controlling terminal, whatever agentd's.

The tests of agentd's own Worker run in every environment (AGENTOS_SANDBOX none, srt, bwrap); those
of the macOS launcher's protocol only under srt."""

import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import pytest
from temporalio.api.enums.v1 import EventType

from tests.harness import (CTTY_EXEC, REPO, SANDBOX, TERMINAL, TEST_MODELS, WORKER_LAUNCHER, Stack, command_of, descendants,
                           event_types, free_port, started_at, wait_for)
from tests.rogue_workflows import SLOW_HELD, SLOW_HOLDING

NO_TTY = {"??", "?"}  # what ps says of a process with no controlling terminal: macOS, Linux


def tty_of(pid: int) -> str:
    return subprocess.run(["ps", "-o", "tty=", "-p", str(pid)], capture_output=True, text=True,
                          start_new_session=True).stdout.strip()


def at(entry: dict) -> float:
    return datetime.fromisoformat(entry["time"]).timestamp()


def seconds(duration: str) -> float:
    """A Go duration as agentd logs it, 1s, 500ms or 1m0s, in seconds."""
    total = 0.0
    for n, unit in re.findall(r"([\d.]+)(ms|s|m|h)", duration):
        total += float(n) * {"ms": 0.001, "s": 1, "m": 60, "h": 3600}[unit]
    return total


def exit_records(stack: Stack) -> list[dict]:
    return [json.loads(r["payload"]) for r in
            stack.ledger("SELECT payload FROM audit_log WHERE type = 'worker.exited' ORDER BY seq")]


def last_start(stack: Stack) -> dict:
    [r] = stack.ledger("SELECT payload FROM audit_log WHERE type = 'agentd.started' ORDER BY seq DESC LIMIT 1")
    return json.loads(r["payload"])


def state_dirs(agentd_pid: int) -> list[Path]:
    """The macOS state directories agentd process agentd_pid made for its Workers."""
    return sorted(Path("/tmp").glob(f"agentos-worker.{agentd_pid}.*"))


def succeeds(stack: Stack) -> None:
    run = stack.start_run()
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED", final


def test_agentd_starts_its_worker_before_it_says_it_is_ready(stack: Stack) -> None:
    stack.start_agentd(worker=True)
    w = stack.worker()
    msgs = [e.get("msg") for e in stack.agentd_log()]
    assert msgs.index("worker started") < msgs.index("worker ready") < msgs.index("agentd ready"), msgs
    [ready] = w.log("worker ready")
    assert ready["generation"] == 1, ready
    pid = w.pid()
    assert stack.worker_processes() == [pid]
    [launch] = w.log("worker started")
    record = json.loads((stack.data_dir / "worker.pid").read_text())
    assert record["pid"] == launch["pid"] and record["generation"] == 1, record
    started = last_start(stack)
    assert started["no_worker"] is False, started
    worker = started["worker"]
    assert {k: worker.get(k) for k in ("sandbox", "python", "code", "workflows", "limits")} == {
        "sandbox": str(WORKER_LAUNCHER[SANDBOX]) if SANDBOX in WORKER_LAUNCHER else "",
        "python": sys.prefix, "code": str(REPO), "workflows": "agentos_worker.workflows:WORKFLOWS", "limits": {}}, started
    # The copy of the code it runs (M5 C2c), which the start's log names too, and the liveness thresholds.
    assert re.fullmatch(r"[0-9a-f]{64}", worker["code_sha256"]) and launch["code_sha256"] == worker["code_sha256"], (worker, launch)
    assert worker["code_files"] > 1000 and worker["code_bytes"] > 50 << 20, worker
    assert any(d.startswith("temporalio==") for d in worker["distributions"]), worker["distributions"]
    assert worker["interpreter"] == os.path.realpath(Path(sys.prefix) / "bin" / "python"), worker
    assert worker["liveness"] == {"quiet_ms": 60_000, "hold_ms": 60_000}, worker
    assert len(worker) == 11, sorted(worker)
    if SANDBOX == "srt":  # the launcher became srt, whose settings are in agentd's state directory for it
        found = re.search(r" --settings (\S+) ", command_of(launch["pid"]))
        assert found and Path(found.group(1)).parent in state_dirs(stack.agentd.popen.pid), command_of(launch["pid"])
    succeeds(stack)


def test_a_worker_that_exits_is_started_again_with_a_growing_delay(stack: Stack) -> None:
    stack.start_agentd(worker=True)
    w = stack.worker()
    for n in (1, 2, 3):
        os.kill(w.wait_ready(n), signal.SIGKILL)
        wait_for(lambda: len(w.log("worker exited")) == n, f"the Worker's exit {n}")
    pid = w.wait_ready(4)
    exits, starts = w.log("worker exited"), w.log("worker started")
    assert [e["restart_in"] for e in exits] == ["1s", "2s", "4s"], exits
    for e, after in zip(exits, starts[1:]):
        assert at(after) - at(e) >= seconds(e["restart_in"]) - 0.01, (e, after)
    # One record at once; the next two exits are held, for a minute after it.
    records = exit_records(stack)
    assert len(records) == 1 and records[0]["generation"] == 1 and records[0]["ready"] is True, records
    assert ("exit_code" in records[0]) != ("signal" in records[0]) and records[0]["restart_in_ms"] == 1000, records
    # A Worker up a minute starts the delays over.
    time.sleep(max(0.0, at(starts[3]) + 61 - time.time()))
    os.kill(pid, signal.SIGKILL)
    wait_for(lambda: len(w.log("worker exited")) == 4, "the fourth exit")
    assert w.log("worker exited")[3]["restart_in"] == "1s", w.log("worker exited")
    w.wait_ready(5)
    # The minute after the first record is up: the second describes the third exit and counts the second.
    records = exit_records(stack)
    assert len(records) == 2 and records[1]["generation"] == 3 and records[1]["unrecorded"] == 1, records
    r = stack.audit_verify()
    assert r.returncode == 0, r.stdout + r.stderr
    succeeds(stack)


@pytest.mark.audit_verify(skip="agentd stops by design: its first Worker never says it is ready")
@pytest.mark.parametrize("fault", ["exits", "hangs"])
def test_agentd_stops_when_its_first_worker_is_not_ready(stack: Stack, fault: str) -> None:
    workflows = {"exits": "tests.no_such_module:WORKFLOWS", "hangs": "tests.worker_hangs:WORKFLOWS"}[fault]
    start = time.monotonic()
    with pytest.raises(AssertionError):
        stack.start_agentd(worker=True, workflows=workflows)
    took = time.monotonic() - start
    assert stack.agentd is not None and stack.agentd.popen.wait(timeout=30) == 1
    entries = stack.agentd_log()
    assert not any(e.get("msg") == "agentd ready" for e in entries), entries
    [stopped] = [e for e in entries if e.get("msg") == "agentd stopped"]
    if fault == "exits":
        assert took < 45, took
        assert "exited before it said it was ready" in stopped["error"], stopped
        assert "No module named 'tests.no_such_module'" in stopped["error"], stopped  # the Worker's own words
        [record] = exit_records(stack)
        assert record["generation"] == 1 and record["ready"] is False and "restart_in_ms" not in record, record
    else:
        assert 60 <= took < 90, took
        assert "did not say it was ready within 1m0s" in stopped["error"], stopped
        assert exit_records(stack) == []  # agentd stopped it
    assert len([e for e in entries if e.get("msg") == "worker started"]) == 1, entries
    assert stack.worker_processes() == []
    assert not (stack.data_dir / "worker.pid").exists()
    assert state_dirs(stack.agentd.popen.pid) == []


@pytest.mark.parametrize("worker_state", ["responsive", "frozen"])
def test_agentd_stops_its_worker_first(stack: Stack, worker_state: str) -> None:
    """agentd asks its Worker to stop (its stop channel ends), first: the Worker stops by itself, on
    every platform. One that cannot (frozen) gets SIGTERM after 5 s, then SIGKILL."""
    stack.start_agentd(worker=True)
    w = stack.worker()
    pid = w.wait_ready(1)
    succeeds(stack)
    [launch] = w.log("worker started")
    agentd = stack.agentd
    assert agentd is not None
    if worker_state == "frozen":
        os.kill(pid, signal.SIGSTOP)  # it cannot act on its stop channel, nor on SIGTERM
    start = time.monotonic()
    agentd.popen.send_signal(signal.SIGTERM)
    assert agentd.popen.wait(timeout=60) == 0
    took = time.monotonic() - start
    entries = stack.agentd_log()
    after = entries[[e.get("msg") for e in entries].index("shutting down"):]
    assert not any(e.get("msg") == "worker started" for e in after), after
    [stopped] = [e for e in after if e.get("msg") == "worker stopped"]
    # Before the Runtime entry closes: a supervisor still running then would start a Worker that cannot reach it.
    msgs = [e.get("msg") for e in after]
    assert msgs.index("worker stopped") < msgs.index("the Runtime entry is closed"), msgs
    if worker_state == "responsive":  # it shut down by itself, the Runtime entry still serving
        assert stopped["status"] == "exit status 0" and took < 5, (stopped, took)
        assert not any(e.get("msg") == "the Worker did not stop when asked: SIGTERM" for e in after), after
    else:  # SIGTERM after the grace (bwrap dies of it at once); srt and the Worker itself need SIGKILL
        assert stopped["status"].startswith("signal: ") and 5 <= took < 20, (stopped, took)
    assert started_at(pid) == "" and started_at(launch["pid"]) == "", (pid, launch)
    assert stack.worker_processes() == []
    assert not (stack.data_dir / "worker.pid").exists()
    assert state_dirs(agentd.popen.pid) == []
    assert exit_records(stack) == []  # agentd's own doing
    stack.restart_agentd()  # its log verifies, with the stop in it


def test_kill_9_of_agentd_leaves_no_second_worker(stack: Stack) -> None:
    """agentd killed -9: its Worker ends with it, on every platform. Its stop channel ends with agentd,
    and an honest Worker stops by itself; on macOS the launcher's lifeline kills it, on Linux bwrap's
    --die-with-parent."""
    stack.start_agentd(worker=True)
    w = stack.worker()
    pid = w.wait_ready(1)
    born = started_at(pid)
    [launch] = w.log("worker started")
    old = stack.agentd.popen.pid
    stack.kill_agentd()
    wait_for(lambda: started_at(pid) == "" and started_at(launch["pid"]) == "", "the Worker to end with agentd", timeout=10)
    stack.restart_agentd()
    assert started_at(pid) != born
    now = stack.worker().pid()
    assert stack.worker_processes() == [now]
    assert state_dirs(old) == []
    succeeds(stack)


@pytest.mark.parametrize("next_agentd", ["with its Worker", "--no-worker"])
def test_the_next_agentd_stops_a_worker_the_last_left_running(stack: Stack, next_agentd: str) -> None:
    """A Worker that cannot stop by itself (frozen here; a subverted one would not), and on macOS has
    lost its lifeline too, outlives a killed agentd; the next agentd on the data directory stops it
    before it listens, whether it runs a Worker of its own or not (worker.pid). On Linux bwrap's
    --die-with-parent has ended it already."""
    stack.start_agentd(worker=True)
    w = stack.worker()
    pid = w.wait_ready(1)
    born = started_at(pid)
    [launch] = w.log("worker started")
    if SANDBOX == "srt":  # the lifeline's watcher is a subshell of the launcher (now srt), with its cat
        watcher = [p for p in descendants(launch["pid"]) if "run-worker.sh" in command_of(p)]
        assert watcher, [command_of(p) for p in descendants(launch["pid"])]
        for p in [*watcher, *(c for p in watcher for c in descendants(p))]:
            os.kill(p, signal.SIGKILL)
    os.kill(pid, signal.SIGSTOP)
    stack.kill_agentd()
    time.sleep(3)
    if SANDBOX == "bwrap":
        assert started_at(pid) == "", "bwrap's --die-with-parent did not end the sandbox"
    else:
        assert started_at(pid) == born, "the Worker ended with agentd: the test shows nothing"
    stack.start_agentd(worker=next_agentd == "with its Worker")
    assert started_at(pid) == "", "the next agentd left the last one's Worker running"
    swept = [e for e in stack.agentd_log() if e.get("msg") == "stopped the Worker the last agentd on this data directory left running"]
    assert len(swept) == (0 if SANDBOX == "bwrap" else 1), swept
    if next_agentd == "with its Worker":
        assert stack.worker_processes() == [stack.worker().pid()]
        succeeds(stack)
    else:
        assert stack.worker_processes() == []


# The SDK's own line as a Worker begins to shut down (temporalio.worker).
SDK_SHUTDOWN = "Beginning worker shutdown"


def test_a_workflow_task_in_flight_is_reported_across_agentds_stop(stack: Stack) -> None:
    """agentd stops its Worker through the stop channel, and nothing else, while the Worker holds a
    workflow task: agentd gets SIGTERM once the task logs that its hold (of 1 s) has begun; the Worker
    begins to shut down (the SDK says so) before the hold is over, which only the channel can make it
    do in time, agentd's SIGTERM coming only after 5 s, and agentd never falls back to it. The Worker
    finishes the task and reports it while the Runtime entry still serves: the task does not time
    out. A signal through the launcher would cut it off: srt drops the connections it proxies before
    it passes SIGTERM on, and bwrap dies of SIGTERM, the sandbox with it."""
    stack.start_agentd(worker=True, workflows="tests.rogue_workflows:SlowTask")
    w = stack.worker()
    run = stack.start_run()
    wait_for(lambda: any(SLOW_HOLDING in e["line"] for e in w.log("worker output")), "the run's workflow task to begin its hold",
             interval=0.05)
    agentd = stack.agentd
    assert agentd is not None
    agentd.popen.send_signal(signal.SIGTERM)
    assert agentd.popen.wait(timeout=60) == 0
    events = event_types(stack.history(run))
    assert "WORKFLOW_TASK_TIMED_OUT" not in events and "WORKFLOW_EXECUTION_COMPLETED" in events, events
    entries = stack.agentd_log()

    def what(e: dict) -> str | None:  # the Worker's lines that matter here, else agentd's message
        if e.get("msg") == "worker output":
            for mark in (SLOW_HELD, SDK_SHUTDOWN):
                if mark in e["line"]:
                    return mark
        return e.get("msg")

    msgs = [what(e) for e in entries]
    assert SDK_SHUTDOWN in msgs and SLOW_HELD in msgs, msgs
    assert msgs.index("shutting down") < msgs.index(SDK_SHUTDOWN) < msgs.index(SLOW_HELD), msgs
    assert "the Worker did not stop when asked: SIGTERM" not in msgs, msgs
    [stopped] = [e for e in entries if e.get("msg") == "worker stopped"]
    assert stopped["status"] == "exit status 0", stopped
    stack.restart_agentd()  # its log verifies


def test_a_worker_asked_to_stop_while_it_starts_never_polls(stack: Stack) -> None:
    """A Worker whose stop channel ended while it started (its agentd stopped meanwhile, or died) stops
    without polling its task queue: it takes no workflow task only to cut it short. Here it is run by
    hand, as with --no-worker, on a stop channel that has ended already."""
    stack.start_agentd()
    r, w = os.pipe()
    os.close(w)
    try:
        worker = stack.start_worker(args=("--stop-on-stdin-eof",), stdin=r, ready=False)
    finally:
        os.close(r)
    assert worker.popen.wait(timeout=60) == 0, worker.text()
    assert "worker stopped before it polled" in worker.text() and "worker ready" not in worker.text(), worker.text()


def test_a_worker_whose_stdin_cannot_be_watched_runs_on(stack: Stack) -> None:
    """--stop-on-stdin-eof with stdin on /dev/null, which kqueue and epoll refuse to watch: the Worker
    says so, once, and serves; only a signal stops it then."""
    stack.start_agentd()
    with open(os.devnull) as null:
        worker = stack.start_worker(args=("--stop-on-stdin-eof",), stdin=null.fileno())
    assert worker.text().count("not watching stdin") == 1, worker.text()
    succeeds(stack)


def code_dirs(agentd_pid: int) -> list[Path]:
    """The copies of the Worker's code agentd process agentd_pid made."""
    return sorted(Path("/tmp").glob(f"agentos-code.{agentd_pid}.*"))


def test_agentd_starts_again_a_worker_that_stops_polling(stack: Stack) -> None:
    """A Worker that has had no poll open for 60 s is not serving: agentd stops it and starts another. A
    frozen one keeps the polls it had open until Temporal ends them (a long poll's minute or so), then
    has none."""
    stack.start_agentd(worker=True)
    w = stack.worker()
    w.wait_ready(1)
    frozen = time.time()
    w.pause()
    w.wait_ready(2, timeout=240)
    [found] = w.log("the Worker is not serving")
    assert found["rule"] == "not_polling" and found["generation"] == 1, found
    assert 60 <= at(found) - frozen <= 155 + 30, at(found) - frozen
    [record] = exit_records(stack)
    assert record["generation"] == 1 and record["unresponsive"] == "not_polling" and record["unresponsive_ms"] >= 60_000, record
    succeeds(stack)


def test_agentd_starts_again_a_worker_that_leaves_a_task_unanswered(stack: Stack) -> None:
    """A Worker that has left a workflow task it was handed unanswered for 60 s is not serving: agentd
    stops it and starts another, which takes the task once it times out (T1's frozen Worker, without the
    kill). The run goes on: one model call, one timed-out workflow task."""
    hold = stack.tmp / "hold-model"
    hold.touch()
    stack.start_agentd(faults=[{"name": "audit_v1", "hold_while_exists": str(hold)}], worker=True)
    w = stack.worker()
    run = stack.start_run()
    wait_for(lambda: len(stack.model_calls(run)) == 1, "the model call to start")
    w.pause()  # it keeps its polls: it is handed the next workflow task, and never answers
    hold.unlink()

    def handed() -> float | None:
        events = stack.history(run)
        done = [i for i, e in enumerate(events) if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED]
        later = [e for e in events[done[-1]:] if e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED] if len(done) == 2 else []
        return later[0].event_time.ToNanoseconds() / 1e9 if later else None
    handed_at = wait_for(handed, "the frozen Worker to be handed the workflow task", timeout=90)
    w.wait_ready(2, timeout=180)
    [found] = w.log("the Worker is not serving")
    assert found["rule"] == "not_answering" and found["generation"] == 1, found
    assert 60 <= at(found) - handed_at <= 60 + 5 + 30, at(found) - handed_at
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED", final
    assert len(stack.model_calls(run)) == 1, [dict(r) for r in stack.model_calls(run)]
    assert event_types(stack.history(run)).count("WORKFLOW_TASK_TIMED_OUT") == 1
    [record] = exit_records(stack)
    assert record["unresponsive"] == "not_answering" and record["unresponsive_ms"] >= 60_000, record


PINNED = """\"\"\"The test's own workflow module (tests/test_worker_supervision.py): it says its version whenever it
is imported, as the SDK's sandbox does at each workflow task.\"\"\"

from temporalio import workflow

from agentos_worker.workflows.security_audit import AuditInput, SecurityAudit

print("pinned {version}", flush=True)


@workflow.defn(name="security-audit")
class Pinned(SecurityAudit):
    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        return await super().run(a)

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()


WORKFLOWS = [Pinned]
"""


def test_the_worker_runs_the_code_agentd_pinned_at_its_start(stack: Stack) -> None:
    """agentd copies the Worker's code when it starts, and every generation runs the copy: a change to
    the code directory reaches neither the running Worker (whose sandbox imports workflow modules afresh
    at every task) nor the next generation, only the next agentd. The copy's manifest is in the ledger;
    the copy goes when agentd stops, and the next agentd sweeps one a killed agentd left."""
    code = stack.tmp / "code"
    code.mkdir()
    module = code / "pinned_workflows.py"
    module.write_text(PINNED.format(version="v1"))
    stack.start_agentd(worker=True, extra={"worker": {**stack.worker_config("pinned_workflows:WORKFLOWS"), "code": str(code)}})
    w = stack.worker()

    def versions(generation: int | None = None) -> set[str]:
        return {e["line"] for e in w.log("worker output") if e["line"].startswith("pinned ")
                and (generation is None or e["generation"] == generation)}
    succeeds(stack)
    assert versions() == {"pinned v1"}, versions()
    v1 = hashlib.sha256(module.read_bytes()).hexdigest()
    module.write_text(PINNED.format(version="v2"))
    succeeds(stack)  # the same generation, which imports the module afresh at each task
    w.kill()
    w.wait_ready(2)
    succeeds(stack)
    assert versions() == {"pinned v1"} and versions(2) == {"pinned v1"}, versions()
    root = last_start(stack)["worker"]["code_sha256"]
    [blob] = stack.ledger("SELECT bytes FROM blobs WHERE sha256 = ?", root)
    assert hashlib.sha256(blob["bytes"]).hexdigest() == root
    assert f"code/pinned_workflows.py\tfile\t-\t{len(PINNED.format(version='v1'))}\t{v1}" in blob["bytes"].decode(), blob["bytes"][:500]

    first = stack.agentd.popen.pid
    [copy] = code_dirs(first)
    stack.agentd.stop()
    assert not copy.exists(), copy  # gone with its agentd, before the next one could sweep it
    stack.restart_agentd()
    assert last_start(stack)["worker"]["code_sha256"] != root
    succeeds(stack)
    assert {e["line"] for e in stack.worker().log("worker output") if e["line"].startswith("pinned ")} == {"pinned v2"}
    killed = stack.agentd.popen.pid
    [left] = code_dirs(killed)
    stack.kill_agentd()
    assert left.exists()
    stack.restart_agentd()
    assert not left.exists(), left  # swept by the next agentd


def test_t16_a_second_agentd_leaves_the_first_ones_worker_alone(stack: Stack) -> None:
    stack.start_agentd(worker=True)
    w = stack.worker()
    pid = w.wait_ready(1)
    born = started_at(pid)
    record = (stack.data_dir / "worker.pid").read_text()
    dirs = state_dirs(stack.agentd.popen.pid)
    second = subprocess.run([str(a) for a in stack._agentd_args], capture_output=True, text=True, timeout=60)
    assert second.returncode != 0 and "another agentd is using the data directory" in second.stderr, second.stderr
    assert started_at(pid) == born and stack.worker_processes() == [pid]
    assert (stack.data_dir / "worker.pid").read_text() == record
    assert state_dirs(stack.agentd.popen.pid) == dirs
    assert len(w.log("worker started")) == 1
    succeeds(stack)


def test_the_workers_output_is_logged_escaped_and_bounded(stack: Stack) -> None:
    stack.start_agentd(worker=True, workflows="tests.chatty_worker:WORKFLOWS")
    w = stack.worker()
    w.wait_ready(1)  # through 2,000 lines and more than a pipe holds: agentd read them all
    succeeds(stack)
    raw = stack.agentd.log.read_bytes()
    assert b"\x1b" not in raw and "‮".encode() not in raw  # escaped in agentd's log
    lines = w.log("worker output")
    assert any("\x1b[31mred" in e["line"] and "‮" in e["line"] for e in lines), lines[:5]
    assert all(len(e["line"].encode()) <= 2048 + len("…".encode()) for e in lines)
    [cut] = [e for e in lines if "bytes" in e]
    assert cut["bytes"] == len("chatty: ") + 10_000 and cut["line"].endswith("…"), cut
    chatty = [e for e in lines if e["line"].startswith("chatty: line ")]
    assert chatty and len(chatty) <= 20 + 20 * (at(chatty[-1]) - at(chatty[0])) + 1, len(chatty)
    # The count of what was left out is logged within a second of the first line left out.
    wait_for(lambda: sum(e["lines"] for e in w.log("worker output not logged")) > 0, "the count of the lines not logged",
             timeout=10)


@pytest.mark.audit_verify(skip="agentd refuses to start by design")
@pytest.mark.parametrize("what", ["runtime_listen", "code holds the data", "python holds a mirror", "no sandbox",
                                  "code holds a denied path"])
def test_agentd_refuses_a_worker_it_cannot_run(stack: Stack, what: str) -> None:
    if what == "no sandbox" and SANDBOX == "none":
        pytest.skip("--insecure-dev runs the Worker without one")
    worker, extra = stack.worker_config(), {}
    if what == "runtime_listen":  # the one the launcher cannot use
        extra["runtime_listen"] = (f"tcp://127.0.0.1:{stack.runtime_port}" if SANDBOX == "bwrap"
                                   else f"unix://{stack.runtime_socket}")
        want = "runtime_listen"
    elif what == "code holds the data":
        worker["code"], want = str(stack.tmp / "agentd"), "holds the data directory"
    elif what == "python holds a mirror":
        worker["python"], want = str(stack.tmp / "mirror"), "holds repo acme/payments's mirror"
    elif what == "code holds a denied path":  # the copy of the code would hold it (M5 C2c)
        code = stack.tmp / "code"
        (code / "secret").mkdir(parents=True)
        (code / "secret" / "key.py").write_text("")
        worker["code"], worker["deny_read"] = str(code), [*worker["deny_read"], str(code / "secret")]
        want = "which the Worker must not read"
    else:
        del worker["sandbox"]
        want = "worker.sandbox is required"
    extra["worker"] = worker
    with pytest.raises(AssertionError):
        stack.start_agentd(worker=True, extra=extra)
    assert stack.agentd is not None and stack.agentd.popen.wait(timeout=30) != 0
    assert want in stack.agentd.text(), stack.agentd.text()
    assert not (stack.data_dir / "ledger.db").exists()  # refused before agentd changed anything


def test_no_unsandboxed_worker_next_to_a_real_secret(stack: Stack) -> None:
    """An unsandboxed Worker runs as the user, and could read a key from the keychain: agentd starts
    none while its configuration names a real secret (the user's decision). A provider's key counts
    whether a task reaches it or not, and agentd reads none it does not use: this one is not there."""
    models = TEST_MODELS.replace("providers:\n", 'providers:\n  spare: {adapter: anthropic, key: "keyring:agentos-test-not-there"}\n', 1)
    assert models != TEST_MODELS
    if SANDBOX != "none":  # a sandbox keeps the keychain from the Worker
        stack.start_agentd(worker=True, models=models)
        succeeds(stack)
        return
    with pytest.raises(AssertionError):
        stack.start_agentd(worker=True, models=models)
    assert stack.agentd is not None and stack.agentd.popen.wait(timeout=30) == 1
    [stopped] = [e for e in stack.agentd_log() if e.get("msg") == "agentd stopped"]
    assert "models.yaml's providers.spare.key names a real secret" in stopped["error"], stopped
    assert "--no-worker" in stopped["error"], stopped
    assert stack.worker_processes() == []
    # The way out: agentd with --no-worker, and a Worker started by hand.
    stack.start_agentd(models=models)
    stack.start_worker()
    succeeds(stack)


def test_with_no_worker_agentd_starts_none(stack: Stack) -> None:
    stack.start_agentd()  # the harness's default: --no-worker
    assert not any(e.get("msg") == "worker started" for e in stack.agentd_log())
    started = last_start(stack)
    assert started["no_worker"] is True and "worker" not in started, started
    assert not (stack.data_dir / "worker.pid").exists()
    stack.start_worker()
    succeeds(stack)


def test_agentd_on_a_terminal_gives_its_worker_and_git_tool_servers_none(stack: Stack) -> None:
    """srt lets what it runs ioctl /dev/tty: a sandboxed process with a controlling terminal could type
    into it (TIOCSTI). agentd on a terminal starts the Worker and each git tool server in a session of
    its own, which has none."""
    hold = stack.tmp / "hold-in-tool"
    hold.touch()
    stack.start_agentd(worker=True, terminal=True,
                       faults=[{"name": "git.show_diff", "attempt": 1, "hold_in_tool_while_exists": str(hold)}])
    agentd = stack.agentd.popen.pid
    assert tty_of(agentd) not in NO_TTY, tty_of(agentd)  # agentd has a terminal
    stack.worker().wait_ready(1)
    run = stack.start_run()

    def server() -> int | None:
        return next((p for p in descendants(agentd) if Path((command_of(p).split() or [""])[0]).name == "agentos-mcp-git"), None)
    wait_for(server, "the git tool server of the run's git.show_diff", timeout=60)
    found = {p: tty_of(p) for p in descendants(agentd)}
    assert all(tty in NO_TTY or tty == "" for tty in found.values()), {p: (t, command_of(p)) for p, t in found.items()}
    hold.unlink()
    assert stack.wait_state(run, TERMINAL, timeout=120)["state"] == "SUCCEEDED"


# --- the macOS launcher's protocol ------------------------------------------------------------

MACOS = pytest.mark.skipif(SANDBOX != "srt", reason="the macOS launcher")
LAUNCHER = REPO / "sandbox/macos/run-worker.sh"


def on_a_terminal(args: list, timeout: float = 60) -> subprocess.CompletedProcess:
    """args, on a pseudo-terminal that is its controlling terminal, as a person runs a command."""
    master, slave = os.openpty()
    try:
        return subprocess.run([sys.executable, "-c", CTTY_EXEC, *map(str, args)], stdin=slave, capture_output=True,
                              text=True, timeout=timeout, start_new_session=True)
    finally:
        os.close(slave)
        os.close(master)


def readme_perl() -> str:
    """The perl script README.md has a Worker run by hand with, in a session of its own."""
    found = re.search(r"perl -MPOSIX -e '([^']+)'", (REPO / "README.md").read_text())
    assert found, "README.md runs no Worker by hand through perl"
    return found.group(1)


@MACOS
def test_the_macos_launcher_refuses_a_terminal() -> None:
    probe = "print('in the sandbox')"
    launch = [LAUNCHER, free_port(), sys.prefix, REPO, "--", os.path.realpath(sys.executable), "-I", "-c", probe]
    r = on_a_terminal(launch)
    assert r.returncode == 2 and "controlling terminal" in r.stderr and r.stdout == "", (r.stdout, r.stderr)
    # The README's way runs it by hand in a session of its own.
    r = on_a_terminal(["/bin/sh", "-c", 'exec perl -MPOSIX -e "$0" "$@" </dev/null', readme_perl(), *launch])
    assert r.returncode == 0 and "in the sandbox" in r.stdout, (r.stdout, r.stderr)


@MACOS
def test_the_macos_launcher_keeps_agentds_state_and_ends_with_its_lifeline() -> None:
    """Given agentd's state directory and its lifeline on fd 3, the launcher uses the directory and
    leaves it for agentd to remove, and once the lifeline's write end closes (agentd gone, however it
    ended), its watcher kills the whole launch: srt and the Worker."""
    state = Path(tempfile.mkdtemp(prefix=f"agentos-worker.{os.getpid()}.", dir="/tmp"))
    r, w = os.pipe()
    probe = "import time; print('up', flush=True); time.sleep(120)"
    launch = [LAUNCHER, free_port(), sys.prefix, REPO, "--", os.path.realpath(sys.executable), "-I", "-c", probe]
    try:
        p = subprocess.Popen(["/bin/sh", "-c", f'exec 3<&{r} {r}<&-; exec "$@"', "sh", *map(str, launch)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, pass_fds=(r,), start_new_session=True,
                             env={**os.environ, "AGENTOS_SANDBOX_STATE": str(state), "AGENTOS_SANDBOX_LIFELINE": "3",
                                  "AGENTOS_SANDBOX_DENY_READ": "[]"})
        os.close(r)
        assert p.stdout.readline() == "up\n", p.stderr.read()
        found = re.search(r" --settings (\S+) ", command_of(p.pid))
        assert found and Path(found.group(1)).resolve().parent == state.resolve(), command_of(p.pid)
        worker = descendants(p.pid)
        os.close(w)  # as agentd's death closes it
        assert p.wait(timeout=10) == -signal.SIGKILL
        wait_for(lambda: all(started_at(q) == "" for q in worker), "the launch to end", timeout=10)
        assert (state / "work").is_dir()  # agentd's to remove
    finally:
        subprocess.run(["rm", "-rf", str(state)], check=False)
