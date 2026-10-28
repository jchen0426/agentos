"""T12: inject the five kinds of fault the design doc names (section 11) - a tool's error, a model's
refusal, a policy's denial, a budget run out, a process that crashes (the Worker, agentd, the git
tool server, one each) - and agentctl runs explain points at the right step and cause each time;
besides, the workflow's own code failing, and a run that is stuck. Each explanation is read through
agentctl, as JSON: its verdict, kind, step, activity and attempt; that its cause names the fault;
and that its evidence points at the audit log's record and the history's event behind it. The
report each run posts says what explain says: agentd renders it from the same records, by the
same rules."""

import json
import os
import re
import signal
import subprocess
import sys
import time

import pytest
from temporalio.api.enums.v1 import EventType

from tests.harness import (BIN, MODEL_TIMEOUT, TERMINAL, TEST_MODELS, TEST_POLICY, Stack, approval_policy, command_of,
                           descendants, gateway_calls, wait_for)
from tests.test_t13_model_errors import POLICY, PRICES, models_yaml


def explain_text(stack: Stack, run: dict) -> str:
    r = subprocess.run([BIN / "agentctl", "--socket", stack.control_socket, "runs", "explain", run["run_id"]],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    return r.stdout


def record_seq(stack: Stack, run: dict, activity_id: str, attempt: int, status: str) -> int:
    """The seq of agentd's call record of the attempt."""
    seqs = [r["seq"] for r in stack.ledger("SELECT seq, payload FROM audit_log WHERE workflow_id = ? AND type = 'call'",
                                           run["run_id"])
            if (lambda p: (p["activity_id"], p["attempt"], p["status"]) == (activity_id, attempt, status))(json.loads(r["payload"]))]
    assert len(seqs) == 1, (activity_id, attempt, status, seqs)
    return seqs[0]


def end_event(stack: Stack, run: dict, activity_id: str) -> int:
    """The ID of the history's event that ended the call."""
    for c in gateway_calls(stack.history(run)):
        if c["activity_id"] == activity_id:
            ended = c["completed"] or next(e for e in stack.history(run) if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED
                                           and e.activity_task_failed_event_attributes.scheduled_event_id == c["scheduled"].event_id)
            return ended.event_id
    raise AssertionError(f"no call {activity_id}")


def points(e: dict, verdict: str, kind: str = "", step: str = "", activity_id: str = "", attempt: int = 0,
           seq: int | None = None, event: int | None = None, says: tuple[str, ...] = ()) -> None:
    got = (e["verdict"], e["kind"], e["step"], e["activity_id"], e["attempt"])
    assert got == (verdict, kind, step, activity_id, attempt), (got, e)
    for s in says:
        assert s in e["cause"], (s, e["cause"])
    if seq is not None:
        assert any(s["source"] == "audit" and int(s["seq"]) == seq for s in e["sources"]), (seq, e["sources"])
    if event is not None:
        assert any(s["source"] == "temporal" and int(s["event_id"]) == event for s in e["sources"]), (event, e["sources"])


# A reviewer's patch that touches a path no patch of agentd's may (the policy's and GitHub's protected paths).
EDITED = """diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml
--- a/.github/workflows/ci.yml
+++ b/.github/workflows/ci.yml
@@ -1,2 +1,2 @@
 def find_user(conn, name):
-    return conn.execute("SELECT * FROM users WHERE name = '%s'" % name)
+    return conn.execute("SELECT * FROM users WHERE name = :name", {"name": name})
"""

OUTCOME = re.compile(r"^Outcome: ([a-z_]+)(?: at ([a-z0-9_]+))?(?: \(([a-z0-9_]+)\))?$", re.M)


def report_agrees(stack: Stack, run: dict, e: dict) -> str:
    """The run's report says what its explanation says: the same verdict, step and kind, in agentd's
    words outside the report's fence. Returns the report."""
    body = stack.report_comment(run)
    head = body.split("```")[0]
    m = OUTCOME.search(head)
    assert m, body
    assert (m.group(1), m.group(2) or "", m.group(3) or "") == (e["verdict"], e["step"], e["kind"]), (m.groups(), e)
    return body


def incidents(e: dict) -> list[tuple[str, str, int]]:
    return [(i["kind"], i["step"], i["attempt"]) for i in e["incidents"]]


def test_t12_a_tool_error(stack: Stack) -> None:
    # The tool's own words, with escapes and line breaks in them: they reach the operator's screen.
    error = "\x1b[31mfatal: bad object\x1b[0m\nforged line\rcarriage"
    stack.start_agentd(faults=[{"name": "git.show_diff", "tool_error": error}])
    stack.start_worker()
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"  # reported, as every end path is
    e = stack.explain(run)
    points(e, "step_failed", "tool_error", "fetch_diff", "1:git.show_diff", 1,
           seq=record_seq(stack, run, "1:git.show_diff", 1, "error"), event=end_event(stack, run, "1:git.show_diff"),
           says=("fatal: bad object", r"\x1b[31m", r"\nforged line\rcarriage"))
    body = report_agrees(stack, run, e)
    assert "fatal: bad object" in body.split("```")[1] and "\x1b" not in body, body

    # As text: every line is agentctl's own, and no escape reaches the terminal.
    text = explain_text(stack, run)
    assert "\x1b" not in text and "\r" not in text, repr(text)
    assert not any(line.startswith("forged line") for line in text.splitlines()), text
    [cause] = [line for line in text.splitlines() if line.startswith("cause")]
    assert r"\x1b[31mfatal: bad object\x1b[0m\nforged line\rcarriage" in cause, cause


def test_t12_a_model_refusal(stack: Stack) -> None:
    stack.start_agentd(faults=[{"name": "audit_v1", "provider_refuses": "fake"}])
    stack.start_worker()
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
    e = stack.explain(run)
    points(e, "step_failed", "model_refused", "analyze", "2:audit_v1", 1,
           seq=record_seq(stack, run, "2:audit_v1", 1, "refused"), event=end_event(stack, run, "2:audit_v1"),
           says=("declined", "fake/audit-fixture", "the fake model declines"))
    assert any(s["source"] == "ledger" and s["table"] == "model_calls" for s in e["sources"]), e["sources"]
    assert e["claim"] == "refused"  # the workflow's own word, apart
    [u] = e["usage"]
    assert (u["model"], int(u["requests"]), int(u["refusals"])) == ("fake/audit-fixture", 1, 1), u
    report_agrees(stack, run, e)
    # The workflow tells the report only why it reports: its outcome is agentd's to write.
    [report] = [c for c in gateway_calls(stack.history(run)) if c["name"] == "github.comment_commit"]
    assert "outcome" not in report["args"] and report["args"]["reason"] == "done", report["args"]


def test_t12_a_policy_denial(stack: Stack) -> None:
    stack.start_agentd(policy=TEST_POLICY.replace("allowed_providers: [fake, anthropic]", "allowed_providers: [anthropic]"))
    stack.start_worker()
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
    e = stack.explain(run)
    points(e, "step_failed", "policy_denied", "analyze", "2:audit_v1", 1,
           seq=record_seq(stack, run, "2:audit_v1", 1, "denied"), event=end_event(stack, run, "2:audit_v1"),
           says=("the policy denied analyze", 'provider \\"fake\\" not allowed for audit_v1'))
    report_agrees(stack, run, e)


def test_t12_a_budget_run_out(stack: Stack) -> None:
    stack.start_agentd()
    stack.start_worker()
    run = stack.start_run(budget_micros=1000)
    stack.wait_state(run, {"PAUSED_BUDGET"})
    e = stack.explain(run)
    points(e, "waiting", "budget_exhausted", "analyze", "2:audit_v1", 1,
           seq=record_seq(stack, run, "2:audit_v1", 1, "budget_exhausted"), event=end_event(stack, run, "2:audit_v1"),
           says=("budget ran out", "was left"))
    # What it says to do next is what lets the run go on.
    command = e["next"].split()
    assert command[:4] == ["agentctl", "budget", "extend", run["run_id"]] and command[4] == "--usd", e["next"]
    stack.agentctl(*command[1:])
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
    e = stack.explain(run)
    points(e, "ok")
    assert incidents(e) == [("budget_exhausted", "analyze", 1)], e["incidents"]
    [inc] = e["incidents"]
    extended = stack.ledger("SELECT seq FROM audit_log WHERE workflow_id = ? AND type = 'budget.extended'", run["run_id"])
    assert any(int(s["seq"]) == extended[0]["seq"] for s in inc["evidence"]) and "extended it by" in inc["cause"], inc
    report_agrees(stack, run, e)


def test_t12_the_worker_crashes(stack: Stack) -> None:
    """agentd's own Worker, paused while it holds a workflow task, then killed: agentd starts it again."""
    hold = stack.tmp / "hold-model"
    hold.touch()
    stack.start_agentd(faults=[{"name": "audit_v1", "hold_while_exists": str(hold)}], worker=True)
    w = stack.worker()
    run = stack.start_run()
    wait_for(lambda: len(stack.model_calls(run)) == 1, "the model call to start")
    w.pause()  # it keeps its polls: it takes the next workflow task, and never finishes it
    hold.unlink()

    def task_started_after_result() -> bool:
        events = stack.history(run)
        done = [i for i, ev in enumerate(events) if ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED]
        return len(done) == 2 and any(ev.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED for ev in events[done[1]:])
    wait_for(task_started_after_result, "the frozen Worker to take the workflow task", timeout=MODEL_TIMEOUT + 30)
    w.kill()
    w.wait_ready(2)
    assert stack.wait_state(run, TERMINAL, timeout=120)["state"] == "SUCCEEDED"
    assert stack.worker_processes() == [w.pid()]
    e = stack.explain(run)
    points(e, "ok")
    assert incidents(e) == [("worker_stopped", "analyze", 0)], e["incidents"]
    [timed_out] = [ev.event_id for ev in stack.history(run) if ev.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT]
    [inc] = e["incidents"]
    assert "stopped or hung" in inc["cause"] and [int(s["event_id"]) for s in inc["evidence"]] == [timed_out], inc
    report_agrees(stack, run, e)


def test_t12_agentd_crashes(stack: Stack) -> None:
    hold = stack.tmp / "hold-model"
    hold.touch()
    stack.start_agentd(faults=[{"name": "audit_v1", "attempt": 1, "hold_while_exists": str(hold)}])
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: len(stack.model_calls(run)) == 1, "the model call to start")
    stack.restart_agentd(kill=True)  # kill -9 during the call's first attempt
    hold.unlink()
    assert stack.wait_state(run, TERMINAL, timeout=180)["state"] == "SUCCEEDED"
    e = stack.explain(run)
    points(e, "ok")
    assert incidents(e) == [("agentd_stopped", "analyze", 1)], e["incidents"]
    [inc] = e["incidents"]
    # The attempt began (its call.started, written once the policy let the call by) and never ended,
    # and agentd started again after it: by the records' order, not inferred from the times.
    second = stack.ledger("SELECT seq FROM audit_log WHERE type = 'agentd.started' ORDER BY seq")[1]["seq"]
    [began] = [r["seq"] for r in stack.ledger("SELECT seq, payload FROM audit_log WHERE workflow_id = ? AND type = 'call.started'",
                                              run["run_id"])
               if (lambda p: (p["activity_id"], p["attempt"]) == ("2:audit_v1", 1))(json.loads(r["payload"]))]
    seqs = {int(s["seq"]) for s in inc["evidence"] if s["source"] == "audit"}
    assert {began, second} <= seqs, inc
    assert "began and never ended: agentd stopped during it" in inc["cause"], inc
    report_agrees(stack, run, e)


@pytest.mark.parametrize(("call", "step", "activity"), [("git.show_diff", "fetch_diff", "1:git.show_diff"),
                                                        ("git.apply_check", "validate_patch", "3:git.apply_check"),
                                                        ("github.open_draft_pr", "open_pr", "4:github.open_draft_pr")])
def test_t12_the_tool_server_crashes(stack: Stack, call: str, step: str, activity: str) -> None:
    # Killed in each step that runs it: fetching the diff, checking the patch, and reading the files
    # the pull request's patch changes. Each is told by the error's type, which agentd records.
    hold = stack.tmp / "hold-in-tool"
    stack.start_agentd(faults=[{"name": call, "attempt": 1, "hold_in_tool_while_exists": str(hold)}])
    stack.start_worker()
    hold.touch()
    run = stack.start_run()

    def server() -> int | None:
        for pid in descendants(stack.agentd.popen.pid):
            argv = command_of(pid).split()
            if argv and os.path.basename(argv[0]) == "agentos-mcp-git":
                return pid
        return None
    # The call has begun (the loop guard counts it first), and its server is up, held after its handshake.
    wait_for(lambda: stack.ledger("SELECT 1 FROM calls WHERE workflow_id = ? AND name = ?", run["run_id"], call),
             f"the run's {call}", timeout=120)
    pid = wait_for(server, f"the git tool server of the run's {call}")
    time.sleep(0.5)
    os.kill(pid, signal.SIGKILL)
    hold.unlink()
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
    e = stack.explain(run)
    points(e, "ok")
    assert incidents(e) == [("tool_server_failed", step, 1)], e["incidents"]
    [inc] = e["incidents"]
    seq = record_seq(stack, run, activity, 1, "error")
    assert any(s["source"] == "audit" and int(s["seq"]) == seq for s in inc["evidence"]), inc
    assert "the git tool server failed" in inc["cause"], inc
    [r] = [json.loads(r["payload"]) for r in stack.ledger("SELECT payload FROM audit_log WHERE seq = ?", seq)]
    assert r["error_cause"] == "tool_server", r
    report_agrees(stack, run, e)


@pytest.mark.parametrize("fault", ["missing key", "long integer", "its own type", "a type it cannot name"])
def test_t12_the_workflows_own_code_fails(stack: Stack, fault: str) -> None:
    # A result the workflow's code cannot take: a call's value without the key it reads, and a
    # model's line number of 5,000 digits, which Python's json refuses to read. Or its code raises an
    # exception of a type of its own, whose name the workflow sends with its report, or cannot. The
    # public report names only Python's built-in exceptions (and json's JSONDecodeError): any other
    # name the Worker could choose to carry something out. runs explain keeps the workflow's own words.
    workflows, error, sent, shown = None, None, True, True
    if fault == "missing key":
        stack.start_agentd(faults=[{"name": "git.show_diff", "status": "ok"}])
        step, activity, error = "fetch_diff", "1:git.show_diff", "KeyError"
    elif fault == "long integer":
        report = {"summary": "one finding", "patch": None, "findings": [{"severity": "high", "cwe": "CWE-89",
                  "file": "app.py", "line": "LINE", "title": "SQL injection", "explanation": "x"}]}
        answers = json.dumps([{"answers": [{"report": report}]}]).replace('"LINE"', "9" * 5000)
        stack.start_agentd(models=models_yaml("anthropic", stack.start_modelsim(answers=answers)), prices=PRICES, policy=POLICY)
        step, activity, error = "analyze", "2:audit_v1", "ValueError"
    else:
        stack.start_agentd()
        step, activity = "fetch_diff", "1:git.show_diff"
        if fault == "its own type":
            workflows, error, shown = "tests.t12_workflows:RaisesItsOwnError", "PatchUnreadable", False
        else:
            workflows, error, sent, shown = "tests.t12_workflows:RaisesAnUnnamedError", "Fehlerä", False, False
    stack.start_worker(**({"workflows": workflows} if workflows else {}))
    run = stack.start_run()
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "FAILED", final  # not RUNNING for good, its tasks failing
    e = stack.explain(run)
    points(e, "failed", "workflow_error", step, activity, 1, seq=record_seq(stack, run, activity, 1, "ok"),
           says=("own code failed after", "post_report went out"))
    assert error in e["claim"], e["claim"]
    body = report_agrees(stack, run, e)
    fenced = body.split("```")[1]
    [report] = [c for c in gateway_calls(stack.history(run)) if c["name"] == "github.comment_commit"]
    assert report["args"]["reason"] == "workflow_error", report["args"]
    assert (report["args"].get("error_type") == error) == sent and ("error_type" in report["args"]) == sent, report["args"]
    if shown:
        assert f"its own code failed ({error})" in fenced, body
    else:
        assert "its own code failed (an exception of another type)" in fenced and error not in body, body
    if fault == "long integer":
        assert "app.py:" + "9" * 20 + "…" in fenced, body  # the run's accepted report, whose line is cut


def test_t12_a_report_whose_result_cannot_be_read(stack: Stack) -> None:
    # The report's call returns a value the workflow's code cannot read (an id of 5,000 digits): the
    # run fails, with why, rather than its workflow tasks for good.
    digits = sys.get_int_max_str_digits()
    sys.set_int_max_str_digits(0)
    try:
        stack.start_agentd(faults=[{"name": "github.comment_commit", "status": "ok",
                                    "value": {"url": "https://github.example/c/1", "id": 10 ** 5000}}])
    finally:
        sys.set_int_max_str_digits(digits)
    stack.start_worker()
    run = stack.start_run()
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "FAILED", final
    e = stack.explain(run)
    assert (e["verdict"], e["kind"]) == ("failed", "workflow_error"), e
    assert "ValueError on its report's result" in e["claim"] and "post_report went out" in e["cause"], e
    failed = [ev for ev in stack.history(run) if ev.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED]
    assert not failed, failed


def test_t12_a_run_cancelled_while_it_reports_ends_cancelled(stack: Stack) -> None:
    # What goes wrong from the report call on fails the run; a cancel there is still a cancel.
    hold = stack.tmp / "hold-report"
    hold.touch()
    stack.start_agentd(faults=[{"name": "github.comment_commit", "hold_while_exists": str(hold)}])
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: stack.ledger("SELECT 1 FROM calls WHERE workflow_id = ? AND name = 'github.comment_commit'", run["run_id"]),
             "the report call", timeout=120)
    stack.agentctl("runs", "cancel", run["run_id"])
    hold.unlink()
    assert stack.wait_state(run, TERMINAL, timeout=120)["state"] == "CANCELLED"
    e = stack.explain(run)
    assert (e["verdict"], e["kind"]) == ("cancelled", "cancelled"), e


def test_t12_an_edit_the_validation_refuses(stack: Stack) -> None:
    # A reviewer's patch that touches a protected path: the check of it refuses it, and that check,
    # with its reason, is the step that changed the run's course, not the edit.
    stack.start_agentd(policy=approval_policy())
    stack.start_worker()
    run = stack.start_run()
    first = stack.wait_approval(run)
    path = stack.tmp / "edited.diff"
    path.write_text(EDITED)
    stack.decide(first, "edit", "--patch-file", str(path), "--as", "carol")
    assert stack.wait_state(run, TERMINAL, timeout=60)["state"] == "SUCCEEDED"
    e = stack.explain(run)
    points(e, "step_failed", "tool_denied", "validate_patch", "6:git.apply_check", 1, says=(".github",))
    report_agrees(stack, run, e)


def test_t12_a_stale_attempt_that_failed_late(stack: Stack) -> None:
    # T8's kind: the diff's first attempt hangs without heartbeats; Temporal times it out, and the second
    # returns ok. The first ends later, with an error, after the second's record: the run went on from
    # the second's result, and so do its explanation and its report, which the run posts once the late
    # record is in.
    stack.start_agentd(policy=approval_policy(), faults=[{"name": "git.show_diff", "attempt": 1, "delay": "45s",
                                                          "no_heartbeat": True, "tool_error": "a late failure"}])
    stack.start_worker()
    run = stack.start_run()
    item = stack.wait_approval(run, timeout=120)
    wait_for(lambda: len(stack.ledger("SELECT 1 FROM audit_log WHERE workflow_id = ? AND type = 'call' "
                                      "AND payload LIKE '%a late failure%'", run["run_id"])) == 1,
             "the first attempt's late record", timeout=90)
    stack.decide(item, "approve")
    assert stack.wait_state(run, TERMINAL, timeout=120)["state"] == "SUCCEEDED"
    e = stack.explain(run)
    points(e, "ok")
    assert incidents(e) == [("activity_timeout", "fetch_diff", 1)], e["incidents"]
    assert "ended later, with an error" in e["incidents"][0]["cause"], e["incidents"]
    report_agrees(stack, run, e)


def test_t12_a_stuck_run(stack: Stack) -> None:
    stack.start_agentd()
    stack.start_worker(workflows="tests.t12_workflows:FailsItsTasks")
    run = stack.start_run()
    e = wait_for(lambda: (x := stack.explain(run))["verdict"] == "stuck" and x, "the run to be stuck", timeout=90, interval=2)
    points(e, "stuck", "workflow_task_failed", "fetch_diff", "1:git.show_diff", 1, says=("keeps failing",))
    [failed] = [ev.event_id for ev in stack.history(run) if ev.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED]
    assert any(s["source"] == "temporal" and int(s["event_id"]) == failed for s in e["sources"]), e["sources"]
    assert "RuntimeError" in e["claim"] and e["state"] == "RUNNING", e


def test_t12_a_report_takes_nothing_from_the_workflow(stack: Stack) -> None:
    # An older Worker's outcome is taken, and not shown.
    stack.start_agentd()
    stack.start_worker(workflows="tests.t12_workflows:OldStyleReport")
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
    body = stack.report_comment(run)
    assert "security team" not in body and "evil" not in body and "@mallory" not in body, body
    # It made no call of the model: the run cannot have ended well, whatever it says.
    assert OUTCOME.search(body.split("```")[0]).groups() == ("step_failed", "analyze", "step_missing"), body


@pytest.mark.parametrize("base", ["main", "raises"])
def test_t12_a_call_the_workflow_made_up_is_not_named_in_the_report(stack: Stack, base: str) -> None:
    # A call of the workflow's own making, under a name that carries words of its own: agentd refuses
    # it, and the report, which is public, names it only as a call the task does not name, with
    # nothing of what the workflow chose. runs explain, which is local, names it.
    from tests.t12_workflows import CARRIED
    stack.start_agentd()
    stack.start_worker(workflows="tests.t12_workflows:MakesUpACall")
    run = stack.start_run(base=base)
    assert stack.wait_state(run, TERMINAL)["state"] == ("FAILED" if base == "raises" else "SUCCEEDED")
    body = stack.report_comment(run)
    assert CARRIED not in body and "xxxx" not in body, body
    fenced = body.split("```")[1]
    e = stack.explain(run)
    assert CARRIED in e["step"] and e["activity_id"].startswith("2:" + CARRIED), e
    if base == "raises":
        assert (e["verdict"], e["kind"]) == ("failed", "workflow_error"), e
        assert OUTCOME.search(body.split("```")[0]).groups() == ("failed", None, "workflow_error"), body
        assert "its code failed after a call the task does not name returned denied" in fenced, body
    else:
        assert (e["verdict"], e["kind"]) == ("step_failed", "denied") and "no such tool" in e["cause"], e
        assert OUTCOME.search(body.split("```")[0]).groups() == ("step_failed", None, "denied"), body
        assert "Outcome: agentd refused a call the task does not name\n" in fenced, body


@pytest.mark.parametrize("base", ["main", "other-commit"])
def test_t12_a_report_that_is_not_the_runs_own_is_refused(stack: Stack, base: str) -> None:
    # The diff as the report, or another commit as the target.
    stack.start_agentd()
    stack.start_worker(workflows="tests.t12_workflows:ForeignReport")
    run = stack.start_run(base=base)
    final = stack.wait_state(run, TERMINAL)
    assert json.loads(final["result_json"])["status"] == "denied", final
    assert stack.external(run, "github.comment_commit") == []
    e = stack.explain(run)
    points(e, "step_failed", "report_not_posted", "post_report", "3:github.comment_commit", 1,
           says=("not the run's latest accepted answer of audit_v1",) if base == "main" else ("commit is not this run's",))


def test_t12_a_run_agentd_did_not_start(stack: Stack) -> None:
    stack.start_agentd()
    r = stack.agentctl_run("runs", "explain", "0f8b1c2e-1111-4222-8333-444455556666")
    assert r.returncode == 1 and "no run" in r.stderr, (r.returncode, r.stderr)


def test_t12_a_hostile_history_is_explained_within_bounds(stack: Stack) -> None:
    stack.start_agentd()
    stack.start_worker(workflows="tests.t12_workflows:Floods")
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL, timeout=180)["state"] == "SUCCEEDED"
    r = stack.agentctl_run("runs", "explain", run["run_id"])
    assert r.returncode == 0 and len(r.stdout) < 64 << 10, (r.returncode, len(r.stdout), r.stderr)
    e = json.loads(r.stdout)
    # It never reported; before that, the loop guard refused its calls.
    assert (e["verdict"], e["kind"], e["step"]) == ("step_failed", "report_not_posted", "post_report"), e
    [guard] = [i for i in e["incidents"] if i["kind"] == "loop_guard"]
    assert guard["step"] == "fetch_diff" and "before that" in guard["cause"], guard
    assert r"\x1b[2J" in guard["activity_id"] and len(guard["activity_id"]) <= 200, guard["activity_id"]
    text = explain_text(stack, run)
    assert "\x1b" not in text and "\x07" not in text and "\r" not in text, repr(text[:500])
    labels = ("run id", "state", "verdict", "step", "kind", "cause", "workflow says", "next", "cost", "usage", "evidence",
              "incidents", "missing", " ")
    assert all(line.startswith(labels) for line in text.splitlines()), text
    assert stack.agentd.alive()


def test_t12_a_cancellation_in_the_workflows_code_posts_no_report(stack: Stack) -> None:
    # Temporal's CancelledError raised in the workflow's own code is a cancellation, in either of its
    # forms (asyncio's is tests/test_cancel.py's): not an error of its code to report.
    stack.start_agentd()
    stack.start_worker(workflows="tests.t12_workflows:CancelledInItsCode")
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL)["state"] == "FAILED"
    assert stack.external(run, "github.comment_commit") == []
    assert not [c for c in gateway_calls(stack.history(run)) if c["name"] == "github.comment_commit"]


@pytest.mark.audit_verify(skip="agentd refuses to start, by design")
def test_t12_agentd_refuses_models_its_task_cannot_be_held_to(stack: Stack) -> None:
    # A run ended well once it validated and opened the patch the audit proposed: what the task's
    # rule reads (patch_ref) is there only while models.yaml keeps the patch a blob.
    models = re.sub(r",? *blobs: \[patch\]", "", TEST_MODELS)
    assert models != TEST_MODELS
    with pytest.raises(AssertionError, match=r"holds patch_ref, which model task audit_v1 does not produce"):
        stack.start_agentd(models=models)
