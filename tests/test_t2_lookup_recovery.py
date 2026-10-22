"""T2: a lookup write has taken effect on GitHub (the draft PR is open, the report is posted),
and agentd is killed before it records the result. When agentd starts again it finds the PR by
its head branch, or the comment by its marker, records it, and does not write it again (design
doc, section 6). Against the simulated GitHub; one run against the real test repository comes
at the end of M3b."""

import json

from tests.harness import TERMINAL, Stack, wait_for

PR = "github.open_draft_pr"
COMMENT = "github.comment_commit"


def run_with_a_hold(stack: Stack, tool: str, fault: str) -> tuple[dict, object]:
    hold = stack.tmp / "hold"
    hold.touch()
    stack.start_agentd(faults=[{"name": tool, "attempt": 1, fault: str(hold)}])
    stack.start_worker()
    return stack.start_run(), hold


def succeeded_with_one_pr(stack: Stack, run: dict) -> None:
    final = stack.wait_state(run, TERMINAL, timeout=150)
    assert final["state"] == "SUCCEEDED", final
    [pr] = stack.external(run, PR)  # opened once
    assert json.loads(final["result_json"])["pr_url"].endswith(f"/pull/{pr['number']}"), final
    assert stack.effect(run, PR)["status"] == "SUCCEEDED"
    assert len(stack.external(run, COMMENT)) == 1


def test_t2_the_pr_was_opened_before_the_result_was_recorded(stack: Stack) -> None:
    run, hold = run_with_a_hold(stack, PR, "hold_before_record_while_exists")
    wait_for(lambda: stack.external(run, PR), "the PR to open on GitHub", timeout=60)
    stack.restart_agentd(kill=True)  # GitHub has the PR; agentd's ledger does not
    hold.unlink()
    succeeded_with_one_pr(stack, run)
    effect = stack.effect(run, PR)
    recovered = [e for e in stack.agentd_log() if e["msg"] == "effect recovered" and e.get("effect_id") == effect["effect_id"]]
    assert recovered and recovered[0]["status"] == "ok", recovered
    assert not [e for e in stack.agentd_log() if e["msg"] == "sending an effect again"]
    # agentd's lookup has its record before it went to GitHub, and what it found is in the records
    # after: the record of the lookup's request, then the effect's settlement.
    rows = [{**dict(r), "data": json.loads(r["payload"])} for r in stack.ledger("SELECT * FROM audit_log ORDER BY seq")]
    mine = [r for r in rows if r["data"].get("effect_id") == effect["effect_id"]]
    reads = [r for r in mine if r["type"] == "agentd.read" and r["data"]["purpose"] == "lookup"]
    settled = [r for r in mine if r["type"] == "effect.settled"]
    assert reads and settled and reads[0]["seq"] < settled[-1]["seq"], mine
    assert {(r["actor"], r["data"]["tool"], r["data"]["method"], r["data"]["path"]) for r in reads} == {
        ("agentd", PR, "GET", "/repos/acme/payments/pulls")}, reads


def test_t2_agentd_stopped_after_claiming_before_sending(stack: Stack) -> None:
    run, hold = run_with_a_hold(stack, PR, "hold_after_claim_while_exists")
    wait_for(lambda: any(e["claimed_by"] for e in stack.effects(run)), "the PR to be claimed", timeout=60)
    stack.restart_agentd(kill=True)  # nothing reached GitHub, but agentd cannot know that
    hold.unlink()
    succeeded_with_one_pr(stack, run)
    # On start agentd found nothing, waited until a request in flight could no longer land, and
    # left the PR to the run's next call, which opened it once.
    assert [e for e in stack.agentd_log() if e["msg"] == "the request never took effect; the run's next call sends it"]


def test_t2_the_report_was_posted_before_the_result_was_recorded(stack: Stack) -> None:
    run, hold = run_with_a_hold(stack, COMMENT, "hold_before_record_while_exists")
    wait_for(lambda: stack.external(run, COMMENT), "the report to be posted", timeout=60)
    stack.restart_agentd(kill=True)
    hold.unlink()
    final = stack.wait_state(run, TERMINAL, timeout=150)
    assert final["state"] == "SUCCEEDED", final
    assert len(stack.external(run, COMMENT)) == 1  # found by its marker, not posted again
    assert stack.effect(run, COMMENT)["status"] == "SUCCEEDED"


def test_t2_githubs_answer_is_lost(stack: Stack) -> None:
    """No crash: GitHub opens the PR but its answer never arrives (the request times out).
    agentd looks the PR up instead of opening a second one."""
    hold = stack.tmp / "hold-answer"
    hold.touch()
    stack.hold_github("create_pull", hold)
    stack.start_agentd()
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: [e for e in stack.agentd_log() if e["msg"] == "effect outcome unknown"],
             "agentd to give up waiting for GitHub's answer", timeout=60)
    final = stack.wait_state(run, TERMINAL, timeout=60)
    hold.unlink()
    assert final["state"] == "SUCCEEDED", final
    assert len(stack.external(run, PR)) == 1
