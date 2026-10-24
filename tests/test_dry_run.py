"""Dry runs (design doc, sections 11 and 12): the evaluation runs on the same runtime with the
write tools rendering and recording what they would send, and sending nothing. The same holds
for an agentd without GitHub configured."""

import json

from tests.harness import TERMINAL, Stack, approval_policy

WRITES = ("github.open_draft_pr", "github.comment_commit")


def assert_dry(stack: Stack, run: dict) -> None:
    final = stack.wait_state(run, TERMINAL)
    assert final["state"] == "SUCCEEDED", final
    assert json.loads(final["result_json"])["pr_url"].startswith("dry-run://"), final
    effects = [e for e in stack.effects(run) if e["tool"] in WRITES]
    assert {e["tool"] for e in effects} == set(WRITES) and all(e["status"] == "DRY_RUN" for e in effects), \
        [dict(e) for e in effects]
    assert [e for e in stack.github_requests() if e["method"] == "POST"] == []


def test_a_dry_run_sends_nothing(stack: Stack) -> None:
    stack.start_agentd()
    stack.start_worker()
    run = stack.start_run(dry_run=True)
    assert run["dry_run"] is True
    assert_dry(stack, run)
    # What would have been sent is on record, rendered in full.
    payload_ref = stack.effect(run, "github.comment_commit")["payload_blob"]
    [blob] = stack.ledger("SELECT bytes FROM blobs WHERE sha256 = ?", payload_ref.removeprefix("sha256:"))
    assert "agentos security audit" in json.loads(blob["bytes"])["body"]


def test_without_github_configured_nothing_is_sent(stack: Stack) -> None:
    stack.start_agentd(extra={"github": None})
    stack.start_worker()
    assert_dry(stack, stack.start_run())


def test_a_dry_run_asks_for_no_approval(stack: Stack) -> None:
    """Nothing a dry run writes goes out, so nothing in it waits for a person (section 10)."""
    stack.start_agentd(policy=approval_policy())
    stack.start_worker()
    assert_dry(stack, stack.start_run(dry_run=True))
    assert stack.ledger("SELECT * FROM approvals") == []


def test_a_live_runs_write_that_was_only_dry_run_did_not_go_out(stack: Stack) -> None:
    """A run that is not a dry run, whose approved pull request an agentd without GitHub takes up: that
    agentd can only dry-run it, so it did not reach GitHub, and the run did not end well (dry_ran)."""
    hold = stack.tmp / "hold-doorbells"
    stack.start_agentd(policy=approval_policy(), hold_doorbells=hold)
    stack.start_worker()
    run = stack.start_run()
    item = stack.wait_approval(run)
    hold.touch()  # the approval's doorbell waits for the next agentd
    stack.decide(item, "approve")
    stack.agentd.kill()
    hold.unlink()
    stack.start_agentd(policy=approval_policy(), extra={"github": None})
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED" and json.loads(final["result_json"])["pr_url"].startswith("dry-run://"), final
    writes = {e["tool"]: e["status"] for e in stack.effects(run) if e["tool"] in WRITES}
    assert writes == {w: "DRY_RUN" for w in WRITES}, writes
    assert [e for e in stack.github_requests() if e["method"] == "POST"] == []
    e = stack.explain(run)
    assert (e["verdict"], e["kind"], e["step"]) == ("step_failed", "dry_ran", "open_pr"), e
    assert "went out" not in e["cause"] and "dry-run" in e["cause"], e["cause"]
