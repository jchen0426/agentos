"""Dry runs (design doc, sections 11 and 12): the evaluation runs on the same runtime with the
write tools rendering and recording what they would send, and sending nothing. The same holds
for an agentd without GitHub configured."""

import json

from tests.harness import TERMINAL, Stack

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
