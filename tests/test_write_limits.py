"""A run writes each kind of external effect once unless the policy allows more (max_per_run,
default 1). A workflow that posts reports the comment tells apart (another reason, another error
type the report names) gets "denied" from the second write on: the outside world sees one report.
Reports that differ only in what the comment does not show are one effect, and one comment."""

import json

from tests.harness import TERMINAL, TEST_POLICY, Stack


def test_a_run_posts_one_report(stack: Stack) -> None:
    stack.start_agentd()
    stack.start_worker(workflows="tests.rogue_workflows:ManyReports")
    run = stack.start_run()
    final = stack.wait_state(run, TERMINAL)
    assert final["state"] == "SUCCEEDED", final
    assert json.loads(final["result_json"]) == {"statuses": ["ok", "denied", "denied"]}, final
    assert len(stack.external(run, "github.comment_commit")) == 1


def test_the_policy_can_allow_more(stack: Stack) -> None:
    policy = TEST_POLICY.replace("github.comment_commit: {decision: allow, effect: lookup}",
                                 "github.comment_commit: {decision: allow, effect: lookup, max_per_run: 2}")
    assert policy != TEST_POLICY
    stack.start_agentd(policy=policy)
    stack.start_worker(workflows="tests.rogue_workflows:ManyReports")
    run = stack.start_run()
    final = stack.wait_state(run, TERMINAL)
    assert json.loads(final["result_json"]) == {"statuses": ["ok", "ok", "denied"]}, final
    assert len(stack.external(run, "github.comment_commit")) == 2


def test_reports_the_comment_does_not_tell_apart_are_one(stack: Stack) -> None:
    # An older Worker's outcome, pr_url and an error type the report does not name name no effect:
    # the three calls are the first one's effect, each answered with its comment.
    stack.start_agentd()
    stack.start_worker(workflows="tests.rogue_workflows:SameReportThrice")
    run = stack.start_run()
    final = stack.wait_state(run, TERMINAL)
    assert final["state"] == "SUCCEEDED", final
    assert json.loads(final["result_json"]) == {"statuses": ["ok", "ok", "ok"]}, final
    effects = [e for e in stack.effects(run) if e["tool"] == "github.comment_commit"]
    assert len(effects) == 1, effects
    # One comment, found by that one effect's marker.
    assert len(stack.external(run, "github.comment_commit")) == 1
