"""T6: two reviewers decide on one approval at once, one approving and one rejecting (design doc,
sections 10 and 11). Only the first valid decision counts, decided in one SQLite transaction; the
other is told who decided what, and when. The run follows the decision that counted. The same
decision sent again under its idempotency key is that decision, not a conflict."""

import json
import subprocess

from tests.harness import TERMINAL, Stack, approval_policy

PR = "github.open_draft_pr"
REASON = "bob: the fix belongs in the query builder"


def report(stack: Stack, run: dict) -> str:
    marker = f"<!-- agent-effect:{stack.effect(run, 'github.comment_commit')['effect_id']} -->"
    [body] = [c["body"] for c in stack.github_comments() if c["body"].rstrip().endswith(marker)]
    return body


def test_t6_only_the_first_decision_counts(stack: Stack) -> None:
    stack.start_agentd(policy=approval_policy())
    stack.start_worker()
    runs = [stack.start_run() for _ in range(4)]
    items = [stack.wait_approval(r) for r in runs]

    decisions = []
    for it in items:
        sha = ("--payload-sha256", it["payload_sha256"])
        approve = ["approvals", "approve", it["id"], *sha, "--as", "alice", "--idempotency-key", f"alice-{it['id']}"]
        reject = ["approvals", "reject", it["id"], *sha, "--as", "bob", "--reason", REASON, "--idempotency-key", f"bob-{it['id']}"]
        procs = [subprocess.Popen(stack.agentctl_args(*args), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                 for args in (approve, reject)]
        decisions.append((approve, reject, procs))

    for run, it, (approve, reject, procs) in zip(runs, items, decisions):
        ended = [(p.wait(timeout=60), p.stdout.read(), p.stderr.read()) for p in procs]
        outcomes = list(zip(ended, [("APPROVED", "alice", approve), ("REJECTED", "bob", reject)]))
        won = [(o, d) for o, d in outcomes if o[0] == 0]
        lost = [(o, d) for o, d in outcomes if o[0] != 0]
        assert len(won) == 1 and len(lost) == 1, outcomes
        (_, out, _), (status, by, args) = won[0]
        assert (json.loads(out)["status"], json.loads(out)["decided_by"]) == (status, by), out
        assert f"already decided: {status} by {by} at" in lost[0][0][2], lost
        [row] = stack.ledger("SELECT * FROM approvals WHERE approval_id = ?", it["id"])
        assert (row["status"], row["decided_by"]) == (status, by), dict(row)

        # Sent again under its key, the winning decision is what it recorded; the other still loses.
        assert stack.agentctl(*args)["status"] == status
        again = stack.agentctl_run(*lost[0][1][2])
        assert again.returncode != 0 and f"already decided: {status} by {by} at" in again.stderr, again.stderr

        final = stack.wait_state(run, TERMINAL, timeout=60)
        assert final["state"] == "SUCCEEDED", final
        result = json.loads(final["result_json"])
        if status == "APPROVED":
            [pr] = stack.external(run, PR)
            assert result["outcome"] == "ok" and result["pr_url"].endswith(f"/pull/{pr['number']}"), result
        else:
            assert stack.external(run, PR) == [] and result == {"outcome": "rejected", "pr_url": None}, result
            body = report(stack, run)
            assert "Approval of github.open_draft_pr: rejected" in body and REASON in body, body
