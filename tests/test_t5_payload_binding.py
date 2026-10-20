"""T5: an approval holds for the payload it was given for (design doc, sections 10 and 11). What
goes out is the payload agentd recorded, which must hash to the one approved: changed after the
approval by a single byte, it is never sent. A reviewer's edit goes back to the workflow, which
checks it again; the edited write needs an approval of its own, and the payload first asked for
is never sent, even when the workflow asks for it once the edit is approved."""

import hashlib
import json
import sqlite3

from tests.harness import TERMINAL, Stack, approval_policy, gateway_calls

PR = "github.open_draft_pr"
# Another fix for the fixture's SQL injection than the fake model's: a named parameter.
EDITED = """diff --git a/app/db.py b/app/db.py
--- a/app/db.py
+++ b/app/db.py
@@ -1,2 +1,2 @@
 def find_user(conn, name):
-    return conn.execute("SELECT * FROM users WHERE name = '%s'" % name)
+    return conn.execute("SELECT * FROM users WHERE name = :name", {"name": name})
"""
EDITED_REF = "sha256:" + hashlib.sha256(EDITED.encode()).hexdigest()
MODEL_LINE = 'conn.execute("SELECT * FROM users WHERE name = ?", (name,))'


def github_writes(stack: Stack) -> list[dict]:
    return [e for e in stack.github_requests() if e["method"] == "POST" and e["status"] < 300 and e["action"] != "create_comment"]


def report(stack: Stack, run: dict) -> str:
    marker = f"<!-- agent-effect:{stack.effect(run, 'github.comment_commit')['effect_id']} -->"
    [body] = [c["body"] for c in stack.github_comments() if c["body"].rstrip().endswith(marker)]
    return body


def edit(stack: Stack, approval: dict, patch: str) -> dict:
    path = stack.tmp / "edited.diff"
    path.write_text(patch)
    return stack.decide(approval, "edit", "--patch-file", str(path), "--as", "carol")


def test_t5_a_payload_changed_after_the_approval_is_not_sent(stack: Stack) -> None:
    hold = stack.tmp / "hold-doorbells"
    stack.start_agentd(policy=approval_policy(), hold_doorbells=hold)
    stack.start_worker()
    run = stack.start_run()
    item = stack.wait_approval(run)
    hold.touch()  # the run sleeps on: agentd stops with the approval recorded and nothing sent
    assert stack.decide(item, "approve")["status"] == "APPROVED"
    stack.agentd.stop()

    # One byte of the payload in agentd's ledger changes.
    db = sqlite3.connect(stack.data_dir / "ledger.db")
    [(payload,)] = db.execute("SELECT bytes FROM blobs WHERE sha256 = ?", (item["payload_sha256"],)).fetchall()
    changed = payload.replace(b"security fix", b"security fiX", 1)
    assert len(changed) == len(payload) and sum(a != b for a, b in zip(changed, payload)) == 1
    db.execute("UPDATE blobs SET bytes = ? WHERE sha256 = ?", (changed, item["payload_sha256"]))
    db.commit()
    db.close()
    hold.unlink()
    stack.restart_agentd()

    final = stack.wait_state(run, TERMINAL, timeout=60)
    assert final["state"] == "SUCCEEDED", final  # the report went out
    assert json.loads(final["result_json"]) == {"outcome": "denied", "pr_url": None}, final
    assert github_writes(stack) == [], github_writes(stack)
    effect = stack.effect(run, PR)
    assert effect["status"] == "ABANDONED" and "is not the payload approved" in effect["reason"], dict(effect)
    assert any(e["msg"] == "an approved payload changed; not sending it" for e in stack.agentd_log())
    # Nor will agentd show it as the payload waiting: it no longer hashes to its own record.
    shown = stack.agentctl_run("approvals", "show", item["id"])
    assert shown.returncode != 0 and "does not hash to" in shown.stderr, shown.stderr


def test_t5_an_edit_is_checked_again_and_needs_its_own_approval(stack: Stack) -> None:
    stack.start_agentd(policy=approval_policy())
    stack.start_worker()
    run = stack.start_run()
    first = stack.wait_approval(run)
    assert edit(stack, first, EDITED)["status"] == "EDITED"

    second = stack.wait_approval(run, count=2)
    assert second["id"] != first["id"]
    card = stack.approval(second["id"])
    assert (card["status"], card["supersedes"]) == ("PENDING", first["id"]), card
    write = card["context"]["write"]
    assert write["patch"] == EDITED and write["apply_check"] == {"applies": True}, write
    [f] = card["payload"]["files"]
    assert ':name", {"name": name})' in f["content"], f
    old = stack.approval(first["id"])
    assert (old["status"], old["superseded_by"], old["decided_by"]) == ("SUPERSEDED", second["id"], "carol"), old
    assert old["edited_patch"] == EDITED, old
    # The workflow checked the reviewer's patch before it asked to open the PR with it.
    calls = [(c["name"], c["args"].get("patch_ref")) for c in gateway_calls(stack.history(run))]
    check = calls.index(("git.apply_check", EDITED_REF))
    assert (PR, EDITED_REF) in calls[check + 1:], calls
    assert github_writes(stack) == []

    assert stack.decide(second, "approve")["status"] == "APPROVED"
    final = stack.wait_state(run, TERMINAL, timeout=60)
    assert final["state"] == "SUCCEEDED", final
    [pr] = stack.external(run, PR)
    assert json.loads(final["result_json"])["pr_url"].endswith(f"/pull/{pr['number']}"), final
    # GitHub got the reviewer's fix, and never the model's.
    contents = [e.get("content") or "" for t in stack.github_requests("create_tree") for e in t["tree"]]
    assert any(':name", {"name": name})' in c for c in contents) and not any(MODEL_LINE in c for c in contents), contents
    statuses = sorted((e["status"], e["payload_sha256"] == first["payload_sha256"]) for e in stack.effects(run) if e["tool"] == PR)
    assert statuses == [("ABANDONED", True), ("SUCCEEDED", False)], statuses
    body = report(stack, run)
    assert "Approval of github.open_draft_pr: superseded" in body and "Approval of github.open_draft_pr: approved" in body, body


def test_t5_an_edit_its_checks_refuse_opens_nothing(stack: Stack) -> None:
    stack.start_agentd(policy=approval_policy())
    stack.start_worker()
    run = stack.start_run()
    first = stack.wait_approval(run)
    edit(stack, first, EDITED.replace("app/db.py", ".github/workflows/ci.yml"))

    final = stack.wait_state(run, TERMINAL, timeout=60)
    assert json.loads(final["result_json"]) == {"outcome": "patch rejected", "pr_url": None}, final
    assert github_writes(stack) == []
    [row] = stack.ledger("SELECT * FROM approvals WHERE workflow_id = ?", run["run_id"])  # no second approval
    assert row["status"] == "EDITED" and stack.effect(run, PR)["status"] == "AWAITING_APPROVAL", dict(row)


def test_t5_only_the_approved_payload_goes_out(stack: Stack) -> None:
    """The workflow has the reviewer's edit approved, then asks for its own patch instead."""
    stack.start_agentd(policy=approval_policy())
    stack.start_worker(workflows="tests.rogue_workflows:SwapsThePatch")
    run = stack.start_run()
    first = stack.wait_approval(run)
    edit(stack, first, EDITED)
    second = stack.wait_approval(run, count=2)
    stack.decide(second, "approve")

    final = stack.wait_state(run, TERMINAL, timeout=60)
    assert final["state"] == "SUCCEEDED", final
    assert json.loads(final["result_json"]) == {"statuses": ["edited", "needs_approval", "edited"]}, final
    assert github_writes(stack) == []  # neither patch: the approved one was never asked for again
    rows = {r["approval_id"]: r for r in stack.ledger("SELECT * FROM approvals WHERE workflow_id = ?", run["run_id"])}
    assert rows[first["id"]]["status"] == "SUPERSEDED" and rows[second["id"]]["status"] == "APPROVED"
    assert rows[second["id"]]["used_at"] is None
