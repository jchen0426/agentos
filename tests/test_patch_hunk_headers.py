"""A model's patch whose hunk headers its own lines contradict, as most of Claude's were in the
first evaluation: the counts of lines are wrong, and so is where a hunk starts. agentd reads each
hunk by its lines (internal/patch): the check passes, and the pull request's commit holds what the
lines say, nothing more or less. A hunk whose lines are not in the file is still refused. The
reviewer sees the patch as agentd applies it, approves the payload built from it, and may edit it
with headers as wrong as the model's."""

import json

from tests.harness import AUDIT_SCHEMA, TERMINAL, TEST_POLICY, Stack, approval_policy, gateway_calls

PR = "github.open_draft_pr"
# The simulated Anthropic API answers the audit with the scripted patch.
MODELS = """
providers:
  anthropic: {{adapter: anthropic, base_url: "{url}"}}
tasks:
  audit_v1: {{model: anthropic/claude-opus-5-5, max_tokens: 4096, output_schema: {schema}, blobs: [patch]}}
"""
PRICES = """
price_table_version: hunk-headers
models:
  anthropic/claude-opus-5-5: {input: 1.00, output: 5.00}
"""
# A fix of the fixture's SQL injection, as a model writes one. app/db.py's header counts more lines
# than the hunk has and puts it at line 3 of a file of two; app.py's counts fewer, and git apply and
# go-gitdiff would both have left out its last line without a word.
MISCOUNTED = """--- a/app/db.py
+++ b/app/db.py
@@ -3,7 +3,5 @@ def find_user(conn, name):
 def find_user(conn, name):
-    return conn.execute("SELECT * FROM users WHERE name = '%s'" % name)
+    # Bind the name: never format it into the SQL text.
+    return conn.execute("SELECT * FROM users WHERE name = ?", (name,))
--- a/app.py
+++ b/app.py
@@ -1 +1,2 @@
 print('hello')
+print('the audit fixed app/db.py')
+print('bye')
"""
# The patch as agentd applies it, which the reviewer reads: git's file headers, and the hunks'
# headers as their lines say (the heading of the hunk that moved is the model's, and goes).
AS_APPLIED = ("diff --git a/app/db.py b/app/db.py\n" + MISCOUNTED.split("--- a/app.py\n")[0]
              .replace("@@ -3,7 +3,5 @@ def find_user(conn, name):", "@@ -1,2 +1,3 @@")
              + "diff --git a/app.py b/app.py\n--- a/app.py\n" + MISCOUNTED.split("--- a/app.py\n")[1]
              .replace("@@ -1 +1,2 @@", "@@ -1,1 +1,3 @@"))
DB = ("def find_user(conn, name):\n    # Bind the name: never format it into the SQL text.\n"
      '    return conn.execute("SELECT * FROM users WHERE name = ?", (name,))\n')
APP = "print('hello')\nprint('the audit fixed app/db.py')\nprint('bye')\n"
REWRITTEN = [{"file": "app/db.py", "hunk": 1, "was": "@@ -3,7 +3,5 @@", "now": "@@ -1,2 +1,3 @@"},
             {"file": "app.py", "hunk": 1, "was": "@@ -1 +1,2 @@", "now": "@@ -1,1 +1,3 @@"}]


def report(patch: str) -> dict:
    return {"summary": "find_user formats the name into its SQL.", "patch": patch, "findings": [{
        "severity": "high", "cwe": "CWE-89", "file": "app/db.py", "line": 2, "title": "SQL built with string formatting",
        "explanation": "A crafted name changes the query."}]}


def start(stack: Stack, patch: str, policy: str = TEST_POLICY) -> dict:
    url = stack.start_modelsim([{"answers": [{"report": report(patch)}] * 3}])
    stack.start_agentd(models=MODELS.format(url=url, schema=AUDIT_SCHEMA), prices=PRICES, policy=policy)
    stack.start_worker()
    return stack.start_run()


def results(stack: Stack, run: dict, name: str) -> list[dict]:
    """What the run's calls of a tool returned: status and value."""
    out = []
    for c in gateway_calls(stack.history(run)):
        if c["name"] == name and c["completed"] is not None:
            r = json.loads(c["completed"].activity_task_completed_event_attributes.result.payloads[0].data)
            out.append({"status": r.get("status"), "value": json.loads(r.get("valueJson") or "{}")})
    return out


def tree(stack: Stack) -> list[dict]:
    """The entries of the one tree the pull request's commit was made with."""
    [t] = [e for e in stack.github_requests("create_tree") if e["status"] == 201]
    return sorted(t["tree"], key=lambda e: e["path"])


def github_writes(stack: Stack) -> list[str]:
    return [e["action"] for e in stack.github_requests() if e["method"] == "POST" and e["status"] < 300]


def test_a_patch_with_wrong_hunk_headers_opens_the_pull_request_its_lines_say(stack: Stack) -> None:
    run = start(stack, MISCOUNTED)
    final = stack.wait_state(run, TERMINAL)
    assert final["state"] == "SUCCEEDED", final
    [pr] = stack.external(run, PR)
    result = json.loads(final["result_json"])
    assert result["outcome"] == "ok" and result["pr_url"].endswith(f"/pull/{pr['number']}"), final
    # The check passed, and said that the patch was not quite as written.
    assert results(stack, run, "git.apply_check") == [
        {"status": "ok", "value": {"applies": True, "files": ["app/db.py", "app.py"], "rewritten_hunks": 2}}]
    # The commit holds exactly what the hunks' lines say: app.py's last line too.
    assert tree(stack) == [{"path": "app.py", "mode": "100644", "type": "blob", "content": APP},
                           {"path": "app/db.py", "mode": "100644", "type": "blob", "content": DB}], tree(stack)


def test_a_patch_whose_lines_are_not_in_the_file_is_still_refused(stack: Stack) -> None:
    # A context line the file does not have: wherever the hunk goes, it does not apply.
    run = start(stack, MISCOUNTED.replace(" def find_user(conn, name):\n", " def find_user(db, name):\n"))
    final = stack.wait_state(run, TERMINAL)
    assert final["state"] == "SUCCEEDED", final
    assert json.loads(final["result_json"]) == {"outcome": "patch rejected", "pr_url": None}, final
    [check] = results(stack, run, "git.apply_check")
    assert check["status"] == "denied" and check["value"]["reason"].startswith(
        "app/db.py: the patch does not apply: the lines of hunk 1 (its context and the lines it removes) "
        "are not in the file at the commit"), check
    assert github_writes(stack) == ["create_comment"], github_writes(stack)  # the report alone


def test_the_reviewer_sees_approves_and_edits_the_patch_as_applied(stack: Stack) -> None:
    run = start(stack, MISCOUNTED, policy=approval_policy())
    first = stack.wait_approval(run)
    card = stack.approval(first["id"])
    write = card["context"]["write"]
    assert write["patch"] == AS_APPLIED and write["patch_rewritten"] == REWRITTEN, write
    assert write["apply_check"] == {"applies": True}, write
    assert any("agentd rewrote 2 of the patch's hunk headers" in n for n in write["notes"]), write["notes"]
    # What the payload holds, and what the approval is bound to, is the patch as applied.
    files = {f["path"]: f["content"] for f in card["payload"]["files"]}
    assert files == {"app/db.py": DB, "app.py": APP}, files

    # The reviewer writes another fix by hand, and gets the header wrong as people do: it counts
    # one line too few, and go-gitdiff would have left the last line out.
    edited = ("--- a/app/db.py\n+++ b/app/db.py\n@@ -1,2 +1,2 @@\n def find_user(conn, name):\n"
              "-    return conn.execute(\"SELECT * FROM users WHERE name = '%s'\" % name)\n"
              "+    # Never format the name into the SQL text.\n"
              '+    return conn.execute("SELECT * FROM users WHERE name = :name", {"name": name})\n')
    path = stack.tmp / "edited.diff"
    path.write_text(edited)
    assert stack.decide(first, "edit", "--patch-file", str(path), "--as", "carol")["status"] == "EDITED"
    second = stack.wait_approval(run, count=2)
    card = stack.approval(second["id"])
    write = card["context"]["write"]
    assert write["patch"] == "diff --git a/app/db.py b/app/db.py\n" + edited.replace("@@ -1,2 +1,2 @@", "@@ -1,2 +1,3 @@"), write
    assert write["patch_rewritten"] == [{"file": "app/db.py", "hunk": 1, "was": "@@ -1,2 +1,2 @@", "now": "@@ -1,2 +1,3 @@"}]
    want = ("def find_user(conn, name):\n    # Never format the name into the SQL text.\n"
            '    return conn.execute("SELECT * FROM users WHERE name = :name", {"name": name})\n')
    assert [(f["path"], f["content"]) for f in card["payload"]["files"]] == [("app/db.py", want)], card["payload"]
    assert github_writes(stack) == []

    assert stack.decide(second, "approve")["status"] == "APPROVED"
    final = stack.wait_state(run, TERMINAL, timeout=60)
    assert final["state"] == "SUCCEEDED", final
    [pr] = stack.external(run, PR)
    assert json.loads(final["result_json"])["pr_url"].endswith(f"/pull/{pr['number']}"), final
    assert tree(stack) == [{"path": "app/db.py", "mode": "100644", "type": "blob", "content": want}], tree(stack)
