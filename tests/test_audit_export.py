"""agentctl audit export (design doc, section 4: the audit export merges Temporal's history with
agentd's ledger): one JSON document that a reader checks without agentd. The test recomputes every
exported record's sha256 and the chain's links with code of its own, and checks every anchor
against the exported history: the event that carries it, the record it names, and the sha256 of
the result in that record, from the bytes the anchor holds; and each head agentd pushed to Temporal
against the record at its seq."""

import base64
import hashlib
import json
import subprocess

from temporalio.client import WorkflowHistory

from tests.harness import BIN, TERMINAL, Stack, approval_policy, wait_for
from tests.test_audit_log import result_sha256

GENESIS = "0" * 64


def sha256_of(r: dict) -> str:
    """A record's sha256, as the export says it is computed: the netstrings of its fields, in order."""
    uid = "" if r["peer_uid"] is None else str(r["peer_uid"])
    fields = ["agentos-audit/1", str(r["seq"]), r["at"], r["workflow_id"], r["type"], r["actor"], uid, r["payload"],
              r["prev_sha256"]]
    return hashlib.sha256(b"".join(f"{len(f.encode())}:".encode() + f.encode() + b"," for f in fields)).hexdigest()


def export(stack: Stack, *args: str) -> dict:
    out = stack.tmp / "export.json"
    r = subprocess.run([BIN / "agentctl", "--socket", stack.control_socket, "audit", "export", "-o", out, *args],
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr
    assert out.stat().st_mode & 0o077 == 0, oct(out.stat().st_mode)  # it holds the ledger's records
    return json.loads(out.read_text())


def check_anchors(doc: dict) -> int:
    """Every completed gateway call in each exported history names a record, the export lists it as
    an anchor, and that record is the call's: its attempt, its status, its result's sha256."""
    records = {r["seq"]: r for r in doc["records"]}
    checked = 0
    for run in doc["runs"]:
        assert not run.get("unavailable"), run
        events = {int(e["eventId"]): e for e in run["history"]["events"]}
        completed = {i for i, e in events.items() if e["eventType"] == "EVENT_TYPE_ACTIVITY_TASK_COMPLETED"}
        assert {a["event_id"] for a in run["anchors"]} == completed, (run["run_id"], run["anchors"])
        for a in run["anchors"]:
            attrs = events[a["event_id"]]["activityTaskCompletedEventAttributes"]
            result = json.loads(base64.b64decode(attrs["result"]["payloads"][0]["data"]))
            value = result.get("valueJson") or "{}"
            assert json.loads(value)["_audit"] == {"seq": a["seq"], "sha256": a["sha256"]}, (a, value)
            scheduled = events[int(attrs["scheduledEventId"])]["activityTaskScheduledEventAttributes"]
            started = events[int(attrs["startedEventId"])]["activityTaskStartedEventAttributes"]
            assert (scheduled["activityId"], started["attempt"]) == (a["activity_id"], a["attempt"]), a
            rec = records[a["seq"]]
            payload = json.loads(rec["payload"])
            assert (rec["sha256"], rec["type"], rec["workflow_id"]) == (a["sha256"], "call", run["run_id"]), (a, rec)
            assert (payload["activity_id"], payload["attempt"], payload["status"]) == (
                a["activity_id"], a["attempt"], result["status"]), (a, payload)
            assert payload["result_sha256"] == result_sha256(value), (a, payload)
            # The bytes the sha256 is of come with the anchor: they are the history's value, without its anchor.
            hashed = base64.b64decode(a["result_json"])
            assert hashlib.sha256(hashed).hexdigest() == payload["result_sha256"], (a, payload)
            assert json.loads(hashed) == {k: v for k, v in json.loads(value).items() if k != "_audit"}, (a, value)
            checked += 1
    return checked


def test_an_export_checks_without_agentd(stack: Stack) -> None:
    stack.start_agentd(policy=approval_policy())
    stack.start_worker()
    a = stack.start_run()
    approval = stack.wait_approval(a)
    stack.decide(stack.approval(approval["id"]), "approve")
    assert stack.wait_state(a, TERMINAL)["state"] == "SUCCEEDED"
    b = stack.start_run()
    rejected = stack.wait_approval(b)
    stack.decide(stack.approval(rejected["id"]), "reject", "--reason", "not now")
    assert stack.wait_state(b, TERMINAL)["state"] == "SUCCEEDED"

    doc = export(stack)
    assert doc["format"] == "agentos-audit-export/1" and doc["run_id"] == "", doc.keys()
    # The whole chain, from seq 1 to the head, each record hashed and linked as itself.
    head, records = doc["head"], doc["records"]
    assert head["records"] == head["seq"] == len(records) > 0, head
    prev = GENESIS
    for i, r in enumerate(records, start=1):
        assert r["seq"] == i and r["prev_sha256"] == prev, r
        assert r["sha256"] == sha256_of(r), r
        prev = r["sha256"]
    assert head["sha256"] == prev
    assert [r for r in records if r["type"] == "approval.decided"][0]["peer_uid"] is not None
    # The records are the ledger's, field for field.
    ledger = [dict(r) for r in stack.ledger("SELECT * FROM audit_log WHERE seq <= ? ORDER BY seq", head["seq"])]
    assert [{k: r[k] for k in ledger[0]} for r in records] == ledger
    # Each run's history as Temporal returns it, and the anchors in it.
    assert sorted(r["run_id"] for r in doc["runs"]) == sorted([a["run_id"], b["run_id"]]), doc["runs"]
    for run in doc["runs"]:
        mine = a if run["run_id"] == a["run_id"] else b
        assert run["temporal_run_id"] == mine["temporal_run_id"]
        parsed = WorkflowHistory.from_json(run["run_id"], json.dumps(run["history"]))
        assert [e.event_id for e in parsed.events] == [e.event_id for e in stack.history(mine)]
    assert check_anchors(doc) >= 12

    # The heads agentd pushed to Temporal (at the latest when it stops), each the record's at its seq.
    stack.restart_agentd()
    doc = wait_for(lambda: (d := export(stack))["pushed_heads"] and d, "a head pushed to Temporal", timeout=60, interval=2)
    by_seq = {r["seq"]: r for r in doc["records"]}
    assert all(by_seq[h["seq"]]["sha256"] == h["sha256"] and h["anchor_workflow_id"] for h in doc["pushed_heads"]), doc["pushed_heads"]

    # One run's records and histories only: each record still hashes as itself.
    one = export(stack, "--run", b["run_id"])
    assert [r["run_id"] for r in one["runs"]] == [b["run_id"]] and one["run_id"] == b["run_id"] and one["pushed_heads"] == []
    assert one["head"]["records"] == len(one["records"]) > 0 and one["head"]["seq"] >= head["seq"]
    assert {r["workflow_id"] for r in one["records"]} == {b["run_id"]}
    for r in one["records"]:
        assert r["sha256"] == sha256_of(r), r
    assert check_anchors({"runs": one["runs"], "records": one["records"]}) > 0


def test_export_says_what_it_cannot_do(stack: Stack) -> None:
    stack.start_agentd()

    def agentctl(*args: str) -> subprocess.CompletedProcess:
        return subprocess.run([BIN / "agentctl", "--socket", stack.control_socket, *args], capture_output=True, text=True,
                              timeout=60)
    # -o names the file: an export is always JSON, and agentctl -o json, as for other commands, is refused.
    for args in (["audit", "export", "-o", "json"], ["-o", "json", "audit", "export"]):
        r = agentctl(*args)
        assert r.returncode == 1 and "./json" in r.stderr, (args, r.stderr)
        assert not (stack.tmp / "json").exists()
    r = agentctl("audit", "export", "--run", "0f8b1c2e-1111-4222-8333-444455556666")
    assert r.returncode == 1 and "has a run" in r.stderr, r.stderr
    # To standard output, and with no runs: agentd's own records.
    r = agentctl("audit", "export")
    assert r.returncode == 0, r.stderr
    doc = json.loads(r.stdout)
    assert doc["runs"] == [] and doc["records"][0]["type"] == "agentd.started", doc



def test_an_exports_snapshot_is_in_agentds_data_directory(stack: Stack, monkeypatch) -> None:
    """The snapshot of the chain an export sends from is a file in agentd's data directory, not in the
    system's temporary one (here one that does not exist), and it goes with the export; one an agentd
    that stopped mid-export left there is removed when agentd starts."""
    stack.start_agentd()
    stack.start_worker()
    assert stack.wait_state(stack.start_run(), TERMINAL)["state"] == "SUCCEEDED"
    stale = stack.data_dir / "export-snapshot-0123"
    stale.write_bytes(b"what an export cut short left")
    monkeypatch.setenv("TMPDIR", str(stack.tmp / "no-such-directory"))
    stack.restart_agentd()
    assert not stale.exists()
    doc = export(stack)
    assert doc["head"]["records"] == doc["head"]["seq"] == len(doc["records"]) > 0 and "cut" not in doc["head"], doc["head"]
    assert list(stack.data_dir.glob("export-snapshot-*")) == []
