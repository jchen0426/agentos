"""agentd's hash-chained audit log (design doc, section 8). Every policy decision, side effect and
approval is a record in agentd's ledger, with who made it; each record holds the sha256 of the one
before it and its own; every result of a gateway call names the record of that call in Temporal's
history (_audit), and that record holds the sha256 of the result; and agentd pushes the chain's
head to a workflow of its own in Temporal, out of reach of whoever can write the ledger. The tests
read the ledger file read-only and check the chain and the results' hashes with code of their own,
read the anchors from Temporal's API, and change the ledger with agentd stopped, as someone with
the file could."""

import hashlib
import json
import os
import pwd
import shutil
import sqlite3
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import grpc
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice import v1 as ws
from temporalio.api.workflowservice.v1.service_pb2_grpc import WorkflowServiceStub

from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

from tests.harness import BIN, SANDBOX, TERMINAL, TOOL_LAUNCHER, Stack, approval_policy, gateway_calls, wait_for

GENESIS = "0" * 64
PR = "github.open_draft_pr"
ANCHOR_TYPE = "agentos-audit-anchor"


def netstring(s: str) -> bytes:
    b = s.encode()
    return str(len(b)).encode() + b":" + b + b","


def record_hash(r: dict) -> str:
    """The design's hash, written apart from agentd's: the netstrings of the record's fields, in order."""
    uid = "" if r["peer_uid"] is None else str(r["peer_uid"])
    fields = ["agentos-audit/1", str(r["seq"]), r["at"], r["workflow_id"], r["type"], r["actor"], uid, r["payload"],
              r["prev_sha256"]]
    return hashlib.sha256(b"".join(netstring(f) for f in fields)).hexdigest()


class Number(str):
    """A JSON number as it was written, as Go's json.Number keeps it."""


def go_string(s: str) -> str:
    """s as Go's encoding/json writes a string: HTML characters, U+2028 and U+2029 escaped."""
    named = {'"': '\\"', "\\": "\\\\", "\n": "\\n", "\r": "\\r", "\t": "\\t", "\b": "\\b", "\f": "\\f"}
    return '"' + "".join(named.get(c) or (f"\\u{ord(c):04x}" if ord(c) < 0x20 or c in "<>&\u2028\u2029" else c)
                         for c in s) + '"'


def go_json(v) -> str:
    """v as Go's json.Marshal writes it: object keys sorted, nothing between the tokens."""
    if isinstance(v, dict):
        return "{" + ",".join(f"{go_string(k)}:{go_json(v[k])}" for k in sorted(v)) + "}"
    if isinstance(v, list):
        return "[" + ",".join(go_json(x) for x in v) + "]"
    if isinstance(v, Number):
        return str(v)
    if isinstance(v, str):
        return go_string(v)
    return {True: "true", False: "false", None: "null"}[v]


def result_sha256(value_json: str) -> str:
    """The sha256 a call's record holds of its result: of the value without its _audit, as Go
    encodes it, every number as it was written."""
    value = json.loads(value_json, parse_int=Number, parse_float=Number)
    value.pop("_audit", None)
    return hashlib.sha256(go_json(value).encode()).hexdigest()


def records(stack: Stack) -> list[dict]:
    return [{**dict(r), "data": json.loads(r["payload"])} for r in stack.ledger("SELECT * FROM audit_log ORDER BY seq")]


def check_chain(rows: list[dict]) -> None:
    """Seq 1, 2, 3 and on, each record linked to the one before it and hashed as itself."""
    prev = GENESIS
    for i, r in enumerate(rows, start=1):
        assert r["seq"] == i, (i, r)
        assert r["prev_sha256"] == prev, r
        assert r["sha256"] == record_hash(r), r
        prev = r["sha256"]


def results(stack: Stack, run: dict) -> list[dict]:
    """Each gateway call's result in the run's history: its activity ID, the attempt that returned
    it, its status, its value as JSON and the _audit in it."""
    out = []
    for c in gateway_calls(stack.history(run)):
        if c["completed"] is not None:
            result = json.loads(c["completed"].activity_task_completed_event_attributes.result.payloads[0].data)
            value = result.get("valueJson") or "{}"
            out.append({"activity_id": c["activity_id"], "attempt": c["started"].activity_task_started_event_attributes.attempt,
                        "status": result["status"], "value_json": value, "audit": json.loads(value).get("_audit")})
    return out


def anchors(stack: Stack, run: dict) -> list[dict]:
    """The _audit of each gateway call's result in the run's history."""
    return [r["audit"] for r in results(stack, run)]


def check_anchors(stack: Stack, run: dict, rows: list[dict]) -> int:
    """Each result names the record of its own call, the run's, by seq and sha256; that record is of
    the attempt that returned it, with its status, and holds the result's sha256. Returns how many."""
    by_seq = {r["seq"]: r for r in rows}
    found = results(stack, run)
    assert found, "no results in the history"
    for res in found:
        a = res["audit"]
        assert a is not None, res
        r = by_seq[a["seq"]]
        assert (r["sha256"], r["type"], r["workflow_id"], r["actor"]) == (a["sha256"], "call", run["run_id"], "run"), (a, r)
        assert (r["data"]["activity_id"], r["data"]["attempt"], r["data"]["status"]) == (
            res["activity_id"], res["attempt"], res["status"]), (res, r)
        assert r["data"]["result_sha256"] == result_sha256(res["value_json"]), (res, r)
    return len(found)


def pushed(stack: Stack) -> list[dict]:
    """The chain's heads agentd pushed to Temporal, in the order of its anchor workflows' signals."""
    async def fetch(c: Client) -> list[dict]:
        heads = []
        async for w in c.list_workflows(f"WorkflowType = '{ANCHOR_TYPE}'"):
            try:
                history = await c.get_workflow_handle(w.id, run_id=w.run_id).fetch_history()
            except RPCError as e:
                if e.status == RPCStatusCode.NOT_FOUND:
                    continue  # deleted since the listing
                raise
            for e in history.events:
                if e.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
                    a = e.workflow_execution_signaled_event_attributes
                    heads.append({"workflow_id": w.id, "signal": a.signal_name, **json.loads(a.input.payloads[0].data)})
        return heads
    return stack._temporal(fetch)


def outside(rep: dict) -> list[dict]:
    """The report's mismatches of the heads agentd pushed to Temporal, apart from the runs'."""
    return [m for m in rep["anchors_mismatched"] if m["run_id"].startswith(ANCHOR_TYPE + "-")]


def of_runs(rep: dict) -> list[dict]:
    return [m for m in rep["anchors_mismatched"] if not m["run_id"].startswith(ANCHOR_TYPE + "-")]


def verify(stack: Stack, *args: str) -> tuple[int, dict]:
    """agentctl audit verify: its exit status and its report, which agree: 1 when anything does not
    match, 2 when only some runs could not be checked."""
    r = stack.agentctl_run("audit", "verify", *args)
    assert r.stdout, r.stderr
    rep = json.loads(r.stdout)
    for k in ("records", "head_seq", "broken_at_seq", "anchors_checked", "runs_checked", "external_anchors_checked",
              "last_external_anchor_seq", "records_after_external_anchor", "retention_seconds",
              "other_anchor_workflows"):  # int64s are strings in JSON
        rep[k] = int(rep[k])
    failed = rep["broken_at_seq"] != 0 or any(rep[k] != [] for k in ("anchors_mismatched", "runs_mismatched", "state_mismatched"))
    unchecked = (rep["runs_unavailable"] != [] and "--allow-unavailable" not in args) or (
        rep["runs_before_log"] != [] and "--allow-before-log" not in args)
    assert r.returncode == (1 if failed else 2 if unchecked else 0), (r.returncode, rep, r.stderr)
    return r.returncode, rep


def verify_text(stack: Stack) -> subprocess.CompletedProcess:
    """agentctl audit verify, as a person reads it."""
    return subprocess.run([BIN / "agentctl", "--socket", stack.control_socket, "audit", "verify"], capture_output=True,
                          text=True, timeout=60)


def test_the_log_records_a_run_and_its_approval(stack: Stack) -> None:
    stack.start_agentd(policy=approval_policy())
    stack.start_worker()
    run = stack.start_run()
    item = stack.wait_approval(run)
    stack.decide(item, "approve")  # as this user, agentctl's default
    final = stack.wait_state(run, TERMINAL, timeout=90)
    assert final["state"] == "SUCCEEDED", final

    rows = records(stack)
    check_chain(rows)
    me, uid = pwd.getpwuid(os.getuid()).pw_name, os.getuid()

    # agentd's start comes first: agentd's own, with what it was started with.
    first = rows[0]
    assert (first["type"], first["actor"], first["workflow_id"], first["peer_uid"]) == ("agentd.started", "agentd", "", None)
    started = first["data"]
    assert started["pid"] == stack.agentd.popen.pid and started["test_tools"] is False and started["fault_script"] is False
    assert started["insecure_dev"] == (SANDBOX == "none"), started
    assert started["retention_seconds"] > 0, started  # the namespace's, as agentd found it
    # A retention shorter than a week loses runs' anchors sooner: agentd says so as it starts.
    warned = any(e.get("msg", "").startswith("the namespace keeps closed runs' histories for a short time")
                 for e in stack.agentd_log())
    assert warned == (started["retention_seconds"] < 7 * 86400), started
    assert started["git_tools_sandbox"] == (str(TOOL_LAUNCHER[SANDBOX]) if SANDBOX in TOOL_LAUNCHER else ""), started
    # Who it is in the heads it pushes to Temporal: from its configuration, not from the ledger.
    assert len(started["agentd"]) == 16 and "anchor_findings" not in started, started
    config = stack.tmp / "agentd"
    for name, file in (("agentd", "agentd.yaml"), ("policy", "policy.yaml"), ("models", "models.yaml"), ("prices", "prices.yaml")):
        assert started["config"][name] == {"path": str((config / file).resolve()),
                                           "sha256": hashlib.sha256((config / file).read_bytes()).hexdigest()}, started

    mine = [r for r in rows if r["workflow_id"] == run["run_id"]]
    assert {r["workflow_id"] for r in rows} == {"", run["run_id"]}

    def one(typ: str, **fields) -> dict:
        found = [r for r in mine if r["type"] == typ and all(r["data"].get(k) == v for k, v in fields.items())]
        assert len(found) == 1, (typ, fields, found)
        return found[0]

    # What a person did through agentctl is the operator's, with the uid the kernel gave agentd.
    created = one("run.created")
    assert (created["actor"], created["peer_uid"]) == (f"operator:{me}", uid), created
    assert created["data"] == {"task": "security-audit", "params": run["params"], "budget_limit_micros": 5_000_000,
                               "dry_run": False}, created
    assert one("run.started")["data"] == {"temporal_run_id": run["temporal_run_id"]}
    decided = one("approval.decided")
    assert (decided["actor"], decided["peer_uid"]) == (f"operator:{me}", uid), decided
    assert decided["data"] == {"approval_id": item["id"], "decision": "approve", "payload_sha256": item["payload_sha256"],
                               "decided_by": me}

    # The run's calls are the run's, and what they ask for: the write, and the approval it needs.
    calls = [r for r in mine if r["type"] == "call"]
    assert calls and all((r["actor"], r["peer_uid"]) == ("run", None) for r in calls), calls
    pr = one("effect.recorded", tool=PR)
    assert pr["actor"] == "run" and pr["data"]["status"] == "AWAITING_APPROVAL", pr
    effect = stack.effect(run, PR)
    assert (pr["data"]["effect_id"], pr["data"]["payload_sha256"]) == (effect["effect_id"], effect["payload_sha256"])
    asked = one("approval.created", approval_id=item["id"])
    assert asked["actor"] == "run" and asked["data"]["payload_sha256"] == item["payload_sha256"], asked
    # Each call says what the policy decided of it; the first call of the write waited for the approval.
    # (An attempt that failed and was retried has a record of its own, with status "error".)
    by_name = {}
    for r in calls:
        if r["data"]["status"] != "error":
            by_name.setdefault(r["data"]["name"], []).append(r["data"])
    assert [c["policy"] for c in by_name["git.show_diff"]] == [{"decision": "allow"}]
    assert [c["policy"] for c in by_name["audit_v1"]] == [{"decision": "allow"}]
    assert [(c["status"], c["policy"]["decision"]) for c in by_name[PR]] == [("needs_approval", "require_approval"),
                                                                              ("ok", "require_approval")], by_name[PR]

    # What the reviewer was shown, agentd read from GitHub of itself: each request has its record
    # first, which names why, the effect, and the request's method and path.
    steps = [r for r in mine if r["data"].get("effect_id") == effect["effect_id"]]
    reads = [r for r in steps if r["type"] == "agentd.read"]
    assert reads and all(r["actor"] == "agentd" and r["seq"] < pr["seq"] and r["data"]["purpose"] == "approval_card" and
                         r["data"]["tool"] == PR and r["data"]["method"] == "GET" and
                         r["data"]["path"].startswith("/repos/acme/payments/") for r in reads), reads
    # The steps of the effect protocol are agentd's, in their order, after the person's approval.
    steps = [r for r in steps if r["type"] != "agentd.read"]
    sends = sum(r["type"] == "effect.sending" for r in steps)  # one before each visible request
    assert sends >= 2 and [(r["type"], r["actor"]) for r in steps] == [
        ("effect.recorded", "run"), ("approval.created", "run"), ("approval.used", "agentd"),
        ("effect.claimed", "agentd"), *[("effect.sending", "agentd")] * sends, ("effect.settled", "agentd")], steps
    assert steps[-1]["data"]["status"] == "SUCCEEDED" and steps[-1]["data"]["result_sha256"], steps[-1]
    assert steps[1]["seq"] < decided["seq"] < steps[2]["seq"]
    assert created["seq"] < one("run.started")["seq"] < calls[0]["seq"]

    # Every result in Temporal's history names its call's record; agentctl agrees.
    n = check_anchors(stack, run, rows)
    code, rep = verify(stack)
    assert code == 0 and rep["broken_at_seq"] == 0 and rep["anchors_mismatched"] == [] and rep["state_mismatched"] == [], rep
    assert (rep["anchors_checked"], rep["runs_checked"], rep["records"]) == (n, 1, rep["head_seq"]), rep
    assert rep["retention_seconds"] == started["retention_seconds"], rep
    assert rep["head_seq"] >= rows[-1]["seq"]
    code, rep = verify(stack, "--run", run["run_id"])
    assert code == 0 and (rep["anchors_checked"], rep["runs_checked"]) == (n, 1), rep


def ledger_files(stack: Stack) -> list[Path]:
    return [stack.data_dir / f"ledger.db{suffix}" for suffix in ("", "-wal", "-shm")]


def last_start(stack: Stack) -> dict:
    """The record of agentd's last start."""
    return [r for r in records(stack) if r["type"] == "agentd.started"][-1]


def signal_head(stack: Stack, head: dict) -> None:
    """A head signalled to the chain's anchor workflow of the day by someone with Temporal's API,
    as agentd signals one: Temporal does not say who sent it."""
    chain = records(stack)[0]["sha256"][:16]
    workflow_id = f"{ANCHOR_TYPE}-{chain}-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
    stack._temporal(lambda c: c.get_workflow_handle(workflow_id).signal("anchor", head))


def test_a_ledger_cut_while_agentd_was_down(stack: Stack) -> None:
    """Someone with the ledger cuts the audit log's last records while agentd is down, and someone
    signals the anchor workflow a head of what is left, after the one agentd pushed as it stopped. A
    head of Temporal's names no sender, so agentd starts all the same; what it found, it logs and
    puts in its start's record: the highest head it pushed is not in the ledger, and a lower one
    came after it. verify finds the same."""
    stack.start_agentd()
    stack.start_worker()
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
    stack.agentd.stop()  # pushes its head as it stops
    head = records(stack)[-1]
    wait_for(lambda: any(h["seq"] == head["seq"] for h in pushed(stack)), "the last head in Temporal", timeout=60)
    db = sqlite3.connect(stack.data_dir / "ledger.db")
    db.execute("DELETE FROM audit_log WHERE seq > ?", (head["seq"] - 3,))
    db.commit()
    db.close()
    left = records(stack)[-1]
    signal_head(stack, {"seq": left["seq"], "sha256": left["sha256"]})  # what the cut left, newer than any
    stack.restart_agentd()
    found = {(f["seq"], f["sha256"]): f["why"] for f in last_start(stack)["data"]["anchor_findings"]}
    assert set(found) == {(head["seq"], head["sha256"]), (left["seq"], left["sha256"])}, found
    assert "does not have" in found[(head["seq"], head["sha256"])] and "went backwards" in found[(left["seq"], left["sha256"])]
    assert any(e.get("msg", "").startswith("THE LEDGER DOES NOT BEAR OUT A HEAD") for e in stack.agentd_log())
    code, rep = verify(stack)
    whys = {int(m["seq"]): m["why"] for m in outside(rep)}
    assert code == 1 and "does not have" in whys[head["seq"]] and "went backwards" in whys[left["seq"]], rep


def test_a_flood_of_forged_heads(stack: Stack) -> None:
    """Someone with Temporal's API floods the anchor workflow with signals that are no head agentd
    pushes: agentd starts, and its start's record names at most 20 of them, copies nothing of them,
    and counts the rest."""
    stack.start_agentd()
    stack.agentd.stop()
    wait_for(lambda: pushed(stack), "a head pushed to Temporal", timeout=60)
    junk = "\u202e<script>" * 50
    for i in range(30):
        signal_head(stack, [{"seq": -1, "sha256": "0" * 64}, {"seq": 1, "sha256": junk}, {"seq": 1, "sha256": "0" * 64, "agentd": junk}][i % 3])
    stack.restart_agentd()
    started = last_start(stack)["data"]
    assert len(started["anchor_findings"]) == 20 and started["anchor_findings_more"] == 10, started
    assert all(f["why"] == "an unreadable head: not a head agentd pushes" and "seq" not in f for f in started["anchor_findings"]), started
    found = json.dumps(started["anchor_findings"], ensure_ascii=False)
    assert "script" not in found and "\u202e" not in found, found
    code, rep = verify(stack)
    assert code == 1 and len([m for m in outside(rep) if "cannot be read" in m["why"]]) == 30, rep
    assert "script" not in json.dumps(outside(rep), ensure_ascii=False), rep


def test_many_anchor_workflows_made_by_someone_else(stack: Stack) -> None:
    """Someone with Temporal's API makes many workflows with anchor workflows' IDs of the day: agentd's
    start check reads at most 50 of them, its own chain's first, says how many it did not read, and
    agentd starts as it would."""
    stack.start_agentd()
    stack.agentd.stop()
    wait_for(lambda: pushed(stack), "a head pushed to Temporal", timeout=60)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    async def make(c: Client) -> None:
        for i in range(60):
            await c.start_workflow("someone-elses", id=f"{ANCHOR_TYPE}-junk{i:04d}-{day}", task_queue="nowhere")
    stack._temporal(make)
    wait_for(lambda: len(stack._temporal(lambda c: _count(c, f"WorkflowId STARTS_WITH '{ANCHOR_TYPE}-junk'"))) == 60,
             "the workflows listed", timeout=60)
    started = time.monotonic()
    stack.restart_agentd()
    took = time.monotonic() - started
    [found] = [f for f in last_start(stack)["data"]["anchor_findings"] if "not_read" in f]
    assert found["not_read"] == 12 and "the start reads at most 50 anchor workflows" in found["why"], found
    assert took < 30, took


async def _count(c: Client, query: str) -> list:
    return [w async for w in c.list_workflows(query)]


def test_a_head_signalled_by_someone_else(stack: Stack) -> None:
    """Someone with Temporal's API signals the anchor workflow a head the ledger never reached: it
    does not keep agentd from starting, which records it, and verify fails on it."""
    stack.start_agentd()
    stack.agentd.stop()
    wait_for(lambda: pushed(stack), "a head pushed to Temporal", timeout=60)
    signal_head(stack, {"seq": 10**9, "sha256": "0" * 64})
    stack.restart_agentd()
    assert [(f["seq"], f["sha256"]) for f in last_start(stack)["data"]["anchor_findings"]] == [(10**9, "0" * 64)]
    code, rep = verify(stack)
    assert code == 1 and any(int(m["seq"]) == 10**9 for m in outside(rep)), rep


def test_a_ledger_cut_while_agentd_runs(stack: Stack) -> None:
    """Someone with the ledger cuts the audit log's last record while agentd runs: agentd appends
    nothing more to it, and pushes no head of it, and says so; verify says the log no longer holds
    the record agentd appended last."""
    stack.start_agentd()
    stack.start_worker()
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
    db = sqlite3.connect(stack.data_dir / "ledger.db", timeout=30)
    db.execute("DELETE FROM audit_log WHERE seq = (SELECT MAX(seq) FROM audit_log)")
    db.commit()
    db.close()
    left = records(stack)[-1]["seq"]
    r = stack.agentctl_run("runs", "start", "security-audit", "--repo", "acme/payments", "--commit", stack.commit[:7])
    assert r.returncode != 0 and records(stack)[-1]["seq"] == left, (r.stdout, r.stderr)
    wait_for(lambda: any(e.get("msg", "").startswith("THE AUDIT LOG CHANGED UNDER AGENTD") for e in stack.agentd_log()),
             "agentd to say the audit log changed under it")
    code, rep = verify(stack)
    assert code == 1 and any(m["why"].startswith("the last record this agentd appended") for m in rep["anchors_mismatched"]), rep


def test_tables_changed_without_their_records(stack: Stack) -> None:
    """What agentd's limits count is in the records too: the calls the loop guard counts and the
    budget's entries, emptied by someone with the ledger, are differences. The report names the
    tables it does not check, and why."""
    stack.start_agentd()
    stack.start_worker()
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
    code, rep = verify(stack)
    assert code == 0 and {t["name"] for t in rep["unchecked_tables"]} == {"outbox", "goose_db_version", "migration_files"}, rep
    db = sqlite3.connect(stack.data_dir / "ledger.db", timeout=30)
    db.execute("DELETE FROM calls")
    db.execute("DELETE FROM budget_entries")
    db.commit()
    db.close()
    code, rep = verify(stack)
    assert code == 1 and {"call", "budget"} <= {m["kind"] for m in rep["state_mismatched"]}, rep
    assert any("the loop guard counts what calls holds" in m["why"] for m in rep["state_mismatched"]), rep
    r = verify_text(stack)
    assert r.returncode == 1 and "unchecked table" in r.stdout and "outbox" in r.stdout, r.stdout


NO_COUNT = ("INSERT INTO sqlite_master (type, name, tbl_name, rootpage, sql) VALUES ('trigger', 'sqlite_no_count', "
            "'calls', 0, 'CREATE TRIGGER sqlite_no_count BEFORE INSERT ON calls BEGIN SELECT RAISE(IGNORE); END')")


def write_schema(stack: Stack, statement: str) -> None:
    """statement writes sqlite_master directly, as PRAGMA writable_schema lets anyone with the ledger:
    an object under a name CREATE refuses, and schema_version as it was."""
    db = sqlite3.connect(stack.data_dir / "ledger.db", timeout=30, isolation_level=None)
    try:
        if hasattr(db, "setconfig"):
            db.setconfig(sqlite3.SQLITE_DBCONFIG_DEFENSIVE, False)
        db.execute("PRAGMA writable_schema=ON")
        db.execute(statement)
        db.execute("PRAGMA writable_schema=OFF")
    finally:
        db.close()


def test_a_trigger_written_into_the_ledger(stack: Stack) -> None:
    """Someone with the ledger writes into sqlite_master a trigger that drops every count of the loop
    guard, under a name of SQLite's that CREATE refuses, and schema_version stays as it was. agentd,
    running, writes nothing while it is there: a run's start fails, and verify does not pass. Taken
    out, runs go on. Written in while agentd is down, agentd does not start, and says why."""
    stack.start_agentd()
    stack.start_worker()
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
    version = stack.ledger("PRAGMA schema_version")[0][0]
    write_schema(stack, NO_COUNT)
    assert stack.ledger("PRAGMA schema_version")[0][0] == version
    named = 'trigger "sqlite_no_count" on "calls", which no migration makes'
    left = records(stack)[-1]["seq"]
    r = stack.agentctl_run("runs", "start", "security-audit", "--repo", "acme/payments", "--commit", stack.commit[:7],
                           "--base", "main")
    assert r.returncode != 0 and named in r.stderr and records(stack)[-1]["seq"] == left, (r.stdout, r.stderr)
    r = stack.agentctl_run("audit", "verify")
    if r.stdout:  # a report, the schema a mismatch in it
        rep = json.loads(r.stdout)
        assert r.returncode == 1 and any(m["kind"] == "schema" and named in m["why"] for m in rep["state_mismatched"]), rep
    else:  # or no report: the ledger hands out no new connection while the schema is not its migrations'
        assert r.returncode != 0 and named in r.stderr, r.stderr
    write_schema(stack, "DELETE FROM sqlite_master WHERE name = 'sqlite_no_count'")
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
    stack.agentd.stop()
    write_schema(stack, NO_COUNT)
    refused = subprocess.run([str(a) for a in stack._agentd_args], capture_output=True, text=True, timeout=60)
    out = refused.stdout + refused.stderr
    [stopped] = [e for e in map(json.loads, out.splitlines()) if e.get("msg") == "agentd stopped"]
    assert refused.returncode != 0 and named in stopped["error"] and "agentd does not open it" in stopped["error"], out
    write_schema(stack, "DELETE FROM sqlite_master WHERE name = 'sqlite_no_count'")
    stack._launch_agentd()
    code, rep = verify(stack)
    assert code == 0, rep


def test_a_run_cancelled_while_its_model_call_runs(stack: Stack) -> None:
    """A run cancelled while its model call is still in agentd ends at once, the call still going:
    its call.started is open, after the run ended. verify takes it as in flight, since agentd says
    the attempt is executing; once the call ends, its record lands, and verify passes again."""
    hold = stack.tmp / "hold-before-record"
    hold.touch()
    stack.start_agentd(faults=[{"name": "audit_v1", "attempt": 1, "hold_before_record_while_exists": str(hold)}])
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: stack.model_answered(run), "the provider to answer")
    stack.agentctl("runs", "cancel", run["run_id"])
    assert stack.wait_state(run, TERMINAL)["state"] == "CANCELLED"
    [started] = [r for r in records(stack) if r["type"] == "call.started" and r["data"]["name"] == "audit_v1"]

    def recorded() -> list[dict]:
        return [r for r in records(stack) if r["type"] == "call" and r["data"].get("started_seq") == started["seq"]]
    assert recorded() == []
    code, rep = verify(stack)
    assert code == 0, rep
    assert recorded() == []  # the call was in agentd all along
    hold.unlink()
    wait_for(recorded, "the call's record", timeout=60)
    code, rep = verify(stack)
    assert code == 0, rep


def test_a_decision_appended_by_someone_else(stack: Stack) -> None:
    """While a run waits for approval, someone with the ledger appends a decision nobody gave, chained
    as agentd chains its records, and changes the approval to match, without touching Temporal.
    agentd's next append lands on a record it did not append: it appends nothing more, says so, and
    the run fails with nothing sent. verify reports the record as one this agentd did not append,
    and that agentd found the log changed under it."""
    stack.start_agentd(policy=approval_policy())
    stack.start_worker()
    run = stack.start_run()
    item = stack.wait_approval(run)
    now = datetime.now(timezone.utc)
    at = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond * 1000:09d}Z"
    db = sqlite3.connect(stack.data_dir / "ledger.db", timeout=30, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("BEGIN IMMEDIATE")
    last = dict(db.execute("SELECT * FROM audit_log ORDER BY seq DESC LIMIT 1").fetchone())
    payload = json.dumps({"approval_id": item["id"], "decision": "approve", "payload_sha256": item["payload_sha256"]},
                         separators=(",", ":"), sort_keys=True)
    r = {"seq": last["seq"] + 1, "at": at, "workflow_id": run["run_id"], "type": "approval.decided",
         "actor": "operator:alice", "peer_uid": os.getuid(), "payload": payload, "prev_sha256": last["sha256"]}
    r["sha256"] = record_hash(r)
    db.execute("INSERT INTO audit_log VALUES (:seq, :at, :workflow_id, :type, :actor, :peer_uid, :payload, :prev_sha256, :sha256)", r)
    db.execute("UPDATE approvals SET status = 'APPROVED', decided_by = 'alice', decided_at = ?, decision_key = 'forged' "
               "WHERE approval_id = ? AND status = 'PENDING'", (at, item["id"]))
    db.execute("COMMIT")
    db.close()
    stack.signal(run, "doorbell")
    assert stack.wait_state(run, TERMINAL, timeout=90)["state"] == "FAILED"
    assert stack.external(run, PR) == []
    assert any(e.get("msg", "").startswith("THE AUDIT LOG CHANGED UNDER AGENTD") for e in stack.agentd_log())
    code, rep = verify(stack)
    whys = [m["why"] for m in rep["anchors_mismatched"]]
    assert code == 1 and any(f'records it did not append: {r["seq"]} ("approval.decided" by "operator:alice")' in w
                             for w in whys), rep
    assert any(w.startswith("agentd found the audit log changed under it") for w in whys), rep


def test_a_changed_ledger_does_not_verify(stack: Stack) -> None:
    stack.start_agentd()
    stack.start_worker()
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
    code, rep = verify(stack)
    assert code == 0, rep
    rows = records(stack)
    anchored = sorted(a["seq"] for a in anchors(stack, run))
    target = anchored[1]  # the record of the run's second call: records follow it, and anchors
    stack.agentd.stop()
    head = records(stack)[-1]["seq"]  # pushed to Temporal as agentd stopped
    saved = stack.tmp / "ledger-saved"
    saved.mkdir()
    for f in ledger_files(stack):
        if f.exists():
            shutil.copy2(f, saved / f.name)

    def tampered(change) -> tuple[int, dict]:
        """The ledger as it was, changed by `change` with agentd stopped; agentd's report of it."""
        for f in ledger_files(stack):
            f.unlink(missing_ok=True)
            if (saved / f.name).exists():
                shutil.copy2(saved / f.name, f)
        db = sqlite3.connect(stack.data_dir / "ledger.db")
        change(db)
        db.commit()
        db.close()
        stack.restart_agentd()
        try:
            return verify(stack)
        finally:
            stack.agentd.stop()

    # One record's payload changed: the chain breaks there.
    changed = json.dumps({**json.loads(rows[target - 1]["payload"]), "status": "denied"}, separators=(",", ":"))
    code, rep = tampered(lambda db: db.execute("UPDATE audit_log SET payload = ? WHERE seq = ?", (changed, target)))
    assert code == 1 and rep["broken_at_seq"] == target and "not the hash" in rep["broken_reason"], rep
    assert rep["records"] == target - 1 and outside(rep) == [], rep

    # One record deleted: the chain breaks there, and the result that named it finds nothing. The
    # ledger put back as it was is a change too: the head the last agentd pushed as it stopped is
    # not the record after the head of the copy.
    code, rep = tampered(lambda db: db.execute("DELETE FROM audit_log WHERE seq = ?", (target,)))
    assert code == 1 and rep["broken_at_seq"] == target and "missing" in rep["broken_reason"], rep
    assert [int(m["seq"]) for m in of_runs(rep)] == [target], rep
    seqs = {int(m["seq"]) for m in outside(rep)}
    assert head + 1 in seqs and seqs <= {target, head + 1}, rep  # target too, if a head pushed on the way was it

    # The chain rewritten from that record on, every hash made again: the chain is whole, but the
    # results in Temporal's history name the records as they were.
    def rewrite(db: sqlite3.Connection) -> None:
        db.row_factory = sqlite3.Row
        [(prev,)] = db.execute("SELECT sha256 FROM audit_log WHERE seq = ?", (target - 1,)).fetchall()
        for row in db.execute("SELECT * FROM audit_log WHERE seq >= ? ORDER BY seq", (target,)).fetchall():
            r = dict(row)
            if r["seq"] == target:
                r["payload"] = changed
            r["prev_sha256"] = prev
            r["sha256"] = record_hash(r)
            db.execute("UPDATE audit_log SET payload = ?, prev_sha256 = ?, sha256 = ? WHERE seq = ?",
                       (r["payload"], r["prev_sha256"], r["sha256"], r["seq"]))
            prev = r["sha256"]
    code, rep = tampered(rewrite)
    assert code == 1 and rep["broken_at_seq"] == 0 and rep["broken_reason"] == "", rep
    assert sorted(int(m["seq"]) for m in of_runs(rep)) == [s for s in anchored if s >= target], rep
    assert all("sha256" in m["why"] for m in rep["anchors_mismatched"]), rep
    # The heads pushed while the run went on, and as agentd stopped, name the records as they were.
    assert head in {int(m["seq"]) for m in outside(rep)}, rep
    assert all(int(m["seq"]) >= target for m in outside(rep)), rep

    # A table changed without its record: the state the records imply is not the ledger's.
    code, rep = tampered(lambda db: db.execute("UPDATE runs SET budget_limit_micros = budget_limit_micros * 10"))
    assert code == 1 and rep["broken_at_seq"] == 0, rep
    assert [(m["kind"], m["id"]) for m in rep["state_mismatched"]] == [("run", run["run_id"])], rep

    # The text report says the same, and agentctl exits 1.
    stack.restart_agentd()
    code, rep = verify(stack)
    r = verify_text(stack)
    assert r.returncode == 1 and "whole: " in r.stdout and f"{len(rep['anchors_mismatched'])} do not match" in r.stdout, r.stdout
    assert "1 differences from what the records say" in r.stdout, r.stdout
    assert "does not verify" in r.stderr, r.stderr


def test_a_run_dropped_from_the_ledger_does_not_verify(stack: Stack) -> None:
    """Whoever can rewrite the ledger can drop a run from it: its row, and its records, with the chain
    made whole again after them. The ledger then says nothing of the run; Temporal still has it, and
    the results in its history name records that are gone."""
    stack.start_agentd()
    stack.start_worker()
    runs = []
    for _ in range(3):  # one after another: the dropped run's records lie between the others'
        run = stack.start_run()
        assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
        runs.append(run)
    code, rep = verify(stack)
    assert code == 0 and rep["runs_checked"] == 3, rep
    dropped = runs[1]["run_id"]
    stack.agentd.stop()

    db = sqlite3.connect(stack.data_dir / "ledger.db")
    db.row_factory = sqlite3.Row
    db.execute("DELETE FROM runs WHERE workflow_id = ?", (dropped,))
    [(first,)] = db.execute("SELECT MIN(seq) FROM audit_log WHERE workflow_id = ?", (dropped,)).fetchall()
    db.execute("DELETE FROM audit_log WHERE workflow_id = ?", (dropped,))
    [(prev,)] = db.execute("SELECT sha256 FROM audit_log WHERE seq = ?", (first - 1,)).fetchall()
    seq = first
    for row in db.execute("SELECT * FROM audit_log WHERE seq > ? ORDER BY seq", (first,)).fetchall():
        r = {**dict(row), "seq": seq, "prev_sha256": prev}
        r["sha256"] = record_hash(r)
        db.execute("UPDATE audit_log SET seq = ?, prev_sha256 = ?, sha256 = ? WHERE seq = ?",
                   (seq, prev, r["sha256"], row["seq"]))
        prev, seq = r["sha256"], seq + 1
    db.commit()
    db.close()
    stack.restart_agentd()
    # The head it pushed as it stopped is gone too: its start says so.
    assert last_start(stack)["data"]["anchor_findings"], last_start(stack)

    rows = records(stack)
    check_chain(rows)  # whole on its own, with no trace of the run
    assert dropped not in {r["workflow_id"] for r in rows} and stack.ledger("SELECT * FROM runs WHERE workflow_id = ?", dropped) == []
    code, rep = verify(stack)
    assert code == 1 and rep["broken_at_seq"] == 0, rep
    [named] = rep["runs_mismatched"]
    assert named["run_id"] == dropped and "the ledger has no row of the run" in named["why"], rep
    # Its results name records the ledger no longer has; the records after them moved, and so did
    # the heads agentd pushed to Temporal after them.
    assert {m["run_id"] for m in of_runs(rep)} == {dropped, runs[2]["run_id"]}, rep
    assert outside(rep) and all(int(m["seq"]) >= first for m in outside(rep)), rep
    r = verify_text(stack)
    assert r.returncode == 1 and dropped in r.stdout and "do not match the ledger" in r.stdout, r.stdout
    # Asked about by its ID, it is found all the same: in Temporal.
    code, rep = verify(stack, "--run", dropped)
    assert code == 1 and [m["run_id"] for m in rep["runs_mismatched"]] == [dropped] and rep["runs_checked"] == 1, rep


def test_concurrent_runs_make_one_chain(stack: Stack) -> None:
    stack.start_agentd()
    stack.start_worker()
    runs = [stack.start_run() for _ in range(4)]
    for run in runs:
        assert stack.wait_state(run, TERMINAL, timeout=120)["state"] == "SUCCEEDED"
    rows = records(stack)
    check_chain(rows)
    # Their records are interleaved in one chain: some run's records have another's between them.
    # Each run's anchors are its own.
    order = [r["workflow_id"] for r in rows if r["workflow_id"]]
    assert {r["run_id"] for r in runs} == set(order)
    at: dict[str, list[int]] = {}
    for i, wf in enumerate(order):
        at.setdefault(wf, []).append(i)
    assert any(ix[-1] - ix[0] + 1 > len(ix) for ix in at.values()), ("the runs never interleaved", order)
    checked = sum(check_anchors(stack, run, rows) for run in runs)
    code, rep = verify(stack)
    assert code == 0 and (rep["anchors_checked"], rep["runs_checked"]) == (checked, 4), rep


def test_a_killed_agentd_leaves_the_chain_whole(stack: Stack) -> None:
    hold = stack.tmp / "hold-before-record"
    hold.touch()
    stack.start_agentd(faults=[{"name": "audit_v1", "attempt": 1, "hold_before_record_while_exists": str(hold)}])
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: stack.model_answered(run), "the provider to answer")
    stack.restart_agentd(kill=True)  # kill -9 in the middle of the model call
    hold.unlink()
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED", final

    rows = records(stack)
    check_chain(rows)
    starts = [r for r in rows if r["type"] == "agentd.started"]
    assert len(starts) == 2 and starts[0]["data"]["incarnation"] != starts[1]["data"]["incarnation"], starts
    assert starts[0]["data"]["fault_script"] is True, starts
    # The attempt the kill cut off recorded itself before its request went out (call.started, with
    # the policy's decision), and has no call record: the next agentd's start comes after it. The
    # next agentd charged its open reservation in full, after it started; the call's second attempt
    # has its call.started too, and its record names it.
    started = [r for r in rows if r["type"] == "call.started" and r["data"]["name"] == "audit_v1"]
    model = [r for r in rows if r["type"] == "call" and r["data"]["name"] == "audit_v1"]
    assert [(r["data"]["attempt"], r["data"]["policy"]["decision"]) for r in started] == [(1, "allow"), (2, "allow")], started
    assert started[0]["seq"] < starts[1]["seq"] < started[1]["seq"], (started, starts)
    assert [(r["data"]["attempt"], r["data"]["status"], r["data"]["started_seq"]) for r in model] == [(2, "ok", started[1]["seq"])], model
    assert all(r["actor"] == "run" and r["workflow_id"] == run["run_id"] for r in started), started
    [settled] = [r for r in rows if r["type"] == "budget.settled_open"]
    [charged] = stack.ledger("SELECT amount_micros FROM budget_entries WHERE workflow_id = ? AND kind = 'SETTLE' "
                             "AND reason = 'open_at_start'", run["run_id"])
    assert starts[1]["seq"] < settled["seq"] and (settled["actor"], settled["workflow_id"]) == ("agentd", run["run_id"]), settled
    assert settled["data"] == {"count": 1, "amount_micros": charged["amount_micros"]} and charged["amount_micros"] > 0, settled
    check_anchors(stack, run, rows)
    code, rep = verify(stack)
    assert code == 0, rep
    # verify says so: an attempt cut short, not a mismatch.
    assert [(a["run_id"], int(a["attempt"]), int(a["seq"]), int(a["restarted_seq"])) for a in rep["attempts_cut_short"]] == [
        (run["run_id"], 1, started[0]["seq"], starts[1]["seq"])], rep


def test_refused_runtime_calls_are_recorded(stack: Stack) -> None:
    """What agentd's Runtime entry refuses goes into the audit log as its log has it: each refusal
    the log writes in full, and the count of those it does not."""
    stack.start_agentd()
    listen = stack.runtime_listen()
    target = "unix:" + listen.removeprefix("unix://") if listen.startswith("unix://") else listen.removeprefix("tcp://")
    with grpc.insecure_channel(target) as channel:
        s = WorkflowServiceStub(channel)
        for call in (lambda: s.SignalWorkflowExecution(ws.SignalWorkflowExecutionRequest(namespace="default")),
                     lambda: s.TerminateWorkflowExecution(ws.TerminateWorkflowExecutionRequest(namespace="default")),
                     lambda: s.PollWorkflowTaskQueue(ws.PollWorkflowTaskQueueRequest(
                         namespace="default", task_queue=TaskQueue(name="someone-elses")), timeout=5)):
            try:
                call()
                raise AssertionError("a call the Runtime entry should refuse went through")
            except grpc.RpcError as e:
                assert e.code() == grpc.StatusCode.PERMISSION_DENIED, e
    logged = wait_for(lambda: (lines := [e for e in stack.agentd_log() if e.get("msg") == "runtime call refused"])
                      and len(lines) == 3 and lines, "the refusals in agentd's log")
    refused = wait_for(lambda: (rows := [r for r in records(stack) if r["type"] == "runtime.refused"]) and len(rows) == 3
                       and rows, "the refusals in the audit log")
    check_chain(records(stack))
    for line, r in zip(logged, refused):
        assert (r["actor"], r["workflow_id"], r["peer_uid"]) == ("agentd", "", None), r
        assert (r["data"]["method"], r["data"]["reason"], r["data"]["count"]) == (line["method"], line["reason"], 1), (line, r)
    assert [r["data"]["method"].rsplit("/", 1)[1] for r in refused] == [
        "SignalWorkflowExecution", "TerminateWorkflowExecution", "PollWorkflowTaskQueue"], refused


def test_the_chains_head_goes_to_temporal(stack: Stack) -> None:
    """agentd pushes the chain's head to Temporal within 30 seconds of a change, and as it stops, to
    a workflow of its own that nobody polls: the records no call's result names, agentd's own at the
    chain's end among them, are anchored too. agentctl checks each head against the chain, and says
    how many records no head follows yet."""
    stack.start_agentd()
    stack.start_worker()
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
    done = records(stack)[-1]
    wait_for(lambda: (h := pushed(stack)) and h[-1]["seq"] == done["seq"], "the head pushed while agentd runs", timeout=60)

    # A record just before agentd stops goes out as it stops.
    r = stack.agentctl_run("runs", "start", "no-such-task")
    assert r.returncode != 0 and "unknown task" in r.stderr, (r.returncode, r.stderr)
    refused = records(stack)[-1]
    assert (refused["type"], refused["data"]["task"]) == ("run.refused", "no-such-task"), refused
    stack.agentd.stop()
    rows = records(stack)
    assert rows[-1]["seq"] == refused["seq"], rows[-1]
    heads = wait_for(lambda: (h := pushed(stack)) and h[-1]["seq"] == refused["seq"] and h, "agentd's last head in Temporal")
    assert heads[-1]["sha256"] == refused["sha256"], heads
    # The chain's own workflow, named by its first record.
    chain = rows[0]["sha256"][:16]
    assert {(h["signal"], h["workflow_id"].rsplit("-", 3)[0]) for h in heads} == {("anchor", f"{ANCHOR_TYPE}-{chain}")}, heads
    by_seq = {r["seq"]: r for r in rows}
    assert all(by_seq[h["seq"]]["sha256"] == h["sha256"] for h in heads), heads

    stack.restart_agentd()
    again = records(stack)[-1]
    assert again["type"] == "agentd.started" and again["data"]["chain"] == chain, again  # a later start names it
    assert "chain" not in rows[0]["data"], rows[0]  # the first record cannot name its own sha256
    code, rep = verify(stack)
    assert code == 0 and rep["external_anchors_checked"] >= len(heads) and rep["chain"] == chain, rep
    assert rep["last_external_anchor_seq"] >= refused["seq"] and rep["other_anchor_workflows"] == 0, rep
    assert rep["records_after_external_anchor"] == rep["head_seq"] - rep["last_external_anchor_seq"], rep
    r = verify_text(stack)
    assert r.returncode == 0 and "heads pushed to Temporal checked" in r.stdout, r.stdout


def test_two_agentds_on_one_temporal_each_verify_their_own(stack: Stack, tmp_path: Path) -> None:
    """Two agentds, with ledgers of their own, on one Temporal namespace: each pushes its chain's head
    to anchor workflows of the chain's own, and each verifies clean, counting the other's workflows.
    --allow-chain accepts the other's chain because it pushed after this chain's first push: an
    agentd beside this one, not a ledger this one replaced."""
    stack.start_agentd()
    stack.start_worker()
    (tmp_path / "other").mkdir()
    other = Stack(tmp_path / "other")
    try:
        other.temporal_address = stack.temporal_address  # the first stack's Temporal, not its own
        other.start_agentd(extra={"task_queues": {"gateway": "gateway-other", "workflows": "agentos-workflows-other"}})
        run = stack.start_run()
        assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
        stack.agentd.stop()  # each pushes its head as it stops
        other.agentd.stop()
        chains = {records(stack)[0]["sha256"][:16], records(other)[0]["sha256"][:16]}
        assert len(chains) == 2, chains
        wait_for(lambda: {h["workflow_id"].rsplit("-", 3)[0] for h in pushed(stack)} == {f"{ANCHOR_TYPE}-{c}" for c in chains},
                 "both chains' heads in Temporal")
        stack.restart_agentd()
        other.restart_agentd()
        # The first stack pushes again as it stops, after the other's first push: each chain pushed
        # after the other began.
        mine, head = f"{ANCHOR_TYPE}-{records(stack)[0]['sha256'][:16]}-", records(stack)[-1]["seq"]
        stack.restart_agentd()
        wait_for(lambda: any(h["workflow_id"].startswith(mine) and h["seq"] >= head for h in pushed(stack)),
                 "the first stack's head pushed again", timeout=60)
        for s in (stack, other):
            own = records(s)[0]["sha256"][:16]
            (theirs,) = chains - {own}
            # The other agentd's chain is a mismatch until named: it looks just like the old key of a
            # chain rewritten from its first record.
            code, rep = verify(s)
            assert code == 1 and rep["other_chains"] == [theirs], rep
            assert [m["run_id"] for m in rep["anchors_mismatched"]] == [f"{ANCHOR_TYPE}-{theirs}-*"], rep
            code, rep = verify(s, "--allow-chain", theirs)
            assert code == 0 and rep["chain"] == own and rep["other_anchor_workflows"] == 1, rep
            assert rep["external_anchors_checked"] >= 1 and rep["last_external_anchor_seq"] >= 1, rep
    finally:
        other.close()


def test_an_anchor_workflow_made_by_someone_else(stack: Stack) -> None:
    """Someone with Temporal's API makes the day's anchor workflow of agentd's chain before agentd's
    first push, as a workflow of their own: SignalWithStart signals it all the same. agentd looks at
    it, and says so; verify reports it, and does not take what it holds for the chain's heads."""
    stack.start_agentd()
    chain = records(stack)[0]["sha256"][:16]
    workflow_id = f"{ANCHOR_TYPE}-{chain}-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
    stack._temporal(lambda c: c.start_workflow("someone-elses", id=workflow_id, task_queue="elsewhere"))
    wait_for(lambda: any(e.get("msg") == "the audit log's anchor workflow is not agentd's" for e in stack.agentd_log()),
             "agentd to find the anchor workflow is not its own", timeout=60)
    code, rep = verify(stack)
    assert code == 1, rep
    assert any(m["run_id"] == workflow_id and '"someone-elses"' in m["why"] for m in rep["anchors_mismatched"]), rep
    assert rep["external_anchors_checked"] == 0, rep


def test_an_emptied_log_does_not_verify(stack: Stack) -> None:
    """Someone with the ledger empties the audit log, while agentd runs: a log with no records is no
    agentd's, and what Temporal holds of it, the heads agentd pushed and the results that name
    records, says there were."""
    stack.start_agentd()
    stack.start_worker()
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
    wait_for(lambda: pushed(stack), "a head pushed to Temporal", timeout=60)
    db = sqlite3.connect(stack.data_dir / "ledger.db", timeout=30)
    db.execute("DELETE FROM audit_log")
    db.commit()
    db.close()
    assert records(stack) == []
    code, rep = verify(stack)
    assert code == 1 and rep["records"] == 0 and rep["chain"] == "", rep
    whys = [m["why"] for m in rep["anchors_mismatched"]]
    assert "Temporal holds heads or anchored results, but the audit log has no records" in whys, rep
    # agentd itself knows its start, and the last record it appended: neither is there.
    assert any(w.startswith("this agentd's start (agentd.started)") for w in whys), rep
    assert any(w.startswith("the last record this agentd appended") for w in whys), rep
    assert rep["runs_before_log"] == [], rep  # without a first record, nothing came before it
    # No record speaks of any row: the run's is a difference.
    assert any(m["kind"] == "run" and m["id"] == run["run_id"] for m in rep["state_mismatched"]), rep


def test_a_chain_rewritten_from_its_first_record(stack: Stack) -> None:
    """The whole chain rewritten from record 1, every hash made again, while agentd is down: it is
    whole, and has a new key, so it has no anchor workflows of its own yet. The old key's are still
    in Temporal, with heads this agentd pushed: a ledger replaced, by a reset or a rewrite, which
    only the operator can tell apart. A mismatch that names the old chain, states when each chain
    pushed, and suggests no flag; --allow-chain, for another agentd's chain, does not accept it,
    --allow-replaced-ledger does."""
    stack.start_agentd()
    stack.agentd.stop()  # pushes its head as it stops
    old = records(stack)[0]["sha256"][:16]
    wait_for(lambda: pushed(stack), "a head pushed to Temporal", timeout=60)
    db = sqlite3.connect(stack.data_dir / "ledger.db")
    db.row_factory = sqlite3.Row
    prev = GENESIS
    for row in db.execute("SELECT * FROM audit_log ORDER BY seq").fetchall():
        r = {**dict(row), "prev_sha256": prev}
        if r["seq"] == 1:
            r["payload"] = json.dumps({**json.loads(r["payload"]), "pid": 1}, separators=(",", ":"), sort_keys=True)
        r["sha256"] = record_hash(r)
        db.execute("UPDATE audit_log SET payload = ?, prev_sha256 = ?, sha256 = ? WHERE seq = ?",
                   (r["payload"], r["prev_sha256"], r["sha256"], r["seq"]))
        prev = r["sha256"]
    db.commit()
    db.close()
    pushed_last = max(h["seq"] for h in pushed(stack))
    stack.restart_agentd()
    # The new chain has no anchor workflows yet; the highest head this agentd pushed is the old one's.
    [found] = last_start(stack)["data"]["anchor_findings"]
    assert found["seq"] == pushed_last and found["workflow_id"].startswith(f"{ANCHOR_TYPE}-{old}-") and \
        f'is record {pushed_last} of chain "{old}"' in found["why"], found
    rows = records(stack)
    check_chain(rows)
    new = rows[0]["sha256"][:16]
    assert new != old
    code, rep = verify(stack)
    assert code == 1 and rep["broken_at_seq"] == 0 and rep["chain"] == new and rep["other_chains"] == [old], rep
    [m] = [m for m in rep["anchors_mismatched"] if m["run_id"] == f"{ANCHOR_TYPE}-{old}-*"]
    assert "this agentd pushed its heads, the last record" in m["why"] and "--allow" not in m["why"], m
    r = verify_text(stack)
    assert r.returncode == 1 and old in r.stdout, r.stdout
    code, rep = verify(stack, "--allow-chain", old)
    assert code == 1 and rep["other_anchor_workflows"] == 0, rep
    assert any("--allow-chain accepts only a chain that pushed after this chain's first push" in m["why"]
               for m in rep["anchors_mismatched"]), rep
    code, rep = verify(stack, "--allow-replaced-ledger", old)
    assert code == 0 and rep["other_anchor_workflows"] >= 1, rep


def test_two_ledgers_of_one_agentd_live_at_once(stack: Stack) -> None:
    """One agentd, two ledgers taking turns in its data directory: the first, then a new one, then
    the first again. Each pushes its chain's heads under the same agentd. The first's verify finds
    the second's chain pushing after its own first push, by Temporal's times: two ledgers of one
    agentd live at once, which no reset makes, and which --allow-replaced-ledger does not accept."""
    stack.start_agentd()
    stack.agentd.stop()
    first = records(stack)[0]["sha256"][:16]
    saved = stack.tmp / "first-ledger"
    saved.mkdir()
    for f in ledger_files(stack):
        if f.exists():
            shutil.move(f, saved / f.name)  # set aside, as a reset does
    stack.restart_agentd()
    stack.agentd.stop()  # the second ledger pushes its head as it stops
    second = records(stack)[0]["sha256"][:16]
    wait_for(lambda: {h["workflow_id"].rsplit("-", 3)[0] for h in pushed(stack)} >= {f"{ANCHOR_TYPE}-{c}" for c in (first, second)},
             "both chains' heads in Temporal")
    for f in ledger_files(stack):
        f.unlink(missing_ok=True)
        if (saved / f.name).exists():
            shutil.copy2(saved / f.name, f)
    stack.restart_agentd()
    assert records(stack)[0]["sha256"][:16] == first
    code, rep = verify(stack, "--allow-replaced-ledger", second)
    [m] = [m for m in rep["anchors_mismatched"] if m["run_id"] == f"{ANCHOR_TYPE}-{second}-*"]
    assert code == 1 and "two ledgers of this agentd were live at once" in m["why"], rep


def test_runs_from_before_the_log_are_unchecked(stack: Stack) -> None:
    """A dev ledger keeps runs of an older agentd (M1 to M4), made before the audit log began: no
    record of theirs, and nothing left in Temporal. agentctl counts them unchecked (exit 2), unless
    --allow-before-log."""
    stack.start_agentd()
    stack.agentd.stop()
    db = sqlite3.connect(stack.data_dir / "ledger.db")
    db.execute("INSERT INTO runs (workflow_id, run_id, task, params_json, budget_limit_micros, created_at) "
               "VALUES ('m4-run', ?, 'security-audit', '{}', 1, '2026-01-01T00:00:00Z')", (str(uuid.uuid4()),))
    db.commit()
    db.close()
    stack.restart_agentd()
    code, rep = verify(stack)
    assert code == 2 and [r["run_id"] for r in rep["runs_before_log"]] == ["m4-run"], rep
    assert rep["runs_unavailable"] == [] and rep["state_mismatched"] == [], rep
    code, rep = verify(stack, "--allow-before-log")
    assert code == 0, rep


def test_a_lost_anchor_workflow_is_found(stack: Stack) -> None:
    """Someone with Temporal's API deletes the anchor workflow agentd pushed the chain's head to. Once
    agentd has run longer than two pushes, records older than that which no pushed head covers are
    a mismatch: nothing outside the ledger vouches for them any more."""
    stack.start_agentd()
    started = time.monotonic()
    heads = wait_for(lambda: pushed(stack), "the first head pushed to Temporal", timeout=60)

    async def delete(c: Client) -> None:
        h = c.get_workflow_handle(heads[0]["workflow_id"])
        d = await h.describe()
        await c.workflow_service.delete_workflow_execution(ws.DeleteWorkflowExecutionRequest(
            namespace="default", workflow_execution=WorkflowExecution(workflow_id=d.id, run_id=d.run_id)))
    stack._temporal(delete)
    wait_for(lambda: not pushed(stack), "the anchor workflow gone", timeout=30)
    time.sleep(max(0.0, 65 - (time.monotonic() - started)))  # past twice the push interval of 30 s
    code, rep = verify(stack)
    assert code == 1 and rep["external_anchors_checked"] == 0, rep
    assert any(m["seq"] == "1" and "no head pushed to Temporal covers them" in m["why"] for m in rep["anchors_mismatched"]), rep
