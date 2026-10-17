"""T7: 50 model calls in one run compete for its budget (design doc, sections 9 and 11).

The run starts with room for about 13 reservations. The other calls wait in PAUSED_BUDGET
until `agentctl budget extend` rings the doorbell; then every call finishes. After the
extension the fake provider answers 429 and 500, loses an answer it billed, never receives
one request and answers one as the fallback model. At every entry of the ledger, settled
spending plus open reservations stay within the limit, and the provider's own record
matches the ledger request by request.
"""

import json

from tests.harness import AUDIT_SCHEMA, TERMINAL, Stack, wait_for
from tests.t7_workflows import CALLS

MODEL, FALLBACK = "fake/audit-fixture", "fake/audit-fallback"
PRICES = {MODEL: {"input": 1.00, "output": 5.00}, FALLBACK: {"input": 2.00, "output": 10.00}}  # $ per M tokens
MODELS = f"""
providers:
  fake: {{adapter: fake}}
tasks:
  audit_v1: {{model: {MODEL}, max_tokens: 4096, fallback: [{FALLBACK}], output_schema: {AUDIT_SCHEMA}, blobs: [patch]}}
"""
# Every request holds its reservation for half a second before it goes out, so the calls
# really do compete: a fake answer is otherwise settled within a millisecond, and the next
# call would find the room freed again. The first wave is exactly the first CALLS Invokes
# and gets no faults. The faults hit the second wave, which has budget to spare, so every
# one of them fires.
FAULTS = [
    {"name": "audit_v1", "delay": "500ms"},
    {"name": "audit_v1", "occurrence": CALLS + 3, "provider_status": 429},
    {"name": "audit_v1", "occurrence": CALLS + 6, "provider_status": 500},
    {"name": "audit_v1", "occurrence": CALLS + 9, "provider_lose_answer": True},
    {"name": "audit_v1", "occurrence": CALLS + 12, "provider_unreachable": True},
    {"name": "audit_v1", "occurrence": CALLS + 15, "provider_served_by": FALLBACK},
]
# micros: room for about 13 reservations of about 65,500 each. A reservation covers the model
# and its fallback both billing in full, since within one request both can.
START_BUDGET = 900_000
EXTENSION = 5_000_000


def billed(record: dict) -> int:
    """What a provider record says was billed, priced here rather than by agentd."""
    u, price = record["usage"], PRICES[record["model"]]
    nano = sum(u[f"{kind}_tokens"] * round(price[kind] * 1000) for kind in ("input", "output"))
    assert u["cache_read_tokens"] == u["cache_write_5m_tokens"] == u["cache_write_1h_tokens"] == 0, u
    return -(-nano // 1000)  # whole micros, rounded up


def test_t7_concurrent_calls_never_overspend(stack: Stack) -> None:
    prices = json.dumps({"price_table_version": "t7", "models": PRICES})
    # 50 calls, 30 of them sent again after the pause: more than the default loop limits.
    stack.start_agentd(models=MODELS, prices=prices, faults=FAULTS,
                       extra={"loop_limits": {"model_calls": 200, "gateway_calls": 400}})
    stack.start_worker(workflows="tests.t7_workflows:FanOut")
    run = stack.start_run(budget_micros=START_BUDGET)

    # First wave: the calls that could reserve finish; the rest wait for more budget.
    stack.wait_state(run, {"PAUSED_BUDGET"}, timeout=120)
    paused = wait_for(lambda: (r := stack.get_run(run))["budget_reserved_micros"] == "0" and r,
                      "the first wave to settle", timeout=60)
    assert paused["state"] == "PAUSED_BUDGET", paused
    granted = [e for e in stack.budget_entries(run) if e["kind"] == "RESERVE"]
    assert 0 < len(granted) < CALLS, len(granted)

    extended = stack.agentctl("budget", "extend", run["run_id"], "--usd", str(EXTENSION / 1e6))
    assert int(extended["budget_limit_micros"]) == START_BUDGET + EXTENSION, extended

    final = stack.wait_state(run, TERMINAL, timeout=180)
    assert final["state"] == "SUCCEEDED", final
    # One doorbell woke every waiting call: none was lost or left waiting for the hourly recheck.
    assert json.loads(final["result_json"])["statuses"] == ["ok"] * CALLS

    # Replay the ledger: after every entry, settled + open reservations <= the limit.
    entries = stack.budget_entries(run)
    reserves = {e["request_id"]: e for e in entries if e["kind"] == "RESERVE"}
    closes: dict[str, dict] = {}
    limit, spent, held = START_BUDGET, 0, 0
    for e in entries:
        if e["kind"] == "EXTEND":
            limit += e["amount_micros"]
        elif e["kind"] == "RESERVE":
            held += e["amount_micros"]
        else:
            assert e["request_id"] not in closes, dict(e)  # closed once
            closes[e["request_id"]] = dict(e)
            held -= reserves[e["request_id"]]["amount_micros"]
            spent += e["amount_micros"] if e["kind"] == "SETTLE" else 0
        assert spent + held <= limit, (dict(e), spent, held, limit)
    assert held == 0 and closes.keys() == reserves.keys()
    assert (int(final["budget_spent_micros"]), int(final["budget_reserved_micros"]), int(final["budget_limit_micros"])) \
        == (spent, 0, limit)

    # The provider's own record against the ledger, request by request.
    records = stack.provider_calls(run)
    provider = {r["request_id"]: r for r in records}
    assert len(provider) == len(records)  # one record per request
    assert provider.keys() <= reserves.keys()  # nothing reached the provider unreserved
    for request_id, reserve in reserves.items():
        close, record = closes[request_id], provider.get(request_id)
        assert close["amount_micros"] <= reserve["amount_micros"], (close, dict(reserve))
        if close["kind"] == "RELEASE":
            assert record is None and close["reason"] == "not_sent", (close, record)
        elif record["status"] == 429:  # turned away: certainly not billed
            assert (close["reason"], close["amount_micros"]) == ("rejected", 0), (close, record)
        elif close["reason"] == "outcome_unknown":  # a 500, or an answer lost: charged the whole reservation
            assert billed(record) <= close["amount_micros"] == reserve["amount_micros"], (close, record)
        else:
            assert (close["reason"], close["model"], close["amount_micros"]) == ("usage", record["model"], billed(record)), \
                (close, record)
    assert sum(billed(r) for r in records if r["status"] == 200) <= spent

    # Every fault fired: retries, a lost answer, an unsent request and the fallback were all accounted.
    assert sorted(r["status"] for r in records if r["status"] != 200) == [429, 500]
    reasons = [c["reason"] for c in closes.values()]
    assert reasons.count("outcome_unknown") == 2 and reasons.count("not_sent") == 1, reasons  # the 500 and the lost answer
    assert [c["model"] for c in closes.values() if c["model"] == FALLBACK] == [FALLBACK]

    # Logical calls and network requests are counted apart: 50 answers, more requests.
    answered = {m["activity_id"] for m in stack.model_calls(run) if m["response_blob"] is not None}
    assert len(answered) == CALLS
    assert len(reserves) == CALLS + 4  # one request per answer, plus one per fault but the fallback
