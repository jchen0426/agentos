"""T13: the two model interfaces, Claude-native and OpenAI-compatible (OpenAI's Responses API,
and Chat Completions for Ollama and vLLM), map the same faults to the same outcome (design
doc, section 11): a 429, a 500, an answer cut off on its way (as a timeout would), a refusal,
and an answer that breaks the output schema. Which requests are retried, how each is settled
and what the run ends with do not depend on the provider. (The part about agentctl runs
explain waits, with T12, for M5.)

One simulator, internal/modelsim, speaks all three APIs and answers all three alike: the
same report, the same usage. At the same prices the same answer costs the same; a Chat
Completions server is one of the user's own, and free (agentd refuses prices for it, since
it cannot count tokens before a request).
"""

import json

import pytest

from tests.harness import AUDIT_SCHEMA, TERMINAL, TEST_POLICY, Stack

PRICES = """
price_table_version: t13
models:
  anthropic/claude-opus-5-5: {input: 1.00, output: 5.00}
  openai/gpt-6.1-sol:        {input: 1.00, output: 5.00}
  local/gpt-oss:20b:         {input: 0, output: 0}
  local/cloud-coder:         {input: 0, output: 0}
"""
USAGE = 1000 * 1 + 500 * 5  # micros: every answer's 1000 input and 500 output tokens, when priced

# provider, model, its models.yaml settings, the path of its answers, and what an answer costs
APIS = {
    "anthropic": ("anthropic", "claude-opus-5-5", '{{adapter: anthropic, base_url: "{url}"}}', "/v1/messages", USAGE),
    "responses": ("openai", "gpt-6.1-sol",
                  '{{adapter: openai_compat, api: responses, base_url: "{url}/v1", efforts: [low, high]}}',
                  "/v1/responses", USAGE),
    "chat": ("local", "gpt-oss:20b",
             '{{adapter: openai_compat, api: chat, base_url: "{url}/v1", context_tokens: 65536, efforts: [low, high]}}',
             "/v1/chat/completions", 0),
}
POLICY = TEST_POLICY.replace("allowed_providers: [fake, anthropic]", "allowed_providers: [anthropic, openai, local]")

# For each fault: what the run ends with, and how each request to the provider is settled.
EXPECTED = {
    "429": ("ok", ["rejected", "usage"]),  # unbilled, retried
    "500": ("ok", ["outcome_unknown", "usage"]),  # it may have run: charged its reservation, retried
    "drop": ("ok", ["outcome_unknown", "usage"]),  # cut off on its way: the same
    "refusal": ("refused", ["usage"]),  # an answer, billed, not retried
    "invalid": ("ok", ["usage", "usage"]),  # breaks the schema: asked for again
}


def models_yaml(api: str, url: str, model: str | None = None) -> str:
    provider, default, settings, _, _ = APIS[api]
    return f"""
providers:
  {provider}: {settings.format(url=url)}
tasks:
  audit_v1:
    model: {provider}/{model or default}
    max_tokens: 4096
    effort: xhigh
    output_schema: {AUDIT_SCHEMA}
    blobs: [patch]
"""


@pytest.mark.parametrize("api", list(APIS))
def test_t13_every_interface_maps_faults_alike(stack: Stack, api: str) -> None:
    stack.start_agentd(models=models_yaml(api, stack.start_modelsim()), prices=PRICES, policy=POLICY)
    stack.start_worker()
    _, _, _, path, usage = APIS[api]
    for fault, (outcome, settlements) in EXPECTED.items():
        stack.model_faults({api: [fault]})
        before = len([r for r in stack.modelsim_requests(api) if r["path"] == path])
        run = stack.start_run()
        final = stack.wait_state(run, TERMINAL, timeout=120)
        assert final["state"] == "SUCCEEDED", (fault, final)
        assert json.loads(final["result_json"])["outcome"] == outcome, (fault, final)

        # The provider got the fault once, then answered: as many requests as settlements.
        sent = [r for r in stack.modelsim_requests(api) if r["path"] == path][before:]
        assert [r["fault"] for r in sent] == [fault] + [""] * (len(settlements) - 1), (fault, sent)
        entries = stack.budget_entries(run)
        reserved = {e["request_id"]: e["amount_micros"] for e in entries if e["kind"] == "RESERVE"}
        settled = [e for e in entries if e["kind"] == "SETTLE"]
        assert [e["reason"] for e in settled] == settlements, (fault, [dict(e) for e in settled])
        for e in settled:
            want = {"usage": usage, "rejected": 0, "outcome_unknown": reserved[e["request_id"]]}[e["reason"]]
            assert e["amount_micros"] == want, (fault, dict(e))

    # Every request asked for the task's max_tokens, and for its effort as the provider takes
    # it: Claude takes xhigh; the others list low and high, and get the highest not above it.
    sent = [r for r in stack.modelsim_requests(api) if r["path"] == path]
    want = "xhigh" if api == "anthropic" else "high"
    assert {(r["max_tokens"], r["effort"]) for r in sent} == {(4096, want)}, sent
    if api == "responses":
        # The tier the price table lists, nothing kept at OpenAI, and the one random identifier
        # of this agentd, kept in its data directory.
        identifier = (stack.data_dir / "safety-identifier").read_text().strip()
        assert identifier.startswith("agentos-") and len(identifier) > 20, identifier
        assert {(r["service_tier"], r["store"], r["safety_identifier"]) for r in sent} == {("default", False, identifier)}
    # A server on this machine is asked whether the model runs on it before each count and each
    # request (any program here can rename a model through Ollama's API); Claude never is.
    shows = [r["model"] for r in stack.modelsim_requests("ollama")]
    assert shows == ([] if api == "anthropic" else [APIS[api][1]] * (2 * len(sent))), (len(shows), len(sent))


def test_t13_a_cloud_model_is_never_sent_the_diff(stack: Stack) -> None:
    # Under a name of the user's choosing: only the server knows it runs on Ollama's servers.
    stack.start_agentd(models=models_yaml("chat", stack.start_modelsim(), model="cloud-coder"),
                       prices=PRICES, policy=POLICY)
    stack.start_worker()
    run = stack.start_run()
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED", final
    assert "cloud model" in json.loads(final["result_json"])["outcome"], final
    assert stack.modelsim_requests("chat") == [], "a request went to a cloud model"
    assert [r["model"] for r in stack.modelsim_requests("ollama")] == ["cloud-coder"]
    assert stack.budget_entries(run) == [], "nothing is reserved for a request that is never sent"


@pytest.mark.audit_verify(skip="agentd refuses to start, by design")
def test_t13_agentd_refuses_prices_for_a_chat_server(stack: Stack) -> None:
    # Its tokens are only estimated before the request: a reservation is a bound only at 0.
    with pytest.raises(AssertionError, match="api chat, whose input is only estimated"):
        stack.start_agentd(models=models_yaml("chat", "http://127.0.0.1:1"), policy=POLICY,
                           prices=PRICES.replace("local/gpt-oss:20b:         {input: 0, output: 0}",
                                                 "local/gpt-oss:20b:         {input: 0.10, output: 0.40}"))
