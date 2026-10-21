"""agentd's requests to the world outside the machine go straight to their hosts: it takes no
proxy from its environment, which belongs to whatever started it (internal/outbound, design doc
section 8). The Go test sends requests through the transport; here agentd, started with proxy
variables set, says which ones it ignores."""

import pytest

from tests.harness import Stack


def test_agentd_names_the_proxy_settings_it_ignores(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    monkeypatch.setenv("no_proxy", "example.com")
    stack.start_agentd()
    warned = [e for e in stack.agentd_log() if e.get("msg", "").startswith("agentd ignores the proxy settings")]
    assert len(warned) == 1, warned
    assert {"HTTPS_PROXY", "no_proxy"} <= set(warned[0]["variables"]), warned
