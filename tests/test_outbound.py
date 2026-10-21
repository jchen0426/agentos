"""agentd's requests to the world outside the machine go straight to their hosts: it takes no
proxy from its environment, which belongs to whatever started it (internal/outbound, design doc
section 8). Started with proxy variables naming a listener, agentd says which ones it ignores,
and the listener hears nothing from it. Temporal's address here is localhost. (with the dot):
it resolves to this machine, but it is not a name Go keeps off a proxy, as it does localhost and
loopback addresses. The model providers and GitHub are on loopback addresses in these tests,
which no proxy setting reaches: internal/outbound's Go test covers HTTP."""

import socket
import threading

import pytest

from tests.harness import TERMINAL, Stack

PROXY_VARS = ["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"]


def test_agentd_takes_no_proxy_from_its_environment(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    reached: list[bytes] = []
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()

        def record() -> None:  # a proxy that writes down what it is asked, and answers nothing
            while True:
                try:
                    conn, _ = listener.accept()
                except OSError:
                    return
                with conn:
                    conn.settimeout(2)
                    try:
                        reached.append(conn.recv(200))
                    except OSError:
                        reached.append(b"")
        threading.Thread(target=record, daemon=True).start()

        proxy = f"http://127.0.0.1:{listener.getsockname()[1]}"
        for v in PROXY_VARS:
            monkeypatch.setenv(v, proxy)
        monkeypatch.delenv("NO_PROXY", raising=False)
        monkeypatch.delenv("no_proxy", raising=False)
        port = stack.temporal_address.rsplit(":", 1)[1]
        stack.start_agentd(extra={"temporal": {"address": f"localhost.:{port}"}})
        stack.start_worker()
        # The gateway activity, the doorbells and the Worker's Runtime connection all reach
        # Temporal at that address.
        run = stack.start_run(dry_run=True)
        assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"

    warned = [e for e in stack.agentd_log() if e.get("msg", "").startswith("agentd ignores the proxy settings")]
    assert len(warned) == 1, warned
    assert set(PROXY_VARS) <= set(warned[0]["variables"]), warned
    assert not reached, f"agentd went through the environment's proxy: {reached}"
