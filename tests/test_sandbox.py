"""The Worker sandbox and its bridge to Temporal (design doc, section 8; the M1 prototype
of T9). Inside the sandbox the Temporal SDK reaches Temporal through agentd's Runtime entry,
and nothing else: not Temporal's own port, not the Control socket, not the network, not the
home directory, not the caller's environment.

Run with AGENTOS_SANDBOX=srt on macOS or AGENTOS_SANDBOX=bwrap on Linux.
"""

import json
import os
import secrets
import subprocess
import sys
from pathlib import Path

import pytest

from tests.harness import REPO, SANDBOX, Stack

pytestmark = pytest.mark.skipif(SANDBOX == "none", reason="needs AGENTOS_SANDBOX=bwrap or srt")


def probe(stack: Stack, canary: Path) -> dict:
    args = [sys.executable, REPO / "tests/sandbox_probe.py",
            "--temporal-port", stack.temporal_address.rsplit(":", 1)[1],
            "--control-socket", stack.control_socket, "--home", Path.home(), "--home-canary", canary,
            "--ledger", stack.data_dir / "ledger.db"]
    if SANDBOX == "bwrap":
        launcher = [REPO / "sandbox/linux/run-worker.sh", stack.runtime_socket, sys.prefix, REPO, "--"]
        args += ["--runtime", "127.0.0.1:7233"]
    else:
        launcher = [REPO / "sandbox/macos/run-worker.sh", stack.runtime_port, sys.prefix, REPO, "--"]
        args += ["--runtime", f"127.0.0.1:{stack.runtime_port}", "--proxy", "env"]
    r = subprocess.run([str(a) for a in [*launcher, *args]], capture_output=True, text=True, timeout=120,
                       cwd=REPO, env={**stack.sandbox_env(), "AGENTOS_TEST_CANARY": "a secret the Worker must not see"})
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout.strip().splitlines()[-1])


def test_worker_sandbox_reaches_temporal_only_through_agentd(stack: Stack) -> None:
    stack.start_agentd()
    canary = Path.home() / f".agentos-canary-{secrets.token_hex(4)}"
    canary.write_text("a file the Worker must not read")
    try:
        report = probe(stack, canary)
    finally:
        canary.unlink()
    print(json.dumps(report, indent=2))

    assert report["runtime_sdk"].startswith("connected"), report
    assert report["forbidden"] and all(v.startswith("refused: PERMISSION_DENIED")
                                       for v in report["forbidden"].values()), report["forbidden"]
    for key in ("temporal_sdk", "temporal_tcp", "internet_tcp", "control_socket", "home_canary", "ssh_dir", "ledger"):
        assert report[key].startswith("refused"), (key, report[key])
    assert "AGENTOS_TEST_CANARY" not in report["env"]
