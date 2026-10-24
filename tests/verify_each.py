"""--verify-after-each: after each test that started a stack, while the stack is still up, run
`agentctl audit verify` against its agentd with the default flags. An honest flow leaves a log that
verifies, so verify must exit 0; anything else fails the test, with verify's output quoted.

A test that tampers with the ledger or Temporal on purpose, or expects another verdict for its own
reasons, declares the exit it expects: @pytest.mark.audit_verify(exit=1). A test whose agentd
refuses to start by design cannot be verified, and says why: @pytest.mark.audit_verify(skip="...").
A test that fails on its own is not verified: its failure says more.

No wait is needed for runs still executing as the test ends: verify takes an attempt whose call
record is yet to come for one in flight while its run runs, and while agentd says it is executing.

Off by default: without the option nothing here is registered, and nothing changes (conftest.py).
"""
from __future__ import annotations

import textwrap

import pytest

OPTION = "--verify-after-each"
MARK = "audit_verify"
STACKS = ("stack", "restartable_stack", "live_stack")  # the fixtures that start a stack (conftest.py)


def add_option(parser: pytest.Parser) -> None:
    parser.addoption(OPTION, action="store_true", default=False,
                     help="after each test with a stack, run agentctl audit verify against its agentd: an honest test "
                          f"must exit 0, one marked {MARK}(exit=N) with N")


def configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", f"{MARK}(exit=0, skip=None): the exit of agentctl audit verify after the test, "
                                       f"under {OPTION}; or why the test cannot be verified")
    if config.getoption(OPTION):
        config.pluginmanager.register(VerifyAfterEach(), "verify-after-each")


class VerifyAfterEach:
    """The plugin, registered only under the option."""

    def __init__(self) -> None:
        self.verified: list[str] = []            # exited 0, as honest tests must
        self.declared: list[tuple[str, int]] = []  # exited as their mark declares
        self.skipped: list[tuple[str, str]] = []   # not verified, and why

    @pytest.hookimpl(wrapper=True)
    def pytest_runtest_call(self, item: pytest.Item):
        result = yield  # a test that failed raises here: not verified
        funcargs = getattr(item, "funcargs", {})
        stack = next((funcargs[name] for name in STACKS if name in funcargs), None)
        if stack is None:
            return result
        mark = item.get_closest_marker(MARK)
        want, skip = (mark.kwargs.get("exit", 0), mark.kwargs.get("skip")) if mark else (0, None)
        if skip:
            self.skipped.append((item.nodeid, skip))
            return result
        if stack.agentd is None:  # the test started no agentd: no log of its to verify
            return result
        if not stack.agentd.alive():
            pytest.fail(f"{OPTION}: agentd is not running after the test, so its log cannot be verified; mark the test "
                        f"{MARK}(skip=...) if that is by design", pytrace=False)
        r = stack.audit_verify()
        if r.returncode != want:
            said = textwrap.indent((r.stdout + r.stderr).rstrip() or "(nothing)", "    | ")
            pytest.fail(f"{OPTION}: agentctl audit verify exited {r.returncode}, and the test declares {want}:\n{said}",
                        pytrace=False)
        if want:
            self.declared.append((item.nodeid, want))
        else:
            self.verified.append(item.nodeid)
        return result

    def pytest_terminal_summary(self, terminalreporter) -> None:
        tr = terminalreporter
        tr.section(OPTION)
        tr.line(f"verify after the test: {len(self.verified)} exited 0, {len(self.declared)} as their mark declares, "
                f"{len(self.skipped)} not verified")
        for nodeid, want in self.declared:
            tr.line(f"  exit {want}: {nodeid}")
        for nodeid, why in self.skipped:
            tr.line(f"  not verified: {nodeid}: {why}")
