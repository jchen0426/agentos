"""--verify-after-each (tests/verify_each.py) on tests of its own, run by a pytest of their own: a
stack whose agentctl audit verify exits as each test sets it. An honest test passes, and so does a
marked tampering test, but only with the exit it declares."""

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

CONFTEST = '''
import json
import subprocess

import pytest

from tests import verify_each

OUTCOMES = {}  # each test's: passed, or the message it failed with


def pytest_addoption(parser):
    verify_each.add_option(parser)


def pytest_configure(config):
    verify_each.configure(config)


def pytest_runtest_logreport(report):
    if report.when == "call":
        OUTCOMES[report.nodeid.split("::")[-1]] = "passed" if report.passed else str(report.longrepr)


def pytest_sessionfinish(session):
    (session.config.rootpath / "outcomes.json").write_text(json.dumps(OUTCOMES))


class Agentd:
    def __init__(self):
        self.running = True

    def alive(self):
        return self.running


class Stack:
    """A stack whose verify exits with exit, and says so."""

    def __init__(self):
        self.agentd, self.exit = Agentd(), 0

    def audit_verify(self, *args):
        return subprocess.CompletedProcess(["agentctl", "audit", "verify"], self.exit, f"state  {self.exit} differences\\n", "")


@pytest.fixture
def stack():
    return Stack()
'''

TESTS = '''
import pytest


def test_honest(stack):
    pass


def test_tampers_unmarked(stack):
    stack.exit = 1


@pytest.mark.audit_verify(exit=1)
def test_tampers_as_declared(stack):
    stack.exit = 1


@pytest.mark.audit_verify(exit=1)
def test_declares_a_mismatch_and_verifies(stack):
    stack.exit = 0


@pytest.mark.audit_verify(exit=2)
def test_declares_another_exit(stack):
    stack.exit = 1


@pytest.mark.audit_verify(skip="agentd refuses to start, by design")
def test_agentd_refused(stack):
    stack.agentd.running = False


def test_agentd_gone_unmarked(stack):
    stack.agentd.running = False


def test_fails_on_its_own(stack):
    stack.exit = 1
    assert False, "its own failure"


def test_without_a_stack():
    pass


def test_with_no_agentd_started(stack):
    stack.agentd = None
    stack.exit = 1
'''


def run(tmp_path: Path, *args: str) -> tuple[dict[str, str], str]:
    """The outcome of each test of TESTS (passed, or the message it failed with), and pytest's output."""
    (tmp_path / "conftest.py").write_text(CONFTEST)
    (tmp_path / "test_them.py").write_text(TESTS)
    r = subprocess.run([sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "--rootdir", str(tmp_path),
                        str(tmp_path / "test_them.py"), *args],
                       capture_output=True, text=True, timeout=120, cwd=tmp_path, env={"PYTHONPATH": str(REPO), "PATH": ""})
    return json.loads((tmp_path / "outcomes.json").read_text()), r.stdout + r.stderr


def test_honest_tests_pass_and_marked_ones_only_with_their_exit(tmp_path: Path) -> None:
    got, out = run(tmp_path, "--verify-after-each")
    assert {name for name, o in got.items() if o == "passed"} == {
        "test_honest", "test_tampers_as_declared", "test_agentd_refused", "test_without_a_stack", "test_with_no_agentd_started"}, (got, out)
    assert "verify exited 1, and the test declares 0" in got["test_tampers_unmarked"], got
    assert "| state  1 differences" in got["test_tampers_unmarked"], got  # verify's output, quoted
    assert "verify exited 0, and the test declares 1" in got["test_declares_a_mismatch_and_verifies"], got
    assert "verify exited 1, and the test declares 2" in got["test_declares_another_exit"], got
    assert "agentd is not running after the test" in got["test_agentd_gone_unmarked"], got
    assert "its own failure" in got["test_fails_on_its_own"], got  # not verified: its own failure stands
    assert "not verified: test_them.py::test_agentd_refused: agentd refuses to start, by design" in out, out
    assert "verify after the test: 1 exited 0, 1 as their mark declares, 1 not verified" in out, out
    assert "exit 1: test_them.py::test_tampers_as_declared" in out, out


def test_without_the_option_nothing_changes(tmp_path: Path) -> None:
    got, out = run(tmp_path)
    assert {name for name, o in got.items() if o != "passed"} == {"test_fails_on_its_own"}, (got, out)
    assert "--verify-after-each" not in out, out
