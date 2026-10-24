"""--verify-after-each (tests/verify_each.py) on tests of its own, run by a pytest of their own, with
stacks whose agentctl audit verify finds what each test sets. Every stack that starts an agentd is
verified, whoever built it; an honest test passes, and so does a marked tampering test, but only
with the exit and the kinds of mismatch it declares."""

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

CONFTEST = '''
import json
import subprocess
from pathlib import Path

import pytest

from tests import harness, verify_each

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
    """A stack whose verify finds report, and exits with exit; it tells the watch as harness.Stack does."""

    def __init__(self, name="stack"):
        self.name, self.tmp, self.agentd, self.exit, self.report = name, Path("/fake") / name, None, 0, {}

    def start_agentd(self):
        self.agentd = Agentd()
        if harness.WATCH.on_start is not None:
            harness.WATCH.on_start(self)

    def agentctl_run(self, *args):
        return subprocess.CompletedProcess(args, self.exit, json.dumps(self.report), "")

    def audit_verify(self, *args):
        return subprocess.CompletedProcess(args, self.exit, f"verify of {self.name}: {sorted(self.report)}\\\\n", "")

    def close(self):
        if harness.WATCH.on_close is not None:
            harness.WATCH.on_close(self)
        if self.agentd is not None:
            self.agentd.running = False


@pytest.fixture
def stack():
    s = Stack()
    s.start_agentd()
    yield s
    s.close()
'''

TESTS = '''
import pytest

from conftest import Stack

ANCHORS = {"anchors_mismatched": [{"why": "x"}]}


def test_honest(stack):
    pass


def test_tampers_unmarked(stack):
    stack.exit, stack.report = 1, ANCHORS


@pytest.mark.audit_verify(exit=1, kinds=["anchors_mismatched"])
def test_tampers_as_declared(stack):
    stack.exit, stack.report = 1, ANCHORS


@pytest.mark.audit_verify(exit=1, kinds=["anchors_mismatched"])
def test_tampers_with_a_kind_undeclared(stack):
    stack.exit, stack.report = 1, {**ANCHORS, "state_mismatched": [{"kind": "budget"}]}


@pytest.mark.audit_verify(exit=1, kinds=["state_mismatched"])
def test_any_kind_of_state(stack):
    stack.exit, stack.report = 1, {"state_mismatched": [{"kind": "budget"}, {"kind": "call"}]}


@pytest.mark.audit_verify(exit=1, kinds=["state_mismatched:call"])
def test_only_that_kind_of_state(stack):
    stack.exit, stack.report = 1, {"state_mismatched": [{"kind": "budget"}]}


@pytest.mark.audit_verify(exit=1)
def test_declares_an_exit_without_kinds(stack):
    stack.exit, stack.report = 1, ANCHORS


@pytest.mark.audit_verify(exit=1, kinds=["anchors_mismatched"])
def test_declares_a_mismatch_and_verifies(stack):
    pass


@pytest.mark.audit_verify(exit=2, kinds=["runs_before_log"])
def test_declares_another_exit(stack):
    stack.exit, stack.report = 1, {"runs_before_log": [{}]}


def test_a_broken_chain_unmarked(stack):
    stack.exit, stack.report = 1, {"broken_at_seq": "3"}


def test_a_stack_of_its_own_tampered():
    s = Stack("own")
    s.start_agentd()
    s.exit, s.report = 1, {"runs_mismatched": [{}]}
    s.close()  # verified as it closes, before its agentd stops


def test_a_stack_of_its_own_honest():
    s = Stack("own")
    s.start_agentd()
    s.close()


@pytest.mark.audit_verify(exit=1, kinds=["anchors_mismatched"], stack="other")
def test_two_stacks_the_named_one_as_declared(stack):
    other = Stack("other")
    other.start_agentd()
    other.exit, other.report = 1, ANCHORS
    other.close()


@pytest.mark.audit_verify(exit=1, kinds=["anchors_mismatched"], stack="other")
def test_two_stacks_the_other_one_tampered(stack):
    stack.exit, stack.report = 1, ANCHORS  # not the one the mark names: it must exit 0
    other = Stack("other")
    other.start_agentd()
    other.exit, other.report = 1, ANCHORS
    other.close()


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


def test_with_no_agentd_started():
    Stack("idle").close()
'''


def run(tmp_path: Path, *args: str) -> tuple[dict[str, str], str]:
    """The outcome of each test of TESTS (passed, or the message it failed with), and pytest's output."""
    (tmp_path / "conftest.py").write_text(CONFTEST)
    (tmp_path / "test_them.py").write_text(TESTS)
    r = subprocess.run([sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "--rootdir", str(tmp_path),
                        str(tmp_path / "test_them.py"), *args],
                       capture_output=True, text=True, timeout=120, cwd=tmp_path, env={"PYTHONPATH": str(REPO), "PATH": ""})
    return json.loads((tmp_path / "outcomes.json").read_text()), r.stdout + r.stderr


def test_every_agentd_is_verified_and_a_marked_test_passes_only_as_it_declares(tmp_path: Path) -> None:
    got, out = run(tmp_path, "--verify-after-each")
    assert {name for name, o in got.items() if o == "passed"} == {
        "test_honest", "test_tampers_as_declared", "test_any_kind_of_state", "test_a_stack_of_its_own_honest",
        "test_two_stacks_the_named_one_as_declared", "test_agentd_refused", "test_without_a_stack",
        "test_with_no_agentd_started"}, (got, out)
    for name, says in {
        "test_tampers_unmarked": ["verify exited 1, and the test declares 0", "| verify of stack: ['anchors_mismatched']"],
        "test_tampers_with_a_kind_undeclared": ["found state_mismatched:budget, which the test does not declare"],
        "test_only_that_kind_of_state": ["found state_mismatched:budget, which the test does not declare"],
        "test_declares_an_exit_without_kinds": ["declares exit 1 without the kinds of mismatch it expects"],
        "test_declares_a_mismatch_and_verifies": ["verify exited 0, and the test declares 1"],
        "test_declares_another_exit": ["verify exited 1, and the test declares 2"],
        "test_a_broken_chain_unmarked": ["verify exited 1, and the test declares 0", "['broken_at_seq']"],
        "test_a_stack_of_its_own_tampered": ["stack 'own' (/fake/own): agentctl audit verify exited 1"],
        "test_two_stacks_the_other_one_tampered": ["stack 'stack' (/fake/stack): agentctl audit verify exited 1"],
        "test_agentd_gone_unmarked": ["its agentd is not running, so its log cannot be verified"],
        "test_fails_on_its_own": ["its own failure"],
    }.items():
        assert all(s in got[name] for s in says), (name, got[name])
    assert "stack 'other'" not in got["test_two_stacks_the_other_one_tampered"], got  # the named one verified as declared
    assert ("19 stacks started an agentd, in 17 tests: 3 verified with exit 0, 4 as their mark declares, "
            "10 failed verification, 2 not verified") in out, out
    for line in ["exit 1: test_them.py::test_two_stacks_the_named_one_as_declared: stack 'other' (/fake/other)",
                 "not verified: test_them.py::test_agentd_refused: stack 'stack' (/fake/stack): agentd refuses to start, by design",
                 "not verified: test_them.py::test_fails_on_its_own: stack 'stack' (/fake/stack): the test did not pass on its own",
                 "failed: test_them.py::test_agentd_gone_unmarked: stack 'stack' (/fake/stack): its agentd is not running"]:
        assert line in out, (line, out)


def test_without_the_option_nothing_changes(tmp_path: Path) -> None:
    got, out = run(tmp_path)
    assert {name for name, o in got.items() if o != "passed"} == {"test_fails_on_its_own"}, (got, out)
    assert "--verify-after-each" not in out, out
