"""--verify-after-each: every agentd a test starts has its audit log verified, with agentctl audit
verify and its default flags, while the agentd still runs. An honest flow leaves a log that
verifies, so verify must exit 0; anything else fails the test, with verify's output quoted.

Which agentds: every one a Stack starts during the test (harness.WATCH hears each start), whatever
built the stack: a fixture, the test itself, or code it calls (the evaluation's runner). A stack the
test closes is verified as it closes, before its agentd stops; one still up when the test ends is
verified then, before teardown. An agentd that is gone by then, its stack not closed, fails the test
unless its mark says why it cannot be verified. Agentds started by another process (a runner run as
a subprocess) are not seen.

What a test that tampers on purpose expects, or one whose verdict is another for its own reasons, it
declares: @pytest.mark.audit_verify(exit=1, kinds=["anchors_mismatched"]). kinds are the fields of
verify's report (-o json) that may hold what does not match: broken_at_seq, anchors_mismatched,
runs_mismatched, state_mismatched, unread, attempts_unrecorded, runs_unavailable, runs_before_log;
state_mismatched:<kind> allows only state mismatches of that kind. A finding of a kind not declared
fails the test, even with the declared exit. A test with more than one stack marks each by its name
(Stack(..., name="other")): audit_verify(..., stack="other"); a mark without stack= is for the rest.
A test whose agentd refuses to start by design says why: audit_verify(skip="..."). A test that fails
on its own is not verified: its failure says more.

No wait is needed for runs still executing as the test ends: verify takes an attempt whose call
record is yet to come for one in flight while its run runs, and while agentd says it is executing.

Off by default: without the option nothing here is registered, nothing watches, and nothing changes
(conftest.py).
"""
from __future__ import annotations

import json
import textwrap
from dataclasses import dataclass, field

import pytest

from tests import harness

OPTION = "--verify-after-each"
MARK = "audit_verify"
FIELDS = ("broken_at_seq", "anchors_mismatched", "runs_mismatched", "state_mismatched", "unread", "attempts_unrecorded",
          "runs_unavailable", "runs_before_log")  # what a report holds that is not a clean verification


def add_option(parser: pytest.Parser) -> None:
    parser.addoption(OPTION, action="store_true", default=False,
                     help="verify the audit log of every agentd a test starts (agentctl audit verify): an honest test's "
                          f"must exit 0, a marked one's as its {MARK} mark declares")


def configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", f"{MARK}(exit=0, kinds=(), stack=None, skip=None): under {OPTION}, the exit of "
                                       "agentctl audit verify for the test's stacks (or the one named stack), and the "
                                       "report's fields that may hold mismatches; or why a stack cannot be verified")
    if config.getoption(OPTION):
        config.pluginmanager.register(VerifyAfterEach(), "verify-after-each")


def findings(report: dict) -> set[str]:
    """What report holds that is not a clean verification, by field: state mismatches by their kind too."""
    out = {f for f in FIELDS if f not in ("broken_at_seq", "state_mismatched") and report.get(f)}
    if str(report.get("broken_at_seq", "0")) != "0":
        out.add("broken_at_seq")
    out |= {f"state_mismatched:{m.get('kind', '')}" for m in report.get("state_mismatched") or []}
    return out


def undeclared(found: set[str], kinds: set[str]) -> set[str]:
    """What of found the kinds do not allow."""
    return {f for f in found if f not in kinds and not (f.startswith("state_mismatched:") and "state_mismatched" in kinds)}


@dataclass
class Watched:
    """A stack that started an agentd during the test, and what became of it."""
    stack: object
    outcome: str = ""     # "", or "verified", "declared", "failed", "skipped"
    detail: str = ""      # why it failed, or was not verified
    code: int | None = None

    @property
    def label(self) -> str:
        return f"stack {getattr(self.stack, 'name', 'stack')!r} ({getattr(self.stack, 'tmp', '?')})"


@dataclass
class TestWatch:
    item: pytest.Item
    stacks: list[Watched] = field(default_factory=list)
    calling: bool = False  # the test's body runs: a stack it closes is verified then


class VerifyAfterEach:
    """The plugin, registered only under the option."""

    def __init__(self) -> None:
        self.current: TestWatch | None = None
        self.done: list[TestWatch] = []

    # --- what the test starts ------------------------------------------------------------------

    @pytest.hookimpl(wrapper=True)
    def pytest_runtest_protocol(self, item: pytest.Item, nextitem):
        self.current = TestWatch(item)
        harness.WATCH.on_start, harness.WATCH.on_close = self.started, self.closing
        try:
            return (yield)
        finally:
            harness.WATCH.on_start = harness.WATCH.on_close = None
            for w in self.current.stacks:
                if not w.outcome:
                    w.outcome, w.detail = "skipped", "the test's body never ran (its setup failed)"
            if self.current.stacks:
                self.done.append(self.current)
            self.current = None

    def started(self, stack) -> None:
        if self.current is not None and all(w.stack is not stack for w in self.current.stacks):
            self.current.stacks.append(Watched(stack))

    def closing(self, stack) -> None:
        """A stack the test's body closes: verified now, before its agentd stops."""
        w = self.watched(stack)
        if w is not None and not w.outcome and self.current.calling:
            try:
                self.verify(w)
            except Exception as e:  # noqa: BLE001 - the stack closes all the same; the test fails after
                w.outcome, w.detail = "failed", f"verifying it failed: {e!r}"

    def watched(self, stack) -> Watched | None:
        if self.current is None:
            return None
        return next((w for w in self.current.stacks if w.stack is stack), None)

    # --- the verdicts ---------------------------------------------------------------------------

    def expectation(self, stack) -> tuple[int, set[str], str | None]:
        """What the test's marks declare of stack: its exit, the kinds it allows, why it is not verified."""
        assert self.current is not None
        marks = list(self.current.item.iter_markers(MARK))
        name = getattr(stack, "name", "stack")
        mark = next((m for m in marks if m.kwargs.get("stack") == name), None) or \
            next((m for m in marks if "stack" not in m.kwargs), None)
        if mark is None:
            return 0, set(), None
        return mark.kwargs.get("exit", 0), set(mark.kwargs.get("kinds", ())), mark.kwargs.get("skip")

    def verify(self, w: Watched) -> None:
        want, kinds, skip = self.expectation(w.stack)
        if skip:
            w.outcome, w.detail = "skipped", skip
            return
        agentd = getattr(w.stack, "agentd", None)
        if agentd is None or not agentd.alive():
            w.outcome, w.detail = "failed", (f"its agentd is not running, so its log cannot be verified; mark the test "
                                             f"{MARK}(skip=...) if that is by design")
            return
        if want and not kinds:
            w.outcome, w.detail = "failed", f"the test declares exit {want} without the kinds of mismatch it expects"
            return
        r = w.stack.agentctl_run("audit", "verify")
        w.code = r.returncode
        try:
            found = findings(json.loads(r.stdout)) if r.stdout else set()
        except ValueError:
            found = set()
        extra = undeclared(found, kinds)
        if r.returncode == want and not extra:
            w.outcome = "declared" if want else "verified"
            return
        why = (f"agentctl audit verify exited {r.returncode}, and the test declares {want}" if r.returncode != want else
               f"agentctl audit verify found {', '.join(sorted(extra))}, which the test does not declare "
               f"(it declares {', '.join(sorted(kinds)) or 'none'})")
        text = w.stack.audit_verify()  # what a person reads, quoted
        said = textwrap.indent((text.stdout + text.stderr).rstrip() or "(nothing)", "    | ")
        w.outcome, w.detail = "failed", f"{why}:\n{said}"

    @pytest.hookimpl(wrapper=True)
    def pytest_runtest_call(self, item: pytest.Item):
        self.current.calling = True
        try:
            result = yield
        except BaseException:
            for w in self.current.stacks:
                if not w.outcome:
                    w.outcome, w.detail = "skipped", "the test did not pass on its own"
            raise
        finally:
            self.current.calling = False
        for w in self.current.stacks:
            if not w.outcome:
                self.verify(w)
        failed = [w for w in self.current.stacks if w.outcome == "failed"]
        if failed:
            pytest.fail("\n".join(f"{OPTION}: {w.label}: {w.detail}" for w in failed), pytrace=False)
        return result

    def pytest_terminal_summary(self, terminalreporter) -> None:
        tr = terminalreporter
        stacks = [w for t in self.done for w in t.stacks]
        count = {o: sum(w.outcome == o for w in stacks) for o in ("verified", "declared", "failed", "skipped")}
        tr.section(OPTION)
        tr.line(f"{len(stacks)} stacks started an agentd, in {len(self.done)} tests: {count['verified']} verified with "
                f"exit 0, {count['declared']} as their mark declares, {count['failed']} failed verification, "
                f"{count['skipped']} not verified")
        for t in self.done:
            for w in t.stacks:
                if w.outcome == "declared":
                    tr.line(f"  exit {w.code}: {t.item.nodeid}: {w.label}")
        for t in self.done:
            for w in t.stacks:
                if w.outcome in ("skipped", "failed"):
                    tr.line(f"  {'failed' if w.outcome == 'failed' else 'not verified'}: {t.item.nodeid}: {w.label}: "
                            f"{w.detail.splitlines()[0]}")
