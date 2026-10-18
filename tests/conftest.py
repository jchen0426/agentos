from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.harness import GITHUB_LIVE_REPO, Stack


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    setattr(item, "rep_" + outcome.get_result().when, outcome.get_result())


def _stack(request: pytest.FixtureRequest, s: Stack) -> Iterator[Stack]:
    try:
        yield s
    finally:
        failed = getattr(request.node, "rep_call", None)
        if failed is not None and failed.failed:
            print(s.logs())
        s.close()


@pytest.fixture
def stack(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Stack]:
    yield from _stack(request, Stack(tmp_path))


@pytest.fixture
def live_stack(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Stack]:
    """A stack that writes to the real test repository (AGENTOS_GITHUB=live): only for the tests
    that ask for it by name."""
    if not GITHUB_LIVE_REPO:
        pytest.skip("needs AGENTOS_GITHUB=live and AGENTOS_GITHUB_REPO")
    yield from _stack(request, Stack(tmp_path, live_repo=GITHUB_LIVE_REPO))
