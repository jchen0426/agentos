from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.harness import Stack


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    setattr(item, "rep_" + outcome.get_result().when, outcome.get_result())


@pytest.fixture
def stack(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Stack]:
    s = Stack(tmp_path)
    try:
        yield s
    finally:
        failed = getattr(request.node, "rep_call", None)
        if failed is not None and failed.failed:
            print(s.logs())
        s.close()
