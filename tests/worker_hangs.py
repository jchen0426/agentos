"""A workflows module that never finishes importing: a Worker that never says it is ready
(tests/test_worker_supervision.py)."""

import time

time.sleep(3600)

WORKFLOWS: list = []
