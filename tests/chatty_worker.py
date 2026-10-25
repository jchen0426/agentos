"""A workflows module that floods the Worker's output as it is imported, then serves the product's
workflows (tests/test_worker_supervision.py): an escape sequence and a bidi override, a line longer
than agentd logs, and far more lines than agentd logs in a second, more than a pipe holds."""

import sys

print("chatty: \x1b[31mred\x1b[0m and ‮ reversed", flush=True)
print("chatty: " + "x" * 10_000, flush=True)
for i in range(2000):
    print(f"chatty: line {i} " + "y" * 200)
sys.stdout.flush()

from agentos_worker.workflows import WORKFLOWS  # noqa: E402,F401
