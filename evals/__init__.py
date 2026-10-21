"""The evaluation of the first task, the security audit (design doc, section 11): the case set,
the mirrors it is audited in, a runner that audits each case through the runtime in dry-run,
the scoring and the per-model report. `python -m evals --help` lists the commands.

Standard library only, on purpose: the runner reuses the black-box harness (tests/harness.py)
and nothing here may be installed into the environment the sandboxed Worker runs from.
"""
