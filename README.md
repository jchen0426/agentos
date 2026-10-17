# Agent OS

A self-hosted runtime for agent tasks that you can audit: Temporal owns execution, `agentd`
(Go) holds the keys, the ledgers and every way out, and the task's workflow (Python) runs in
a sandbox with no network and no keys. Design: the v0.1 design doc (Claude Doc).

This is milestone M1, the skeleton: the gateway activity with a fake model and stub tools,
the security-audit workflow, the Runtime socket proxy, `agentctl runs`, and the sandbox
prototypes. What M1 found and verified is in [docs/m1-notes.md](docs/m1-notes.md).

## Layout

| Path | What |
| --- | --- |
| `proto/agentos/v1/` | `gateway.proto` (Invoke's `Call`/`Result`), `control.proto` (the Control service) |
| `cmd/agentd`, `cmd/agentctl` | the daemon and its command line |
| `internal/gateway` | `Invoke`: caller check, policy, model calls, the effect protocol |
| `internal/runtimeproxy` | the Worker's only way to Temporal; an allowlist of WorkflowService methods |
| `internal/control` | the Control service on the 0700 control socket |
| `internal/ledger` | SQLite ledgers (goose migrations, sqlc queries) |
| `python/agentos_worker` | the Worker and the `security-audit` workflow |
| `sandbox/linux`, `sandbox/macos` | bubblewrap and sandbox-runtime launchers for the Worker |
| `tests/` | black-box tests: T1 (Worker and agentd kills), T8, T10, the replay gate, the sandbox probe, mTLS, gateway identity, agentd restart, cancel and its enforcement |
| `vm/lima/agentos.yaml` | the Linux VM for the bubblewrap side |

## Requirements

Go 1.26, Python 3.12 with [uv](https://docs.astral.sh/uv/), the Temporal CLI
(`brew install temporal`), git. For the macOS sandbox: Node 22 (`npm ci` in
`sandbox/macos`). For the Linux sandbox: bubblewrap and socat, or the Lima VM
(`brew install lima`, then `make vm`).

## Run it by hand

```bash
uv sync --group dev && make build
temporal server start-dev --db-filename .agentos/temporal.db --search-attribute AgentState=Keyword
```

Without `--db-filename` the dev server keeps everything in memory and loses every run when
it exits.

In another shell, with any git repository at `.agentos/mirror` standing in for `acme/payments`:

```bash
bin/agentd --config config/dev/agentd.yaml
```

```bash
uv run python -m agentos_worker --endpoint 127.0.0.1:7244
```

```bash
bin/agentctl --socket .agentos/control/control.sock runs start security-audit --repo acme/payments --commit <sha> --base main
```

That Worker runs unsandboxed, for development only. Sandboxed, it starts through
`sandbox/macos/run-worker.sh` (macOS) or `sandbox/linux/run-worker.sh` (Linux); see the
headers of those scripts.

## Tests

```bash
make test        # Workers unsandboxed
make test-srt    # macOS, Workers under sandbox-runtime
make test-linux  # in the Lima VM, Workers under bubblewrap
```

`tests/test_temporal_mtls.py` also needs the full Temporal server: `make temporal-server`.
`go test ./...` covers config, policy, argument canonicalization, the heartbeat interval, and the proxy's method allowlist and request rules.

After a workflow change that is meant to be compatible, the replay gate
(`tests/test_replay.py`) must still pass. To add samples to it, run
`AGENTOS_WRITE_HISTORIES=1 uv run pytest tests/test_t10_versioning.py`.

Generated code is committed: `make proto` (protobuf, Go and Python) and `make sqlc`.

## License

Apache-2.0. See [LICENSE](LICENSE).
