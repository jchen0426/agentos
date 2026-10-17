# Agent OS

A self-hosted runtime for agent tasks that you can audit: Temporal owns execution, `agentd`
(Go) holds the keys, the ledgers and every way out, and the task's workflow (Python) runs in
a sandbox with no network and no keys. Design: the v0.1 design doc (Claude Doc).

Milestone M1, the skeleton, is done: the gateway activity with a fake model and stub tools,
the security-audit workflow, the Runtime socket proxy, `agentctl runs`, and the sandbox
prototypes ([docs/m1-notes.md](docs/m1-notes.md)). M2a adds the budget: a versioned price
table, a reservation for every model request before it is sent and a settlement after, and
`agentctl budget extend`. M2b adds the Claude adapter: keys from the OS keyring, structured
output checked against the task's schema, refusals and server-side fallback, rate limits and
loop limits ([docs/m2-notes.md](docs/m2-notes.md)).

## Layout

| Path | What |
| --- | --- |
| `proto/agentos/v1/` | `gateway.proto` (Invoke's `Call`/`Result`), `control.proto` (the Control service) |
| `cmd/agentd`, `cmd/agentctl` | the daemon and its command line |
| `internal/gateway` | `Invoke`: caller check, policy, model calls, the effect protocol |
| `internal/budget`, `internal/pricing` | per-request reservations and settlements; the price table (`config/prices.yaml`) |
| `internal/models`, `internal/secrets` | model routing (`config/models.yaml`), the Claude and fake adapters; keys from the OS keyring |
| `config/prompts`, `config/schemas` | each model task's prompt and output schema |
| `internal/runtimeproxy` | the Worker's only way to Temporal; an allowlist of WorkflowService methods |
| `internal/control` | the Control service on the 0700 control socket |
| `internal/ledger` | SQLite ledgers (goose migrations, sqlc queries) |
| `python/agentos_worker` | the Worker and the `security-audit` workflow |
| `sandbox/linux`, `sandbox/macos` | bubblewrap and sandbox-runtime launchers for the Worker |
| `tests/` | black-box tests: T1 (Worker and agentd kills), T7 (concurrent calls against one budget), T8, T10, the replay gate, the sandbox probe, mTLS, gateway identity, agentd restart, cancel and its enforcement |
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

To send the audit to Claude instead of the fake model, store an API key in the login keychain
(the command asks for it, so it stays out of your shell history), then start agentd with
`config/dev/agentd-claude.yaml`. Every run then costs money; the budget caps each run.
Run the Worker in its sandbox whenever agentd holds a real key: an unsandboxed Worker runs as
you and could read the key from the keychain with `security`. Under the sandbox the keychain is
out of its reach, which `tests/test_sandbox.py` checks.

```bash
security add-generic-password -U -s agentos-anthropic-api-key -a "$USER" -w
```

```bash
bin/agentd --config config/dev/agentd-claude.yaml
```

A run that runs out of budget waits in `PAUSED_BUDGET`. Raise its limit and it carries on:

```bash
bin/agentctl --socket .agentos/control/control.sock budget extend <run-id> --usd 2.50
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

T1 on Claude instead of the fake model (it costs money, a few cents a run; the Worker runs
under its sandbox because agentd holds a real key), and a free check of the key:

```bash
AGENTOS_MODEL=claude AGENTOS_SANDBOX=srt uv run pytest tests/test_t1_worker_crash.py tests/test_t1_agentd_crash.py
```

```bash
AGENTOS_LIVE=1 go test ./internal/models -run Live
```
`go test ./...` covers config, policy, prices and budget reservations (50 at once against one limit), the Claude adapter against a local stand-in for the API, keyring lookups, the loop guard and rate limits, argument canonicalization, the heartbeat interval, and the proxy's method allowlist and request rules.

After a workflow change that is meant to be compatible, the replay gate
(`tests/test_replay.py`) must still pass. To add samples to it, run
`AGENTOS_WRITE_HISTORIES=1 uv run pytest tests/test_t10_versioning.py`.

Generated code is committed: `make proto` (protobuf, Go and Python) and `make sqlc`.

## License

Apache-2.0. See [LICENSE](LICENSE).
