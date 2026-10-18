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
loop limits ([docs/m2-notes.md](docs/m2-notes.md)). M3a adds the effect protocol for external
writes: each one is rendered and recorded before it is claimed and sent, recovered by its class
after a crash, and settled by a person when nobody can tell what happened; plus the GitHub
write tools, tool argument schemas, dry runs and a lock that keeps one agentd per data
directory ([docs/m3-notes.md](docs/m3-notes.md)). M3b reads repositories through a read-only
git MCP server that agentd runs once per call in a sandbox: the call's mirror read-only, a
private temporary directory, no network. agentd itself runs no git on a mirror: it resolves a
run's commit and reads the files a patch touches through the same server.

## Layout

| Path | What |
| --- | --- |
| `proto/agentos/v1/` | `gateway.proto` (Invoke's `Call`/`Result`), `control.proto` (the Control service) |
| `cmd/agentd`, `cmd/agentctl` | the daemon and its command line |
| `internal/gateway` | `Invoke`: caller check, policy, model calls, the effect protocol (claims, recovery by class, reconciliation) |
| `internal/tools`, `internal/patch` | the tools and the effect class and argument schema each declares; checks and applies a model's patch |
| `internal/gitmcp`, `cmd/agentos-mcp-git` | the read-only git MCP server (`show_diff`, `apply_check`, `resolve_commit`, `read_files`) and agentd's client, which runs it once per call in a sandbox |
| `internal/githubtool` | `github.open_draft_pr` and `github.comment_commit` (go-github) |
| `internal/githubsim`, `cmd/githubsim` | the simulated GitHub the tests write to |
| `internal/budget`, `internal/pricing` | per-request reservations and settlements; the price table (`config/prices.yaml`) |
| `internal/models`, `internal/secrets` | model routing (`config/models.yaml`), the Claude and fake adapters; keys from the OS keyring |
| `config/prompts`, `config/schemas` | each model task's prompt and output schema |
| `internal/runtimeproxy` | the Worker's only way to Temporal; an allowlist of WorkflowService methods |
| `internal/control` | the Control service on the 0700 control socket |
| `internal/ledger` | SQLite ledgers (goose migrations, sqlc queries) |
| `python/agentos_worker` | the Worker and the `security-audit` workflow |
| `sandbox/linux`, `sandbox/macos` | bubblewrap and sandbox-runtime launchers for the Worker (`run-worker.sh`) and the git tool server (`run-tool.sh`) |
| `tests/` | black-box tests: T1 (Worker and agentd kills), T2 (lookup recovery), T3 (reconciliation), T7 (concurrent calls against one budget), T8, T10, T15 (cancel, then restart), T16 (one sender per effect), dry runs, the replay gate, the Worker's and the git tool server's sandbox probes, mTLS, gateway identity, agentd restart, cancel and its enforcement |
| `vm/lima/agentos.yaml` | the Linux VM for the bubblewrap side |

## Requirements

Go 1.26, Python 3.12 with [uv](https://docs.astral.sh/uv/), the Temporal CLI
(`brew install temporal`), git 2.41 or later (the git tool server reads attributes from the
empty tree, `GIT_ATTR_SOURCE`). For the macOS sandbox: Node 22 (`npm ci` in
`sandbox/macos`). For the Linux sandbox: bubblewrap 0.8 or later and socat, or the Lima VM
(`brew install lima`, then `make vm`). Mirrors must belong to the user agentd runs as: git
runs without the global configuration, so `safe.directory` does not apply.

## Run it by hand

```bash
uv sync --group dev && make build
temporal server start-dev --db-filename .agentos/temporal.db --search-attribute AgentState=Keyword
```

Without `--db-filename` the dev server keeps everything in memory and loses every run when
it exits.

In another shell, with any git repository at `.agentos/mirror` standing in for `acme/payments`
(agentd reads it through the git tool server, which the dev config runs per call under
sandbox-runtime: run `npm ci` in `sandbox/macos` once):

```bash
bin/agentd --config config/dev/agentd.yaml
```

Without a sandbox for the git tool server, agentd refuses to start; `--insecure-dev` runs the
server unsandboxed, for development only, and says so at every start.

```bash
uv run python -m agentos_worker --endpoint 127.0.0.1:7244
```

```bash
bin/agentctl --socket .agentos/control/control.sock runs start security-audit --repo acme/payments --commit <sha> --base main
```

The dev configs leave GitHub out, so the GitHub tools only dry-run: they render and record the
PR and the comment, and send nothing. `runs start ... --dry-run` does the same for one run on
an agentd that has GitHub. To write to GitHub, give agentd a `github` section with a
fine-grained token in the keychain, and map the logical repository to its GitHub name:

```yaml
repos:
  acme/payments: {mirror: ../../.agentos/mirror, github: <owner>/<test-repo>}
github: {token: "keyring:agentos-github-token"}
```

Until approvals arrive (M4), do that only with the fake model: with the dev policy nothing asks
before a PR is opened, and a real model can be steered by text in the diff.

A write whose outcome nobody knows (agentd stopped after sending it, and the tool cannot look it
up) waits in `WAITING_RECONCILE`. Check the outside world, then say what you found:

```bash
bin/agentctl --socket .agentos/control/control.sock reconcile list
```

```bash
bin/agentctl --socket .agentos/control/control.sock reconcile resolve <effect-id> --not-executed
```

`--executed --result '{...}'` records what the write produced instead, and `--executed --lookup`
lets agentd look it up for tools that can.

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

T2 once against a real test repository (design doc, section 11), with the fake model. The
repository holds `scripts/make-audit-target.sh`'s fixture, has Actions turned off and no
secrets, and agentd gets a fine-grained token for it alone (Contents and Pull requests: read
and write) from its own login keychain item, `agentos-github-test-token`. The Worker runs in its
sandbox, since agentd holds a real token; the run leaves one draft PR and one commit comment
there.

```bash
AGENTOS_GITHUB=live AGENTOS_GITHUB_REPO=<owner>/<name> AGENTOS_SANDBOX=srt uv run pytest tests/test_t2_live.py -s
```
`go test ./...` covers config, policy, prices and budget reservations (50 at once against one limit), the Claude adapter against a local stand-in for the API, keyring lookups, the loop guard and rate limits, argument canonicalization, the heartbeat interval, the proxy's method allowlist and request rules, patch checks, the GitHub tools against the simulator, and the effect protocol: one claimant sends (T16's compare-and-swap), recovery by class, reconciliation.

After a workflow change that is meant to be compatible, the replay gate
(`tests/test_replay.py`) must still pass. To add samples to it, run
`AGENTOS_WRITE_HISTORIES=1 uv run pytest tests/test_t10_versioning.py`.

Generated code is committed: `make proto` (protobuf, Go and Python) and `make sqlc`.

## License

Apache-2.0. See [LICENSE](LICENSE).
