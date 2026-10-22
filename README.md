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
run's commit and reads the files a patch touches through the same server. M4 adds approvals:
a write the policy marks waits for a person, who sees what agentd itself observed and exactly
what will be sent, and approves it, edits it or rejects it; the approval holds for that payload
alone, to the byte, and for a limited time. Decisions, reconciliations and budget extensions
wake their run through a doorbell outbox, which survives a crash ([docs/m4-notes.md](docs/m4-notes.md)).

## Layout

| Path | What |
| --- | --- |
| `proto/agentos/v1/` | `gateway.proto` (Invoke's `Call`/`Result`), `control.proto` (the Control service) |
| `cmd/agentd`, `cmd/agentctl` | the daemon and its command line |
| `internal/gateway` | `Invoke`: caller check, policy, model calls, the effect protocol (claims, recovery by class, reconciliation), approvals (decisions, expiry, the payload binding) |
| `internal/outbox` | the doorbells that wake runs waiting for a person or for budget, delivered until Temporal has them |
| `internal/tools`, `internal/patch` | the tools and the effect class and argument schema each declares; checks and applies a model's patch |
| `internal/gitmcp`, `cmd/agentos-mcp-git` | the read-only git MCP server (`show_diff`, `apply_check`, `resolve_commit`, `read_files`) and agentd's client, which runs it once per call in a sandbox |
| `internal/githubtool` | `github.open_draft_pr` and `github.comment_commit` (go-github) |
| `internal/githubsim`, `cmd/githubsim` | the simulated GitHub the tests write to, which holds the mirror's commits and trees |
| `internal/modelsim`, `cmd/modelsim` | the simulated model providers (Anthropic, OpenAI's Responses API, Chat Completions) that T13 injects faults through |
| `internal/budget`, `internal/pricing` | per-request reservations and settlements; the price table (`config/prices.yaml`) |
| `internal/models`, `internal/secrets` | model routing (`config/models.yaml`), the Claude, OpenAI-compatible and fake adapters; keys from the OS keyring |
| `config/prompts`, `config/schemas` | each model task's prompt and output schema |
| `internal/runtimeproxy` | the Worker's only way to Temporal; an allowlist of WorkflowService methods |
| `internal/control` | the Control service on the 0700 control socket |
| `internal/ledger` | SQLite ledgers (goose migrations, sqlc queries) |
| `python/agentos_worker` | the Worker and the `security-audit` workflow |
| `sandbox/linux`, `sandbox/macos` | bubblewrap and sandbox-runtime launchers for the Worker (`run-worker.sh`) and the git tool server (`run-tool.sh`) |
| `tests/` | black-box tests: T1 (Worker and agentd kills), T2 (lookup recovery), T3 (reconciliation), T4 (an approval across restarts of agentd and Temporal, and its expiry), T5 (the payload binding, edits), T6 (two decisions at once), T7 (concurrent calls against one budget), T8, T10, T14 (a doorbell that agentd never delivered), T15 (cancel, then restart), T16 (one sender per effect), dry runs, the replay gate, the Worker's and the git tool server's sandbox probes, mTLS, gateway identity, agentd restart, cancel and its enforcement, model patches whose hunk headers are wrong |
| `evals/`, `docs/eval/` | the first task's evaluation: the case set, its mirrors, the runner, the scoring and the per-model report (`python -m evals`); the reports |
| `vm/lima/agentos.yaml` | the Linux VM for the bubblewrap side |

## Requirements

Go 1.26, Python 3.12 with [uv](https://docs.astral.sh/uv/), the Temporal CLI
(`brew install temporal`), git 2.41 or later (the git tool server reads attributes from the
empty tree, `GIT_ATTR_SOURCE`). For the macOS sandbox: Node 22 (`npm ci` in
`sandbox/macos`). For the Linux sandbox: bubblewrap 0.8 or later and socat, or the Lima VM
(`brew install lima`, then `make vm`). Mirrors must belong to the user agentd runs as: git
runs without the global configuration, so `safe.directory` does not apply. agentd refuses a
mirror that holds its data directory, either socket's directory, the Temporal client certificate
or key, or another repo's mirror: on Linux the git tool server's sandbox mounts all of the call's
mirror.

## Run it by hand

```bash
uv sync --group dev && make build
mkdir -p -m 700 ~/.agentos
temporal server start-dev --db-filename ~/.agentos/temporal.db --search-attribute AgentState=Keyword
```

Without `--db-filename` the dev server keeps everything in memory and loses every run when
it exits. The dev configs keep agentd's data in `~/.agentos` too, out of the repository; move a
checkout's own `.agentos` from before out of it as well (the Worker launchers refuse a code
directory that holds one). All checkouts share `~/.agentos`: run one dev stack at a time, or give
another a config with a `data_dir` of its own, also under the home directory. agentd refuses a
ledger another branch's agentd has migrated: one with migrations this agentd does not have, or
another file under one of its migration numbers. A migration edited after the ledger applied it
counts as another file, so leave applied migrations as they are. agentd also refuses a new
migration it cannot tell from its own: one an agentd built before this check applied, such as
your own branch's before it merged main (and such an agentd checks nothing). Run the agentd
that migrated the ledger, or stop agentd, start a fresh development ledger and keep the old
one, with its WAL, aside where no Worker can read it:

```bash
d=~/.agentos/old-$(date +%Y%m%d-%H%M%S) && mkdir "$d" && mv ~/.agentos/ledger.db* "$d"/
```

Runs recorded in the old ledger cannot continue. If any wrote to GitHub, first settle what
`bin/agentctl reconcile list` shows, with the agentd that migrated the ledger. For a clean slate,
stop the dev server and move `~/.agentos/temporal.db*` there too.

In another shell, with any git repository at `~/.agentos/mirror` standing in for `acme/payments`
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
bin/agentctl runs start security-audit --repo acme/payments --commit <sha> --base main
```

The dev configs leave GitHub out, so the GitHub tools only dry-run: they render and record the
PR and the comment, and send nothing. `runs start ... --dry-run` does the same for one run on
an agentd that has GitHub. To write to GitHub, give agentd a `github` section with a
fine-grained token in the keychain, and map the logical repository to its GitHub name:

```yaml
repos:
  acme/payments: {mirror: ~/.agentos/mirror, github: <owner>/<test-repo>}
github: {token: "keyring:agentos-github-token"}
```

The dev policy then has a person approve each pull request before it is opened: text in a diff
can steer a real model, so someone looks at the patch first. A run that asks waits in
`WAITING_APPROVAL`:

```bash
bin/agentctl approvals list
```

```bash
bin/agentctl approvals show <approval-id>
```

`show` prints what agentd itself observed (the patch in full, the files it changes, `git apply
--check`, the findings, where the base branch stands on GitHub, what the run has spent), exactly
what GitHub will receive, and the sha256 of that payload, which every decision names. The patch is
shown as agentd applies it: models get the numbers in hunk headers wrong, so agentd counts each
hunk's lines and finds them in the file (where the header says if they are there, else the one
place they are; never a line that differs), and writes the patch anew, in git's format, with the
headers to match and nothing outside its files' headers and hunks; `patch_rewritten` lists the
headers it rewrote. git checks that patch, and must make the same files of it.

```bash
bin/agentctl approvals approve <approval-id> --payload-sha256 <sha256>
```

`reject ... --reason '...'` stops the pull request; the reason goes into the run's report. `edit
... --patch-file fix.diff` sends your patch back to the run, which checks it again and asks for
approval of the result; its hunk headers need not count right, as above. The first decision counts: a second, from you or anyone, is told who
decided what, and when. An approval nobody decides in time (`approval_ttl` in the policy, 24 hours
by default) expires as a rejection. Decisions are recorded under your user name; `--as` names
someone else.

A write whose outcome nobody knows (agentd stopped after sending it, and the tool cannot look it
up) waits in `WAITING_RECONCILE`. Check the outside world, then say what you found:

```bash
bin/agentctl reconcile list
```

```bash
bin/agentctl reconcile resolve <effect-id> --not-executed
```

`--not-executed` has the run send the write again under its original idempotency key, unless the
run has ended or the write's approval has run out by then. For a write its tool can look up,
agentd takes `--not-executed` only once no request of it can still take effect, and says when
that is. `--executed --result '{...}'` records
what the write produced instead, and `--executed --lookup` lets agentd look it up for tools that
can.

To send the audit to Claude instead of the fake model, store an API key in the login keychain
(the command asks for it, so it stays out of your shell history), then start agentd with
`config/dev/agentd-claude.yaml`. Every run then costs money; the budget caps each run.
Run the Worker in its sandbox whenever agentd holds a real key: an unsandboxed Worker runs as
you and could read the key from the keychain with `security`. Under the sandbox the keychain is
out of its reach, which `tests/test_t9_isolation.py` checks.

```bash
security add-generic-password -U -s agentos-anthropic-api-key -a "$USER" -w
```

```bash
bin/agentd --config config/dev/agentd-claude.yaml
```

OpenAI and a local model server work the same way. `config/models.yaml` lists both:
- **OpenAI**, through its Responses API. Its key goes in the item `agentos-openai-api-key`, and
  a project of its own with a monthly budget in OpenAI's console is the outer limit.
- **Ollama**, through Chat Completions (vLLM uses the same adapter). Start it with the context
  window that `context_tokens` names: it cuts longer input without a word, and agentd refuses to
  send what does not fit. Its models must be priced at 0 in `config/prices.yaml`: a Chat
  Completions server cannot count tokens before a request. Start it with cloud models off, too:
  they run on Ollama's servers. agentd also asks Ollama about the model before every request and
  sends nothing to a cloud model, whatever it is called.

A provider that is not on this machine is reached over https only. agentd takes no proxy from
its environment (`HTTPS_PROXY` and the like): its requests go straight to the hosts its
configuration names, and it says at start which proxy variables it ignores. On a network that
reaches the internet only through a proxy, its model and GitHub requests fail: v0.1 has no proxy
setting of its own.

Point a task at either with `model`. Or list them in `cross_provider_fallback`, where they are
asked, each in a request of its own, when the model declines.

```bash
security add-generic-password -U -s agentos-openai-api-key -a "$USER" -w
```

```bash
OLLAMA_NO_CLOUD=1 OLLAMA_CONTEXT_LENGTH=65536 ollama serve
```

```bash
ollama pull gpt-oss:20b
```

A run that runs out of budget waits in `PAUSED_BUDGET`. Raise its limit and it carries on:

```bash
bin/agentctl budget extend <run-id> --usd 2.50
```

That Worker runs unsandboxed, for development only. Sandboxed, it starts through
`sandbox/macos/run-worker.sh` (macOS) or `sandbox/linux/run-worker.sh` (Linux), with `python`
as its code directory; see the headers of those scripts. A sandboxed Worker can read all of its
code directory and Python environment, so a launcher refuses either if it holds dev data
(agentd's ledger, a `.agentos` directory, Temporal's database), a FIFO, or on Linux a Unix socket
(a read-only mount does not stop `connect()`). On macOS the sandbox can also read everything
outside the home directory and the shared temporary directories (`/tmp`, `/var/tmp`,
`/var/folders`): keep
agentd's data and the mirrors under the home directory, or list them in
`AGENTOS_SANDBOX_DENY_READ`, a JSON array of paths.

```bash
sandbox/macos/run-worker.sh 7244 "$PWD/.venv" "$PWD/python" -- "$PWD/.venv/bin/python" -m agentos_worker --endpoint 127.0.0.1:7244 --http-connect-proxy env
```

## Tests

```bash
make test        # Workers unsandboxed
make test-srt    # macOS, Workers under sandbox-runtime
make test-linux  # in the Lima VM, Workers under bubblewrap
```

`tests/test_temporal_mtls.py` also needs the full Temporal server: `make temporal-server`.

T1 on a real model instead of the fake one: `AGENTOS_MODEL=claude`, `openai` (gpt-6.1-sol) or
`local` (gpt-oss:20b on the Ollama server above, which is free). Claude and OpenAI cost money,
a few cents a run; the Worker runs under its sandbox because agentd holds a real key. Check the
key, or the Ollama server, first: `-run LiveCountTokens` for Claude, `LiveOpenAI` (a token
count, at most a fraction of a cent) or `LiveOllama` (its version, the model, and the window it
runs with).

```bash
AGENTOS_LIVE=1 go test ./internal/models -run LiveOpenAI -v
```

```bash
AGENTOS_MODEL=openai AGENTOS_SANDBOX=srt uv run pytest tests/test_t1_worker_crash.py tests/test_t1_agentd_crash.py
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
`go test ./...` covers config, policy, prices and budget reservations (50 at once against one limit), the Claude adapter against a local stand-in for the API, keyring lookups, the loop guard and rate limits, argument canonicalization, the heartbeat interval, the proxy's method allowlist and request rules, patch checks, the GitHub tools against the simulator, and the effect protocol: one claimant sends (T16's compare-and-swap), recovery by class, reconciliation; approvals (the first of concurrent decisions wins, expiry, a payload changed after its approval, edits) and the doorbell outbox.

After a workflow change that is meant to be compatible, the replay gate
(`tests/test_replay.py`) must still pass. To add samples to it, run
`AGENTOS_WRITE_HISTORIES=1 uv run pytest tests/test_t10_versioning.py`.

Generated code is committed: `make proto` (protobuf, Go and Python) and `make sqlc`.

## Evaluation

The first task's evaluation (design doc, section 11) audits a set of commits through the same
runtime, in dry-run, under each model, and writes a per-model report: detection, false
positives, injection false passes, patch applicability, cost, latency, cache hits, refusals and
broken structured output, with confidence intervals from a bootstrap over cases. `evals/` uses
the standard library only and reuses the black-box harness to bring up Temporal, agentd and the
Worker; nothing of it is installed where the Worker runs.

`evals/cases.json` holds commit IDs, labels (file, lines, CWE) and injection instructions,
never the projects' code. `python -m evals mirrors` fetches each commit and its parent from the
project into a cache outside the repository (`~/.cache/agentos-eval`, or
`AGENTOS_EVAL_CACHE`) and makes the commits we make (a fix applied in reverse, a case with
injected text) with a fixed author and date, so their IDs are the ones recorded.

```bash
uv run python -m evals mirrors                          # fetch, make, check every case
uv run python -m evals table --out docs/eval/cases.md   # the case table, for spot checks
```

A run needs `make build`, and, like any run with a real model, the Worker in its sandbox. The
state directory (`<cache>/state/default`, or `--state`) keeps the journal, and each profile's
Temporal database and agentd ledger; a runner on it picks up where the last one stopped. A paid
profile needs a cap on what the state directory's runs spend, every runner on it together (a
new state directory is a new evaluation, with a cap of its own): a run starts only while what
the ledgers say the runs have spent, plus the budgets of the paid runs in flight, plus its own
budget (`--budget-usd`, $1.50 by default: room for the gateway's worst-case reservation) stay
within it. A run whose model call did not end with the model's answer (one cancelled while it
waited for budget, an approval or a reconciliation, or took longer than its profile allows; a
policy that denied the call; a provider that kept failing) is void: it is in no rate, the
report counts it by its reason, and `--retry-void` runs it again. A void first run stops the
runner; after a void run the runs go one at a time until one is answered, and three void runs in
a row stop the runner (not counting runs it stopped itself for the cap, runs picked up from an
earlier runner, nor void runs run again that cost nothing). On macOS the
state directory and the cache must be under the home directory (the default) or a temporary
directory, which the sandboxes deny. The profiles are `config/models.yaml`'s providers,
one at a time: `claude` and `openai` need their keys in the keychain (above), `local` needs the
Ollama server above with gpt-oss:20b.

```bash
AGENTOS_SANDBOX=srt uv run python -m evals run --profile claude --profile openai --profile local --cap-usd 50
uv run python -m evals report --out docs/eval/<name>
```

## License

Apache-2.0. See [LICENSE](LICENSE).
