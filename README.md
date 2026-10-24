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
| `internal/control` | the Control service on the 0700 control socket, which records the uid of each caller |
| `internal/audit` | agentd's hash-chained audit log: each policy decision, side effect and approval, with who made it |
| `internal/explain` | why a run is where it is: its history in Temporal read with the ledger, for `agentctl runs explain` and the run's report |
| `internal/ledger` | SQLite ledgers (goose migrations, sqlc queries) |
| `python/agentos_worker` | the Worker and the `security-audit` workflow |
| `sandbox/linux`, `sandbox/macos` | bubblewrap and sandbox-runtime launchers for the Worker (`run-worker.sh`) and the git tool server (`run-tool.sh`) |
| `tests/` | black-box tests: T1 (Worker and agentd kills), T2 (lookup recovery), the audit log (its chain, its anchors in Temporal's history, a ledger changed by hand) and its export (every hash recomputed, every anchor checked against the exported history), T3 (reconciliation), T4 (an approval across restarts of agentd and Temporal, and its expiry), T5 (the payload binding, edits), T6 (two decisions at once), T7 (concurrent calls against one budget), T8, T10, T12 (`runs explain` names the step and the cause of a tool error, a model refusal, a policy denial, a budget run out, a Worker and agentd killed, the git tool server killed in each of the three steps that run it, the workflow's own code failing (on a call's result, on its report's, or with an exception of its own), a reviewer's edit the patch check refuses, an attempt that ends after the next, and a stuck run; each run's report says the same), T13 (the model APIs map 429s, 5xx, cut-off answers, refusals and broken output alike, and `runs explain` concludes alike), T14 (a doorbell that agentd never delivered), T15 (cancel, then restart), T16 (one sender per effect), dry runs, the replay gate, the Worker's and the git tool server's sandbox probes, mTLS, gateway identity, agentd restart, cancel and its enforcement, model patches whose hunk headers are wrong |
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
stop the dev server and move `~/.agentos/temporal.db*` there too. Keep the dev server's database,
and `bin/agentctl audit verify` reports what the old ledger left there, until Temporal no longer
keeps it: its runs, missing from the new ledger, and the anchor workflows of its chain. To verify,
a fresh ledger looks like one cut short, or rewritten from its first record. Note the old chain's
key before you move the ledger aside (the report's `chain key` line): a reset ledger needs
`--allow-replaced-ledger <old key>` until the old chain's anchor workflows expire (8 days, then the
namespace's retention). A ledger with runs of M1 to M4, from before the audit log began, needs
`--allow-before-log` until those runs leave Temporal and the ledger.

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

`show` prints what agentd itself observed (the patch in full, the files it changes, git's own
check of it, the findings, where the base branch stands on GitHub, what the run has spent), exactly
what GitHub will receive, and the sha256 of that payload, which every decision names. The patch is
shown as agentd applies it: models get the numbers in hunk headers wrong, so agentd counts each
hunk's lines and finds them in the file (where the header says if they are there, else the one
place they are; never a line that differs), and writes the patch anew, in git's format, with the
headers to match and nothing outside its files' headers and hunks (no index lines or headings: the
patch's own claims); `patch_rewritten` lists the headers it rewrote. git checks that patch, and must
make the same files of it.

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

To send the audit to Claude instead of the fake model, store an API key in the login keychain,
then start agentd with `config/dev/agentd-claude.yaml`. Every run then costs money; the budget
caps each run. Copy the key, run the command below, then empty the clipboard with
`pbcopy </dev/null`. The shell's own `printf` hands the key to `security` through a pipe, so it
stays out of your shell history and out of every process's arguments. Don't let `security`
prompt for it (`-w` last): the prompt reads with getpass(3), which keeps the first 128
characters and drops the rest without a word, and OpenAI's keys are longer. A key cut there is
refused (401); agentd warns at start when a key in the keychain is exactly 128 characters.
Run the Worker in its sandbox whenever agentd holds a real key: an unsandboxed Worker runs as
you and could read the key from the keychain with `security`. Under the sandbox the keychain is
out of its reach, which `tests/test_t9_isolation.py` checks.

```bash
printf 'add-generic-password -U -s agentos-anthropic-api-key -a %s -w %s\n' "$USER" "$(pbpaste)" | security -i
```

```bash
bin/agentd --config config/dev/agentd-claude.yaml
```

OpenAI and a local model server work the same way. `config/models.yaml` lists both:
- **OpenAI**, through its Responses API. Its key goes in the item `agentos-openai-api-key`, and
  a project of its own with a monthly budget in OpenAI's console is the outer limit.
- **Ollama**, through Chat Completions (vLLM uses the same adapter). Start it with the context
  window that `context_tokens` names: it cuts longer input without a word, and agentd refuses to
  send what does not fit. Its `efforts` stop at medium, which caps what gpt-oss is sent: at high
  it spends all its output tokens reasoning and writes no answer to most audits. Its models must
  be priced at 0 in `config/prices.yaml`: a Chat Completions server cannot count tokens before a
  request. Start it with cloud models off, too: they run on Ollama's servers. agentd also asks
  Ollama about the model before every request and sends nothing to a cloud model, whatever it is
  called.

A provider that is not on this machine is reached over https only. agentd takes no proxy from
its environment (`HTTPS_PROXY` and the like): its requests go straight to the hosts its
configuration names, and it says at start which proxy variables it ignores. On a network that
reaches the internet only through a proxy, its model and GitHub requests fail: v0.1 has no proxy
setting of its own.

Point a task at either with `model`. Or list them in `cross_provider_fallback`, where they are
asked, each in a request of its own, when the model declines.

What a run must have done to have ended well reads its model task's output: the audit's patch
(`blobs: [patch]`) and its output schema's fields. agentd checks at start that `config/models.yaml`
still gives every field those rules read, and refuses to start, naming the field, if one is gone.

```bash
printf 'add-generic-password -U -s agentos-openai-api-key -a %s -w %s\n' "$USER" "$(pbpaste)" | security -i
```

```bash
OLLAMA_NO_CLOUD=1 OLLAMA_CONTEXT_LENGTH=65536 ollama serve
```

```bash
ollama pull gpt-oss:20b
```

Every policy decision, side effect and approval goes into agentd's audit log, in the ledger, with
who made it: the run, agentd, or the operator whose uid the control socket reported. Each record
holds the sha256 of the one before it; every result of a gateway call names its call's record in
Temporal's history, and that record holds the result's sha256; and agentd pushes the chain's head
to a workflow of its own in Temporal every 30 seconds, and as it stops. To check that the ledger
still holds what was recorded:

```bash
bin/agentctl audit verify
```

It walks the chain, checks each run's results and the pushed heads against the records they name,
for the runs Temporal lists and the audit log records as well as the ledger's, and compares the
ledger's tables, and the bytes of every blob, with what the records say. It exits 0 when all of it
was checked and matches, 1 on any mismatch, 3 when it could not verify at all, and 2 when some runs
could not be checked: Temporal no longer keeps their histories (`--allow-unavailable` accepts
that; keep the namespace's retention at 7 days or more, or agentd warns at start), they were made
before the audit log began (`--allow-before-log` accepts those), or a fault lost a call's record
after its `call.started` (agentd's log says which; once agentd restarts, the attempt counts as cut
short). agentd records each termination before it asks Temporal for it: an execution Temporal says
was terminated with no such record was ended outside agentd, from Temporal's UI or CLI, or by
Temporal itself (a workflow task too large for it). That changes nothing in the ledger, so it is no
mismatch and does not change the exit status; the report lists it, with when, and who its caller
said it was and why, which is that caller's own word. The pushed heads go to
workflows named by the chain's key, its first record's sha256; another chain's workflows in the
namespace are a mismatch, since a chain rewritten from its first record has a new key and leaves
the old one's behind. Each head names the agentd that pushed it, by the sha256 of its Worker's task
queue and its data directory as the file system resolves it: no ledger says that. A chain that
pushed after this one's first push, by Temporal's times, is a second agentd beside this one:
`--allow-chain <key>` (as often as needed) accepts it, which the mismatch suggests when its heads
name another agentd; when they name this one, two ledgers of one agentd were live at once, which
no flag accepts. Any other chain, whatever agentd its heads name (a name that comes from
configuration), is of a ledger replaced: a reset, or a rewrite from its first record, which only
you can tell apart. Its mismatch says when each chain pushed and suggests no flag; `--allow-chain`
does not accept it, `--allow-replaced-ledger <key>` does (the reset above). So does a second
agentd's chain that has not pushed since this one began. A head pushed
after a higher one is a mismatch too: agentd pushes them in order, so the ledger went backwards
in between. The report names the tables it does not check, and why: the outbox, which holds the
runs' doorbells only, and the schema's own two, which agentd checks as it opens the ledger.

agentd opens a ledger only when SQLite finds its file sound (`PRAGMA quick_check`) and its schema
is the one its migrations make, row for row of `sqlite_master`, SQLite's own objects too (only the
tables `ANALYZE` makes are left aside): a trigger or an index of anyone else's changes what
agentd's writes do without a trace, and `PRAGMA writable_schema` writes objects under the names
SQLite keeps for itself, without changing `schema_version`. It compares the schema again on each
connection as it opens, and in each transaction, write or read, once the connection has loaded the
schema afresh. While it is not the migrations', agentd writes nothing to the ledger, and shows
nothing of it: what agentctl shows of runs and approvals comes through a read transaction, which
fails, naming what differs, rather than show what a view put in place of a table makes up. verify
checks the schema first, then has SQLite check every page and every index against its table
(`PRAGMA integrity_check`, which sees an index whose rootpage was swapped, as a comparison of
`sqlite_master` does not); either finding is a mismatch, and verify reads no further.

agentd also keeps in memory where the chain ends: its start's record, the last record it appended,
and the last head it pushed. A log cut, emptied or rewritten under it is a mismatch however whole
what is left, and so is one someone else appended to, however well chained: verify names each
record after the last one this agentd appended, and says when agentd found the log changed. agentd
appends no more records to such a log and pushes no more heads of it, and says so in its log. As it
starts, it checks every head of its chain's anchor workflows of the day and the
day before: each must be a record the ledger holds, and none may come after a higher one, or the
log was cut, or the ledger put back to an older copy, while agentd was down; and the highest head
it pushed under any chain must be this chain's, or the ledger was replaced. The check is bounded
(10 seconds, 50 anchor workflows, its own chain's first, 10,000 events of each) and best-effort.
What it finds, and what it could not read, does not stop it, since anyone who can signal Temporal
can add a head; it logs it, and its start's record names it (`anchor_findings`, at most 20).
verify reads more, but bounded too (2,000 anchor workflows, 20,000 events of each): past that it
says what it did not read, and exits 3, or 1 if anything it read did not match. Its own requests to
GitHub, lookups of a write in doubt and what an approval card shows, each have a record before
they go out (`agentd.read`), with the request's method and path.

What the chain shows is that the records are the ones appended, in their order, and that nothing
changed them since; not who appended them. Whoever can write the ledger while agentd is down can
append records with the right links after the last head it pushed, an operator's approval that
nobody gave, say: they verify, and the next head agentd pushes anchors them like any other (while
agentd runs, its next append finds them, as above). The report says how many
records no pushed head follows yet. Nor does anything anchor the records agentd appended after its
last push before it was killed (`kill -9`): what cuts those while agentd is down, nothing tells. In
v0.1 whoever can write the ledger is the user agentd runs as, whom its threat model does not defend
against.

To share what the ledger and Temporal hold with someone who checks it without agentd:

```bash
bin/agentctl audit export -o audit.json
```

The file (readable by you only) holds each run's histories as agentd reads them from Temporal (its
own JSON of Temporal's events), with which event carries which `_audit` anchor and the bytes the
anchored record's `result_sha256` is of; the heads agentd pushed to the chain's anchor workflows in
Temporal; then the chain's head and every record up to it, with every field its sha256 covers. Its
`hashes` says how to recompute them. `--run <run-id>` exports one run's histories and records: their
hashes check, the chain's links (through other runs' records) do not, and it holds no pushed heads. agentd
takes the records as one snapshot into a file of its own, then sends them: a slow reader holds no
transaction of the ledger open. The snapshot is a file in agentd's data directory, removed once
the export is sent, or when agentd next starts. An export stops at 1 GiB: past that, its head's
`cut` says from which record on it holds nothing, and agentctl says so on stderr and exits 2, as
`audit verify` does when not everything could be checked: the file is whole JSON all the same.

To see why a run is where it is:

```bash
bin/agentctl runs explain <run-id>
```

It reads the run's history with agentd's ledger and says: the verdict (`ok`, `step_failed`,
`not_approved` for a write nobody approved, `failed`, `cancelled`, `terminated`, `waiting`, `stuck`,
`running`, `unknown`), the step (`fetch_diff`, `analyze`, `validate_patch`, `open_pr`,
`post_report`: the task names them by the call's kind and name; any other call shows as its kind and
name, quoted), the kind of cause and the cause, with the audit log's records, the history's events
and the ledger's rows behind it; for a run that waits, the command that lets it go on (none for a
run being cancelled or stuck, which a decision would not move); the trouble along the way (a retried
attempt, a crash, a budget pause, a result agentd did not send); and what the run's model requests
cost, cache hits included. A run ended well only if every step the task requires has a result agentd
sent (`fetch_diff` and `analyze`; `validate_patch` and `open_pr` once the audit proposed a patch)
and its report went out. A result in the history counts only when it names agentd's record of it
(its `_audit` anchor, which agentd reads by its seq); one that does not came from someone else with
access to Temporal's namespace, and the call is judged by agentd's record. So does a failure: it is
agentd's only when agentd's record of that attempt ended in an error of the same type, and one
agentd has no record of at all (nor stopped during) is shown as Temporal's word only, its type
quoted. When agentd cannot read the record a result names, how the call ended is not known, and
neither is the verdict: `unknown` (`record_unreadable`), never `ok`. A write that was only dry-run,
in a run that is not a dry run (an agentd started without GitHub took it up), did not reach GitHub:
`dry_ran`. Who refused a call is the record's word (`refused_by`), not the refusal's text. Crashes
leave no record of their own: one is inferred from what it left (a timed-out workflow task, an
attempt without its record and a new start of agentd, the git tool server's error as agentd's git
client types it), and the cause says so. What the workflow says of itself (its outcome, its failure)
is shown apart, quoted, as its claim; so is what it chose (call names, activity IDs). A run whose
records are longer than agentd reads is explained from the records it read, and `missing` says which
were not (agentd's records of the calls that failed are read past that bound, to one of their own: a
failure whose record is past both leaves the verdict `unknown`); a history longer than agentd reads
makes the verdict `unknown` (`history_unavailable`), and so does a completed run whose history
Temporal no longer has: no result can then be held to agentd's records. Records written for callers
agentd does not know as runs (`unknown-run`) are counted apart, and never take the run's own past
the bound. `-o json` has every field.

The run's report on the commit says the same: agentd writes its outcome by the same rules, from the
run's history and its own records of the calls before the report (without the history, the outcome
is `unknown`, never `ok`: when Temporal answers that it has none, or with an error of its own; while
agentd cannot reach Temporal, the report's call fails before anything of it is recorded, and
Temporal tries it again once it answers), and takes the report from the run's own accepted audit;
the workflow tells it only why it reports (done, or that its own code failed, and the error's type).
The report shows no internal error's text: where agentd could not read something, or a tool's server
failed, it says so and that agentd's log has the details; a model's or a tool's own words are shown
fenced. When the outcome rests on everything it needs but other evidence was past agentd's bounds,
the report says which, and that it does not change the outcome.

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

`uv run pytest --verify-after-each` also runs `agentctl audit verify` on every agentd a test starts, before it stops: every honest flow must leave an audit log that verifies (exit 0), and a test that tampers on purpose declares the exit and the kinds of mismatch it expects (`@pytest.mark.audit_verify(exit=1, kinds=["anchors_mismatched"])`).

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
`go test ./...` covers config, policy, prices and budget reservations (50 at once against one limit), the Claude adapter against a local stand-in for the API, keyring lookups, the loop guard and rate limits, argument canonicalization, the heartbeat interval, the proxy's method allowlist and request rules, patch checks, the GitHub tools against the simulator, and the effect protocol: one claimant sends (T16's compare-and-swap), recovery by class, reconciliation; approvals (the first of concurrent decisions wins, expiry, a payload changed after its approval, edits), the doorbell outbox, the audit log (each record in the transaction of its change, the hash chain, each way of changing it by hand), and `runs explain`'s verdict rules over made-up histories and records, the report's among them.

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
Ollama server above with gpt-oss:20b. Each profile is the product's configuration as it is (a
test holds them equal), and the report shows the effort the ledger says each request was sent.

```bash
AGENTOS_SANDBOX=srt uv run python -m evals run --profile claude --profile openai --profile local --cap-usd 25
uv run python -m evals report --out docs/eval/<name>
```

The first report, for all three profiles, is [`docs/eval/2026-10-audit-v1.md`](docs/eval/2026-10-audit-v1.md)
(2026-10-22).

## License

Apache-2.0. See [LICENSE](LICENSE).
