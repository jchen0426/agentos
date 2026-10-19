# M1 记录：骨架、两项验证和发现的问题

2026-10-16。M1 的验收测试 T1（假模型，两个 kill 点）、T8、T10 全部自动化并通过，在三种环境各跑一遍：macOS 上 Worker 不进沙箱、macOS 上 Worker 进 sandbox-runtime、Lima 里的 Ubuntu 24.04 虚拟机上 Worker 进 bubblewrap。两项验证的结论已写回设计文档第 8 节（只追加了三句）。

| 测试 | 内容 | macOS 不进沙箱 | macOS + srt | Linux + bwrap |
| --- | --- | --- | --- | --- |
| T1 ×2 | 模型调用中 kill -9 Worker；调用返回后、工作流任务提交前 kill -9 Worker | 通过 | 通过 | 通过 |
| T1 ×2（补做） | 结果落账后 kill -9 agentd：账本回答重试；供应商已回答、未落账时 kill -9 agentd：再问一次，留下记录 | 通过 | 通过 | 通过 |
| T8 ×2 | 心跳超时后旧尝试才返回：写操作一次；模型调用不重复、不重复计费 | 通过 | 通过 | 通过 |
| T10 | 挂起期间发布新代码；CI 重放拦下不兼容改动 | 通过 | 通过 | 通过 |
| 重放门禁 | `tests/histories/` 里的历史在当前代码下重放 | 通过 | 通过 | 通过 |
| 沙箱探针 | 见验证 1 | 跳过 | 通过 | 通过 |
| mTLS | 见验证 2 | 通过 | 通过 | 通过 |
| 身份核对 | 新执行复用已结束任务的 workflow ID，网关拒绝（子工作流这条路已在代理上拒绝） | 通过 | 通过 | 通过 |
| agentd 重启 | Worker 不重启，重启后的 agentd 照常服务 | 通过 | 通过 | 通过 |
| 取消 ×2 | 等待审批时取消：CANCELLED，不发报告；工作流无视取消：网关拒绝它之后的调用，到期被 Terminate | 通过 | 通过 | 通过 |

另有 Go 单元测试（配置、默认拒绝的策略、参数规范化、心跳间隔、代理放行名单对 WorkflowService 全部方法逐个核对、代理对命名空间、任务队列和每种命令的检查），以及用开了 race detector 的 agentd 跑 T1、T8，没有数据竞争。身份核对和 T8 的模型用例做过变异检查：去掉 run ID 核对、去掉模型调用合并后，对应测试都会失败。

## 验证 1：沙箱里的 Python SDK 能否经转接连上 Temporal

两个平台都能，而且只能经 agentd 连。`tests/test_sandbox.py` 在沙箱里跑一个探针，结果如下。

| 探针 | macOS（sandbox-runtime 0.0.79） | Linux（bubblewrap 0.9，Ubuntu 24.04） |
| --- | --- | --- |
| SDK 经 Runtime 入口连 Temporal | 连上（经代理到 agentd 的回环端口） | 连上（经 socat 到 Runtime socket） |
| 经 Runtime 入口发信号、终止、启动工作流、拉/完成活动任务 | agentd 拒绝，PERMISSION_DENIED | 同左 |
| SDK 直连 Temporal 端口 | 代理拒绝 CONNECT，403 | 连接被拒（空网络命名空间） |
| 直接 TCP 连 Temporal 端口 | Seatbelt 拦下 | 连接被拒 |
| 外网 | Seatbelt 拦下 | 网络不可达 |
| Control socket | Seatbelt 拦下 | 不存在（没挂载） |
| 家目录里的文件、`~/.ssh` | Seatbelt 拦下 | 不存在 |
| agentd 的账本文件 | Seatbelt 拦下 | 不存在 |
| 调用方的环境变量 | 看不到（只剩 srt 加的代理变量） | 看不到（`--clearenv`） |

要点：

- sandbox-runtime 的 `allowedDomains` 支持 `IP:端口` 条目，写 `127.0.0.1:<端口>` 就只放行这一个端口；IP 字面量条目不受它“拒绝回环地址”的检查影响（README 明确说明）。不开 `allowLocalBinding`。
- sandbox-runtime 给沙箱的代理地址带每个会话的凭据（`http://<user>:<token>@localhost:<端口>`），Worker 必须从 `HTTPS_PROXY` 解析出主机、端口和 basic auth 再交给 `HttpConnectProxyConfig`，只去掉 `http://` 前缀会连不上。
- Linux 上 Ubuntu 23.10 起默认 `kernel.apparmor_restrict_unprivileged_userns=1`，没有 profile 的 bwrap 报 `loopback: Failed RTM_NEWADDR: Operation not permitted`。Ubuntu 不带 bwrap 的 profile，要照它给 flatpak 的写法加一个（`vm/lima/agentos.yaml` 里有）。这一条团队虚拟机部署也要做。
- Linux 上要挂载 Runtime socket 所在的目录，而不是 socket 文件本身：agentd 重启时会重建 socket，挂进去的文件会一直指向旧的那个，Worker 从此连不上（代码评审发现，已在虚拟机里复现）。所以这个目录里只能放 Runtime socket，启动脚本会检查。
- Unix socket 路径不能超过 104 字节（macOS），agentd 现在启动时检查并给出明确报错。

## 验证 2：Temporal 开发服务器能否只接受 agentd 证书的 mTLS 连接

开发服务器不能；同版本的完整服务器可以。

- `temporal server start-dev`（CLI 1.9.1，内嵌 Server 1.32.0）没有任何 TLS 选项，也不读服务器配置文件。
- 同版本的完整 `temporal-server`（官方发布包，`scripts/fetch-temporal-server.sh` 下载并校验）加配置文件可以：SQLite 存储，frontend 和 internode 都 `requireClientAuth: true`，`clientCaFiles` 只放一个专用 CA。`tests/test_temporal_mtls.py` 的结果：明文连接、TLS 不带客户端证书、另一个 CA 签发的同名（CN=agentd）证书全部被拒；agentd 的证书能连上，整条流程（agentd 的网关 worker、Control、Runtime 代理的后端连接都带证书）跑到 SUCCEEDED，Worker 不持有任何证书。
- 限制：官方二进制只能按 CA 过滤，不能按证书主体过滤（要按主体授权得自己编译服务器，接自定义 ClaimMapper）。所以“只认 agentd”靠一个只给 agentd 和服务器自身签证书的 CA；以后的 Web UI 也要一张这个 CA 签的证书。
- 完整服务器不会自动建 `default` 命名空间，搜索属性 `AgentState` 注册后约 10 秒（命名空间缓存刷新）才能在启动工作流时使用，部署脚本要等它生效。
- 对设计的含义：本机如果要这道第二防线，就不能用 `start-dev`，要改跑完整服务器（SQLite 文件存储即可）。第 4 节组件表里本机的写法要不要改，留给你定。

## 和设计文档草图不同的地方

第 7 节说字段在 M1 定稿，下面是定稿时改的地方：

1. `Call.args_json` 和 `Result.value_json` 从 `bytes` 改成 `string`。用 bytes 时 Temporal Web UI 里看到的是 base64，看不出调用了什么。
2. `runs` 表加 `run_id`，网关同时核对 workflow ID 和 run ID。只核对 workflow ID 会被冒用：Worker 本来就要调用 `RespondWorkflowTaskCompleted`，可以在里面带 `StartChildWorkflowExecution` 命令，用一个已结束任务的 workflow ID 起子工作流（Temporal 默认允许复用），网关就会把它当成那个任务。
3. 每次网关调用的活动 ID 改成 `<序号>:<调用名>`（如 `3:git.apply_check`），见下面问题 2。这是对第 7 节工作流代码唯一的改动。
4. Control 服务加了 `GetRun` 和 `ListRuns`，给 `agentctl runs get/list` 用。
5. 副作用账本的最小版本提前到 M1：T8 要求“副作用账本没有重复记录”，而账本原定在 M3。现在是：先写 PENDING 意图，再执行，再记结果；同一 effect_id 的并发尝试合并成一次执行；PENDING 但没有在途执行的（agentd 中途重启）返回 needs_reconcile，按类别恢复留给 M3。
6. 策略要求审批的调用在 M1 一律返回 needs_approval（审批表在 M4），所以默认策略下任务会停在 WAITING_APPROVAL；测试用的策略不要求审批（和 M3 的做法一样）。
7. Worker 加了 `--workflows` 参数，部署新版工作流时用；T10 用它换上新版本。

## 发现的问题

1. **按方法过滤挡不住命令级的越权**（第 4 节原来的“所以它发不了信号，也终止不了任务”说过头了）。Worker 必须能调用 `RespondWorkflowTaskCompleted`，而这个请求里可以带 `SignalExternalWorkflowExecution`、`RequestCancelExternalWorkflowExecution`、`StartChildWorkflowExecution`，以及排到任意任务队列的活动。M1 补做里已经修了，见下面“M1 补做”。
2. **所有调用都是同一个活动类型 Invoke，Temporal 的重放检查看不出调用顺序变了**。重放只比较活动类型和活动 ID，不比较输入。实测：默认活动 ID（"1"、"2"……）下，把 `git.apply_check` 和 `github.open_draft_pr` 对调，重放不报错，记录的结果被交给了错误的调用。改成按调用命名的活动 ID 后，同样的改动报 `Activity id of scheduled event '3:git.apply_check' does not match activity id of activity command '3:github.open_draft_pr'`。T10 的“不兼容的改动在 CI 的重放测试里就被拦下”靠的就是这一点。还剩一个盲区：只改参数、不改调用顺序的改动重放查不出来。
3. **重放只能拦下改动点之前已经有历史的任务**。挂起在改动点之前的任务，即使改动没有用 `patched()` 包起来，恢复后也照样能跑。所以 CI 的重放样本要覆盖走完全程的历史，`tests/histories/` 里两份都要留。
4. **代理的 gRPC 参数要和 Temporal 对齐**。grpc-go 服务端默认只容许客户端每 5 分钟一次 keepalive ping，Python SDK 每 30 秒一次，会被回 GOAWAY（too_many_pings）断开；默认 4 MiB 的消息上限也会截断大的历史。代理已按 Temporal frontend 的设置放宽（10 秒、128 MiB）。
5. T8 里旧尝试被拒的证据来自 Go SDK 的日志（`Task processing failed with error ... activity already timed out`），历史里看到的是第 2 次尝试的完成和第 1 次的心跳超时。

## 代码评审后修复的问题

用独立的评审 agent 查了一遍正确性，确认属实后修了这些：

1. **Worker 能让 agentd 崩溃**：心跳超时由工作流（不可信）设定，设成 2ns 时 `time.NewTicker(timeout/3)` 直接 panic，整个 agentd 退出，别的任务的副作用全卡在 PENDING。现在心跳间隔最少 100ms。
2. Linux 沙箱挂载 socket 文件的问题（见验证 1）。
3. agentd 收到 SIGTERM 时不等在途的执行就关账本，正常重启也会留下 PENDING 的副作用。现在先等活动结束、再等执行写完结果（各最多 30 秒）。
4. StartRun：工作流已经启动后，写 run ID 不再跟着客户端的取消一起中断；启动成功后不再因为查询状态失败而报错（报错会诱使人重试，重试就是第二个任务、第二个 PR）；agentd 启动时补全上次中断留下的空 run ID。
5. Temporal 里已经没有的执行（过了保留期，或内存版开发服务器重启过）不再让 `runs list` 整个失败，显示为 UNKNOWN。
6. 工具出错一律记成 FAILED 并且永不重试是错的：只有确定请求没发出去的才算 FAILED，其余记 UNKNOWN 并返回 needs_reconcile，和第 6 节一致。
7. `AgentState` 是 Worker 写的，agentd 只认四个运行中的状态，Worker 不能让一个还在跑的任务显示成 SUCCEEDED。
8. 策略列了 repos 时，不带 repo 参数的工具调用也拒绝（原来只在带了 repo 时才检查）。
9. macOS 启动脚本的 srt 配置改用 node 生成 JSON，路径里的引号不会破坏或篡改配置；端口做了校验。

## M1 补做（2026-10-16）

设计文档第 12 节给 M1 加的三项补做都已完成，代码也跟上了修订后的文档。

1. **Runtime 命令级过滤**（`internal/runtimeproxy/rules.go`）。代理在转发前先读出请求，按方法解码后检查：命名空间只能是配置的那个；拉取工作流任务只能拉 Worker 自己的任务队列（粘性队列后来改为一律拒绝，见下节）；`RespondWorkflowTaskCompleted` 里的命令只放行这个工作流本来就会发的几种：往 gateway 队列排 Invoke 活动（不许 eager 执行）、定时器、`core_patch` 标记、取消自己的活动、结束自己，以及只改 `AgentState` 的搜索属性。给别的工作流发信号、取消别的工作流、起子工作流、continue-as-new、Nexus 操作和以后新增的命令一律拒绝，返回 PERMISSION_DENIED，请求不会到 Temporal。实测发现一处必须放行的：`workflow.patched()` 会顺带写 Temporal 自带的 `TemporalChangeVersion` 搜索属性，没放行时 T10 失败。被拒后 Worker 不会崩，只是那个工作流任务超时重试。
2. **取消拦截**。`CancelRun` 先在账本记下取消（新迁移 `00002_cancel.sql` 加了 `runs.cancelled_at`），再请 Temporal 取消。从记下那一刻起，网关拒绝这个任务的所有调用。到 `cancel_grace`（默认 2 分钟）还没结束的，agentd 直接 Terminate；agentd 重启后会重新挂上这个期限。取消后结束的任务一律显示为 CANCELLED。
3. **杀 agentd 的 T1**（`tests/test_t1_agentd_crash.py`）。结果落账后杀掉 agentd：Temporal 的重试由账本直接回答，假供应商自己的记录显示只被问过一次。供应商已回答、结果还没落账时杀掉：重试会再问一次供应商，账本里留着没回答的第 1 次尝试，M2 的预算要按全额计这一次。

同时跟上文档的代码：门铃改成计数器；核对和预算的等待加 1 小时上限；`report_ref` 存在工作流实例上，后面一步失败也会发出已有的发现；报告没发出去时任务失败；README 的启动命令加了 `--db-filename`；`config/policy.yaml` 的 `open_draft_pr` 加了 `deny_paths`；`ResolveReconciliationRequest` 加了 `result_json`。这些改动不改变已有历史的命令，`tests/histories/` 的重放门禁照常通过。

## 推送后的安全审查（2026-10-17）

f22fc37 推送后，自动安全审查报了三处问题，核实都成立，已修复：

1. **Runtime 代理的粘性队列**（`internal/runtimeproxy/rules.go`）。原规则对粘性队列只看 normal name，normal name 为空就放行，队列名随便填。实测 Temporal 1.32 并不把粘性队列和它的 normal name 绑在一起，代理分辨不出一个粘性队列属于谁，只收紧 normal name 不够。现在沙箱里的 Worker 关掉粘性执行（`max_cached_workflows=0`），代理拒绝一切粘性队列：粘性拉取、`RespondWorkflowTaskCompleted` 里的 `sticky_attributes`、带粘性队列名的 `ShutdownWorker`；`ResetStickyTaskQueue` 也移出了白名单。代价是每个工作流任务都要从完整历史重放，M1 的历史很短，影响不大。
2. **工具调用不受任务自身范围约束**（`internal/policy/policy.go`）。策略只检查 repo 在不在 `repos` 列表里，所以一个对 acme/payments 启动的任务，可以对列表里的其他 repo、同一 repo 的其他 commit 或别的 base 分支发起写操作。现在任务注册时声明 `Scope`（security-audit 把工具参数 `repo`、`commit`、`base_branch` 对应到启动时解析出的 `repo`、`commit`、`base`）。网关按账本里这个任务的参数算出范围，工具调用带了这些参数、值却不同，就拒绝；没带的由网关补上任务自己的值，工具不会退回到它自己的默认值（比如默认分支）。这一条是第二轮审查指出的：只查“带了且不同”是 fail-open。
3. **终端转义注入**（`internal/control/server.go`、`agentctl`）。任务结果和失败信息由 Worker 写入，原来原样打到终端：转义序列能改写屏幕，换行能伪造一行输出（比如一行假的 `state SUCCEEDED`）。新的 `internal/safetext` 做两件事：Control 服务把失败信息里不可打印的字符写成 `\x1b`、`\n` 这样的文本；结果 JSON 里的 DEL、C1 控制字符和格式字符（如双向覆盖）换成 `\u` 转义，解码后的值不变。agentctl 的文本输出逐字段转义，JSON 输出同样处理；agentd 的日志也经过同样的过滤，因为里面有 Worker 自己挑的活动 ID 和调用名。

修复后三种环境的测试全部通过：macOS 不加沙箱 15 个通过、1 个跳过（沙箱测试要在沙箱模式下跑），macOS + srt 16 个，Linux + bwrap 16 个。真实 Worker 的调用没有一次被代理拒绝，正常任务的工具调用也没有一次被范围检查拒绝。

## Worker 沙箱里不能再建文件（2026-10-18）

M3b 给工具服务器补探针时发现，bwrap 自己建的根目录是可写的 tmpfs，最大可到内存的一半（见 `docs/m3-notes.md`）。Worker 的 `sandbox/linux/run-worker.sh` 也一样，而且有四处：根目录、`/dev`（含 `/dev/shm`）、`/tmp`、`/run`。tmpfs 占的内存不记在任何进程名下，被攻破的 Worker 把它写满时，OOM killer 不会挑中它，先被杀的会是 agentd、Temporal 这些进程。

改之前先看了 Worker 到底写什么：把四处都换成虚拟机磁盘上的目录跑整套测试，34 次沙箱启动后哪里都没留下东西；再在沙箱里用 strace 跑一遍，记下建、删、改名文件和 memfd、共享内存的系统调用，Worker 的近 4 万次调用全是只读的 openat。所以现在 Worker 在哪儿都不能建文件：

- 根目录和 `/dev` 只读（`--remount-ro`，放在参数最后，前面的挂载要先在上面建好挂载点）。
- `/tmp` 和 `/run` 不再挂 tmpfs，只是根目录里的目录，跟着只读；`/run` 里放 Runtime socket 的挂载点。起初给 `/tmp` 留了 64 MiB（`--size`），两个评审代理都实测发现 `--size` 只限文件内容：inode 数还是内核默认的内存页数的一半，空文件和 xattr 照样占不记在进程名下的内核内存（实测 6 万个空文件占了 51 MiB 不可回收的 slab，`/tmp` 却显示用了 0 字节），合起来能到内存的约 1/8。bwrap 设不了 inode 数，Worker 又用不着 `/tmp`，就整个去掉了。以后真要临时文件，`tempfile` 会明确报 “No usable temporary directory found”。
- 不许再建用户命名空间（`--disable-userns`）。补探针时发现：没有这一条，沙箱里的 Python 能自己建用户和挂载命名空间，挂一个任意大小的 tmpfs（实测挂了 2 GiB、写进 256 MiB），上面几条等于白改。
- 没有可写的 `/dev/shm`，POSIX 信号量和共享内存（`multiprocessing` 的锁、队列）都会报 EROFS。Worker 不用。真要用，得先有限住内存的 cgroup（`memory.max`）：给 `/dev/shm` 挂一个限了大小的 tmpfs，同样管不住 inode 和 xattr，探针的可写挂载检查也要跟着放开。

`tests/test_sandbox.py` 加了一个探针，经启动脚本在沙箱里试：在根目录、`/dev/shm`、`/run`、`/tmp` 建文件都得到 EROFS（只读挂载，不是权限不够或目录不存在）；`/proc/self/mountinfo` 里除了 `/proc`、伪终端和设备节点，没有一个挂载可写；建用户命名空间得到 ENOSPC（`--disable-userns` 设的上限）；IPC 命名空间是沙箱自己的。沙箱外先确认宿主机的 `/dev/shm` 和 `/tmp` 能写、能建用户命名空间，写的东西也没出现在宿主机上。改之前它失败：四处都写进去了，根目录、`/dev`、`/tmp`、`/run` 四个 tmpfs 都是可写挂载，用户命名空间也建得出来。逐条改坏启动脚本，它都在对应的检查上失败，包括把代码目录、Python 环境或 Runtime socket 的目录改成可写挂载，多挂一个 tmpfs，不再单独建 IPC 命名空间。

没有照工具服务器加 `ulimit`：

- `-v` 只管单个进程。Worker 可以 fork 出很多个小进程，每个都比 agentd 小，内存用光时 OOM killer 照样先杀 agentd（评审实测过）。Worker 的地址空间还随 CPU 数增长：glibc 给 SDK 的线程各分一个 malloc arena，4 核时 SDK 运行时约 0.4 GB，设 `MALLOC_ARENA_MAX=1` 只有 0.09 GB。整套测试里 Worker 的地址空间最高 0.78 GB，常驻内存最高 74 MB。
- `-f` 也管 Worker 的 stdout/stderr。它们常常重定向到普通的日志文件，日志一过上限（或者追加到本来就超过上限的日志），Worker 就被 SIGXFSZ 杀掉。反过来，这个输出是 Worker 唯一能无限写的东西，能写满日志所在的磁盘，该交给管道或 journald，由读的一方限住，不要直接指向普通文件。

## Worker 看得到的目录里不能有 agentd 的数据（2026-10-19）

PR #11 的对抗评审指出过：开发配置把数据目录放在仓库里（`.agentos`），启动脚本把代码目录整个交给 Worker，测试给的代码目录又是仓库根。这次在两个平台上复现了。agentd 用 `config/dev/agentd.yaml` 起，Temporal 开发服务器照 README 把数据库放在 `.agentos/temporal.db`，再像测试那样以仓库根为代码目录起沙箱，里面的探针：

- Linux（bwrap）：用 SQLite 打开了账本（12 张表），在 `ledger.db-wal` 和 `temporal.db` 里都找到了运维者刚起的 run ID，镜像也读得到。它还连上了 Control socket（只读挂载挡不住 `connect()`）：`ListRuns` 列出了那个 run，`ExtendBudget` 把它的预算从 1000 改成 1001 微美元，运维者随后看到的也是 1001。换成 `agentd-claude.yaml`，同一个 Control 接口就能给花真钱的任务加预算、起任务、取消任务、了结核对。虚拟机里没有存 key 的 Secret Service，这个配置在 Linux 上起不来，所以只用假模型演示；两份配置的数据目录是同一行。
- macOS（srt）：账本、Temporal 数据库和镜像同样读得到。两份配置都试了，`agentd-claude.yaml` 只起 agentd，不起任务。Control socket 连不上：srt 不放行任何按路径的 Unix socket 连接，`socket()` 和 `socketpair()` 能用，`connect()` 得到 EPERM。

改动：

- 两个 Worker 启动脚本启动前检查 Python 环境目录和代码目录，里面有开发数据就拒绝：`ledger.db*`（agentd 的账本，连同它的 WAL 和改名的备份；M3a 加锁文件之前的旧数据目录也有它）、`.agentos` 目录、`temporal.db*`（Temporal 开发服务器的数据库）。FIFO 也拒绝：Linux 上只读挂载挡不住往里写，macOS 上沙箱虽然写不进去，却能从里面读。Linux 上还拒绝 Unix socket，只读挂载挡不住 `connect()`，别的 agentd 的 Control socket、ssh-agent、Docker 的都是出路。macOS 不查 socket：srt 一个都不让连，查了反而会误伤 git fsmonitor 放在 `.git` 里的 socket。用 `find -H`，给的目录本身是符号链接时查它指向的地方，bwrap 挂的也是那里。扫描时有目录被删（比如同一个 worktree 里正在 `npm ci`），`find` 会报错，所以最多试三次，还不行才拒绝。扫一个 worktree，虚拟机里约 0.08 秒，Mac 上 0.03 秒；从主 checkout 跑测试时，里面还有别的 worktree，约 2.6 万个文件，虚拟机经 virtiofs 每次约 0.7 秒。
- Linux 启动脚本原先检查 Runtime socket 的目录时，`find` 不带 `-H`：目录本身是符号链接时一项都看不到，bwrap 却把它指向的整个目录挂到 `/run/agentos`，账本和 Control socket 一起进了沙箱（评审实测）。现在也带 `-H`。
- macOS 启动脚本把每个拒读路径连同它的真实路径一起交给 srt，总是拒读 `~/.agentos` 的真实路径，家目录也不只看 `$HOME`，用户数据库里的那个一样拒读（`run-tool.sh` 本来就这样）：`HOME` 指向别处或是相对路径时，原先真正的家目录读得到（评审实测）。Seatbelt 按真实路径匹配，srt 遇到指向别处的符号链接时保留原来的写法，所以原先写成符号链接的拒读条目不起作用，`~/.agentos` 是指向家目录外的符号链接时，里面的数据也读得到（评审实测）。
- 开发配置不再写 `data_dir`，用默认的 `~/.agentos`，也就是 agentctl 默认找 Control socket 的地方；镜像放在 `~/.agentos/mirror`，README 让 Temporal 的数据库也放进去。macOS 沙箱读不到家目录，Linux 沙箱不挂家目录。为此 agentd 配置里的路径可以用 `~/` 开头。别的 `~` 写法（单独一个 `~`、`~user/`）报错，不再静默地当成配置旁边的目录；取不到家目录、或家目录不是绝对路径，也报错，否则数据目录会相对于 agentd 的当前目录，socket 却在配置旁边。
- 不写 `data_dir` 是为了旧的 agentd。这次改动之前编译的 agentd 不认识 `~/`，评审用它实测过：配置里写 `data_dir: ~/.agentos`，它会建出 `config/dev/~/.agentos`，账本又回到仓库里，而 `.gitignore` 原先只管根目录的 `.agentos`。不写的话，旧 agentd 也把数据放进 `~/.agentos`，只是找不到镜像。`.gitignore` 现在不论在哪一层，都不提交 `.agentos/`、`ledger.db*`、`temporal.db*`（仓库是公开的）。拉了新代码还是要先 `make build`。
- README：先建 `~/.agentos`（`mkdir -p -m 700`，Temporal CLI 不会自己建）；agentctl 命令不再带 `--socket`；沙箱 Worker 的代码目录写成 `python`，里面只有任务代码；旧 checkout 自己的 `.agentos` 要挪出仓库；所有 checkout 共用 `~/.agentos`，同一时间只跑一套开发环境。

测试：

- `tests/test_sandbox.py::test_worker_launcher_refuses_agentd_data_in_reach` 起一个真实的 stack，把数据目录放进交给 Worker 的目录，分三种情形：代码目录里有数据目录，Control socket 也在里面；代码目录是指向它的符号链接；Python 环境目录里只有账本，Control socket 在别处，Linux 上必须靠账本认出来。macOS 上这个目录跟家目录一样在拒读列表里，只有交给 Worker 的目录能把它重新放开，原来的漏洞就是这样。改之前三种都失败：探针读到了账本，Linux 上还连上了 socket。只有 macOS 的符号链接那一种例外：srt 不跟随符号链接形式的代码目录，探针跑了起来，但没读到。
- `test_worker_launcher_refuses_dev_data_by_name`：代码目录里只有 `.agentos/mirror`，或者只有 `temporal.db`。
- `test_worker_launcher_refuses_a_way_out_in_the_code_dir`：代码目录里有一个在监听的 socket（只在 Linux 上跑），或者一个有人在读的 FIFO。改之前探针连上了 socket，也写进了 FIFO（macOS 上探针跑了起来，但写不进去）。
- `test_worker_launcher_refuses_a_directory_it_cannot_look_through`：代码目录里有个只能进、不能列的目录（0111），账本在里面。`find` 列不出来，Worker 却能按名字打开它，所以查不完就得拒绝。
- `test_worker_launcher_refuses_a_runtime_dir_that_holds_more`（只在 Linux 上跑）：Runtime socket 的目录经符号链接给出，里面还有账本。改之前探针在 `/run/agentos` 读到了它。
- `test_worker_sandbox_denies_data_behind_a_symlink`（只在 macOS 上跑）：`~/.agentos` 是指向别处的符号链接，或者拒读条目写成符号链接，后面的账本都读不到。改之前两种都读到了。
- `test_worker_sandbox_denies_the_home_directory_whatever_home_says`（只在 macOS 上跑）：`HOME` 指向别处时，真正家目录里的文件也读不到。改之前读到了。
- 逐条改坏启动脚本，每一处都有对应的用例失败：不查 Python 环境、socket、FIFO、账本、`.agentos` 或 `temporal.db`，扫描不带 `-H`，查不完也放行，Runtime 目录的检查不带 `-H`；macOS 上拒读条目不解析真实路径，不拒读 `~/.agentos`，或只拒读 `$HOME`。
- `internal/config`：两份开发配置解析出来的数据目录、socket 和镜像都不在仓库里，都在家目录里；`~/` 路径会展开；`~`、`~user/`、取不到的家目录和相对的家目录都报错。
- 照新 README 在两个平台上各走了一遍：数据在 `~/.agentos`，沙箱 Worker 的代码目录是 `python`，假模型的任务跑到 SUCCEEDED。再把整个仓库当代码目录，沙箱里读不到账本和 Temporal 数据库，也连不上 Control socket：macOS 得到 EPERM，Linux 上这些路径根本没挂进来。

推送前，两个评审代理（安全、对抗）在各自的副本里做了实验，都没找到高危问题。上面 Runtime 目录、FIFO、macOS 符号链接、旧 agentd、`~` 写法、扫描重试这几条都是它们找出来的。它们还发现，macOS 上原来的测试数据放在 `/tmp`，本来就读得到，测不出真正的漏洞；现在测试先把它拒读。改完再请对抗评审复查了一轮：上面那些都修好了；它又找出 macOS 能读 FIFO、`HOME` 不可靠、“查不完就拒绝”没有用例这三处，也都补上了。

## agentd 拒绝别的 agentd 迁移过的账本（2026-10-19）

PR #20 的对抗评审读过 goose v3.29 的源码（`internal/gooseutil/resolve.go` 的 `UpVersions`）：goose 只看二进制里有文件的迁移版本，账本里应用过、二进制里却没有的版本，它一概不管。所有 checkout 的开发环境共用 `~/.agentos` 以后，开发中很容易碰上：

- 旧分支的 agentd 照常在新分支迁移过的账本上启动，跑在一个不是为它写的表结构上。用 main 编出来的 agentd 实测过：账本到了版本 8，它照样往下走，一直走到连 Temporal。
- 两个分支各加一个同号的迁移（比如都是 00008）：后启动的 agentd 以为 8 已经应用过，它自己的 00008 永远不会执行。
- 两个分支把编号错开时更隐蔽：账本里有 A 分支的 8，B 分支的 agentd 有 9 没有 8。B 的最高版本比账本高，只比最高版本挡不住；goose 还会把 B 的 9 叠在 A 的 8 上（测试里改之前就是这样）。

改动（`internal/ledger/migrate.go`）：

- `Open` 在 goose 迁移之前，在一个 IMMEDIATE 事务里核对账本，下面任何一条成立就拒绝启动（`ErrMigrationMismatch`），账本里什么都不写：
  1. 账本应用过 agentd 没有的版本。不只比最高版本，哪一个不认识都拒绝，错开编号的情形靠的就是这条。
  2. 某个版本账本记的文件和 agentd 自己的不同（比文件内容的 sha256）：别的分支的同号迁移，或者应用之后又改过的迁移文件。
  3. 8 和以后的版本应用过，却没有有效的文件记录：是不记文件的 agentd（这次改动之前编的）或者 goose 命令行应用的，分不清是不是自己的那个文件。
  4. 账本没应用 agentd 的某个版本，却应用了比它高的版本。goose 本来也拒绝，但它的报错不说怎么办。
- 文件记录在新表 `migration_files`（版本、goose 版本行的 id、文件名、sha256），goose 自己只记版本号。记录由 goose 写版本行的那一步顺带写（给 goose 一个包了它自己 SQLite store 的 store，`Insert` 里多写一行），和迁移本身、版本行在同一个事务里提交：迁移失败或者中途被杀，记录和迁移一起回滚。写成 `-- +goose NO TRANSACTION` 的迁移不在事务里跑，这时 `Insert` 自己开一个事务，版本行和记录还是一起写。这张表要在任何迁移执行之前就在，所以由核对的事务建，不放在迁移里。
- 记录只在 goose 那个版本的行还是写记录时的那一行时才算数（记下行的 id；goose 的版本表是 AUTOINCREMENT，id 不会重用）。goose 命令行 down 一个版本，删的是版本行，记录留着；之后不管谁再应用这个版本，都是新的一行，旧记录不再替它作保。1 到 7 例外：这几个版本的记录不管是不是当前那一行的，都拿来比文件，没有记录时才当作本机的文件；否则改过的 00007 用 goose 命令行 down 再 up 一次，旧记录就被无视了（安全评审实测）。
- 00001 到 00007 在这次改动之前就有，那时的账本都没有记录：没有记录时当作本机的文件，记下来，以后照样核对。8 和以后的必须有当前那一行的记录。
- goose 的版本表按 SQLite 找表的规则找：名字不分大小写，视图也算。原来只认小写的表，账本的版本表换个大小写或者换成视图，核对就看不到版本，goose 却照样看得到（安全评审实测）。
- 错误信息写出账本到了哪个版本、agentd 的迁移到哪个版本、每个出问题的版本是怎么回事（不认识的版本写出账本记的文件名；同号时写出两边的文件名和 sha256 的前 12 位），再说怎么办：用迁移它的那个 agentd，或者把 `ledger.db`（连同 `ledger.db-wal`、`ledger.db-shm`，如果有）挪进数据目录里的一个新目录，开一个新的开发账本；旧账本里记的任务不能接着跑。只挪 `ledger.db` 不行：留下的 WAL 会被当成新账本的。要挪进数据目录，是因为 macOS 的 Worker 沙箱按黑名单拦读，顺手挪到 `/tmp` 的话，沙箱里读得到旧账本里的模型请求和补丁（两个评审都提了）。
- README 开发那段写了换新账本的命令（挪进 `~/.agentos/old-<时间>/`），先停 agentd；账本里还有等待核对的副作用时，先用迁移它的 agentd 处理完（`agentctl reconcile list`）；也写了已经应用过的迁移不要改，和第 3 种拒绝是怎么来的。`internal/ledger/ledger.go` 里迁移的 `go:embed` 旁边也写了不要改。Go 写的迁移直接报错（记录只认 SQL 文件），不再在读文件时报一个看不懂的错。

我定的几处：

- 不认识的版本一律拒绝，不只拒绝比自己最高版本还高的（上面错开编号的情形）。
- 比文件内容，不只比文件名：开发中改了已经应用过的迁移，账本的表结构就和文件对不上了，一样拒绝。代价是应用过的迁移连注释也不能改。Flyway 默认也是这样核对的。
- 记录跟着 goose 的版本行写，绑在那一行上。最初的做法是核对时先把这次要应用的版本都记下，迁移失败或中途被杀就留下“记了却没应用”的记录；之后要是有不记文件的 agentd 应用了自己的同号迁移，这条旧记录就替它作了保（两个评审都实测过）。第二版改成跟着版本行写，复审又发现 goose 命令行 down 掉的版本会留下记录，照样能替之后应用的同号迁移作保，所以再绑上行 id。
- 没有“自动换新账本”的开关：账本里有预算和副作用的记录，丢不丢要人决定；换新账本只是挪走文件，旧的留作备份。

测试（`internal/ledger/ledger_test.go`，用 `fstest.MapFS` 模拟别的分支的 agentd，用单独的 goose 模拟不记文件的 agentd 和 goose 命令行）：

- 新分支迁移过的账本：拒绝，信息里有账本和 agentd 各自的最高版本、账本记的文件名和换新账本的办法；拒绝之后，迁移它的 agentd 照样打得开。
- 错开编号：拒绝，两个版本号的位置没写反，而且自己的迁移没有先执行。
- 同号不同文件：拒绝，写出两边的文件名；迁移它的 agentd 照样打得开。
- 应用之后改过的迁移：拒绝。
- 改动之前的账本（只有 goose 的记录，1 到 7）：接受并记下；之后换一个改过 00007 的 agentd：拒绝。
- goose 命令行 down 再 up 了 00007（还是本机的文件）：照样当作本机的；之后改过 00007 的 agentd 照样被拒绝。
- 改过 00007 的 agentd 迁移过账本，之后 goose 命令行 down 再 up 了这个改过的 00007：本机拒绝。
- 不记文件的 agentd 在 1 到 7 上应用了自己的 8：拒绝。这里的 8 是写死的，main 以后有了真的 00008，这个测试照样守着 7 这条界线（对抗评审指出，原来按“下一个版本号”算，有了 00008 以后界线挪到 8 也测不出来）。
- 别的 agentd 没应用成自己的 8：不妨碍本机应用自己的 8，之后再打开也没问题。
- 本机没应用成自己的 8，之后不记文件的 agentd 应用了它自己的 8：本机再启动时拒绝。
- goose 命令行 down 掉本机的 8，再应用别的分支的 8：本机拒绝。
- NO TRANSACTION 的迁移，记录写不进去：版本行也没写。
- goose 的版本表名字是大写、或者是视图：照样拒绝。
- 账本没应用 8、却应用了 9：拒绝。改之前这是 goose 自己的报错。
- 最初那 8 个测试，改之前（只加了测试要用的入口 `open(ctx, path, fsys)`）有 7 个失败；剩下那个本来就过，防的是把别人没应用成的迁移算到本机头上。评审后为修复加的测试（本机失败后别人应用、大写或视图的版本表、goose 命令行 down 后再应用、NO TRANSACTION、改过的 00007 重新应用）在修复之前都失败。写死的 8、两个版本号的检查、重新应用 00007 这三个是守护测试，修复之前就过，各自守着下面的一处改坏。逐条改坏核对，共 24 处（不查不认识的版本、迁移之后才查、只比文件名、1 到 7 的界线差一、`Insert` 不写记录、记录不绑行 id、行 id 写错、不在自己的事务里写、7 以内不看旧记录、找版本表分大小写或不认视图、错误信息里两个版本号写反、把 goose 的 0 版本当成应用过的版本，等等），只有一处改坏后测试照过：7 以内第一次记录时写的行 id。7 以内比文件不看行 id，这处改了也没有影响。
- 用 main 和这个分支各编一个 agentd，在同一个临时账本上加一条版本 8：main 的照常往下走到连 Temporal，新的启动时就拒绝，日志里是上面的信息。
- 这台 Mac 上 `~/.agentos/ledger.db` 是 10-16 的 M1 账本，只到版本 1：新的 agentd 会把 1 当作本机文件记下，再照常迁移到 7，不会误拒（对抗评审拿它的副本实测过）。

推送前，两个评审代理（安全、对抗）在各自的副本里做了实验，都没找到高危问题。上面“记录跟着 goose 的版本行写”“绑上行 id”“NO TRANSACTION 也在一个事务里”“版本表按 SQLite 的规则找”“挪进数据目录”、写死的 8 和两个版本号的测试，都是它们找出来的。它们还指出这些说法不对，已经改掉：“记了却没应用的记录不算数”（只在没有不记文件的 agentd 插进来时才成立）；“同名文件改过”的提示（也可能是别的分支起了同样的名字，现在只说是另一个同名文件）；下面风险里“第二个 agentd 会卡在锁上”（锁是非阻塞的，第二个直接拒绝启动）。最后一轮复审：安全评审发现绑上行 id 以后 7 以内的旧记录被直接无视（已修，见上）；对抗评审指出这个分支中间版本建的旧表（没有 `goose_id` 列）会让启动报 SQL 错，那些版本从没推送过，没有处理。

## 任务的第一个调用等 run ID 记下（2026-10-19）

StartRun 先在账本里记下任务（run ID 留空），再请 Temporal 启动工作流，拿到 run ID 才记上。这中间，已经在拉任务的 Worker 可能把任务的第一个调用发到了网关。原来网关一看 run ID 还没记上就拒绝这次尝试（"run registration in progress"），Temporal 1 秒后重试，这个调用白白用掉工作流 5 次尝试里的一次。Worker 是热的时候这很常见：CI（run 39178263143）里第一个调用比 run ID 早到 33 ms；按 Temporal 的尝试次数注入的故障因此落空，T3 不稳定，PR #22 先在测试里绕开（任务起来之后才起 Worker）。

现在网关遇到这种调用先等：反复重读账本（间隔从 10 ms 加到 100 ms），等到 StartRun 记下这条任务之后 5 秒为止，期间按工作流定的心跳超时发心跳（现在的工作流定 30 秒，等待用不上它；工作流定得比等待还短时才要靠它）。记上之后照原样核对 workflow ID 和 run ID：记上的 run ID 不是调用方的，照样按未知任务拒绝，不可重试；启动失败、StartRun 删掉了这条任务的，也一样。过了这 5 秒还没记上，才像原来那样拒绝（可重试），日志里记一条警告。StartRun 没记上 run ID 时会终止这个工作流，agentd 中途停掉留下的，下次启动时由 RecoverStarts 补上。

和设计文档的关系：第 5 节“未登记的任务报不可重试的错误”不变，没有这条任务、或者 run ID 不是调用方的，仍是不可重试的拒绝。第 7 节的网关草图把查不到任务一律当成不可重试的“unknown run”，没有“记下了任务、还没记上 run ID”这一段；这一段代码原来就报可重试的错误，现在是先等、等不到再报。账本读出错时，代码也一直报可重试的错误。

评审改进的两处：

- 5 秒按任务算，不按尝试算。安全评审指出：要是按尝试算，一直记不上 run ID 的任务（StartRun 既没记上也没终止掉它，或者 RecoverStarts 发现这个 workflow ID 下的执行有父工作流、不记），被攻破的 Worker 给它排上许多活动、重试次数不限，每次尝试都要占网关的一个并发位 5 秒，而尝试次数上限和循环保护都在这之后才查。按任务算，过了 5 秒的尝试立即拒绝，和原来一样。Worker 自己造不出这样的任务（workflow ID 是新的 UUID，代理也不放行子工作流），正常起任务时等的只是那几毫秒。代价：Temporal 启动工作流本身就花了 5 秒以上时，或者墙上时钟往前跳了，第一个调用照旧被拒，回到原来的重试。墙上时钟往回调过、任务记下的时间落在“将来”的，也不等，否则在时钟追上之前，每次尝试又会各等 5 秒（安全复审指出）。
- 心跳改用这次尝试的 context。对抗评审在真实的 Temporal 上复现了：心跳辅助函数原来把自己的 context 交给 SDK，SDK 把攒下、稍后才发的那次心跳也用这个 context 发；等待结束时它被取消，那次心跳就丢了，接着执行调用的那段要过心跳超时的三分之一才发下一次。工作流把心跳超时定在约 7.5 秒以下、等待又较长时，这次尝试就会在执行中途超时（心跳超时 6 秒、等 4.5 秒、模型调用 6 秒，3 次都超时）。现在的工作流定 30 秒，不受影响。

测试：

- 故障脚本加了 `run_id_delay`（只给测试用），让 StartRun 晚这么久才记 run ID。`tests/test_gateway_identity.py::test_a_runs_first_call_waits_for_the_run_id` 先起 Worker 再起任务，run ID 晚 2 秒记上：第一个调用先等，再以第 1 次尝试被故障挂住，Temporal 的历史里它只开始过一次、之前没有失败的尝试。改之前它失败：第 1、2 次尝试被拒，第 3 次没被挂住就执行了。
- `internal/gateway/start_test.go` 经 Temporal SDK 的测试环境调用真正的 `Invoke`：等到 run ID 记上后照常执行；记上的是别的 run ID、或者任务被删，立即按未知任务拒绝，不等满；一直没记上，等满之后可重试地拒绝；任务一分钟前就记下了的，或者记下的时间在“将来”的，不再等，立即拒绝；工作流把心跳超时定得比等待还短时，这次尝试不会超时，心跳用的 context 在尝试结束前都没结束；直接调用等待的函数时，尝试的 context 一结束它就停。网关不等的时候（等待函数换成只查一次），除了两条本来就不该等的，这些用例都失败；按尝试计时的时候，一分钟前记下的那条失败；不处理时钟往回调的时候，“将来”的那条失败；心跳用辅助函数自己的 context 时，心跳那条失败。逐个改坏实现（等待时不发心跳、遇到未知任务接着等），对应的用例也都失败。
- T3 恢复原来的顺序，先起 Worker。

## 还没解决的风险

- macOS 上 Runtime 入口是回环 TCP 端口，本机任何进程（包括同一台 Mac 上的其他用户）都能连上去冒充 Worker。v0.1 的信任模型不防同一用户，但多用户的 Mac 要注意；Linux 上是 0700 目录里的 Unix socket，没有这个问题。
- 工作流代码里取不到的键（比如 `diff["blob"]`）会让工作流任务无限重试：任务挂住，也不发报告。这是第 7 节工作流代码的行为，T12 时要处理。
- Runtime 代理不限制 `GetWorkflowExecutionHistory` 读哪个工作流，同一命名空间里任何工作流的历史，Worker 都能读到。M1 只有一种任务，所有 Worker 同样不可信，问题不大。以后有可信的工作流，或者按租户分 Worker 时，要么给沙箱 Worker 单独一个命名空间，要么让代理只放行分给这个 Worker 的工作流。
- `Scope` 只约束列出来的参数名。如果工具还接受别的能指定目标的参数（比如 `head_repo`），就能绕过去。M3 接真实工具时，参数要按 schema 校验，未知参数一律拒绝。
- Linux 沙箱里还有不记在任何进程名下的内存，OOM killer 挑不中 Worker：memfd、SysV 共享内存和消息队列（沙箱里能改 `kernel.msgmnb` 这些 IPC 参数，以前就是这样），以及管道和 Unix socket 的缓冲区。实测在改好的沙箱里 memfd 和 SysV 共享内存各占 128 MiB，宿主机的 Shmem 多了 256 MiB，进程自己的常驻内存只有 10 MB；评审实测 3000 个管道占 83 MiB，200 对 socket 占 91 MiB。匿名内存分散到很多小进程时也一样（见上）。进程数只受这个用户的 `ulimit -u` 限制，和 agentd、Temporal 共用：Worker 把它用光，它们的 Go 运行时建不了线程就会退出。这些都要靠 cgroup v2（`memory.max`、`memory.swap.max`、`pids.max`；评审实测上面这些内存都记在 memcg 上）或者 seccomp，M5 定稿沙箱时一起做。`/dev/pts` 还能占宿主机共用的伪终端。macOS 的 Worker 沙箱则不限私有目录的大小，能写满磁盘。
- Runtime 代理每条请求最多收 128 MiB，并发流数不限（grpc-go 的默认值），转发前还要把整条请求读进内存检查。被攻破的 Worker 开很多流，就能让 agentd 自己占满内存，OOM killer 杀的就是 agentd。可以加 `grpc.MaxConcurrentStreams`、按方法收紧大小上限、限每个 Worker 的连接数（看代码得出，没实测）。
- Worker 启动脚本只在启动时检查一次代码目录和 Python 环境。之后才出现在里面的 socket 或数据目录，比如后起的、数据目录放在里面的 agentd，正在跑的 Worker 照样看得到。M5 改由 agentd 拉起 Worker 时，可以在源头比较代码目录和自己的数据目录、socket。
- macOS 的 Worker 沙箱按黑名单拦读，家目录以外默认都能读。数据目录和镜像的真实路径不在家目录里时，得列进 `AGENTOS_SANDBOX_DENY_READ`，启动脚本不知道它们在哪：第二套开发环境的数据目录、`~/.agentos` 里指向家目录外的镜像都是这样（评审实测读到了；`~/.agentos` 本身不用列，启动脚本总会拒读它的真实路径，但它只在启动时解析：那时还悬空的符号链接，指向的地方之后才建出来，正在跑的 Worker 就读得到）。agentd 给工具服务器算拒读列表时本来就有这些路径（`toolDenyRead`），可以让启动脚本直接用它，不靠手写的环境变量。Linux 按白名单挂载，没有这个问题。
- 启动脚本只按名字认开发数据，改了名的副本、硬链接和压缩包都认不出来（评审实测过硬链接和改名的副本）。代码目录给得太大，里面别的东西照样被 Worker 看到：镜像、`.git/config` 里的凭据、别的 worktree。代码目录应该只放任务代码。README 用的是 `python`；测试要加载 `tests.*` 里的工作流，用的还是仓库根，可以改成每个测试会话把 `python/` 和 `tests/` 复制到一个临时目录交给 Worker。
- 开发数据现在由所有 checkout 共用一份 `~/.agentos`：开发用的 agentd 同一时间只能跑一个，第二个因为锁直接拒绝启动；不同分支轮流用同一个账本。agentd 现在会拒绝别的 agentd 迁移过的账本（见上），但只有带这次改动的 agentd 会查：没把这次改动合进来的分支编出来的 agentd 照旧不查，在新账本上照样跑，它自己应用的迁移也不留记录，之后带这次改动的 agentd 会因为“没有记录”拒绝这个账本，哪怕文件其实一样。老分支要尽快把 main 合进来。账本里 1 到 7 的版本只能当作本机文件，它们当初是不是别的样子查不出来。
- 虚拟机里的 Linux 启动脚本也会拒绝 Mac 那边留在共享目录里的 socket，比如开了 git fsmonitor 时 `.git` 里的那个，虽然虚拟机根本连不上它（评审实测，连接被拒）。
- 和这次改动无关、评审顺带发现的 macOS 问题：Worker 启动脚本从不删除自己的状态目录（`/tmp/agentos-sandbox.*`，这台 Mac 上已经积了 910 个），后起的 Worker 都能读到前面那些；srt 默认允许沙箱写 `/tmp/claude`，`run-tool.sh` 禁了，Worker 的启动脚本没禁。

## 后续（不在 M1 范围）

- CI 已由 PR #1 配好（`.github/workflows/ci.yml`：Go 检查，加上无沙箱和 bwrap 两套黑盒测试）。2026-10-17 起这三个检查是 main 的必需检查；PR 打开自动合并后，全绿即合并。
- 按副作用类别恢复 PENDING 的记录，以及核对的两个出口（M3）；审批表、EDITED 和 outbox（M4）。
- Worker 由谁拉起：M1 由启动脚本拉起，“沙箱起不来就拒绝启动”和 `--insecure-dev` 开关留到 M5 沙箱定稿时放进 agentd。
