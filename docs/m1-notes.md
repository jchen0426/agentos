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
4. **代理的 gRPC 参数要和 Temporal 对齐**。grpc-go 服务端默认只容许客户端每 5 分钟一次 keepalive ping，Python SDK 每 30 秒一次，会被回 GOAWAY（too_many_pings）断开；默认 4 MiB 的消息上限也会截断大的历史。代理已按 Temporal frontend 的设置放宽（10 秒、128 MiB）。（2026-10-19 更正：128 MiB 只该给响应，那是 Temporal 的客户端收历史的上限；Temporal frontend 收请求的上限就是 grpc-go 默认的 4 MiB，见下面“Runtime 代理的资源上限”。）
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
## Runtime 代理的资源上限（2026-10-19）

原来记在“还没解决的风险”里的一条：代理每条请求收 128 MiB、并发流数不限、整条读进内存再检查，被攻破的 Worker 能让 agentd 自己占满内存，OOM killer 杀的就是 agentd。这次先实测再改。测量工具像 Worker 一样连 Runtime 入口（macOS 上是回环端口，Lima 虚拟机里是 Unix socket），直接写 HTTP/2 帧，所以能做 gRPC 客户端不会做的事：留着请求的最后一个字节不发、不理服务器的流数上限、从不读响应、发超大的头。每个场景用新起的 agentd 和 Temporal 开发服务器（1.32），每 0.1 秒用 `ps` 取常驻内存。

先确认的两件事：

- Temporal 的 frontend 收请求的上限就是 grpc-go 默认的 4 MiB，超过的一律回 ResourceExhausted（`grpc: received message larger than max (N vs. 4194304)`），直连和经代理都一样，也没有动态配置能改。所以代理收 128 MiB 的请求毫无用处：大于 4 MiB 的请求永远不会成功，只会先在 agentd 里占着内存。
- 正常的 Worker 用得很少。用插桩的 agentd 跑整套测试（47 个）：请求最大 11.7 KB（`RespondWorkflowTaskCompleted`），响应最大 88 KB（带历史的 `PollWorkflowTaskQueue`），同时最多 2 个调用、2 个连接，元数据最多 179 字节。Python SDK 的请求是 gzip 压缩的；它自己检查单个 payload（2 MiB），不查整条请求。

改之前（macOS；括号里是虚拟机里经 Unix socket 的同类场景）：

| 场景 | agentd 常驻内存 |
| --- | --- |
| 8 个流各声明 100 MiB 的请求，留最后一个字节不发 | +809 MiB（Linux +816 MiB），连接不断就一直占着 |
| 4 个连接 × 8 个流，同上 | 峰值 2.4 GB（macOS 压缩了一部分内存，实际占着 3.2 GB 数据） |
| 4 个连接 × 64 个流 × 4 MiB，同上（即使上限降到 4 MiB） | +1.0 GB；grpc-go 的动态窗口把流窗口撑到了 1.4 MB |
| 8 个完整的 100 MiB 请求（命名空间不对，被规则拒绝） | +3.2 GB：接收、拷贝、解码，每条约 4 倍 |
| 32 个 3.9 MiB 的请求，全是最短的 START_TIMER 命令（在 Temporal 的上限以内，会被转发） | 峰值 4.1 GB：每条解码出约 100 万个命令，约 30 倍 |
| 1 个 4 MB 的 `RecordWorkerHeartbeat`，里面是 200 万个空心跳 | 峰值 +700 MiB（Linux +428 MiB），Temporal 还照收了；4 个同时来 2.8 GB |
| 2000 个空闲连接 | +83 MiB，2017 个文件描述符，没有上限 |
| 8 个流各带 8 MiB 的头 | 照收（grpc-go 默认 16 MiB） |
| 一个 45 MB 的历史页，4 个连接 × 8 个流请求、从不读响应 | 峰值 2.6 GB（Linux 1 个连接 × 8 个流 +657 MiB），连接不断就一直占着 |

改了什么（`internal/runtimeproxy/limits.go`，数值都在 `DefaultLimits`）：

- **请求最多 4 MiB**（解压后），就是 Temporal 自己的上限。不压缩的请求，grpc-go 读到 5 字节的长度前缀就拒绝；SDK 发的是 gzip 请求，grpc-go 读完最多 4 MiB 的压缩数据、解压到 4 MiB 加 1 字节就停下拒绝，每条最多占约 8 MiB。返回的仍是 ResourceExhausted，SDK 照旧把工作流任务记成 GRPC_MESSAGE_TOO_LARGE，Temporal 以 `GrpcMessageTooLarge` 终止这个任务，Worker 照常运行，跟以前由 Temporal 拒绝时一样；agentd 记一行 `runtime call refused`。
- **不带任务结果的方法，请求最多 64 KiB**：除了 `RespondWorkflowTaskCompleted`、`RespondWorkflowTaskFailed`、`RespondQueryTaskCompleted`，其余都是轮询、心跳、取历史这类请求，SDK 发的都在 0.5 KB 以内。取历史的请求里带着 Temporal 自己给的 `next_page_token`：经 Runtime 入口把 302 个事件的历史每页 5 个翻完（61 页），令牌最长 310 字节（黑盒测试 `test_a_long_history_pages_through_the_runtime_entry` 每次都这样翻一遍）。
- **解码之前先数值**：在线路格式上数字段值，嵌套消息和 packed 列表的每个元素都算，超过 5 万个就拒绝，嵌套超过 100 层也拒绝。解码的开销跟值的个数成正比，跟字节数关系不大：一个空心跳在线路上 2 字节，解码后约 350 字节。正常的请求只有几百个值。
- 这三种超限的拒绝都用 grpc 自己的措辞（`grpc: received message larger than max (...)`）。SDK（sdk-core 的 `retry.rs`）只认这几个开头：认出来就把工作流任务记成 GRPC_MESSAGE_TOO_LARGE、任务结束；认不出来就当成可重试的错误，工作流任务一次次超时，永远停在那里（评审发现；`tests/test_runtime_limits.py` 的值超限用例在改措辞前就是这样超时的）。
- **每个连接最多 32 个同时的调用**（`SETTINGS_MAX_CONCURRENT_STREAMS`），多出来的流被 REFUSED_STREAM 拒绝；正常的 HTTP/2 客户端会自己排队。
- **最多 4 个连接**，多的留在内核的 accept 队列里，不占 agentd 的内存和文件描述符；满了记一行日志（每分钟最多一行）。5 分钟没有调用的连接会被关掉：活着的 Worker 总有一个轮询开着，所以只有没人用的连接会让出位置。
- **同一时间只有 4 个请求在读、检查和转发**（所有连接合计）。等着的调用只占它的流窗口：流窗口固定为 64 KiB，关掉了 grpc-go 的动态窗口。正在读的消息会按自己的大小开窗口，所以不变慢。
- **交给 Temporal 而 Temporal 还没回答的请求合计最多 16 MiB**。请求交给 agentd 到 Temporal 的连接以后，Temporal 读走以前一直在 agentd 里；Temporal 慢下来时，这些请求以前可以一直攒下去（评审发现）。现在一个请求要在自己的名额里等到这 16 MiB 里有它的份，Temporal 回答（发出响应头）时还回来，所以 Temporal 跟不上时请求留在 Worker 那边。长轮询要等 Temporal 回答才还，所以轮询请求才限在 64 KiB。
- **头最多 16 KiB。**
- **一个调用只能带一条请求**：以前第二条请求不经检查就转给了 Temporal（Temporal 只用第一条）。
- **慢的 Worker 会被断开**：30 秒还没发完一条请求、或者发完了却不结束自己那一半调用，agentd 断开它的连接（它的调用跟着结束，占的内存释放）；30 秒还没开始 HTTP/2 的连接也断开。
- **响应**：agentd 跟着每个连接两个方向的 HTTP/2 帧，记每个调用的响应还有多少没写出去：响应的大小是它第一个 DATA 帧开头的 gRPC 长度前缀，写出 trailers 就算完，任何一方重置这个流就算作废（grpc-go 不会再写它剩下的部分）。所以知道 Worker 还有多少响应没读。起初只按连接数“交出去的减去写出去的”，对抗评审第二轮发现：守规矩的 Worker 因为调用超时取消一个读到一半的大响应，剩下的部分会一直算没读，连接 30 秒后就被断开；现在取消的调用不再算。agentd 写出的字节在交给 socket 之前就记下：写出去以后，Worker 可能先读到、先重置调用，而重置之后的字节不再计入（CI 上撞到过一次，整个响应一直算没读）。只剩一个窄缝：流在响应的头 5 个字节写出去之前就被重置，这个响应还算没读，只有给调用的窗口是 0 再重置它的 Worker 才做得到，代价是它自己的连接。没读的超过 8 MiB 时，这个连接的下一个响应先留在 Temporal（agentd 只收一个流窗口），等 Worker 读；有响应在等、Worker 又 30 秒不读，就断开连接，记一行日志。没有响应在等时不断开：没读的那些已经有上限（8 MiB 加一个响应）。每个连接一次只从 Temporal 收一个响应，所有连接合计同时最多收两个（收的时候 agentd 里约有这个响应的三份拷贝）；Worker 读完了前面的，这个响应才去排这两个名额，所以不读响应的 Worker 占不住它们。起初是所有连接合计一次一个，两个评审都指出这会让一个 Worker 的大响应卡住所有 Worker（Temporal 不在本机、或者先发响应头时尤其严重）；改成每个连接一个以后，自动安全审查又指出 4 个连接能同时各收一个 128 MiB 的响应，所以加了合计两个的上限。实测 Temporal 把响应头和响应一起发（空的长轮询头和消息都在 56 秒后才到），所以长轮询等的时候不占名额。这个前提没有专门的测试：要是哪天 Temporal 先发头，等着的长轮询会挡住同一个连接上的其他响应，每次最多约 60 秒。为了按字节数得准，发给 Worker 的响应不再压缩（以前 Worker 发 gzip 的请求，响应也是 gzip）。
- 连 Temporal 的后端连接同样用固定的 64 KiB 流窗口（agentd 读一个响应时，grpc-go 会按这个响应的大小给它开窗口，所以单个大响应不受它限），连接窗口是 16 MiB：grpc-go 收到数据就补连接窗口，不管读没读，所以它不占内存；评审实测连接窗口也是 64 KiB 时，离 Temporal 10 ms 的 45 MiB 历史要 8 秒，改成 16 MiB 后约 0.1 秒。请求上限 4 MiB，响应上限仍是 128 MiB（大的历史在响应里）。
- 被拒绝的调用：每一类（方法、命令、哪条规则拒的，不含 Worker 填的值；这个 API 版本不认识的命令类型都算同一类，否则 Worker 能编出无数类，每类都记全文，这是又一次自动安全审查指出的）每小时第一次照记全文，最多 256 类，一阵刷屏盖不住另一类尝试；其余所有连接合计每秒最多记一行（先放过 20 行），没记的最迟一分钟内记一个数；所以同一类的后续尝试只留下次数，不留各自的目标（比如给哪个工作流发信号），这些调用照样都被拒绝。方法名最多记 128 字节，原因最多 300 字节（原因里会引用 Worker 发的值，最长 4 MiB）。以前一个 Worker 能以每秒约 60 MB 的速度写满日志（评审实测）；按连接限速会被重连绕过，只在连接关闭时记数又会让人看不到正在发生的事（自动安全审查）。

改之后，同样的场景：

| 场景 | agentd 常驻内存 |
| --- | --- |
| 声明 100 MiB 的请求（留着不发或者发完） | 读到长度前缀就拒绝，内存不变 |
| 4 个连接 × 64 个流 × 4 MiB，留最后一个字节 | +22 MiB：每个连接后 32 个流被拒绝，4 个请求读完，其余每个最多 64 KiB |
| 32 个 3.9 MiB 的 START_TIMER 请求 | 峰值 +115 MiB，全部在解码前被拒绝 |
| 4 个 4 MB 的空心跳请求 | +81 MiB（Linux 1 个 +27 MiB），全部被拒绝（现在先撞上 64 KiB） |
| 128 个 3.8 MiB 的请求，Temporal 在建好连接后停住（SIGSTOP） | 峰值 182 MiB；第一轮修改后的代码是 776 MiB，请求全攒在去 Temporal 的连接里 |
| 200 个连接 | 4 个被服务，其余留在内核队列（满了以后连接超时），agentd 共 21 个文件描述符 |
| 8 MiB 的头 | 连接被重置 |
| 45 MB 的历史页，4 个连接 × 8 个流，从不读 | 峰值 424 MiB，之后 243 MiB（每个连接排着一个响应），30 秒后 4 个连接都被断开（Linux 1 个连接 +133 MiB） |

最坏情况有了上限，不再随 Worker 开的流和连接增长：请求一侧是 4 个正在读的请求（每个 4 MiB 的几份拷贝，加上解码：5 万个值，最大的结构约 350 字节一个），约 110 MB，加上交给 Temporal 还没回答的 16 MiB，和等着的流（每个最多 64 KiB 窗口加 16 KiB 的头，4 个连接 × 32 个流约 10 MB）；响应一侧每个连接是 8 MiB 加一个排着的响应，其中最多两个连接正在收（收的时候约有三份拷贝）。响应按最大的 128 MiB 算，合计约 1.2 GB；按 Temporal 默认 50 MB 的历史上限算，约 0.55 GB。Go 的垃圾回收还会让常驻内存高出这些活的数据。

`internal/runtimeproxy/limits_test.go` 用一个假的 Temporal 和上面那种手写 HTTP/2 帧的客户端逐条测试，第一轮改之前 7 个失败（超大请求、值太多、流数、连接数、同时读的请求数、头、第二条请求），评审后加的 12 个在改之前也都失败（短调用的上限、拒绝的措辞、Temporal 没回答时请求要等、Worker 不读时不挡别人的请求和响应、一个连接的慢响应不挡别的连接、同时收的响应数、空闲连接被关、一阵拒绝盖不住另一类也写不满日志、后端连接的窗口、取消一个读到一半的响应不丢连接、两个方向的流记账）；另有响应记账、两种超时、固定窗口、gzip 客户端、帧计数和数值的测试。最后对现在的代码逐条改坏全部机制（36 处变异，比如去掉某个选项、把请求上限改回 128 MiB、不再强制不压缩、所有连接共用一个收响应的名额、先占收响应的名额再等 Worker 读、不理 Worker 的重置），对应的测试都失败。黑盒测试时发现 SDK 的请求是 gzip 压缩的（agentd 的拒绝写的是 `received message after decompression larger than max`，SDK 照样认作 GRPC_MESSAGE_TOO_LARGE）。响应若照旧跟着压缩，写到 socket 上的字节会远少于从 Temporal 收的字节，记账就会把 Worker 当成不读响应，所以有了上面的不压缩和 gzip 客户端的测试。`tests/test_runtime_limits.py` 有两个黑盒用例：完成请求超过 4 MiB（三个 1.5 MiB 的网关调用），和值超过 5 万（一次排 5000 个网关调用）；两者都由 agentd 拒绝、Temporal 以 `GrpcMessageTooLarge` 结束任务、Worker 照常运行。

推送前有两个独立的评审代理（安全、对抗）各自在副本里做实验。都没发现绕过请求检查的路，也都确认了记账只会往多算、内存有上限。安全评审的三个中等问题（交给 Temporal 的请求能攒到 512 MiB、4 个空闲连接能把别的 Worker 永远挡在外面、后端连接窗口太小）和几个小问题（值超限的拒绝 SDK 认不出、日志能被刷满、计时器边界、说明里言过其实的地方），对抗评审的中等问题（所有连接共用一个收响应的名额）和文档问题（gzip 请求并不是读到长度前缀就停）都按上面改了，各有测试。之后提交上的自动安全审查又指出两处（拒绝日志限速后，一阵刷屏能藏住别的尝试；每个连接各收一个响应时，4 个连接同时收的内存比原来多），也按上面改了。两个评审代理又复审了改动：安全评审逐条用实验确认修好，另提了取历史的令牌会不会超过 64 KiB（实测最长 310 字节，加了翻页的黑盒测试）；对抗评审确认没有泄漏、重复释放或死锁（约 3900 次随机取消的压力测试），另发现取消读到一半的响应会断开连接，改成了按流记账。最后一轮两个评审都没有新的问题：安全评审用 736 轮“收下响应再重置”的攻击确认按流记账压不到实际以下（常驻内存稳定在约 276 MiB，没有连接被断开），对抗评审试了快速开流再重置、2000 个被拒的流、窗口为 0 再重置，记账都只会往多算，流表也有上限。

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
## macOS 启动脚本的状态目录和 `/tmp/claude`（2026-10-19）

PR #20 的评审顺带发现的三处 macOS 启动脚本问题（比它早就有），这次一起修了。改之前都复现过：

- srt 默认放行沙箱写 `/tmp/claude`（和 `/private/tmp/claude`），还把沙箱里的 `TMPDIR` 换成它。`run-tool.sh` 禁了，Worker 的启动脚本没禁：Worker 能在那里建文件，`tempfile` 也建在那里。这个目录由第一个用到它的进程建出来，人人可读，本机所有按 srt 默认设置跑的命令共用。启动脚本自己传的 `TMPDIR` 被 srt 盖掉了，从来没生效。
- 启动脚本每次在 `/tmp/agentos-sandbox.XXXXXX` 建一个状态目录（srt 的设置、srt 自己的 socket、Worker 的私有目录），然后 `exec` srt，没有谁删它：这台 Mac 上已经积了 1300 多个。macOS 沙箱家目录以外默认都能读，所以 Worker 读得到别的 Worker 的私有目录、别的 srt 的设置（里面是整张拒读列表），也读得到留下来的那些。
- `run-tool.sh` 拒读用户数据库里的家目录，只按原样交给 srt。家目录要是指向别处的符号链接（比如 `/Users/x` 指向另一个卷），srt 保留原写法，Seatbelt 按真实路径匹配，等于没拒读。Worker 的启动脚本在 PR #20 里已经连真实路径一起给了。

改动：

- Worker 启动脚本禁写 `/tmp/claude` 和 `/private/tmp/claude`。两种写法都要：`/tmp/claude` 不存在时，srt 解析不出它的真实路径。srt 把 `CLAUDE_CODE_TMPDIR` 当作沙箱里的 `TMPDIR`，启动脚本把它设成 Worker 的私有目录；srt 自己的 `TMPDIR`（它的 socket 放在那里）原先也是这个私有目录，现在改成状态目录本身，Worker 读写不到。
- 状态目录跟着 srt 走。启动脚本 `exec` srt 之前开一条管道：写端是 fd 9，srt 一直拿着，不传给它起的进程（macOS 上 libuv 用 `POSIX_SPAWN_CLOEXEC_DEFAULT` 起进程）；读端交给一个不在 Worker 进程树里的进程（fork 两次，父进程成了 launchd）。读到管道关闭（srt 以任何方式结束，内核都会关掉它的 fd）就删掉状态目录。启动脚本照旧 `exec` srt，信号处理和以前完全一样：SIGTERM 发给 srt，srt 转给 Worker，Worker 死于 SIGTERM 时 srt 以 0 退出。没用“不 exec，等 srt 结束再删”：harness 遇到停不下来的 Worker、T1 测试要它崩溃时，都 `kill -9` 整棵进程树，启动脚本自己也在里面。
- 删除本身不能被 Worker 利用（这是 PR #20 两个评审复查本次改动时发现、我引入的高危回归）：删目录的进程在沙箱外，用非沙箱用户的权限跑；Worker 虽然只能写自己的私有目录，却能在里面放符号链接，`rm -rf` 遍历时要删的目录被换成符号链接就会跟出去，把沙箱外的东西（家目录、账本、仓库）递归删掉。真正利用要靠竞态：Worker 留一个 `setsid` 的子孙进程（不在被杀的进程树和进程组里），在 srt 退出、删除开始时不停地把目录换成符号链接。所以删除交给 `sandbox/macos/rm-state.py`：它不按名字判断，只沿着自己用 `O_RDONLY|O_NOFOLLOW|O_DIRECTORY` 打开的 fd 往下走——符号链接打不开（ELOOP），当成链接本身删掉，绝不跟随；非目录打不开（ENOTDIR），也删掉；只有真目录能打开，递归用那个 fd，不再用名字。检查和下降之间没有窗口。找不到这样的 Python（先用 pyenv 自带的，再找 PATH 上的 `python3`）就不删，把目录留下（仍然拒读），绝不用不安全的方式删。
- Worker 还能用 `chflags uchg` 钉住自己私有目录里的文件、或把子目录 `chmod 000`，让一次 `rm` 删不掉、状态目录留下来（两个评审都发现了；只影响自己这一个目录，仍然拒读，不泄密，是整洁/占用问题）。`rm-state.py` 删不掉时尽力清一次 flag 和 mode 再重试，失败就只留下那一个条目。整个进程组一起被杀（或 SIGKILL/SIGSTOP）会带走删目录的进程，目录也会留下；这些都等 M5 由 agentd 建、删这个目录来根治。读端只忽略它能忽略的作业控制和终端信号（INT QUIT TSTP TTIN TTOU TERM HUP USR1 USR2）。
- 启动脚本开头保证 fd 0、1、2 都是开的（关着就接 `/dev/null`）：否则调用者把它们关掉时，bash 会把上面那条管道的写端放到 fd 2 这种低位，沙箱里的 Worker 就继承了管道（评审发现）。
- 每次启动都拒读所有启动的状态目录（`/private/tmp/agentos-sandbox.*`，按 mktemp 的命名），再放开 Worker 自己的私有目录（allowRead 优先）：别的 Worker 的文件、srt 的设置、留下来的目录都读不到。glob 要写成真实路径：基目录正好是 `/tmp` 的 glob，srt 不会改写成 `/private/tmp`，Seatbelt 就匹配不上。
- `run-tool.sh` 把用户数据库里的家目录连同真实路径一起拒读，和别的拒读条目一样。

测试（都只在 macOS 上跑）：

- `tests/test_sandbox.py::test_worker_sandbox_writes_only_its_private_directory`：Worker 往 `/tmp/claude`、`/private/tmp/claude`、`/tmp`、`/var/tmp`、家目录、代码目录写都被拒（EPERM），沙箱外也没出现；`tempfile` 能用，文件建在私有目录里。改之前 `/tmp/claude` 下的两个都写进去了。
- `test_worker_launcher_leaves_no_state_behind`：Worker 自己退出、SIGTERM 发给 srt、SIGTERM 发给整个进程组、Ctrl-C（SIGINT 发给进程组）、SIGHUP 发给进程组（srt 不捕获它，死于它；关终端就是这个信号）、`kill -9` 整棵进程树，六种结束方式之后 10 秒内，srt 的设置所在的目录（从 srt 的命令行取）和 Worker 的私有目录都没了；srt 的退出状态也对（前四种 0，SIGHUP 死于信号，kill -9 死于信号），Worker 的进程也都不在了。改之前都留下了。
- `test_rm_state_removes_a_tree_without_following_a_symlink_out_of_it`：直接跑 `rm-state.py`，状态目录里放一个指向目录外 canary 的符号链接、一个 `uchg` 文件、一个 `chmod 000` 的子目录；删完目录没了，canary 原封不动（没跟随符号链接）。去掉 `O_NOFOLLOW` 这个用例就失败（符号链接被跟进去，canary 被删）。另外实测了真正的竞态：Worker 留一个 `setsid` 子孙进程不停换符号链接，单杀 srt，canary 10 次都没被删。
- `test_worker_sandbox_cannot_read_another_launchs_state`：一个 Worker 跑着，另一个去读它私有目录里的文件、它的 srt 设置、一个按启动脚本的命名留下的目录，都被拒；自己私有目录里的文件读得到。沙箱外先确认这些都读得到。改之前三样都读到了。
- `tests/test_tool_sandbox.py::test_the_git_tool_server_cannot_read_a_home_directory_named_through_a_symlink`：测试改不了用户数据库，就在启动脚本问的地方（Node 的 `os.userInfo()`，经 `NODE_OPTIONS` 预加载一段脚本）把家目录换成指向真正家目录的符号链接。改之前探针读到了家目录里的文件。
- 逐条改坏：Worker 启动脚本不禁写 `/tmp/claude`、只禁一种写法（`/tmp/claude` 不存在时才测得出）、不设 `CLAUDE_CODE_TMPDIR`、不起删目录的进程、它留在进程树里、它不忽略信号（只有发给进程组的 SIGTERM 测得出：bash 本来就让后台命令忽略 SIGINT 和 SIGQUIT）、不拒读状态目录、glob 写成 `/tmp/...`、不放开自己的私有目录；`rm-state.py` 去掉 `O_NOFOLLOW`、不清 `uchg`、不清 `chmod 000`；`run-tool.sh` 不解析家目录的真实路径——每一处都有用例失败。

原来留在 `/tmp` 的状态目录（连同本次测试留下的）没有删，挪到了家目录的 `~/agentos-backups/2026-10-19-macos-worker-state/`（Worker 读不到）；还有 srt 进程在用的（命令行里点名的）留在原处。别的 checkout 和 worktree 用的还是旧启动脚本，在它们更新之前还会继续留下新的。

## 测试里的 Temporal 开发服务器答应之后又退出（2026-10-19）

CI 偶尔在测试一开头失败：Temporal 开发服务器答应了测试框架的就绪检查，不到一秒又退出，agentd 连不上它（"connection refused"），`_launch_agentd` 的断言失败。见过两次，在两个 PR、两种模式下：run 39191441167（PR #23，none，`tests/test_cancel.py` 的第一个测试）和 run 39181890891（PR #21 的分支，bwrap，`tests/test_write_limits.py::test_a_run_posts_one_report`）。两次 Temporal 的日志都是 worker 服务的 gRPC 监听失败（`bind: address already in use`），端口正是框架给前端选的那个，随后进程 panic。框架只在服务器答应之前就退出时才换端口重试，这种漏了过去。CI 到现在的 96 次运行（共 99 次尝试，算上重跑前失败的那几次）里，只有这两次是这个原因。

原因（看的是 Temporal CLI 1.9.1 的源码：`internal/temporalcli/commands.server.go`、`internal/devserver/server.go`、`internal/devserver/freeport.go`；1.9.1 也是现在最新的版本）：

- 框架的 `free_port()` 绑端口 0，读出端口号就放手。`temporal server start-dev --port P` 先试绑 P（绑上就放），再向系统要 5 个端口：metrics、HTTP API，和 history、matching、worker 三个内部服务的 gRPC 端口，每个都是绑端口 0。内部服务的端口没有参数可以指定，`--port 0` 直接报 `missing frontend port`：既不能固定内部端口，也不能让 Temporal 自己选前端端口再读出来。
- Linux 随机分配端口 0，刚放手的端口下一次就可能分出去。在虚拟机里（内核 6.8，默认范围 32768–60999）各绑 4000 次：不设 SO_REUSEADDR 时（框架原来的 `free_port()`）在整个范围的奇数端口里挑，设了时（Go 默认设）只在下半段（32768–46883）的奇数端口里挑。CI 上那两个端口 37977、33757 都是下半段的奇数。粗算每个测试栈约有 1/2800 的机会让 Temporal 的 5 个端口之一落在 P 上，落在 worker 上约 1/14000；现在每次 CI 约 110 个栈，不到 1% 的 CI 会因此失败。
- 落在谁身上结果不同：另外 4 个端口拿到 P 时，前端绑不上或者 HTTP API 起不来（下面的复现里都是这样），服务器在答应之前就退出，框架换端口重试；worker 在前端之后才绑，这时前端已经能答应了，于是漏过去。
- Temporal 自己选的端口防了这一点：在 Linux 上它连一次刚选的端口，从监听那一侧先关，这一侧的连接在 TIME_WAIT 里留 60 秒。这期间 Linux 不把这个端口分给绑端口 0 的 socket 或往外连的连接，按端口号带 SO_REUSEADDR 绑定的仍然可以。macOS 按顺序分配端口（这台 Mac 上实测也是），刚放手的端口要轮完一圈才会再分出去，它在 macOS 上就不做这一步，本机的测试也就几乎碰不上这个问题。框架给的端口没有这样留住。
- 同一个原因也可能让 Temporal 拿走 agentd 的 Runtime 端口（none 模式，栈起来时就选好，agentd 后起），或者让几次 `free_port()` 给出同一个端口（复现里常见），或者让 mTLS 测试给完整服务器的 9 个端口有重复。CI 上没见过。

复现：在虚拟机里，每次用一个新的网络命名空间，临时端口范围设成 40000–40199，奇数端口除了下半段的 5 个都保留（`ip_local_reserved_ports`），Temporal 要的 5 个端口就一定把框架给前端的那个也拿走。原来的框架 25 次里 6 次失败，和 CI 一样（worker 绑不上前端的端口，Temporal 只起了一次，agentd "connection refused"）；另外 19 次都是 Temporal 在答应之前就退出，靠重试才过。

修复（`tests/harness.py`）：`free_port()` 像 Temporal 那样留住端口：设 SO_REUSEADDR 监听端口 0，连一次，从监听那一侧先关。SO_REUSEADDR 不能少：留在 TIME_WAIT 里的连接继承这个选项，没有它，Linux 连服务器按端口号的绑定也拒绝（实测）。Linux 上留 60 秒，够用：Temporal 一起来就绑上前端端口，它自己的 5 个端口也在那一两秒里要完；Runtime 端口在测试启动 agentd 时才绑，通常也就几秒。只有 mTLS 测试先等完整服务器起来（最多 90 秒）再启动 agentd，慢的机器上可能过了 60 秒，那以后 Runtime 端口回到原来的情形，不过那时已经没有 Temporal 在要端口了（完整服务器的端口都由测试给定）。macOS 上 TIME_WAIT 是 30 秒，那里本来就按顺序分配。同样的复现跑了两轮，每轮 25 次全过，Temporal 每次只起一次，日志里没有监听错误。`_start_temporal` 的重试留着，防的只剩按端口号绑定的进程。新测试 `tests/test_harness.py` 检查这个约定：拿到的端口先被占着（不带 SO_REUSEADDR 的绑定被拒），带 SO_REUSEADDR 的绑定拿得到。改之前它失败；监听不设 SO_REUSEADDR 时它在 Linux 上失败，在 macOS 上照过（macOS 只看新绑定的那个 socket 有没有这个选项）。

没采用的：答应之后再观察一段时间，进程还在、日志里没有监听错误才往下走。每个栈都要多等，等多久才够也说不准，而且是出了事再补救，原因还在。

## 还没解决的风险

- macOS 上 Runtime 入口是回环 TCP 端口，本机任何进程（包括同一台 Mac 上的其他用户）都能连上去冒充 Worker。v0.1 的信任模型不防同一用户，但多用户的 Mac 要注意；Linux 上是 0700 目录里的 Unix socket，没有这个问题。
- 工作流代码里取不到的键（比如 `diff["blob"]`）会让工作流任务无限重试：任务挂住，也不发报告。这是第 7 节工作流代码的行为，T12 时要处理。
- Runtime 代理不限制 `GetWorkflowExecutionHistory` 读哪个工作流，同一命名空间里任何工作流的历史，Worker 都能读到。M1 只有一种任务，所有 Worker 同样不可信，问题不大。以后有可信的工作流，或者按租户分 Worker 时，要么给沙箱 Worker 单独一个命名空间，要么让代理只放行分给这个 Worker 的工作流。
- `Scope` 只约束列出来的参数名。如果工具还接受别的能指定目标的参数（比如 `head_repo`），就能绕过去。M3 接真实工具时，参数要按 schema 校验，未知参数一律拒绝。
- Linux 沙箱里还有不记在任何进程名下的内存，OOM killer 挑不中 Worker：memfd、SysV 共享内存和消息队列（沙箱里能改 `kernel.msgmnb` 这些 IPC 参数，以前就是这样），以及管道和 Unix socket 的缓冲区。实测在改好的沙箱里 memfd 和 SysV 共享内存各占 128 MiB，宿主机的 Shmem 多了 256 MiB，进程自己的常驻内存只有 10 MB；评审实测 3000 个管道占 83 MiB，200 对 socket 占 91 MiB。匿名内存分散到很多小进程时也一样（见上）。进程数只受这个用户的 `ulimit -u` 限制，和 agentd、Temporal 共用：Worker 把它用光，它们的 Go 运行时建不了线程就会退出。这些都要靠 cgroup v2（`memory.max`、`memory.swap.max`、`pids.max`；评审实测上面这些内存都记在 memcg 上）或者 seccomp，M5 定稿沙箱时一起做。`/dev/pts` 还能占宿主机共用的伪终端。macOS 的 Worker 沙箱则不限私有目录的大小，能写满磁盘。
- Runtime 代理的资源上限（见上面那节）之后还剩下的：被攻破的 Worker（macOS 上本机任何进程）开着调用占住 4 个连接，就能把同一个 agentd 的其他 Worker 挡在外面，空闲的连接 5 分钟后才会让出来，要按 Worker 分开入口（Linux 上每个 Worker 一个 socket，macOS 上启动时给一个凭据）才能解决；它占着 4 个请求名额，每轮最多 30 秒，也能让其他 Worker 的工作流任务（10 秒超时）超时，但弄不垮 agentd。Worker 并发读大历史时，生成和排队响应的内存记在 Temporal 名下（实测 32 个 45 MB 的历史请求让 Temporal 从 470 MiB 涨到 2.7 GB，改之前是 2.1 GB，另加 agentd 自己的 2 GB），要再收紧得按连接限制会带回历史的调用，或者让 `GetWorkflowExecutionHistory` 只能读分给这个 Worker 的工作流。响应的头 5 个字节还没写出去、Worker 就重置了这个流时（只有给调用的窗口是 0 才会这样），这个响应一直算没读，攒过 8 MiB 后，下一个响应等 30 秒，这个连接被断开，重连后重新计数。交给 Temporal 还没回答的 16 MiB 是所有 Worker 共用的：一个 Worker 用快到 64 KiB 的长轮询能占住其中约 8 MiB。Temporal 进程卡住而连接还在时（比如被 SIGSTOP），请求名额会都等在这 16 MiB 上，代理停顿，直到后端连接的 keepalive（30 秒加 15 秒）断开卡住的连接；内存仍有上限。另外可以给 agentd 设 `GOMEMLIMIT`，让常驻内存贴近上面的上限；Linux 上把沙箱里 Worker 的 `oom_score_adj` 调高、agentd 的调低，内存真不够时 OOM killer 先杀 Worker，它的连接一断，agentd 为它占的内存也就放了。
- Worker 启动脚本只在启动时检查一次代码目录和 Python 环境。之后才出现在里面的 socket 或数据目录，比如后起的、数据目录放在里面的 agentd，正在跑的 Worker 照样看得到。M5 改由 agentd 拉起 Worker 时，可以在源头比较代码目录和自己的数据目录、socket。
- macOS 的 Worker 沙箱按黑名单拦读，家目录以外默认都能读。数据目录和镜像的真实路径不在家目录里时，得列进 `AGENTOS_SANDBOX_DENY_READ`，启动脚本不知道它们在哪：第二套开发环境的数据目录、`~/.agentos` 里指向家目录外的镜像都是这样（评审实测读到了；`~/.agentos` 本身不用列，启动脚本总会拒读它的真实路径，但它只在启动时解析：那时还悬空的符号链接，指向的地方之后才建出来，正在跑的 Worker 就读得到）。agentd 给工具服务器算拒读列表时本来就有这些路径（`toolDenyRead`），可以让启动脚本直接用它，不靠手写的环境变量。Linux 按白名单挂载，没有这个问题。
- 启动脚本只按名字认开发数据，改了名的副本、硬链接和压缩包都认不出来（评审实测过硬链接和改名的副本）。代码目录给得太大，里面别的东西照样被 Worker 看到：镜像、`.git/config` 里的凭据、别的 worktree。代码目录应该只放任务代码。README 用的是 `python`；测试要加载 `tests.*` 里的工作流，用的还是仓库根，可以改成每个测试会话把 `python/` 和 `tests/` 复制到一个临时目录交给 Worker。
- 开发数据现在由所有 checkout 共用一份 `~/.agentos`：开发用的 agentd 同一时间只能跑一个，第二个因为锁直接拒绝启动；不同分支轮流用同一个账本。agentd 现在会拒绝别的 agentd 迁移过的账本（见上），但只有带这次改动的 agentd 会查：没把这次改动合进来的分支编出来的 agentd 照旧不查，在新账本上照样跑，它自己应用的迁移也不留记录，之后带这次改动的 agentd 会因为“没有记录”拒绝这个账本，哪怕文件其实一样。老分支要尽快把 main 合进来。账本里 1 到 7 的版本只能当作本机文件，它们当初是不是别的样子查不出来。
- 虚拟机里的 Linux 启动脚本也会拒绝 Mac 那边留在共享目录里的 socket，比如开了 git fsmonitor 时 `.git` 里的那个，虽然虚拟机根本连不上它（评审实测，连接被拒）。
- macOS 的 Worker 沙箱家目录以外按黑名单拦读，`/tmp` 和 `/var/folders` 里除了各次启动的状态目录，别的都读得到：工具服务器每次调用的目录（`/tmp/agentos-tools.*`，里面有 srt 的设置，也就是拒读列表）、别的程序的临时文件，比如 Claude Code 自己的 `/tmp/claude-<uid>`（实测读到了，权限 0700 挡不住同一个用户）。可以像 `run-tool.sh` 那样把 `/private/tmp` 和 `/private/var/folders` 整个拒读，只放开 Python 环境、代码目录和私有目录；那样测试里放在 `/tmp` 的数据就要挪到 `/var/tmp`，否则测不出拒读条目本身。
- macOS 启动脚本删状态目录的进程和 srt 在同一个进程组里。两个一起被杀（比如 `kill -9` 整个进程组）时，状态目录留下来，Worker 读不到，但没人删。M5 改由 agentd 拉起 Worker 时，可以像工具服务器那样由 agentd 建、删这个目录（`AGENTOS_SANDBOX_STATE`）。

## 后续（不在 M1 范围）

- CI 已由 PR #1 配好（`.github/workflows/ci.yml`：Go 检查，加上无沙箱和 bwrap 两套黑盒测试）。2026-10-17 起这三个检查是 main 的必需检查；PR 打开自动合并后，全绿即合并。2026-10-22 起（PR #41）：
  - push 只在 main 上触发：PR 的分支每次推送只跑一遍（`pull_request`，测的是它和 main 的合并），原来 push 和 pull_request 各跑一遍；没开 PR 的分支不跑。
  - 进 main 的 PR 只改 `docs/` 下的 Markdown 和根目录 `README.md` 时，跳过这三个检查，合并进 main 后照跑。判定由 `changes` job 做：拿 GitHub 要测的合并提交和 main 的头比，改名的两边都算；判定 job 失败或被跳过（push 到 main、进别的分支的 PR）时照常全跑。被 `if:` 跳过的 job 也满足必需检查。不是所有 `.md` 都是文档（`config/prompts/audit_v1/claude.md` 是模型的提示词），`docs/` 下也不全是文档（`go test ./...` 会编译那里的 `.go` 文件）。进别的分支的 PR 不跳：它之后改成进 main 时不会重跑，跳过的检查就顶着和错的 base 比出来的结果。
  - 文件名由 PR 决定，而 runner 在一行里任何位置都认 `##[set-output ...]` 这样的命令。审查时在一次性 PR #44 上实测过：只加一个名叫 `##[set-output name=docs-only;]true` 的非文档文件，三个检查就全被跳过。现在打印文件名时用 `::stop-commands::` 关掉命令解析，结果无论真假都写进 `GITHUB_OUTPUT`（步骤结束后处理，盖过中途设的值）。
  - 黑盒测试为此从 matrix 拆成两个 job，步骤用 YAML 锚点共用。matrix 的 job 被 `if:` 跳过时，GitHub 只报一个检查，名字里留着字面量 `${{ matrix.sandbox }}`，两个必需检查就永远等不到（在一次性分支上实测过）。
  - 设计文档 rev 347–348 按你批准的清单改了：第 11 节“故障注入测试”一句（rev 347）、第 12 节 M1 一条的最后一句（rev 348）。
- 按副作用类别恢复 PENDING 的记录，以及核对的两个出口（M3）；审批表、EDITED 和 outbox（M4）。
- Worker 由谁拉起：M1 由启动脚本拉起，“沙箱起不来就拒绝启动”和 `--insecure-dev` 开关留到 M5 沙箱定稿时放进 agentd。
