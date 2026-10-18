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

## 还没解决的风险

- macOS 上 Runtime 入口是回环 TCP 端口，本机任何进程（包括同一台 Mac 上的其他用户）都能连上去冒充 Worker。v0.1 的信任模型不防同一用户，但多用户的 Mac 要注意；Linux 上是 0700 目录里的 Unix socket，没有这个问题。
- 工作流代码里取不到的键（比如 `diff["blob"]`）会让工作流任务无限重试：任务挂住，也不发报告。这是第 7 节工作流代码的行为，T12 时要处理。
- Runtime 代理不限制 `GetWorkflowExecutionHistory` 读哪个工作流，同一命名空间里任何工作流的历史，Worker 都能读到。M1 只有一种任务，所有 Worker 同样不可信，问题不大。以后有可信的工作流，或者按租户分 Worker 时，要么给沙箱 Worker 单独一个命名空间，要么让代理只放行分给这个 Worker 的工作流。
- `Scope` 只约束列出来的参数名。如果工具还接受别的能指定目标的参数（比如 `head_repo`），就能绕过去。M3 接真实工具时，参数要按 schema 校验，未知参数一律拒绝。
- Linux 沙箱里还有不记在任何进程名下的内存，OOM killer 挑不中 Worker：memfd、SysV 共享内存和消息队列（沙箱里能改 `kernel.msgmnb` 这些 IPC 参数，以前就是这样），以及管道和 Unix socket 的缓冲区。实测在改好的沙箱里 memfd 和 SysV 共享内存各占 128 MiB，宿主机的 Shmem 多了 256 MiB，进程自己的常驻内存只有 10 MB；评审实测 3000 个管道占 83 MiB，200 对 socket 占 91 MiB。匿名内存分散到很多小进程时也一样（见上）。进程数只受这个用户的 `ulimit -u` 限制，和 agentd、Temporal 共用：Worker 把它用光，它们的 Go 运行时建不了线程就会退出。这些都要靠 cgroup v2（`memory.max`、`memory.swap.max`、`pids.max`；评审实测上面这些内存都记在 memcg 上）或者 seccomp，M5 定稿沙箱时一起做。`/dev/pts` 还能占宿主机共用的伪终端。macOS 的 Worker 沙箱则不限私有目录的大小，能写满磁盘。
- Runtime 代理每条请求最多收 128 MiB，并发流数不限（grpc-go 的默认值），转发前还要把整条请求读进内存检查。被攻破的 Worker 开很多流，就能让 agentd 自己占满内存，OOM killer 杀的就是 agentd。可以加 `grpc.MaxConcurrentStreams`、按方法收紧大小上限、限每个 Worker 的连接数（看代码得出，没实测）。
- 开发配置 `config/dev/agentd.yaml` 把数据目录放在仓库里（`.agentos`），Linux 的启动脚本又把代码目录整个只读挂进沙箱，而测试用的代码目录就是仓库根。照这样起的 Worker 能读账本、连 Control socket：只读挂载挡不住 `connect()`，Runtime socket 就是这样连的。测试把数据放在仓库外面，所以没测到。要么把开发配置的数据目录挪出仓库，要么只挂 Worker 自己的代码。

## 后续（不在 M1 范围）

- CI 已由 PR #1 配好（`.github/workflows/ci.yml`：Go 检查，加上无沙箱和 bwrap 两套黑盒测试）。2026-10-17 起这三个检查是 main 的必需检查；PR 打开自动合并后，全绿即合并。
- 按副作用类别恢复 PENDING 的记录，以及核对的两个出口（M3）；审批表、EDITED 和 outbox（M4）。
- Worker 由谁拉起：M1 由启动脚本拉起，“沙箱起不来就拒绝启动”和 `--insecure-dev` 开关留到 M5 沙箱定稿时放进 agentd。
