# M3 记录：工具与副作用

M3 拆成两段（设计文档第 12 节，rev 262）：M3a 副作用协议和 GitHub 写工具，M3b 只读 git MCP。

## M3a：副作用协议和 GitHub 写工具（2026-10-18）

验收 T2、T3、T15、T16 通过，GitHub 用本地模拟的。真实测试仓库上的那次 T2 放在 M3b 收尾。

### 做了什么

- **数据目录锁**：agentd 启动后第一件事是对数据目录加排他锁（`agentd.lock`，flock），拿不到就退出，账本和 socket 都不碰。以前第二个 agentd 会删掉并抢走 Control socket（Linux 上还有 Runtime socket），而且在绑定端口之前就执行 `SettleOpen`，把第一个 agentd 还在途的模型预留按全额结算，还会启动 gateway worker 领活动任务。锁随进程走，kill -9 也会释放。
- **副作用协议**（`internal/gateway/effects.go`，迁移 `00007_effect_claims.sql`）。一次写分四步：
  1. 渲染载荷：工具的 Prepare 生成外部系统将收到的全部内容，同一个请求每次渲染结果相同。
  2. 记录：载荷存成 blob，哈希存进 `payload_sha256`。
  3. 领取：按 `version` 比较并交换，记下 `claimed_by`（本进程的标识）和 `claimed_at`。领取在同一个事务里检查任务没被取消，所以 CancelRun 落库之后任何尝试都领不到。
  4. 发送，记结果。

  重试时重新渲染，载荷和记录的不一致就不执行（`PayloadChanged`）。M4 的审批绑定的就是这份载荷。
- **发送的结果分四种**：
  - 成功：SUCCEEDED。
  - 没发出去（`ErrNotSent`）或对方要求稍后再试（限流）：交还领取，返回可重试错误，下次尝试再领。
  - 对方明确拒绝（4xx）：FAILED，不可重试。
  - 同名资源已存在但内容不同：UNKNOWN，等人核对，不覆盖。

  其余情况（5xx、超时、连接断开）结果未知，当场按类别恢复。
- **按类别恢复**：
  - 适用于两种记录：被已经不在的进程领取的；被本进程领取、但当前没有执行在跑的。
  - 规则：
    - idempotent：同键重发。
    - lookup：先查。查不到就等到“最后一个可见请求发出的时间 + Settle”，再查一次，仍没有才重发；查询本身出错就转人工核对。
    - unsafe：记 UNKNOWN，返回 needs_reconcile。
    - 已取消的任务：只查，不发；查不到记 ABANDONED。
  - agentd 启动时还有两件事：
    - 已取消任务里没领取过的记录直接记 ABANDONED。
    - 上个进程领取的记录在后台恢复，和工作流重试走同一个 singleflight 键，谁先到谁做，另一个等结果。启动时的恢复只查不发：查不到就交还，由任务的下一次调用发出。任务已经结束的，不会再有下一次调用，也就不会再发。
  - 本进程领取、执行还在跑的记录，同时到的另一个执行者返回可重试的 `EffectBusy`，不会再发一次。
- **核对**：
  - `ListPending` 列出 UNKNOWN 的记录，包括当时要发的载荷全文。
  - `ResolveReconciliation` 有两个出口：
    - 已执行：结果由人给出（按工具的结果 schema 校验），或者 `--lookup` 让 agentd 去查，记录改成 SUCCEEDED。
    - 未执行：记录改回 PENDING，下次调用用原幂等键重发；任务已取消或已结束则改成 ABANDONED。lookup 工具在最后一个请求发出后的 Settle 时间之内，不接受“未执行”。
  - 并发和重复提交的处理：
    - 按 `version` 防并发，`resolve_key` 防重复提交。
    - 同一个 key 再发一次，返回同样的结果。
    - 换一个 key 再处理，报“已不在待核对状态”。
  - 处理完直接给工作流发门铃（outbox 留到 M4）。
  - 命令行：`agentctl reconcile list | show | resolve`。
- **工具注册表**（`internal/tools`）：
  - 每个工具在代码里声明三样东西：类别、参数 schema（`additionalProperties: false`）、写工具的结果 schema。
  - 写工具实现 Prepare、Execute 和 DryRun；lookup 类工具另外实现 Find 和 Settle。
  - 策略文件里的 `effect` 改为可选。要写，只能写工具自己声明的类别，或者对写工具写 unsafe。
  - agentd 启动时检查：策略写了更弱的类别，或者列出 agentd 没有的工具，都拒绝启动。
  - 网关只给工具补它声明了的范围参数；schema 拒收未知参数。由此堵上两个口子：Scope 只管列出的参数名（m1-notes），以及塞无关字段绕过同参数计数（m2-notes）。
- **GitHub 写工具**（`internal/githubtool`，go-github v92）：
  - `github.open_draft_pr`：
    1. 检查补丁，应用到镜像里的原文件。
    2. 经 Git Data API 建 tree、建提交（作者和时间固定）、建分支。分支已存在时，比较 tree 和父提交，不比较提交哈希。
    3. 按 head 分支查 PR（state=all），查不到才开草稿 PR；遇到 422 再查一次。
    4. PR 正文里没有模型写的文字。
  - `github.comment_commit`：
    - 按模板从 report_ref 渲染。
    - 模型和工作流给的文字全部放进代码围栏，围栏比其中最长的一串反引号还长。
    - 围栏外只有 agentd 自己核实过的内容：提交 SHA、按严重度的计数、PR 编号。PR 编号从本任务 open_draft_pr 的副作用记录里取，不用工作流传来的 pr_url。
    - 带隐藏标记 `<!-- agent-effect:<effect_id> -->`。查找时只认令牌本人账号发的评论。
- **补丁检查**（`internal/patch`，go-gitdiff + doublestar）。open_draft_pr 和 git.apply_check 共用这段代码，拒绝以下补丁：
  - 二进制改动、改名和复制、权限位变化；改完后不是普通文件（100644）的。
  - 路径经过 `.git`、`.gitmodules`、`.gitattributes` 的。
  - 路径不规范的，包括 `..`、绝对路径、反斜杠、控制字符。
  - 超出大小和文件数上限的。
  - 命中 deny_paths 的。deny_paths 不区分大小写，`makefile` 绕不过 `Makefile`。

  补丁按 hunk 写的位置严格应用，对不上就拒绝，不做模糊匹配。原文件用 `git ls-tree` 和 `git cat-file` 从镜像读，不跑过滤器、钩子或 diff 驱动，也不读用户和系统的 git 配置。不带 `diff --git` 头的普通 diff 去掉第一层路径（同 `git apply -p1`）。
- **dry-run**：
  - 用法：`agentctl runs start ... --dry-run`。是否 dry-run 记在 runs 表里，Worker 改不了。
  - 效果：写工具只做 Prepare，记成 DRY_RUN，返回带 `dry_run` 标记的结果。
  - agentd 没配 GitHub 时，GitHub 工具一律 dry-run，启动时打印警告。`agentd-claude.yaml` 不配 GitHub，配置测试断言了这一点：M4 之前，真模型和免审批的真实 GitHub 写入不同时出现。
- **循环保护**：每次调用记下它返回的状态。返回等待状态（needs_approval、needs_reconcile、budget_exhausted）的调用不计入模型调用次数和同参数次数，步骤数照算。以前等核对的任务每小时醒来重发一次，大约 3 小时后会被当成死循环拒绝。
- **配置**：
  - `repos` 的值可以写成镜像路径，也可以写成 `{mirror, github: owner/name}`；GitHub 名默认就是逻辑 ID。
  - 新增 `github` 一节：`api_url`、`token`（钥匙串引用；只有 api_url 是回环地址的模拟器才可以不填）、`request_timeout`（默认 30s）、`settle`（默认请求超时加 30s）、作者。
  - 默认作者 `agentos <agentos@agentos.invalid>`，这个地址不属于任何人。

### 实现中我定的几处（方案里没细说）

- **M3a 的 git.apply_check 已经做真实检查**：在 agentd 里用和 open_draft_pr 同一段代码检查补丁，并严格应用到镜像。代价：模型给的行号不准时，`git apply` 能靠偏移打上的补丁，这里会被拒。M3b 的 MCP 服务器要是用 `git apply --check`（容忍偏移），两边会不一致。M3b 开工时要定：两边都严格，还是都用 git。
- **恢复时在活动里等 Settle**，期间照常发心跳，不靠 Temporal 重试来等：工作流的重试策略最多 5 次尝试，退避加起来约 15 秒，等不到 60 秒。
- **副作用的 result_blob 现在只存工具的结果**。M1 存的是 `{status, value}`；状态改由副作用的状态决定。
- **模拟 GitHub 用 Go 写**：Go 单元测试直接用它，黑盒测试把它作为单独进程启动。它不校验令牌，只记录请求带没带令牌，Go 测试断言每个请求都带了。
- **T3 的 unsafe 工具 `test.unsafe_write`**：只在 `agentd --test-tools` 下注册，写的是数据目录里的一个本地日志。

### 推送前审查（安全审查 + 对抗性审查）发现并修掉的问题

两个审查代理都没改文件，下面的问题都核实过、已修，并补了测试：

1. **同一个核对幂等键可以生效两次**（高）。“未执行”之后重发，又一次结果未知，记录回到 UNKNOWN；这时 agentctl 重放同一个 key，会再触发一次重发。改法：核对结果按（effect_id, key）记进新表 `effect_resolutions`，同一个 key 再来只返回当初的结果；同一个 key 配不同结论直接拒绝。
2. **启动时的恢复会替已经结束的任务开 PR**（高）。例如重试次数在 GitHub 限流期间用完、任务已经发完失败报告，下次启动的恢复还是会重发。改法有三处：
   - 启动时的恢复只查不发：查不到就交还，由任务的下一次调用发；任务已结束就不会有下一次调用。
   - 查询出错直接转人工核对（UNKNOWN），不再无限重试。
   - 等待期间任务被取消，记 ABANDONED。
3. **领取之后的取消挡不住 open_draft_pr 后面的请求**（中）。这个工具要连发 7 个请求，取消只在领取时查一次。改法：工具在每个外人能看到的请求（建分支、开 PR、发评论）之前都调用 `proceed`。`proceed` 在同一条 UPDATE 里检查任务没被取消，并把 `claimed_at` 更新为这一刻，lookup 的等待从最后一个可见请求算起。
4. **Settle 没有下限**（中）。现在要求 settle ≥ request_timeout + 10 秒；回环地址上的模拟器只要求 ≥ request_timeout。测试改成 request_timeout 2 秒、settle 3 秒。另外，开 PR 时 GitHub 回 422 “already exists”、但查不到这个 PR，按结果未知处理，不再记 FAILED。
5. **工作流可以改参数发任意多条公开评论**（中）。参数每改一次就是一个新的副作用。改法：每个任务、每种写工具默认最多一次（策略的 `max_per_run`，可调大），按“已发出或可能发出”的记录计数，在插入副作用记录的同一个事务里判断。新测试 `tests/test_write_limits.py`。
6. **路径规则不够死**（低）。补丁路径里的 `:/app/db.py` 会被 git 当成 pathspec 魔法；`ſcripts/` 这类非 ASCII 字符能绕过大小写不敏感的匹配；还能在符号链接下面建文件。改法：
   - 路径各段只允许 `[A-Za-z0-9._+-]`。
   - git 命令加 `GIT_LITERAL_PATHSPECS=1`。
   - 父路径必须是目录。
   - 先用 `ls-tree -l` 查大小，超限的文件不读。
   - git 报错里的镜像路径替换成 `<mirror>`，因为报错可能经工作流进入公开报告。
7. **评论的边界情况**（低）。
   - 一长串反引号能把评论撑过 GitHub 的长度上限：现在反引号串最长截到 16 个。
   - 工作流可以把别的副作用的标记塞进围栏里的文字：现在查找只认最后一行的标记。
   - Unicode 格式字符（如双向覆盖符）也删掉。
8. **开 PR 时永远拒绝 `.github/**`**，不管策略怎么写：GitHub 自己会对这些文件采取行动。开发策略和测试策略也加上了 deny_paths；正式策略加了 `GNUmakefile`，GNU make 会先读它。
9. **我自查发现的一处窄窗口**：同一进程里刚领取成功、还没登记“正在发送”时，另一个执行者若绕过合并读到这条记录，会把它当成无人在发。改法：领取和登记放进同一把锁。正常路径不会走到这里，但 T16 的 Go 测试正是绕过合并来测的。

还有一处改动：之后的调用不再重新渲染载荷，一律沿用第一次记录的那份。否则这种情况会出错：PR 的副作用在两次评论调用之间结算，评论载荷因此变了，已经成功的评论被当成“载荷改变”而拒绝。

### 测试怎么测

- **T2**（`tests/test_t2_lookup_recovery.py`），四个场景，都只开出一个 PR 或一条评论：
  1. PR 已在模拟 GitHub 上开出、结果还没落账时 kill -9 agentd。重启后靠查找补记，不再发。
  2. agentd 领取后、请求发出前被杀。重启时查不到，等过 Settle 再查，仍没有就交还；由任务的下一次调用发出，只发一次。
  3. 报告评论已发出、结果没落账时被杀。重启后按标记找到它。
  4. 不杀进程，模拟 GitHub 开出 PR 但扣住答复，agentd 的请求超时。agentd 当场去查，补记这个 PR。
- **T3**（`tests/test_t3_reconcile.py`），三个场景，最后都只写了一次：
  1. 写入已到外部、结果没落账时被杀。重启后任务停在 WAITING_RECONCILE，核对列表里有这条记录和它的载荷；`--executed --result` 之后任务跑完。
  2. 领取后、发出前被杀。`--not-executed` 之后用原幂等键发出。
  3. 不合工具结果 schema 的结果被拒，记录保持 UNKNOWN。
- **每任务一次**（`tests/test_write_limits.py`）：恶意工作流发三条不同的报告，只有第一条发出，后两条返回 denied；策略写 `max_per_run: 2` 时发出两条。
- **T15**（`tests/test_t15_cancel_restart.py`），三个场景：
  1. PR 已开出、结果没落账时取消任务，再 kill -9 重启。PR 靠查找补记，取消之后模拟 GitHub 没收到任何写请求，也没发报告。
  2. 记录已建、还没领取时取消再重启。记录在启动时记成 ABANDONED，什么都没发。
  3. 同样的时机，取消但不重启。领取在同一个事务里看到取消，记成 ABANDONED。
- **T16**（`tests/test_t16_one_executor.py`），两个黑盒场景，外加一个 Go 测试：
  1. 第二个 agentd 在同一数据目录上起不来，第一个还在服务。第一个 agentd 那笔开着的预留，最后按它自己的用量结算，没有被第二个当成 `open_at_start` 全额结算。
  2. 第 1 次尝试领取后卡住且不发心跳，Temporal 开出第 2 次尝试，两者合到同一个执行上；接着 kill -9，重启后的恢复和第 3 次尝试也碰到这条记录。三方合起来只开出一个 PR。
  3. 领取用的比较并交换由 Go 测试验证：`internal/gateway` 的 `TestOnlyTheClaimantSends` 让两个执行者绕过合并、同时处理同一条记录，只有一个发出。
- **dry-run**（`tests/test_dry_run.py`）：带 `--dry-run` 的任务、没配 GitHub 的 agentd，都没往模拟 GitHub 写任何东西，载荷全文都在账本里。
- **已有测试的变化**：
  - 测试镜像改成两次提交，被审计的那次正好是桩 diff 的改动，假模型的补丁能打上。
  - `stack.external()` 改为看外部世界自己的记录：模拟 GitHub 的请求日志，或测试工具的日志。

### 留到后面

- **M3b**：只读 git MCP（show_diff、apply_check），每次调用起一个沙箱进程；真实测试仓库上的 T2，到时需要你建公开测试仓库和只授权它的细粒度令牌。
- **M4**：审批表（AWAITING_APPROVAL，拒绝和修改后的 ABANDONED）；门铃改走 outbox，现在核对后的门铃是直接发的。
- **其他**：
  - 同参数计数用的是工作流原样发来的参数，还没补范围参数。所以工作流可以靠写或不写某个范围参数，凑出同一调用的几个变体，最多 2³ 种。写操作有副作用去重和每任务次数上限，重复发也只算一次；读操作只是多读几次。
  - 等人核对时，工作流每小时醒一次，每次都算一个调用。总调用上限是 100，所以大约 4 天没人处理，任务就会被循环保护拒绝，以失败结束。之后再处理成“未执行”，记录会改成 ABANDONED，因为任务已经结束。
  - unsafe 工具没有 Settle。重启后，人可以马上判“未执行”；如果旧请求其实还在对方那里处理，就可能出现重复。lookup 工具会拒绝过早的“未执行”。
  - 同一进程领取的记录，如果请求还在途、尝试次数又用完了，在 agentd 重启前不会出现在核对列表里。
  - 模拟 GitHub 的建 tree 只是对请求求哈希，没有模拟 GitHub 合并 base_tree 的行为。M3b 在真实仓库上跑 T2 时会覆盖到。
  - 设计文档第 7 节策略示例的 deny_paths 还没有 `GNUmakefile`，`max_per_run` 和“启动时只查不发”也还没写进文档。这几处等你确认后再改文档。
  - 报告超过 60,000 个字符会被截断。
  - lookup 的等待时长依赖 GitHub 文档写的 10 秒处理上限。就算估错，这两个工具的后果也有限：迟到的建 PR 会被 422 挡住，评论最多多出一条。
