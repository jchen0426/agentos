# M4 记录：审批

2026-10-20。验收 T4、T5、T6、T14 写成黑盒测试，三种环境都通过（结果见“测试怎么测”）。设计文档第 10 节的全部内容都做了；第 5 节第 7 项和 M2a、M3a 留下的“门铃先直接发信号，到 M4 改走 outbox”也一起做了：批准、核对和追加预算的门铃都走同一个 outbox。

## 做了什么

- **审批表**（迁移 `00008_approvals.sql`）：00001 里的 approvals 表从没写过一行，重建成审批真正要的样子：所属任务和工具、调用的规范化参数、副作用记录的载荷哈希、审批单上下文（blob）、状态（PENDING、APPROVED、REJECTED、EXPIRED、EDITED、SUPERSEDED，带 CHECK）、建立和到期时间、谁在何时决定、决定的幂等键、拒绝理由、修改后的补丁和修改后调用的参数哈希、取代关系、批准后被用掉的时间。一条副作用记录最多一张审批单（`effect_id UNIQUE`）。SQL 要比较的时间（到期、门铃何时再试）用定宽格式写（`ledger.Stamp`，九位小数），按文本排序就是按时间排序；`ledger.Now` 会去掉末尾的 0，不能直接比较。
- **触发**：策略写 `require_approval` 的写工具，第一次调用时渲染并记录载荷（M3 的协议），副作用记录写成 AWAITING_APPROVAL，同一个事务里建审批单，返回 `needs_approval`（带审批单 ID 和到期时间）。工作流照旧把 AgentState 设为 WAITING_APPROVAL，等门铃或 24 小时定时器。只读工具不能要求审批，agentd 启动时就拒绝这样的策略（审批卡的是副作用，读没有副作用）。
- **绑定**：审批单记下副作用记录的载荷哈希。批准之后，网关在一个事务里核对：审批单还是 APPROVED、没被用过、没过期，任务没被取消，副作用记录还在 AWAITING_APPROVAL，而且记录下来的载荷 blob 重新算出的 sha256 等于审批单绑定的那个，然后才改成 PENDING，之后按 M3 的协议领取、发送、恢复。差一个字节就不发：副作用记为 ABANDONED，原因写明两边的哈希，任务得到 `denied`，agentd 记一条 error 日志。执行时发出的就是记录下来、审批过的那份载荷，不重新渲染（文档第 10 节“绑定”，rev 272）。工作流换了参数，就是另一条副作用记录，要另一张审批单，或者被每任务一次的上限拒绝；所以一张审批单只放行它绑定的那一份载荷。
- **人看到的哈希也要对**：每个决定都要带审批人看到的载荷 sha256（`--payload-sha256`），和审批单的对不上就不算数；决定的事务里还要再算一遍记录下来的载荷，载荷在记录之后被改过也不算数。`approvals show` 也先核对，对不上就报错，不把被改过的载荷当成待批的展示出来。
- **过期**：以审批表的 `expires_at` 为准（策略的 `approval_ttl`，默认 24 小时，只能和 `require_approval` 一起写）。agentd 有一个后台循环，到时间就在一个事务里把审批单改成 EXPIRED（决定人记 agentd，原因“到期前没人决定”）、副作用记录改成 ABANDONED、写一条门铃；启动时先处理 agentd 停着的时候到期的。新审批单建立时唤醒它，没有待批的就每分钟看一次。调用和决定也各自检查到期：过期按拒绝处理，任务得到 `expired` 和原因。工作流的 24 小时定时器只是唤醒，比它短的 TTL 也照样准时过期，因为 agentd 到期时会自己敲门铃。
- **三种决定**：
  - 批准：门铃唤醒任务，它重发同一个调用，按上面的绑定核对后发出。
  - 修改后批准（`agentctl approvals edit --patch-file`）：补丁存成这个任务的 blob，审批单记为 EDITED。任务对原调用的重发得到 `edited` 和新的 `patch_ref`，工作流照设计文档第 7 节的代码重新 `git.apply_check`，再用新补丁调用 `open_draft_pr`。那是一条新的副作用记录，要一张新的审批单；新审批单建立的那个事务里，原审批单改为 SUPERSEDED，原副作用记录改为 ABANDONED，然后才数这个任务的写操作，所以 `max_per_run: 1` 仍然放得下修改后的那一个。只有参数正好是“原调用换上审批人的补丁”的调用才算修改后的调用；工作流拿别的补丁来，原副作用还占着名额，被拒绝。
  - 拒绝：必须写理由（可打印文字，最多 2000 字节），副作用记录同一个事务里改为 ABANDONED，任务得到 `rejected` 和理由。
- **报告里的审批**：`github.comment_commit` 渲染报告时从 agentd 自己的审批表读这个任务的审批，像 PR 编号一样不信工作流传来的东西。围栏外只写 agentd 自己记录的状态（“Approval of github.open_draft_pr: rejected”），谁决定的、拒绝理由这些人写的文字放进围栏，链接和 @提及不生效。
- **并发**：决定在一个 SQLite 事务（BEGIN IMMEDIATE）里判定，`UPDATE ... WHERE status = 'PENDING'`，先到的有效决定生效。后到的得到 FAILED_PRECONDITION，消息里写着“已经由谁在何时作了什么决定”。同一个幂等键再发一次同样的决定，返回当初记下的结果，不算冲突；同一个键配别的决定，拒绝。到期也是一种决定：过期之后再批准，得到“already decided: EXPIRED by agentd at …”。
- **审批单内容由 agentd 生成**（`agentctl approvals show`，Control 的新接口 `GetApproval`）：
  - 写操作本身：`open_draft_pr` 的 `Describe` 给出目标仓库、base 分支、新分支名、被审计的完整提交 SHA、补丁全文、改动的文件和改法（修改、新建、删除）、git MCP 服务器在沙箱里跑的 `git apply --check` 结果；还有 GitHub 上此刻的情况：base 分支的头、被审计提交和它比较的结果（落后几个提交、在不在 base 分支的历史里），以及 Execute 写之前要做的两处核对（补丁涉及的文件在被审计提交上、在 base 的头上）此刻的结果。这些都是只读请求，也写明了“提交建在发送时的头上，补丁涉及的文件在头上变了就不推”和“推分支会触发 push、create 的工作流”。
  - 任务的上下文（`tasks.Task.ApprovalContext`，D12：任务配置决定）：安全审计给出这个任务被接受的那次审计的摘要和发现列表（严重度、CWE、文件和行号、标题）。
  - 将发出的完整载荷和它的 sha256、到期时间、状态和决定记录，以及本任务此刻已花、已预留的钱和上限（展示时现查）。
  - 文字里所有不可打印的字符都转义（新的 `safetext.Text` 保留换行和制表符，补丁照原样可读）。上下文按键名排序、缩进打印，agentctl 不认识任何一种任务的字段。
- **门铃 outbox**（`internal/outbox`，新表 `outbox`）：决定、到期、核对、追加预算的事务里各写一条门铃，提交之后唤醒 outbox。outbox 按任务合并，一个信号覆盖它之前写的所有门铃，发给 agentd 启动的那个执行（workflow ID 加 run ID）。Temporal 收下就记 delivered；执行已经结束（NotFound）记 run_ended，不再试；别的失败按 1、2、4…秒退避重试，最长一分钟一次，直到送达。agentd 崩溃留下的门铃，下一个 agentd 启动就发。M2a 的追加预算和 M3a 的核对原来直接发信号，现在都走它，重复的请求不再补发（门铃不会丢了）。
- **入口**：Control socket 上 `ListPending` 列出待批的审批单（已取消任务的不列，过期的不列），新加 `GetApproval`，`ResolveApproval` 做三种决定；命令行 `agentctl approvals list | show | approve | edit | reject --reason`，决定都要 `--payload-sha256`，审批人名字默认是当前用户，`--as` 可以换。
- **开发策略**：`config/dev/policy.yaml` 的 `open_draft_pr` 改成需要审批。开发配置都不配 GitHub，GitHub 工具只 dry-run，不受影响；配了 GitHub 之后，真模型和真实 GitHub 写操作之间总有人看过补丁。配置测试改成断言这一点。

## 我定的几处（文档没细说）

- **dry-run 不要审批**：任务是 dry-run，或者 agentd 没配 GitHub（工具只能 dry-run），需要审批的写操作照旧只渲染、记录成 DRY_RUN，不建审批单。什么都不发，没有东西要批；评测要用 dry-run 跑（第 11 节），不能每次都停下来等人。
- **修改后的调用要再批一次**：审批人改的是补丁，可审批绑定的是完整载荷（PR 标题、正文、分支名、每个文件改后的内容和原来的 blob），这些在工作流重新校验、agentd 重新渲染之前审批人都没见过。所以新审批单是 PENDING，要再批准一次，审批人批的始终是他看过的那份载荷。
- **改了等于没改的修改不收**：补丁和原来的一模一样，修改后的调用就是原调用，原调用又得到 edited，工作流会一直转圈。agentd 直接拒绝这样的修改，让审批人改为批准。补丁也要能被解析、不超过 256 KiB；是否越界、能否打上，交给工作流的 `git.apply_check` 和 `open_draft_pr` 自己的检查（validate_patch 那两道）。
- **批准了却没用上的也会过期**：文档第 6 节说“确认审批已批准、未过期”在同一个事务里，我照字面做：批准之后任务在到期前没有重发调用（比如 Worker 停了一天），到期后审批作废，记为 EXPIRED，原因写明是谁在何时批准的。
- **审批人的名字由 agentctl 给**：Control socket 在 0700 目录里，能连上的就是操作者本人，v0.1 是单用户；agentctl 默认填当前用户名，`--as` 可以写别人。名字要是可打印文字、不超过 100 个字符，`agentd` 保留给 agentd 自己的决定（过期）。
- **冲突用错误表示**：proto 里 `ResolveApprovalResponse` 原注释是“冲突时返回之前的决定”。我改成 FAILED_PRECONDITION 错误，消息里写清之前的决定、谁、何时：agentctl 冲突时以非 0 退出，脚本和人都不会把别人的决定当成自己的。
- **已取消、已结束的任务**：取消之后网关拒绝这个任务的一切调用，审批了也发不出去，所以 `approvals list` 不列已取消任务的审批单，对它的决定直接拒绝；Temporal 里已经结束的任务（`runs get` 是 SUCCEEDED、FAILED、CANCELLED、UNKNOWN）同样拒绝决定。所以做决定时要能连上 Temporal，和 `reconcile resolve` 一样。
- **审批单在建立时生成并存下**，展示时只现查花费：审批单记的是“出审批单时”看到的样子（文档第 10 节），之后 base 分支怎么动，Execute 发送前都会再核对。生成审批单要读 GitHub、起一次 git MCP 服务器，这些读之前先数这个任务的写操作，已经到上限的调用直接拒绝，被攻破的工作流不能靠一遍遍换参数耗掉令牌的 API 配额。
- **门铃按任务合并、按执行发**：一个信号唤醒任务一次就够，工作流醒来后以 agentd 的表为准；信号带上 run ID，发不到同一个 workflow ID 下别的执行。

## 和父会话任务说明不一致的一处

任务说明写的是“执行前 agentd 重新渲染载荷并比对，差一个字节就拒绝执行”。设计文档 rev 272 已经改成“执行时发出的就是记录下来、审批过的那份载荷，不重新渲染”，M3a 的代码也是这样（重新渲染会把已经成功的评论当成载荷变了，见 m3-notes）。我照设计文档做：不重新渲染，执行前核对的是“要发出的那份载荷的哈希等于审批过的哈希”，差一个字节同样拒绝执行（T5 第一个用例）。

## 测试怎么测

- **T4**（`tests/test_t4_approval_restart.py`），两个场景：
  1. 任务停在 WAITING_APPROVAL 时 kill -9 agentd、重启 Temporal 开发服务器（测试框架给它一个数据库文件，`restartable_stack`，原来是内存的，重启就丢）、再起 agentd，Worker 不重启。审批单还在，ID 和载荷哈希不变；模拟 GitHub 没收到任何写请求。审批单的内容逐项核对：载荷的 base 和被审计提交、补丁全文、改动的文件、`git apply --check` 通过、base 的头和“identical、落后 0、在 base 历史里”、两处核对都是 ok、发现列表、已花的钱。批准之后任务跑完，只开出一个 PR，报告里写着这次批准；历史里只有一个信号，outbox 里那条门铃记 delivered。
  2. 审批 8 秒过期，到期时 agentd 正停着：它启动后立刻把审批单记为 EXPIRED（agentd，“到期前没人决定”），任务被门铃唤醒、得到 `expired`、发报告（报告写明过期和原因）、以 SUCCEEDED 结束，没有 PR，副作用记录 ABANDONED。之后再批准，得到“already decided: EXPIRED by agentd”。
- **T5**（`tests/test_t5_payload_binding.py`），四个场景：
  1. 批准时把门铃扣住，停掉 agentd，把账本里记录的载荷改一个字节（`security fix` 改成 `security fiX`），再起 agentd：门铃送达，任务重发，网关拒绝发送，副作用记录 ABANDONED、原因写明两边的哈希，任务得到 `denied` 并发报告，GitHub 没收到任何写请求；`approvals show` 也拒绝展示这份载荷。
  2. 审批人用自己的补丁修改：原审批单 EDITED，工作流对审批人的补丁重新跑了 `git.apply_check`（历史里它排在用新补丁开 PR 的调用之前），出现第二张审批单，取代第一张（SUPERSEDED），新审批单的载荷里是审批人的改法；批准第二张后 PR 开出，GitHub 收到的树里只有审批人的修复、没有模型的；第一条副作用 ABANDONED。
  3. 审批人的补丁改了 `.github/workflows/`：工作流的 `git.apply_check` 拒绝，报告写 `patch rejected`，没有第二张审批单，没开 PR。
  4. 恶意工作流（`tests/rogue_workflows.py` 的 `SwapsThePatch`）：让审批人修改、批准修改后的补丁，然后不用批准的那份，改用模型自己的补丁再调用：得到 `edited`，什么都没发出去；第二张审批单是 APPROVED 但没被用过。
- **T6**（`tests/test_t6_concurrent_decisions.py`）：四个任务同时等审批，每张审批单同时起两个 agentctl，一个以 alice 批准、一个以 bob 拒绝。每张都只有一个成功，另一个以非 0 退出，错误里写着先到的决定、谁、何时；账本里的状态就是先到的那个。各自用原来的幂等键再发一次：先到的那个照样成功，后到的照样冲突。之后任务按先到的决定走：批准的开出一个 PR，拒绝的不开 PR，报告里有 bob 的理由。
- **T14**（`tests/test_t14_doorbell_outbox.py`），两个场景：批准（和追加预算）的门铃扣在 outbox 里，确认它没送出、任务还在睡，kill -9 agentd，再起一个：门铃送达，任务跑完，历史里一个信号，outbox 记 delivered。工作流自己的定时器是 24 小时（预算是 1 小时），测试的等待只有 60 秒，所以唤醒任务的只能是门铃。
- **其他黑盒测试的变化**：`test_cancel.py` 等审批时取消，现在还核对审批单不再列出、对它的批准被拒绝；`test_dry_run.py` 加了“dry-run 在需要审批的策略下也不建审批单”。测试框架加了 `approval_policy()`、`restart_temporal()`、扣住门铃的故障（`hold_doorbells_while_exists`）和读模拟 GitHub 评论的辅助函数；模拟 GitHub 的日志记下建树请求里的文件内容，测试才能看出 PR 里是谁的修复。
- **Go 测试**：`internal/gateway/approvals_test.go`（批准前一直 needs_approval、只有带对哈希的决定算数、批准后只发一次；拒绝要理由；16 个决定同时到只有一个生效，其余被告知先到的决定，同键重发；三种过期；记录的载荷被改一个字节就不发、也批不了；修改、取代、只有修改后的那个调用能取代、批准后原调用照样得到 edited；不收改了等于没改的修改和不能修改的工具；已取消任务；只读工具不能要求审批），`internal/outbox/outbox_test.go`（每个任务一个信号、失败退避重试且不挡别的任务、执行已结束就丢弃、留到下一个 agentd 发、Poke 立即唤醒），`internal/githubtool`（审批单的 Describe：base 前进并改了补丁涉及的文件时，审批单写明落后 1 个提交、头上的核对被拒；报告里的审批：状态在围栏外、谁和理由在围栏里），`internal/policy`（approval_ttl），`internal/safetext`（Text）。
- **改坏检查**：对 Go 代码逐条改坏 21 处（转 PENDING 时的哈希、到期、取消检查；决定时审批人的哈希、记录的载荷、先到者生效、门铃、拒绝要理由、拒绝时放弃副作用、改了等于没改；到期的门铃；取代原审批单；outbox 的丢弃、扣住、退避；审批单的落后数；报告里的理由；approval_ttl 只配 require_approval；只读工具不能要求审批），每一处都有用例失败。

三种环境的结果：（待补。）

## 推送前审查

（待补。）

## 留到后面

- **已经结束的任务的审批单**：取消的不列，但 Temporal 里已经结束（失败、被终止）的任务，审批单会一直列到过期；对它的决定会被拒绝（“the run has ended”）。要在列表里也排除，`ListPending` 得逐个问 Temporal。
- **做决定要连得上 Temporal**：为了拒绝对已结束任务的决定，`ResolveApproval` 先问 Temporal 任务的状态，和 `reconcile resolve` 一样；Temporal 不可用时决定报 Unavailable，稍后重试。
- **审批人是自己报的名字**：v0.1 单用户，Control socket 在 0700 目录里。多人共用一台 agentd 时（v0.2 的团队部署），要按连接认人（Unix socket 的对端凭据，或者 mTLS 的证书主体），审计记录里才有可信的“谁”。
- **修改后的补丁 agentd 只做语法检查**：越界、打不打得上，交给工作流的 `git.apply_check` 和 `open_draft_pr` 自己的检查。被攻破的工作流可以跳过 `apply_check`，但跳不过 `open_draft_pr` 在 agentd 里的检查，审批人还会在第二张审批单上看到 `git apply --check` 的结果。
- **改过、但修改后的调用一直没来的审批单**：停在 EDITED，原副作用停在 AWAITING_APPROVAL，永远不会发出，也一直占着这个任务这种写操作的名额。
- **策略中途改成需要审批**：改之前已经记成 PENDING 的副作用，是在旧策略下记下的，之后照样发出，不会补一张审批单。
- **审批疲劳**（第 10 节最后一项）：v0.1 只有开 PR 一种审批，批准率和平均停留时长还没记；以后加第二种审批时一起做。
- **审批单是建立时的快照**：base 分支之后再动，审批单上的“落后几个提交”“头上的核对”不会更新；Execute 发送前会再核对一遍，对不上就不推、交给人。
- **门铃只要任务还在就一直重试**（最多每分钟一次）：Temporal 长时间不可用时，outbox 里会攒下这段时间的门铃，送达时按任务合并成一个信号。
