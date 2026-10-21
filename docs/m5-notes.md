# M5 记录：沙箱、评测与查错

M5 按设计文档第 12 节分成几个 PR，各自合并：沙箱定稿（C1、C2、C3）、哈希链审计日志（A）、`agentctl runs explain` 和 T12（B）、评测框架和评测集（D）、第一份评测报告（E）。D 和 E 在另一个会话里做。下面按合并的顺序记。

## C1：T9 的黑盒测试，收紧 Worker 沙箱，agentd 不走环境里的代理（2026-10-21）

验收：T9 写成黑盒测试（`tests/test_t9_isolation.py`），三种环境都通过，结果见“测试怎么测”。

### 做了什么

- **T9 的三组测试**（`tests/test_t9_isolation.py`）：
  1. **工作流任务的提交里夹带越权命令。**
     - 恶意工作流 `StepsOutOfLine`（`tests/rogue_workflows.py`）在第一个工作流任务里发一条 Worker 不许发的命令，具体发哪一种由任务的 base 分支决定：
       - 给别的任务发信号；
       - 取消别的任务；
       - 起子工作流；
       - continue-as-new；
       - 把活动排到别的任务队列；
       - 排别的活动类型；
       - 改名单外的搜索属性。测试先在 Temporal 里注册这个属性：agentd 一放行，它就会生效。
     - agentd 拒绝整个提交。测试等 agentd 日志里七种拒绝都出现，再核对四件事：
       - 受害任务的历史只有最初两条事件；
       - 没有叫 `t9-child` 的工作流；
       - 七个恶意任务的历史里，没有一条 WorkflowTaskCompleted，也没有这些命令的事件；
       - 网关一次调用都没收到。
  2. **Runtime 入口拒绝的方法。**
     - 直接对 Runtime 入口发 Worker 用不着的调用，共 17 个，全都得到 PERMISSION_DENIED：
       - 对任务：发信号、SignalWithStart、取消、终止、重置、更新、启动工作流；
       - 对队列和活动：轮询别的队列，领取或回答网关的活动；
       - 读别的任务：描述、列出、查询；
       - 带 deployment 选项的轮询和提交；
       - OperatorService 的方法。
     - 事后核对：受害任务没变，也没建出 Worker Deployment。
     - 带 deployment 选项的提交用的是编出来的 task token。Temporal 拿到它会自己报错，所以只要回的是 PERMISSION_DENIED，就说明是 agentd 先拒的。
  3. **沙箱里只够得着 Runtime 入口**（只在 srt、bwrap 下跑）。原来 `test_sandbox.py` 里的探针挪到这里，并加了几项：
     - 环境变量按白名单核对：
       - Linux：启动脚本设的四个，加上 sh 设的 PWD；
       - macOS：启动脚本设的那几个；srt 给每个命令设的一批代理和 git 变量；srt 跑命令用的 shell 设的三个；macOS 自己加的一个。
     - macOS：/tmp、/var/tmp 和用户临时目录（/var/folders 下）里放了诱饵文件，Worker 都读不到。
     - Linux：D-Bus 会话总线连不上；/dev 下只有五个设备。
     - 按名字查钥匙串条目单独成一个测试（srt）。钥匙串里没有 agentos 的条目时，这个测试显示为跳过，不会悄悄空跑通过。

  前两组拦东西的是 agentd 的 Runtime 代理，和操作系统的沙箱无关，所以三种环境都跑。
- **Runtime 代理按字段放行**（`internal/runtimeproxy/rules.go`）。
  - 起因是对抗审查发现的一个老问题：Worker 的轮询带上 `deployment_options`，Temporal 就会按 Worker 给的名字注册一个 Worker Deployment，并启动它的系统工作流。
  - 现在每种请求只放行 Python SDK（1.34）实际会设的字段。字段清单照代理上记下的真实流量列出。
  - 读历史和回答查询这两种请求，本身只读或只回答，不限字段。
  - Worker 报的版本信息只能是 build ID。
  - ShutdownWorker 只能提工作流队列。
  - 请求里任何一层出现这个 API 版本不认识的字段，一律拒绝：agentd 不知道 Temporal 会拿它做什么。
- **macOS 的两个启动脚本**：
  - Worker 的启动脚本整体拒读 /private/tmp、/private/var/tmp 和 /private/var/folders，做法和工具服务器一样。只放行 Python 环境、代码目录和 Worker 自己的私有目录，放行的路径同时写上它的真实路径。
  - `AGENTOS_SANDBOX_DENY_READ` 改成 JSON 数组，和工具服务器用的格式一样，路径里可以有任何字符。格式不对就拒绝启动。
  - 工具服务器的启动脚本也加上整体拒读 /private/var/tmp。
- **Linux 的两个启动脚本**：
  - /dev 只绑 null、zero、full、random、urandom 五个设备，不再给伪终端和 /dev/tty。伪终端的数量是宿主机上所有沙箱共用的。
  - Worker 的 /dev/shm 是只读根目录里的一个空目录。
- **agentd 出站不走环境里的代理**（`internal/outbound`、`cmd/agentd/main.go`）：
  - 启动时让 `http.DefaultTransport` 不读代理变量。Claude、OpenAI、GitHub 的客户端都直接用它，或者从它复制一份。
  - gRPC 连接（Temporal 客户端、Runtime 代理的后端）都加上 `grpc.WithNoProxy()`。
  - 环境里设了代理变量时，打一条警告把它们列出来。
  - 默认 transport 被别的库换成了别的类型时，拒绝启动。

### 我定的几处（文档没细说）

- **T9 的命令和方法两组在三种环境都跑**：
  - 拦它们的是 agentd。不加沙箱的 Worker 同样经 Runtime 入口连 Temporal，所以这两组在三种环境都有意义。
  - 只有操作系统层面的那组需要 srt 或 bwrap。
- **环境变量用白名单**：
  - 这比“看不到某个金丝雀变量”更严：srt 升级后多出一个变量，测试就会失败，逼人去看一眼。
  - srt 给的代理变量里带着这次会话的凭据。它只通到 Runtime 端口，不算密钥。
- **字段白名单照实际流量列**：
  - 这比只挡几个已知的危险字段更严：以后 API 新加的字段默认就被拒。
  - 代价是升级 Python SDK 时，新版本只要多设一个字段，Worker 就连不上。好在升级时跑测试就会暴露。
  - `query_results`（工作流任务上捎带的查询的答案）在测试里没出现过，但 SDK 遇到这种查询时会发，所以也放行。
- **不加代理配置项**：
  - v0.1 只有作者一个用户，机器能直连外网。
  - 在只能经代理出网的网络里，模型和 GitHub 的请求会失败。启动警告和 README 都写明了这一点。
  - 真有需要时，再加一个显式的 `proxy` 配置项，但仍不读环境变量。
- **securityd 挡不掉**：
  - srt 的 Seatbelt 配置写死了放行 com.apple.SecurityServer 和 com.apple.securityd.xpc，设置里没有开关能去掉。
  - 安全审查用临时钥匙串实测过：沙箱读不到钥匙串文件时，按名字也查不到条目；把钥匙串复制到沙箱读得到的地方，也只会弹出解锁提示，拿不到密码。
  - 真正起作用的是文件拒读。
- **测试数据放在 /Users/Shared**：
  - /var/tmp 也整体拒读之后，要单独测 agentd 自己算出的拒读列表，测试数据就得放在整体拒读的范围之外。
  - 测试经一个符号链接去访问这份数据，因为 Seatbelt 按真实路径匹配，这样能顺带测到真实路径的处理。
  - Linux 只挂载给定的目录，那边仍然用 /var/tmp。

### 推送前审查（安全审查 + 对抗性审查）发现的问题

两个审查代理各自在自己的 worktree 里做实验，没有改我的文件。

1. **带 deployment 选项的轮询能让 Temporal 启动系统工作流**（中，对抗审查实测，老问题）。
   - 问题和改法见“做了什么”里“Runtime 代理按字段放行”那一条。
   - 补的测试：Go 测试，加上 T9 里的两个调用（轮询和提交各一个）。
2. **Worker 读得到同一队列里所有任务的历史**（中，安全审查实测，老问题）。
   - 原因：`GetWorkflowExecutionHistory` 不限定读哪个任务。
   - 审查代理建议按连接限定，但这挡不住：所有任务共用一个工作流队列，Worker 本来就要执行每个任务的工作流任务，Temporal 交给它的工作流任务里就带着那个任务的历史。
   - 要真正隔离，得每个任务配一个 Worker 和一个队列，那是 v0.2 以后的事。
   - 处理：代码没改，T9 的说明里写清楚了，并记进“留到后面”。
3. **搜索属性那一步测不出 agentd 拦没拦**（低，对抗审查改坏后测试仍通过）。
   - 原因：属性没注册时，Temporal 自己就会拒，agentd 放没放行都一样。
   - 改法：测试先注册 T9Elsewhere。
4. **macOS 的检查依赖 TMPDIR**（低，对抗审查实测）。
   - 原因：TMPDIR 指到 /tmp 下面时，/var/folders 根本测不到。
   - 改法：改用 `getconf DARWIN_USER_TEMP_DIR` 取用户临时目录，每个路径只放一个诱饵。
5. **工具服务器的 /dev 改动没有测试**（低，改坏后测试仍通过）。
   - 改法：工具沙箱的探针也报告 /dev，bwrap 下断言只有五个设备。
6. **gRPC 不走代理没有黑盒测试**（低，改坏后测试仍通过）。
   - 原因：测试里的地址都是回环 IP，Go 本来就不会让它们走代理。
   - 改法：`tests/test_outbound.py` 把 Temporal 的地址写成 `localhost.`。这个带点的名字解析到本机，但 Go 照样会让它走代理。代理变量指向一个专门记录连接的端口，跑一个 dry-run，事后确认这个端口一个连接都没收到。
7. **按名字查钥匙串会空跑通过**（低，安全审查）。
   - 改法：单独成一个测试，没有条目时显示为跳过。
8. **默认 transport 的类型断言没有检查**（低，安全审查）。
   - 改法：不是 `*http.Transport` 就拒绝启动。
9. **/var/tmp 仍然可读**（提示，对抗审查）。
   - 改法：两个 macOS 启动脚本都把它整体拒读。
10. **只能经代理出网时，失败得很晚**（低，对抗审查）。
    - 处理：不加配置项，在警告和 README 里写明（见“我定的几处”）。
11. **拒读路径里的通配符**（提示，安全审查）。
    - srt 会把路径里的 `* ? [ ]` 当成通配符。agentd 自己的路径里没有这些字符，先记下。

### 复审（两个审查代理复查上面的修复）发现并修掉的问题

1. **心跳里的 `deployment_version` 漏过了字段白名单**（中，安全复查实测）。
   - RecordWorkerHeartbeat 和 ShutdownWorker 里的 `worker_heartbeat` 是整个放行的，它里面的 `deployment_version` 可以带着一个 deployment 名字送到 Temporal。在用的 dev server（1.32）上没有看到效果，但这违背了“只放行 SDK 设的字段”这条规矩。
   - SDK 自己的心跳里本来就有 `deployment_version`，只是只带 build ID（我实测过）。所以不能整个拒掉这个字段，否则心跳和关停都会坏。
   - 改法：心跳里的版本信息也只准带 build ID，和别处的版本信息同一条规则。两种请求各补一个 Go 测试，改坏后测试会失败。
2. **活动的 `priority` 没有限制**（低，对抗复查）。
   - SDK 发的每个活动命令都带一个空的 `priority`（我实测过）。被攻破的 Worker 如果填上优先级或公平性键，就能让自己任务的调用在共用的网关队列里插到别的任务前面。
   - 改法：非空的 `priority` 一律拒绝。顺带拒掉 `use_workflow_build_id`，SDK 不设它。
3. **拒绝未知字段的检查里，map 那个分支没有测试**（低，改坏后测试仍通过）。
   - 改法：补测试，在查询结果和活动 header 的 payload 里各放一个未知字段。
4. **T9(a) 靠时序**（低，对抗复查）。
   - agentd 先写拒绝日志、后回复。如果有人改成“记下日志又照样转发”，测试能不能抓到，取决于转发是否恰好比测试读历史早一步。
   - 改法：等每个恶意任务的第一个工作流任务超时或失败之后再核对。如果 agentd 把提交转发了，这里出现的会是 WorkflowTaskCompleted。
5. **outbound.go 的一句注释不对**（提示，对抗复查）。
   - agentd 用的几个客户端都是在发请求时才去取默认 transport，并不是在建客户端时复制。
   - 改法：注释改成“直接用它或者用它的副本”。
6. 复审确认成立、不改的几条：
   - 关于审查第 2 条（Worker 读得到同一队列里所有任务的历史），两个复审都认为推理成立。
   - 读历史、回答查询这两种不限字段的请求：升级 API 时要看看新增的字段，`rules.go` 的注释里写明了。
   - 字段白名单只照 SDK 1.34 列出：以后升级 SDK，只要它多设一个字段，Worker 就会全部连不上，好在会很明显。
   - 测试中途崩掉的话，会在 /Users/Shared 留下一个 `agentos-probe-*` 目录，重启后也还在。
7. 一处事故：对抗审查代理误跑了共享 scratchpad 里我的改坏脚本，在我的 worktree 里执行了 `make build`，没有改源码。当时正在跑的 macOS 不加沙箱套件因此作废，最终结果以下面在定稿代码上的重跑为准。之后脚本放进各自的目录。

### 测试怎么测

- **新测试**：
  - `tests/test_t9_isolation.py`，四个测试：
    - 工作流任务够不着别的任务（三种环境都跑）；
    - Runtime 入口拒绝的方法（三种环境都跑）；
    - 沙箱里只有 Runtime 入口（srt、bwrap）；
    - 按名字查钥匙串（srt）。
  - `tests/test_outbound.py`：agentd 经 `localhost.` 连 Temporal，代理什么都没收到。
  - Go 测试：
    - `internal/runtimeproxy/rules_test.go`，覆盖 SDK 自己发的请求、SDK 从不设的字段、不认识的字段（顶层、命令里、map 的值里）、优先级、带版本的活动；
    - `internal/outbound/outbound_test.go`，确认请求不走环境里的代理，默认 transport 被换掉就拒绝启动。
- **改动的老测试**：
  - `test_sandbox.py`：探针挪走了；srt 的拒读列表测试改在 /Users/Shared 下做；不再排除 devpts。
  - `test_tool_sandbox.py`：在 /Users/Shared 下起栈；报告 /dev；在 /var/tmp 和用户临时目录里放诱饵。
- **字段白名单从哪来**：调试版 agentd 把每个请求设了哪些字段打到日志里，然后跑了 T1、T3、T4、T7、T10、取消、agentd 重启、Runtime 上限、模型输出这几组测试。结果是没有一个未知字段，心跳里的版本只带 build ID，活动命令都带一个空的 priority。
- **三种环境的结果**（代码定稿于 baf741e，之后只改了这份记录）：

  | 环境 | 通过 | 跳过 |
  |---|---|---|
  | macOS 不加沙箱 | 69 | 28 |
  | macOS srt | 92 | 5 |
  | Linux 虚拟机 bwrap | 81 | 16 |

  跳过的都是另一种环境专属的用例，以及要真 GitHub 的 live 用例。srt 那一轮额外跑了要 `AGENTOS_TEMPORAL_SERVER` 的 mTLS 测试。`go test ./...` 全部通过。
- **改坏检查**：一共改坏 19 处，每一处都有测试失败。
  - 第一轮 15 处：
    - 两处 `grpc.WithNoProxy` 拿掉；
    - checkFields 什么都放行、未知字段放行、版本字段不查、ShutdownWorker 放行任意队列、任意搜索属性；
    - Worker 启动脚本的三项整体拒读各拿掉一项，拒读列表不看；
    - 工具启动脚本的 /var/tmp、/var/folders 两项整体拒读各拿掉一项；
    - 两个 Linux 启动脚本各自恢复 `--dev /dev`。
  - 复审之后又改坏 4 处：
    - 心跳里的版本不查；
    - 未知字段检查跳过 map；
    - 活动优先级放行；
    - `use_workflow_build_id` 放行。
  - `outbound.go` 里 `Proxy = nil` 那一行去掉，只有 Go 测试会失败，因为测试里的 HTTP 目标都在回环地址上。这一处在最开始写的时候就验证过了。

### 留到后面

- **Worker 读得到同一队列里所有任务的历史**（审查第 2 条），而且能把读到的东西带出去：
  - 渠道是自己任务报告的 outcome。outcome 是工作流自己写的文字，GitHub 评论里显示前 2,000 个字符，所以被攻破的 Worker 能经 GitHub 把数据带出去。
  - v0.1 只审一个公开的测试仓库，眼下影响有限。
  - 如果 outcome 改由 agentd 从自己的账本生成，这条渠道就没了。是否这样改由用户定，见 B。
- **securityd 挡不掉**：实测拿不到密码，见“我定的几处”。
- **macOS 的资源上限**：Worker 的私有目录不限大小，能写满磁盘；macOS 上也没有内存上限。
- **只能经代理出网的网络不支持。**
- **拒读路径里的通配符**：见审查第 11 条。
