# RD Download Engine V1.0 — 架构设计文档

## 1. 设计总纲

**最高原则：引擎与 UI 彻底解耦。** 引擎是一个纯库 crate（`rd-engine`），不包含任何
终端或图形界面代码；未来的 CUI/TUI 与 Slint GUI 只通过 `Engine` 提供的异步公共 API
与事件流交互，永远不直接触碰 Worker、分段文件或数据库内部实现。

核心权衡取舍：

- **正确性优先于吞吐**：分段边界校验、checkpoint 先刷盘再记库、提交前 SHA-256，
  宁可慢也不能让"数据库显示完成而文件实际不完整"。
- **崩溃安全**：任何时刻 kill -9，重启后任务状态都必须能从磁盘 + SQLite 一致地恢复。
- **运行时不阻塞**：所有文件 I/O 与 SQLite 调用都在 `spawn_blocking` 上执行；
  调度器是单任务顺序状态机，内部无锁竞争。
- **不做提前优化**：单文件模块化（不拆 workspace 子 crate）、一个 SQLite 连接、
  broadcast 事件——满足 V1.0 规模，复杂度最低。

## 2. Cargo Workspace 与模块职责

```
crates/rd-engine          下载引擎库
crates/rd-cli             最小演示二进制（仅调用公共 API）
```

`rd-engine` 内部模块与职责对应表：

| 模块 | 职责 | 提示词要求项 |
|------|------|-------------|
| `engine.rs` | 公共门面：命令通道封装、事件订阅、生命周期（open/shutdown） | 公共 API |
| `scheduler.rs` | 任务调度状态机：命令处理、队列、并发上限、速度统计、事件发射 | 任务调度 |
| `runner.rs` | 单任务运行器：探测、分段规划、动态分段调度、自适应并发控制、远端变化重启、收尾提交 | 分段下载管理 / 动态调度 / 断点续传 |
| `worker.rs` | 分段 Worker：Range 拉取、Content-Range/If-Range 校验、限速接入、checkpoint | 下载 Worker |
| `http.rs` | reqwest 客户端构建、HEAD→GET 探测、头解析、文件名提取 | 网络请求与 HTTP 管理 / 资源探测 |
| `storage.rs` | PartFile 定位写入、原子提交、SHA-256/BLAKE3、文件删除 | 文件读写管理 |
| `limiter.rs` | GCRA 限速器（全局/单任务实例） | 速度限制器 |
| `db.rs` | SQLite（WAL）任务/分段持久化，阻塞池执行，schema 迁移 | SQLite 持久化 |
| `config.rs` | TOML 配置与校验 | 配置管理 |
| `model.rs` | TaskState 状态机、Segment 分段算法、Event 模型 | 状态与事件 |
| `error.rs` | thiserror 结构化错误 + 可重试判定 + 失败分类（自适应控制器输入） | 错误处理 |
| `util.rs` | 文件名净化、唯一路径、退避、Retry-After | 公共工具 |

## 3. 并发模型与数据流

```
UI (clone of Engine)                 引擎内部
     │  Command (mpsc)                   ┌─────────────────────────┐
     ├──────────────────────────────────► │ Scheduler (单任务状态机) │
     │  reply (oneshot)                   │  entries: HashMap<Id>   │
     ◄────────────────────────────────── │  queue:  VecDeque<Id>   │
     │                                    └───────┬────────┬────────┘
     │  Event (broadcast)              RunSpec │        │ RunnerMsg (unbounded)
     ▼                                        ▼        ▼
  所有订阅者                            tokio::spawn(runner::run)
                                              │ JoinSet
                                              ├── worker #0 (segment 0)
                                              ├── worker #1 (segment 1)
                                              └── worker #N (segment N)
```

- **命令流**：`Engine` 的每个公共方法把请求封装成 `Command` 发到 mpsc 通道，
  Scheduler 单线程顺序处理并经 oneshot 回复——天然避免竞态，不需要内部锁。
- **消息流**：Runner/Worker 通过无界通道上报状态迁移、进度、重试、完成结果；
  Scheduler 统一落库、更新内存快照、发射事件。
- **事件流**：`tokio::broadcast`（容量 1024）。慢订阅者会收到 `Lagged`，
  因此 UI 必须能通过 `get_task`/`list_tasks` 重新同步——这是有意为之的
  "事件做通知、状态做事实源"模式。
- **停机协议**：`CancellationToken + AtomicU8 停止原因`。暂停/取消/关机都先置原因
  再取消 token；Worker 在每个 await 点（请求、流读取、限速、退避睡眠）检查 token，
  保证取消的及时性（亚秒级）。暂停时 Scheduler 会等 Runner 确认退出后才回复调用者，
  reply 经 `entry.pending` 缓存。

## 4. 任务状态机

```
            add(auto_start)      队列启动
  Pending ───────────► [queue] ───────► Probing ───► Downloading ───► Completed
     ▲                    │                │  │            │ │
     │ resume             │                │  └─► Retrying │ └─(资源变化)→ 回到 Probing 重启
     │                    │                │    (全部退避) │
     │                    │                ▼              ▼
     │                    │         Failed ◄────(重试耗尽)──┘
     │                    ▼
     ├────── Paused ◄─ pause / 关机恢复 / 崩溃恢复
     │
     └────── Cancelled ◄─ cancel（删除 part 文件）
```

- 状态只允许 Scheduler 写入（内存 + SQLite 双写），Runner 只能"上报"。
- 崩溃恢复（`Engine::open`）：凡处于活动态（Probing/Downloading/Retrying/Pending）
  的一律改为 Paused；`Completed` 任务若最终文件缺失/长度不符则重新打开为 Paused
  并按 part 文件现状决定是否清零分段。
- `resume_on_startup = true` 时，打开引擎后自动把 Paused 任务重新入队。

## 5. 分段下载与边界安全

**分段规划**（`plan_segments`）：
- `count = min(requested_connections, ceil(size / min_segment_size))`
- 连续等分（余数摊到前 `rem` 段），保证 `[0, size)` 无重叠无遗漏（单元测试覆盖）。
- 文件为空或服务器无 Content-Length 或不支持 Range → 单个开放分段（`end = None`）。

**IDM 式动态分段（调度器统一分配 + work stealing）**：
- 每个分段的结束位置保存在 `Arc<AtomicU64>`（live end）中，Worker 每收到一个数据
  块都按 live end 截断接受范围。
- 下载循环维护明确的槽位状态机：`segs`（全部分段）× `active`（有 Worker 的分段）
  × `done`（已由 Worker 成功退出的分段）。当存在空闲槽位
  （`active.len() < 目标并发`）时按优先级分配工作：
  1. **孤儿段回收**：优先整体重新认领"无 Worker 且未完成"的分段
     （Worker 被释放或异常退出后留下的段，从已刷盘进度续传）；
  2. **最大段拆分**：把"剩余字节最多的活动分段"从中点劈开——受害分段的
     live end 缩到中点，为其尾部生成新 Worker，并将拆分持久化到 SQLite
     （`update_segment_end` + `insert_segment`）。
- 已完成分段永不再次分配（`done` 集合 + `is_complete` 双重过滤）。
- 拆分瞬间可能存在一个跨界数据块的短暂重叠，但两个 Writer 写入的是同一资源的
  相同字节（If-Range 保证资源一致），数据不受影响；进度计数按各分段持有长度
  截断（clamp）后汇总。
- 效果：快连接永远不会空闲等待慢连接，慢段的尾部被持续接管——这是消除
  "最后 1% 时间拖 50%" 的关键机制。

**自适应并发控制（`ConcurrencyCtl`，IDM 式智能连接数）**：
- 任务分两种模式：显式指定连接数（`connections: Some(n)` / CLI `-n`）→ 固定模式，
  控制器完全不干预；未指定且 `adaptive.enabled` → 自动模式，从
  `default_connections` 起步。
- 每个 `adaptive.window_ms` 观察窗口执行一次决策（状态机）：
  1. **节流降档**：窗口内失败分 ≥ 3（429/503 计 2 分，超时/网络错误计 1 分，
     错误分类来自 `Error::kind()`）→ 目标连接数 −1（下限 `min_connections`），
     并释放超编 Worker（每 Worker 独立 `release` token，被释放的 Worker 在任意
     await 点快速退出、留下孤儿段等待回收）；
  2. **引导探测**：稳态没有增益信号，控制器先乐观加 1 个连接，再验证；
  3. **验证增长**：加连接后吞吐量增益 ≥ `grow_gain_pct` → 继续增长；
     增益 < 1/3 阈值 → 判定饱和并停止增长（不盲目堆到 64 上限）。
- 目标变化通过 `RunnerMsg::Connections` 上报 Scheduler（内存快照），
  从不直接修改数据库——连接数是运行时状态，不是持久事实。

**Worker 校验链**（`worker.rs::download_once`）：
1. 请求头带 `Range: bytes={next_offset}-{live_end}` 与 `If-Range: {ETag|Last-Modified}`，
   并显式声明 `Accept-Encoding: identity`——Range 偏移是"存储表示"的字节偏移，
   任何内容编码（gzip/br）都会悄悄改变偏移语义，identity 从协议层禁止转换。
2. 响应必须为 `206`：校验 `Content-Range` 起始偏移等于请求偏移、总长与探测一致
   （不一致 → 资源已变化）、结束不超过分段边界。
3. 对 Range 请求返回 `200` 或 `416` → 视为资源变化，任务级重启（非 Worker 级重试）。
4. 流式接收：任一块若使接受范围超出 live end → 截断至边界；分段收满即完成。
5. 流提前结束但分段未满 → 协议错误，走 Worker 级重试。

**写入安全**：每个 Worker 通过 `PartFile::worker_handle()` 打开**独立文件句柄**，
使用 OS 级定位写（Windows `seek_write` / Unix `write_at`），从不共享句柄——
Windows 的定位写会临时移动共享文件指针，共享句柄并发写存在错位竞态。
收到的数据块先在 256 KiB 写缓冲中合并，再落盘；**进度计数只在数据真正写盘后
推进**，保证"DB 记录的进度永远 ≤ 已刷盘数据"的崩溃安全不变式。

## 6. 断点续传与一致性机制

崩溃安全的关键不变式：**SQLite 中记录的分段进度，永远不超过已刷盘的数据。**

实现顺序（Runner 级批量检查点，`download_phase` 的 flush tick）：
1. Runner 按 `checkpoint.interval_ms` 周期触发（每周期一次，而非每 Worker 一次——
   避免多连接下每秒 N 次 `FlushFileBuffers` 风暴拖慢下载）；
2. `part.sync_data()` —— 一次性把整个文件刷到磁盘；
3. 才把所有分段的进度（截断到各自持有长度后的值）批量写入 SQLite。

Worker 内部：数据块先合并进 256 KiB 写缓冲，缓冲刷盘成功后才推进共享进度计数；
任何退出路径（完成/取消/流错误）都会先冲刷缓冲，保证进度不超前于磁盘。

恢复流程（`runner.rs::open_part`）：
- part 文件存在：将 DB 分段进度截断（clamp）到文件实际长度以内，防止 DB 超前；
- part 文件丢失：全部分段归零、重新下载；
- 探测阶段：比对 size/ETag/Last-Modified，任一变化（或 size 未知）→
  删除 part、清零分段、全新重启（受 `retry.max_restarts` 限制）。

提交流程（`finalize`）：
1. `sync_all()` 刷盘 + 元数据；
2. **文件长度校验**：part 文件实际长度必须与分段合计长度完全一致，否则失败；
3. 可选 SHA-256 / BLAKE3 校验（不匹配 → 删 part、失败，DB 分段归零）；
4. `storage::commit`：原子 rename 到最终文件名（冲突时 `name (n).ext`）；
5. 最后才把 DB 状态置为 Completed 并记录实际 SHA-256 / BLAKE3。

因此任何一步中断，任务都停留在"有 part 文件 + 未完成状态"，重启后正确恢复，
绝不会出现"DB 完成而文件缺失/截断"。

## 7. 限速算法与传输层调优

`RateLimiter` 采用 **GCRA（Generic Cell Rate Algorithm，虚拟时间漏桶）**：

- `acquire(n)` 以锁序预留 n 字节的虚拟完成时间（TAT），若早于允许时刻则 sleep 补齐；
- 长期聚合速率精确，多个 Worker/任务竞争同一实例时公平排队；
- 容量容忍（burst）= 100ms 流量，至少 64 KiB，单块数据不必过度等待；
- **全局限速 = 全引擎共享一个实例**（`Engine` 持有并传给所有 Runner），
  不可能被各任务重复消耗；单任务限速是每任务独立实例；
- 取消安全：等待中的 future 被 drop 即放弃预留，仅轻微放宽上限；
- 运行中调整：`set_rate` 即时生效（Engine 公共方法直接可达，无需经过命令通道）。

限速接入点在 Worker 的两处 acquire（任务限速 → 全局限速），作用于每个数据块。

**传输层吞吐调优**（`http.rs::build_client`）：
- HTTP/2 模式（默认）：所有分段复用一条 TCP 连接，h2 默认 64 KiB 流控窗口会把
  高 RTT 链路吞吐压到窗口/RTT（数百 KiB/s），因此显式设置 16 MiB 连接窗口与
  2 MiB 流窗口；
- HTTP/1.1 模式（`http_version = "http1"`）：每个分段独占一条 TCP 连接，
  适配对 h2 多路复用限速的网络；
- `tcp_nodelay`：禁用 Nagle 算法，64 个 Worker 的重试/小请求不受 40ms 人为延迟拖累；
- 连接池 `pool_max_idle_per_host = max_connections_per_task`（上限 64）：
  HTTP/1.1 模式下重试与补位请求直接复用既有 TCP+TLS 会话，免新握手；
- 系统代理（`system-proxy` feature，默认开启）：遵循 Windows 注册表 /
  macOS 系统设置 / 环境变量代理。这是国际链路下达成 IDM 级速度的关键——
  直连被 ISP 限速时（实测 ~10 KiB/s），走用户代理客户端可恢复至 MiB/s 级；
- IPv4 优先 DNS 解析：规避 getaddrinfo 在 dual-stack 机器上偏好劣质 IPv6
  路由的问题。

## 8. 重试与退避

- **Worker 级**（网络瞬断/5xx/429）：`max_attempts` 次指数退避（base 500ms，
  max 30s，≤25% 抖动）；`Retry-After` 优先且封顶 300s；单次重试间隔内累计
  收到 ≥1 MiB 进度则重置计数器，长任务在弱网下不会耗尽重试。
- **探测级**（Probe 阶段同样的可重试错误）：Runner 内独立重试循环。
- **任务级重启**（资源变化、Range 被拒）：删除 part 从零开始，受 `max_restarts` 限制。
- **快速失败**：404 等不可重试状态立即 Failed，不浪费时间。
- 全部退避睡眠都在 `select!` 中与停止 token 竞争，暂停/取消即时打断。

## 9. SQLite 数据结构

```sql
tasks(id PK, url, final_url, file_name, file_path, part_path, state,
      total_size, downloaded, connections, name_locked, range_supported,
      etag, last_modified, expected_sha256, actual_sha256,
      expected_blake3, actual_blake3, auto_connections,
      rate_limit, error, restart_count, created_at, updated_at)

segments(task_id FK→tasks ON DELETE CASCADE, idx, start, end NULL, downloaded,
         PRIMARY KEY(task_id, idx))
```

- `PRAGMA journal_mode=WAL` + `synchronous=NORMAL`：崩溃后由 WAL 保证一致；
- 单连接 + `Mutex` + `spawn_blocking`：异步运行时零阻塞，串行化避免 SQLITE_BUSY；
- `busy_timeout=5s` 兜底；分段整表替换走事务；
- `user_version` 迁移机制：v1（初始 schema）→ v2（新增 BLAKE3 两列与
  `auto_connections` 标记列，纯增列，旧库原地升级、任务零丢失）。

## 10. 测试架构

`tests/common/mod.rs` 提供基于 hyper 的**本地可编程测试服务器**：
- 可编程行为：Range 支持/广播、HEAD 拒绝/无长度、ETag/Last-Modified、
  Content-Disposition、指定状态码、Retry-After、前 N 次失败、传输 N 字节后断流、
  分块延迟/分块大小、N 次请求后切换内容/ETag、无 Content-Length、
  **并发数据连接上限（超限 429）**、**共享总带宽令牌桶**；
- HTTPS 模式：rcgen 现场签发自签 CA + 叶子证书，验证私有 CA 信任链路；
- 全部测试不访问公网，确定性、可离线重复。
- 崩溃恢复测试直接关闭引擎再重开，模拟进程异常退出。

测试套件（73 个用例）：单元测试（分段规划、GCRA、配置校验、文件名、哈希）+
集成测试 `basic` / `multiseg` / `adaptive` / `resume` / `retry` / `limits` /
`scheduler`；基准矩阵 `bench`（`--ignored` 显式运行，见
`docs/performance.md`）。

## 11. 已知架构取舍

- 事件 broadcast 满时慢订阅者丢事件：以"轮询兜底"协议弥补，换取零背压阻塞。
- 单 SQLite 连接：写吞吐不是瓶颈（checkpoint 每 1s / 1MiB 一次），换串行一致性。
- Runner 在 RunnerMsg::Refresh 时让 Scheduler 重读 DB 行：内存快照与落库解耦的最简方案。
- 未知大小文件无断点续传：HTTP 协议限制（Range 需要已知边界），如实降级。
