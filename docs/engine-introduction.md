> 此文档保留早期架构说明。当前 CLI 已提供任务列表、状态、恢复、取消、删除及任务复用；运行方式、构建要求和默认值以根目录 README 与源码为准。

# RD Download Engine V1.0 — 引擎详细介绍文档

> 本文档面向要接入该引擎的开发者（未来 CLI/TUI/GUI 的实现者）与需要评估引擎能力
> 的技术决策者，完整覆盖：技术栈选型与理由、功能清单、公共 API、从零启动到完整
> 使用的全部细节。

---

## 1. 项目定位

RD Downloader 是一款对标 Internet Download Manager 的现代化下载器，分三阶段开发：

| 阶段 | 内容 | 状态 |
|------|------|------|
| 一 | Rust 下载引擎（本项目） | ✅ V1.0 完成 |
| 二 | 基于同一引擎的 CUI/TUI 终端界面 | 未开始 |
| 三 | 基于同一引擎的 Slint 桌面 GUI | 未开始 |

引擎是整个产品的唯一"下载事实来源"：**所有 UI 形态都只调用引擎公共 API 与事件流，
自身零下载逻辑**。当前仓库同时包含一个最小演示程序 `rd-cli`（仅用于验证 API 与
教学演示，不是第二阶段的完整终端界面）。

## 2. 技术栈

| 类别 | 选型 | 版本策略 | 选型理由 |
|------|------|----------|----------|
| 语言 | Rust（edition 2021） | stable | 内存安全 + 零成本抽象，下载器这种长驻 I/O 密集型程序的理想选择 |
| 工程组织 | Cargo Workspace | — | 引擎库（`rd-engine`）与演示二进制（`rd-cli`）分离，依赖版本由 workspace 统一管理 |
| 异步运行时 | Tokio | 1.x | 全功能运行时：多线程调度器、`select!`、`CancellationToken`、`spawn_blocking` |
| HTTP 客户端 | Reqwest | 0.12 | 成熟稳定；启用 `rustls-tls`（纯 Rust TLS，无 OpenSSL/C 依赖）+ `stream`（流式响应）+ `http2`（含大流量窗口调优）+ `system-proxy`（遵循操作系统代理设置） |
| TLS | rustls（经 reqwest） | — | 纯 Rust 实现，配合 reqwest 默认关闭解压特性（保证字节偏移与磁盘一致，Range 请求才有效） |
| 持久化 | SQLite via rusqlite（bundled） | 0.32 | 单文件零部署；`bundled` 特性自带 SQLite 源码编译，宿主机无需安装任何 C 库；WAL 模式保证崩溃一致性 |
| 配置 | TOML via `toml` crate | 0.8 | 人可读、注释友好；`deny_unknown_fields` 拼错即报错 |
| 序列化 | Serde | 1.x | `TaskInfo`/`Event` 快照可序列化，未来 UI 进程间通信可直接复用 |
| 错误处理 | thiserror | 2.x | 声明式结构化错误枚举（12 个变体），支持 `is_retryable()` 语义判定 |
| 日志 | Tracing + tracing-subscriber | 0.1 / 0.3 | 结构化日志，`RUST_LOG` 环境变量分级控制 |
| 校验/编码 | sha2、blake3、hex、percent-encoding、url | — | SHA-256 / BLAKE3 校验、URL 文件名提取与百分号解码 |
| 并发工具 | tokio-util（CancellationToken）、bytes、futures-util | — | 协作式取消、零拷贝数据块、流式工具 |
| 测试 | hyper 1 + rcgen + tokio-rustls + tempfile | dev-deps | 本地可编程 HTTP/HTTPS 测试服务器 + 自签 CA + 隔离临时目录 |

**刻意未引入**：C/C++ 原生依赖（除 SQLite bundled 源码）、解压中间件（会破坏
Range 偏移一致性）、任何 UI 框架。

## 3. 功能总览

### 3.1 HTTP/HTTPS 下载

- HTTP 与 HTTPS 全支持；HTTPS 默认系统信任库，私有 CA 经配置注入，可选跳过校验。
- **自动探测**（`http.rs::probe`）：先 HEAD 获取大小/ETag/Last-Modified/
  Accept-Ranges/Content-Disposition；HEAD 被拒（405/501）或信息不全时自动降级为
  `Range: bytes=0-0` 的 GET 探测，从 `Content-Range` 精确取得真实大小与真实 Range
  支持情况（防"声明支持但实际忽略 Range"的服务器）。
- **重定向**：跟随至多 10 跳，记录最终 URL；重定向错误不重试。
- **状态码处理**：408/425/429/500/502/503/504 判定为可重试；404 等快速失败；
  429/503 尊重 `Retry-After`。
- **降级策略**：服务器不支持 Range / 文件无 Content-Length / 空文件 →
  单连接流式下载。

### 3.2 多连接分段下载（IDM 式动态分段 + 自适应并发）

- 最多 **64 并发连接**（`max_connections_per_task` 默认 64）；按
  `min(请求连接数, ceil(文件大小/最小分段))` 规划连续分段，边界精确无重叠
  （单元测试穷举验证）。
- 每个分段一个独立 Worker（tokio 任务），各持独立文件句柄做 **OS 级定位写**。
- **动态分段调度**（调度器统一管理所有未完成区间）：
  1. **孤儿段回收**：Worker 被释放/异常退出留下的未完成分段，空闲 Worker 整体
     认领、从已刷盘进度续传；
  2. **最大段拆分（work stealing）**：存在空闲槽位时，把"剩余字节最多的活动
     分段"从中点劈开，新 Worker 接管尾部——快连接永远不会闲着等慢连接，
     这是 IDM 级速度的核心机制之一；
  3. 已完成分段永不再次分配；拆分持久化到 SQLite，崩溃后恢复边界一致。
- **自适应并发**：未显式指定连接数的任务默认进入自动模式——控制器按观察窗口
  监测总吞吐与失败率：429/503/超时累计超阈值 → 降档并释放多余 Worker；
  加连接带来 ≥`grow_gain_pct` 吞吐增益 → 继续增长；无收益 → 判定饱和停止增长。
  显式 `-n` / `connections: Some(n)` 则固定并发，控制器完全不干预。
- Worker 端五重校验：Content-Range 起始偏移匹配、总长与探测一致、
  分段边界不越界、`If-Range` 防止服务器静默换资源、流式累计字节数与分段长度比对。
- 任何校验失败都能区分"Worker 级重试"（网络瞬断）与"任务级重启"（资源变化）。
- 明确承认"多连接不一定更快"：小文件（< 最小分段大小）自动单连接。
- **传输层自适应**：默认 HTTP/2（ALPN 协商，16 MiB 连接窗口 / 2 MiB 流窗口，
  消除 h2 默认 64 KiB 窗口在高 RTT 链路上的吞吐瓶颈）；配置
  `http_version = "http1"` 可切换为每分段一条 HTTP/1.1 TCP 连接；
  `tcp_nodelay` 禁用 Nagle；连接池 idle 上限对齐 64 连接以复用 TCP+TLS 会话。
- **数据请求显式 `Accept-Encoding: identity`**：Range 偏移是"存储表示"的字节
  偏移，任何内容编码都会悄悄破坏偏移语义，identity 从协议层禁止服务器转换。
- **系统代理**：默认遵循 OS 代理设置与 `HTTP(S)_PROXY` 环境变量。在直连国际
  链路被 ISP 限速的网络环境下（实测直连 ~10 KiB/s，走系统代理 ~8 MiB/s），
  这是获得 IDM 级速度的决定性因素；`use_system_proxy = false` 强制直连。

### 3.3 断点续传与任务恢复（引擎重点）

- **暂停/恢复**：暂停 = 取消 token + 等待 Worker 落盘 checkpoint + 状态持久化为
  Paused；恢复 = 校验远端未变化后从 DB 分段进度继续。
- **崩溃恢复**：`Engine::open` 把活动态任务一律改为 Paused，再按
  `resume_on_startup` 自动入队；`Completed` 任务若最终文件缺失/截断会自动重新打开。
- **双阶段 checkpoint 不变式**：先 `sync_data()` 刷盘，再写 SQLite 进度——
  DB 记录的进度永远 ≤ 磁盘真实数据，崩溃后宁可少记不会多记。
- **恢复期自愈**：DB 进度超前于文件长度时自动截断；part 文件丢失时自动清零重下。
- **远端变化防护**：恢复前比对 size/ETag/Last-Modified，变化即安全重启
  （删除 part、清零分段，受 `max_restarts` 约束）。
- 提交前文件长度校验 + 可选 SHA-256 / BLAKE3 校验 + `sync_all` + 原子 rename，
  杜绝"假完成"。

### 3.4 下载速度限制

- **全局限速**：整个引擎共享一个 GCRA 限速器实例，所有任务的所有 Worker 竞争同一
  配额，合计速率精确受控——**不会**被多任务重复消费。
- **单任务限速**：每任务独立实例，持久化到 DB，恢复后仍生效。
- **动态调整**：两个限速都可在运行中即时调整（`set_global_rate_limit` /
  `set_task_rate_limit`），调整立即对新数据块生效。
- **算法**：GCRA（虚拟时间漏桶）——长期速率精确、消费者公平排队、突发容忍约
  100ms 流量、取消安全。

### 3.5 下载任务调度

- 最大并发任务数限制（默认 3），超出排队 FIFO。
- 暂停 / 恢复 / 取消 / 移除（可选连文件删除）完整生命周期。
- 失败自动重试：指数退避（500ms 起、30s 封顶、≤25% 抖动）+ Retry-After 优先
  （封顶 300s）+ "有 ≥1 MiB 实际进度则重置重试计数"的弱网友好策略。
- 取消及时性：Worker 在请求、流读取、限速等待、退避睡眠每个 await 点都
  `select!` 停止 token，取消后 part 文件即刻清理、DB 状态即时落库。
- 资源竞争安全：Scheduler 单线程顺序状态机处理全部状态变更，无锁竞态。

### 3.6 文件完整性与安全性

- 分段定位写入互不冲突；写入零字节检测；磁盘空间不足/权限异常 → 任务 Failed
  带清晰错误。
- 文件名三重净化：路径穿越剥离、Windows 非法字符与保留名（CON/NUL/...）处理、
  截断超长名。
- 命名冲突自动 `name (1).ext` 递增；提交时刻二次解析冲突。
- SHA-256 校验：请求时可声明期望值，完成时实测；不匹配 → 删 part、清零、失败。
- BLAKE3 校验：与 SHA-256 可同时声明（`expected_blake3`），完成时实测记录。
- 生命周期：`.part` 文件贯穿下载期，提交后消失；取消/移除任务同步清理 part。

### 3.7 任务状态与事件系统

状态机（8 态，与产品规划一一对应）：

```
Pending → Probing → Downloading ⇄ Retrying → Completed
                  ↘              ↘
              Paused ←（暂停/关机/崩溃恢复）   Failed
    Paused/Failed/Cancelled →（resume）→ Pending
    任意活动态/Pending →（cancel）→ Cancelled
```

事件（`broadcast` 通道，UI 通过 `engine.subscribe()` 订阅）：

| 事件 | 时机 |
|------|------|
| `TaskCreated` | 任务入库后 |
| `TaskStarted` | 任务从队列启动 |
| `TaskStateChanged` | 每次状态迁移 |
| `ProgressUpdated` | 250ms 节流的进度快照 |
| `SpeedUpdated` | 500ms 节流的实时速度 |
| `TaskPaused` / `TaskResumed` | 暂停 / 恢复 |
| `TaskRetrying` | 每次重试（含 attempt / delay / error） |
| `ResourceChanged` | 远端资源变化、任务重启 |
| `TaskCompleted` | 完成（含最终路径与实测 SHA-256 / BLAKE3） |
| `TaskFailed` | 失败（含错误描述） |
| `TaskCancelled` / `TaskRemoved` | 取消 / 移除 |

> 事件通道容量 1024，慢订阅者会收到 `Lagged`。约定：**事件只做通知，
> 状态事实源是 `get_task`/`list_tasks`**，UI 收到 Lagged 后应重新拉取快照。

## 4. 公共 API 详解

`rd-engine` 对外仅暴露四个核心类型：`Engine`（门面）、`EngineConfig`、
`AddTaskRequest` / `TaskInfo` / `Event` / `TaskState`（model）、`Error`/`Result`。
Worker、PartFile、Db 等全部私有——UI 无法绕过门面触碰内部实现。

```rust
pub struct Engine { /* Clone 廉价，多克隆共享同一引擎 */ }

impl Engine {
    /// 打开/创建数据库，恢复崩溃任务，启动调度器。
    pub async fn open(config: EngineConfig) -> Result<Self>;

    // ---- 任务生命周期 ----
    pub async fn add_task(&self, req: AddTaskRequest) -> Result<TaskId>;
    pub async fn pause_task(&self, id: TaskId) -> Result<()>;
    pub async fn resume_task(&self, id: TaskId) -> Result<()>;
    pub async fn cancel_task(&self, id: TaskId) -> Result<()>;
    /// 移除任务记录（活动任务须先 cancel）；delete_file=true 连最终文件一起删。
    pub async fn remove_task(&self, id: TaskId, delete_file: bool) -> Result<()>;

    // ---- 查询 ----
    pub async fn get_task(&self, id: TaskId) -> Result<TaskInfo>;
    pub async fn list_tasks(&self) -> Result<Vec<TaskInfo>>;

    // ---- 限速 ----
    pub async fn set_task_rate_limit(&self, id: TaskId, bps: Option<u64>) -> Result<()>;
    pub fn set_global_rate_limit(&self, bps: Option<u64>);   // 即时生效
    pub fn global_rate_limit(&self) -> u64;

    // ---- 事件与生命周期 ----
    pub fn subscribe(&self) -> broadcast::Receiver<Event>;
    pub async fn shutdown(&self) -> Result<()>;  // 暂停所有活动任务并停止调度器
    pub fn config(&self) -> &EngineConfig;
}
```

**`AddTaskRequest` 参数**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `url` | String | HTTP/HTTPS 链接（唯一必填项） |
| `output_dir` | Option\<PathBuf\> | 覆盖全局下载目录 |
| `file_name` | Option\<String\> | 指定文件名（锁定，不再被 Content-Disposition 覆盖） |
| `connections` | Option\<u32\> | 固定连接数（会被 max_connections_per_task 截断）；**不填则进入自适应自动模式**（`adaptive.enabled`） |
| `expected_sha256` | Option\<String\> | 完成后强制校验（SHA-256） |
| `expected_blake3` | Option\<String\> | 完成后强制校验（BLAKE3，可与 sha256 同时用） |
| `rate_limit_bps` | Option\<u64\> | 单任务限速（字节/秒） |
| `auto_start` | bool | 默认 true；false 表示只入库不入队，稍后 `resume_task` 启动 |

**错误语义**（`Error`，共 12 变体）：`TaskNotFound` / `InvalidState`（如对
Completed 任务 cancel）/ `Shutdown`（引擎停机后调用）等业务错误可直接匹配处理；
网络类错误经 `is_retryable()` 统一判定可重试性。

## 5. 启动方式

### 5.1 环境要求

- Rust stable 1.85+（`rustup` 安装）。Windows / macOS / Linux 均可。
- 无需任何外部依赖：SQLite 由 rusqlite `bundled` 自带，TLS 由 rustls 纯 Rust 实现。

### 5.2 编译

```powershell
git clone <repo> Amass && cd Amass   # 或直接进入现有项目目录

cargo build --release                 # 引擎 + 演示 CLI
# 产物：target/release/rd-cli.exe（引擎为库，不单独产出二进制）
```

### 5.3 运行测试

```powershell
cargo test --workspace     # 73 个测试，全部基于本地测试服务器，不访问公网
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings

# 性能基准（显式运行，输出速度/重试/完整性矩阵表）
cargo test --release -p rd-engine --test bench -- --ignored --nocapture
```

### 5.4 启动演示程序 rd-cli

```powershell
# 基本下载（保存到 ./downloads/，数据库为 ./rd-downloader.db）
target\release\rd-cli.exe https://example.com/file.zip

# 完整参数
target\release\rd-cli.exe https://example.com/file.zip `
    -o D:\Downloads `              # 输出目录
    -n 8 `                         # 8 连接（固定并发；不传 -n 则自适应自动模式）
    --limit 5242880 `              # 单任务限速 5 MiB/s
    --sha256 <期望值> `             # 完成后校验 SHA-256
    --blake3 <期望值> `             # 完成后校验 BLAKE3（可与 --sha256 同时用）
    --config rd-engine.toml        # 使用配置文件
```

运行行为：

- 控制台实时显示 `百分比 / 已下载 / 总量 / 速度`（每 200ms 刷新）。
- **Ctrl-C = 暂停**：任务与分段进度落库，进程安全退出（退出码 0）。
- **断点续传**：再次运行同一条命令（同一数据库文件），自动从断点继续。
- 退出码：`0` 完成/暂停；`1` 失败（含 404、校验不匹配等，错误信息打印在 stderr）；
  `2` 用法错误。
- 日志：设置 `RUST_LOG=rd_engine=debug` 可查看引擎内部日志。

## 6. 作为库使用的完整指南

### 6.1 最小示例

```rust
use rd_engine::{AddTaskRequest, Engine, EngineConfig, Event, TaskState};

#[tokio::main]
async fn main() -> rd_engine::Result<()> {
    // 1. 打开引擎（默认配置：./downloads + ./rd-downloader.db）
    let engine = Engine::open(EngineConfig::default()).await?;

    // 2. 订阅事件（必须在 add_task 之前，否则收不到 TaskCreated）
    let mut events = engine.subscribe();

    // 3. 添加任务（默认自动开始下载）
    let id = engine
        .add_task(AddTaskRequest::new("https://example.com/bigfile.zip"))
        .await?;

    // 4. 消费事件驱动 UI
    while let Ok(ev) = events.recv().await {
        match ev {
            Event::ProgressUpdated { id: tid, downloaded, total_size } if tid == id => {
                if let Some(total) = total_size {
                    println!("{:.1}%", downloaded as f64 / total as f64 * 100.0);
                }
            }
            Event::TaskCompleted { id: tid, file_path, sha256 } if tid == id => {
                println!("完成: {} (sha256={:?})", file_path.display(), sha256);
                break;
            }
            Event::TaskFailed { id: tid, error } if tid == id => {
                eprintln!("失败: {error}");
                break;
            }
            _ => {}
        }
    }

    // 5. 优雅停机（活动任务转为 Paused 持久化，下次 open 可恢复）
    engine.shutdown().await?;
    Ok(())
}
```

### 6.2 暂停 / 恢复 / 取消

```rust
engine.pause_task(id).await?;    // 等待所有 Worker 落盘后才返回，状态=Paused
engine.resume_task(id).await?;   // Paused/Failed/Cancelled 均可恢复，从断点继续
engine.cancel_task(id).await?;   // 删除 part 文件，状态=Cancelled（仍可 resume 重下）
engine.remove_task(id, true).await?; // 从列表删除记录，true 时删除最终文件
```

恢复时的远端校验是自动的：若 size/ETag/Last-Modified 与上次不一致，引擎会
发 `ResourceChanged` 事件并安全重启下载，UI 无需处理。

### 6.3 限速

```rust
// 全局限速 10 MiB/s（所有任务合计），运行中随时调整
engine.set_global_rate_limit(Some(10 * 1024 * 1024));

// 单任务限速 2 MiB/s，并持久化（崩溃恢复后仍生效）
engine.set_task_rate_limit(id, Some(2 * 1024 * 1024)).await?;

// 解除限速
engine.set_global_rate_limit(None);
engine.set_task_rate_limit(id, None).await?;
```

### 6.4 使用配置文件

```rust
let config = EngineConfig::load_or_default("rd-engine.toml")?; // 不存在则默认
let config = EngineConfig {                                    // 覆盖个别字段
    resume_on_startup: true,
    ..config
};
let engine = Engine::open(config).await?;
```

### 6.5 失败重试语义（UI 侧需知）

- 网络瞬断 / 5xx / 429：引擎自动重试（退避 + Retry-After），UI 只会看到
  `TaskRetrying` 事件与状态短暂进入 `Retrying`。
- 重试耗尽 → `TaskFailed`；UI 调 `resume_task` 即可再次尝试（分段进度保留）。
- 404 / DNS 失败 / 校验不匹配 → 直接 `TaskFailed`，不重试。
- `TaskFailed` 不清 part 文件，因此"恢复"是真实的断点续传。

### 6.6 UI 集成约定（为第二/三阶段预设）

1. 事件用于刷新界面；`Lagged` 后调用 `list_tasks()` 重建快照。
2. `TaskInfo.speed_bps` 仅在引擎运行时有意义，停机后为 0。
3. 界面关闭 = 调 `engine.shutdown().await`（内部会暂停全部活动任务并落盘）。
4. 不要缓存 `TaskInfo` 长期使用——它是快照，以事件/轮询刷新为准。
5. `Engine` 可 Clone，多窗口/多模块共享同一引擎是设计内行为。

## 7. 数据与文件布局

运行时产物（全部位于配置指定路径，可整体删除重建）：

| 路径 | 内容 |
|------|------|
| `{db_path}` | SQLite 主库：任务、分段进度、元数据（WAL 模式） |
| `{db_path}-wal` / `-shm` | WAL 伴生文件，进程退出后可能自动消失 |
| `{download_dir}/{name}.part` | 进行中任务的分块数据文件（任务完成后消失） |
| `{download_dir}/{name}` | 最终文件 |

## 8. V1.0 验收情况摘要

- 提示词要求的 15 类测试场景 **全部** 有对应自动化测试并全部通过
  （33 单元 + 39 集成 + 1 文档测试 = **73 tests green**），另含 64 并发连接
  分区完整性测试、4 个自适应调度集成测试与 identity 编码头断言。
- 性能升级完成度：64 并发上限、IDM 式动态分段（孤儿回收 + 最大段拆分）、
  自适应并发（引导探测 + 验证增长 + 节流降档）、`Accept-Encoding: identity`、
  `tcp_nodelay`/连接池复用、完成前长度校验、BLAKE3、`checkpoint.bytes` 节流。
- 本地基准：每连接 ~4 MiB/s 节流下，固定 1/4/8/16/32/64 连接实测
  4.0/16.0/31.9/63.7/118.5/228.2 MiB/s，全部 SHA-256 完整；自适应模式自动
  增长连接并稳定完成。详见 [基准测试报告](performance.md)。
  （2.5G/10G 真实网络验收条件本机不具备，报告中已如实声明。）
- `cargo check` / `cargo test` / `cargo fmt --check` / `cargo clippy -D warnings`
  四项质量门禁全绿。
- 端到端实测：rd-cli 经本地 HTTP 服务器 4 连接下载 3 MiB 文件，SHA-256 校验一致；
  404 路径正确失败并返回非零退出码。
- 详细的模块设计、一致性机制与已知取舍见
  [架构设计文档](architecture.md)；全部配置项见
  [配置说明文档](configuration.md)。

## 9. 已知限制与待改进

| 限制 | 说明 | 缓解/规划 |
|------|------|-----------|
| 协议范围 | 仅 HTTP/HTTPS；无 FTP/BT/M3U8 | 按产品规划不在 V1.0 |
| 未知大小文件 | 无 Content-Length 时单连接流式，无断点续传 | HTTP 协议固有限制 |
| 事件订阅 | 慢消费者丢事件（Lagged） | 约定以 `list_tasks` 轮询兜底 |
| Retry-After | 仅支持秒数格式，不支持 HTTP-date；`Retry-After: 0` 被钳制为 ≥100ms | 防忙循环，保护重试预算 |
| 限速粒度 | 长时精确，瞬时突发 ≤100ms 流量 | 对体验无实际影响 |
| 证书校验 | 系统信任库；私有 CA 需配置 `tls.ca_cert_path` | 已支持；`accept_invalid_certs` 仅供测试 |
| 命名策略 | 冲突固定为 `name (n).ext`，不可配置 | 可按需扩展 |
| 认证 | 未实现 Basic/Bearer/Cookie 等认证下载 | V1.1 候选 |
| 镜像/多源 | 未实现同一文件多 URL 拉取 | V1.x 候选 |
| 代理 | 自动遵循 OS 代理设置与环境变量（`use_system_proxy = false` 强制直连）；显式代理 URL 配置为 V1.1 候选 | 直连国际链路在部分网络下会被 ISP 限速，走系统代理通常可恢复 MiB/s 级速度 |
| 自适应增长节奏 | 默认 `window_ms=1000` 下连接数爬升偏保守（每步 ≈ 引导探测 + 2 个验证窗口） | 大文件无影响；短文件可调小 `window_ms`，或未来引入前期指数步进 |
| 2.5G/10G 验收 | 本机无真实高带宽网络 | 架构无已知瓶颈；需 iperf + 高速盘环境复测 |
