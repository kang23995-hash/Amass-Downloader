> 当前 CLI 用法见根目录 README；默认配置已更新为 HTTP/1.1、16 个初始连接。

# RD Download Engine — 配置说明文档

引擎配置通过 `EngineConfig` 结构表达，支持三种来源（可叠加，后者覆盖前者字段）：

1. **代码默认值** —— `EngineConfig::default()`
2. **TOML 文件** —— `EngineConfig::from_file(path)`（严格校验，未知字段报错）
3. **TOML 文件 + 缺省合并** —— `EngineConfig::load_or_default(path)`（文件不存在则全默认）

`Engine::open(config)` 会先执行 `validate()`，非法配置直接返回 `Error::Config`。

## 完整配置文件示例

把以下内容保存为 `rd-engine.toml`，配合 `rd-cli --config rd-engine.toml` 使用：

```toml
# 下载文件保存目录（不存在会自动创建）
download_dir = "downloads"

# SQLite 数据库文件路径（父目录自动创建；WAL 模式会生成 -wal/-shm 伴生文件）
db_path = "rd-downloader.db"

# 同时运行下载任务的全球最大数量（>= 1）
max_concurrent_tasks = 3

# 新任务默认连接数（实际会被 max_connections_per_task 截断）
default_connections = 16

# 单个任务允许的最大 HTTP 连接数（>= 1，硬上限，自适应模式也不会超过它）
max_connections_per_task = 64

# 最小分段大小（字节）。服务器支持 Range 时，分段数不超过 ceil(文件大小 / 此值)；
# 小于该值的文件自动单连接下载，避免多连接反而降低速度
min_segment_size = 1048576

# 引擎启动时是否自动恢复上次中断（Paused）的任务
resume_on_startup = false

# 可选 NTFS 稀疏文件；本机实测未证明持续提速，默认关闭
sparse_files = false

[retry]
# 单个分段/探测的最大连续重试次数（>= 1）；单次重试间累计下载 >= 1 MiB 会重置计数
max_attempts = 8
# 指数退避基准延迟（毫秒）
base_delay_ms = 500
# 退避上限（毫秒）；抖动最多再加 25%，封顶 2 倍
max_delay_ms = 30000
# 远端资源变化导致从零重启的最大次数
max_restarts = 3

[network]
# TCP 连接超时（秒）
connect_timeout_secs = 15
# 相邻数据块之间的空闲超时（秒），用于识别"服务器卡住不吐数据"
read_timeout_secs = 30
# User-Agent
user_agent = "rd-engine/0.1.0"
# 最大重定向次数
max_redirects = 10
# HTTP 版本：http1 默认为各并发请求使用独立 HTTP/1.1 TCP 连接。
# auto 允许 ALPN 协商 HTTP/2；同一主机的分段可能共享 TCP 连接。
# 两者的实际性能取决于源站和代理，不能把请求并发数当作 TCP 连接数。
http_version = "http1"
# DNS 解析时优先使用 IPv4 地址（规避劣质 IPv6 国际路由）
prefer_ipv4 = true
# 是否使用操作系统级代理设置（Windows 注册表 / macOS 系统设置 /
# HTTP_PROXY 等环境变量）。
# 重要：在中国大陆等国际链路受限的网络环境下，浏览器与 IDM 都走系统代理，
# 引擎直连国际 CDN 通常只有 ~10 KiB/s，走系统代理可达数 MiB/s。
use_system_proxy = true

[rate_limit]
# 全局下载速度上限（字节/秒）。0 = 不限速。
# 全局上限由所有任务共享消费，运行中可用 set_global_rate_limit() 动态调整
global_bps = 0

[checkpoint]
# 分段进度检查点间隔（毫秒）
interval_ms = 1000
# 或每累计写入多少字节做一次检查点（两者任一满足即触发）
bytes = 1048576

[adaptive]
# 自适应并发（IDM 式动态分段调度）。仅对未显式指定连接数的任务生效；
# 通过 API 指定 connections 或 CLI -n 的任务永远固定并发。
# enabled = true 时：任务以 default_connections 起步，引擎按观察窗口自动增减连接
enabled = true
# 自动模式的连接数下限
min_connections = 2
# 吞吐量观察窗口（毫秒，>= 50）
window_ms = 1000
# 增加一个连接所需的最小吞吐量增益（百分比）
grow_gain_pct = 5

[tls]
# 接受无效证书（仅测试用，生产禁用）
accept_invalid_certs = false
# 额外信任的 CA 证书路径（PEM 或 DER），用于私有 CA 站点
# ca_cert_path = "corp-root-ca.pem"
```

## 字段参考

### 顶层

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `download_dir` | 路径 | `downloads` | 默认下载目录；`AddTaskRequest.output_dir` 可按任务覆盖 |
| `db_path` | 路径 | `rd-downloader.db` | SQLite 库路径；任务与分段状态、元数据都存这里 |
| `max_concurrent_tasks` | usize | 3 | 并发运行任务上限，其余任务排队 |
| `default_connections` | u32 | 16 | 新任务默认连接数（自动模式下的起步值） |
| `max_connections_per_task` | u32 | 64 | 连接数硬上限（请求值会被 clamp，自适应也不会超过） |
| `min_segment_size` | u64 | 1048576 | 最小分段字节数 |
| `resume_on_startup` | bool | false | 启动时自动恢复 Paused 任务（CLI 恒为 false，仅恢复明确选择的任务） |

### `retry`

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `max_attempts` | u32 | 8 | 分段/探测最大连续重试次数；带 ≥1 MiB 进度的重试重置计数 |
| `base_delay_ms` | u64 | 500 | 退避基数，第 n 次重试延迟 ≈ base × 2^(n-1)，含 ≤25% 抖动 |
| `max_delay_ms` | u64 | 30000 | 退避上限 |
| `max_restarts` | u32 | 3 | 远端资源变化（size/ETag/Last-Modified 变化）时从零重启的最大次数 |

### `network`

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `connect_timeout_secs` | u64 | 15 | TCP 建连超时 |
| `read_timeout_secs` | u64 | 30 | 数据块间空闲超时（识别僵死连接） |
| `user_agent` | String | `rd-engine/{version}` | 请求 UA |
| `max_redirects` | usize | 10 | 重定向深度，超限报错 |
| `http_version` | `auto`/`http1` | `http1` | `auto` = ALPN 协商 HTTP/2（大流量窗口）；`http1` = 每分段独立 TCP 连接 |
| `prefer_ipv4` | bool | true | DNS 结果 IPv4 优先，规避劣质 IPv6 路由 |
| `use_system_proxy` | bool | true | 遵循操作系统代理设置与环境变量。国际链路下这通常是从 KiB/s 到 MiB/s 的关键 |

### `rate_limit`

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `global_bps` | u64 | 0 | 全局合计限速（字节/秒），0 = 不限。运行时 `engine.set_global_rate_limit(Some(n))` 即时生效 |

### `checkpoint`

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `interval_ms` | u64 | 1000 | 检查点最小时间间隔 |
| `bytes` | u64 | 1048576 | 检查点最小字节间隔 |

检查点动作 = 先 `sync_data()` 刷盘，再更新 SQLite 分段进度。两个条件（时间或字节）
任一满足即触发；慢速下载会按 `bytes` 间隔跳过部分 DB 写入以降低 I/O 开销。
增大间隔可降低开销，但崩溃后可能多丢一点进度（绝不会多记——数据库状态落后于磁盘
始终是安全的，恢复时会用文件长度钳制分段进度）。

### `adaptive`

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | true | 未显式指定连接数的任务启用自动并发调节 |
| `min_connections` | u32 | 2 | 自动模式连接数下限 |
| `window_ms` | u64 | 1000 | 吞吐量观察窗口（毫秒），≥ 50 |
| `grow_gain_pct` | u32 | 5 | 增加连接所需的最小吞吐量增益百分比 |

工作方式（每 `window_ms` 一个观察窗口）：

1. **节流降档**：窗口内累计失败分 ≥ 3（429/503 计 2 分，超时/网络错误计 1 分）
   → 目标连接数 −1（不低于 `min_connections`），并释放多余 Worker；
2. **引导探测**：稳态下没有增益信号，引擎乐观地加 1 个连接再验证；
3. **验证增长**：加连接后若吞吐量增益 ≥ `grow_gain_pct` → 继续加；若增益
   < 1/3 阈值 → 判定饱和，停止增长（连接数不会被盲目堆到上限）。

被释放的 Worker 留下部分完成分段，由调度器在空闲时回收续传，不会重复下载。

### `tls`

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `accept_invalid_certs` | bool | false | 跳过证书校验。仅限测试环境 |
| `ca_cert_path` | 可选路径 | 无 | 追加信任的根证书（PEM/DER），用于自签或企业 CA |

## 运行时可调项（无需重启）

- **全局限速**：`engine.set_global_rate_limit(bytes_per_sec: Option<u64>)`
- **单任务限速**：`engine.set_task_rate_limit(id, bytes_per_sec: Option<u64>)`，
  同时持久化到 SQLite，崩溃恢复后仍生效

## 配置加载注意事项

- TOML 使用 `deny_unknown_fields`：拼错的键名会直接报错，避免静默失效。
- 所有分组（`[retry]` 等）支持部分填写，缺省字段取默认值。
- `EngineConfig::validate()` 保证：任务数/连接数/分段大小 ≥ 1、重试次数 ≥ 1、
  `base_delay_ms ≤ max_delay_ms`、`adaptive.min_connections ≥ 1`、
  `adaptive.window_ms ≥ 50`、`download_dir` 非空。
- 引擎创建文件时自动 `create_dir_all`，无需预先建目录。
- 数据库自动从 v1 升级到 v2（新增 `expected_blake3`/`actual_blake3`/
  `auto_connections` 列），旧任务数据不丢失。
