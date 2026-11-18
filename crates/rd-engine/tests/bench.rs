//! Repeatable throughput benchmark against the local test server.
//!
//! Run with:
//!   cargo test --release -p rd-engine --test bench -- --ignored --nocapture
//!
//! Every scenario is run `RUNS` times; the table reports the median and mean
//! average speed so a single noisy run cannot skew the result. Conditions
//! covered: per-connection throttle, aggregate pipe cap, unthrottled high
//! bandwidth, a per-IP concurrent-connection limit (429), and HTTP/1.1 vs
//! HTTP/2 over HTTPS. All numbers are real measurements — nothing is simulated
//! or scaled.

#![allow(dead_code)]

mod common;

use std::time::{Duration, Instant};

use common::*;
use rd_engine::config::HttpVersion;
use rd_engine::{AddTaskRequest, Event, TaskState};
use sysinfo::{Pid, ProcessesToUpdate, System};

const FILE_SIZE: usize = 64 * 1024 * 1024;
/// Per-connection throttle applied by the test server (8 MiB/s nominal).
const PER_CONN_BPS: u64 = 8 * 1024 * 1024;
/// Aggregate pipe cap for the "adaptive capped" scenario (32 MiB/s).
const TOTAL_BPS: u64 = 32 * 1024 * 1024;
/// Independent repetitions per scenario (median + mean reported).
const RUNS: usize = 5;

/// Server / engine conditions for one scenario.
#[derive(Clone, Copy)]
struct Opts {
    total_bps: Option<u64>,
    window_ms: u64,
    max_concurrent: Option<usize>,
    per_conn_throttle: bool,
    https: bool,
    http1: bool,
    patient_retry: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            total_bps: None,
            window_ms: 1000,
            max_concurrent: None,
            per_conn_throttle: true,
            https: false,
            http1: false,
            patient_retry: false,
        }
    }
}

struct Sample {
    cpu_percent: f32,
    rss_bytes: u64,
}

/// Best-effort process CPU/RSS sample; numbers are approximate by nature.
fn sample(sys: &mut System) -> Sample {
    let pid = Pid::from(std::process::id() as usize);
    std::thread::sleep(Duration::from_millis(250));
    sys.refresh_processes(ProcessesToUpdate::All, true);
    match sys.process(pid) {
        Some(p) => Sample {
            cpu_percent: p.cpu_usage(),
            rss_bytes: p.memory(),
        },
        None => Sample {
            cpu_percent: 0.0,
            rss_bytes: 0,
        },
    }
}

struct RunResult {
    connections_seen: u32,
    elapsed: Duration,
    avg_bps: u64,
    peak_bps: u64,
    retries: u32,
    integrity_ok: bool,
    cpu_percent: f32,
    rss_bytes: u64,
    /// (elapsed seconds, cumulative downloaded bytes) at each whole second
    /// and at completion.
    timeline: Vec<(f64, u64)>,
}

async fn run_scenario(
    sys: &mut System,
    label: &str,
    connections: Option<u32>,
    opts: Opts,
) -> RunResult {
    let dir = tempfile::tempdir().unwrap();
    let file_size = std::env::var("RD_BENCH_MIB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| (1..=2048).contains(&v))
        .map(|v| v * 1024 * 1024)
        .unwrap_or(FILE_SIZE);
    let data = pattern_data(file_size);
    let mut behavior = Behavior::new(data.clone());
    behavior.chunk_size = 64 * 1024;
    if opts.per_conn_throttle {
        behavior.chunk_delay =
            Duration::from_nanos(64 * 1024 * 1_000_000_000 / PER_CONN_BPS.max(1));
    }
    behavior.total_bps = opts.total_bps;
    behavior.max_concurrent_data = opts.max_concurrent;
    let (server, ca_pem) = if opts.https {
        let (s, ca) = TestServer::start_https(behavior).await;
        (s, Some(ca))
    } else {
        (TestServer::start(behavior).await, None)
    };

    let mut cfg = test_config(dir.path());
    cfg.adaptive.window_ms = opts.window_ms;
    cfg.network.http_version = if opts.http1 {
        HttpVersion::Http1
    } else {
        HttpVersion::Auto
    };
    if connections.is_none() {
        cfg.default_connections = 2;
        cfg.max_connections_per_task = 64;
    }
    if opts.patient_retry {
        // A sustained per-IP connection limit needs patient backoff, not the
        // transient-error retry budget.
        cfg.retry.max_attempts = 40;
        cfg.retry.max_delay_ms = 1000;
    }
    if let Some(ca) = ca_pem {
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, ca).unwrap();
        cfg.tls.ca_cert_path = Some(ca_path);
    }
    let engine = open_engine(cfg).await;

    let mut req = AddTaskRequest::new(server.url("/bench.bin"));
    req.connections = connections;
    let id = engine.add_task(req).await.unwrap();
    let mut events = engine.subscribe();

    sys.refresh_processes(ProcessesToUpdate::All, true);
    let started = Instant::now();
    let (mut peak, mut retries, mut conns_seen) = (0u64, 0u32, connections.unwrap_or(2));
    let mut timeline: Vec<(f64, u64)> = Vec::new();
    let mut last_sec = 0u64;
    loop {
        tokio::time::sleep(Duration::from_millis(15)).await;
        let info = engine.get_task(id).await.unwrap();
        conns_seen = conns_seen.max(info.connections);
        let t = started.elapsed().as_secs_f64();
        let sec = t as u64;
        if sec > last_sec {
            timeline.push((t, info.downloaded));
            last_sec = sec;
        }
        if info.state == TaskState::Completed {
            timeline.push((t, info.downloaded));
            break;
        }
        if info.state == TaskState::Failed {
            panic!("scenario {label} FAILED: {:?}", info.error);
        }
        if started.elapsed() > Duration::from_secs(120) {
            panic!(
                "scenario {label} timed out in state {:?}: {:?}",
                info.state, info.error
            );
        }
        while let Ok(ev) = events.try_recv() {
            match ev {
                Event::SpeedUpdated { bytes_per_sec, .. } => peak = peak.max(bytes_per_sec),
                Event::TaskRetrying { .. } => retries += 1,
                _ => {}
            }
        }
    }
    let elapsed = started.elapsed();
    while let Ok(ev) = events.try_recv() {
        match ev {
            Event::SpeedUpdated { bytes_per_sec, .. } => peak = peak.max(bytes_per_sec),
            Event::TaskRetrying { .. } => retries += 1,
            _ => {}
        }
    }
    let info = engine.get_task(id).await.unwrap();
    let integrity_ok = read_file(&info.file_path) == data.to_vec();
    let s = sample(sys);
    engine.shutdown().await.unwrap();

    RunResult {
        connections_seen: conns_seen,
        elapsed,
        avg_bps: (file_size as f64 / elapsed.as_secs_f64()) as u64,
        peak_bps: peak,
        retries,
        integrity_ok,
        cpu_percent: s.cpu_percent,
        rss_bytes: s.rss_bytes,
        timeline,
    }
}

struct ScenarioStats {
    label: String,
    connections_seen: u32,
    median_elapsed: Duration,
    median_bps: u64,
    mean_bps: u64,
    min_bps: u64,
    max_bps: u64,
    peak_bps: u64,
    retries: u32,
    integrity_ok: bool,
    cpu_percent: f32,
    rss_bytes: u64,
    timeline: Vec<(f64, u64)>,
}

fn median_u64(mut v: Vec<u64>) -> u64 {
    v.sort_unstable();
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2
    }
}

async fn run_scenario_stats(
    sys: &mut System,
    label: &str,
    connections: Option<u32>,
    opts: Opts,
) -> ScenarioStats {
    let mut runs = Vec::with_capacity(RUNS);
    for i in 0..RUNS {
        eprintln!("[bench] {label} — run {}/{}", i + 1, RUNS);
        runs.push(run_scenario(sys, label, connections, opts).await);
    }
    let speeds: Vec<u64> = runs.iter().map(|r| r.avg_bps).collect();
    let median_bps = median_u64(speeds.clone());
    let mean_bps = (speeds.iter().sum::<u64>() as f64 / speeds.len() as f64) as u64;
    let mut elapsed: Vec<Duration> = runs.iter().map(|r| r.elapsed).collect();
    elapsed.sort_unstable();
    let median_elapsed = elapsed[elapsed.len() / 2];
    let rep = runs
        .iter()
        .min_by_key(|r| (r.avg_bps as i64 - median_bps as i64).unsigned_abs())
        .unwrap();
    let mut cpu: Vec<f32> = runs.iter().map(|r| r.cpu_percent).collect();
    cpu.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mut rss: Vec<u64> = runs.iter().map(|r| r.rss_bytes).collect();
    rss.sort_unstable();
    ScenarioStats {
        label: label.to_string(),
        connections_seen: runs.iter().map(|r| r.connections_seen).max().unwrap_or(0),
        median_elapsed,
        median_bps,
        mean_bps,
        min_bps: *speeds.iter().min().unwrap(),
        max_bps: *speeds.iter().max().unwrap(),
        peak_bps: runs.iter().map(|r| r.peak_bps).max().unwrap_or(0),
        retries: runs.iter().map(|r| r.retries).sum(),
        integrity_ok: runs.iter().all(|r| r.integrity_ok),
        cpu_percent: cpu[cpu.len() / 2],
        rss_bytes: rss[rss.len() / 2],
        timeline: rep.timeline.clone(),
    }
}

fn mibbps(bps: u64) -> String {
    format!("{:.1}", bps as f64 / (1024.0 * 1024.0))
}

fn print_table(results: &[ScenarioStats]) {
    println!();
    println!(
        "| 场景 | 并发 | 中位耗时 | 中位速度 | 平均速度 | 区间(min~max) | 峰值 | 重试 | 完整性 | CPU% | RSS MiB |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|");
    for r in results {
        println!(
            "| {} | {} | {:.2}s | {} | {} | {}~{} | {} | {} | {} | {:.0} | {} |",
            r.label,
            r.connections_seen,
            r.median_elapsed.as_secs_f64(),
            mibbps(r.median_bps),
            mibbps(r.mean_bps),
            mibbps(r.min_bps),
            mibbps(r.max_bps),
            mibbps(r.peak_bps),
            r.retries,
            if r.integrity_ok { "OK" } else { "FAIL" },
            r.cpu_percent,
            r.rss_bytes / (1024 * 1024),
        );
    }
    println!("\n（每场景 {RUNS} 次独立运行；速度单位 MiB/s；CPU/RSS 为近似采样）");
}

fn print_timeline(label: &str, timeline: &[(f64, u64)]) {
    println!("\n### {label} — 累计下载与区间速率（代表性运行）");
    println!("| 时刻(s) | 累计 MiB | 区间速率 MiB/s |");
    println!("|---:|---:|---:|");
    let (mut prev_t, mut prev_b) = (0.0f64, 0u64);
    for (t, b) in timeline {
        let dt = (t - prev_t).max(0.001);
        let rate = b.saturating_sub(prev_b) as f64 / dt / (1024.0 * 1024.0);
        println!(
            "| {t:.2} | {:.1} | {rate:.1} |",
            *b as f64 / (1024.0 * 1024.0),
        );
        prev_t = *t;
        prev_b = *b;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "throughput benchmark; run explicitly with --ignored"]
async fn benchmark_matrix() {
    let mut sys = System::new();
    let mut results = Vec::new();

    for n in [1u32, 4, 8, 16, 32, 64] {
        results.push(
            run_scenario_stats(
                &mut sys,
                &format!("固定 {n} 连接"),
                Some(n),
                Opts::default(),
            )
            .await,
        );
    }
    let adaptive = run_scenario_stats(
        &mut sys,
        "自适应（动态分段）",
        None,
        Opts {
            window_ms: 250,
            ..Default::default()
        },
    )
    .await;
    print_timeline("自适应（动态分段）", &adaptive.timeline);
    results.push(adaptive);
    results.push(
        run_scenario_stats(
            &mut sys,
            "自适应（总带宽 32MiB/s 封顶）",
            None,
            Opts {
                total_bps: Some(TOTAL_BPS),
                window_ms: 250,
                ..Default::default()
            },
        )
        .await,
    );
    results.push(
        run_scenario_stats(
            &mut sys,
            "高带宽本地（无节流，固定16）",
            Some(16),
            Opts {
                per_conn_throttle: false,
                ..Default::default()
            },
        )
        .await,
    );
    results.push(
        run_scenario_stats(
            &mut sys,
            "连接数受限（429，上限4，固定8）",
            Some(8),
            Opts {
                max_concurrent: Some(4),
                patient_retry: true,
                ..Default::default()
            },
        )
        .await,
    );
    results.push(
        run_scenario_stats(
            &mut sys,
            "HTTPS 固定8（HTTP/1.1）",
            Some(8),
            Opts {
                https: true,
                http1: true,
                ..Default::default()
            },
        )
        .await,
    );
    results.push(
        run_scenario_stats(
            &mut sys,
            "HTTPS 固定8（HTTP/2）",
            Some(8),
            Opts {
                https: true,
                http1: false,
                ..Default::default()
            },
        )
        .await,
    );

    print_table(&results);
    for r in &results {
        assert!(r.integrity_ok, "{} produced wrong bytes", r.label);
    }
}

/// Focused, repeatable disk/network hot-path measurement.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "throughput benchmark; run explicitly"]
async fn hot_path_benchmark() {
    let mut sys = System::new();
    let result = run_scenario_stats(
        &mut sys,
        "local unthrottled 16",
        Some(16),
        Opts {
            per_conn_throttle: false,
            ..Default::default()
        },
    )
    .await;
    assert!(result.integrity_ok);
    print_table(&[result]);
}
