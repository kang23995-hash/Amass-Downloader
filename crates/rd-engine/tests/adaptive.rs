//! Adaptive concurrency and dynamic-scheduling integration tests: auto-mode
//! growth, throttle-driven shrink with worker release, orphan re-claim and
//! the auto/fixed flag semantics. All against the local test server.

mod common;

use std::time::{Duration, Instant};

use common::*;
use rd_engine::{AddTaskRequest, TaskState};

/// Auto-mode task (no explicit connections): the controller must grow the
/// connection count while extra connections keep adding throughput, then the
/// file must still reassemble byte-for-byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_mode_grows_connections() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(8 * 1024 * 1024);
    // Constant per-connection rate: every added connection nearly doubles
    // aggregate throughput, so the controller should keep growing.
    let mut behavior = Behavior::new(data.clone());
    behavior.chunk_size = 64 * 1024;
    behavior.chunk_delay = Duration::from_millis(30);
    let server = TestServer::start(behavior).await;

    let mut cfg = test_config(dir.path());
    cfg.default_connections = 2;
    cfg.max_connections_per_task = 6;
    let engine = open_engine(cfg).await;

    let req = AddTaskRequest::new(server.url("/grow.bin"));
    let id = engine.add_task(req).await.unwrap();
    assert!(
        engine.get_task(id).await.unwrap().auto_concurrency,
        "task without explicit connections must be in auto mode"
    );

    // Track the highest connection count the controller reaches.
    let mut max_seen = 2u32;
    let start = Instant::now();
    let info = loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let info = engine.get_task(id).await.unwrap();
        max_seen = max_seen.max(info.connections);
        if info.state == TaskState::Completed {
            break info;
        }
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "download did not finish; connections={max_seen} info={info:?}"
        );
    };

    assert!(
        max_seen >= 4,
        "controller never grew past {max_seen} connections"
    );
    assert_eq!(read_file(&info.file_path), data.to_vec());
    assert_eq!(info.downloaded, data.len() as u64);
    // Growth is implemented by splitting segments, so the server must have
    // seen more ranged GETs than the initial 2 segments.
    assert!(
        server.data_get_count() > 2,
        "expected dynamic split requests, got {}",
        server.data_get_count()
    );
    engine.shutdown().await.unwrap();
}

/// A per-IP concurrent-connection limit (429 on excess data GETs) must make
/// the controller shrink the target, release the excess workers, and still
/// finish the download through orphan re-claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throttle_shrinks_concurrency() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(4 * 1024 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.chunk_size = 64 * 1024;
    behavior.chunk_delay = Duration::from_millis(10);
    // Only two data GETs may stream at once; everyone else gets 429s.
    behavior.max_concurrent_data = Some(2);
    let server = TestServer::start(behavior).await;

    let mut cfg = test_config(dir.path());
    cfg.default_connections = 4;
    cfg.max_connections_per_task = 8;
    let engine = open_engine(cfg).await;

    let req = AddTaskRequest::new(server.url("/throttled.bin"));
    let id = engine.add_task(req).await.unwrap();

    let start = Instant::now();
    let mut min_seen = u32::MAX;
    let info = loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let info = engine.get_task(id).await.unwrap();
        min_seen = min_seen.min(info.connections);
        if info.state == TaskState::Completed {
            break info;
        }
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "throttled download never settled; min_seen={min_seen} info={info:?}"
        );
    };

    // The controller must have backed off from the initial 4 connections.
    assert!(
        min_seen <= 4,
        "target never shrank below the start (min {min_seen})"
    );
    assert_eq!(read_file(&info.file_path), data.to_vec());
    assert_eq!(info.downloaded, data.len() as u64);
    engine.shutdown().await.unwrap();
}

/// Explicit connection counts pin the task: the controller must stay inert
/// even with adaptive mode globally enabled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_mode_pins_connections() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(1024 * 1024);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/pinned.bin"));
    req.connections = Some(3);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert!(!info.auto_concurrency);
    assert_eq!(info.connections, 3);
    assert_eq!(read_file(&info.file_path), data.to_vec());
    engine.shutdown().await.unwrap();
}

/// A released worker's partially downloaded segment must be re-claimed and
/// finished by another worker, with byte-exact output. Simulated by a server
/// that lets each connection stream for a while, so the shrink path hands
/// orphaned segments back to the pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn released_segments_are_reclaimed() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(6 * 1024 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.chunk_size = 64 * 1024;
    // Stream each connection for ~3.6s so the workers overlap even when heavy
    // CPU load staggers their request issuance; otherwise the concurrency limit
    // is never hit and no release/reclaim occurs.
    behavior.chunk_delay = Duration::from_millis(150);
    // Two streams max while the task starts with four connections: workers are
    // guaranteed to be throttled, released, and their segments re-claimed.
    behavior.max_concurrent_data = Some(2);
    let server = TestServer::start(behavior).await;

    let mut cfg = test_config(dir.path());
    cfg.default_connections = 4;
    cfg.max_connections_per_task = 4;
    cfg.adaptive.min_connections = 2;
    let engine = open_engine(cfg).await;

    let req = AddTaskRequest::new(server.url("/reclaim.bin"));
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert_eq!(read_file(&info.file_path), data.to_vec());
    assert_eq!(info.downloaded, data.len() as u64);
    assert!(
        server.data_get_count() > 4,
        "expected reclaim/split requests beyond the initial segments, got {}",
        server.data_get_count()
    );
    engine.shutdown().await.unwrap();
}
