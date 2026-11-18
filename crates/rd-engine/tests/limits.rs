mod common;

use std::time::{Duration, Instant};

use common::*;
use rd_engine::AddTaskRequest;

/// Scenario 12a: a per-task cap throttles the download well below loopback speed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_task_rate_limit_caps_speed() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(512 * 1024);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/capped.bin"));
    req.connections = Some(1);
    req.rate_limit_bps = Some(100 * 1024);
    let start = Instant::now();
    let id = engine.add_task(req).await.unwrap();
    let done = wait_completed(&engine, id).await;
    let elapsed = start.elapsed();

    assert_eq!(read_file(&done.file_path), data.to_vec());
    assert_eq!(done.rate_limit_bps, Some(100 * 1024));
    // 512 KiB at 100 KiB/s is ~4.5s; unlimited loopback would be milliseconds.
    assert!(
        elapsed >= Duration::from_millis(2500),
        "too fast: {elapsed:?}"
    );
    engine.shutdown().await.unwrap();
}

/// Scenario 12b: the global cap is shared across tasks, not duplicated per task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_rate_limit_is_shared() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(256 * 1024);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let mut cfg = test_config(dir.path());
    cfg.max_concurrent_tasks = 2;
    cfg.rate_limit.global_bps = 100 * 1024;
    let engine = open_engine(cfg).await;
    assert_eq!(engine.global_rate_limit(), 100 * 1024);

    let start = Instant::now();
    let mut ids = Vec::new();
    for i in 0..2 {
        let mut req = AddTaskRequest::new(server.url(&format!("/shared{i}.bin")));
        req.connections = Some(1);
        ids.push(engine.add_task(req).await.unwrap());
    }
    for id in &ids {
        let done = wait_completed(&engine, *id).await;
        assert_eq!(read_file(&done.file_path), data.to_vec());
    }
    let elapsed = start.elapsed();

    // 512 KiB total through one 100 KiB/s bucket is ~4.5s. If the global cap
    // were duplicated per task, both would finish in ~2s.
    assert!(
        elapsed >= Duration::from_millis(3000),
        "global cap not shared: {elapsed:?}"
    );
    engine.shutdown().await.unwrap();
}

/// Scenario 12c: the global cap can be changed at runtime without breaking the
/// in-flight download.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_global_limit_adjustment() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(384 * 1024);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/dynamic.bin"));
    req.connections = Some(1);
    let id = engine.add_task(req).await.unwrap();
    wait_downloaded(&engine, id, 64 * 1024).await;

    engine.set_global_rate_limit(Some(80 * 1024));
    assert_eq!(engine.global_rate_limit(), 80 * 1024);
    let done = wait_completed(&engine, id).await;
    assert_eq!(read_file(&done.file_path), data.to_vec());

    engine.set_global_rate_limit(None);
    assert_eq!(engine.global_rate_limit(), 0);
    engine.shutdown().await.unwrap();
}

/// Per-task cap can be set and cleared through the public API at runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_task_rate_limit_at_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(64 * 1024);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/runtime.bin"));
    req.connections = Some(1);
    req.auto_start = false;
    let id = engine.add_task(req).await.unwrap();

    engine
        .set_task_rate_limit(id, Some(123 * 1024))
        .await
        .unwrap();
    assert_eq!(
        engine.get_task(id).await.unwrap().rate_limit_bps,
        Some(123 * 1024)
    );
    engine.set_task_rate_limit(id, None).await.unwrap();
    assert_eq!(engine.get_task(id).await.unwrap().rate_limit_bps, None);

    engine.resume_task(id).await.unwrap();
    let done = wait_completed(&engine, id).await;
    assert_eq!(read_file(&done.file_path), data.to_vec());
    engine.shutdown().await.unwrap();
}
