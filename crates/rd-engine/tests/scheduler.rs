mod common;

use std::time::{Duration, Instant};

use common::*;
use rd_engine::{AddTaskRequest, TaskState};

/// Scenario 11: cancelling mid-download stops the task and removes the part file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_mid_download_removes_part() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(512 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.chunk_size = 16 * 1024;
    behavior.chunk_delay = Duration::from_millis(20);
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/cancel.bin"));
    req.connections = Some(1);
    let id = engine.add_task(req).await.unwrap();
    wait_downloaded(&engine, id, 128 * 1024).await;

    engine.cancel_task(id).await.unwrap();
    let info = engine.get_task(id).await.unwrap();
    assert_eq!(info.state, TaskState::Cancelled);
    assert!(!info.part_path.exists(), "part file removed on cancel");
    assert!(!info.file_path.exists(), "no final file on cancel");
    engine.shutdown().await.unwrap();
}

/// Scenario 13: an unwritable destination (a file in place of a directory)
/// fails the task with an error instead of panicking or hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_write_failure_fails_task() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(16 * 1024);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    // A regular file where a directory is expected makes part creation fail.
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, b"not a directory").unwrap();

    let mut req = AddTaskRequest::new(server.url("/fail.bin"));
    req.output_dir = Some(blocker);
    req.connections = Some(1);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_failed(&engine, id).await;
    assert!(info.error.is_some());
    engine.shutdown().await.unwrap();
}

/// Scenario 15: with several tasks competing, no more than max_concurrent_tasks
/// run at once, and every task still completes correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrency_limit_respected() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(64 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.chunk_size = 8 * 1024;
    behavior.chunk_delay = Duration::from_millis(15);
    let server = TestServer::start(behavior).await;
    let mut cfg = test_config(dir.path());
    cfg.max_concurrent_tasks = 2;
    let engine = open_engine(cfg).await;

    let mut ids = Vec::new();
    for i in 0..5 {
        let mut req = AddTaskRequest::new(server.url(&format!("/job{i}.bin")));
        req.connections = Some(1);
        ids.push(engine.add_task(req).await.unwrap());
    }

    let mut max_active = 0usize;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let list = engine.list_tasks().await.unwrap();
        let active = list.iter().filter(|t| t.state.is_active()).count();
        max_active = max_active.max(active);
        if list.iter().all(|t| t.state == TaskState::Completed) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "tasks did not finish; max_active={max_active}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    assert!(max_active <= 2, "concurrency limit exceeded: {max_active}");
    for id in &ids {
        let info = engine.get_task(*id).await.unwrap();
        assert_eq!(read_file(&info.file_path), data.to_vec());
    }
    engine.shutdown().await.unwrap();
}

/// With a single slot, tasks are serialized (never two active at once).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_slot_serializes_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(48 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.chunk_size = 8 * 1024;
    behavior.chunk_delay = Duration::from_millis(15);
    let server = TestServer::start(behavior).await;
    let mut cfg = test_config(dir.path());
    cfg.max_concurrent_tasks = 1;
    let engine = open_engine(cfg).await;

    let mut ids = Vec::new();
    for i in 0..3 {
        let mut req = AddTaskRequest::new(server.url(&format!("/seq{i}.bin")));
        req.connections = Some(1);
        ids.push(engine.add_task(req).await.unwrap());
    }

    let mut max_active = 0usize;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let list = engine.list_tasks().await.unwrap();
        max_active = max_active.max(list.iter().filter(|t| t.state.is_active()).count());
        if list.iter().all(|t| t.state == TaskState::Completed) {
            break;
        }
        assert!(Instant::now() < deadline, "tasks did not finish");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(max_active, 1, "single slot must serialize");
    engine.shutdown().await.unwrap();
}

/// Removing a completed task with delete_file erases the file; active tasks
/// must be cancelled before removal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remove_task_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(16 * 1024);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/remove.bin"));
    req.connections = Some(1);
    let id = engine.add_task(req).await.unwrap();
    let done = wait_completed(&engine, id).await;
    let path = done.file_path.clone();
    assert!(path.exists());

    engine.remove_task(id, true).await.unwrap();
    assert!(!path.exists(), "delete_file removes the final file");
    assert!(engine.get_task(id).await.is_err(), "task record gone");
    engine.shutdown().await.unwrap();
}
