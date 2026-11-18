mod common;

use std::time::Duration;

use common::*;
use rd_engine::{AddTaskRequest, Event, TaskState};

/// Scenario 6: pause mid-download keeps durable data, resume finishes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_and_resume() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(512 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.chunk_size = 16 * 1024;
    behavior.chunk_delay = Duration::from_millis(20);
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/pause.bin"));
    req.connections = Some(1);
    let id = engine.add_task(req).await.unwrap();

    wait_downloaded(&engine, id, 128 * 1024).await;
    engine.pause_task(id).await.unwrap();
    let paused = engine.get_task(id).await.unwrap();
    assert_eq!(paused.state, TaskState::Paused);
    assert!(paused.downloaded > 0);
    assert!(paused.part_path.exists(), "part file kept while paused");

    // Progress must not advance while paused.
    let frozen = paused.downloaded;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(engine.get_task(id).await.unwrap().downloaded, frozen);

    engine.resume_task(id).await.unwrap();
    let done = wait_completed(&engine, id).await;
    assert_eq!(read_file(&done.file_path), data.to_vec());
    assert!(!done.part_path.exists());

    // Resuming must reuse the partial data (a Range request from offset > 0).
    let resumed = server.requests().iter().any(|r| {
        r.range
            .as_deref()
            .and_then(|s| s.strip_prefix("bytes="))
            .and_then(|s| s.split('-').next())
            .and_then(|s| s.parse::<u64>().ok())
            .map(|start| start > 0)
            .unwrap_or(false)
    });
    assert!(resumed, "resume should request a non-zero offset");
    engine.shutdown().await.unwrap();
}

/// Scenario 7: a hard crash (runtime dropped without shutdown) is recovered
/// from SQLite + the part file, and the task resumes to a correct file.
#[test]
fn crash_recovery_resumes_from_disk() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(512 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.chunk_size = 8 * 1024;
    behavior.chunk_delay = Duration::from_millis(30);

    // The server lives on its own runtime so it survives the engine "crash".
    let server_rt = tokio::runtime::Runtime::new().unwrap();
    let server = server_rt.block_on(TestServer::start(behavior));
    let url = server.url("/crash.bin");

    let id = {
        let rt1 = tokio::runtime::Runtime::new().unwrap();
        rt1.block_on(async {
            let engine = open_engine(test_config(dir.path())).await;
            let mut req = AddTaskRequest::new(url.clone());
            req.connections = Some(1);
            let id = engine.add_task(req).await.unwrap();
            wait_downloaded(&engine, id, 128 * 1024).await;
            // Let the periodic tick persist task-level progress before crashing.
            tokio::time::sleep(Duration::from_millis(600)).await;
            let info = engine.get_task(id).await.unwrap();
            assert!(info.downloaded > 0);
            assert!(info.part_path.exists());
            id
            // engine dropped without shutdown; rt1 dropped below == crash
        })
    };

    let rt2 = tokio::runtime::Runtime::new().unwrap();
    rt2.block_on(async {
        let engine = open_engine(test_config(dir.path())).await;
        let info = engine.get_task(id).await.unwrap();
        assert_eq!(
            info.state,
            TaskState::Paused,
            "crashed task recovered as paused"
        );
        assert!(
            info.downloaded > 0,
            "recovered downloaded={}",
            info.downloaded
        );
        assert!(info.part_path.exists());
        assert!(std::fs::metadata(&info.part_path).unwrap().len() > 0);

        engine.resume_task(id).await.unwrap();
        let done = wait_completed(&engine, id).await;
        assert_eq!(read_file(&done.file_path), data.to_vec());
        engine.shutdown().await.unwrap();
    });
}

/// Scenario 10: the remote resource changes mid-download; the engine detects
/// it via If-Range, discards the stale partial data and restarts cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_change_restarts_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let v1 = pattern_data(128 * 1024);
    let v2 = bytes::Bytes::from(vec![0xABu8; 200 * 1024]);
    let mut behavior = Behavior::new(v1.clone());
    behavior.etag = Some("\"v1\"".into());
    behavior.switch_after = Some(1);
    behavior.switch_data = Some(v2.clone());
    behavior.switch_etag = Some("\"v2\"".into());
    behavior.drop_first = 1;
    behavior.drop_after_bytes = 16 * 1024;
    behavior.chunk_size = 8 * 1024;
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;
    let mut events = engine.subscribe();

    let mut req = AddTaskRequest::new(server.url("/changing.bin"));
    req.connections = Some(1);
    let id = engine.add_task(req).await.unwrap();
    let done = wait_completed(&engine, id).await;

    assert_eq!(
        read_file(&done.file_path),
        v2.to_vec(),
        "must contain the new resource, not a mix"
    );
    assert_eq!(done.total_size, Some(v2.len() as u64));
    assert_eq!(done.etag.as_deref(), Some("\"v2\""));
    let evs = {
        let mut out = Vec::new();
        while let Ok(ev) = events.try_recv() {
            out.push(ev);
        }
        out
    };
    assert!(evs
        .iter()
        .any(|e| matches!(e, Event::ResourceChanged { .. })));
    engine.shutdown().await.unwrap();
}

/// A completed task whose final file was deleted is reopened on startup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_file_missing_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(32 * 1024);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let cfg = test_config(dir.path());

    let id = {
        let engine = open_engine(cfg.clone()).await;
        let mut req = AddTaskRequest::new(server.url("/gone.bin"));
        req.connections = Some(1);
        let id = engine.add_task(req).await.unwrap();
        let done = wait_completed(&engine, id).await;
        std::fs::remove_file(&done.file_path).unwrap();
        engine.shutdown().await.unwrap();
        id
    };

    let engine = open_engine(cfg).await;
    let info = engine.get_task(id).await.unwrap();
    assert_eq!(info.state, TaskState::Paused);
    engine.resume_task(id).await.unwrap();
    let done = wait_completed(&engine, id).await;
    assert_eq!(read_file(&done.file_path), data.to_vec());
    engine.shutdown().await.unwrap();
}

/// A pause acknowledgement is also a barrier for all queued disk writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_joins_pending_writes_before_resume() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(8 * 1024 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.chunk_size = 16 * 1024;
    behavior.chunk_delay = Duration::from_millis(10);
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;
    let mut req = AddTaskRequest::new(server.url("/barrier.bin"));
    req.connections = Some(4);
    let id = engine.add_task(req).await.unwrap();
    wait_downloaded(&engine, id, 512 * 1024).await;
    engine.pause_task(id).await.unwrap();
    let paused = engine.get_task(id).await.unwrap();
    let snapshot = read_file(&paused.part_path);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        read_file(&paused.part_path),
        snapshot,
        "write after pause acknowledgement"
    );
    engine.resume_task(id).await.unwrap();
    let done = wait_completed(&engine, id).await;
    assert_eq!(read_file(&done.file_path), data.to_vec());
    engine.shutdown().await.unwrap();
}

/// Persisted split rows must not each spawn a connection on resume.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_respects_reduced_connection_cap() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(64 * 1024 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.chunk_size = 16 * 1024;
    behavior.chunk_delay = Duration::from_millis(10);
    let server = TestServer::start(behavior).await;
    let mut cfg = test_config(dir.path());
    cfg.max_connections_per_task = 64;
    let engine = open_engine(cfg.clone()).await;
    let mut req = AddTaskRequest::new(server.url("/resume-cap.bin"));
    req.connections = Some(64);
    let id = engine.add_task(req).await.unwrap();
    wait_downloaded(&engine, id, 512 * 1024).await;
    engine.pause_task(id).await.unwrap();
    engine.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cfg.max_connections_per_task = 4;
    let engine = open_engine(cfg).await;
    engine.resume_task(id).await.unwrap();
    loop {
        let info = engine.get_task(id).await.unwrap();
        assert!(
            server.state.inflight_data.load(Ordering::SeqCst) <= 4,
            "resume spawned more requests than the configured cap"
        );
        if info.state == TaskState::Completed {
            assert_eq!(read_file(&info.file_path), data.to_vec());
            break;
        }
        assert_ne!(info.state, TaskState::Failed, "{:?}", info.error);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_range_pause_restarts_without_appending_duplicate_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(1024 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.support_ranges = false;
    behavior.advertise_ranges = false;
    behavior.chunk_size = 16 * 1024;
    behavior.chunk_delay = Duration::from_millis(20);
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;
    let id = engine
        .add_task(AddTaskRequest::new(server.url("/no-range.bin")))
        .await
        .unwrap();
    wait_downloaded(&engine, id, 256 * 1024).await;
    engine.pause_task(id).await.unwrap();
    engine.resume_task(id).await.unwrap();
    let done = wait_completed(&engine, id).await;
    assert_eq!(done.downloaded, data.len() as u64);
    assert_eq!(read_file(&done.file_path), data.to_vec());
    engine.shutdown().await.unwrap();
}
