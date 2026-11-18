mod common;

use common::*;
use rd_engine::{AddTaskRequest, Event};

fn drain(events: &mut tokio::sync::broadcast::Receiver<Event>) -> Vec<Event> {
    let mut out = Vec::new();
    while let Ok(ev) = events.try_recv() {
        out.push(ev);
    }
    out
}

/// Scenario 8: the connection drops mid-stream twice; the worker retries,
/// resumes from the dropped offset and finishes with correct bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn network_interrupt_retries_and_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(128 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.drop_first = 2;
    behavior.drop_after_bytes = 16 * 1024;
    behavior.chunk_size = 8 * 1024;
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;
    let mut events = engine.subscribe();

    let mut req = AddTaskRequest::new(server.url("/flaky.bin"));
    req.connections = Some(1);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert_eq!(read_file(&info.file_path), data.to_vec());
    assert!(
        server.data_get_count() >= 3,
        "expected retries, got {}",
        server.data_get_count()
    );
    let evs = drain(&mut events);
    assert!(evs.iter().any(|e| matches!(e, Event::TaskRetrying { .. })));
    // A later request resumed from a non-zero offset.
    let resumed = server.requests().iter().any(|r| {
        r.range
            .as_deref()
            .and_then(|s| s.strip_prefix("bytes="))
            .and_then(|s| s.split('-').next())
            .and_then(|start| start.parse::<u64>().ok())
            .map(|start| start > 0)
            .unwrap_or(false)
    });
    assert!(
        resumed,
        "expected a Range request resuming from a non-zero offset"
    );
    engine.shutdown().await.unwrap();
}

/// Scenario 9a: a 404 is fatal and not retried.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn not_found_fails_fast() {
    let dir = tempfile::tempdir().unwrap();
    let mut behavior = Behavior::new(pattern_data(1024));
    behavior.always_status = Some(404);
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;

    let id = engine
        .add_task(AddTaskRequest::new(server.url("/missing.bin")))
        .await
        .unwrap();
    let info = wait_failed(&engine, id).await;
    assert!(info.error.unwrap().contains("404"));
    engine.shutdown().await.unwrap();
}

/// Scenario 9b: a persistent 500 is retried up to the limit, then fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_error_retries_then_fails() {
    let dir = tempfile::tempdir().unwrap();
    let mut behavior = Behavior::new(pattern_data(1024));
    behavior.always_status = Some(500);
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;
    let mut events = engine.subscribe();

    let id = engine
        .add_task(AddTaskRequest::new(server.url("/boom.bin")))
        .await
        .unwrap();
    let info = wait_failed(&engine, id).await;
    assert!(info.error.unwrap().contains("500"));
    let evs = drain(&mut events);
    let retries = evs
        .iter()
        .filter(|e| matches!(e, Event::TaskRetrying { .. }))
        .count();
    assert!(retries >= 1, "expected retry events, got {retries}");
    engine.shutdown().await.unwrap();
}

/// Scenario 9c: 429 with Retry-After is honored, then the download succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limited_429_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(32 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.fail_first = 2;
    behavior.fail_status = 429;
    behavior.retry_after = Some(1);
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/throttle.bin"));
    req.connections = Some(1);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert_eq!(read_file(&info.file_path), data.to_vec());
    assert!(server.data_get_count() >= 3);
    engine.shutdown().await.unwrap();
}

/// A transient 503 on the first attempts recovers without failing the task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_503_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(48 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.fail_first = 2;
    behavior.fail_status = 503;
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/recover.bin"));
    req.connections = Some(1);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;
    assert_eq!(read_file(&info.file_path), data.to_vec());
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_range_stream_error_replays_without_duplicate_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(1024 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.support_ranges = false;
    behavior.advertise_ranges = false;
    behavior.drop_first = 1;
    behavior.drop_after_bytes = 128 * 1024;
    behavior.chunk_size = 16 * 1024;
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;
    let id = engine
        .add_task(AddTaskRequest::new(server.url("/no-range-retry.bin")))
        .await
        .unwrap();
    let done = wait_completed(&engine, id).await;
    assert_eq!(done.downloaded, data.len() as u64);
    assert_eq!(read_file(&done.file_path), data.to_vec());
    engine.shutdown().await.unwrap();
}
