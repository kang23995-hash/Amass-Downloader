mod common;

use std::time::Duration;

use common::*;
use rd_engine::AddTaskRequest;

/// Parse the start/end of a `bytes=a-b` range header.
fn parse_range_header(value: &str) -> Option<(u64, u64)> {
    let rest = value.strip_prefix("bytes=")?;
    let (a, b) = rest.split_once('-')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

/// Scenario 3 + 5: a Range server is split into several connections whose
/// ranges partition the file exactly, and the reassembled bytes are correct.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_connection_partitions_file() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(256 * 1024);
    let total = data.len() as u64;
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/big.bin"));
    req.connections = Some(4);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert_eq!(info.connections, 4);
    assert!(info.range_supported);
    assert_eq!(info.total_size, Some(total));
    assert_eq!(info.downloaded, total);
    assert_eq!(read_file(&info.file_path), data.to_vec());

    // The worker ranges (excluding the 0-0 probe) must cover [0, total).
    let mut ranges: Vec<(u64, u64)> = server
        .requests()
        .iter()
        .filter(|r| r.method == "GET")
        .filter_map(|r| r.range.as_deref())
        .filter(|r| *r != "bytes=0-0")
        .filter_map(parse_range_header)
        .collect();
    ranges.sort();
    assert!(
        ranges.len() >= 4,
        "expected >=4 segment requests, got {ranges:?}"
    );
    let mut expect = 0u64;
    for (start, end) in &ranges {
        assert_eq!(
            *start, expect,
            "gap or overlap in segment ranges: {ranges:?}"
        );
        expect = end + 1;
    }
    assert_eq!(expect, total);
    engine.shutdown().await.unwrap();
}

/// Eight connections over a larger file still reassemble byte-for-byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eight_connections_correct_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(1024 * 1024);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/eight.bin"));
    req.connections = Some(8);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert_eq!(info.connections, 8);
    assert_eq!(read_file(&info.file_path), data.to_vec());
    engine.shutdown().await.unwrap();
}

/// Every worker sends If-Range with the probed validator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workers_send_if_range() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(128 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.etag = Some("\"abc123\"".into());
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/ifrange.bin"));
    req.connections = Some(3);
    let id = engine.add_task(req).await.unwrap();
    wait_completed(&engine, id).await;

    let requests = server.requests();
    let worker_gets: Vec<_> = requests
        .iter()
        .filter(|r| r.method == "GET" && r.range.as_deref() != Some("bytes=0-0"))
        .collect();
    assert!(!worker_gets.is_empty());
    for r in worker_gets {
        assert_eq!(r.if_range.as_deref(), Some("\"abc123\""));
    }
    engine.shutdown().await.unwrap();
}

/// min_segment_size caps the connection count for small files.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_file_uses_fewer_segments() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(8 * 1024);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let mut cfg = test_config(dir.path());
    cfg.min_segment_size = 4096;
    let engine = open_engine(cfg).await;

    let mut req = AddTaskRequest::new(server.url("/small.bin"));
    req.connections = Some(8);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    // 8 KiB / 4 KiB minimum => at most 2 segments.
    assert!(info.connections <= 2, "connections={}", info.connections);
    assert_eq!(read_file(&info.file_path), data.to_vec());
    engine.shutdown().await.unwrap();
}

/// IDM-style dynamic splitting: the last quarter of the file is served at a
/// crawl while the first three quarters are instant. As the fast segment
/// workers finish they must steal the slow segment's tail (extra ranged GETs
/// beyond the initial 4 connections) instead of idling, and the file still
/// reassembles byte-for-byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_segment_tail_gets_stolen() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(4 * 1024 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.chunk_size = 64 * 1024;
    behavior.chunk_delay = Duration::from_millis(60);
    behavior.slow_from_start = Some(3 * 1024 * 1024);
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/steal.bin"));
    req.connections = Some(4);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert_eq!(read_file(&info.file_path), data.to_vec());
    assert_eq!(info.downloaded, data.len() as u64);
    let gets = server.data_get_count();
    assert!(gets > 4, "expected at least one steal request, got {gets}");
    engine.shutdown().await.unwrap();
}

/// The engine's advertised ceiling: 64 concurrent connections partition a
/// multi-MiB file into 64 disjoint ranges and still reassemble exactly.
/// 64 is a cap, not a promise — the plan never creates more segments than
/// `total / min_segment_size`, which is asserted implicitly via the exact
/// range coverage below.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sixty_four_connections_partition_file() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(8 * 1024 * 1024);
    let total = data.len() as u64;
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/wide.bin"));
    req.connections = Some(64);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert_eq!(info.connections, 64);
    assert_eq!(info.total_size, Some(total));
    assert_eq!(info.downloaded, total);
    assert_eq!(read_file(&info.file_path), data.to_vec());

    // Worker ranges (excluding the 0-0 probe) must partition [0, total).
    let mut ranges: Vec<(u64, u64)> = server
        .requests()
        .iter()
        .filter(|r| r.method == "GET")
        .filter_map(|r| r.range.as_deref())
        .filter(|r| *r != "bytes=0-0")
        .filter_map(parse_range_header)
        .collect();
    ranges.sort();
    assert!(ranges.len() >= 64, "expected >=64 segment requests");
    let mut expect = 0u64;
    for (start, end) in &ranges {
        assert_eq!(*start, expect, "gap or overlap at offset {expect}");
        expect = end + 1;
    }
    assert_eq!(expect, total);
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_transport_frames_preserve_exact_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(8 * 1024 * 1024 + 137);
    let mut behavior = Behavior::new(data.clone());
    behavior.chunk_size = 1024 * 1024;
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;
    let mut req = AddTaskRequest::new(server.url("/large-frames.bin"));
    req.connections = Some(4);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;
    assert_eq!(info.downloaded, data.len() as u64);
    assert_eq!(read_file(&info.file_path), data.to_vec());
    engine.shutdown().await.unwrap();
}
