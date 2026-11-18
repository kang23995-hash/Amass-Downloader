mod common;

use std::time::Duration;

use common::*;
use rd_engine::AddTaskRequest;

/// Scenario 1: plain HTTP download of a small file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_single_connection() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(128 * 1024);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/file.bin"));
    req.connections = Some(1);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert_eq!(info.total_size, Some(data.len() as u64));
    assert_eq!(info.downloaded, data.len() as u64);
    assert!(info.range_supported);
    assert_eq!(read_file(&info.file_path), data.to_vec());
    assert!(
        !info.part_path.exists(),
        "part file must be gone after commit"
    );
    engine.shutdown().await.unwrap();
}

/// Scenario 2: HTTPS download with a private CA trusted via config.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn https_with_private_ca() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(96 * 1024);
    let (server, ca_pem) = TestServer::start_https(Behavior::new(data.clone())).await;
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, ca_pem).unwrap();

    let mut cfg = test_config(dir.path());
    cfg.tls.ca_cert_path = Some(ca_path);
    let engine = open_engine(cfg).await;

    let mut req = AddTaskRequest::new(server.url("/secure.bin"));
    req.connections = Some(2);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert_eq!(read_file(&info.file_path), data.to_vec());
    assert!(info.final_url.unwrap().starts_with("https://"));
    engine.shutdown().await.unwrap();
}

/// Scenario 4: a server that does not support Range falls back to one stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_range_falls_back_to_single_stream() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(200 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.support_ranges = false;
    behavior.advertise_ranges = false;
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;

    // Ask for 4 connections; the engine must collapse to a single open stream.
    let mut req = AddTaskRequest::new(server.url("/norange.bin"));
    req.connections = Some(4);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert!(!info.range_supported);
    assert_eq!(info.connections, 1);
    assert_eq!(read_file(&info.file_path), data.to_vec());
    engine.shutdown().await.unwrap();
}

/// Scenario 14a: SHA-256 verification succeeds and is recorded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sha256_matching() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(64 * 1024);
    let expected = sha256_hex(&data);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/hash.bin"));
    req.connections = Some(2);
    req.expected_sha256 = Some(expected.clone());
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert_eq!(info.actual_sha256.as_deref(), Some(expected.as_str()));
    assert_eq!(read_file(&info.file_path), data.to_vec());
    engine.shutdown().await.unwrap();
}

/// Scenario 14b: a wrong SHA-256 fails the task and leaves no final file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sha256_mismatch_fails() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(64 * 1024);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/bad.bin"));
    req.connections = Some(2);
    req.expected_sha256 = Some("0".repeat(64));
    let id = engine.add_task(req).await.unwrap();
    let info = wait_failed(&engine, id).await;

    assert!(info.error.unwrap().to_lowercase().contains("checksum"));
    assert!(
        !info.file_path.exists(),
        "no final file on checksum failure"
    );
    assert!(
        !info.part_path.exists(),
        "part file cleaned up on checksum failure"
    );
    engine.shutdown().await.unwrap();
}

/// Scenario 14c: BLAKE3 verification succeeds and is recorded alongside
/// SHA-256 when both are requested.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blake3_verification_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(64 * 1024);
    let expected_blake3 = blake3::Hasher::new()
        .update(&data)
        .finalize()
        .to_hex()
        .to_string();
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/blake3.bin"));
    req.connections = Some(2);
    req.expected_blake3 = Some(expected_blake3.clone());
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert_eq!(
        info.actual_blake3.as_deref(),
        Some(expected_blake3.as_str())
    );
    assert_eq!(
        info.actual_sha256, None,
        "no sha256 requested, none computed"
    );
    assert_eq!(read_file(&info.file_path), data.to_vec());
    engine.shutdown().await.unwrap();
}

/// Scenario 14d: a wrong BLAKE3 fails the task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blake3_mismatch_fails() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(64 * 1024);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/bad3.bin"));
    req.connections = Some(2);
    req.expected_blake3 = Some("0".repeat(64));
    let id = engine.add_task(req).await.unwrap();
    let info = wait_failed(&engine, id).await;

    assert!(info.error.unwrap().to_lowercase().contains("checksum"));
    assert!(!info.file_path.exists());
    engine.shutdown().await.unwrap();
}

/// Content-Disposition supplies the file name when none is given.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filename_from_content_disposition() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(8 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.content_disposition = Some("attachment; filename=\"report.pdf\"".into());
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/"));
    req.connections = Some(1);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert_eq!(info.file_name, "report.pdf");
    assert_eq!(info.file_path.file_name().unwrap(), "report.pdf");
    engine.shutdown().await.unwrap();
}

/// HEAD rejected (405) -> the engine probes with a Range GET instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn head_rejected_uses_get_probe() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(40 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.head_mode = HeadMode::Reject;
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/nohead.bin"));
    req.connections = Some(1);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_completed(&engine, id).await;

    assert_eq!(read_file(&info.file_path), data.to_vec());
    assert!(server
        .requests()
        .iter()
        .any(|r| r.method == "GET" && r.range.as_deref() == Some("bytes=0-0")));
    engine.shutdown().await.unwrap();
}

/// Unknown length (no Content-Length) streams to completion as one segment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_length_stream() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(50 * 1024);
    let mut behavior = Behavior::new(data.clone());
    behavior.no_content_length = true;
    behavior.support_ranges = false;
    behavior.advertise_ranges = false;
    behavior.chunk_size = 8 * 1024;
    let server = TestServer::start(behavior).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/stream.bin"));
    req.connections = Some(1);
    let id = engine.add_task(req).await.unwrap();
    let info = wait_for(&engine, id, Duration::from_secs(30), |i| {
        i.state == rd_engine::TaskState::Completed
    })
    .await;

    assert_eq!(read_file(&info.file_path), data.to_vec());
    engine.shutdown().await.unwrap();
}

/// Every GET the engine issues must ask for the identity content coding so
/// Range offsets always refer to the stored representation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_gets_request_identity_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern_data(96 * 1024);
    let server = TestServer::start(Behavior::new(data.clone())).await;
    let engine = open_engine(test_config(dir.path())).await;

    let mut req = AddTaskRequest::new(server.url("/identity.bin"));
    req.connections = Some(2);
    let id = engine.add_task(req).await.unwrap();
    wait_completed(&engine, id).await;

    let gets: Vec<_> = server
        .requests()
        .into_iter()
        .filter(|r| r.method == "GET")
        .collect();
    assert!(gets.len() >= 3, "expected probe + data GETs");
    for r in &gets {
        assert_eq!(
            r.accept_encoding.as_deref(),
            Some("identity"),
            "GET {} must send Accept-Encoding: identity",
            r.range.as_deref().unwrap_or("(no range)")
        );
    }
    engine.shutdown().await.unwrap();
}
