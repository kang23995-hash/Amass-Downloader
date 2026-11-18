//! Local HTTP/HTTPS test server with programmable behavior, plus shared
//! helpers for the integration tests. No public network is used.

#![allow(dead_code)]

use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::stream;
use http_body::Body as HttpBody;
use http_body_util::{combinators::BoxBody, BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::server::conn::{http1, http2};
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

use rd_engine::{Engine, EngineConfig, TaskId, TaskInfo, TaskState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadMode {
    Normal,
    Reject,
    NoLength,
}

/// Everything the fake server can be told to do. Tests build a value and
/// mutate the fields they care about; the rest stay at sensible defaults.
#[derive(Debug, Clone)]
pub struct Behavior {
    pub data: Bytes,
    pub support_ranges: bool,
    pub advertise_ranges: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_disposition: Option<String>,
    pub head_mode: HeadMode,
    /// Fail the first N data GETs with `fail_status`.
    pub fail_first: usize,
    pub fail_status: u16,
    pub retry_after: Option<u64>,
    /// Return this status for every request (probe included).
    pub always_status: Option<u16>,
    /// Abort the first N data GETs after sending `drop_after_bytes`.
    pub drop_first: usize,
    pub drop_after_bytes: u64,
    pub chunk_delay: Duration,
    pub chunk_size: usize,
    /// Apply `chunk_delay` only to ranged GETs starting at or after this
    /// offset; earlier ranges answer instantly. Simulates one slow segment so
    /// tests can exercise dynamic splitting.
    pub slow_from_start: Option<u64>,
    /// After this many data GETs, serve `switch_data` / `switch_etag`.
    pub switch_after: Option<usize>,
    pub switch_data: Option<Bytes>,
    pub switch_etag: Option<String>,
    pub no_content_length: bool,
    /// Reject data GETs with 429 while more than this many are in flight,
    /// simulating a per-IP concurrent-connection limit.
    pub max_concurrent_data: Option<usize>,
    /// Cap the server's *aggregate* data throughput in bytes/second across
    /// all connections (a shared token bucket), simulating a limited pipe.
    pub total_bps: Option<u64>,
}

impl Default for Behavior {
    fn default() -> Self {
        Self {
            data: Bytes::new(),
            support_ranges: true,
            advertise_ranges: true,
            etag: Some("\"v1\"".to_string()),
            last_modified: None,
            content_disposition: None,
            head_mode: HeadMode::Normal,
            fail_first: 0,
            fail_status: 500,
            retry_after: None,
            always_status: None,
            drop_first: 0,
            drop_after_bytes: 4096,
            chunk_delay: Duration::ZERO,
            chunk_size: 64 * 1024,
            switch_after: None,
            switch_data: None,
            switch_etag: None,
            no_content_length: false,
            slow_from_start: None,
            max_concurrent_data: None,
            total_bps: None,
        }
    }
}

impl Behavior {
    pub fn new(data: impl Into<Bytes>) -> Self {
        Self {
            data: data.into(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
    pub range: Option<String>,
    pub if_range: Option<String>,
    pub accept_encoding: Option<String>,
}

pub struct State {
    pub behavior: Mutex<Behavior>,
    pub data_get_count: AtomicUsize,
    /// Number of data GET bodies currently being streamed.
    pub inflight_data: Arc<AtomicUsize>,
    /// Shared throughput cap across all connections, if configured.
    pub bucket: Option<BandwidthBucket>,
    pub requests: Mutex<Vec<RecordedRequest>>,
}

/// A shared token-bucket-ish pacer: every served byte is scheduled against a
/// common timeline, so N connections together never exceed `bps`.
#[derive(Clone)]
pub struct BandwidthBucket {
    start: Instant,
    served: Arc<Mutex<u64>>,
    bps: u64,
}

impl BandwidthBucket {
    fn new(bps: u64) -> Self {
        Self {
            start: Instant::now(),
            served: Arc::new(Mutex::new(0)),
            bps: bps.max(1),
        }
    }

    async fn take(&self, n: u64) {
        let pos = {
            let mut served = self.served.lock().unwrap();
            let pos = *served;
            *served += n;
            pos
        };
        let target = self.start + Duration::from_nanos(pos * 1_000_000_000 / self.bps);
        let now = Instant::now();
        if target > now {
            tokio::time::sleep(target - now).await;
        }
    }
}

pub struct TestServer {
    addr: SocketAddr,
    scheme: &'static str,
    pub state: Arc<State>,
    shutdown: CancellationToken,
}

impl TestServer {
    pub async fn start(behavior: Behavior) -> Self {
        Self::start_inner(behavior, None).await
    }

    /// Start an HTTPS server; returns the server and the CA PEM to trust.
    pub async fn start_https(behavior: Behavior) -> (Self, String) {
        let (acceptor, ca_pem) = make_tls();
        let server = Self::start_inner(behavior, Some(acceptor)).await;
        (server, ca_pem)
    }

    async fn start_inner(behavior: Behavior, tls: Option<TlsAcceptor>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let state = Arc::new(State {
            behavior: Mutex::new(behavior.clone()),
            data_get_count: AtomicUsize::new(0),
            inflight_data: Arc::new(AtomicUsize::new(0)),
            bucket: behavior.total_bps.map(BandwidthBucket::new),
            requests: Mutex::new(Vec::new()),
        });
        let shutdown = CancellationToken::new();
        let scheme = if tls.is_some() { "https" } else { "http" };

        let accept_state = state.clone();
        let accept_shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = tokio::select! {
                    _ = accept_shutdown.cancelled() => break,
                    accepted = listener.accept() => match accepted {
                        Ok(v) => v,
                        Err(_) => continue,
                    },
                };
                let state = accept_state.clone();
                let tls = tls.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let state = state.clone();
                        async move { handle(req, state).await }
                    });
                    match tls {
                        Some(acceptor) => {
                            if let Ok(s) = acceptor.accept(stream).await {
                                let h2 = s.get_ref().1.alpn_protocol() == Some(b"h2".as_slice());
                                if h2 {
                                    let _ = http2::Builder::new(TokioExecutor::new())
                                        .initial_stream_window_size(2 * 1024 * 1024u32)
                                        .initial_connection_window_size(16 * 1024 * 1024u32)
                                        .serve_connection(TokioIo::new(s), service)
                                        .await;
                                } else {
                                    let _ = http1::Builder::new()
                                        .serve_connection(TokioIo::new(s), service)
                                        .await;
                                }
                            }
                        }
                        None => {
                            let _ = http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service)
                                .await;
                        }
                    }
                });
            }
        });

        Self {
            addr,
            scheme,
            state,
            shutdown,
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}://127.0.0.1:{}{}", self.scheme, self.addr.port(), path)
    }

    pub fn set_behavior(&self, behavior: Behavior) {
        *self.state.behavior.lock().unwrap() = behavior;
    }

    pub fn data_get_count(&self) -> usize {
        self.state.data_get_count.load(Ordering::SeqCst)
    }

    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.state.requests.lock().unwrap().clone()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

fn make_tls() -> (TlsAcceptor, String) {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};
    use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
    use tokio_rustls::rustls::ServerConfig;

    // Both `ring` and `aws-lc-rs` are enabled transitively, so rustls cannot
    // pick a process-default provider on its own; install one explicitly.
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();

    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().expect("ca key");
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca cert");

    let mut leaf_params = CertificateParams::default();
    leaf_params.is_ca = IsCa::NoCa;
    leaf_params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into().unwrap()),
        SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
    ];
    let leaf_key = KeyPair::generate().expect("leaf key");
    let leaf_cert = leaf_params
        .signed_by(&leaf_key, &ca_cert, &ca_key)
        .expect("leaf cert");

    let cert_der = leaf_cert.der().clone();
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der()));
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("server config");
    // Offer h2 first so an Auto client negotiates HTTP/2; an http1_only client
    // still falls back to HTTP/1.1. This lets one server serve both protocols.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    (TlsAcceptor::from(Arc::new(config)), ca_cert.pem())
}

fn full_body(data: Bytes) -> BoxBody<Bytes, io::Error> {
    Full::new(data)
        .map_err(|e: Infallible| -> io::Error { match e {} })
        .boxed()
}

fn streaming_body(
    data: Bytes,
    chunk_size: usize,
    delay: Duration,
    drop_after: Option<u64>,
    bucket: Option<&BandwidthBucket>,
) -> BoxBody<Bytes, io::Error> {
    let total = data.len() as u64;
    let chunk_size = chunk_size.max(1) as u64;
    let bucket = bucket.cloned();
    let s = stream::unfold(0u64, move |pos| {
        let data = data.clone();
        let bucket = bucket.clone();
        async move {
            if pos >= total {
                return None;
            }
            if let Some(limit) = drop_after {
                if pos >= limit {
                    // Let hyper flush the partial bytes already produced before
                    // we end the body early; otherwise the buffered chunks are
                    // discarded when the connection closes and the client sees
                    // zero bytes. Content-Length still promises the full range,
                    // so the client detects an interrupted transfer.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    return None;
                }
            }
            let end = (pos + chunk_size).min(total);
            if let Some(bucket) = &bucket {
                bucket.take(end - pos).await;
            }
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let chunk = data.slice(pos as usize..end as usize);
            Some((Ok(Frame::data(chunk)), end))
        }
    });
    StreamBody::new(s).boxed()
}

fn make_body(
    b: &Behavior,
    data: Bytes,
    drop_after: Option<u64>,
    delay: Duration,
    bucket: Option<&BandwidthBucket>,
) -> BoxBody<Bytes, io::Error> {
    if delay.is_zero() && drop_after.is_none() && !b.no_content_length && bucket.is_none() {
        full_body(data)
    } else {
        streaming_body(data, b.chunk_size, delay, drop_after, bucket)
    }
}

/// Body wrapper that decrements the server's in-flight counter when the
/// response body is dropped (completed or aborted), so the concurrent
/// connection limit reflects reality.
struct TrackBody {
    inner: BoxBody<Bytes, io::Error>,
    inflight: Arc<AtomicUsize>,
}

impl HttpBody for TrackBody {
    type Data = Bytes;
    type Error = io::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        // Safe projection: TrackBody is structurally pinned via Pin<&mut Self>.
        let this = unsafe { self.get_unchecked_mut() };
        Pin::new(&mut this.inner).poll_frame(cx)
    }
}

impl Drop for TrackBody {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

fn tracked(
    body: BoxBody<Bytes, io::Error>,
    inflight: Arc<AtomicUsize>,
) -> BoxBody<Bytes, io::Error> {
    inflight.fetch_add(1, Ordering::SeqCst);
    TrackBody {
        inner: body,
        inflight,
    }
    .boxed()
}

fn parse_range(value: &str, total: u64) -> Option<(u64, u64)> {
    let rest = value.strip_prefix("bytes=")?;
    let (start, end) = rest.split_once('-')?;
    let start: u64 = start.trim().parse().ok()?;
    let end: u64 = if end.trim().is_empty() {
        total.saturating_sub(1)
    } else {
        end.trim().parse().ok()?
    };
    if total == 0 {
        return None;
    }
    Some((start, end.min(total - 1)))
}

async fn handle(
    req: Request<Incoming>,
    state: Arc<State>,
) -> Result<Response<BoxBody<Bytes, io::Error>>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let range = req
        .headers()
        .get("range")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let if_range = req
        .headers()
        .get("if-range")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let accept_encoding = req
        .headers()
        .get("accept-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    state.requests.lock().unwrap().push(RecordedRequest {
        method: method.as_str().to_string(),
        path,
        range: range.clone(),
        if_range: if_range.clone(),
        accept_encoding,
    });

    let b = state.behavior.lock().unwrap().clone();

    // A blanket status (404 / 500 / ...) short-circuits everything.
    if let Some(code) = b.always_status {
        let mut builder = Response::builder().status(code);
        if let Some(ra) = b.retry_after {
            builder = builder.header("retry-after", ra.to_string());
        }
        return Ok(builder
            .body(full_body(Bytes::from_static(b"error")))
            .unwrap());
    }

    let is_probe_get = method == Method::GET && range.as_deref() == Some("bytes=0-0");
    let is_data_get = method == Method::GET && !is_probe_get;
    let data_idx = if is_data_get {
        state.data_get_count.fetch_add(1, Ordering::SeqCst)
    } else {
        state.data_get_count.load(Ordering::SeqCst)
    };
    let switched = b.switch_after.map(|n| data_idx >= n).unwrap_or(false);
    let data = if switched {
        b.switch_data.clone().unwrap_or_else(|| b.data.clone())
    } else {
        b.data.clone()
    };
    let etag = if switched {
        b.switch_etag.clone().or_else(|| b.etag.clone())
    } else {
        b.etag.clone()
    };
    let total = data.len() as u64;

    // Transient failures on the first N data GETs.
    if is_data_get && data_idx < b.fail_first {
        let mut builder = Response::builder().status(b.fail_status);
        if let Some(ra) = b.retry_after {
            builder = builder.header("retry-after", ra.to_string());
        }
        return Ok(builder
            .body(full_body(Bytes::from_static(b"busy")))
            .unwrap());
    }

    let drop_after = if is_data_get && data_idx < b.drop_first {
        Some(b.drop_after_bytes)
    } else {
        None
    };

    let mut base = Response::builder();
    if let Some(e) = &etag {
        base = base.header("etag", e);
    }
    if let Some(lm) = &b.last_modified {
        base = base.header("last-modified", lm);
    }
    if let Some(cd) = &b.content_disposition {
        base = base.header("content-disposition", cd);
    }

    // Concurrent data-GET limit: reject the excess with 429 and an immediate
    // retry-after, mimicking per-IP throttling.
    if is_data_get {
        if let Some(max) = b.max_concurrent_data {
            if state.inflight_data.load(Ordering::SeqCst) >= max {
                return Ok(Response::builder()
                    .status(429)
                    .header("retry-after", "0")
                    .body(full_body(Bytes::from_static(b"busy")))
                    .unwrap());
            }
        }
    }

    if method == Method::HEAD {
        match b.head_mode {
            HeadMode::Reject => {
                return Ok(Response::builder()
                    .status(405)
                    .body(full_body(Bytes::new()))
                    .unwrap());
            }
            HeadMode::NoLength => {
                if b.advertise_ranges {
                    base = base.header("accept-ranges", "bytes");
                }
                return Ok(base.status(200).body(full_body(Bytes::new())).unwrap());
            }
            HeadMode::Normal => {
                if b.advertise_ranges {
                    base = base.header("accept-ranges", "bytes");
                }
                if !b.no_content_length {
                    base = base.header("content-length", total.to_string());
                }
                return Ok(base.status(200).body(full_body(Bytes::new())).unwrap());
            }
        }
    }

    // GET (and any other method) falls through to a body response.
    let if_range_ok = match &if_range {
        Some(v) => {
            etag.as_deref() == Some(v.as_str()) || b.last_modified.as_deref() == Some(v.as_str())
        }
        None => true,
    };

    if b.support_ranges && if_range_ok {
        if let Some(r) = &range {
            if let Some((start, end)) = parse_range(r, total) {
                if start >= total {
                    return Ok(Response::builder()
                        .status(416)
                        .header("content-range", format!("bytes */{total}"))
                        .body(full_body(Bytes::new()))
                        .unwrap());
                }
                let slice = data.slice(start as usize..=end as usize);
                let len = slice.len();
                let eff_delay = if is_data_get {
                    match b.slow_from_start {
                        Some(t) if start >= t => b.chunk_delay,
                        Some(_) => Duration::ZERO,
                        None => b.chunk_delay,
                    }
                } else {
                    b.chunk_delay
                };
                let builder = base
                    .status(206)
                    .header("content-range", format!("bytes {start}-{end}/{total}"))
                    .header("content-length", len.to_string())
                    .header("accept-ranges", "bytes");
                let body = make_body(&b, slice, drop_after, eff_delay, state.bucket.as_ref());
                let body = if is_data_get {
                    tracked(body, state.inflight_data.clone())
                } else {
                    body
                };
                return Ok(builder.body(body).unwrap());
            }
        }
    }

    let mut builder = base.status(200);
    if b.advertise_ranges && b.support_ranges {
        builder = builder.header("accept-ranges", "bytes");
    }
    if !b.no_content_length {
        builder = builder.header("content-length", total.to_string());
    }
    let body = make_body(&b, data, drop_after, b.chunk_delay, state.bucket.as_ref());
    let body = if is_data_get {
        tracked(body, state.inflight_data.clone())
    } else {
        body
    };
    Ok(builder.body(body).unwrap())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn test_config(dir: &std::path::Path) -> EngineConfig {
    let mut cfg = EngineConfig {
        download_dir: dir.join("downloads"),
        db_path: dir.join("test.db"),
        min_segment_size: 4096,
        ..Default::default()
    };
    cfg.retry.base_delay_ms = 50;
    cfg.retry.max_delay_ms = 200;
    cfg.retry.max_attempts = 6;
    cfg.checkpoint.interval_ms = 100;
    cfg.checkpoint.bytes = 64 * 1024;
    // Short adaptive windows keep auto-mode tests fast and deterministic.
    cfg.adaptive.window_ms = 150;
    // Tests talk to a loopback server; never route them through the OS proxy.
    cfg.network.use_system_proxy = false;
    cfg
}

pub fn pattern_data(len: usize) -> Bytes {
    Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(data))
}

pub async fn open_engine(cfg: EngineConfig) -> Engine {
    Engine::open(cfg).await.expect("open engine")
}

/// Poll `get_task` until `pred` holds, or panic after `timeout`.
pub async fn wait_for(
    engine: &Engine,
    id: TaskId,
    timeout: Duration,
    pred: impl Fn(&TaskInfo) -> bool,
) -> TaskInfo {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let info = engine.get_task(id).await.expect("get_task");
        if pred(&info) {
            return info;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for task {id}; last state: {info:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub async fn wait_completed(engine: &Engine, id: TaskId) -> TaskInfo {
    wait_for(engine, id, Duration::from_secs(30), |i| {
        i.state == TaskState::Completed
    })
    .await
}

pub async fn wait_failed(engine: &Engine, id: TaskId) -> TaskInfo {
    wait_for(engine, id, Duration::from_secs(30), |i| {
        i.state == TaskState::Failed
    })
    .await
}

pub async fn wait_downloaded(engine: &Engine, id: TaskId, min: u64) -> TaskInfo {
    wait_for(engine, id, Duration::from_secs(30), |i| {
        i.downloaded >= min || i.state == TaskState::Completed
    })
    .await
}

pub fn read_file(path: &std::path::Path) -> Vec<u8> {
    std::fs::read(path).expect("read final file")
}
