use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{
    HeaderMap, ACCEPT_ENCODING, ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, ETAG,
    LAST_MODIFIED,
};
use reqwest::{Client, StatusCode};
use url::Url;

use crate::config::{EngineConfig, HttpVersion};
use crate::error::{Error, Result};
use crate::util::{parse_retry_after, sanitize_file_name};

/// DNS resolver that puts IPv4 addresses before IPv6 ones. On dual-stack
/// machines with a poor IPv6 route this alone can change throughput by orders
/// of magnitude; getaddrinfo otherwise prefers IPv6 per RFC 6724.
struct Ipv4PreferredResolver;

impl reqwest::dns::Resolve for Ipv4PreferredResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
                .collect();
            let (v4, rest): (Vec<_>, Vec<_>) = addrs.into_iter().partition(|a| a.is_ipv4());
            let ordered = v4.into_iter().chain(rest);
            Ok(Box::new(ordered) as Box<dyn Iterator<Item = SocketAddr> + Send>)
        })
    }
}

#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub final_url: String,
    pub size: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub range_supported: bool,
    pub file_name: Option<String>,
}

pub fn build_client(config: &EngineConfig) -> Result<Client> {
    let mut builder = Client::builder()
        .connect_timeout(Duration::from_secs(config.network.connect_timeout_secs))
        .read_timeout(Duration::from_secs(config.network.read_timeout_secs))
        .user_agent(config.network.user_agent.clone())
        .redirect(reqwest::redirect::Policy::limited(
            config.network.max_redirects,
        ))
        // Sub-second latency matters when 64 workers run small retry requests;
        // Nagle adds up to 40ms of artificial delay per request otherwise.
        .tcp_nodelay(true)
        // Keep every worker's HTTP/1.1 connection warm so retries and
        // replacement requests reuse the TCP+TLS session instead of paying a
        // fresh handshake. The HTTP/2 case multiplexes over one connection
        // and simply ignores the per-host idle cap.
        .pool_max_idle_per_host(config.max_connections_per_task.clamp(1, 64) as usize);
    // Decompression features are deliberately not enabled: byte offsets must
    // match the on-disk file for Range requests to be valid.
    match config.network.http_version {
        HttpVersion::Http1 => {
            // One TCP connection per in-flight request: each segment worker
            // gets its own connection, like classic multi-connection
            // downloaders.
            builder = builder.http1_only();
        }
        HttpVersion::Auto => {
            // HTTP/2 multiplexes all segment workers over one connection, so
            // the flow-control windows must be large or they cap aggregate
            // throughput to window/RTT (the h2 defaults of 64 KiB throttle
            // high-latency links to a few hundred KiB/s).
            builder = builder
                .http2_initial_connection_window_size(Some(16 * 1024 * 1024))
                .http2_initial_stream_window_size(Some(2 * 1024 * 1024));
        }
    }
    if !config.network.use_system_proxy {
        // Default behavior (with the `system-proxy` feature) honors OS proxy
        // settings and standard env vars; this switch opts out entirely.
        builder = builder.no_proxy();
    }
    if config.network.prefer_ipv4 {
        builder = builder.dns_resolver(Arc::new(Ipv4PreferredResolver));
    }
    if config.tls.accept_invalid_certs {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(ca_path) = &config.tls.ca_cert_path {
        let pem = std::fs::read(ca_path)?;
        let cert = reqwest::Certificate::from_pem(&pem)
            .or_else(|_| reqwest::Certificate::from_der(&pem))
            .map_err(|e| {
                Error::Config(format!("invalid CA certificate {}: {e}", ca_path.display()))
            })?;
        builder = builder.add_root_certificate(cert);
    }
    builder.build().map_err(Error::Http)
}

/// Discover size, validators, Range support and file name.
///
/// Starts with HEAD; falls back to a `Range: bytes=0-0` GET when HEAD is
/// rejected or lacks information. When `confirm_range` is set, the Range
/// probe is performed even if HEAD advertised support, because some servers
/// advertise `Accept-Ranges` but ignore `Range`.
pub async fn probe(client: &Client, url: &str, confirm_range: bool) -> Result<ProbeResult> {
    let mut result = ProbeResult {
        final_url: url.to_string(),
        size: None,
        etag: None,
        last_modified: None,
        range_supported: false,
        file_name: None,
    };

    let head = client.head(url).send().await;
    let need_get;
    match head {
        Ok(resp) if resp.status().is_success() => {
            absorb_headers(&mut result, resp.url().as_str(), resp.headers());
            let advertised = resp
                .headers()
                .get(ACCEPT_RANGES)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.eq_ignore_ascii_case("bytes"))
                .unwrap_or(false);
            result.range_supported = advertised && result.size.is_some();
            need_get = result.size.is_none() || (confirm_range && result.range_supported);
            if !need_get {
                return Ok(result);
            }
        }
        Ok(resp) => {
            let status = resp.status();
            if status != StatusCode::METHOD_NOT_ALLOWED && status != StatusCode::NOT_IMPLEMENTED {
                return Err(status_error(url, &resp));
            }
        }
        Err(e) => {
            if e.is_redirect() {
                return Err(Error::Http(e));
            }
            // HEAD may be blocked by middleboxes; try the GET probe.
        }
    }

    let resp = client
        .get(url)
        .header(reqwest::header::RANGE, "bytes=0-0")
        .header(ACCEPT_ENCODING, "identity")
        .send()
        .await?;
    let status = resp.status();
    if status == StatusCode::PARTIAL_CONTENT {
        let cr = resp
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_content_range)
            .ok_or_else(|| Error::Protocol("206 response without valid Content-Range".into()))?;
        absorb_headers(&mut result, resp.url().as_str(), resp.headers());
        result.size = cr.2;
        result.range_supported = true;
    } else if status.is_success() {
        absorb_headers(&mut result, resp.url().as_str(), resp.headers());
        result.range_supported = false;
    } else {
        return Err(status_error(url, &resp));
    }
    // Drop the body without consuming it; the connection is closed/reused.
    drop(resp);
    Ok(result)
}

fn absorb_headers(result: &mut ProbeResult, final_url: &str, headers: &HeaderMap) {
    result.final_url = final_url.to_string();
    if let Some(len) = headers.get(CONTENT_LENGTH).and_then(|v| v.to_str().ok()) {
        if let Ok(n) = len.parse::<u64>() {
            result.size = Some(n);
        }
    }
    if let Some(etag) = headers.get(ETAG).and_then(|v| v.to_str().ok()) {
        result.etag = Some(etag.to_string());
    }
    if let Some(lm) = headers.get(LAST_MODIFIED).and_then(|v| v.to_str().ok()) {
        result.last_modified = Some(lm.to_string());
    }
    if let Some(cd) = headers
        .get(CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(name) = content_disposition_filename(cd) {
            result.file_name = Some(name);
        }
    }
}

pub fn status_error(url: &str, resp: &reqwest::Response) -> Error {
    let retry_after_secs = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_retry_after);
    Error::HttpStatus {
        code: resp.status().as_u16(),
        url: url.to_string(),
        retry_after_secs,
    }
}

/// Parse `bytes start-end/total` (total may be `*`).
pub fn parse_content_range(value: &str) -> Option<(u64, u64, Option<u64>)> {
    let value = value.trim();
    let rest = value.strip_prefix("bytes ")?.trim();
    let (range, total) = rest.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let start = start.trim().parse::<u64>().ok()?;
    let end = end.trim().parse::<u64>().ok()?;
    let total = match total.trim() {
        "*" => None,
        t => Some(t.parse::<u64>().ok()?),
    };
    if end < start {
        return None;
    }
    Some((start, end, total))
}

/// Extract a file name from a Content-Disposition header, preferring the
/// RFC 5987 `filename*` form over plain `filename`.
pub fn content_disposition_filename(value: &str) -> Option<String> {
    let mut plain: Option<String> = None;
    for part in value.split(';') {
        let part = part.trim();
        if let Some(rest) = part.strip_prefix("filename*=") {
            // charset'lang'percent-encoded
            let mut pieces = rest.splitn(3, '\'');
            let _charset = pieces.next()?;
            let _lang = pieces.next()?;
            let encoded = pieces.next()?;
            let decoded = percent_encoding::percent_decode_str(encoded)
                .decode_utf8()
                .ok()?;
            let name = sanitize_file_name(decoded.trim_matches('"'));
            if !name.is_empty() {
                return Some(name);
            }
        } else if let Some(rest) = part.strip_prefix("filename=") {
            plain = Some(sanitize_file_name(rest.trim().trim_matches('"')));
        }
    }
    plain.filter(|s| !s.is_empty())
}

pub fn filename_from_url(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let segment = parsed.path_segments()?.next_back()?;
    if segment.is_empty() {
        return None;
    }
    let decoded = percent_encoding::percent_decode_str(segment)
        .decode_utf8()
        .ok()?;
    let name = sanitize_file_name(&decoded);
    if name == "download" && decoded.is_empty() {
        None
    } else {
        Some(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_range_parsing() {
        assert_eq!(
            parse_content_range("bytes 0-499/1000"),
            Some((0, 499, Some(1000)))
        );
        assert_eq!(
            parse_content_range("bytes 500-999/*"),
            Some((500, 999, None))
        );
        assert_eq!(parse_content_range("bytes 0-0/1"), Some((0, 0, Some(1))));
        assert_eq!(parse_content_range("items 0-10/20"), None);
        assert_eq!(parse_content_range("bytes 9-1/20"), None);
        assert_eq!(parse_content_range("garbage"), None);
    }

    #[test]
    fn disposition_parsing() {
        assert_eq!(
            content_disposition_filename("attachment; filename=\"report.pdf\""),
            Some("report.pdf".into())
        );
        assert_eq!(
            content_disposition_filename("attachment; filename=plain.bin"),
            Some("plain.bin".into())
        );
        assert_eq!(
            content_disposition_filename(
                "attachment; filename=\"fallback.bin\"; filename*=UTF-8''%E6%96%87%E4%BB%B6.bin"
            ),
            Some("文件.bin".into())
        );
        assert_eq!(
            content_disposition_filename("attachment; filename=\"../../evil.exe\""),
            Some("evil.exe".into())
        );
        assert_eq!(content_disposition_filename("inline"), None);
    }

    #[test]
    fn url_filename() {
        assert_eq!(
            filename_from_url("https://example.com/files/setup%20v2.exe?x=1"),
            Some("setup v2.exe".into())
        );
        assert_eq!(filename_from_url("https://example.com/"), None);
        assert_eq!(
            filename_from_url("https://example.com/a/b/data.zip"),
            Some("data.zip".into())
        );
    }
}
