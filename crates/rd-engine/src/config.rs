use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EngineConfig {
    pub download_dir: PathBuf,
    pub db_path: PathBuf,
    pub max_concurrent_tasks: usize,
    pub default_connections: u32,
    pub max_connections_per_task: u32,
    pub min_segment_size: u64,
    /// Opt-in NTFS sparse allocation. Benchmark on the destination volume;
    /// physical extent allocation can trade startup latency for write overhead.
    pub sparse_files: bool,
    /// Automatically resume interrupted tasks when the engine opens.
    pub resume_on_startup: bool,
    pub retry: RetryConfig,
    pub network: NetworkConfig,
    pub rate_limit: RateLimitConfig,
    pub checkpoint: CheckpointConfig,
    pub adaptive: AdaptiveConfig,
    pub tls: TlsConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            download_dir: PathBuf::from("downloads"),
            db_path: PathBuf::from("rd-downloader.db"),
            max_concurrent_tasks: 3,
            default_connections: 16,
            // Hard cap on concurrent HTTP connections per task. 64 matches
            // what classic multi-connection downloaders use; the effective
            // count is still limited by file size / min_segment_size and by
            // the adaptive controller when auto mode is enabled.
            max_connections_per_task: 64,
            min_segment_size: 1024 * 1024,
            sparse_files: false,
            resume_on_startup: false,
            retry: RetryConfig::default(),
            network: NetworkConfig::default(),
            rate_limit: RateLimitConfig::default(),
            checkpoint: CheckpointConfig::default(),
            adaptive: AdaptiveConfig::default(),
            tls: TlsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    /// Max times a task restarts from scratch when the remote resource changes.
    pub max_restarts: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            base_delay_ms: 500,
            max_delay_ms: 30_000,
            max_restarts: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetworkConfig {
    pub connect_timeout_secs: u64,
    /// Idle timeout between received data chunks.
    pub read_timeout_secs: u64,
    pub user_agent: String,
    pub max_redirects: usize,
    /// HTTP version for data connections. `auto` lets TLS ALPN negotiate
    /// (HTTP/2 with large flow-control windows); `http1` forces one TCP
    /// connection per segment, which is faster on some networks that
    /// throttle multiplexed HTTP/2 traffic.
    pub http_version: HttpVersion,
    /// Prefer IPv4 addresses when resolving hosts. Avoids slow IPv6 routes on
    /// dual-stack machines; set to `false` if a host is IPv6-only reachable.
    pub prefer_ipv4: bool,
    /// Honor the OS-level proxy configuration (Windows registry / macOS
    /// system settings / env vars). Downloading through the same proxy the
    /// user's browser uses is typically the difference between KiB/s and
    /// MiB/s on international links.
    pub use_system_proxy: bool,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            connect_timeout_secs: 15,
            read_timeout_secs: 30,
            user_agent: format!("rd-engine/{}", env!("CARGO_PKG_VERSION")),
            max_redirects: 10,
            http_version: HttpVersion::default(),
            prefer_ipv4: true,
            use_system_proxy: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HttpVersion {
    /// Negotiate via ALPN; HTTP/2 is tuned with large flow-control windows.
    Auto,
    /// One TCP connection per segment (classic multi-connection downloaders).
    #[default]
    Http1,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RateLimitConfig {
    /// Global download speed cap in bytes/second; 0 means unlimited.
    pub global_bps: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CheckpointConfig {
    pub interval_ms: u64,
    pub bytes: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval_ms: 1000,
            bytes: 1024 * 1024,
        }
    }
}

/// Adaptive concurrency control (IDM-style): the engine watches aggregate
/// throughput and worker failures over an observation window and grows or
/// shrinks the number of active connections for tasks that were *not* given
/// an explicit connection count.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdaptiveConfig {
    /// Enable auto mode for tasks added without an explicit connection count.
    /// Tasks created with `connections: Some(n)` are always pinned to `n`.
    pub enabled: bool,
    /// Never drop below this many connections in auto mode.
    pub min_connections: u32,
    /// Throughput observation window in milliseconds.
    pub window_ms: u64,
    /// Minimum aggregate-throughput gain (in percent) that justifies adding
    /// one more connection. Below `2 * grow_gain_pct / 3` percent gain the
    /// engine marks the connection count saturated and stops growing.
    pub grow_gain_pct: u32,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_connections: 2,
            window_ms: 1000,
            grow_gain_pct: 5,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsConfig {
    /// Disable certificate verification. Insecure; intended for testing only.
    pub accept_invalid_certs: bool,
    /// Additional trusted CA certificate (PEM or DER), e.g. for private CAs.
    pub ca_cert_path: Option<PathBuf>,
}

impl EngineConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let text = std::fs::read_to_string(path.as_ref())?;
        let config: Self =
            toml::from_str(&text).map_err(|e| Error::Config(format!("invalid TOML: {e}")))?;
        config.validate()?;
        Ok(config)
    }

    /// Load from `path` if it exists, otherwise return defaults.
    pub fn load_or_default(path: impl AsRef<Path>) -> Result<Self> {
        if path.as_ref().exists() {
            Self::from_file(path)
        } else {
            Ok(Self::default())
        }
    }

    pub fn to_toml(&self) -> Result<String> {
        toml::to_string_pretty(self).map_err(|e| Error::Config(format!("serialize config: {e}")))
    }

    pub fn validate(&self) -> Result<()> {
        if self.max_concurrent_tasks == 0 {
            return Err(Error::Config("max_concurrent_tasks must be >= 1".into()));
        }
        if self.default_connections == 0 || self.max_connections_per_task == 0 {
            return Err(Error::Config("connection counts must be >= 1".into()));
        }
        if self.min_segment_size == 0 {
            return Err(Error::Config("min_segment_size must be >= 1".into()));
        }
        if self.retry.max_attempts == 0 {
            return Err(Error::Config("retry.max_attempts must be >= 1".into()));
        }
        if self.retry.base_delay_ms > self.retry.max_delay_ms {
            return Err(Error::Config(
                "retry.base_delay_ms must be <= retry.max_delay_ms".into(),
            ));
        }
        if self.adaptive.min_connections == 0 {
            return Err(Error::Config(
                "adaptive.min_connections must be >= 1".into(),
            ));
        }
        if self.adaptive.window_ms < 50 {
            return Err(Error::Config("adaptive.window_ms must be >= 50".into()));
        }
        if self.adaptive.grow_gain_pct == 0 {
            return Err(Error::Config("adaptive.grow_gain_pct must be >= 1".into()));
        }
        if self.download_dir.as_os_str().is_empty() {
            return Err(Error::Config("download_dir must not be empty".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        EngineConfig::default().validate().unwrap();
    }

    #[test]
    fn partial_toml_uses_defaults() {
        let cfg: EngineConfig = toml::from_str("max_concurrent_tasks = 5").unwrap();
        assert_eq!(cfg.max_concurrent_tasks, 5);
        assert_eq!(cfg.default_connections, 16);
        cfg.validate().unwrap();
    }

    #[test]
    fn full_roundtrip() {
        let cfg = EngineConfig::default();
        let text = cfg.to_toml().unwrap();
        let back: EngineConfig = toml::from_str(&text).unwrap();
        assert_eq!(back.max_concurrent_tasks, cfg.max_concurrent_tasks);
        assert_eq!(back.retry.max_attempts, cfg.retry.max_attempts);
    }

    #[test]
    fn rejects_unknown_fields() {
        let r: std::result::Result<EngineConfig, _> = toml::from_str("bogus_key = 1");
        assert!(r.is_err());
    }

    #[test]
    fn rejects_invalid_values() {
        let cfg = EngineConfig {
            max_concurrent_tasks: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn http_version_and_ipv4_config() {
        let cfg: EngineConfig =
            toml::from_str("[network]\nhttp_version = \"http1\"\nprefer_ipv4 = false").unwrap();
        assert_eq!(cfg.network.http_version, HttpVersion::Http1);
        assert!(!cfg.network.prefer_ipv4);
        let cfg: EngineConfig = toml::from_str("[network]\nhttp_version = \"auto\"").unwrap();
        assert_eq!(cfg.network.http_version, HttpVersion::Auto);
        assert!(cfg.network.prefer_ipv4);
        let bad: std::result::Result<EngineConfig, _> =
            toml::from_str("[network]\nhttp_version = \"http3\"");
        assert!(bad.is_err());
    }
}
