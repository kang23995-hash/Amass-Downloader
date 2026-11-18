use crate::model::{TaskId, TaskState};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("HTTP status {code} for {url}")]
    HttpStatus {
        code: u16,
        url: String,
        retry_after_secs: Option<u64>,
    },

    #[error("protocol violation: {0}")]
    Protocol(String),

    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),

    #[error("invalid URL: {0}")]
    Url(String),

    #[error("task {0} not found")]
    TaskNotFound(TaskId),

    #[error("cannot {action} task {id} in state {state}")]
    InvalidState {
        id: TaskId,
        state: TaskState,
        action: &'static str,
    },

    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("remote resource changed during download")]
    ResourceChanged,

    #[error("engine has been shut down")]
    Shutdown,

    #[error("task cancelled")]
    Cancelled,

    /// Internal signal: this worker's release token fired. Converted to
    /// [`Error::WorkerReleased`] (which carries the segment index) at the
    /// `run_segment` boundary.
    #[error("worker released by the scheduler")]
    Released,

    /// A worker was released by the scheduler (concurrency shrink). This is
    /// not a failure: the segment stays partially downloaded and is either
    /// reclaimed later or covered by other workers.
    #[error("worker for segment {index} released by the scheduler")]
    WorkerReleased { index: u32 },

    #[error("internal error: {0}")]
    Internal(String),
}

/// Coarse failure classification used by the adaptive concurrency controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// Server-side rate limiting (429/503): the strongest signal to reduce
    /// concurrency.
    Throttle,
    /// Request or read timeouts.
    Timeout,
    /// Other transport-level failures (connect errors, 5xx, resets).
    Network,
    /// Protocol violations and other non-transient errors.
    Fatal,
}

impl Error {
    /// Whether retrying the same request may succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Http(e) => !e.is_redirect(),
            Error::HttpStatus { code, .. } => {
                matches!(code, 408 | 425 | 429 | 500 | 502 | 503 | 504)
            }
            _ => false,
        }
    }

    pub fn retry_after_secs(&self) -> Option<u64> {
        match self {
            Error::HttpStatus {
                retry_after_secs, ..
            } => *retry_after_secs,
            _ => None,
        }
    }

    pub fn kind(&self) -> FailureKind {
        match self {
            Error::HttpStatus {
                code: 429 | 503, ..
            } => FailureKind::Throttle,
            Error::Http(e) if e.is_timeout() => FailureKind::Timeout,
            Error::HttpStatus { .. } | Error::Http(_) => FailureKind::Network,
            _ => FailureKind::Fatal,
        }
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(e: tokio::task::JoinError) -> Self {
        Error::Internal(format!("background task failed: {e}"))
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        Error::Internal(format!("lock poisoned: {e}"))
    }
}
