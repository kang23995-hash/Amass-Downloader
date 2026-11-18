use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub type TaskId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskState {
    Pending,
    Probing,
    Downloading,
    Paused,
    Retrying,
    Completed,
    Failed,
    Cancelled,
}

impl TaskState {
    /// States in which a runner is actively working on the task.
    pub fn is_active(self) -> bool {
        matches!(self, Self::Probing | Self::Downloading | Self::Retrying)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Probing => "probing",
            Self::Downloading => "downloading",
            Self::Paused => "paused",
            Self::Retrying => "retrying",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => Self::Pending,
            "probing" => Self::Probing,
            "downloading" => Self::Downloading,
            "paused" => Self::Paused,
            "retrying" => Self::Retrying,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => return None,
        })
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A byte range assigned to one worker. `end` is inclusive; `None` means the
/// total size is unknown and the segment streams until the server closes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    pub index: u32,
    pub start: u64,
    pub end: Option<u64>,
    pub downloaded: u64,
}

impl Segment {
    pub fn len(&self) -> Option<u64> {
        self.end.map(|end| end - self.start + 1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == Some(0)
    }

    pub fn next_offset(&self) -> u64 {
        self.start + self.downloaded
    }

    pub fn is_complete(&self) -> bool {
        match self.end {
            Some(end) => self.downloaded > end - self.start,
            None => false,
        }
    }
}

/// Split `total_size` into at most `connections` contiguous, non-overlapping
/// segments, each at least `min_segment_size` bytes (except a short tail).
pub fn plan_segments(total_size: u64, connections: u32, min_segment_size: u64) -> Vec<Segment> {
    let connections = connections.max(1) as u64;
    let min_segment_size = min_segment_size.max(1);
    if total_size == 0 {
        // An empty file has no byte range; stream it as one open segment.
        return vec![Segment {
            index: 0,
            start: 0,
            end: None,
            downloaded: 0,
        }];
    }
    let count = connections
        .min(total_size.div_ceil(min_segment_size))
        .max(1);
    let base = total_size / count;
    let rem = total_size % count;
    let mut segments = Vec::with_capacity(count as usize);
    let mut start = 0u64;
    for i in 0..count {
        let len = base + if i < rem { 1 } else { 0 };
        let end = start + len - 1;
        segments.push(Segment {
            index: i as u32,
            start,
            end: Some(end),
            downloaded: 0,
        });
        start += len;
    }
    segments
}

#[derive(Debug, Clone)]
pub struct AddTaskRequest {
    pub url: String,
    pub output_dir: Option<PathBuf>,
    pub file_name: Option<String>,
    pub connections: Option<u32>,
    pub expected_sha256: Option<String>,
    pub expected_blake3: Option<String>,
    pub rate_limit_bps: Option<u64>,
    pub auto_start: bool,
}

impl AddTaskRequest {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            output_dir: None,
            file_name: None,
            connections: None,
            expected_sha256: None,
            expected_blake3: None,
            rate_limit_bps: None,
            auto_start: true,
        }
    }
}

/// Snapshot of a task as exposed through the public API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: TaskId,
    pub url: String,
    pub final_url: Option<String>,
    pub file_name: String,
    pub file_path: PathBuf,
    pub part_path: PathBuf,
    pub state: TaskState,
    pub total_size: Option<u64>,
    pub downloaded: u64,
    pub connections: u32,
    pub range_supported: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub expected_sha256: Option<String>,
    pub actual_sha256: Option<String>,
    pub expected_blake3: Option<String>,
    pub actual_blake3: Option<String>,
    pub rate_limit_bps: Option<u64>,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    /// True when the engine may adapt this task's connection count at
    /// runtime; false when the user pinned it explicitly.
    pub auto_concurrency: bool,
    /// Live speed, only meaningful while the engine is running.
    #[serde(skip)]
    pub speed_bps: u64,
}

/// Events published to UI subscribers.
#[derive(Debug, Clone)]
pub enum Event {
    TaskCreated {
        id: TaskId,
        url: String,
        file_name: String,
    },
    TaskStarted {
        id: TaskId,
    },
    TaskStateChanged {
        id: TaskId,
        state: TaskState,
    },
    ProgressUpdated {
        id: TaskId,
        downloaded: u64,
        total_size: Option<u64>,
    },
    SpeedUpdated {
        id: TaskId,
        bytes_per_sec: u64,
    },
    TaskPaused {
        id: TaskId,
    },
    TaskResumed {
        id: TaskId,
    },
    TaskRetrying {
        id: TaskId,
        attempt: u32,
        delay_ms: u64,
        error: String,
    },
    ResourceChanged {
        id: TaskId,
    },
    TaskCompleted {
        id: TaskId,
        file_path: PathBuf,
        sha256: Option<String>,
        blake3: Option<String>,
    },
    TaskFailed {
        id: TaskId,
        error: String,
    },
    TaskCancelled {
        id: TaskId,
    },
    TaskRemoved {
        id: TaskId,
    },
}

impl Event {
    pub fn task_id(&self) -> TaskId {
        match self {
            Self::TaskCreated { id, .. }
            | Self::TaskStarted { id }
            | Self::TaskStateChanged { id, .. }
            | Self::ProgressUpdated { id, .. }
            | Self::SpeedUpdated { id, .. }
            | Self::TaskPaused { id }
            | Self::TaskResumed { id }
            | Self::TaskRetrying { id, .. }
            | Self::ResourceChanged { id }
            | Self::TaskCompleted { id, .. }
            | Self::TaskFailed { id, .. }
            | Self::TaskCancelled { id }
            | Self::TaskRemoved { id } => *id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segments_cover_range_exactly() {
        for size in [0u64, 1, 7, 1024, 1_048_577, 10_000_000] {
            for conns in [1u32, 2, 4, 8, 16] {
                let segs = plan_segments(size, conns, 1024);
                assert!(!segs.is_empty());
                if size == 0 {
                    assert_eq!(segs.len(), 1);
                    assert_eq!(segs[0].end, None);
                    continue;
                }
                assert_eq!(segs[0].start, 0);
                let mut expect = 0u64;
                for s in &segs {
                    assert_eq!(s.start, expect, "gap or overlap at size={size}");
                    expect = s.end.unwrap() + 1;
                    assert!(s.len().unwrap() > 0 || size == 0);
                }
                assert_eq!(expect, size);
                assert!(segs.len() <= conns as usize);
            }
        }
    }

    #[test]
    fn min_segment_size_limits_count() {
        let segs = plan_segments(3 * 1024 * 1024, 8, 1024 * 1024);
        assert_eq!(segs.len(), 3);
        let segs = plan_segments(512 * 1024, 8, 1024 * 1024);
        assert_eq!(segs.len(), 1);
    }

    #[test]
    fn segment_completion() {
        let mut s = Segment {
            index: 0,
            start: 10,
            end: Some(19),
            downloaded: 0,
        };
        assert!(!s.is_complete());
        s.downloaded = 10;
        assert!(s.is_complete());
        assert_eq!(s.next_offset(), 20);
    }

    #[test]
    fn state_roundtrip() {
        for st in [
            TaskState::Pending,
            TaskState::Probing,
            TaskState::Downloading,
            TaskState::Paused,
            TaskState::Retrying,
            TaskState::Completed,
            TaskState::Failed,
            TaskState::Cancelled,
        ] {
            assert_eq!(TaskState::parse(st.as_str()), Some(st));
        }
        assert_eq!(TaskState::parse("bogus"), None);
    }
}
