use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::EngineConfig;
use crate::db::{self, Db, MetaUpdate};
use crate::error::{Error, FailureKind, Result};
use crate::http::{self, ProbeResult};
use crate::limiter::RateLimiter;
use crate::model::{plan_segments, Segment, TaskId, TaskInfo, TaskState};
use crate::storage::{self, PartFile};
use crate::util::{backoff_delay, unique_path};
use crate::worker::{self, WorkerCtx, WorkerMsg, OPEN_END};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StopReason {
    Paused = 0,
    Cancelled = 1,
    Shutdown = 2,
}

impl StopReason {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Cancelled,
            2 => Self::Shutdown,
            _ => Self::Paused,
        }
    }
}

#[derive(Debug)]
pub enum RunOutcome {
    Completed {
        file_path: PathBuf,
        total_size: u64,
        sha256: Option<String>,
        blake3: Option<String>,
    },
    Failed {
        error: String,
    },
    Stopped {
        reason: StopReason,
    },
}

#[derive(Debug)]
pub enum RunnerMsg {
    State {
        id: TaskId,
        state: TaskState,
    },
    /// Runner persisted new metadata; scheduler should reload the row.
    Refresh {
        id: TaskId,
    },
    Progress {
        id: TaskId,
        downloaded: u64,
    },
    Retrying {
        id: TaskId,
        attempt: u32,
        delay_ms: u64,
        error: String,
    },
    ResourceChanged {
        id: TaskId,
    },
    /// The adaptive controller changed the live connection target.
    Connections {
        id: TaskId,
        count: u32,
    },
    Finished {
        id: TaskId,
        outcome: RunOutcome,
    },
}

pub struct RunSpec {
    pub info: TaskInfo,
    pub segments: Vec<Segment>,
    /// True when the task has never probed successfully; allows naming from
    /// Content-Disposition and skips resume consistency checks.
    pub fresh: bool,
    /// True when the user explicitly chose the file name.
    pub name_locked: bool,
    pub db: Db,
    pub client: reqwest::Client,
    pub config: Arc<EngineConfig>,
    pub global_limiter: Arc<RateLimiter>,
    pub task_limiter: Arc<RateLimiter>,
    pub stop: CancellationToken,
    pub stop_reason: Arc<AtomicU8>,
    pub msg_tx: mpsc::UnboundedSender<RunnerMsg>,
}

pub async fn run(spec: RunSpec) {
    let id = spec.info.id;
    let outcome = run_inner(&spec).await;
    let _ = spec.msg_tx.send(RunnerMsg::Finished { id, outcome });
}

fn send(spec: &RunSpec, msg: RunnerMsg) {
    let _ = spec.msg_tx.send(msg);
}

async fn run_inner(spec: &RunSpec) -> RunOutcome {
    let mut info = spec.info.clone();
    let mut segments = spec.segments.clone();
    let mut fresh = spec.fresh;
    let mut restarts = 0u32;

    loop {
        send(
            spec,
            RunnerMsg::State {
                id: info.id,
                state: TaskState::Probing,
            },
        );
        let probe = match probe_with_retry(spec, &info).await {
            Ok(p) => p,
            Err(ProbeError::Stopped) => return stopped(spec),
            Err(ProbeError::Failed(e)) => {
                return RunOutcome::Failed {
                    error: e.to_string(),
                }
            }
        };

        if !fresh && resource_changed(&info, &probe) {
            restarts += 1;
            let _ = spec
                .db
                .call(move |c| db::bump_restart_count(c, info.id))
                .await;
            send(spec, RunnerMsg::ResourceChanged { id: info.id });
            if restarts > spec.config.retry.max_restarts {
                return RunOutcome::Failed {
                    error: "remote resource keeps changing; giving up".into(),
                };
            }
            tracing::warn!(
                task = info.id,
                "remote resource changed; restarting download"
            );
            let _ = storage::remove_file_if_exists(&info.part_path).await;
            segments.clear();
            fresh = true;
            continue;
        }

        if fresh && !spec.name_locked {
            if let Some(name) = probe.file_name.clone() {
                let dir = info
                    .file_path
                    .parent()
                    .unwrap_or(std::path::Path::new("."))
                    .to_path_buf();
                info.file_name = name;
                info.file_path = unique_path(&dir, &info.file_name);
                info.part_path = part_path_for(&info.file_path, info.id);
            }
        }

        info.final_url = Some(probe.final_url.clone());
        info.total_size = probe.size;
        info.etag = probe.etag.clone();
        info.last_modified = probe.last_modified.clone();
        info.range_supported = probe.range_supported;

        // A server without byte ranges cannot continue a partial stream.
        // Recreate the file instead of appending a fresh GET to old bytes.
        if !probe.range_supported && segments.iter().any(|s| s.downloaded > 0) {
            segments.clear();
        }
        if probe.range_supported {
            if let Some(size) = probe.size {
                reconcile_segments(&mut segments, size);
            }
        }
        let requested = info.connections.max(1);
        if segments.is_empty() {
            segments = match (probe.size, probe.range_supported) {
                (Some(size), true) => plan_segments(size, requested, spec.config.min_segment_size),
                _ => vec![Segment {
                    index: 0,
                    start: 0,
                    end: None,
                    downloaded: 0,
                }],
            };
        }
        // Persisted split rows are units of work, not a concurrency target.
        // A resumed task can have hundreds of rows after tail stealing.
        info.connections = (segments.len() as u32)
            .min(requested)
            .min(spec.config.max_connections_per_task)
            .max(1);

        let meta = MetaUpdate {
            final_url: info.final_url.clone(),
            file_name: info.file_name.clone(),
            file_path: info.file_path.clone(),
            part_path: info.part_path.clone(),
            total_size: info.total_size,
            etag: info.etag.clone(),
            last_modified: info.last_modified.clone(),
            range_supported: info.range_supported,
            connections: info.connections,
            actual_sha256: info.actual_sha256.clone(),
            actual_blake3: info.actual_blake3.clone(),
        };
        let db = spec.db.clone();
        let id = info.id;
        let segs = segments.clone();
        let persist = db.call(move |c| {
            db::replace_segments(c, id, &segs)?;
            db::update_downloaded(c, id, segs.iter().map(|s| s.downloaded).sum())?;
            db::update_meta(c, id, &meta)
        });
        if let Err(e) = persist.await {
            return RunOutcome::Failed {
                error: e.to_string(),
            };
        }
        send(spec, RunnerMsg::Refresh { id: info.id });

        let part = match open_part(&info, &mut segments, &spec.db, spec.config.sparse_files).await {
            Ok(p) => p,
            Err(e) => {
                return RunOutcome::Failed {
                    error: e.to_string(),
                }
            }
        };

        match download_phase(spec, &mut info, &segments, part).await {
            PhaseExit::AllDone(part, total) => {
                return finalize(spec, &info, part, total).await;
            }
            PhaseExit::Failed(e) => {
                return RunOutcome::Failed {
                    error: e.to_string(),
                };
            }
            PhaseExit::Stopped(reason) => {
                return RunOutcome::Stopped { reason };
            }
            PhaseExit::Restart => {
                restarts += 1;
                let _ = spec
                    .db
                    .call(move |c| db::bump_restart_count(c, info.id))
                    .await;
                send(spec, RunnerMsg::ResourceChanged { id: info.id });
                if restarts > spec.config.retry.max_restarts {
                    return RunOutcome::Failed {
                        error: "remote resource keeps changing; giving up".into(),
                    };
                }
                let _ = storage::remove_file_if_exists(&info.part_path).await;
                segments.clear();
                fresh = true;
                continue;
            }
        }
    }
}

pub(crate) fn part_path_for(final_path: &std::path::Path, id: TaskId) -> PathBuf {
    let name = final_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "download".into());
    final_path.with_file_name(format!("{name}.{id}.part"))
}

fn stopped(spec: &RunSpec) -> RunOutcome {
    RunOutcome::Stopped {
        reason: StopReason::from_u8(spec.stop_reason.load(Ordering::Relaxed)),
    }
}

enum ProbeError {
    Stopped,
    Failed(Error),
}

async fn probe_with_retry(
    spec: &RunSpec,
    info: &TaskInfo,
) -> std::result::Result<ProbeResult, ProbeError> {
    let mut attempts = 0u32;
    loop {
        if spec.stop.is_cancelled() {
            return Err(ProbeError::Stopped);
        }
        let confirm = info.connections > 1;
        let result = tokio::select! {
            _ = spec.stop.cancelled() => return Err(ProbeError::Stopped),
            r = http::probe(&spec.client, &info.url, confirm) => r,
        };
        match result {
            Ok(p) => return Ok(p),
            Err(e) if e.is_retryable() && attempts + 1 < spec.config.retry.max_attempts => {
                attempts += 1;
                let delay = backoff_delay(
                    attempts,
                    spec.config.retry.base_delay_ms,
                    spec.config.retry.max_delay_ms,
                );
                send(
                    spec,
                    RunnerMsg::Retrying {
                        id: info.id,
                        attempt: attempts,
                        delay_ms: delay.as_millis() as u64,
                        error: e.to_string(),
                    },
                );
                tokio::select! {
                    _ = spec.stop.cancelled() => return Err(ProbeError::Stopped),
                    _ = tokio::time::sleep(delay) => {}
                }
            }
            Err(e) => return Err(ProbeError::Failed(e)),
        }
    }
}

/// Whether persisted data can no longer be trusted against the remote.
fn resource_changed(info: &TaskInfo, probe: &ProbeResult) -> bool {
    // Unknown-size downloads cannot be resumed with Range at all.
    if info.total_size.is_none() || probe.size.is_none() {
        return true;
    }
    if info.total_size != probe.size {
        return true;
    }
    if let (Some(a), Some(b)) = (&info.etag, &probe.etag) {
        if a != b {
            return true;
        }
    }
    if let (Some(a), Some(b)) = (&info.last_modified, &probe.last_modified) {
        if a != b {
            return true;
        }
    }
    false
}

/// Older split checkpoints could retain a parent's stale end. Rebuild a
/// disjoint partition from persisted start markers, preserving only each
/// row's recorded contiguous prefix. Missing coverage is fetched again.
pub(crate) fn reconcile_segments(segments: &mut Vec<Segment>, size: u64) {
    if segments.is_empty() {
        return;
    }
    if size == 0 || segments.iter().any(|s| s.end.is_none()) {
        segments.clear();
        return;
    }
    segments.retain(|s| s.start < size);
    if segments.is_empty() {
        return;
    }
    for seg in segments.iter_mut() {
        let old_len = seg
            .end
            .unwrap()
            .checked_sub(seg.start)
            .map(|n| n + 1)
            .unwrap_or(0);
        seg.downloaded = seg.downloaded.min(old_len);
    }
    // Prefer the longest durable prefix when duplicate starts exist.
    segments.sort_by_key(|s| (s.start, std::cmp::Reverse(s.downloaded)));
    segments.dedup_by_key(|s| s.start);
    if segments[0].start != 0 {
        let index = segments
            .iter()
            .map(|s| s.index)
            .max()
            .unwrap()
            .saturating_add(1);
        segments.insert(
            0,
            Segment {
                index,
                start: 0,
                end: Some(segments[0].start - 1),
                downloaded: 0,
            },
        );
    }
    for pos in 0..segments.len() {
        let end = segments
            .get(pos + 1)
            .map(|s| s.start - 1)
            .unwrap_or(size - 1);
        let seg = &mut segments[pos];
        seg.end = Some(end);
        seg.downloaded = seg.downloaded.min(end - seg.start + 1);
    }
}

async fn open_part(
    info: &TaskInfo,
    segments: &mut [Segment],
    db: &Db,
    sparse: bool,
) -> Result<PartFile> {
    let has_progress = segments.iter().any(|s| s.downloaded > 0);
    if has_progress {
        match PartFile::open_existing(&info.part_path) {
            Ok(part) => {
                let len = part.len().await?;
                let mut clamped = false;
                for seg in segments.iter_mut() {
                    let max = len.saturating_sub(seg.start);
                    if seg.downloaded > max {
                        seg.downloaded = max;
                        clamped = true;
                    }
                }
                if clamped {
                    let id = info.id;
                    let segs = segments.to_vec();
                    db.call(move |c| db::replace_segments(c, id, &segs)).await?;
                }
                return Ok(part);
            }
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!(task = info.id, "part file missing; restarting from zero");
                for seg in segments.iter_mut() {
                    seg.downloaded = 0;
                }
                let id = info.id;
                let segs = segments.to_vec();
                db.call(move |c| db::replace_segments(c, id, &segs)).await?;
            }
            Err(e) => return Err(e),
        }
    }
    let path = info.part_path.clone();
    let size = info.total_size;
    tokio::task::spawn_blocking(move || PartFile::create_with_sparse(&path, size, sparse)).await?
}

enum PhaseExit {
    AllDone(PartFile, u64),
    Failed(Error),
    Stopped(StopReason),
    Restart,
}

/// A segment with runtime-mutable state. `live_end` (inclusive last byte,
/// `OPEN_END` when the size is unknown) can be shrunk at runtime to hand the
/// segment's tail to a newly spawned worker — IDM-style dynamic splitting.
struct LiveSeg {
    index: u32,
    start: u64,
    live_end: Arc<AtomicU64>,
    progress: Arc<AtomicU64>,
    received: Arc<AtomicU64>,
}

impl LiveSeg {
    fn end_value(&self) -> u64 {
        self.live_end.load(Ordering::Relaxed)
    }

    /// Owned length in bytes, `None` when the size is unknown.
    fn len(&self) -> Option<u64> {
        let end = self.end_value();
        if end == OPEN_END {
            None
        } else {
            Some(end - self.start + 1)
        }
    }

    fn downloaded(&self) -> u64 {
        self.progress.load(Ordering::Relaxed)
    }

    fn is_complete(&self) -> bool {
        self.len().is_some_and(|len| self.downloaded() >= len)
    }

    /// Progress clamped to the owned length. After a split the victim's
    /// counter can transiently exceed its new length (the split happened
    /// while an oversized chunk was in flight); clamped values are what
    /// totals and checkpoints use.
    fn clamped(&self) -> u64 {
        match self.len() {
            Some(len) => self.downloaded().min(len),
            None => self.downloaded(),
        }
    }
}

fn spawn_segment_worker(
    spec: &RunSpec,
    info: &TaskInfo,
    part: &PartFile,
    seg: &LiveSeg,
    status_tx: mpsc::UnboundedSender<WorkerMsg>,
    set: &mut JoinSet<std::result::Result<u32, Error>>,
    releases: &mut HashMap<u32, CancellationToken>,
) -> Result<()> {
    let handle = part.worker_handle()?;
    let release = CancellationToken::new();
    releases.insert(seg.index, release.clone());
    let ctx = WorkerCtx {
        client: spec.client.clone(),
        url: info.final_url.clone().unwrap_or_else(|| info.url.clone()),
        expected_size: info.total_size,
        etag: info.etag.clone(),
        last_modified: info.last_modified.clone(),
        part: part.clone(),
        file: handle,
        received: seg.received.clone(),
        global_limiter: spec.global_limiter.clone(),
        task_limiter: spec.task_limiter.clone(),
        stop: spec.stop.clone(),
        release,
        retry: spec.config.retry.clone(),
        status_tx,
    };
    set.spawn(worker::run_segment(
        ctx,
        seg.index,
        seg.start,
        seg.live_end.clone(),
        seg.progress.clone(),
    ));
    Ok(())
}

/// TCP-inspired adaptive concurrency controller (V2.0).
///
/// Fixed tasks (explicit connection count) never change. Auto tasks move
/// through three regimes, mirroring TCP congestion control:
///
/// - **Slow start**: while throughput keeps scaling with connections, *double*
///   the target each window (2→4→8→16…). The optimal count is reached in
///   `log2` steps instead of one-at-a-time, which is what lets auto mode catch
///   up with a fixed high connection count on short/medium files. An initial
///   optimistic probe bootstraps growth from a steady state.
/// - **Congestion avoidance**: once doubling stops paying off (a bandwidth
///   ceiling flattens the gain), probe `+1` per validated window instead.
/// - **Multiplicative decrease (AIMD)**: throttling / repeated transport
///   errors cut the target by a quarter and leave slow start, so the engine
///   does not re-double into the same server limit.
///
/// Aggregate throughput is EWMA-smoothed to reject single-window noise
/// (Windows timer granularity, worker spawn timing). The controller only
/// *decides*; spawning/releasing workers is the download loop's job (see
/// [`refill`] and [`release_excess`]).
struct ConcurrencyCtl {
    fixed: bool,
    min: u32,
    max: u32,
    target: u32,
    /// EWMA of aggregate bytes/sec, used for every gain decision.
    ewma: Option<f64>,
    /// Smoothed throughput captured at the last target change; the gain
    /// baseline. `None` until the first window completes.
    reference: Option<f64>,
    /// True while doubling (slow start); false in congestion avoidance.
    slow_start: bool,
    /// One optimistic doubling is allowed before any gain evidence exists,
    /// bootstrapping growth from a steady state.
    pending_probe: bool,
    /// Consecutive sub-threshold windows; two in a row leave slow start.
    flat_windows: u32,
    /// Whole windows since the last change (congestion-avoidance cooldown).
    windows_since_change: u32,
    failure_score: u32,
    gain_threshold: f64,
}

/// Failure score that triggers a multiplicative shrink. A 429/503 costs 2, a
/// timeout or transport error costs 1, so one throttled retry or three
/// timeouts in a single window push the score over the line.
const SHRINK_SCORE: u32 = 3;

/// EWMA weight for the newest window. 0.5 tracks a doubling within ~2 windows
/// while still rejecting an isolated noisy sample.
const EWMA_ALPHA: f64 = 0.5;

/// Minimum gain to keep doubling in slow start. An ideal doubling buys +100%
/// throughput; requiring +35% tolerates EWMA lag and noise yet exits promptly
/// when a bandwidth ceiling flattens growth (gain → 0).
const SLOW_START_GAIN: f64 = 0.35;

impl ConcurrencyCtl {
    fn new(fixed: bool, start: u32, min: u32, max: u32, gain_threshold: f64) -> Self {
        let target = start.clamp(min.min(max), max);
        Self {
            fixed,
            min,
            max,
            target,
            ewma: None,
            reference: None,
            slow_start: true,
            pending_probe: true,
            flat_windows: 0,
            windows_since_change: 0,
            failure_score: 0,
            gain_threshold,
        }
    }

    fn penalize(&mut self, kind: FailureKind) {
        if self.fixed {
            return;
        }
        self.failure_score += match kind {
            FailureKind::Throttle => 2,
            FailureKind::Timeout | FailureKind::Network => 1,
            FailureKind::Fatal => 0,
        };
    }

    /// Observe one window. `window_bytes` is what the task received during
    /// the window. Returns the new target when it changed.
    fn observe(&mut self, _now: Instant, window_bytes: u64, window_ms: u64) -> Option<u32> {
        if self.fixed {
            return None;
        }
        let speed = (window_bytes * 1000 / window_ms.max(1)) as f64;
        let smoothed = match self.ewma {
            Some(prev) => EWMA_ALPHA * speed + (1.0 - EWMA_ALPHA) * prev,
            None => speed,
        };
        self.ewma = Some(smoothed);

        // AIMD: throttling / repeated transport errors → give concurrency back
        // by a quarter (floor min) and leave slow start so we don't re-double
        // into the same wall.
        if self.failure_score >= SHRINK_SCORE {
            self.failure_score = 0;
            self.slow_start = false;
            self.pending_probe = false;
            self.flat_windows = 0;
            self.windows_since_change = 0;
            self.reference = Some(smoothed);
            let cut = (self.target / 4).max(1);
            let new = self.min.max(self.target.saturating_sub(cut));
            if new != self.target {
                self.target = new;
                return Some(new);
            }
            return None;
        }
        if self.failure_score > 0 {
            self.failure_score -= 1;
        }

        // First completed window: record the throughput baseline.
        let Some(reference) = self.reference else {
            self.reference = Some(smoothed);
            return None;
        };
        let gain = smoothed / reference.max(1.0) - 1.0;

        if self.slow_start {
            // Bootstrap: double once optimistically before any gain evidence.
            if self.pending_probe {
                self.pending_probe = false;
                if self.target < self.max {
                    return Some(self.grow_double(smoothed));
                }
                self.slow_start = false;
            }
            if gain >= SLOW_START_GAIN && self.target < self.max {
                self.flat_windows = 0;
                return Some(self.grow_double(smoothed));
            }
            // Gain flattened: debounce with two consecutive windows before
            // dropping to congestion avoidance (rejects a single noisy sample).
            if gain < SLOW_START_GAIN {
                self.flat_windows += 1;
                if self.flat_windows >= 2 || self.target >= self.max {
                    self.slow_start = false;
                    self.reference = Some(smoothed);
                    self.windows_since_change = 0;
                }
            } else {
                self.flat_windows = 0;
            }
            return None;
        }

        // Congestion avoidance: +1 per validated window while gain holds.
        self.windows_since_change += 1;
        if self.windows_since_change >= 2 {
            self.windows_since_change = 0;
            if gain >= self.gain_threshold && self.target < self.max {
                self.target += 1;
                self.reference = Some(smoothed);
                return Some(self.target);
            }
            // Track the moving baseline so a later real gain is still visible.
            self.reference = Some(smoothed);
        }
        None
    }

    /// Double the target (capped at `max`) and reset the gain baseline.
    fn grow_double(&mut self, smoothed: f64) -> u32 {
        self.target = self.max.min(self.target.saturating_mul(2));
        self.reference = Some(smoothed);
        self.windows_since_change = 0;
        if self.target >= self.max {
            self.slow_start = false;
        }
        self.target
    }
}

/// Pick the active segment with the most remaining bytes and split off its
/// tail half for a new worker. This lets a fast connection take over a slow
/// one's remaining range instead of idling until the straggler finishes.
fn try_steal(
    segs: &[LiveSeg],
    active: &std::collections::HashSet<u32>,
    next_index: u32,
    min_steal: u64,
) -> Option<(u32, Option<u64>, LiveSeg, Segment)> {
    let mut best: Option<(u64, usize)> = None;
    for (pos, seg) in segs.iter().enumerate() {
        if !active.contains(&seg.index) || seg.is_complete() {
            continue;
        }
        let Some(len) = seg.len() else { continue };
        let remaining = len
            - seg
                .received
                .load(Ordering::Relaxed)
                .max(seg.downloaded())
                .min(len);
        if remaining >= 2 * min_steal && best.is_none_or(|(r, _)| remaining > r) {
            best = Some((remaining, pos));
        }
    }
    let (_, pos) = best?;
    let victim = &segs[pos];
    let len = victim.len()?;
    let done = victim
        .received
        .load(Ordering::Relaxed)
        .max(victim.downloaded())
        .min(len);
    let mid = victim.start + done + (len - done) / 2;
    let old_end = victim.end_value();
    // Shrink the victim first; its worker truncates against this value. The
    // victim and the thief may briefly write the same offsets while an
    // oversized chunk is in flight, but they write identical server bytes.
    victim.live_end.store(mid - 1, Ordering::Relaxed);
    let thief = LiveSeg {
        index: next_index,
        start: mid,
        live_end: Arc::new(AtomicU64::new(old_end)),
        progress: Arc::new(AtomicU64::new(0)),
        received: Arc::new(AtomicU64::new(0)),
    };
    let thief_row = Segment {
        index: next_index,
        start: mid,
        end: Some(old_end),
        downloaded: 0,
    };
    Some((victim.index, Some(mid - 1), thief, thief_row))
}

/// Assign a fresh worker to the best available work when a slot is free:
///
/// 1. Re-claim an orphaned incomplete segment wholesale — its worker was
///    released by a concurrency shrink or otherwise exited early.
/// 2. Otherwise split the tail off the active segment with the most bytes
///    left and spawn a worker for the tail (IDM-style dynamic split).
///
/// Returns `true` when a worker was spawned.
#[allow(clippy::too_many_arguments)]
async fn refill(
    spec: &RunSpec,
    info: &TaskInfo,
    part: &PartFile,
    segs: &mut Vec<LiveSeg>,
    set: &mut JoinSet<std::result::Result<u32, Error>>,
    active: &mut std::collections::HashSet<u32>,
    releases: &mut HashMap<u32, CancellationToken>,
    done: &std::collections::HashSet<u32>,
    next_index: &mut u32,
    target: u32,
    status_tx: mpsc::UnboundedSender<WorkerMsg>,
    min_steal: u64,
) -> Result<bool> {
    let mut spawned = false;
    // Fill every free slot up to the (possibly doubled) target in one pass.
    // Slow-start growth must spawn all new workers now, not one per window.
    while active.len() < target.max(1) as usize {
        // 1. Orphaned segments first: they have no worker at all, so
        //    re-claiming them is strictly better than re-splitting an active
        //    one.
        let orphan = segs
            .iter()
            .find(|s| !active.contains(&s.index) && !done.contains(&s.index) && !s.is_complete());
        if let Some(seg) = orphan {
            let index = seg.index;
            tracing::debug!(task = info.id, "re-claiming orphaned segment {index}");
            spawn_segment_worker(spec, info, part, seg, status_tx.clone(), set, releases)?;
            active.insert(index);
            spawned = true;
            continue;
        }
        // 2. Split the fattest active segment.
        let Some((victim, victim_end, thief, thief_row)) =
            try_steal(segs, active, *next_index, min_steal)
        else {
            break;
        };
        *next_index += 1;
        let db = spec.db.clone();
        let id = info.id;
        // A failed split must stop the task instead of leaving a partial map.
        db.call(move |c| {
            let tx = c.unchecked_transaction()?;
            db::update_segment_end(&tx, id, victim, victim_end)?;
            db::insert_segment(&tx, id, &thief_row)?;
            tx.commit()?;
            Ok(())
        })
        .await?;
        spawn_segment_worker(spec, info, part, &thief, status_tx.clone(), set, releases)?;
        active.insert(thief.index);
        segs.push(thief);
        spawned = true;
        tracing::debug!(
            task = info.id,
            "split segment {victim}; spawned worker for the tail"
        );
    }
    Ok(spawned)
}

/// Cancel the newest workers while more are active than the (shrunk) target
/// allows. Their segments stay incomplete and are re-claimed by [`refill`]
/// when a slot frees up again.
fn release_excess(
    info: &TaskInfo,
    active: &std::collections::HashSet<u32>,
    releases: &HashMap<u32, CancellationToken>,
    target: u32,
) {
    let excess = active.len().saturating_sub(target.max(1) as usize);
    if excess == 0 {
        return;
    }
    let mut victims: Vec<u32> = releases.keys().copied().collect();
    victims.retain(|i| active.contains(i));
    victims.sort_unstable();
    for index in victims.into_iter().rev().take(excess) {
        if let Some(token) = releases.get(&index) {
            tracing::debug!(task = info.id, "releasing worker {index} (target {target})");
            token.cancel();
        }
    }
}

async fn download_phase(
    spec: &RunSpec,
    info: &mut TaskInfo,
    segments: &[Segment],
    part: PartFile,
) -> PhaseExit {
    let (status_tx, mut status_rx) = mpsc::unbounded_channel();
    let mut segs: Vec<LiveSeg> = segments
        .iter()
        .map(|s| LiveSeg {
            index: s.index,
            start: s.start,
            live_end: Arc::new(AtomicU64::new(s.end.unwrap_or(OPEN_END))),
            progress: Arc::new(AtomicU64::new(s.downloaded)),
            received: Arc::new(AtomicU64::new(s.downloaded)),
        })
        .collect();
    let mut next_index = segs.iter().map(|s| s.index).max().unwrap_or(0) + 1;
    let min_steal = spec.config.min_segment_size.max(256 * 1024);
    let mut set = JoinSet::new();
    let mut active = std::collections::HashSet::new();
    let mut releases: HashMap<u32, CancellationToken> = HashMap::new();
    // Indices whose worker has exited with success. Completion is tracked by
    // worker exit rather than `LiveSeg::is_complete` because open-ended
    // segments (unknown size) can never report a byte-based completion.
    let mut done = std::collections::HashSet::new();

    // Adaptive concurrency: pinned tasks observe a fixed target; auto tasks
    // start at the planned segment count and let the controller steer.
    let fixed = !info.auto_concurrency;
    let ctl_min = spec
        .config
        .adaptive
        .min_connections
        .min(spec.config.max_connections_per_task)
        .max(1);
    let ctl_max = spec.config.max_connections_per_task.max(ctl_min);
    let mut ctl = ConcurrencyCtl::new(
        fixed,
        info.connections,
        ctl_min,
        ctl_max,
        spec.config.adaptive.grow_gain_pct as f64 / 100.0,
    );
    let mut target = info.connections.max(1);

    for seg in &segs {
        if seg.is_complete() {
            // Fully downloaded on resume; no worker will ever exit for it.
            done.insert(seg.index);
            continue;
        }
        if active.len() >= target as usize {
            continue; // Remaining resume rows are reclaimed as slots free up.
        }
        if let Err(e) = spawn_segment_worker(
            spec,
            info,
            &part,
            seg,
            status_tx.clone(),
            &mut set,
            &mut releases,
        ) {
            return PhaseExit::Failed(e);
        }
        active.insert(seg.index);
    }

    if set.is_empty() {
        let total: u64 = segs.iter().map(LiveSeg::clamped).sum();
        return PhaseExit::AllDone(part, total);
    }

    send(
        spec,
        RunnerMsg::State {
            id: info.id,
            state: TaskState::Downloading,
        },
    );
    let mut worker_status: HashMap<u32, bool> = HashMap::new();
    let mut last_state = TaskState::Downloading;
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut flush_tick = tokio::time::interval(Duration::from_millis(
        spec.config.checkpoint.interval_ms.max(100),
    ));
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Checkpoint throttling: at low speeds skip DB writes until either
    // `checkpoint.bytes` of progress or 5× the interval elapsed. DB state
    // lagging behind disk is always safe (resume clamps against the file).
    let flush_interval = Duration::from_millis(spec.config.checkpoint.interval_ms.max(100));
    let mut last_ckpt_total: u64 = segs.iter().map(LiveSeg::clamped).sum();
    let mut last_ckpt_time = Instant::now();
    let window_ms = spec.config.adaptive.window_ms.max(50);
    let mut ctl_tick = tokio::time::interval(Duration::from_millis(window_ms));
    ctl_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut prev_total: u64 = segs.iter().map(LiveSeg::clamped).sum();

    enum LoopExit {
        AllDone,
        Failed(Error),
        Stopped(StopReason),
        Restart,
    }

    let exit = loop {
        tokio::select! {
            _ = spec.stop.cancelled() => {
                break LoopExit::Stopped(StopReason::from_u8(spec.stop_reason.load(Ordering::Relaxed)));
            }
            joined = set.join_next(), if !set.is_empty() => {
                match joined {
                    Some(Ok(Ok(index))) | Some(Ok(Err(Error::WorkerReleased { index }))) => {
                        let completed = matches!(joined, Some(Ok(Ok(_))));
                        active.remove(&index);
                        releases.remove(&index);
                        if completed {
                            done.insert(index);
                            // Progress is persisted by the task checkpoint only,
                            // after its file data has been flushed.
                        }
                        // Refill the freed worker slot: reclaim an orphaned
                        // segment first, then steal the tail of the segment
                        // with the most remaining bytes.
                        if let Err(e) = refill(
                            spec, info, &part, &mut segs, &mut set, &mut active,
                            &mut releases, &done, &mut next_index, target,
                            status_tx.clone(), min_steal,
                        )
                        .await
                        {
                            break LoopExit::Failed(e);
                        }
                        if done.len() == segs.len() {
                            break LoopExit::AllDone;
                        }
                    }
                    Some(Ok(Err(Error::ResourceChanged))) => {
                        break LoopExit::Restart;
                    }
                    Some(Ok(Err(Error::Cancelled))) => {
                        // Stop token fired (pause/cancel/shutdown): a clean stop, not a
                        // failure. select! may surface this before the stop.cancelled() arm.
                        break LoopExit::Stopped(StopReason::from_u8(
                            spec.stop_reason.load(Ordering::Relaxed),
                        ));
                    }
                    Some(Ok(Err(e))) => {
                        break LoopExit::Failed(e);
                    }
                    Some(Err(e)) => {
                        break LoopExit::Failed(Error::Internal(format!("worker crashed: {e}")));
                    }
                    None => {}
                }
            }
            Some(msg) = status_rx.recv() => {
                match msg {
                    WorkerMsg::Active { index } => { worker_status.insert(index, true); }
                    WorkerMsg::Backoff { index, attempt, delay_ms, error, kind } => {
                        worker_status.insert(index, false);
                        ctl.penalize(kind);
                        send(spec, RunnerMsg::Retrying { id: info.id, attempt, delay_ms, error });
                    }
                }
                let any_active = worker_status.values().any(|&a| a);
                let new_state = if any_active { TaskState::Downloading } else { TaskState::Retrying };
                if new_state != last_state {
                    last_state = new_state;
                    send(spec, RunnerMsg::State { id: info.id, state: new_state });
                }
            }
            // Adaptive window: feed the controller, apply target changes and
            // fill free slots (growth splits, orphan re-claims).
            _ = ctl_tick.tick() => {
                let total: u64 = segs.iter().map(LiveSeg::clamped).sum();
                let window_bytes = total.saturating_sub(prev_total);
                prev_total = total;
                if let Some(new_target) = ctl.observe(Instant::now(), window_bytes, window_ms) {
                    target = new_target;
                    info.connections = new_target;
                    send(spec, RunnerMsg::Connections { id: info.id, count: new_target });
                    release_excess(info, &active, &releases, target);
                }
                if active.len() < target.max(1) as usize {
                    if let Err(e) = refill(
                        spec, info, &part, &mut segs, &mut set, &mut active,
                        &mut releases, &done, &mut next_index, target,
                        status_tx.clone(), min_steal,
                    )
                    .await
                    {
                        break LoopExit::Failed(e);
                    }
                }
            }
            // One flush + one batched DB write per interval, replacing the old
            // per-worker FlushFileBuffers storm.
            _ = flush_tick.tick() => {
                let total: u64 = segs.iter().map(LiveSeg::clamped).sum();
                let bytes_ok = total.saturating_sub(last_ckpt_total)
                    >= spec.config.checkpoint.bytes.max(1);
                let time_ok = last_ckpt_time.elapsed() >= flush_interval * 5;
                if !bytes_ok && !time_ok {
                    continue;
                }
                if let Err(e) = async {
                    // Snapshot BEFORE flushing: workers may advance during
                    // sync, and those later bytes are not covered by it.
                    let rows: Vec<(u32, u64)> = segs.iter().map(|s| (s.index, s.clamped())).collect();
                    part.sync_data().await?;
                    let db = spec.db.clone();
                    let id = info.id;
                    db.call(move |c| db::checkpoint_segments(c, id, &rows))
                    .await
                }
                .await
                {
                    break LoopExit::Failed(e);
                }
                last_ckpt_total = total;
                last_ckpt_time = Instant::now();
            }
            _ = tick.tick() => {
                let total: u64 = segs.iter().map(LiveSeg::clamped).sum();
                send(spec, RunnerMsg::Progress { id: info.id, downloaded: total });
            }
        }
    };

    // Never abort a worker with a blocking positional write in flight.
    // Release it and join its final flush before syncing, restarting or
    // returning a pause acknowledgement to a caller that may resume at once.
    for release in releases.values() {
        release.cancel();
    }
    while set.join_next().await.is_some() {}

    let total: u64 = segs.iter().map(LiveSeg::clamped).sum();
    send(
        spec,
        RunnerMsg::Progress {
            id: info.id,
            downloaded: total,
        },
    );

    if matches!(exit, LoopExit::AllDone) {
        return PhaseExit::AllDone(part, total);
    }

    // Flush and checkpoint everything the workers reported so a restart can
    // resume from durable data.
    if let Err(e) = part.sync_data().await {
        return PhaseExit::Failed(e);
    }
    {
        let db = spec.db.clone();
        let id = info.id;
        let rows: Vec<(u32, u64)> = segs.iter().map(|s| (s.index, s.clamped())).collect();
        if let Err(e) = db
            .call(move |c| db::checkpoint_segments(c, id, &rows))
            .await
        {
            tracing::error!(task = info.id, "checkpoint on stop failed: {e}");
        }
    }
    match exit {
        LoopExit::AllDone => unreachable!(),
        LoopExit::Failed(e) => PhaseExit::Failed(e),
        LoopExit::Restart => PhaseExit::Restart,
        LoopExit::Stopped(reason) => {
            if reason == StopReason::Cancelled {
                drop(part);
                let _ = storage::remove_file_if_exists(&info.part_path).await;
            }
            PhaseExit::Stopped(reason)
        }
    }
}

async fn finalize(spec: &RunSpec, info: &TaskInfo, part: PartFile, total: u64) -> RunOutcome {
    if info.total_size.is_some_and(|expected| expected != total) {
        return RunOutcome::Failed {
            error: format!("downloaded {total} bytes, expected {:?}", info.total_size),
        };
    }
    if let Err(e) = part.sync_all().await {
        return RunOutcome::Failed {
            error: e.to_string(),
        };
    }
    // Length verification before anything else: the segments can only claim
    // bytes that actually reached the part file, but the pre-allocated file
    // size must also match the summed segment lengths exactly.
    match part.len().await {
        Ok(len) if len == total => {}
        Ok(len) => {
            return RunOutcome::Failed {
                error: Error::Protocol(format!(
                    "final file length {len} does not match expected {total}"
                ))
                .to_string(),
            };
        }
        Err(e) => {
            return RunOutcome::Failed {
                error: e.to_string(),
            };
        }
    }
    let mut actual_sha256 = None;
    let mut actual_blake3 = None;
    if let Some(expected) = &info.expected_sha256 {
        match storage::hash_file(part.path(), storage::HashAlgo::Sha256).await {
            Ok(hash) => {
                if !hash.eq_ignore_ascii_case(expected) {
                    let id = info.id;
                    let _ = spec.db.call(move |c| db::reset_segments(c, id)).await;
                    drop(part);
                    let _ = storage::remove_file_if_exists(&info.part_path).await;
                    return RunOutcome::Failed {
                        error: Error::ChecksumMismatch {
                            expected: expected.clone(),
                            actual: hash,
                        }
                        .to_string(),
                    };
                }
                actual_sha256 = Some(hash);
            }
            Err(e) => {
                return RunOutcome::Failed {
                    error: e.to_string(),
                }
            }
        }
    }
    if let Some(expected) = &info.expected_blake3 {
        match storage::hash_file(part.path(), storage::HashAlgo::Blake3).await {
            Ok(hash) => {
                if !hash.eq_ignore_ascii_case(expected) {
                    let id = info.id;
                    let _ = spec.db.call(move |c| db::reset_segments(c, id)).await;
                    drop(part);
                    let _ = storage::remove_file_if_exists(&info.part_path).await;
                    return RunOutcome::Failed {
                        error: Error::ChecksumMismatch {
                            expected: expected.clone(),
                            actual: hash,
                        }
                        .to_string(),
                    };
                }
                actual_blake3 = Some(hash);
            }
            Err(e) => {
                return RunOutcome::Failed {
                    error: e.to_string(),
                }
            }
        }
    }
    let final_path = match storage::commit(part, &info.file_path).await {
        Ok(p) => p,
        Err(e) => {
            return RunOutcome::Failed {
                error: e.to_string(),
            }
        }
    };
    let meta = MetaUpdate {
        final_url: info.final_url.clone(),
        file_name: info.file_name.clone(),
        file_path: final_path.clone(),
        part_path: info.part_path.clone(),
        total_size: Some(total),
        etag: info.etag.clone(),
        last_modified: info.last_modified.clone(),
        range_supported: info.range_supported,
        connections: info.connections,
        actual_sha256: actual_sha256.clone(),
        actual_blake3: actual_blake3.clone(),
    };
    let db = spec.db.clone();
    let id = info.id;
    if let Err(e) = db
        .call(move |c| {
            db::update_meta(c, id, &meta)?;
            db::update_downloaded(c, id, total)
        })
        .await
    {
        return RunOutcome::Failed {
            error: e.to_string(),
        };
    }
    RunOutcome::Completed {
        file_path: final_path,
        total_size: total,
        sha256: actual_sha256,
        blake3: actual_blake3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resumed_partition_repairs_stale_ends_without_inflating_progress() {
        let mut rows = vec![
            Segment {
                index: 0,
                start: 0,
                end: Some(49),
                downloaded: 40,
            },
            Segment {
                index: 1,
                start: 50,
                end: Some(99),
                downloaded: 30,
            },
            Segment {
                index: 2,
                start: 75,
                end: Some(99),
                downloaded: 10,
            },
            Segment {
                index: 3,
                start: 75,
                end: Some(99),
                downloaded: 15,
            },
        ];
        reconcile_segments(&mut rows, 100);
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows.iter()
                .map(|s| s.end.unwrap() - s.start + 1)
                .sum::<u64>(),
            100
        );
        assert_eq!(rows.iter().map(|s| s.downloaded).sum::<u64>(), 80);
        assert_eq!(rows[1].end, Some(74));
        assert_eq!(rows[2].downloaded, 15);
    }

    #[test]
    fn repeated_splits_persist_live_end_not_original_end() {
        let victim = LiveSeg {
            index: 0,
            start: 0,
            live_end: Arc::new(AtomicU64::new(999)),
            progress: Arc::new(AtomicU64::new(0)),
            received: Arc::new(AtomicU64::new(0)),
        };
        let segs = vec![victim];
        let active = std::collections::HashSet::from([0]);
        let (_, _, _, first) = try_steal(&segs, &active, 1, 10).unwrap();
        let (_, _, second, row) = try_steal(&segs, &active, 2, 10).unwrap();
        assert_eq!(first.end, Some(999));
        assert_eq!(row.end, Some(499));
        assert_eq!(second.end_value(), 499);
        assert_eq!(row.end.unwrap() + 1, first.start);
    }

    #[test]
    fn splits_do_not_re_request_queued_disk_bytes() {
        let victim = LiveSeg {
            index: 0,
            start: 0,
            live_end: Arc::new(AtomicU64::new(99)),
            progress: Arc::new(AtomicU64::new(0)),
            received: Arc::new(AtomicU64::new(80)),
        };
        let segs = vec![victim];
        let active = std::collections::HashSet::from([0]);
        let (_, end, _, row) = try_steal(&segs, &active, 1, 5).unwrap();
        assert_eq!(row.start, 90);
        assert_eq!(end, Some(89));
    }

    /// Feed one 1000ms window whose raw aggregate speed is `mib` MiB/s.
    fn win(c: &mut ConcurrencyCtl, mib: u64) -> Option<u32> {
        c.observe(Instant::now(), mib * 1024 * 1024, 1000)
    }

    #[test]
    fn fixed_mode_is_inert() {
        let mut c = ConcurrencyCtl::new(true, 4, 2, 64, 0.05);
        for _ in 0..20 {
            assert_eq!(win(&mut c, 400), None);
        }
        assert_eq!(c.target, 4);
    }

    #[test]
    fn slow_start_doubles_under_linear_scaling() {
        // 4 MiB/s per connection, no aggregate cap: throughput = 4 * target.
        let mut c = ConcurrencyCtl::new(false, 2, 2, 64, 0.05);
        let mut t = 2u32;
        for _ in 0..8 {
            if let Some(n) = win(&mut c, 4 * t as u64) {
                t = n;
            }
        }
        // log2 growth must reach the ceiling within a handful of windows,
        // unlike the old +1-per-2-windows ramp.
        assert_eq!(t, 64, "slow start failed to reach max, got {t}");
    }

    #[test]
    fn slow_start_exits_under_aggregate_cap() {
        // Per-conn 4 MiB/s but the pipe caps at 32 MiB/s (8 connections' worth).
        let mut c = ConcurrencyCtl::new(false, 2, 2, 64, 0.05);
        let mut t = 2u32;
        for _ in 0..12 {
            let speed = (4 * t as u64).min(32);
            if let Some(n) = win(&mut c, speed) {
                t = n;
            }
        }
        // Must NOT run away to max=64 once the ceiling flattens the gain.
        assert!(t < 32, "controller overshoot the cap to {t}");
        assert!(!c.slow_start, "should have left slow start at the ceiling");
    }

    #[test]
    fn throttle_triggers_multiplicative_decrease() {
        let mut c = ConcurrencyCtl::new(false, 16, 2, 64, 0.05);
        // Establish a baseline and grow a little so target > min.
        win(&mut c, 64);
        c.penalize(FailureKind::Throttle); // +2
        c.penalize(FailureKind::Throttle); // +2 => score 4 >= SHRINK_SCORE
        let before = c.target;
        let after = win(&mut c, 64);
        assert!(after.is_some(), "throttle must shrink the target");
        let new = after.unwrap();
        assert!(new < before, "AIMD must reduce concurrency");
        // Quarter cut, not a single step: 16 -> 12.
        assert_eq!(new, before - (before / 4).max(1));
        assert!(!c.slow_start, "must leave slow start after a throttle");
    }

    #[test]
    fn shrink_respects_min_floor() {
        let mut c = ConcurrencyCtl::new(false, 2, 2, 64, 0.05);
        win(&mut c, 8);
        for _ in 0..5 {
            c.penalize(FailureKind::Throttle);
            win(&mut c, 8);
        }
        assert!(c.target >= c.min, "target dropped below min");
    }
}
