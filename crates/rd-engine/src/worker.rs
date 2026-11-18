use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_util::StreamExt;
use reqwest::header::{ACCEPT_ENCODING, IF_RANGE, RANGE};
use reqwest::StatusCode;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::RetryConfig;
use crate::error::{Error, FailureKind, Result};
use crate::http::{parse_content_range, status_error};
use crate::limiter::RateLimiter;
use crate::storage::PartFile;
use crate::util::backoff_delay;

/// Live-end sentinel for segments whose total size is unknown; such segments
/// stream until the server closes and are never split.
pub const OPEN_END: u64 = u64::MAX;

/// Coalesce received chunks into writes of roughly this size. One positional
/// write of 256 KiB is far cheaper than four 64 KiB writes on the blocking
/// pool, and large sequential writes are much faster on spinning disks.
const WRITE_BUFFER_BYTES: usize = 256 * 1024;

/// A connection that delivered at least this many bytes resets the retry
/// counter, so long downloads on flaky networks don't exhaust attempts.
const PROGRESS_RESET_BYTES: u64 = 1024 * 1024;

#[derive(Debug)]
pub enum WorkerMsg {
    Active {
        index: u32,
    },
    Backoff {
        index: u32,
        attempt: u32,
        delay_ms: u64,
        error: String,
        /// Coarse failure class for the adaptive concurrency controller.
        kind: FailureKind,
    },
}

pub struct WorkerCtx {
    pub client: reqwest::Client,
    pub url: String,
    pub expected_size: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    /// Part-file coordinator for reusable positional writes.
    pub part: PartFile,
    /// This worker's *own* file handle. Positional writes on Windows briefly
    /// move the shared file pointer, so handles are never shared between
    /// workers.
    pub file: Arc<std::fs::File>,
    /// Bytes accepted into this worker's bounded disk pipeline. Splits use
    /// this reservation so buffered data is not requested a second time.
    pub received: Arc<AtomicU64>,
    pub global_limiter: Arc<RateLimiter>,
    pub task_limiter: Arc<RateLimiter>,
    pub stop: CancellationToken,
    /// Per-worker release signal: cancelled by the scheduler when the task's
    /// target concurrency drops and this worker is the excess. Unlike `stop`,
    /// releasing one worker never affects the task or its segments.
    pub release: CancellationToken,
    pub retry: RetryConfig,
    pub status_tx: mpsc::UnboundedSender<WorkerMsg>,
}

/// Download one segment to completion, retrying transient failures.
///
/// `seg_start` is the segment's first byte and `seg_end` is its **inclusive**
/// last byte, held in an `AtomicU64` so the runner can split the segment at
/// runtime (IDM-style work stealing): a shrinking end simply truncates the
/// data this worker accepts. Returns the segment index on success.
pub async fn run_segment(
    ctx: WorkerCtx,
    index: u32,
    seg_start: u64,
    seg_end: Arc<AtomicU64>,
    progress: Arc<AtomicU64>,
) -> Result<u32> {
    let mut attempts: u32 = 0;
    loop {
        if ctx.stop.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if ctx.release.is_cancelled() {
            return Err(Error::WorkerReleased { index });
        }
        if is_complete(seg_start, &seg_end, &progress) {
            return Ok(index);
        }
        if seg_end.load(Ordering::Relaxed) == OPEN_END && progress.load(Ordering::Relaxed) > 0 {
            // download_once drains its queued writes before returning. A
            // non-range response starts at byte zero on every retry.
            ctx.part.reset_len(ctx.expected_size.unwrap_or(0)).await?;
            progress.store(0, Ordering::Relaxed);
            ctx.received.store(0, Ordering::Relaxed);
        }
        let _ = ctx.status_tx.send(WorkerMsg::Active { index });
        let before = progress.load(Ordering::Relaxed);
        let result = download_once(&ctx, seg_start, &seg_end, &progress).await;
        let made_progress = seg_end.load(Ordering::Relaxed) != OPEN_END
            && progress.load(Ordering::Relaxed).saturating_sub(before) >= PROGRESS_RESET_BYTES;

        let err = match result {
            Ok(()) => {
                if is_complete(seg_start, &seg_end, &progress)
                    || seg_end.load(Ordering::Relaxed) == OPEN_END
                {
                    return Ok(index);
                }
                Error::Protocol(format!(
                    "segment {index} stream ended before the range was complete"
                ))
            }
            Err(Error::Cancelled) => return Err(Error::Cancelled),
            Err(Error::Released) => return Err(Error::WorkerReleased { index }),
            Err(Error::ResourceChanged) => return Err(Error::ResourceChanged),
            Err(e) if e.is_retryable() => e,
            Err(e) => return Err(e),
        };

        if made_progress {
            attempts = 0;
        } else {
            attempts += 1;
        }
        if attempts >= ctx.retry.max_attempts {
            return Err(err);
        }
        match sleep_backoff(&ctx, attempts, &err, index).await {
            Ok(()) => {}
            Err(Error::Released) => return Err(Error::WorkerReleased { index }),
            Err(e) => return Err(e),
        }
    }
}

fn is_complete(start: u64, end: &AtomicU64, progress: &AtomicU64) -> bool {
    let end = end.load(Ordering::Relaxed);
    end != OPEN_END && progress.load(Ordering::Relaxed) > end - start
}

async fn download_once(
    ctx: &WorkerCtx,
    seg_start: u64,
    seg_end: &AtomicU64,
    progress: &AtomicU64,
) -> Result<()> {
    let mut req = ctx.client.get(&ctx.url);
    // Range offsets are byte offsets into the *stored* representation. Any
    // content codings (gzip/br) would silently change them; identity forbids
    // the server from transforming the body.
    req = req.header(ACCEPT_ENCODING, "identity");
    let end = seg_end.load(Ordering::Relaxed);
    if end != OPEN_END {
        req = req.header(
            RANGE,
            format!(
                "bytes={}-{}",
                seg_start + progress.load(Ordering::Relaxed),
                end
            ),
        );
        if let Some(validator) = ctx.etag.as_deref().or(ctx.last_modified.as_deref()) {
            req = req.header(IF_RANGE, validator);
        }
    }
    let resp = tokio::select! {
        _ = ctx.stop.cancelled() => return Err(Error::Cancelled),
        _ = ctx.release.cancelled() => return Err(Error::Released),
        r = req.send() => r?,
    };
    let status = resp.status();
    if status == StatusCode::PARTIAL_CONTENT {
        if end == OPEN_END {
            return Err(Error::Protocol(
                "unexpected 206 for an open-ended segment".into(),
            ));
        }
        let cr = resp
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_content_range)
            .ok_or_else(|| Error::Protocol("206 without a valid Content-Range".into()))?;
        if cr.0 != seg_start + progress.load(Ordering::Relaxed) {
            return Err(Error::Protocol(format!(
                "Content-Range start {} does not match requested offset {}",
                cr.0,
                seg_start + progress.load(Ordering::Relaxed)
            )));
        }
        if let (Some(total), Some(expected)) = (cr.2, ctx.expected_size) {
            if total != expected {
                return Err(Error::ResourceChanged);
            }
        }
        if cr.1 > end {
            return Err(Error::Protocol(
                "Content-Range exceeds the segment boundary".into(),
            ));
        }
    } else if status.is_success() {
        // A 200 to a Range request means the resource changed (If-Range) or
        // the server ignores Range; both require a task-level restart.
        if !(end == OPEN_END && seg_start == 0) {
            return Err(Error::ResourceChanged);
        }
    } else if status == StatusCode::RANGE_NOT_SATISFIABLE {
        if is_complete(seg_start, seg_end, progress) {
            return Ok(());
        }
        return Err(Error::ResourceChanged);
    } else {
        return Err(status_error(&ctx.url, &resp));
    }

    let mut stream = resp.bytes_stream();
    let mut writer = DiskWriter::new();
    // `downloaded` counts bytes *accepted* from the stream; the shared
    // `progress` counter is only advanced once those bytes are actually
    // written to disk, preserving the crash-safety invariant that the DB can
    // never claim bytes a crash could lose.
    let mut downloaded = progress.load(Ordering::Relaxed);
    let mut flush_tick = tokio::time::interval(std::time::Duration::from_secs(1));
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let outcome = 'pump: loop {
        let chunk = tokio::select! {
            _ = ctx.stop.cancelled() => break 'pump Err(Error::Cancelled),
            _ = ctx.release.cancelled() => break 'pump Err(Error::Released),
            r = writer.complete_pending(ctx, seg_start, progress), if writer.pending.is_some() => {
                r?;
                continue 'pump;
            }
            _ = flush_tick.tick() => {
                writer.flush(ctx, seg_start, downloaded, progress).await?;
                continue 'pump;
            }
            c = stream.next() => match c {
                Some(Ok(c)) => c,
                Some(Err(e)) => break 'pump Err(e.into()),
                None => break 'pump Ok(()),
            },
        };
        let mut chunk = chunk;
        if chunk.is_empty() {
            continue;
        }
        // The live end may have been shrunk by a split; accept only the bytes
        // still owned by this segment. Both this worker and the thief write
        // identical server bytes for any overlapped region, so the race is
        // benign.
        let end = seg_end.load(Ordering::Relaxed);
        if end != OPEN_END {
            let limit = (end + 1).saturating_sub(seg_start + downloaded);
            if limit == 0 {
                break 'pump Ok(());
            }
            if chunk.len() as u64 > limit {
                chunk = chunk.slice(..limit as usize);
            }
        }
        tokio::select! {
            _ = ctx.stop.cancelled() => break 'pump Err(Error::Cancelled),
            _ = ctx.release.cancelled() => break 'pump Err(Error::Released),
            _ = ctx.task_limiter.acquire(chunk.len() as u64) => {},
        }
        tokio::select! {
            _ = ctx.stop.cancelled() => break 'pump Err(Error::Cancelled),
            _ = ctx.release.cancelled() => break 'pump Err(Error::Released),
            _ = ctx.global_limiter.acquire(chunk.len() as u64) => {},
        }
        // A transport frame may exceed our batch size. Never grow the buffer
        // to fit it: copy and write bounded slices, reusing one allocation.
        let mut remaining = chunk.as_ref();
        while !remaining.is_empty() {
            let n = remaining.len().min(WRITE_BUFFER_BYTES - writer.buf.len());
            writer.buf.extend_from_slice(&remaining[..n]);
            downloaded += n as u64;
            ctx.received.store(downloaded, Ordering::Relaxed);
            remaining = &remaining[n..];
            if writer.buf.len() == WRITE_BUFFER_BYTES {
                writer.flush(ctx, seg_start, downloaded, progress).await?;
            }
        }
        if end != OPEN_END && downloaded > end - seg_start {
            break 'pump Ok(());
        }
    };
    // Flush accepted-but-unwritten data on every exit path (including cancel
    // and stream errors) so the recorded progress never runs ahead of disk.
    writer.flush(ctx, seg_start, downloaded, progress).await?;
    while writer.pending.is_some() {
        writer.complete_pending(ctx, seg_start, progress).await?;
    }
    ctx.received
        .store(progress.load(Ordering::Relaxed), Ordering::Relaxed);
    outcome?;
    Ok(())
}

/// Bounded network/disk pipeline: one receiving buffer, one write and at
/// most six queued 256 KiB batches (2 MiB per worker, at most 128 MiB for
/// 64 workers). A slow flush no longer stops network reads after one batch.
/// All accepted buffers are drained before pause/restart is acknowledged.
struct DiskWriter {
    buf: Vec<u8>,
    pending: Option<(tokio::task::JoinHandle<Result<Vec<u8>>>, u64)>,
    queue: std::collections::VecDeque<(Vec<u8>, u64)>,
    spare: Vec<Vec<u8>>,
}
const WRITE_QUEUE_BATCHES: usize = 6;
impl DiskWriter {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(WRITE_BUFFER_BYTES),
            pending: None,
            queue: Default::default(),
            spare: Vec::new(),
        }
    }
    fn start(&mut self, ctx: &WorkerCtx, start: u64, data: Vec<u8>, end: u64) {
        let offset = start + end - data.len() as u64;
        self.pending = Some((ctx.part.queue_write(ctx.file.clone(), offset, data), end));
    }
    async fn complete_pending(
        &mut self,
        ctx: &WorkerCtx,
        start: u64,
        progress: &AtomicU64,
    ) -> Result<()> {
        // Await by reference: losing a select race must not detach the write.
        if let Some((task, end)) = self.pending.as_mut() {
            let mut buffer = task.await??;
            progress.store(*end, Ordering::Relaxed);
            buffer.clear();
            self.spare.push(buffer);
        }
        self.pending = None;
        if let Some((data, end)) = self.queue.pop_front() {
            self.start(ctx, start, data, end);
        }
        Ok(())
    }
    async fn flush(
        &mut self,
        ctx: &WorkerCtx,
        start: u64,
        received: u64,
        progress: &AtomicU64,
    ) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        if self.queue.len() >= WRITE_QUEUE_BATCHES {
            self.complete_pending(ctx, start, progress).await?;
        }
        let spare = self
            .spare
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(WRITE_BUFFER_BYTES));
        let data = std::mem::replace(&mut self.buf, spare);
        if self.pending.is_none() {
            self.start(ctx, start, data, received);
        } else {
            self.queue.push_back((data, received));
        }
        Ok(())
    }
}

async fn sleep_backoff(ctx: &WorkerCtx, attempt: u32, err: &Error, index: u32) -> Result<()> {
    let delay = match err.retry_after_secs() {
        // `Retry-After: 0` must not busy-loop retries; keep a small floor so
        // the adaptive controller has time to react to the throttle before
        // the attempt budget burns up.
        Some(secs) => {
            std::time::Duration::from_secs(secs.min(300)).max(std::time::Duration::from_millis(100))
        }
        None => backoff_delay(attempt, ctx.retry.base_delay_ms, ctx.retry.max_delay_ms),
    };
    let _ = ctx.status_tx.send(WorkerMsg::Backoff {
        index,
        attempt,
        delay_ms: delay.as_millis() as u64,
        error: err.to_string(),
        kind: err.kind(),
    });
    tokio::select! {
        _ = ctx.stop.cancelled() => Err(Error::Cancelled),
        // A released worker must not stay parked for up to 300s on a
        // Retry-After; exit promptly so the slot frees up.
        _ = ctx.release.cancelled() => Err(Error::Released),
        _ = tokio::time::sleep(delay) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn slow_disk_queue_is_bounded_ordered_and_drained() {
        let dir = tempfile::tempdir().unwrap();
        let part = PartFile::create(&dir.path().join("queue.part"), None).unwrap();
        let file = part.worker_handle().unwrap();
        let (status_tx, _) = mpsc::unbounded_channel();
        let ctx = WorkerCtx {
            client: reqwest::Client::new(),
            url: "http://localhost/unused".into(),
            expected_size: None,
            etag: None,
            last_modified: None,
            part,
            file: file.clone(),
            received: Arc::new(AtomicU64::new(0)),
            global_limiter: Arc::new(RateLimiter::new(0)),
            task_limiter: Arc::new(RateLimiter::new(0)),
            stop: CancellationToken::new(),
            release: CancellationToken::new(),
            retry: RetryConfig::default(),
            status_tx,
        };
        let mut writer = DiskWriter::new();
        // Hold the first real write to make the queue fill under backpressure.
        writer.pending = Some((
            tokio::task::spawn_blocking(move || {
                use std::io::{Seek, SeekFrom, Write};
                std::thread::sleep(std::time::Duration::from_millis(80));
                let data = vec![42; WRITE_BUFFER_BYTES];
                let mut handle = &*file;
                handle.seek(SeekFrom::Start(0))?;
                handle.write_all(&data)?;
                Ok(data)
            }),
            WRITE_BUFFER_BYTES as u64,
        ));
        let progress = AtomicU64::new(0);
        for i in 0..10u8 {
            writer
                .buf
                .extend(std::iter::repeat_n(i, WRITE_BUFFER_BYTES));
            writer
                .flush(
                    &ctx,
                    0,
                    (i as u64 + 2) * WRITE_BUFFER_BYTES as u64,
                    &progress,
                )
                .await
                .unwrap();
            assert!(writer.queue.len() <= WRITE_QUEUE_BATCHES);
            assert!(
                writer.queue.len() + writer.spare.len() + usize::from(writer.pending.is_some()) < 8
            );
        }
        while writer.pending.is_some() {
            writer.complete_pending(&ctx, 0, &progress).await.unwrap();
        }
        assert_eq!(
            progress.load(Ordering::Relaxed),
            11 * WRITE_BUFFER_BYTES as u64
        );
        let data = std::fs::read(ctx.part.path()).unwrap();
        assert_eq!(data.len(), 11 * WRITE_BUFFER_BYTES);
        assert!(data[..WRITE_BUFFER_BYTES].iter().all(|&v| v == 42));
        for i in 0..10 {
            assert!(
                data[(i + 1) * WRITE_BUFFER_BYTES..(i + 2) * WRITE_BUFFER_BYTES]
                    .iter()
                    .all(|&v| v == i as u8)
            );
        }
    }
}
