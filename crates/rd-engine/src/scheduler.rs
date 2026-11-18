use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::config::EngineConfig;
use crate::db::{self, Db, MetaUpdate};
use crate::error::{Error, Result};
use crate::http;
use crate::limiter::RateLimiter;
use crate::model::{AddTaskRequest, Event, TaskId, TaskInfo, TaskState};
use crate::runner::{self, RunOutcome, RunSpec, RunnerMsg, StopReason};
use crate::storage;
use crate::util::{now_unix, sanitize_file_name, unique_path};

pub enum Command {
    Add {
        req: AddTaskRequest,
        reply: oneshot::Sender<Result<TaskId>>,
    },
    Pause {
        id: TaskId,
        reply: oneshot::Sender<Result<()>>,
    },
    Resume {
        id: TaskId,
        reply: oneshot::Sender<Result<()>>,
    },
    Cancel {
        id: TaskId,
        reply: oneshot::Sender<Result<()>>,
    },
    Remove {
        id: TaskId,
        delete_file: bool,
        reply: oneshot::Sender<Result<()>>,
    },
    Get {
        id: TaskId,
        reply: oneshot::Sender<Result<TaskInfo>>,
    },
    List {
        reply: oneshot::Sender<Result<Vec<TaskInfo>>>,
    },
    SetTaskRate {
        id: TaskId,
        rate: Option<u64>,
        reply: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<()>>,
    },
}

struct RunHandle {
    stop: CancellationToken,
    reason: Arc<AtomicU8>,
    _join: JoinHandle<()>,
}

struct Entry {
    info: TaskInfo,
    name_locked: bool,
    limiter: Arc<RateLimiter>,
    run: Option<RunHandle>,
    pending: Vec<oneshot::Sender<Result<()>>>,
    last_bytes: u64,
    last_tick: Instant,
}

/// Everything the scheduler needs at startup; grouped so `spawn` stays
/// readable as its parameter list grows.
pub struct SchedulerInit {
    pub db: Db,
    pub config: Arc<EngineConfig>,
    pub client: reqwest::Client,
    pub global_limiter: Arc<RateLimiter>,
    pub events: broadcast::Sender<Event>,
    pub initial: Vec<TaskInfo>,
    pub name_locked: HashMap<TaskId, bool>,
    pub resume_ids: Vec<TaskId>,
}

pub fn spawn(init: SchedulerInit) -> (mpsc::Sender<Command>, JoinHandle<()>) {
    let SchedulerInit {
        db,
        config,
        client,
        global_limiter,
        events,
        initial,
        name_locked,
        resume_ids,
    } = init;
    let (cmd_tx, cmd_rx) = mpsc::channel(256);
    let (runner_tx, runner_rx) = mpsc::unbounded_channel();
    let entries = initial
        .into_iter()
        .map(|info| {
            let id = info.id;
            let limiter = Arc::new(RateLimiter::new(info.rate_limit_bps.unwrap_or(0)));
            let entry = Entry {
                info,
                name_locked: name_locked.get(&id).copied().unwrap_or(false),
                limiter,
                run: None,
                pending: Vec::new(),
                last_bytes: 0,
                last_tick: Instant::now(),
            };
            (id, entry)
        })
        .collect();
    let mut scheduler = Scheduler {
        db,
        config,
        client,
        global_limiter,
        events,
        entries,
        queue: resume_ids.into_iter().collect(),
        cmd_rx,
        runner_rx,
        runner_tx,
        shutting_down: false,
        shutdown_reply: None,
    };
    let handle = tokio::spawn(async move { scheduler.run().await });
    (cmd_tx, handle)
}

struct Scheduler {
    db: Db,
    config: Arc<EngineConfig>,
    client: reqwest::Client,
    global_limiter: Arc<RateLimiter>,
    events: broadcast::Sender<Event>,
    entries: HashMap<TaskId, Entry>,
    queue: VecDeque<TaskId>,
    cmd_rx: mpsc::Receiver<Command>,
    runner_rx: mpsc::UnboundedReceiver<RunnerMsg>,
    runner_tx: mpsc::UnboundedSender<RunnerMsg>,
    shutting_down: bool,
    shutdown_reply: Option<oneshot::Sender<Result<()>>>,
}

impl Scheduler {
    async fn run(&mut self) {
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => match cmd {
                    Some(c) => self.handle_command(c).await,
                    None => self.begin_shutdown(None),
                },
                msg = self.runner_rx.recv() => {
                    if let Some(m) = msg {
                        self.handle_runner_msg(m).await;
                    }
                }
                _ = tick.tick() => self.on_tick().await,
            }
            self.pump_queue().await;
            if self.shutting_down && self.active_count() == 0 {
                if let Some(reply) = self.shutdown_reply.take() {
                    let _ = reply.send(Ok(()));
                }
                break;
            }
        }
    }

    fn emit(&self, event: Event) {
        let _ = self.events.send(event);
    }

    fn active_count(&self) -> usize {
        self.entries.values().filter(|e| e.run.is_some()).count()
    }

    fn begin_shutdown(&mut self, reply: Option<oneshot::Sender<Result<()>>>) {
        self.shutting_down = true;
        self.shutdown_reply = reply;
        self.queue.clear();
        for entry in self.entries.values_mut() {
            if let Some(run) = &entry.run {
                run.reason
                    .store(StopReason::Shutdown as u8, Ordering::Relaxed);
                run.stop.cancel();
            }
        }
    }

    async fn persist_state(&self, id: TaskId, state: TaskState, error: Option<&str>) {
        let db = self.db.clone();
        let error = error.map(|s| s.to_string());
        if let Err(e) = db
            .call(move |c| db::update_state(c, id, state, error.as_deref()))
            .await
        {
            tracing::error!(task = id, "failed to persist state: {e}");
        }
    }

    async fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::Add { req, reply } => {
                let result = self.add_task(req).await;
                let _ = reply.send(result);
            }
            Command::Pause { id, reply } => {
                self.pause_task(id, reply).await;
            }
            Command::Resume { id, reply } => {
                let result = self.resume_task(id).await;
                let _ = reply.send(result);
            }
            Command::Cancel { id, reply } => {
                self.cancel_task(id, reply).await;
            }
            Command::Remove {
                id,
                delete_file,
                reply,
            } => {
                let result = self.remove_task(id, delete_file).await;
                let _ = reply.send(result);
            }
            Command::Get { id, reply } => {
                let result = self
                    .entries
                    .get(&id)
                    .map(|e| e.info.clone())
                    .ok_or(Error::TaskNotFound(id));
                let _ = reply.send(result);
            }
            Command::List { reply } => {
                let mut list: Vec<TaskInfo> =
                    self.entries.values().map(|e| e.info.clone()).collect();
                list.sort_by_key(|t| t.id);
                let _ = reply.send(Ok(list));
            }
            Command::SetTaskRate { id, rate, reply } => {
                let result = self.set_task_rate(id, rate).await;
                let _ = reply.send(result);
            }
            Command::Shutdown { reply } => self.begin_shutdown(Some(reply)),
        }
    }

    async fn add_task(&mut self, req: AddTaskRequest) -> Result<TaskId> {
        if self.shutting_down {
            return Err(Error::Shutdown);
        }
        let parsed = Url::parse(&req.url).map_err(|e| Error::Url(e.to_string()))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(Error::Url(format!(
                "unsupported scheme: {}",
                parsed.scheme()
            )));
        }
        let dir = req
            .output_dir
            .clone()
            .unwrap_or_else(|| self.config.download_dir.clone());
        let name = req
            .file_name
            .as_deref()
            .map(sanitize_file_name)
            .or_else(|| http::filename_from_url(&req.url))
            .unwrap_or_else(|| "download".into());
        let file_path = unique_path(&dir, &name);
        let connections = req
            .connections
            .unwrap_or(self.config.default_connections)
            .clamp(1, self.config.max_connections_per_task);
        // Auto concurrency only applies when the user did not pin a count and
        // the engine-wide switch is on.
        let auto_concurrency = req.connections.is_none() && self.config.adaptive.enabled;
        let now = now_unix();
        let mut info = TaskInfo {
            id: 0,
            url: req.url.clone(),
            final_url: None,
            file_name: name,
            part_path: file_path.clone(),
            file_path,
            state: TaskState::Pending,
            total_size: None,
            downloaded: 0,
            connections,
            range_supported: false,
            etag: None,
            last_modified: None,
            expected_sha256: req.expected_sha256.clone(),
            expected_blake3: req.expected_blake3.clone(),
            actual_sha256: None,
            actual_blake3: None,
            rate_limit_bps: req.rate_limit_bps,
            error: None,
            created_at: now,
            updated_at: now,
            auto_concurrency,
            speed_bps: 0,
        };
        let name_locked = req.file_name.is_some();
        let db = self.db.clone();
        let insert_info = info.clone();
        let id = db
            .call(move |c| db::insert_task(c, &insert_info, name_locked))
            .await?;
        info.id = id;
        info.part_path = runner::part_path_for(&info.file_path, id);
        let db = self.db.clone();
        let part_path = info.part_path.clone();
        let file_path = info.file_path.clone();
        let file_name = info.file_name.clone();
        db.call(move |c| {
            db::update_meta(
                c,
                id,
                &MetaUpdate {
                    final_url: None,
                    file_name,
                    file_path,
                    part_path,
                    total_size: None,
                    etag: None,
                    last_modified: None,
                    range_supported: false,
                    connections,
                    actual_sha256: None,
                    actual_blake3: None,
                },
            )
        })
        .await?;
        self.emit(Event::TaskCreated {
            id,
            url: info.url.clone(),
            file_name: info.file_name.clone(),
        });
        let limiter = Arc::new(RateLimiter::new(req.rate_limit_bps.unwrap_or(0)));
        self.entries.insert(
            id,
            Entry {
                info,
                name_locked,
                limiter,
                run: None,
                pending: Vec::new(),
                last_bytes: 0,
                last_tick: Instant::now(),
            },
        );
        if req.auto_start {
            self.queue.push_back(id);
        }
        Ok(id)
    }

    /// Defers the reply until the runner acknowledges the stop when a run is
    /// active; otherwise replies immediately.
    async fn pause_task(&mut self, id: TaskId, reply: oneshot::Sender<Result<()>>) {
        let Some(entry) = self.entries.get_mut(&id) else {
            let _ = reply.send(Err(Error::TaskNotFound(id)));
            return;
        };
        if let Some(run) = &entry.run {
            run.reason
                .store(StopReason::Paused as u8, Ordering::Relaxed);
            run.stop.cancel();
            entry.pending.push(reply);
            return;
        }
        let state = entry.info.state;
        match state {
            TaskState::Pending => {
                entry.info.state = TaskState::Paused;
                self.queue.retain(|&q| q != id);
                self.persist_state(id, TaskState::Paused, None).await;
                self.emit(Event::TaskPaused { id });
                let _ = reply.send(Ok(()));
            }
            TaskState::Paused => {
                let _ = reply.send(Ok(()));
            }
            state => {
                let _ = reply.send(Err(Error::InvalidState {
                    id,
                    state,
                    action: "pause",
                }));
            }
        }
    }

    async fn resume_task(&mut self, id: TaskId) -> Result<()> {
        if self.shutting_down {
            return Err(Error::Shutdown);
        }
        let entry = self.entries.get_mut(&id).ok_or(Error::TaskNotFound(id))?;
        match entry.info.state {
            TaskState::Paused | TaskState::Failed | TaskState::Cancelled => {
                entry.info.state = TaskState::Pending;
                entry.info.error = None;
                self.persist_state(id, TaskState::Pending, None).await;
                self.queue.push_back(id);
                self.emit(Event::TaskResumed { id });
                Ok(())
            }
            // A task added with auto_start=false is Pending but not queued;
            // resuming it must enqueue it. An already-queued Pending task is a
            // no-op.
            TaskState::Pending => {
                if !self.queue.contains(&id) {
                    self.queue.push_back(id);
                    self.emit(Event::TaskResumed { id });
                }
                Ok(())
            }
            TaskState::Probing | TaskState::Downloading | TaskState::Retrying => Ok(()),
            state => Err(Error::InvalidState {
                id,
                state,
                action: "resume",
            }),
        }
    }

    async fn cancel_task(&mut self, id: TaskId, reply: oneshot::Sender<Result<()>>) {
        let Some(entry) = self.entries.get_mut(&id) else {
            let _ = reply.send(Err(Error::TaskNotFound(id)));
            return;
        };
        if let Some(run) = &entry.run {
            run.reason
                .store(StopReason::Cancelled as u8, Ordering::Relaxed);
            run.stop.cancel();
            entry.pending.push(reply);
            return;
        }
        let state = entry.info.state;
        if state == TaskState::Completed {
            let _ = reply.send(Err(Error::InvalidState {
                id,
                state,
                action: "cancel",
            }));
            return;
        }
        if state == TaskState::Cancelled {
            let _ = reply.send(Ok(()));
            return;
        }
        let part_path = entry.info.part_path.clone();
        entry.info.state = TaskState::Cancelled;
        self.queue.retain(|&q| q != id);
        self.persist_state(id, TaskState::Cancelled, None).await;
        let _ = storage::remove_file_if_exists(&part_path).await;
        self.emit(Event::TaskCancelled { id });
        let _ = reply.send(Ok(()));
    }

    async fn remove_task(&mut self, id: TaskId, delete_file: bool) -> Result<()> {
        let entry = self.entries.get(&id).ok_or(Error::TaskNotFound(id))?;
        if entry.run.is_some() || entry.info.state == TaskState::Pending {
            return Err(Error::InvalidState {
                id,
                state: entry.info.state,
                action: "remove (cancel first)",
            });
        }
        let part_path = entry.info.part_path.clone();
        let file_path = entry.info.file_path.clone();
        self.entries.remove(&id);
        self.queue.retain(|&q| q != id);
        let db = self.db.clone();
        db.call(move |c| db::delete_task(c, id)).await?;
        let _ = storage::remove_file_if_exists(&part_path).await;
        if delete_file {
            let _ = storage::remove_file_if_exists(&file_path).await;
        }
        self.emit(Event::TaskRemoved { id });
        Ok(())
    }

    async fn set_task_rate(&mut self, id: TaskId, rate: Option<u64>) -> Result<()> {
        let entry = self.entries.get_mut(&id).ok_or(Error::TaskNotFound(id))?;
        entry.limiter.set_rate(rate.unwrap_or(0));
        entry.info.rate_limit_bps = rate;
        let db = self.db.clone();
        db.call(move |c| db::update_rate_limit(c, id, rate)).await?;
        Ok(())
    }

    async fn handle_runner_msg(&mut self, msg: RunnerMsg) {
        match msg {
            RunnerMsg::State { id, state } => {
                if let Some(entry) = self.entries.get_mut(&id) {
                    entry.info.state = state;
                    self.persist_state(id, state, None).await;
                    self.emit(Event::TaskStateChanged { id, state });
                }
            }
            RunnerMsg::Refresh { id } => {
                let db = self.db.clone();
                if let Ok(Some(mut fresh)) = db.call(move |c| db::load_task(c, id)).await {
                    if let Some(entry) = self.entries.get_mut(&id) {
                        fresh.downloaded = entry.info.downloaded;
                        fresh.speed_bps = entry.info.speed_bps;
                        entry.info = fresh;
                    }
                }
            }
            RunnerMsg::Progress { id, downloaded } => {
                if let Some(entry) = self.entries.get_mut(&id) {
                    entry.info.downloaded = downloaded;
                }
            }
            RunnerMsg::Retrying {
                id,
                attempt,
                delay_ms,
                error,
            } => {
                self.emit(Event::TaskRetrying {
                    id,
                    attempt,
                    delay_ms,
                    error,
                });
            }
            RunnerMsg::ResourceChanged { id } => {
                self.emit(Event::ResourceChanged { id });
            }
            RunnerMsg::Connections { id, count } => {
                if let Some(entry) = self.entries.get_mut(&id) {
                    entry.info.connections = count;
                }
            }
            RunnerMsg::Finished { id, outcome } => {
                self.finish_task(id, outcome).await;
            }
        }
    }

    async fn finish_task(&mut self, id: TaskId, outcome: RunOutcome) {
        let entry = match self.entries.get_mut(&id) {
            Some(e) => e,
            None => return,
        };
        entry.run = None;
        entry.info.speed_bps = 0;
        let pending = std::mem::take(&mut entry.pending);
        match outcome {
            RunOutcome::Completed {
                file_path,
                total_size,
                sha256,
                blake3,
            } => {
                entry.info.state = TaskState::Completed;
                entry.info.downloaded = total_size;
                entry.info.total_size = Some(total_size);
                entry.info.file_path = file_path.clone();
                entry.info.actual_sha256 = sha256.clone();
                entry.info.actual_blake3 = blake3.clone();
                entry.info.error = None;
                let info = entry.info.clone();
                let db = self.db.clone();
                let _ = db
                    .call(move |c| {
                        db::update_state(c, id, TaskState::Completed, None)?;
                        db::update_meta(
                            c,
                            id,
                            &MetaUpdate {
                                final_url: info.final_url,
                                file_name: info.file_name,
                                file_path: info.file_path,
                                part_path: info.part_path,
                                total_size: info.total_size,
                                etag: info.etag,
                                last_modified: info.last_modified,
                                range_supported: info.range_supported,
                                connections: info.connections,
                                actual_sha256: info.actual_sha256,
                                actual_blake3: info.actual_blake3,
                            },
                        )?;
                        db::update_downloaded(c, id, total_size)
                    })
                    .await;
                self.emit(Event::TaskCompleted {
                    id,
                    file_path,
                    sha256,
                    blake3,
                });
                for reply in pending {
                    let _ = reply.send(Err(Error::InvalidState {
                        id,
                        state: TaskState::Completed,
                        action: "stop",
                    }));
                }
            }
            RunOutcome::Failed { error } => {
                entry.info.state = TaskState::Failed;
                entry.info.error = Some(error.clone());
                self.persist_state(id, TaskState::Failed, Some(&error))
                    .await;
                self.emit(Event::TaskFailed { id, error });
                for reply in pending {
                    let _ = reply.send(Err(Error::InvalidState {
                        id,
                        state: TaskState::Failed,
                        action: "stop",
                    }));
                }
            }
            RunOutcome::Stopped { reason } => {
                let state = if reason == StopReason::Cancelled {
                    TaskState::Cancelled
                } else {
                    TaskState::Paused
                };
                entry.info.state = state;
                let downloaded = entry.info.downloaded;
                self.persist_state(id, state, None).await;
                let db = self.db.clone();
                let _ = db
                    .call(move |c| db::update_downloaded(c, id, downloaded))
                    .await;
                if state == TaskState::Cancelled {
                    self.emit(Event::TaskCancelled { id });
                } else {
                    self.emit(Event::TaskPaused { id });
                }
                for reply in pending {
                    let _ = reply.send(Ok(()));
                }
            }
        }
    }

    async fn on_tick(&mut self) {
        let now = Instant::now();
        let mut updates = Vec::new();
        for entry in self.entries.values_mut() {
            if entry.run.is_none() {
                entry.info.speed_bps = 0;
                continue;
            }
            let dt = now.duration_since(entry.last_tick).as_secs_f64().max(0.001);
            let delta = entry.info.downloaded.saturating_sub(entry.last_bytes);
            let instant = (delta as f64 / dt) as u64;
            // Seed the EMA from the first instant sample so the displayed
            // speed converges immediately instead of crawling up from 0.
            entry.info.speed_bps = if entry.info.speed_bps == 0 {
                instant
            } else {
                (instant + entry.info.speed_bps) / 2
            };
            entry.last_bytes = entry.info.downloaded;
            entry.last_tick = now;
            updates.push((entry.info.id, entry.info.downloaded, entry.info.speed_bps));
        }
        for (id, downloaded, speed) in updates {
            self.emit(Event::SpeedUpdated {
                id,
                bytes_per_sec: speed,
            });
            let total = self.entries.get(&id).and_then(|e| e.info.total_size);
            self.emit(Event::ProgressUpdated {
                id,
                downloaded,
                total_size: total,
            });
            let db = self.db.clone();
            let _ = db
                .call(move |c| db::update_downloaded(c, id, downloaded))
                .await;
        }
    }

    async fn pump_queue(&mut self) {
        while !self.shutting_down && self.active_count() < self.config.max_concurrent_tasks {
            let id = match self.queue.pop_front() {
                Some(id) => id,
                None => break,
            };
            let entry = match self.entries.get_mut(&id) {
                Some(e) if e.info.state == TaskState::Pending => e,
                _ => continue,
            };
            let db = self.db.clone();
            let segments = match db.call(move |c| db::load_segments(c, id)).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(task = id, "failed to load segments: {e}");
                    entry.info.state = TaskState::Failed;
                    entry.info.error = Some(e.to_string());
                    self.persist_state(id, TaskState::Failed, Some(&e.to_string()))
                        .await;
                    self.emit(Event::TaskFailed {
                        id,
                        error: e.to_string(),
                    });
                    continue;
                }
            };
            let stop = CancellationToken::new();
            let reason = Arc::new(AtomicU8::new(StopReason::Paused as u8));
            let spec = RunSpec {
                fresh: entry.info.total_size.is_none() && entry.info.downloaded == 0,
                name_locked: entry.name_locked,
                info: entry.info.clone(),
                segments,
                db: self.db.clone(),
                client: self.client.clone(),
                config: self.config.clone(),
                global_limiter: self.global_limiter.clone(),
                task_limiter: entry.limiter.clone(),
                stop: stop.clone(),
                stop_reason: reason.clone(),
                msg_tx: self.runner_tx.clone(),
            };
            let join = tokio::spawn(runner::run(spec));
            entry.run = Some(RunHandle {
                stop,
                reason,
                _join: join,
            });
            entry.last_bytes = entry.info.downloaded;
            entry.last_tick = Instant::now();
            self.emit(Event::TaskStarted { id });
        }
    }
}
