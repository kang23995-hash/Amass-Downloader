use std::sync::{Arc, Mutex};

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::config::EngineConfig;
use crate::db::{self, Db};
use crate::error::{Error, Result};
use crate::http;
use crate::limiter::RateLimiter;
use crate::model::{AddTaskRequest, Event, TaskId, TaskInfo, TaskState};
use crate::scheduler::{self, Command};
use crate::storage;

/// The download engine handle. Cheap to clone; all clones share one engine.
///
/// This is the only public entry point: UIs add and control tasks here and
/// subscribe to events. Workers, segment files and the database are private.
#[derive(Clone)]
pub struct Engine {
    cmd: mpsc::Sender<Command>,
    events: broadcast::Sender<Event>,
    global_limiter: Arc<RateLimiter>,
    config: Arc<EngineConfig>,
    scheduler: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Engine {
    /// Open (or create) the engine database and reconcile persisted tasks.
    ///
    /// Tasks interrupted by a crash are recovered as `Paused`; completed
    /// tasks whose final file disappeared are reopened for resume.
    pub async fn open(config: EngineConfig) -> Result<Self> {
        config.validate()?;
        std::fs::create_dir_all(&config.download_dir)?;
        let db = Db::open(&config.db_path)?;
        let mut initial = db.call(db::load_all_tasks).await?;
        let name_locked = db.call(db::load_name_locked).await?;
        let mut resume_ids = Vec::new();
        for task in &mut initial {
            match task.state {
                TaskState::Pending
                | TaskState::Probing
                | TaskState::Downloading
                | TaskState::Retrying => {
                    task.state = TaskState::Paused;
                    let id = task.id;
                    db.call(move |c| db::update_state(c, id, TaskState::Paused, None))
                        .await?;
                }
                TaskState::Completed => {
                    let final_len = storage::file_len(&task.file_path).await?;
                    let intact = match (final_len, task.total_size) {
                        (Some(len), Some(total)) => len == total,
                        (Some(_), None) => true,
                        (None, _) => false,
                    };
                    if !intact {
                        tracing::warn!(
                            task = task.id,
                            "completed file missing or truncated; reopening"
                        );
                        task.state = TaskState::Paused;
                        let id = task.id;
                        db.call(move |c| db::update_state(c, id, TaskState::Paused, None))
                            .await?;
                        if storage::file_len(&task.part_path).await?.is_none() {
                            task.downloaded = 0;
                            db.call(move |c| {
                                db::reset_segments(c, id)?;
                                db::update_downloaded(c, id, 0)
                            })
                            .await?;
                        }
                    }
                }
                _ => {}
            }
            // Reconcile legacy split maps and their aggregate counters before
            // exposing a recovered task to callers (including CLI sampling).
            if !matches!(task.state, TaskState::Completed | TaskState::Cancelled)
                && task.range_supported
            {
                if let Some(size) = task.total_size {
                    let id = task.id;
                    let mut rows = db.call(move |c| db::load_segments(c, id)).await?;
                    let signature = |rows: &[crate::model::Segment]| {
                        rows.iter()
                            .map(|s| (s.index, (s.start, s.end, s.downloaded)))
                            .collect::<std::collections::BTreeMap<_, _>>()
                    };
                    let before = signature(&rows);
                    crate::runner::reconcile_segments(&mut rows, size);
                    let total = rows.iter().map(|s| s.downloaded).sum();
                    if signature(&rows) != before || task.downloaded != total {
                        db.call(move |c| {
                            db::replace_segments(c, id, &rows)?;
                            db::update_downloaded(c, id, total)
                        })
                        .await?;
                    }
                    task.downloaded = total;
                }
            }
            if config.resume_on_startup && task.state == TaskState::Paused {
                resume_ids.push(task.id);
            }
        }
        let client = http::build_client(&config)?;
        let global_limiter = Arc::new(RateLimiter::new(config.rate_limit.global_bps));
        let (events, _) = broadcast::channel(1024);
        let config = Arc::new(config);
        let (cmd, join) = scheduler::spawn(scheduler::SchedulerInit {
            db,
            config: config.clone(),
            client,
            global_limiter: global_limiter.clone(),
            events: events.clone(),
            initial,
            name_locked,
            resume_ids,
        });
        Ok(Self {
            cmd,
            events,
            global_limiter,
            config,
            scheduler: Arc::new(Mutex::new(Some(join))),
        })
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Subscribe to engine events. Slow receivers may lag and miss events;
    /// state can always be re-read with `get_task` / `list_tasks`.
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    pub async fn add_task(&self, req: AddTaskRequest) -> Result<TaskId> {
        self.request(|reply| Command::Add { req, reply }).await
    }

    pub async fn pause_task(&self, id: TaskId) -> Result<()> {
        self.request(|reply| Command::Pause { id, reply }).await
    }

    pub async fn resume_task(&self, id: TaskId) -> Result<()> {
        self.request(|reply| Command::Resume { id, reply }).await
    }

    pub async fn cancel_task(&self, id: TaskId) -> Result<()> {
        self.request(|reply| Command::Cancel { id, reply }).await
    }

    /// Remove a task record. Active tasks must be cancelled first.
    /// `delete_file` also removes the completed file from disk.
    pub async fn remove_task(&self, id: TaskId, delete_file: bool) -> Result<()> {
        self.request(|reply| Command::Remove {
            id,
            delete_file,
            reply,
        })
        .await
    }

    pub async fn get_task(&self, id: TaskId) -> Result<TaskInfo> {
        self.request(|reply| Command::Get { id, reply }).await
    }

    pub async fn list_tasks(&self) -> Result<Vec<TaskInfo>> {
        self.request(|reply| Command::List { reply }).await
    }

    /// Set or clear (None / Some(0)) the per-task speed cap at runtime.
    pub async fn set_task_rate_limit(&self, id: TaskId, bytes_per_sec: Option<u64>) -> Result<()> {
        self.request(|reply| Command::SetTaskRate {
            id,
            rate: bytes_per_sec,
            reply,
        })
        .await
    }

    /// Set or clear the global speed cap shared by all tasks at runtime.
    pub fn set_global_rate_limit(&self, bytes_per_sec: Option<u64>) {
        self.global_limiter.set_rate(bytes_per_sec.unwrap_or(0));
    }

    pub fn global_rate_limit(&self) -> u64 {
        self.global_limiter.rate()
    }

    /// Stop all active tasks (persisted as paused), flush state and stop the
    /// scheduler. Dropping the engine without calling this also triggers a
    /// graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        self.request(|reply| Command::Shutdown { reply }).await?;
        let handle = self.scheduler.lock().map_err(Error::from)?.take();
        if let Some(handle) = handle {
            handle.await?;
        }
        Ok(())
    }

    async fn request<R>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<R>>) -> Command,
    ) -> Result<R> {
        let (tx, rx) = oneshot::channel();
        self.cmd.send(make(tx)).await.map_err(|_| Error::Shutdown)?;
        rx.await.map_err(|_| Error::Shutdown)?
    }
}
