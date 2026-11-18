//! # RD Download Engine
//!
//! A UI-independent, async download engine built on Tokio and Reqwest.
//!
//! The engine provides multi-connection HTTP/HTTPS downloads, pause/resume,
//! crash-safe persistence in SQLite, dynamic rate limiting and an event
//! stream for UIs. All interaction goes through [`Engine`]; workers, files
//! and database internals are private.
//!
//! ```no_run
//! use rd_engine::{AddTaskRequest, Engine, EngineConfig};
//!
//! # async fn example() -> rd_engine::Result<()> {
//! let engine = Engine::open(EngineConfig::default()).await?;
//! let id = engine.add_task(AddTaskRequest::new("https://example.com/file.bin")).await?;
//! let mut events = engine.subscribe();
//! // ... observe events, pause/resume/cancel via the engine API ...
//! engine.shutdown().await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod error;
pub mod model;

mod db;
mod engine;
mod http;
mod limiter;
mod runner;
mod scheduler;
mod storage;
mod util;
mod worker;

pub use config::EngineConfig;
pub use engine::Engine;
pub use error::{Error, Result};
pub use model::{AddTaskRequest, Event, Segment, TaskId, TaskInfo, TaskState};
