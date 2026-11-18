//! Explicit WAN diagnostic. Uses an isolated DB; never resumes user tasks.
use rd_engine::config::HttpVersion;
use rd_engine::{AddTaskRequest, Engine, EngineConfig, TaskState};
use std::time::{Duration, Instant};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        return Err(
            "usage: netdiag URL auto|http1 CONNECTIONS(0=adaptive) SECONDS [DIRECTORY]".into(),
        );
    }
    let dir = std::path::PathBuf::from(
        args.get(5)
            .map(String::as_str)
            .unwrap_or("diagnostics/transfer"),
    );
    std::fs::create_dir_all(&dir)?;
    let mut cfg = EngineConfig {
        download_dir: dir.clone(),
        db_path: dir.join("tasks.db"),
        ..Default::default()
    };
    cfg.network.http_version = if args[2] == "http1" {
        HttpVersion::Http1
    } else {
        HttpVersion::Auto
    };
    let engine = Engine::open(cfg).await?;
    let mut events = engine.subscribe();
    let mut req = AddTaskRequest::new(&args[1]);
    let n: u32 = args[3].parse()?;
    req.connections = if n == 0 { None } else { Some(n) };
    let id = engine.add_task(req).await?;
    let started = Instant::now();
    let seconds: u64 = args[4].parse()?;
    let (mut previous, mut retries) = (0, 0);
    println!("seconds,bytes,interval_MBps,connections,state,retries");
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        while let Ok(ev) = events.try_recv() {
            if matches!(ev, rd_engine::Event::TaskRetrying { .. }) {
                retries += 1;
            }
        }
        let info = engine.get_task(id).await?;
        println!(
            "{:.3},{},{:.3},{},{:?},{}",
            started.elapsed().as_secs_f64(),
            info.downloaded,
            info.downloaded.saturating_sub(previous) as f64 / 1e6,
            info.connections,
            info.state,
            retries
        );
        previous = info.downloaded;
        if info.state == TaskState::Completed
            || info.state == TaskState::Failed
            || started.elapsed().as_secs() >= seconds
        {
            break;
        }
    }
    let info = engine.get_task(id).await?;
    println!(
        "average_MBps={:.3} error={:?}",
        info.downloaded as f64 / started.elapsed().as_secs_f64() / 1e6,
        info.error
    );
    if info.state != TaskState::Completed {
        engine.cancel_task(id).await?;
    }
    engine.shutdown().await?;
    Ok(())
}
