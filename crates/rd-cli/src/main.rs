//! Persistent download CLI. No unrelated tasks are started implicitly.
use rd_engine::config::HttpVersion;
use rd_engine::{AddTaskRequest, Engine, EngineConfig, Event, TaskState};
use std::{
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};

const USAGE: &str = "Usage:
  rd-cli <URL> [options]           Download, or resume matching unfinished task
  rd-cli list [--db PATH]          List persisted tasks
  rd-cli status ID [--db PATH]     Show one task
  rd-cli resume ID [options]       Resume and monitor a task
  rd-cli pause ID [--db PATH]      Persist a paused task
  rd-cli cancel ID [--db PATH]     Cancel and remove partial data
  rd-cli remove ID [--delete-file] [--db PATH]
Options:
  --config PATH    TOML configuration (default: rd-engine.toml)
  --db PATH        Task database (default from configuration)
  -o DIR           Download directory
  -n N             Fixed concurrency (1..64); omit for adaptive mode
  --http1          Independent HTTP/1.1 connections (default)
  --http2          ALPN negotiation with HTTP/2 enabled
  --direct         Bypass system proxy
  --limit BPS      Per-task bytes/second (0 = unlimited)
  --sha256 HEX     Verify expected SHA-256
  --blake3 HEX     Verify expected BLAKE3
  --new            Create another task even if URL already exists
  -h, --help       Show help
Ctrl+C pauses the monitored task and waits for writes to finish.
Do not open the same database in multiple CLI processes simultaneously.";

#[derive(Debug, Default)]
struct Args {
    action: String,
    value: Option<String>,
    config: Option<PathBuf>,
    db: Option<PathBuf>,
    output: Option<PathBuf>,
    connections: Option<u32>,
    rate: Option<u64>,
    sha256: Option<String>,
    blake3: Option<String>,
    protocol: Option<HttpVersion>,
    direct: bool,
    new_task: bool,
    delete_file: bool,
}
fn parse(input: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut it = input.into_iter();
    let first = it.next().ok_or_else(|| USAGE.to_string())?;
    let mut args = Args::default();
    match first.as_str() {
        "list" => args.action = first,
        "status" | "resume" | "pause" | "cancel" | "remove" => {
            args.action = first;
            let id = it.next().ok_or("missing task ID")?;
            if id.parse::<u64>().ok().filter(|&n| n > 0).is_none() {
                return Err("invalid task ID".into());
            }
            args.value = Some(id);
        }
        "-h" | "--help" => {
            args.action = "help".into();
            return Ok(args);
        }
        _ => {
            if !(first.starts_with("https://") || first.starts_with("http://")) {
                return Err("expected an HTTP/HTTPS URL or command; use --help".into());
            }
            args.action = "download".into();
            args.value = Some(first);
        }
    }
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--http1" => args.protocol = Some(HttpVersion::Http1),
            "--http2" => args.protocol = Some(HttpVersion::Auto),
            "--direct" => args.direct = true,
            "--new" => args.new_task = true,
            "--delete-file" => args.delete_file = true,
            "--help" | "-h" => {
                args.action = "help".into();
                return Ok(args);
            }
            "--config" | "--db" | "-o" | "--output-dir" | "-n" | "--connections" | "--limit"
            | "--sha256" | "--blake3" => {
                let value = it
                    .next()
                    .ok_or_else(|| format!("missing value for {flag}"))?;
                match flag.as_str() {
                    "--config" => args.config = Some(value.into()),
                    "--db" => args.db = Some(value.into()),
                    "-o" | "--output-dir" => args.output = Some(value.into()),
                    "-n" | "--connections" => {
                        let n: u32 = value
                            .parse()
                            .map_err(|_| "connections must be an integer")?;
                        if !(1..=64).contains(&n) {
                            return Err("connections must be 1..64".into());
                        }
                        args.connections = Some(n);
                    }
                    "--limit" => {
                        args.rate = Some(
                            value
                                .parse()
                                .map_err(|_| "limit must be a nonnegative integer")?,
                        )
                    }
                    _ => {
                        if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
                            return Err(format!("{flag} requires 64 hexadecimal characters"));
                        }
                        if flag == "--sha256" {
                            args.sha256 = Some(value.to_ascii_lowercase());
                        } else {
                            args.blake3 = Some(value.to_ascii_lowercase());
                        }
                    }
                }
            }
            _ => return Err(format!("unknown option: {flag}")),
        }
    }
    Ok(args)
}
fn human(n: u64) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let (mut value, mut unit) = (n as f64, 0);
    while value >= 1024.0 && unit < 4 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.2} {}", units[unit])
}
fn show(task: &rd_engine::TaskInfo) {
    println!(
        "#{} {}  {} / {}  connections={}  {}",
        task.id,
        task.state,
        human(task.downloaded),
        task.total_size
            .map(human)
            .unwrap_or_else(|| "unknown".into()),
        task.connections,
        task.file_path.display()
    );
    if let Some(error) = &task.error {
        println!("  error: {error}");
    }
}
fn matching(task: &rd_engine::TaskInfo, args: &Args) -> bool {
    args.value.as_deref() == Some(task.url.as_str())
        && task.state != TaskState::Cancelled
        && args
            .sha256
            .as_ref()
            .is_none_or(|v| task.expected_sha256.as_ref() == Some(v))
        && args
            .blake3
            .as_ref()
            .is_none_or(|v| task.expected_blake3.as_ref() == Some(v))
        && args.output.as_ref().is_none_or(|dir| {
            let absolute = |p: &std::path::Path| {
                if p.is_absolute() {
                    p.to_path_buf()
                } else {
                    std::env::current_dir().unwrap_or_default().join(p)
                }
            };
            task.file_path
                .parent()
                .is_some_and(|parent| absolute(parent) == absolute(dir))
        })
        && args
            .connections
            .is_none_or(|n| !task.auto_concurrency && task.connections == n)
}
async fn monitor(engine: &Engine, id: u64) -> Result<(), Box<dyn std::error::Error>> {
    let mut events = engine.subscribe();
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let (started, mut previous_time, mut previous_bytes) = (
        Instant::now(),
        Instant::now(),
        engine.get_task(id).await?.downloaded,
    );
    let mut transferred = 0u64;
    let mut samples = std::collections::VecDeque::new();
    let mut retries = 0u64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                engine.pause_task(id).await?;
                println!("\npaused #{id}; use: rd-cli resume {id} --db \"{}\"", engine.config().db_path.display());
                return Ok(());
            }
            _ = tick.tick() => {}
        }
        while let Ok(event) = events.try_recv() {
            if let Event::TaskRetrying {
                id: task_id, error, ..
            } = event
            {
                if task_id == id {
                    retries += 1;
                    tracing::warn!(task=id, %error, "retry");
                }
            }
        }
        let task = engine.get_task(id).await?;
        let now = Instant::now();
        let elapsed = now.duration_since(previous_time).as_secs_f64();
        let delta = task.downloaded.saturating_sub(previous_bytes);
        transferred = transferred.saturating_add(delta);
        samples.push_back((delta, elapsed));
        if samples.len() > 6 {
            samples.pop_front();
        }
        previous_bytes = task.downloaded;
        previous_time = now;
        let (bytes, seconds) = samples
            .iter()
            .fold((0u64, 0.0), |(b, t), (n, s)| (b + n, t + s));
        let speed = bytes as f64 / seconds.max(0.001);
        let average = transferred as f64 / started.elapsed().as_secs_f64().max(0.001);
        print!(
            "\r{} / {}  {:.2} MB/s  avg {:.2} MB/s  connections={} retries={} {:?}      ",
            human(task.downloaded),
            task.total_size
                .map(human)
                .unwrap_or_else(|| "unknown".into()),
            speed / 1e6,
            average / 1e6,
            task.connections,
            retries,
            task.state
        );
        std::io::stdout().flush()?;
        match task.state {
            TaskState::Completed => {
                println!("\ncompleted: {}\nelapsed: {:.2}s; transferred this run: {}; average: {:.2} MB/s", task.file_path.display(), started.elapsed().as_secs_f64(), human(transferred), average/1e6);
                if let Some(hash) = task.actual_sha256 {
                    println!("sha256: {hash}");
                }
                if let Some(hash) = task.actual_blake3 {
                    println!("blake3: {hash}");
                }
                return Ok(());
            }
            TaskState::Failed => {
                return Err(task
                    .error
                    .unwrap_or_else(|| "download failed".into())
                    .into())
            }
            TaskState::Cancelled | TaskState::Paused => {
                println!();
                return Ok(());
            }
            _ => {}
        }
    }
}
async fn execute(engine: &Engine, args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if args.action == "list" {
        for task in engine.list_tasks().await? {
            show(&task);
        }
        return Ok(());
    }
    if args.action != "download" {
        let id: u64 = args.value.as_ref().ok_or("missing task ID")?.parse()?;
        match args.action.as_str() {
            "status" => show(&engine.get_task(id).await?),
            "pause" => {
                engine.pause_task(id).await?;
                show(&engine.get_task(id).await?);
            }
            "cancel" => {
                engine.cancel_task(id).await?;
                show(&engine.get_task(id).await?);
            }
            "remove" => engine.remove_task(id, args.delete_file).await?,
            "resume" => {
                if let Some(rate) = args.rate {
                    engine.set_task_rate_limit(id, Some(rate)).await?;
                }
                engine.resume_task(id).await?;
                monitor(engine, id).await?;
            }
            _ => return Err("unknown command".into()),
        }
        return Ok(());
    }
    if !args.new_task {
        let existing = engine
            .list_tasks()
            .await?
            .into_iter()
            .rev()
            .find(|task| matching(task, args));
        if let Some(task) = existing {
            if task.state == TaskState::Completed {
                show(&task);
                println!("already completed; use --new to download another copy");
                return Ok(());
            }
            if let Some(rate) = args.rate {
                engine.set_task_rate_limit(task.id, Some(rate)).await?;
            }
            println!("resuming task #{}", task.id);
            engine.resume_task(task.id).await?;
            return monitor(engine, task.id).await;
        }
    }
    let mut req = AddTaskRequest::new(args.value.as_ref().ok_or("missing URL")?);
    req.output_dir = args.output.clone();
    req.connections = args.connections;
    req.rate_limit_bps = args.rate;
    req.expected_sha256 = args.sha256.clone();
    req.expected_blake3 = args.blake3.clone();
    let id = engine.add_task(req).await?;
    println!("task #{id} created; Ctrl+C pauses");
    monitor(engine, id).await
}
#[tokio::main]
async fn main() {
    let args = match parse(std::env::args().skip(1)) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    if args.action == "help" {
        println!("{USAGE}");
        return;
    }
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();
    let config_path = args
        .config
        .clone()
        .unwrap_or_else(|| "rd-engine.toml".into());
    let mut config = match EngineConfig::load_or_default(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{}: {e}", config_path.display());
            std::process::exit(2);
        }
    };
    config.resume_on_startup = false;
    if let Some(db) = &args.db {
        config.db_path = db.clone();
    }
    if let Some(protocol) = args.protocol {
        config.network.http_version = protocol;
    }
    if args.direct {
        config.network.use_system_proxy = false;
    }
    let engine = match Engine::open(config).await {
        Ok(e) => e,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    let result = execute(&engine, &args).await;
    let shutdown = engine.shutdown().await;
    if let Err(e) = result {
        eprintln!("\nfailed: {e}");
        std::process::exit(1);
    }
    if let Err(e) = shutdown {
        eprintln!("shutdown failed: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn args(s: &str) -> Result<Args, String> {
        parse(s.split_whitespace().map(str::to_owned))
    }
    #[test]
    fn invalid_values_rejected() {
        for text in [
            "https://example.org -n abc",
            "https://example.org -n 0",
            "https://example.org -n 65",
            "https://example.org --limit nope",
            "https://example.org --sha256 bad",
            "resume nope",
            "list --db",
        ] {
            assert!(args(text).is_err(), "{text}");
        }
    }
    #[test]
    fn controls_and_download_parse() {
        let a = args("https://example.org -n 32 --http1 --db isolated.db").unwrap();
        assert_eq!(a.connections, Some(32));
        assert_eq!(a.protocol, Some(HttpVersion::Http1));
        assert_eq!(args("resume 12").unwrap().value.as_deref(), Some("12"));
        assert_eq!(args("list").unwrap().action, "list");
    }
}
