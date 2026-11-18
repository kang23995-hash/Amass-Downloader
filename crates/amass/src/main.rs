mod app;
mod ui;
use app::{App, Dialog, Form, Result};
use crossterm::{
    event::{
        self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
        Event, KeyEventKind, MouseButton, MouseEventKind,
    },
    execute,
};
use rd_engine::{Engine, EngineConfig};
use std::{
    io::{self, IsTerminal},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

struct TerminalGuard;
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), DisableBracketedPaste, DisableMouseCapture);
        ratatui::restore();
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Amass: {e}");
        std::process::exit(1);
    }
}
async fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut config_override = None;
    let mut direct = false;
    let mut preview = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_override = Some(PathBuf::from(args.next().ok_or("--config 缺少路径")?))
            }
            "--direct" => direct = true,
            "--preview" => {
                preview = Some(PathBuf::from(args.next().ok_or("--preview 缺少输出路径")?))
            }
            "--help" | "-h" => {
                println!("Amass — 终端下载工作台\n\nAmass                 打开交互页面\nAmass --direct        以直连模式启动\nAmass --config PATH   使用指定配置\n\n页面内按 N 新建下载，? 查看快捷键，Q 保存退出。");
                return Ok(());
            }
            _ => return Err(format!("未知参数：{arg}；使用 Amass --help").into()),
        }
    }
    let base = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| ".".into())
                .join(".local/share")
        })
        .join("Amass");
    let config_path = config_override
        .clone()
        .unwrap_or_else(|| base.join("amass.toml"));
    let mut config = if config_path.exists() {
        EngineConfig::from_file(&config_path)?
    } else if config_override.is_some() {
        return Err(format!("配置文件不存在：{}", config_path.display()).into());
    } else {
        let home = std::env::var_os("USERPROFILE")
            .or_else(|| std::env::var_os("HOME"))
            .map(PathBuf::from)
            .unwrap_or_else(|| base.clone());
        EngineConfig {
            db_path: base.join("tasks.db"),
            download_dir: home.join("Downloads/Amass"),
            ..Default::default()
        }
    };
    config.resume_on_startup = false;
    if direct {
        config.network.use_system_proxy = false;
    }
    let mut app = App::new(config.clone(), config_path);
    if let Some(path) = preview {
        return snapshot(&mut app, path);
    }
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err(
            "请在 Windows Terminal / PowerShell 终端直接运行 Amass，不要重定向输入输出".into(),
        );
    }
    app.save()?;
    let mut engine = Engine::open(config).await?;
    // Always restore the console before reporting an error, including shutdown errors.
    let result = interactive(&mut app, &mut engine).await;
    let shutdown = engine.shutdown().await;
    result?;
    shutdown?;
    Ok(())
}
async fn interactive(app: &mut App, engine: &mut Engine) -> Result<()> {
    let mut terminal = ratatui::init();
    let _guard = TerminalGuard;
    execute!(io::stdout(), EnableBracketedPaste, EnableMouseCapture)?;
    let stopped = Arc::new(AtomicBool::new(false));
    let flag = stopped.clone();
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let reader = std::thread::spawn(move || {
        while !flag.load(Ordering::Relaxed) {
            match event::poll(Duration::from_millis(100)) {
                Ok(true) => match event::read() {
                    Ok(ev) => {
                        if tx.blocking_send(Ok(ev)).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e));
                        break;
                    }
                },
                Ok(false) => {}
                Err(e) => {
                    let _ = tx.blocking_send(Err(e));
                    break;
                }
            }
        }
    });
    let result=async{
        app.refresh(engine).await?;
        let mut tick=tokio::time::interval(Duration::from_millis(250));tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop{
            terminal.draw(|f|ui::draw(f,app))?;
            tokio::select!{
                _=tick.tick()=>app.refresh(engine).await?,
                ev=rx.recv()=>match ev.ok_or("终端输入已关闭")??{
                    Event::Key(key) if key.kind!=KeyEventKind::Release=>{
                        match app.key(key,engine).await{Ok(true)=>break,Ok(false)=>{},Err(e)=>app.notice=e.to_string()}
                        app.refresh(engine).await?;
                    }
                    Event::Paste(text)=>{
                        if app.dialog.is_none(){app.dialog=Some(Dialog::New(Form::new(&app.config.download_dir)));}
                        if let Some(Dialog::New(form))=&mut app.dialog{if form.focus<3{form.fields[form.focus].insert(text.trim());}}
                    }
                    Event::Mouse(mouse)=>{
                        if app.dialog.is_none(){match mouse.kind{
                            MouseEventKind::ScrollDown=>app.selected=(app.selected+1).min(app.visible().len().saturating_sub(1)),
                            MouseEventKind::ScrollUp=>app.selected=app.selected.saturating_sub(1),
                            MouseEventKind::Down(MouseButton::Left)=>{
                                if let Some((_,id))=app.hits.iter().find(|(r,_)|r.contains((mouse.column,mouse.row).into())){
                                    let id=*id;if id==u64::MAX{app.dialog=Some(Dialog::New(Form::new(&app.config.download_dir)));}
                                    else if let Some(pos)=app.visible().iter().position(|t|t.id==id){app.selected=pos;}
                                }
                            }
                            _=>{}
                        }}
                    }
                    Event::Resize(_,_)=>{},_=>{}
                }
            }
        }
        Ok::<(),Box<dyn std::error::Error+Send+Sync>>(())
    }.await;
    stopped.store(true, Ordering::Relaxed);
    drop(rx);
    let _ = reader.join();
    result
}

/// Headless render of the actual widgets, for layout review and CI only.
fn snapshot(app: &mut App, path: PathBuf) -> Result<()> {
    use rd_engine::{TaskInfo, TaskState};
    let task = |id, name: &str, state, downloaded, total, speed| TaskInfo {
        id,
        url: "https://modelscope.cn/models/Qwen".into(),
        final_url: None,
        file_name: name.into(),
        file_path: app.config.download_dir.join(name),
        part_path: PathBuf::new(),
        state,
        total_size: Some(total),
        downloaded,
        connections: 32,
        range_supported: true,
        etag: None,
        last_modified: None,
        expected_sha256: None,
        actual_sha256: if state == TaskState::Completed {
            Some("preview".into())
        } else {
            None
        },
        expected_blake3: None,
        actual_blake3: None,
        rate_limit_bps: None,
        error: None,
        created_at: 0,
        updated_at: 0,
        auto_concurrency: true,
        speed_bps: speed,
    };
    app.tasks = vec![
        task(
            3,
            "qwen2-7b-instruct-q4_k_m.gguf",
            TaskState::Downloading,
            2_810_000_000,
            4_683_071_264,
            29_250_000,
        ),
        task(
            2,
            "Rocky-9-latest-x86_64-minimal.iso",
            TaskState::Paused,
            891_230_000,
            2_950_234_112,
            0,
        ),
        task(
            1,
            "project-assets.zip",
            TaskState::Completed,
            134_217_728,
            134_217_728,
            0,
        ),
    ];
    app.history = (0..60)
        .map(|i| 20_000_000 + (i * 733_333) % 12_000_000)
        .collect();
    app.notice = "布局预览 · 示例数据，不代表实际下载速度".into();
    let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(124, 38))?;
    let mut render = |app: &mut App, path: &std::path::Path| -> Result<()> {
        terminal.draw(|f| ui::draw(f, app))?;
        let buffer = terminal.backend().buffer();
        let cells: Vec<_> = (0..38)
            .flat_map(|y| (0..124).map(move |x| (x, y)))
            .map(|(x, y)| {
                let c = &buffer[(x, y)];
                let rgb = |c| match c {
                    ratatui::style::Color::Rgb(r, g, b) => [r, g, b],
                    _ => [238, 233, 227],
                };
                serde_json::json!({"x":x,"y":y,"text":c.symbol(),"fg":rgb(c.fg),"bg":rgb(c.bg)})
            })
            .collect();
        std::fs::write(
            path,
            serde_json::to_vec(&serde_json::json!({"width":124,"height":38,"cells":cells}))?,
        )?;
        Ok(())
    };
    render(app, &path)?;
    app.dialog = Some(Dialog::New(Form::new(&app.config.download_dir)));
    render(app, &path.with_extension("modal.json"))?;
    Ok(())
}
