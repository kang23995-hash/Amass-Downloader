use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use rd_engine::{AddTaskRequest, Engine, EngineConfig, TaskInfo, TaskState};
use std::{collections::VecDeque, path::PathBuf};
use unicode_width::UnicodeWidthStr;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Default, Clone)]
pub struct Editor {
    pub text: String,
    pub cursor: usize,
}
impl Editor {
    pub fn new(text: String) -> Self {
        let cursor = text.len();
        Self { text, cursor }
    }
    pub fn insert(&mut self, text: &str) {
        let clean: String = text.chars().filter(|c| !c.is_control()).collect();
        self.text.insert_str(self.cursor, &clean);
        self.cursor += clean.len();
    }
    pub fn key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('a') if key.modifiers.contains(KeyModifiers::CONTROL) => self.cursor = 0,
            KeyCode::Char('e') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.cursor = self.text.len()
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.text.clear();
                self.cursor = 0;
            }
            KeyCode::Char(c)
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                self.insert(&c.to_string())
            }
            KeyCode::Home => self.cursor = 0,
            KeyCode::End => self.cursor = self.text.len(),
            KeyCode::Left | KeyCode::Backspace if self.cursor > 0 => {
                let prev = self.text[..self.cursor].char_indices().last().unwrap().0;
                if key.code == KeyCode::Backspace {
                    self.text.drain(prev..self.cursor);
                }
                self.cursor = prev;
            }
            KeyCode::Right | KeyCode::Delete if self.cursor < self.text.len() => {
                let next =
                    self.cursor + self.text[self.cursor..].chars().next().unwrap().len_utf8();
                if key.code == KeyCode::Delete {
                    self.text.drain(self.cursor..next);
                } else {
                    self.cursor = next;
                }
            }
            _ => {}
        }
    }
    pub fn view(&self, width: usize) -> (&str, u16) {
        let mut start = 0;
        while self.text[start..self.cursor].width() >= width.max(1) {
            start += self.text[start..].chars().next().unwrap().len_utf8();
        }
        (
            &self.text[start..],
            self.text[start..self.cursor].width() as u16,
        )
    }
}

pub struct Form {
    pub fields: [Editor; 3],
    pub focus: usize,
    pub connections: usize,
    pub error: Option<String>,
}
impl Form {
    pub fn new(folder: &std::path::Path) -> Self {
        Self {
            fields: [
                Editor::default(),
                Editor::new(folder.to_string_lossy().into()),
                Editor::default(),
            ],
            focus: 0,
            connections: 0,
            error: None,
        }
    }
    pub fn request(&self) -> Result<AddTaskRequest> {
        let url = self.fields[0].text.trim();
        let parsed = url::Url::parse(url).map_err(|_| "请输入完整的 HTTP / HTTPS 下载链接")?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err("仅支持 HTTP / HTTPS 下载链接".into());
        }
        if self.fields[1].text.trim().is_empty() {
            return Err("请选择保存目录".into());
        }
        let mut req = AddTaskRequest::new(url);
        req.output_dir = Some(PathBuf::from(self.fields[1].text.trim()));
        req.connections = [None, Some(16), Some(32), Some(64)][self.connections];
        let hash = self.fields[2].text.trim();
        if !hash.is_empty() {
            if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err("SHA-256 应为 64 位十六进制字符，或留空".into());
            }
            req.expected_sha256 = Some(hash.to_ascii_lowercase());
        }
        Ok(req)
    }
}
pub enum Dialog {
    New(Form),
    Remove(u64),
    Cancel(u64),
    Help,
}

pub struct App {
    pub tasks: Vec<TaskInfo>,
    pub selected: usize,
    pub filter: usize,
    pub dialog: Option<Dialog>,
    pub notice: String,
    pub history: VecDeque<u64>,
    pub config: EngineConfig,
    pub config_path: PathBuf,
    pub hits: Vec<(ratatui::layout::Rect, u64)>,
    pub remove_hit: Option<ratatui::layout::Rect>,
}
impl App {
    pub fn new(config: EngineConfig, config_path: PathBuf) -> Self {
        Self {
            tasks: vec![],
            selected: 0,
            filter: 0,
            dialog: None,
            notice: "N 新建下载 · 粘贴链接即可开始".into(),
            history: VecDeque::new(),
            config,
            config_path,
            hits: vec![],
            remove_hit: None,
        }
    }
    pub fn visible(&self) -> Vec<&TaskInfo> {
        self.tasks
            .iter()
            .filter(|t| match self.filter {
                1 => !matches!(t.state, TaskState::Completed | TaskState::Cancelled),
                2 => t.state == TaskState::Completed,
                _ => true,
            })
            .collect()
    }
    pub fn selected_task(&self) -> Option<&TaskInfo> {
        self.visible().get(self.selected).copied()
    }
    pub fn confirm_remove(&mut self) {
        if let Some(t) = self.selected_task() {
            self.dialog = Some(Dialog::Remove(t.id));
        }
    }
    pub fn speed(&self) -> u64 {
        self.tasks
            .iter()
            .filter(|t| t.state.is_active())
            .map(|t| t.speed_bps)
            .sum()
    }
    pub async fn refresh(&mut self, engine: &Engine) -> Result<()> {
        self.tasks = engine.list_tasks().await?;
        self.tasks.sort_by_key(|t| std::cmp::Reverse(t.id));
        self.selected = self.selected.min(self.visible().len().saturating_sub(1));
        self.history.push_back(self.speed());
        if self.history.len() > 60 {
            self.history.pop_front();
        }
        Ok(())
    }
    pub fn save(&self) -> Result<()> {
        if let Some(parent) = self.config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.config_path, self.config.to_toml()?)?;
        Ok(())
    }
    pub async fn toggle(&mut self, engine: &Engine) -> Result<()> {
        if let Some(task) = self.selected_task() {
            let (id, state) = (task.id, task.state);
            match state {
                TaskState::Completed => self.open_folder()?,
                TaskState::Cancelled => self.notice = "已取消的任务不能恢复；按 N 新建下载".into(),
                s if s.is_active() || s == TaskState::Pending => {
                    engine.pause_task(id).await?;
                    self.notice = "已暂停，数据已保存".into();
                }
                _ => {
                    engine.resume_task(id).await?;
                    self.notice = "正在恢复下载".into();
                }
            }
        }
        Ok(())
    }
    pub fn open_folder(&self) -> Result<()> {
        let path = self
            .selected_task()
            .map(|t| t.file_path.parent().unwrap_or(&self.config.download_dir))
            .unwrap_or(&self.config.download_dir);
        std::fs::create_dir_all(path)?;
        #[cfg(windows)]
        let program = "explorer.exe";
        #[cfg(target_os = "macos")]
        let program = "open";
        #[cfg(all(not(windows), not(target_os = "macos")))]
        let program = "xdg-open";
        std::process::Command::new(program).arg(path).spawn()?;
        Ok(())
    }
    /// Returns true only when the UI should close. Engine shutdown is owned
    /// by main, so errors and Ctrl+C still drain pending disk writes.
    pub async fn key(&mut self, key: KeyEvent, engine: &mut Engine) -> Result<bool> {
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            return Ok(true);
        }
        if let Some(dialog) = self.dialog.take() {
            match dialog {
                Dialog::New(mut form) => {
                    match key.code {
                        KeyCode::Esc => return Ok(false),
                        KeyCode::Tab => form.focus = (form.focus + 1) % 4,
                        KeyCode::BackTab => form.focus = (form.focus + 3) % 4,
                        KeyCode::Enter => {
                            let req = match form.request() {
                                Ok(req) => req,
                                Err(e) => {
                                    self.notice = e.to_string();
                                    form.error = Some(e.to_string());
                                    self.dialog = Some(Dialog::New(form));
                                    return Ok(false);
                                }
                            };
                            self.config.download_dir = req.output_dir.clone().unwrap();
                            match engine.add_task(req).await {
                                Ok(id) => {
                                    self.notice = format!("任务 #{id} 已加入下载队列");
                                    self.selected = 0;
                                    self.filter = 0;
                                    if let Err(e) = self.save() {
                                        self.notice = format!("下载已开始，但设置保存失败：{e}");
                                    }
                                }
                                Err(e) => {
                                    self.notice = e.to_string();
                                    form.error = Some(e.to_string());
                                    self.dialog = Some(Dialog::New(form));
                                }
                            }
                            return Ok(false);
                        }
                        KeyCode::Left if form.focus == 3 => {
                            form.connections = (form.connections + 3) % 4
                        }
                        KeyCode::Right | KeyCode::Char(' ') if form.focus == 3 => {
                            form.connections = (form.connections + 1) % 4
                        }
                        _ if form.focus < 3 => form.fields[form.focus].key(key),
                        _ => {}
                    }
                    self.dialog = Some(Dialog::New(form));
                }
                Dialog::Remove(id) | Dialog::Cancel(id) => {
                    if matches!(key.code, KeyCode::Enter | KeyCode::Char('y')) {
                        let state = engine.get_task(id).await?.state;
                        if state != TaskState::Completed && state != TaskState::Cancelled {
                            engine.cancel_task(id).await?;
                        }
                        if matches!(dialog, Dialog::Remove(_)) {
                            engine.remove_task(id, false).await?;
                            self.notice = "下载记录已删除；已完成的文件保留在磁盘上".into();
                        } else {
                            self.notice = "任务已取消，未完成的临时数据已清理".into();
                        }
                    } else if !matches!(key.code, KeyCode::Esc | KeyCode::Char('n')) {
                        self.dialog = Some(dialog);
                    }
                }
                Dialog::Help => {
                    if !matches!(key.code, KeyCode::Esc | KeyCode::Enter | KeyCode::Char('?')) {
                        self.dialog = Some(Dialog::Help);
                    }
                }
            }
            return Ok(false);
        }
        match key.code {
            KeyCode::Char('q') => return Ok(true),
            KeyCode::Char('n') => {
                self.dialog = Some(Dialog::New(Form::new(&self.config.download_dir)));
                self.notice = "Tab 切换字段 · Enter 开始下载 · Esc 返回".into();
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.selected = (self.selected + 1).min(self.visible().len().saturating_sub(1))
            }
            KeyCode::Up | KeyCode::Char('k') => self.selected = self.selected.saturating_sub(1),
            KeyCode::Tab => {
                self.filter = (self.filter + 1) % 3;
                self.selected = 0;
            }
            KeyCode::Enter | KeyCode::Char(' ') | KeyCode::Char('r') => self.toggle(engine).await?,
            KeyCode::Char('o') => self.open_folder()?,
            KeyCode::Char('d') | KeyCode::Delete => self.confirm_remove(),
            KeyCode::Char('x') => {
                if let Some(t) = self.selected_task() {
                    if t.state != TaskState::Completed {
                        self.dialog = Some(Dialog::Cancel(t.id));
                    }
                }
            }
            KeyCode::Char('?') => self.dialog = Some(Dialog::Help),
            KeyCode::Char('p') => {
                if self
                    .tasks
                    .iter()
                    .any(|t| t.state.is_active() || t.state == TaskState::Pending)
                {
                    self.notice = "请先暂停所有下载，再切换直连 / 系统代理".into();
                } else {
                    let old = self.config.clone();
                    let mut next = old.clone();
                    next.network.use_system_proxy = !next.network.use_system_proxy;
                    engine.shutdown().await?;
                    match Engine::open(next.clone()).await {
                        Ok(new) => {
                            *engine = new;
                            self.config = next;
                            self.save()?;
                            self.notice = "网络模式已切换，下次恢复下载时生效".into();
                        }
                        Err(e) => {
                            *engine = Engine::open(old).await?;
                            self.notice = format!("切换失败：{e}");
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(false)
    }
}

pub fn human(n: u64) -> String {
    if n >= 1024 * 1024 * 1024 {
        format!("{:.2} GiB", n as f64 / 1073741824.)
    } else if n >= 1024 * 1024 {
        format!("{:.1} MiB", n as f64 / 1048576.)
    } else {
        format!("{:.1} KiB", n as f64 / 1024.)
    }
}
pub fn status(s: TaskState) -> &'static str {
    match s {
        TaskState::Pending => "排队中",
        TaskState::Probing => "正在连接",
        TaskState::Downloading => "下载中",
        TaskState::Paused => "已暂停",
        TaskState::Retrying => "重试中",
        TaskState::Completed => "已完成",
        TaskState::Failed => "失败",
        TaskState::Cancelled => "已取消",
    }
}
pub fn safe(s: &str) -> String {
    s.chars().filter(|c| !c.is_control()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unicode_editing_and_terminal_controls() {
        let mut e = Editor::default();
        e.insert("中文a\u{1b}\n");
        assert_eq!(e.text, "中文a");
        e.key(KeyCode::Left.into());
        e.key(KeyCode::Backspace.into());
        assert_eq!(e.text, "中a");
        let (_, x) = e.view(2);
        assert!(x < 2);
    }
    #[test]
    fn request_validation() {
        let mut f = Form::new(std::path::Path::new("downloads"));
        f.fields[0] = Editor::new("file:///secret".into());
        assert!(f.request().is_err());
        f.fields[0] = Editor::new("https://example.com/file".into());
        f.fields[2] = Editor::new("bad".into());
        assert!(f.request().is_err());
        f.fields[2] = Editor::default();
        f.connections = 2;
        assert_eq!(f.request().unwrap().connections, Some(32));
    }
}
