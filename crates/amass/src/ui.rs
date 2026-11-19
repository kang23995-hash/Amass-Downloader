use crate::app::{human, safe, status, App, Dialog};
use ratatui::{
    prelude::*,
    widgets::{
        Block, BorderType, Borders, Clear, List, ListItem, ListState, Paragraph, Sparkline, Wrap,
    },
};
use rd_engine::TaskState;
pub const RED: Color = Color::Rgb(227, 59, 47);
const BG: Color = Color::Rgb(14, 16, 20);
const PANEL: Color = Color::Rgb(22, 25, 31);
const TEXT: Color = Color::Rgb(238, 233, 227);
const MUTED: Color = Color::Rgb(140, 147, 160);
const LINE: Color = Color::Rgb(46, 51, 61);
const GREEN: Color = Color::Rgb(119, 194, 157);
fn style(c: Color) -> Style {
    Style::default().fg(c)
}
fn block(title: &str) -> Block<'_> {
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(style(LINE))
        .title(Span::styled(format!(" {title} "), style(MUTED)))
        .style(style(TEXT).bg(PANEL))
}
fn fraction(n: u64, total: Option<u64>) -> f64 {
    total
        .filter(|n| *n > 0)
        .map(|t| (n as f64 / t as f64).clamp(0., 1.))
        .unwrap_or(0.)
}

pub fn draw(f: &mut Frame, app: &mut App) {
    let area = f.area();
    app.hits.clear();
    app.remove_hit = None;
    f.render_widget(Block::default().style(style(TEXT).bg(BG)), area);
    if area.width < 64 || area.height < 22 {
        f.render_widget(
            Paragraph::new("AMASS\n\n请将终端放大至至少 64 × 22\nCtrl+C 安全退出")
                .style(style(RED))
                .alignment(Alignment::Center),
            area,
        );
        return;
    }
    let rows = Layout::vertical([
        Constraint::Length(5),
        Constraint::Length(5),
        Constraint::Length(2),
        Constraint::Min(6),
        Constraint::Length(3),
    ])
    .margin(1)
    .split(area);
    let top = Layout::horizontal([Constraint::Min(40), Constraint::Length(24)]).split(rows[0]);
    f.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "  A M A S S",
                style(RED).add_modifier(Modifier::BOLD),
            )),
            Line::from(Span::styled(
                "  D O W N L O A D   W O R K S P A C E",
                style(MUTED),
            )),
            Line::from("  每一次下载，都从容一点。"),
        ]),
        top[0],
    );
    f.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "  ＋ 新建下载  N  ",
                style(TEXT).bg(RED).add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from(Span::styled(
                if app.config.network.use_system_proxy {
                    "● 系统代理 / 自动"
                } else {
                    "● 直连 / 不使用代理"
                },
                style(MUTED),
            )),
        ])
        .alignment(Alignment::Right),
        top[1],
    );
    app.hits
        .push((Rect::new(top[1].x, top[1].y, top[1].width, 1), u64::MAX));
    let stats = Layout::horizontal([
        Constraint::Percentage(44),
        Constraint::Percentage(28),
        Constraint::Percentage(28),
    ])
    .spacing(1)
    .split(rows[1]);
    let active = app
        .tasks
        .iter()
        .filter(|t| t.state.is_active() || t.state == TaskState::Pending)
        .count();
    let complete = app
        .tasks
        .iter()
        .filter(|t| t.state == TaskState::Completed)
        .count();
    for (i, title, value, color) in [
        (
            0,
            "实时下载",
            format!("{:.2} MB/s", app.speed() as f64 / 1e6),
            RED,
        ),
        (1, "进行中", format!("{active:02}  个任务"), TEXT),
        (2, "已完成", format!("{complete:02}  个文件"), GREEN),
    ] {
        f.render_widget(
            Paragraph::new(format!("\n  {value}"))
                .style(style(color).add_modifier(Modifier::BOLD))
                .block(block(title)),
            stats[i],
        );
    }
    let actions = Layout::horizontal([Constraint::Min(0), Constraint::Length(20)]).split(rows[2]);
    let labels = [" 全部 ", " 进行中 ", " 已完成 "];
    f.render_widget(
        Paragraph::new(Line::from(
            labels
                .iter()
                .enumerate()
                .flat_map(|(i, label)| {
                    [
                        Span::styled(
                            *label,
                            if i == app.filter {
                                style(TEXT).bg(RED)
                            } else {
                                style(MUTED)
                            },
                        ),
                        Span::raw("  "),
                    ]
                })
                .collect::<Vec<_>>(),
        )),
        actions[0],
    );
    let can_remove = app.selected_task().is_some();
    f.render_widget(
        Paragraph::new(" 删除记录 D / Del ")
            .alignment(Alignment::Right)
            .style(if can_remove {
                style(TEXT).bg(RED)
            } else {
                style(MUTED)
            }),
        Rect::new(actions[1].x, actions[1].y, actions[1].width, 1),
    );
    if can_remove {
        app.remove_hit = Some(Rect::new(actions[1].x, actions[1].y, actions[1].width, 1));
    }
    let body = if area.width >= 110 {
        Layout::horizontal([Constraint::Min(54), Constraint::Length(34)])
            .spacing(2)
            .split(rows[3])
    } else {
        Layout::horizontal([Constraint::Percentage(100)]).split(rows[3])
    };
    let tasks = app.visible();
    if tasks.is_empty() {
        let inner = block("下载列表").inner(body[0]);
        f.render_widget(block("下载列表"), body[0]);
        let pad = inner.height.saturating_sub(6) / 2;
        f.render_widget(
            Paragraph::new(vec![
                Line::from(""),
                Line::from(Span::styled("↓", style(RED).add_modifier(Modifier::BOLD))),
                Line::from("下一份文件，从这里开始"),
                Line::from(""),
                Line::from(Span::styled("按 N 新建下载，粘贴链接即可", style(MUTED))),
            ])
            .alignment(Alignment::Center),
            Rect::new(
                inner.x,
                inner.y + pad,
                inner.width,
                inner.height.saturating_sub(pad),
            ),
        );
    } else {
        let bar_width = body[0].width.saturating_sub(28).max(6) as usize;
        let items: Vec<ListItem> = tasks
            .iter()
            .map(|t| {
                let ratio = if t.state == TaskState::Completed {
                    1.0
                } else {
                    fraction(t.downloaded, t.total_size)
                };
                let filled = (ratio * bar_width as f64) as usize;
                let color = if t.state == TaskState::Completed {
                    GREEN
                } else if t.state == TaskState::Failed {
                    RED
                } else {
                    MUTED
                };
                ListItem::new(vec![
                    Line::from(vec![
                        Span::styled(
                            format!(" {}  ", safe(&t.file_name)),
                            style(TEXT).add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(status(t.state), style(color)),
                    ]),
                    Line::from(vec![
                        Span::raw(" "),
                        Span::styled(
                            "━".repeat(filled),
                            style(if t.state == TaskState::Completed {
                                GREEN
                            } else {
                                RED
                            }),
                        ),
                        Span::styled("─".repeat(bar_width - filled), style(LINE)),
                        Span::styled(format!("  {:5.1}%", ratio * 100.), style(TEXT)),
                    ]),
                    Line::from(Span::styled(
                        format!(
                            " {} / {} · {:.2} MB/s",
                            human(t.downloaded),
                            t.total_size.map(human).unwrap_or_else(|| "未知大小".into()),
                            t.speed_bps as f64 / 1e6
                        ),
                        style(MUTED),
                    )),
                    Line::from(""),
                ])
            })
            .collect();
        let mut state = ListState::default().with_selected(Some(app.selected));
        f.render_stateful_widget(
            List::new(items)
                .block(block("下载列表"))
                .highlight_style(Style::default().bg(Color::Rgb(39, 27, 29)))
                .highlight_symbol("▎ "),
            body[0],
            &mut state,
        );
        let offset = state.offset();
        let ids: Vec<u64> = tasks.iter().map(|t| t.id).collect();
        for (i, id) in ids.iter().enumerate().skip(offset) {
            let y = body[0].y + 1 + ((i - offset) * 4) as u16;
            if y + 3 >= body[0].bottom() {
                break;
            }
            app.hits
                .push((Rect::new(body[0].x + 1, y, body[0].width - 2, 4), *id));
        }
    }
    if body.len() > 1 {
        detail(f, app, body[1]);
    }
    f.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(safe(&app.notice), style(RED))),
            Line::from(Span::styled(
                "N 新建  ↑↓ 选择  空格 暂停/恢复  D 删除  Tab 分类  O 目录  P 网络  ? 帮助  Q 退出",
                style(MUTED),
            )),
        ]),
        rows[4],
    );
    if let Some(dialog) = &app.dialog {
        modal(f, app, dialog, area);
    }
}
fn detail(f: &mut Frame, app: &App, area: Rect) {
    let panels = Layout::vertical([Constraint::Length(7), Constraint::Min(7)])
        .spacing(1)
        .split(area);
    let history: Vec<u64> = app.history.iter().copied().collect();
    f.render_widget(
        Sparkline::default()
            .block(block("速度走势 · 最近 15 秒"))
            .data(&history)
            .style(style(RED)),
        panels[0],
    );
    let mut lines = vec![];
    if let Some(t) = app.selected_task() {
        let host = url::Url::parse(&t.url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_owned))
            .unwrap_or_default();
        lines.extend([
            Line::from(Span::styled(
                safe(&t.file_name),
                style(TEXT).add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from(Span::styled(
                status(t.state),
                style(if t.state == TaskState::Failed {
                    RED
                } else {
                    GREEN
                }),
            )),
            Line::from(""),
            Line::from(Span::styled("来源", style(MUTED))),
            Line::from(host),
            Line::from(""),
            Line::from(Span::styled("保存位置", style(MUTED))),
            Line::from(safe(&t.file_path.to_string_lossy())),
            Line::from(""),
        ]);
        if t.state.is_active() && t.speed_bps > 0 {
            if let Some(total) = t.total_size {
                let seconds = total.saturating_sub(t.downloaded) / t.speed_bps;
                lines.push(Line::from(format!(
                    "预计剩余  {:02}:{:02}",
                    seconds / 60,
                    seconds % 60
                )));
            }
        }
        if t.total_size.is_some_and(|s| t.downloaded >= s) && t.state.is_active() {
            lines.push(Line::from("正在落盘与校验，请稍候…"));
        }
        if let Some(error) = &t.error {
            lines.push(Line::from(Span::styled(safe(error), style(RED))));
        }
        if t.actual_sha256.is_some() || t.actual_blake3.is_some() {
            lines.push(Line::from(Span::styled("✓ 文件校验通过", style(GREEN))));
        }
    } else {
        lines.extend([
            Line::from("准备好，开始下一次下载。"),
            Line::from(""),
            Line::from(Span::styled("支持断点续传与文件校验", style(MUTED))),
        ]);
    }
    f.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(block("文件详情")),
        panels[1],
    );
}
fn modal(f: &mut Frame, app: &App, dialog: &Dialog, area: Rect) {
    let height = if matches!(dialog, Dialog::New(_)) {
        21
    } else if matches!(dialog, Dialog::Help) {
        18
    } else {
        9
    };
    let width = area.width.saturating_sub(6).min(84);
    let popup = Rect::new(
        (area.width - width) / 2,
        (area.height - height) / 2,
        width,
        height,
    );
    f.render_widget(Clear, popup);
    let title = match dialog {
        Dialog::New(_) => "新建下载",
        Dialog::Help => "快捷键",
        Dialog::Remove(_) => "删除下载记录",
        _ => "确认操作",
    };
    let border = block(title).border_style(style(RED));
    let inner = border.inner(popup);
    f.render_widget(border, popup);
    match dialog{
        Dialog::New(form)=>{
            let fields=Layout::vertical([Constraint::Length(1),Constraint::Length(3),Constraint::Length(3),Constraint::Length(3),Constraint::Length(3),Constraint::Length(2),Constraint::Min(1)]).margin(1).split(inner);
            f.render_widget(Paragraph::new("粘贴文件直链，剩下的交给 Amass。 ").style(style(MUTED)),fields[0]);
            for (i,label) in ["下载链接","保存目录","SHA-256（可选）"].iter().enumerate(){
                let b=Block::default().borders(Borders::ALL).border_type(BorderType::Rounded).border_style(style(if form.focus==i{RED}else{LINE})).title(*label);
                let rect=b.inner(fields[i+1]);let (text,cursor)=form.fields[i].view(rect.width.saturating_sub(1) as usize);
                f.render_widget(Paragraph::new(safe(text)).style(style(TEXT)).block(b),fields[i+1]);
                if form.focus==i {f.set_cursor_position((rect.x+cursor,rect.y));}
            }
            let options=["自动（推荐）","16 连接","32 连接","64 连接"];
            f.render_widget(Paragraph::new(format!(" {}  ← → 切换",options[form.connections])).block(Block::bordered().title("连接策略").border_style(style(if form.focus==3{RED}else{LINE}))),fields[4]);
            f.render_widget(Paragraph::new("Enter 开始下载    Tab 切换字段    Esc 返回").style(style(RED)),fields[5]);
            let mode=if app.config.network.use_system_proxy{"系统代理 / 自动"}else{"直连"};
            let hint = form.error.clone().unwrap_or_else(|| format!("网络：{mode} · 在主页面按 P 切换"));
            f.render_widget(Paragraph::new(hint).style(style(if form.error.is_some() { RED } else { MUTED })),fields[6]);
        }
        Dialog::Help=>f.render_widget(Paragraph::new("\n N             新建下载（支持终端粘贴链接）\n ↑ ↓ / J K     选择任务\n 空格 / Enter  暂停、恢复；已完成则打开目录\n Tab           全部 / 进行中 / 已完成\n O             打开保存目录\n X             取消任务（需确认）\n D / Delete    删除下载记录（需确认，保留已完成文件）\n P             切换直连 / 系统代理（先暂停任务）\n Q / Ctrl+C    保存并退出，下载任务会暂停\n\n 保存位置与网络模式会记住。\n 按 Esc 返回").style(style(TEXT)),inner),
        Dialog::Remove(id) => {
            let name = app.tasks.iter().find(|t| t.id == *id)
                .map(|t| safe(&t.file_name)).unwrap_or_else(|| format!("任务 #{id}"));
            f.render_widget(Paragraph::new(format!("\n  删除记录：{name}\n  已完成的文件保留在磁盘上。\n  未完成的任务将停止，临时数据会被清理。\n\n  Enter / Y 确认          Esc / N 返回")).style(style(TEXT)), inner);
        }
        _=>f.render_widget(Paragraph::new("\n  取消此任务并清理未完成的临时数据。\n  任务记录仍保留在列表中。\n\n  Enter / Y 确认          Esc / N 返回").style(style(TEXT)),inner),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn responsive_empty_and_modal_render() {
        for (w, h) in [(64, 22), (100, 30), (140, 42), (40, 12)] {
            let mut app = App::new(rd_engine::EngineConfig::default(), "unused.toml".into());
            let mut terminal =
                ratatui::Terminal::new(ratatui::backend::TestBackend::new(w, h)).unwrap();
            terminal.draw(|f| draw(f, &mut app)).unwrap();
            app.dialog = Some(Dialog::New(crate::app::Form::new(&app.config.download_dir)));
            terminal.draw(|f| draw(f, &mut app)).unwrap();
        }
    }
    #[test]
    fn progress_is_bounded() {
        assert_eq!(fraction(200, Some(100)), 1.);
        assert_eq!(fraction(0, None), 0.);
    }
}
