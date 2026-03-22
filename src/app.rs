use chrono::NaiveDate;
use eframe::egui;
use rayon::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::backup::{
    backup_files, session_name, spot_check, verify_backup, verify_db_records, verify_files,
};
use crate::config::Config;
use crate::db::Database;
use crate::scanner::{scan_source, unique_dates, MediaFile, MediaType};
use crate::watcher::{start_watcher, CardAlert};

// ── Log ──────────────────────────────────────────────────────
struct LogInner {
    entries: Vec<String>,
    generation: u64,
}

#[derive(Clone)]
struct Log {
    inner: Arc<Mutex<LogInner>>,
}

impl Log {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(LogInner {
                entries: Vec::new(),
                generation: 0,
            })),
        }
    }

    fn push(&self, msg: impl Into<String>) {
        let ts = chrono::Local::now().format("%H:%M:%S");
        let line = format!("[{ts}] {}", msg.into());
        let mut inner = self.inner.lock().unwrap();
        if inner.entries.len() >= 2000 {
            inner.entries.drain(..500);
        }
        inner.entries.push(line);
        inner.generation += 1;
    }

    fn generation(&self) -> u64 {
        self.inner.lock().unwrap().generation
    }

    fn entries(&self) -> Vec<String> {
        self.inner.lock().unwrap().entries.clone()
    }

    fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.entries.clear();
        inner.generation += 1;
    }
}

// ── 后台任务 ─────────────────────────────────────────────────
#[derive(Default)]
enum TaskResult {
    #[default]
    None,
    BackupWithVerify {
        summary: String,
    },
    Verify {
        errors: Vec<String>,
    },
    Reindex {
        summary: String,
    },
    Prescan {
        files: Vec<MediaFile>,
        backed_up: usize,
        new_count: usize,
    },
    Error(String),
}

#[derive(Default)]
struct TaskState {
    current: usize,
    total: usize,
    label: String,
    done: bool,
    result: TaskResult,
}

impl TaskState {
    fn ratio(&self) -> f32 {
        if self.total == 0 {
            0.0
        } else {
            self.current as f32 / self.total as f32
        }
    }
}

// ── Screen ───────────────────────────────────────────────────
#[derive(Default, PartialEq)]
enum Screen {
    Setup,
    #[default]
    Home,
    Prescanning,
    ScanResult,
    DateDecision,
    NamingSingle,
    NamingMulti,
    Running,
    SpotChecking,  // 备份后自动抽检进行中
    SpotCheckDone, // 抽检结果
    Done,
    VerifyPick,
    VerifyDone,
    ListRecords,
}

// ── App ──────────────────────────────────────────────────────
pub struct App {
    config: Config,
    source_path: String,
    screen: Screen,

    setup_photos: String,
    setup_videos: String,
    setup_error: String,

    scanned_files: Vec<MediaFile>,
    unique_dates: Vec<NaiveDate>,

    split_mode: bool,
    suffix_single: String,
    suffix_per_day: Vec<String>,

    task: Arc<Mutex<TaskState>>,
    /// 重建索引独立任务槽，可与主任务并行
    reindex_task: Arc<Mutex<TaskState>>,
    reindex_running: bool,
    reindex_result_msg: String,
    log: Log,
    log_cache: Vec<String>,
    log_gen: u64,

    verify_mismatches: Vec<String>,
    list_records: Vec<crate::db::FileRecord>,
    prescan_backed_up: usize,
    prescan_new: usize,

    /// 备份后抽检结果
    spot_result: Option<crate::backup::SpotCheckResult>,
    spot_result_arc: Arc<Mutex<Option<crate::backup::SpotCheckResult>>>,

    /// 储存卡自动检测结果（后台线程写入，支持多卷）
    card_alert: Arc<Mutex<(Vec<CardAlert>, u64)>>,
    /// 本地缓存，避免每帧克隆
    card_alert_cache: Vec<CardAlert>,
    card_alert_gen: u64,
    /// 用户已手动关闭的通知（按卷路径去重）
    dismissed_alerts: std::collections::HashSet<PathBuf>,
    /// 已写入“检测到设备”日志的卷，避免重复刷屏
    logged_card_alerts: std::collections::HashSet<PathBuf>,
}

// ── 字体 & 样式 ──────────────────────────────────────────────
static NOTO_EMOJI: &[u8] = include_bytes!("../assets/NotoEmoji-Regular.ttf");

fn load_cjk_font(ctx: &egui::Context) {
    // 静态候选路径
    #[allow(unused_mut)]
    let mut candidates: Vec<String> = vec![
        // macOS — 精确路径
        "/System/Library/Fonts/STHeiti Medium.ttc".into(),
        "/System/Library/Fonts/STHeiti Light.ttc".into(),
        "/System/Library/Fonts/Hiragino Sans GB.ttc".into(),
        "/System/Library/Fonts/Supplemental/Songti.ttc".into(),
        // macOS — PingFang 可能在 AssetsV2 下（动态搜索会补充）
        "/System/Library/PrivateFrameworks/FontServices.framework/Versions/A/Resources/Reserved/PingFangUI.ttc".into(),
        // Windows
        "C:/Windows/Fonts/msyh.ttc".into(),
        "C:/Windows/Fonts/simhei.ttf".into(),
        "C:/Windows/Fonts/simsun.ttc".into(),
        // Linux
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc".into(),
        "/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc".into(),
        "/usr/share/fonts/truetype/noto/NotoSansCJKsc-Regular.otf".into(),
    ];

    // macOS: 搜索 PingFang.ttc（路径含哈希，无法硬编码）
    #[cfg(target_os = "macos")]
    {
        if let Ok(entries) =
            std::fs::read_dir("/System/Library/AssetsV2/com_apple_MobileAsset_Font8")
        {
            for entry in entries.flatten() {
                let p = entry.path().join("AssetData/PingFang.ttc");
                if p.exists() {
                    candidates.insert(0, p.to_string_lossy().to_string());
                    break;
                }
            }
        }
    }

    let mut fonts = egui::FontDefinitions::default();

    // Emoji 排在默认字体之后、CJK 之前：
    // - 默认字体 (Ubuntu-Light / Hack) 先渲染 ASCII 数字和拉丁字符（比例宽度）
    // - Emoji 字体处理 emoji 码位
    // - CJK 字体最后兜底中文字符
    // 不能放 position 0，否则 NotoEmoji 的等宽数字字形会抢先渲染数字。
    fonts
        .font_data
        .insert("emoji".to_owned(), egui::FontData::from_static(NOTO_EMOJI));
    fonts
        .families
        .entry(egui::FontFamily::Proportional)
        .or_default()
        .insert(1, "emoji".to_owned());
    fonts
        .families
        .entry(egui::FontFamily::Monospace)
        .or_default()
        .insert(1, "emoji".to_owned());

    for path in &candidates {
        if let Ok(data) = std::fs::read(path) {
            fonts
                .font_data
                .insert("cjk".to_owned(), egui::FontData::from_owned(data));
            // Push CJK to end of fallback chain so default font handles ASCII/digits first.
            // CJK at position 1 would render digits with full-width glyphs.
            fonts
                .families
                .entry(egui::FontFamily::Proportional)
                .or_default()
                .push("cjk".to_owned());
            fonts
                .families
                .entry(egui::FontFamily::Monospace)
                .or_default()
                .push("cjk".to_owned());
            break;
        }
    }

    ctx.set_fonts(fonts);
}

fn setup_style(ctx: &egui::Context) {
    let mut style = (*ctx.style()).clone();
    style.spacing.item_spacing = egui::vec2(8.0, 6.0);
    style.spacing.button_padding = egui::vec2(14.0, 6.0);
    style.visuals.widgets.noninteractive.rounding = egui::Rounding::same(4.0);
    style.visuals.widgets.inactive.rounding = egui::Rounding::same(4.0);
    style.visuals.widgets.active.rounding = egui::Rounding::same(4.0);
    style.visuals.widgets.hovered.rounding = egui::Rounding::same(4.0);
    ctx.set_style(style);
}

// ── 颜色常量 ─────────────────────────────────────────────────
const GREEN: egui::Color32 = egui::Color32::from_rgb(80, 180, 80);
const RED: egui::Color32 = egui::Color32::from_rgb(220, 70, 70);
const ORANGE: egui::Color32 = egui::Color32::from_rgb(220, 150, 40);
const DIM: egui::Color32 = egui::Color32::from_rgb(140, 140, 140);
const LOG_BG: egui::Color32 = egui::Color32::from_rgb(30, 30, 36);
const LOG_FG: egui::Color32 = egui::Color32::from_rgb(180, 190, 200);

const BTN_WIDE: [f32; 2] = [140.0, 32.0];
const BTN_MED: [f32; 2] = [110.0, 32.0];
const BTN_BACK: [f32; 2] = [90.0, 28.0];

// ── 构造 ─────────────────────────────────────────────────────
impl App {
    pub fn new(cc: &eframe::CreationContext) -> Self {
        load_cjk_font(&cc.egui_ctx);
        setup_style(&cc.egui_ctx);
        cc.egui_ctx.set_visuals(egui::Visuals::dark());

        let config = Config::load();
        let log = Log::new();
        log.push("moving_media 启动");

        let first_screen = if config.is_ready() {
            let db_ready = config.photos_db().exists() && config.videos_db().exists();
            if db_ready {
                log.push(format!("照片目录: {}", config.photos_root.display()));
                log.push(format!("视频目录: {}", config.videos_root.display()));

                // 启动时检查数据库版本
                for (label, path) in [("照片", config.photos_db()), ("视频", config.videos_db())]
                {
                    match crate::db::check_version(&path) {
                        Ok(crate::db::VersionStatus::Ok) => {}
                        Ok(crate::db::VersionStatus::TooNew {
                            db_version,
                            app_version,
                        }) => {
                            log.push(format!(
                                "⚠ {label}数据库版本 v{db_version} 高于程序支持的 v{app_version}，请升级程序"
                            ));
                        }
                        Ok(crate::db::VersionStatus::NeedsUpgrade {
                            db_version,
                            app_version,
                        }) => {
                            log.push(format!(
                                "{label}数据库 v{db_version} → v{app_version}，将在首次使用时自动迁移"
                            ));
                        }
                        Err(e) => {
                            log.push(format!("⚠ 检查{label}数据库版本失败: {e}"));
                        }
                    }
                }

                Screen::Home
            } else {
                log.push("检测到目标目录，但数据库尚未初始化");
                log.push("请点击「确认并初始化」创建数据库");
                Screen::Setup
            }
        } else {
            log.push("首次使用，请配置备份目录");
            Screen::Setup
        };

        let card_alert: Arc<Mutex<(Vec<CardAlert>, u64)>> = Arc::new(Mutex::new((Vec::new(), 0)));

        // 启动储存卡监听线程
        start_watcher(card_alert.clone(), cc.egui_ctx.clone());
        log.push("储存卡监听已启动（检测到后等待手动选择）");

        let initial_source = config
            .last_source
            .as_deref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();

        App {
            setup_photos: config.photos_root.to_string_lossy().to_string(),
            setup_videos: config.videos_root.to_string_lossy().to_string(),
            setup_error: String::new(),
            config,
            source_path: initial_source,
            screen: first_screen,
            scanned_files: Vec::new(),
            unique_dates: Vec::new(),
            split_mode: false,
            suffix_single: String::new(),
            suffix_per_day: Vec::new(),
            task: Arc::new(Mutex::new(TaskState::default())),
            reindex_task: Arc::new(Mutex::new(TaskState::default())),
            reindex_running: false,
            reindex_result_msg: String::new(),
            log,
            log_cache: Vec::new(),
            log_gen: 0,
            verify_mismatches: Vec::new(),
            list_records: Vec::new(),
            prescan_backed_up: 0,
            prescan_new: 0,
            spot_result: None,
            spot_result_arc: Arc::new(Mutex::new(None)),
            card_alert,
            card_alert_cache: Vec::new(),
            card_alert_gen: 0,
            dismissed_alerts: std::collections::HashSet::new(),
            logged_card_alerts: std::collections::HashSet::new(),
        }
    }
}

// ── 主循环 ───────────────────────────────────────────────────
impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // 检查预扫描完成
        if self.screen == Screen::Prescanning {
            let done = self.task.lock().unwrap().done;
            if done {
                let result = std::mem::take(&mut self.task.lock().unwrap().result);
                match result {
                    TaskResult::Prescan {
                        files,
                        backed_up,
                        new_count,
                    } => {
                        self.log
                            .push(format!("已备份: {backed_up}, 待备份: {new_count}"));
                        self.scanned_files = files;
                        self.prescan_backed_up = backed_up;
                        self.prescan_new = new_count;
                        self.screen = Screen::ScanResult;
                    }
                    TaskResult::Error(msg) => {
                        self.log.push(&msg);
                        self.screen = Screen::Home;
                    }
                    _ => {
                        self.screen = Screen::Home;
                    }
                }
            } else {
                ctx.request_repaint();
            }
        }

        // 检查后台任务完成
        if self.screen == Screen::Running {
            let done = self.task.lock().unwrap().done;
            if done {
                let result = std::mem::take(&mut self.task.lock().unwrap().result);
                match result {
                    TaskResult::Verify { errors } => {
                        self.verify_mismatches = errors;
                        self.log.push("校验完成");
                        self.screen = Screen::VerifyDone;
                    }
                    TaskResult::BackupWithVerify { summary } => {
                        self.log.push(&summary);
                        self.spot_result = self.spot_result_arc.lock().unwrap().take();
                        self.screen = Screen::SpotCheckDone;
                    }
                    TaskResult::Error(msg) => {
                        self.log.push(&msg);
                        self.screen = Screen::Done;
                    }
                    _ => {
                        self.screen = Screen::Done;
                    }
                }
            } else {
                ctx.request_repaint();
            }
        }

        // 检查抽检完成
        if self.screen == Screen::SpotChecking {
            let done = self.task.lock().unwrap().done;
            if done {
                self.spot_result = self.spot_result_arc.lock().unwrap().take();
                self.screen = Screen::SpotCheckDone;
            } else {
                ctx.request_repaint();
            }
        }

        // ── 刷新储存卡缓存 ─────────────────────────
        {
            let guard = self.card_alert.lock().unwrap();
            if guard.1 != self.card_alert_gen {
                self.card_alert_cache = guard.0.clone();
                self.card_alert_gen = guard.1;
            }
        }

        // 卷被移除后，清理对应的通知状态，避免下次插入同路径的设备被永久隐藏
        {
            let current_paths: std::collections::HashSet<PathBuf> = self
                .card_alert_cache
                .iter()
                .map(|card| card.volume_path.clone())
                .collect();
            self.dismissed_alerts
                .retain(|path| current_paths.contains(path));
            self.logged_card_alerts
                .retain(|path| current_paths.contains(path));
        }

        // ── 储存卡检测日志（每卷只记录一次） ──────
        {
            let cache = self.card_alert_cache.clone();
            for card in &cache {
                if self.logged_card_alerts.insert(card.volume_path.clone()) {
                    self.log.push(format!(
                        "检测到储存设备「{}」，等待手动选择后再扫描",
                        card.volume_name
                    ));
                }
            }
        }

        // ── 储存卡通知 banner ──────────────────────
        self.show_card_banner(ctx);

        // ── 重建索引进度（独立于主任务） ─────────────
        if self.reindex_running {
            let done = self.reindex_task.lock().unwrap().done;
            if done {
                let result = std::mem::take(&mut self.reindex_task.lock().unwrap().result);
                self.reindex_running = false;
                if let TaskResult::Reindex { summary } = result {
                    self.log.push(&summary);
                    self.reindex_result_msg = summary;
                } else if let TaskResult::Error(msg) = result {
                    self.log.push(&msg);
                    self.reindex_result_msg = msg;
                }
            } else {
                ctx.request_repaint();
            }
        }
        self.show_reindex_bar(ctx);

        // ── 下方 log 面板 ──────────────────────────
        egui::TopBottomPanel::bottom("log_panel")
            .min_height(120.0)
            .max_height(180.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.add_space(2.0);
                ui.horizontal(|ui| {
                    ui.colored_label(DIM, "日志");
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.small_button("清除").clicked() {
                            self.log.clear();
                        }
                    });
                });
                ui.separator();

                egui::Frame::none()
                    .fill(LOG_BG)
                    .inner_margin(egui::Margin::same(6.0))
                    .rounding(egui::Rounding::same(4.0))
                    .show(ui, |ui| {
                        egui::ScrollArea::vertical()
                            .auto_shrink([false, false])
                            .stick_to_bottom(true)
                            .show(ui, |ui| {
                                // Only re-clone log entries when generation changes
                                let gen = self.log.generation();
                                if gen != self.log_gen {
                                    self.log_cache = self.log.entries();
                                    self.log_gen = gen;
                                }
                                for line in &self.log_cache {
                                    ui.label(egui::RichText::new(line).size(12.0).color(LOG_FG));
                                }
                            });
                    });
            });

        // ── 上方主内容 ────────────────────────────
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.add_space(6.0);
            match self.screen {
                Screen::Setup => self.ui_setup(ui),
                Screen::Home => self.ui_home(ui),
                Screen::Prescanning => self.ui_prescanning(ui),
                Screen::ScanResult => self.ui_scan_result(ui),
                Screen::DateDecision => self.ui_date_decision(ui),
                Screen::NamingSingle => self.ui_naming_single(ui),
                Screen::NamingMulti => self.ui_naming_multi(ui),
                Screen::Running => self.ui_running(ui),
                Screen::SpotChecking => self.ui_spot_checking(ui),
                Screen::SpotCheckDone => self.ui_spot_check_done(ui),
                Screen::Done => self.ui_done(ui),
                Screen::VerifyPick => self.ui_verify_pick(ui),
                Screen::VerifyDone => self.ui_verify_done(ui),
                Screen::ListRecords => self.ui_list_records(ui),
            }
        });
    }
}

// ── UI ───────────────────────────────────────────────────────
impl App {
    // ─ 重建索引浮动进度条 ──────────────────────────────────
    fn show_reindex_bar(&mut self, ctx: &egui::Context) {
        if !self.reindex_running && self.reindex_result_msg.is_empty() {
            return;
        }

        egui::TopBottomPanel::top("reindex_bar").show(ctx, |ui| {
            egui::Frame::none()
                .fill(egui::Color32::from_rgb(35, 40, 55))
                .inner_margin(egui::Margin::symmetric(12.0, 6.0))
                .show(ui, |ui| {
                    if self.reindex_running {
                        let (ratio, cur, tot) = {
                            let t = self.reindex_task.lock().unwrap();
                            (t.ratio(), t.current, t.total)
                        };
                        ui.horizontal(|ui| {
                            ui.label("🔄 重建索引");
                            ui.add(egui::ProgressBar::new(ratio).desired_width(320.0).text(
                                if tot > 0 {
                                    format!("{cur}/{tot}")
                                } else {
                                    "准备中…".into()
                                },
                            ));
                        });
                    } else {
                        // 显示完成结果，点击关闭
                        ui.horizontal(|ui| {
                            ui.label(format!("🔄 {}", self.reindex_result_msg));
                            ui.with_layout(
                                egui::Layout::right_to_left(egui::Align::Center),
                                |ui| {
                                    if ui.small_button("✕").clicked() {
                                        self.reindex_result_msg.clear();
                                    }
                                },
                            );
                        });
                    }
                });
        });
    }

    // ─ 储存卡通知 banner（支持多卷同时插入）──────────────
    fn show_card_banner(&mut self, ctx: &egui::Context) {
        let visible: Vec<CardAlert> = self
            .card_alert_cache
            .iter()
            .filter(|c| !self.dismissed_alerts.contains(&c.volume_path))
            .cloned()
            .collect();
        if visible.is_empty() {
            return;
        }

        // 先收集操作，避免在 egui 闭包内借用 self
        let mut to_dismiss: Vec<PathBuf> = Vec::new();
        let mut to_select: Option<(PathBuf, String)> = None;

        egui::TopBottomPanel::top("card_banner").show(ctx, |ui| {
            egui::Frame::none()
                .fill(egui::Color32::from_rgb(35, 42, 58))
                .inner_margin(egui::Margin::symmetric(12.0, 7.0))
                .show(ui, |ui| {
                    for card in &visible {
                        ui.horizontal(|ui| {
                            ui.colored_label(
                                ORANGE,
                                format!("已检测到储存设备「{}」", card.volume_name),
                            );
                            ui.colored_label(DIM, "未读取内容，等待你选择");

                            ui.with_layout(
                                egui::Layout::right_to_left(egui::Align::Center),
                                |ui| {
                                    if ui.small_button("✕").clicked() {
                                        to_dismiss.push(card.volume_path.clone());
                                    }
                                    if to_select.is_none() {
                                        if ui
                                            .add_sized([82.0, 22.0], egui::Button::new("选择此卡"))
                                            .clicked()
                                        {
                                            to_select = Some((
                                                card.volume_path.clone(),
                                                card.volume_name.clone(),
                                            ));
                                            to_dismiss.push(card.volume_path.clone());
                                        }
                                    }
                                },
                            );
                        });
                    }
                });
        });

        for path in to_dismiss {
            self.dismissed_alerts.insert(path);
        }
        if let Some((path, name)) = to_select {
            self.source_path = path.to_string_lossy().to_string();
            self.log.push(format!(
                "已选择储存设备「{}」作为来源，等待手动点击“扫描检测”或“备份”",
                name
            ));
        }
    }

    // ─ Setup ───────────────────────────────────────────────
    fn ui_setup(&mut self, ui: &mut egui::Ui) {
        ui.heading("初始配置");
        ui.label("设置照片和视频备份目标目录，数据库将自动创建。");
        ui.add_space(10.0);
        ui.separator();
        ui.add_space(10.0);

        egui::Grid::new("setup_grid")
            .num_columns(3)
            .spacing([8.0, 10.0])
            .show(ui, |ui| {
                ui.label("照片目录：");
                ui.add(egui::TextEdit::singleline(&mut self.setup_photos).desired_width(360.0));
                if ui.button("浏览…").clicked() {
                    if let Some(p) = rfd::FileDialog::new().pick_folder() {
                        self.setup_photos = p.to_string_lossy().to_string();
                    }
                }
                ui.end_row();

                ui.label("视频目录：");
                ui.add(egui::TextEdit::singleline(&mut self.setup_videos).desired_width(360.0));
                if ui.button("浏览…").clicked() {
                    if let Some(p) = rfd::FileDialog::new().pick_folder() {
                        self.setup_videos = p.to_string_lossy().to_string();
                    }
                }
                ui.end_row();
            });

        if !self.setup_error.is_empty() {
            ui.add_space(6.0);
            ui.colored_label(RED, &self.setup_error);
        }

        ui.add_space(14.0);
        ui.horizontal(|ui| {
            // 从 Home ⚙ 进入时才显示返回，初始配置时不显示
            if self.config.is_ready() {
                if ui
                    .add_sized(BTN_BACK, egui::Button::new("← 返回"))
                    .clicked()
                {
                    self.screen = Screen::Home;
                }
            }
            if ui
                .add_sized(BTN_WIDE, egui::Button::new("确认并初始化"))
                .clicked()
            {
                self.do_setup();
            }
        });
    }

    fn do_setup(&mut self) {
        let photos = PathBuf::from(self.setup_photos.trim());
        let videos = PathBuf::from(self.setup_videos.trim());

        if !photos.is_dir() {
            self.setup_error = format!("照片目录不存在: {}", photos.display());
            self.log.push(self.setup_error.as_str());
            return;
        }
        if !videos.is_dir() {
            self.setup_error = format!("视频目录不存在: {}", videos.display());
            self.log.push(self.setup_error.as_str());
            return;
        }

        self.config.photos_root = photos;
        self.config.videos_root = videos;

        if let Err(e) = self.config.save() {
            self.setup_error = format!("保存配置失败: {e}");
            return;
        }
        self.log.push("配置已保存");

        let pm = self.config.photos_mirror_db();
        let vm = self.config.videos_mirror_db();
        if let Err(e) = Database::open(&self.config.photos_db(), Some(&pm)) {
            self.setup_error = format!("初始化照片数据库失败: {e}");
            self.log.push(self.setup_error.as_str());
            return;
        }
        self.log.push(format!(
            "照片数据库已创建: {}",
            self.config.photos_db().display()
        ));

        if let Err(e) = Database::open(&self.config.videos_db(), Some(&vm)) {
            self.setup_error = format!("初始化视频数据库失败: {e}");
            self.log.push(self.setup_error.as_str());
            return;
        }
        self.log.push(format!(
            "视频数据库已创建: {}",
            self.config.videos_db().display()
        ));
        self.log.push("初始化完成，进入主界面");

        self.setup_error.clear();
        self.screen = Screen::Home;
    }

    // ─ Home ────────────────────────────────────────────────
    fn ui_home(&mut self, ui: &mut egui::Ui) {
        ui.heading("moving_media");
        ui.add_space(2.0);

        // 状态行
        ui.horizontal(|ui| {
            let dot = |ui: &mut egui::Ui, ok: bool| {
                let (rect, _) =
                    ui.allocate_exact_size(egui::vec2(10.0, 10.0), egui::Sense::hover());
                let color = if ok {
                    egui::Color32::from_rgb(34, 197, 94)
                } else {
                    egui::Color32::from_rgb(239, 68, 68)
                };
                ui.painter().circle_filled(rect.center(), 5.0, color);
            };
            let p = self.config.photos_root.is_dir();
            let v = self.config.videos_root.is_dir();
            dot(ui, p);
            ui.small("Photos");
            ui.add_space(6.0);
            dot(ui, v);
            ui.small("Videos");
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.small_button("⚙").clicked() {
                    self.screen = Screen::Setup;
                }
            });
        });

        if !self.config.photos_root.is_dir() || !self.config.videos_root.is_dir() {
            ui.colored_label(ORANGE, "⚠ 目标目录不可用，请检查外置硬盘。");
        }

        ui.add_space(6.0);
        ui.separator();
        ui.add_space(8.0);

        // 源路径
        ui.label("SD 卡 / 源文件夹路径：");
        ui.horizontal(|ui| {
            ui.add(egui::TextEdit::singleline(&mut self.source_path).desired_width(f32::INFINITY));
            if ui.button("浏览…").clicked() {
                if let Some(path) = rfd::FileDialog::new().pick_folder() {
                    self.source_path = path.to_string_lossy().to_string();
                }
            }
        });

        ui.add_space(12.0);

        let has_src = !self.source_path.is_empty();
        ui.horizontal(|ui| {
            ui.add_enabled_ui(has_src, |ui| {
                if ui
                    .add_sized(BTN_WIDE, egui::Button::new("🔍 扫描检测"))
                    .clicked()
                {
                    self.do_prescan();
                }
                if ui
                    .add_sized(BTN_MED, egui::Button::new("💾 备份"))
                    .clicked()
                {
                    self.do_scan_for_backup();
                }
            });
        });

        ui.add_space(6.0);
        ui.horizontal(|ui| {
            if ui
                .add_sized(BTN_MED, egui::Button::new("✅ 校验文件"))
                .clicked()
            {
                self.screen = Screen::VerifyPick;
            }
            if ui
                .add_sized(BTN_MED, egui::Button::new("📋 备份记录"))
                .clicked()
            {
                self.do_load_records();
            }
            let reindex_label = if self.reindex_running {
                "🔄 索引中…"
            } else {
                "🔄 重建索引"
            };
            ui.add_enabled_ui(!self.reindex_running, |ui| {
                if ui
                    .add_sized(BTN_MED, egui::Button::new(reindex_label))
                    .clicked()
                {
                    self.do_reindex();
                }
            });
        });
    }

    // ─ ScanResult ──────────────────────────────────────────
    fn ui_scan_result(&mut self, ui: &mut egui::Ui) {
        ui.heading("🔍 扫描结果");
        ui.separator();
        ui.add_space(8.0);

        let total = self.scanned_files.len();
        let photos = self
            .scanned_files
            .iter()
            .filter(|f| f.media_type == MediaType::Photo)
            .count();
        let videos = total - photos;

        egui::Grid::new("scan_g")
            .num_columns(2)
            .spacing([16.0, 6.0])
            .show(ui, |ui| {
                ui.label("扫描到：");
                ui.label(format!("{total} 个（{photos} 照片 / {videos} 视频）"));
                ui.end_row();
                ui.label("已备份：");
                ui.colored_label(GREEN, format!("✓ {}", self.prescan_backed_up));
                ui.end_row();
                ui.label("待备份：");
                ui.colored_label(ORANGE, format!("◎ {}", self.prescan_new));
                ui.end_row();
            });

        ui.add_space(14.0);
        ui.horizontal(|ui| {
            if ui
                .add_sized(BTN_BACK, egui::Button::new("← 返回"))
                .clicked()
            {
                self.screen = Screen::Home;
            }
            if self.prescan_new > 0 {
                if ui
                    .add_sized(BTN_WIDE, egui::Button::new("💾 备份新文件"))
                    .clicked()
                {
                    self.enter_naming();
                }
            }
        });
    }

    // ─ DateDecision ────────────────────────────────────────
    fn ui_date_decision(&mut self, ui: &mut egui::Ui) {
        ui.heading("多个拍摄日期");
        ui.separator();
        ui.add_space(8.0);

        let strs: Vec<String> = self
            .unique_dates
            .iter()
            .map(|d| d.format("%Y-%m-%d").to_string())
            .collect();
        ui.label(format!("检测到日期：{}", strs.join("、")));
        ui.add_space(10.0);

        ui.radio_value(
            &mut self.split_mode,
            false,
            "合并 — 日期范围命名，一个文件夹",
        );
        ui.radio_value(&mut self.split_mode, true, "拆分 — 每天单独文件夹");

        ui.add_space(12.0);
        ui.horizontal(|ui| {
            if ui
                .add_sized(BTN_BACK, egui::Button::new("← 返回"))
                .clicked()
            {
                self.screen = Screen::Home;
            }
            if ui
                .add_sized(BTN_MED, egui::Button::new("下一步 →"))
                .clicked()
            {
                if self.split_mode {
                    self.suffix_per_day = vec![String::new(); self.unique_dates.len()];
                    self.screen = Screen::NamingMulti;
                } else {
                    self.suffix_single.clear();
                    self.screen = Screen::NamingSingle;
                }
            }
        });
    }

    // ─ NamingSingle ────────────────────────────────────────
    fn ui_naming_single(&mut self, ui: &mut egui::Ui) {
        ui.heading("文件夹命名");
        ui.separator();
        ui.add_space(8.0);

        ui.label("输入文件夹后缀（例如：新宿、东京、香港）：");
        ui.add(egui::TextEdit::singleline(&mut self.suffix_single).desired_width(280.0));

        let suffix = self.suffix_single.trim().to_string();
        if !suffix.is_empty() {
            let preview = if self.unique_dates.is_empty() {
                format!("unknown_{suffix}")
            } else {
                session_name(&self.unique_dates, &suffix)
            };
            ui.add_space(8.0);
            egui::Frame::none()
                .fill(egui::Color32::from_rgb(40, 44, 52))
                .inner_margin(egui::Margin::symmetric(12.0, 8.0))
                .rounding(egui::Rounding::same(6.0))
                .show(ui, |ui| {
                    ui.monospace(format!("Photos/{preview}/"));
                    ui.monospace(format!("Videos/{preview}/"));
                });
        }

        ui.add_space(12.0);
        ui.horizontal(|ui| {
            if ui
                .add_sized(BTN_BACK, egui::Button::new("← 返回"))
                .clicked()
            {
                self.screen = if self.unique_dates.len() > 1 {
                    Screen::DateDecision
                } else {
                    Screen::Home
                };
            }
            ui.add_enabled_ui(!suffix.is_empty(), |ui| {
                if ui
                    .add_sized(BTN_WIDE, egui::Button::new("开始备份 →"))
                    .clicked()
                {
                    self.do_backup_single();
                }
            });
        });
    }

    // ─ NamingMulti ─────────────────────────────────────────
    fn ui_naming_multi(&mut self, ui: &mut egui::Ui) {
        ui.heading("每天命名");
        ui.separator();
        ui.add_space(8.0);

        egui::Grid::new("nm_grid")
            .num_columns(2)
            .spacing([8.0, 8.0])
            .show(ui, |ui| {
                for (i, date) in self.unique_dates.clone().iter().enumerate() {
                    ui.label(format!("{}：", date.format("%Y-%m-%d")));
                    ui.add(
                        egui::TextEdit::singleline(&mut self.suffix_per_day[i])
                            .desired_width(240.0),
                    );
                    ui.end_row();
                }
            });

        ui.add_space(12.0);
        let ok = self.suffix_per_day.iter().all(|s| !s.trim().is_empty());
        ui.horizontal(|ui| {
            if ui
                .add_sized(BTN_BACK, egui::Button::new("← 返回"))
                .clicked()
            {
                self.screen = Screen::DateDecision;
            }
            ui.add_enabled_ui(ok, |ui| {
                if ui
                    .add_sized(BTN_WIDE, egui::Button::new("开始备份 →"))
                    .clicked()
                {
                    self.do_backup_multi();
                }
            });
        });
    }

    // ─ Running ─────────────────────────────────────────────
    fn ui_running(&mut self, ui: &mut egui::Ui) {
        ui.add_space(40.0);

        let (ratio, label, cur, tot) = {
            let t = self.task.lock().unwrap();
            (t.ratio(), t.label.clone(), t.current, t.total)
        };

        ui.vertical_centered(|ui| {
            ui.heading("处理中…");
            ui.add_space(20.0);

            ui.add(
                egui::ProgressBar::new(ratio)
                    .desired_width(460.0)
                    .animate(true)
                    .text(if tot > 0 {
                        format!("{cur} / {tot}  ({:.0}%)", ratio * 100.0)
                    } else {
                        "准备中…".to_string()
                    }),
            );

            if !label.is_empty() {
                ui.add_space(10.0);
                ui.colored_label(DIM, format!("→ {label}"));
            }
        });
    }

    // ─ Prescanning ─────────────────────────────────────────
    fn ui_prescanning(&mut self, ui: &mut egui::Ui) {
        ui.add_space(40.0);
        let (ratio, cur, tot) = {
            let t = self.task.lock().unwrap();
            (t.ratio(), t.current, t.total)
        };
        ui.vertical_centered(|ui| {
            ui.heading("扫描检测中…");
            ui.add_space(20.0);
            ui.add(
                egui::ProgressBar::new(ratio)
                    .desired_width(460.0)
                    .animate(true)
                    .text(if tot > 0 {
                        format!("{cur} / {tot}")
                    } else {
                        "扫描文件中…".to_string()
                    }),
            );
        });
    }

    // ─ SpotChecking ────────────────────────────────────────
    fn ui_spot_checking(&mut self, ui: &mut egui::Ui) {
        ui.add_space(40.0);
        let (ratio, label, cur, tot) = {
            let t = self.task.lock().unwrap();
            (t.ratio(), t.label.clone(), t.current, t.total)
        };
        ui.vertical_centered(|ui| {
            ui.heading("抽检中…");
            ui.add_space(6.0);
            ui.colored_label(DIM, "递归检测修改时间有变化的文件夹，全部文件重新哈希校验");
            ui.add_space(20.0);
            ui.add(
                egui::ProgressBar::new(ratio)
                    .desired_width(460.0)
                    .animate(true)
                    .text(if tot > 0 {
                        format!("{cur} / {tot}")
                    } else {
                        "准备中…".to_string()
                    }),
            );
            if !label.is_empty() {
                ui.add_space(10.0);
                ui.colored_label(DIM, format!("→ {label}"));
            }
        });
    }

    // ─ SpotCheckDone ───────────────────────────────────────
    fn ui_spot_check_done(&mut self, ui: &mut egui::Ui) {
        ui.add_space(8.0);
        ui.heading("校验完成");
        ui.separator();
        ui.add_space(8.0);

        // 先显示备份结果摘要
        {
            let t = self.task.lock().unwrap();
            if let TaskResult::BackupWithVerify { ref summary, .. } = t.result {
                for line in summary.lines() {
                    if line.starts_with("✓") {
                        ui.colored_label(GREEN, line);
                    } else if line.starts_with("✗") {
                        ui.colored_label(RED, line);
                    } else {
                        ui.label(line);
                    }
                }
                ui.add_space(8.0);
                ui.separator();
                ui.add_space(6.0);
            }
        }

        if let Some(ref sc) = self.spot_result {
            if sc.checked == 0 {
                ui.colored_label(DIM, "无新文件，跳过校验");
            } else if sc.mismatches.is_empty() {
                ui.colored_label(
                    GREEN,
                    format!(
                        "✅ 全量校验通过：{} 个文件（SD卡 + 磁盘）全部一致",
                        sc.checked
                    ),
                );
            } else {
                ui.colored_label(RED, format!("⚠ 发现 {} 个问题：", sc.mismatches.len()));
                ui.add_space(4.0);
                egui::ScrollArea::vertical()
                    .max_height(120.0)
                    .show(ui, |ui| {
                        for m in &sc.mismatches {
                            ui.colored_label(RED, m);
                        }
                    });
            }
        }

        ui.add_space(14.0);
        if ui
            .add_sized(BTN_WIDE, egui::Button::new("← 返回首页"))
            .clicked()
        {
            self.screen = Screen::Home;
        }
    }

    // ─ Done ────────────────────────────────────────────────
    fn ui_done(&mut self, ui: &mut egui::Ui) {
        ui.add_space(8.0);
        ui.heading("备份完成");
        ui.separator();
        ui.add_space(8.0);

        {
            let t = self.task.lock().unwrap();
            let (summary, errors) = match &t.result {
                TaskResult::BackupWithVerify { ref summary } => (summary.as_str(), [].as_slice()),
                TaskResult::Error(msg) => (msg.as_str(), [].as_slice()),
                _ => ("", [].as_slice()),
            };

            for line in summary.lines() {
                if line.starts_with("✓") {
                    ui.colored_label(GREEN, line);
                } else if line.starts_with("✗") {
                    ui.colored_label(RED, line);
                } else {
                    ui.label(line);
                }
            }

            if !errors.is_empty() {
                ui.add_space(10.0);
                let errors: Vec<String> = errors.to_vec();
                ui.collapsing(format!("错误详情（{}）", errors.len()), |ui| {
                    egui::ScrollArea::vertical()
                        .max_height(100.0)
                        .show(ui, |ui| {
                            for e in &errors {
                                ui.colored_label(RED, e);
                            }
                        });
                });
            }
        }

        ui.add_space(14.0);
        if ui
            .add_sized(BTN_WIDE, egui::Button::new("← 返回首页"))
            .clicked()
        {
            self.screen = Screen::Home;
        }
    }

    // ─ VerifyPick ──────────────────────────────────────────
    fn ui_verify_pick(&mut self, ui: &mut egui::Ui) {
        ui.heading("校验已备份文件");
        ui.separator();
        ui.add_space(8.0);

        ui.label("校验目的文件夹：扫描磁盘所有子文件夹，与数据库交叉比对。");
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            if ui
                .add_sized(BTN_WIDE, egui::Button::new("校验照片文件夹"))
                .clicked()
            {
                self.do_verify(true);
            }
            if ui
                .add_sized(BTN_WIDE, egui::Button::new("校验视频文件夹"))
                .clicked()
            {
                self.do_verify(false);
            }
        });

        ui.add_space(12.0);
        ui.label("校验数据库：仅检查数据库记录对应的文件是否完好。");
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            if ui
                .add_sized(BTN_WIDE, egui::Button::new("校验照片 DB"))
                .clicked()
            {
                self.do_verify_db(true);
            }
            if ui
                .add_sized(BTN_WIDE, egui::Button::new("校验视频 DB"))
                .clicked()
            {
                self.do_verify_db(false);
            }
        });

        ui.add_space(10.0);
        if ui
            .add_sized(BTN_BACK, egui::Button::new("← 返回"))
            .clicked()
        {
            self.screen = Screen::Home;
        }
    }

    // ─ VerifyDone ──────────────────────────────────────────
    fn ui_verify_done(&mut self, ui: &mut egui::Ui) {
        ui.heading("校验结果");
        ui.separator();
        ui.add_space(8.0);

        if self.verify_mismatches.is_empty() {
            ui.colored_label(GREEN, "✓ 所有文件校验通过，无损坏。");
        } else {
            ui.colored_label(
                RED,
                format!("✗ 发现 {} 个问题：", self.verify_mismatches.len()),
            );
            ui.add_space(6.0);
            egui::ScrollArea::vertical()
                .max_height(180.0)
                .show(ui, |ui| {
                    for msg in &self.verify_mismatches {
                        ui.colored_label(RED, msg);
                    }
                });
        }
        ui.add_space(12.0);
        if ui.button("← 返回首页").clicked() {
            self.screen = Screen::Home;
        }
    }

    // ─ ListRecords ─────────────────────────────────────────
    fn ui_list_records(&mut self, ui: &mut egui::Ui) {
        ui.heading("📋 备份记录");
        ui.separator();
        ui.add_space(4.0);

        if self.list_records.is_empty() {
            ui.label("暂无记录。");
        } else {
            ui.small(format!("共 {} 条", self.list_records.len()));
            ui.add_space(4.0);
            egui::ScrollArea::vertical().show(ui, |ui| {
                egui::Grid::new("rec_grid")
                    .num_columns(4)
                    .striped(true)
                    .spacing([14.0, 4.0])
                    .show(ui, |ui| {
                        ui.strong("Session");
                        ui.strong("文件名");
                        ui.strong("大小");
                        ui.strong("时间");
                        ui.end_row();
                        for r in &self.list_records {
                            ui.label(&r.session_name);
                            ui.label(&r.filename);
                            ui.label(format!("{:.1} MB", r.file_size as f64 / 1_048_576.0));
                            ui.label(r.backed_up_at.get(..10).unwrap_or("-"));
                            ui.end_row();
                        }
                    });
            });
        }
        ui.add_space(8.0);
        if ui
            .add_sized(BTN_BACK, egui::Button::new("← 返回"))
            .clicked()
        {
            self.screen = Screen::Home;
        }
    }

    // ── 业务逻辑 ───────────────────────────────────────────

    fn enter_naming(&mut self) {
        self.unique_dates = unique_dates(&self.scanned_files);
        if self.unique_dates.len() > 1 {
            self.suffix_per_day = vec![String::new(); self.unique_dates.len()];
            self.screen = Screen::DateDecision;
        } else {
            self.suffix_single.clear();
            self.screen = Screen::NamingSingle;
        }
    }

    fn do_prescan(&mut self) {
        self.log.push(format!("开始扫描: {}", self.source_path));
        let source = PathBuf::from(&self.source_path);
        self.config.last_source = Some(source.clone());
        let _ = self.config.save();

        let pd = self.config.photos_db();
        let pm = self.config.photos_mirror_db();
        let vd = self.config.videos_db();
        let vm = self.config.videos_mirror_db();
        let task = self.task.clone();
        let log = self.log.clone();
        {
            *task.lock().unwrap() = TaskState::default();
        }

        std::thread::spawn(move || {
            let files = match scan_source(&source) {
                Err(e) => {
                    log.push(format!("扫描失败: {e}"));
                    let mut t = task.lock().unwrap();
                    t.result = TaskResult::Error(format!("扫描失败: {e}"));
                    t.done = true;
                    return;
                }
                Ok(f) => {
                    log.push(format!("扫描到 {} 个媒体文件", f.len()));
                    f
                }
            };

            log.push("正在计算哈希并对比数据库…");
            let photos_db = Database::open(&pd, Some(&pm))
                .ok()
                .or_else(|| Database::open_readonly(&pm).ok());
            let videos_db = Database::open(&vd, Some(&vm))
                .ok()
                .or_else(|| Database::open_readonly(&vm).ok());

            let total = files.len();
            {
                let mut t = task.lock().unwrap();
                t.total = total;
            }

            // 并行哈希（rayon），然后串行查询数据库
            let progress = Arc::new(AtomicUsize::new(0));
            let hashes: Vec<Option<String>> = files
                .par_iter()
                .map({
                    let progress = progress.clone();
                    let task = task.clone();
                    move |file| {
                        let hash = crate::hash::hash_file(&file.path).ok();
                        let cur = progress.fetch_add(1, Ordering::Relaxed) + 1;
                        let mut t = task.lock().unwrap();
                        t.current = cur;
                        hash
                    }
                })
                .collect();

            let mut backed = 0usize;
            let mut new = 0usize;
            for (file, hash) in files.iter().zip(hashes.iter()) {
                if let Some(hash) = hash {
                    let db = match file.media_type {
                        MediaType::Photo => &photos_db,
                        MediaType::Video => &videos_db,
                    };
                    let found = db
                        .as_ref()
                        .and_then(|d| d.find_by_hash(hash).ok().flatten())
                        .is_some();
                    if found {
                        backed += 1;
                    } else {
                        new += 1;
                    }
                }
            }

            let mut t = task.lock().unwrap();
            t.result = TaskResult::Prescan {
                files,
                backed_up: backed,
                new_count: new,
            };
            t.done = true;
        });

        self.screen = Screen::Prescanning;
    }

    fn do_scan_for_backup(&mut self) {
        self.log.push(format!("开始扫描: {}", self.source_path));
        let source = PathBuf::from(&self.source_path);
        self.config.last_source = Some(source.clone());
        let _ = self.config.save();
        match scan_source(&source) {
            Err(e) => {
                self.log.push(format!("扫描失败: {e}"));
                let mut t = self.task.lock().unwrap();
                t.result = TaskResult::Error(format!("扫描失败: {e}"));
                t.done = true;
                self.screen = Screen::Done;
            }
            Ok(files) => {
                self.log.push(format!("扫描到 {} 个媒体文件", files.len()));
                self.unique_dates = unique_dates(&files);
                self.scanned_files = files;
                self.enter_naming();
            }
        }
    }

    fn do_backup_single(&mut self) {
        let session = session_name(&self.unique_dates, self.suffix_single.trim());
        self.log.push(format!("备份到: {session}"));
        let files = self.scanned_files.clone();
        self.spawn_backup(move |_| session.clone(), files);
    }

    fn do_backup_multi(&mut self) {
        let ds: Vec<(NaiveDate, String)> = self
            .unique_dates
            .iter()
            .zip(self.suffix_per_day.iter())
            .map(|(d, s)| (*d, session_name(&[*d], s.trim())))
            .collect();
        for (_, s) in &ds {
            self.log.push(format!("备份到: {s}"));
        }
        let files = self.scanned_files.clone();
        self.spawn_backup(
            move |file| {
                if let Some(date) = file.capture_date {
                    ds.iter()
                        .find(|(d, _)| *d == date)
                        .map(|(_, s)| s.clone())
                        .unwrap_or_else(|| ds.last().unwrap().1.clone())
                } else {
                    ds.last().unwrap().1.clone()
                }
            },
            files,
        );
    }

    fn spawn_backup<F: Fn(&MediaFile) -> String + Send + 'static>(
        &mut self,
        session_for: F,
        files: Vec<MediaFile>,
    ) {
        let pr = self.config.photos_root.clone();
        let vr = self.config.videos_root.clone();
        let pd = self.config.photos_db();
        let vd = self.config.videos_db();
        let pm = self.config.photos_mirror_db();
        let vm = self.config.videos_mirror_db();
        let task = self.task.clone();
        let log = self.log.clone();
        let arc = self.spot_result_arc.clone();

        {
            let mut t = task.lock().unwrap();
            *t = TaskState {
                total: files.len(),
                ..Default::default()
            };
        }

        std::thread::spawn(move || {
            let mut pg: std::collections::HashMap<String, Vec<MediaFile>> =
                std::collections::HashMap::new();
            let mut vg: std::collections::HashMap<String, Vec<MediaFile>> =
                std::collections::HashMap::new();
            for f in &files {
                let s = session_for(f);
                match f.media_type {
                    MediaType::Photo => pg.entry(s).or_default().push(f.clone()),
                    MediaType::Video => vg.entry(s).or_default().push(f.clone()),
                }
            }

            let mut pdb = match Database::open(&pd, Some(&pm)) {
                Ok(d) => d,
                Err(e) => {
                    let mut t = task.lock().unwrap();
                    t.result = TaskResult::Error(format!("照片数据库错误: {e}"));
                    t.done = true;
                    return;
                }
            };
            let mut vdb = match Database::open(&vd, Some(&vm)) {
                Ok(d) => d,
                Err(e) => {
                    let mut t = task.lock().unwrap();
                    t.result = TaskResult::Error(format!("视频数据库错误: {e}"));
                    t.done = true;
                    return;
                }
            };

            // Defer mirror sync during backup — flush once at end
            pdb.set_mirror_deferred(true);
            vdb.set_mirror_deferred(true);

            let mut copied = 0usize;
            let mut skipped = 0usize;
            let mut failed = 0usize;
            let mut errs: Vec<String> = Vec::new();
            let mut processed = 0usize;
            let mut all_backed_up: Vec<crate::backup::BackedUpFile> = Vec::new();

            let groups: Vec<(String, Vec<MediaFile>)> =
                pg.into_iter().chain(vg.into_iter()).collect();

            for (session, group) in &groups {
                log.push(format!("处理 session: {session} ({} 个文件)", group.len()));
                let r = backup_files(
                    group,
                    session,
                    &pr,
                    &vr,
                    &mut pdb,
                    &mut vdb,
                    &mut |cur, _| {
                        processed += 1;
                        let fname = group.get(cur).map(|f| f.filename.as_str()).unwrap_or("");
                        let mut t = task.lock().unwrap();
                        t.current = processed;
                        t.label = fname.to_string();
                    },
                );
                if r.copied > 0 {
                    log.push(format!("  复制 {} 个文件", r.copied));
                }
                if r.skipped > 0 {
                    log.push(format!("  跳过 {} 个（已存在）", r.skipped));
                }
                if r.failed > 0 {
                    log.push(format!("  失败 {} 个", r.failed));
                }
                copied += r.copied;
                skipped += r.skipped;
                failed += r.failed;
                errs.extend(r.errors);
                all_backed_up.extend(r.backed_up);
            }

            // Flush deferred mirror syncs
            if let Err(e) = pdb.flush_mirror() {
                log.push(format!("照片镜像同步失败: {e}"));
            }
            if let Err(e) = vdb.flush_mirror() {
                log.push(format!("视频镜像同步失败: {e}"));
            }

            let backup_summary =
                format!("备份完成\n✓ 已复制: {copied}\n↷ 跳过(重复): {skipped}\n✗ 失败: {failed}");

            // Full post-backup verification: re-hash every copied file on both SD card and disk.
            let verify_total = all_backed_up.len();
            let mismatches = if verify_total > 0 {
                log.push(format!(
                    "开始全量校验 {verify_total} 个文件（SD卡 + 磁盘各一遍）…"
                ));
                {
                    let mut t = task.lock().unwrap();
                    t.current = 0;
                    t.total = verify_total * 2;
                    t.label = String::new();
                }
                verify_backup(&all_backed_up, &mut |cur, tot| {
                    let idx = cur / 2;
                    let side = if cur % 2 == 0 { "SD卡" } else { "磁盘" };
                    let fname = all_backed_up
                        .get(idx)
                        .and_then(|f| f.source.file_name())
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_default();
                    let mut t = task.lock().unwrap();
                    t.current = cur;
                    t.total = tot;
                    t.label = format!("[{side}] {fname}");
                })
            } else {
                Vec::new()
            };

            let spot = crate::backup::SpotCheckResult {
                checked: verify_total,
                sessions_covered: groups.len(),
                mismatches,
            };
            *arc.lock().unwrap() = Some(spot);

            let mut t = task.lock().unwrap();
            // Log any errors that occurred during backup
            if !errs.is_empty() {
                for e in &errs {
                    log.push(format!("  错误: {e}"));
                }
            }

            t.result = TaskResult::BackupWithVerify {
                summary: backup_summary,
            };
            t.done = true;
        });

        self.screen = Screen::Running;
    }

    fn do_verify(&mut self, photos: bool) {
        let label = if photos { "照片库" } else { "视频库" };
        self.log.push(format!("开始校验{label}…"));

        let (db_path, mirror, root) = if photos {
            (
                self.config.photos_db(),
                self.config.photos_mirror_db(),
                self.config.photos_root.clone(),
            )
        } else {
            (
                self.config.videos_db(),
                self.config.videos_mirror_db(),
                self.config.videos_root.clone(),
            )
        };

        let task = self.task.clone();
        let log = self.log.clone();
        {
            *task.lock().unwrap() = TaskState::default();
        }

        std::thread::spawn(move || {
            let mut db = match Database::open(&db_path, Some(&mirror)) {
                Ok(d) => d,
                Err(e) => {
                    let mut t = task.lock().unwrap();
                    t.result = TaskResult::Error(format!("数据库错误: {e}"));
                    t.done = true;
                    return;
                }
            };

            log.push("开始扫描目录并校验…".to_string());

            match verify_files(&mut db, &root, &mut |cur, tot| {
                let mut t = task.lock().unwrap();
                t.current = cur;
                t.total = tot;
            }) {
                Ok(mm) => {
                    if mm.is_empty() {
                        log.push("校验通过，无异常");
                    } else {
                        log.push(format!("发现 {} 个异常", mm.len()));
                    }
                    let mut t = task.lock().unwrap();
                    t.result = TaskResult::Verify { errors: mm };
                    t.done = true;
                }
                Err(e) => {
                    log.push(format!("校验出错: {e}"));
                    let mut t = task.lock().unwrap();
                    t.result = TaskResult::Error(format!("校验失败: {e}"));
                    t.done = true;
                }
            }
        });

        self.screen = Screen::Running;
    }

    fn do_verify_db(&mut self, photos: bool) {
        let label = if photos { "照片 DB" } else { "视频 DB" };
        self.log.push(format!("开始校验{label}…"));

        let (db_path, mirror, root) = if photos {
            (
                self.config.photos_db(),
                self.config.photos_mirror_db(),
                self.config.photos_root.clone(),
            )
        } else {
            (
                self.config.videos_db(),
                self.config.videos_mirror_db(),
                self.config.videos_root.clone(),
            )
        };

        let task = self.task.clone();
        let log = self.log.clone();
        {
            *task.lock().unwrap() = TaskState::default();
        }

        std::thread::spawn(move || {
            let mut db = match Database::open(&db_path, Some(&mirror)) {
                Ok(d) => d,
                Err(e) => {
                    let mut t = task.lock().unwrap();
                    t.result = TaskResult::Error(format!("数据库错误: {e}"));
                    t.done = true;
                    return;
                }
            };

            let cnt = db.list_all().map(|r| r.len()).unwrap_or(0);
            {
                let mut t = task.lock().unwrap();
                t.total = cnt;
            }
            log.push(format!("共 {cnt} 条记录需要校验"));

            match verify_db_records(&mut db, &root, &mut |cur, tot| {
                let mut t = task.lock().unwrap();
                t.current = cur;
                t.total = tot;
            }) {
                Ok(mm) => {
                    if mm.is_empty() {
                        log.push("校验通过，无异常");
                    } else {
                        log.push(format!("发现 {} 个异常", mm.len()));
                    }
                    let mut t = task.lock().unwrap();
                    t.result = TaskResult::Verify { errors: mm };
                    t.done = true;
                }
                Err(e) => {
                    log.push(format!("校验出错: {e}"));
                    let mut t = task.lock().unwrap();
                    t.result = TaskResult::Error(format!("校验失败: {e}"));
                    t.done = true;
                }
            }
        });

        self.screen = Screen::Running;
    }

    fn do_load_records(&mut self) {
        self.log.push("加载备份记录…");
        let p = self.config.photos_db();
        let m = self.config.photos_mirror_db();
        self.list_records = Database::open(&p, Some(&m))
            .or_else(|_| Database::open_readonly(&m))
            .and_then(|db| db.list_all())
            .unwrap_or_default();
        self.log
            .push(format!("加载到 {} 条记录", self.list_records.len()));
        self.screen = Screen::ListRecords;
    }

    fn do_reindex(&mut self) {
        if self.reindex_running {
            self.log.push("重建索引正在进行中，请等待完成");
            return;
        }
        self.log.push("开始重建索引：扫描备份目录…");
        let photos_root = self.config.photos_root.clone();
        let videos_root = self.config.videos_root.clone();
        let pd = self.config.photos_db();
        let pm = self.config.photos_mirror_db();
        let vd = self.config.videos_db();
        let vm = self.config.videos_mirror_db();
        let checkpoint = self.config.reindex_checkpoint();
        let task = self.reindex_task.clone();
        let log = self.log.clone();
        {
            *task.lock().unwrap() = TaskState::default();
        }
        self.reindex_running = true;
        self.reindex_result_msg.clear();

        std::thread::spawn(move || {
            let mut photos_db = match Database::open(&pd, Some(&pm)) {
                Ok(d) => d,
                Err(e) => {
                    let mut t = task.lock().unwrap();
                    t.result = TaskResult::Error(format!("数据库错误: {e}"));
                    t.done = true;
                    return;
                }
            };
            let mut videos_db = match Database::open(&vd, Some(&vm)) {
                Ok(d) => d,
                Err(e) => {
                    let mut t = task.lock().unwrap();
                    t.result = TaskResult::Error(format!("数据库错误: {e}"));
                    t.done = true;
                    return;
                }
            };

            // Collect all media files from both roots.
            let mut candidates: Vec<(std::path::PathBuf, bool)> = Vec::new(); // (path, is_photo)
            for (root, is_photo) in [(&photos_root, true), (&videos_root, false)] {
                for entry in walkdir::WalkDir::new(root)
                    .follow_links(false)
                    .into_iter()
                    .filter_map(|e| e.ok())
                {
                    if !entry.file_type().is_file() {
                        continue;
                    }
                    let path = entry.path().to_path_buf();
                    let ext = path
                        .extension()
                        .and_then(|x| x.to_str())
                        .map(|x| x.to_lowercase())
                        .unwrap_or_default();
                    let is_media = crate::scanner::PHOTO_EXTS.contains(&ext.as_str())
                        || crate::scanner::VIDEO_EXTS.contains(&ext.as_str());
                    if is_media {
                        candidates.push((path, is_photo));
                    }
                }
            }
            candidates.sort_by(|a, b| a.0.cmp(&b.0));

            let resume_from = Config::load_reindex_checkpoint(&checkpoint);
            let start_idx = resume_from
                .as_ref()
                .and_then(|last_path| {
                    candidates
                        .iter()
                        .position(|(path, _)| path.to_string_lossy() == last_path.as_str())
                })
                .map(|idx| idx + 1)
                .unwrap_or(0);

            let total = candidates.len();
            {
                let mut t = task.lock().unwrap();
                t.total = total;
            }
            if let Some(last_path) = &resume_from {
                log.push(format!(
                    "发现 {total} 个媒体文件，从检查点继续：{last_path}"
                ));
            } else {
                log.push(format!(
                    "发现 {total} 个媒体文件，逐个哈希、每 64 条批量写入…"
                ));
            }

            {
                let mut t = task.lock().unwrap();
                t.current = start_idx;
                t.total = total;
            }

            // 4 个工作线程并行哈希，主线程收结果攒满 64 条后批量写入 DB
            let mut batch: Vec<(std::path::PathBuf, bool, String)> = Vec::with_capacity(64);
            let mut imported = 0usize;
            let mut skipped = 0usize;

            let remaining = &candidates[start_idx..];
            let work_idx = Arc::new(AtomicUsize::new(0));
            let (tx, rx) = std::sync::mpsc::sync_channel::<(
                std::path::PathBuf,
                bool,
                Result<String, String>,
            )>(4);

            let workers: Vec<_> = (0..4)
                .map(|_| {
                    let work_idx = work_idx.clone();
                    let tx = tx.clone();
                    let remaining: Vec<(PathBuf, bool)> = remaining.to_vec();
                    std::thread::spawn(move || loop {
                        let idx = work_idx.fetch_add(1, Ordering::Relaxed);
                        if idx >= remaining.len() {
                            break;
                        }
                        let (ref path, is_photo) = remaining[idx];
                        let result = crate::hash::hash_file(path)
                            .map_err(|e| format!("哈希失败: {} — {e}", path.display()));
                        if tx.send((path.clone(), is_photo, result)).is_err() {
                            break;
                        }
                    })
                })
                .collect();
            drop(tx); // 关闭发送端，rx 遍历结束后自动退出

            let mut processed = start_idx;
            let mut last_path_for_checkpoint = String::new();

            for (path, is_photo, result) in rx {
                processed += 1;
                {
                    let mut t = task.lock().unwrap();
                    t.current = processed;
                }

                match result {
                    Ok(hash) => {
                        last_path_for_checkpoint = path.to_string_lossy().to_string();
                        batch.push((path, is_photo, hash));
                    }
                    Err(msg) => log.push(msg),
                }

                if batch.len() >= 64 {
                    if let Err(e) = reindex_flush(
                        &mut batch,
                        &mut photos_db,
                        &mut videos_db,
                        &photos_root,
                        &videos_root,
                        &mut imported,
                        &mut skipped,
                        &log,
                    ) {
                        let mut t = task.lock().unwrap();
                        t.result = TaskResult::Error(e);
                        t.done = true;
                        // 等待工作线程结束
                        for w in workers {
                            let _ = w.join();
                        }
                        return;
                    }
                    if !last_path_for_checkpoint.is_empty() {
                        if let Err(e) =
                            Config::save_reindex_checkpoint(&checkpoint, &last_path_for_checkpoint)
                        {
                            log.push(format!("保存断点失败: {e}"));
                        }
                    }
                }
            }

            for w in workers {
                let _ = w.join();
            }

            // 收尾：写入剩余不足 64 条的记录
            if let Err(e) = reindex_flush(
                &mut batch,
                &mut photos_db,
                &mut videos_db,
                &photos_root,
                &videos_root,
                &mut imported,
                &mut skipped,
                &log,
            ) {
                let mut t = task.lock().unwrap();
                t.result = TaskResult::Error(e);
                t.done = true;
                return;
            }

            Config::clear_reindex_checkpoint(&checkpoint);
            {
                let mut t = task.lock().unwrap();
                t.current = total;
            }
            log.push(format!(
                "重建索引完成：新增 {imported} 条，跳过 {skipped} 条（已在库中）"
            ));
            let mut t = task.lock().unwrap();
            t.result = TaskResult::Reindex {
                summary: format!("新增 {imported} 条记录，跳过 {skipped} 条"),
            };
            t.done = true;
        });
    }

    fn open_db(&self, photos: bool) -> Option<Database> {
        let (p, m) = if photos {
            (self.config.photos_db(), self.config.photos_mirror_db())
        } else {
            (self.config.videos_db(), self.config.videos_mirror_db())
        };
        Database::open(&p, Some(&m))
            .ok()
            .or_else(|| Database::open_readonly(&m).ok())
    }

    fn spawn_spot_check(&mut self) {
        let pd = self.config.photos_db();
        let pm = self.config.photos_mirror_db();
        let pr = self.config.photos_root.clone();
        let vd = self.config.videos_db();
        let vm = self.config.videos_mirror_db();
        let vr = self.config.videos_root.clone();
        let task = self.task.clone();
        let log = self.log.clone();
        let arc = self.spot_result_arc.clone();

        {
            *task.lock().unwrap() = TaskState::default();
        }

        std::thread::spawn(move || {
            let pdb = Database::open(&pd, Some(&pm))
                .ok()
                .or_else(|| Database::open_readonly(&pm).ok());
            let vdb = Database::open(&vd, Some(&vm))
                .ok()
                .or_else(|| Database::open_readonly(&vm).ok());

            let mut all_checked = 0usize;
            let mut all_sessions = 0usize;
            let mut all_mismatches: Vec<String> = Vec::new();

            if let Some(mut db) = pdb {
                match spot_check(&mut db, &pr, &mut |checked, total| {
                    let mut t = task.lock().unwrap();
                    t.current = all_checked + checked;
                    t.total = all_checked + total;
                }) {
                    Ok(r) => {
                        log.push(format!(
                            "照片库: {} 个文件夹有变化，校验 {} 个文件",
                            r.sessions_covered, r.checked
                        ));
                        all_checked += r.checked;
                        all_sessions += r.sessions_covered;
                        all_mismatches.extend(r.mismatches);
                    }
                    Err(e) => log.push(format!("照片库抽检失败: {e}")),
                }
            }

            if let Some(mut db) = vdb {
                match spot_check(&mut db, &vr, &mut |checked, total| {
                    let mut t = task.lock().unwrap();
                    t.current = all_checked + checked;
                    t.total = all_checked + total;
                }) {
                    Ok(r) => {
                        log.push(format!(
                            "视频库: {} 个文件夹有变化，校验 {} 个文件",
                            r.sessions_covered, r.checked
                        ));
                        all_checked += r.checked;
                        all_sessions += r.sessions_covered;
                        all_mismatches.extend(r.mismatches);
                    }
                    Err(e) => log.push(format!("视频库抽检失败: {e}")),
                }
            }

            let summary = if all_mismatches.is_empty() {
                format!(
                    "抽检通过：{} 个有变化的文件夹，共校验 {} 个文件，全部正常",
                    all_sessions, all_checked
                )
            } else {
                format!(
                    "⚠ 发现 {} 个问题（{} 个文件夹有变化）",
                    all_mismatches.len(),
                    all_sessions
                )
            };

            *arc.lock().unwrap() = Some(crate::backup::SpotCheckResult {
                checked: all_checked,
                sessions_covered: all_sessions,
                mismatches: all_mismatches,
            });

            log.push(&summary);
            let mut t = task.lock().unwrap();
            t.done = true;
        });

        self.screen = Screen::SpotChecking;
    }
}

/// 将一批已哈希的文件写入数据库，清空 batch。
fn reindex_flush(
    batch: &mut Vec<(std::path::PathBuf, bool, String)>,
    photos_db: &mut Database,
    videos_db: &mut Database,
    photos_root: &std::path::Path,
    videos_root: &std::path::Path,
    imported: &mut usize,
    skipped: &mut usize,
    log: &Log,
) -> Result<(), String> {
    photos_db
        .begin_bulk()
        .map_err(|e| format!("数据库错误: {e}"))?;
    videos_db.begin_bulk().map_err(|e| {
        let _ = photos_db.rollback_bulk();
        format!("数据库错误: {e}")
    })?;

    for (path, is_photo, hash) in batch.iter() {
        let db: &Database = if *is_photo { photos_db } else { videos_db };
        let root = if *is_photo { photos_root } else { videos_root };

        let session = path
            .strip_prefix(root)
            .ok()
            .and_then(|rel| rel.components().next())
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .unwrap_or_else(|| "imported".to_string());

        let filename = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let rel_path = path
            .strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string();
        let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);

        match db.insert_file_if_missing_bulk(&filename, &rel_path, hash, file_size, None, &session)
        {
            Ok(true) => *imported += 1,
            Ok(false) => *skipped += 1,
            Err(e) => log.push(format!("插入失败: {filename} — {e}")),
        }
    }

    photos_db.commit_bulk().map_err(|e| {
        let _ = videos_db.rollback_bulk();
        format!("数据库错误: {e}")
    })?;
    videos_db
        .commit_bulk()
        .map_err(|e| format!("数据库错误: {e}"))?;
    batch.clear();
    Ok(())
}
