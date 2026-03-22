fn main() {
    // 全局 panic 处理：写入日志文件而不是直接崩溃
    std::panic::set_hook(Box::new(|info| {
        let msg = format!("程序异常: {info}");
        eprintln!("{msg}");
        let log_path = moving_media::config::data_base_dir().join("crash.log");
        if let Some(parent) = log_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let ts = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
        let entry = format!("[{ts}] {msg}\n");
        let _ = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .and_then(|mut f| std::io::Write::write_all(&mut f, entry.as_bytes()));
    }));

    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_title("moving_media")
            .with_inner_size([680.0, 560.0]),
        ..Default::default()
    };

    if let Err(e) = eframe::run_native(
        "moving_media",
        options,
        Box::new(|cc| Ok(Box::new(moving_media::App::new(cc)))),
    ) {
        eprintln!("启动失败: {e}");
        std::process::exit(1);
    }
}
