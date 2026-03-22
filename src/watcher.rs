use rayon::prelude::*;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::db::Database;
use crate::hash::hash_file;
use crate::scanner::{scan_source, MediaType};

/// 检测到储存卡后的扫描结果
#[derive(Clone, Debug)]
pub struct CardAlert {
    pub volume_path: PathBuf,
    pub volume_name: String,
    pub total: usize,
    pub photos_new: usize,
    pub videos_new: usize,
    pub photos_backed_up: usize,
    pub videos_backed_up: usize,
    /// 正在后台扫描中（哈希计算未完成）
    pub scanning: bool,
}

impl CardAlert {
    pub fn all_backed_up(&self) -> bool {
        !self.scanning && self.photos_new == 0 && self.videos_new == 0
    }

    pub fn new_total(&self) -> usize {
        self.photos_new + self.videos_new
    }
}

/// 启动后台线程：每 2 秒轮询挂载卷，发现新卷时自动扫描。
///
/// - `alert`：扫描结果写入此处，UI 线程轮询读取
/// - `ctx`：触发 egui 重绘
pub fn start_watcher(
    photos_db_path: PathBuf,
    videos_db_path: PathBuf,
    photos_mirror: PathBuf,
    videos_mirror: PathBuf,
    alert: Arc<Mutex<(Option<CardAlert>, u64)>>,
    ctx: eframe::egui::Context,
) {
    std::thread::spawn(move || {
        let mut known: HashSet<PathBuf> = list_volumes().into_iter().collect();

        loop {
            std::thread::sleep(Duration::from_secs(2));

            let current: HashSet<PathBuf> = list_volumes().into_iter().collect();
            let new_vols: Vec<PathBuf> = current.difference(&known).cloned().collect();
            known = current;

            for vol in new_vols {
                let name = vol.file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| vol.to_string_lossy().to_string());

                // 先快速上报"扫描中"状态
                {
                    let mut a = alert.lock().unwrap();
                    a.0 = Some(CardAlert {
                        volume_path: vol.to_path_buf(),
                        volume_name: name.clone(),
                        total: 0,
                        photos_new: 0,
                        videos_new: 0,
                        photos_backed_up: 0,
                        videos_backed_up: 0,
                        scanning: true,
                    });
                    a.1 += 1;
                }
                ctx.request_repaint();

                // 扫描并计算哈希
                let result = scan_and_check(
                    &vol,
                    &name,
                    &photos_db_path,
                    &videos_db_path,
                    &photos_mirror,
                    &videos_mirror,
                );

                {
                    let mut a = alert.lock().unwrap();
                    a.0 = Some(result);
                    a.1 += 1;
                }
                ctx.request_repaint();
            }
        }
    });
}

fn scan_and_check(
    vol: &std::path::Path,
    name: &str,
    photos_db_path: &std::path::Path,
    videos_db_path: &std::path::Path,
    photos_mirror: &std::path::Path,
    videos_mirror: &std::path::Path,
) -> CardAlert {
    let files = match scan_source(vol) {
        Ok(f) => f,
        Err(_) => {
            return CardAlert {
                volume_path: vol.to_path_buf(),
                volume_name: name.to_string(),
                total: 0,
                photos_new: 0,
                videos_new: 0,
                photos_backed_up: 0,
                videos_backed_up: 0,
                scanning: false,
            };
        }
    };

    let photos_db = Database::open(photos_db_path, Some(photos_mirror)).ok()
        .or_else(|| Database::open_readonly(photos_mirror).ok());
    let videos_db = Database::open(videos_db_path, Some(videos_mirror)).ok()
        .or_else(|| Database::open_readonly(videos_mirror).ok());

    // 并行哈希
    let hashes: Vec<Option<String>> = files.par_iter()
        .map(|file| hash_file(&file.path).ok())
        .collect();

    // 串行查询数据库
    let mut photos_new = 0usize;
    let mut videos_new = 0usize;
    let mut photos_backed_up = 0usize;
    let mut videos_backed_up = 0usize;

    for (file, hash) in files.iter().zip(hashes.iter()) {
        let Some(hash) = hash else { continue };
        let (db, new_count, backed_count) = match file.media_type {
            MediaType::Photo => (&photos_db, &mut photos_new, &mut photos_backed_up),
            MediaType::Video => (&videos_db, &mut videos_new, &mut videos_backed_up),
        };
        let found = db.as_ref()
            .and_then(|d| d.find_by_hash(hash).ok().flatten())
            .is_some();
        if found { *backed_count += 1; } else { *new_count += 1; }
    }

    CardAlert {
        volume_path: vol.to_path_buf(),
        volume_name: name.to_string(),
        total: files.len(),
        photos_new,
        videos_new,
        photos_backed_up,
        videos_backed_up,
        scanning: false,
    }
}

/// 列出当前已挂载的可移除卷（排除系统卷）
pub fn list_volumes() -> Vec<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let exclude = [
            "Macintosh HD", "Preboot", "Recovery", "VM", "Data",
            "Update", "xarts", "Baseband", "Hardware",
        ];
        let Ok(entries) = std::fs::read_dir("/Volumes") else { return vec![]; };
        entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                let name = p.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
                p.is_dir() && !exclude.iter().any(|ex| name == *ex)
            })
            .collect()
    }

    #[cfg(target_os = "linux")]
    {
        let mut vols = vec![];
        for base in ["/media", "/mnt", "/run/media"] {
            if let Ok(entries) = std::fs::read_dir(base) {
                vols.extend(entries.flatten().map(|e| e.path()).filter(|p| p.is_dir()));
            }
            // /media/$USER/
            if let Ok(user) = std::env::var("USER") {
                if let Ok(entries) = std::fs::read_dir(format!("{base}/{user}")) {
                    vols.extend(entries.flatten().map(|e| e.path()).filter(|p| p.is_dir()));
                }
            }
        }
        vols
    }

    #[cfg(target_os = "windows")]
    {
        (b'A'..=b'Z')
            .map(|c| PathBuf::from(format!("{}:\\", c as char)))
            .filter(|p| p.exists())
            .collect()
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    { vec![] }
}
