use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(target_os = "windows")]
use std::os::windows::ffi::OsStrExt;

#[cfg(target_os = "windows")]
use windows_sys::Win32::Storage::FileSystem::{GetDriveTypeW, DRIVE_REMOVABLE};

/// 检测到的可选储存设备。这里只做发现，不自动读取内容。
#[derive(Clone, Debug)]
pub struct CardAlert {
    pub volume_path: PathBuf,
    pub volume_name: String,
}

/// 启动后台线程：每 2 秒轮询挂载卷，发现新卷时只上报设备，卷弹出时自动移除。
///
/// - `alert`：所有已检测卷的列表，UI 线程轮询读取
/// - `ctx`：触发 egui 重绘
pub fn start_watcher(alert: Arc<Mutex<(Vec<CardAlert>, u64)>>, ctx: eframe::egui::Context) {
    std::thread::spawn(move || {
        let mut known: HashSet<PathBuf> = list_volumes().into_iter().collect();

        loop {
            std::thread::sleep(Duration::from_secs(2));

            let current: HashSet<PathBuf> = list_volumes().into_iter().collect();
            let new_vols: Vec<PathBuf> = current.difference(&known).cloned().collect();
            let removed_vols: Vec<PathBuf> = known.difference(&current).cloned().collect();
            known = current;

            // 移除已弹出的卷
            if !removed_vols.is_empty() {
                let mut a = alert.lock().unwrap();
                a.0.retain(|c| !removed_vols.contains(&c.volume_path));
                a.1 += 1;
                drop(a);
                ctx.request_repaint();
            }

            for vol in new_vols {
                let name = vol
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| vol.to_string_lossy().to_string());

                {
                    let mut a = alert.lock().unwrap();
                    a.0.push(CardAlert {
                        volume_path: vol.to_path_buf(),
                        volume_name: name.clone(),
                    });
                    a.1 += 1;
                }
                ctx.request_repaint();
            }
        }
    });
}

/// 列出当前已挂载的可移除卷（排除系统卷）
pub fn list_volumes() -> Vec<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let exclude = [
            "Macintosh HD",
            "Preboot",
            "Recovery",
            "VM",
            "Data",
            "Update",
            "xarts",
            "Baseband",
            "Hardware",
        ];
        let Ok(entries) = std::fs::read_dir("/Volumes") else {
            return vec![];
        };
        entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                let name = p
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();
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
            .filter(|p| p.exists() && is_removable_windows_drive(p))
            .collect()
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        vec![]
    }
}

#[cfg(target_os = "windows")]
fn is_removable_windows_drive(path: &std::path::Path) -> bool {
    let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    wide.push(0);

    unsafe { GetDriveTypeW(wide.as_ptr()) == DRIVE_REMOVABLE }
}
