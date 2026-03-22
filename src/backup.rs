use anyhow::{bail, Result};
use chrono::NaiveDate;
use std::path::{Path, PathBuf};

use crate::db::Database;
use crate::hash::hash_file;
use crate::scanner::{MediaFile, MediaType};

/// A file that was successfully copied in this backup run.
/// Retained so we can re-verify both sides (SD card + disk) after all copying is done.
pub struct BackedUpFile {
    pub source: PathBuf,
    pub dest: PathBuf,
    pub hash: String,
}

pub struct BackupResult {
    pub copied: usize,
    pub skipped: usize,
    pub failed: usize,
    pub errors: Vec<String>,
    pub backed_up: Vec<BackedUpFile>,
}

/// Build session folder name from date(s) and user-provided suffix.
pub fn session_name(dates: &[NaiveDate], suffix: &str) -> String {
    match dates {
        [] => format!("unknown_{suffix}"),
        [single] => format!("{}_{suffix}", single.format("%Y%m%d")),
        multiple => {
            let first = multiple.first().unwrap().format("%Y%m%d");
            let last = multiple.last().unwrap().format("%Y%m%d");
            format!("{first}-{last}_{suffix}")
        }
    }
}

/// Copy files to destination, verifying hash after copy.
/// `progress_cb` is called with (current, total) after each file.
pub fn backup_files(
    files: &[MediaFile],
    session: &str,
    photos_root: &Path,
    videos_root: &Path,
    photos_db: &mut Database,
    videos_db: &mut Database,
    progress_cb: &mut dyn FnMut(usize, usize),
) -> BackupResult {
    let total = files.len();
    let mut copied = 0;
    let mut skipped = 0;
    let mut failed = 0;
    let mut errors = Vec::new();
    let mut backed_up = Vec::new();

    for (i, file) in files.iter().enumerate() {
        progress_cb(i, total);

        let result = match file.media_type {
            MediaType::Photo => backup_single(file, session, photos_root, photos_db),
            MediaType::Video => backup_single(file, session, videos_root, videos_db),
        };

        match result {
            Ok(Some(bf)) => {
                copied += 1;
                backed_up.push(bf);
            }
            Ok(None) => skipped += 1,
            Err(e) => {
                failed += 1;
                errors.push(format!("{}: {e}", file.filename));
            }
        }
    }

    progress_cb(total, total);
    BackupResult {
        copied,
        skipped,
        failed,
        errors,
        backed_up,
    }
}

/// Re-hash every successfully copied file on both the SD card (source) and disk (dest).
/// `progress_cb(current, total)` — total = files.len() * 2 (source + dest per file).
pub fn verify_backup(
    files: &[BackedUpFile],
    progress_cb: &mut dyn FnMut(usize, usize),
) -> Vec<String> {
    let total = files.len() * 2;
    let mut done = 0;
    let mut mismatches = Vec::new();

    for file in files {
        progress_cb(done, total);
        match hash_file(&file.source) {
            Ok(h) if h == file.hash => {}
            Ok(h) => mismatches.push(format!(
                "SD卡文件变化: {}  期望 {}  实际 {}",
                file.source
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy(),
                &file.hash[..8],
                &h[..8]
            )),
            Err(e) => mismatches.push(format!(
                "SD卡读取失败: {} — {e}",
                file.source
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
            )),
        }
        done += 1;

        progress_cb(done, total);
        match hash_file(&file.dest) {
            Ok(h) if h == file.hash => {}
            Ok(h) => mismatches.push(format!(
                "磁盘文件异常: {}  期望 {}  实际 {}",
                file.dest.file_name().unwrap_or_default().to_string_lossy(),
                &file.hash[..8],
                &h[..8]
            )),
            Err(e) => mismatches.push(format!(
                "磁盘读取失败: {} — {e}",
                file.dest.file_name().unwrap_or_default().to_string_lossy()
            )),
        }
        done += 1;
    }

    progress_cb(total, total);
    mismatches
}

/// Returns Ok(Some(BackedUpFile)) if copied, Ok(None) if already backed up (skipped).
fn backup_single(
    file: &MediaFile,
    session: &str,
    dest_root: &Path,
    db: &mut Database,
) -> Result<Option<BackedUpFile>> {
    let hash = hash_file(&file.path)?;

    // Check database for duplicate; also verify the file actually exists on disk.
    // If the record exists but the file is gone (e.g. disk failure, accidental deletion),
    // remove the stale record and re-backup instead of silently skipping.
    if let Some((_session, dest_path)) = db.find_by_hash(&hash)? {
        if dest_root.join(&dest_path).exists() {
            return Ok(None);
        }
        db.delete_by_hash(&hash)?;
    }

    let dest_dir = dest_root.join(session);
    std::fs::create_dir_all(&dest_dir)?;

    // Filesystem-level dedup: if the exact same file already exists on disk (e.g. DB
    // incomplete during reindex), skip copying and just register it in the DB.
    // Size check first (free stat call) to avoid expensive hash on mismatches.
    let candidate = dest_dir.join(&file.filename);
    if let Ok(meta) = std::fs::metadata(&candidate) {
        if meta.len() == file.file_size {
            if let Ok(existing_hash) = hash_file(&candidate) {
                if existing_hash == hash {
                    let rel_path = format!("{}/{}", session, file.filename);
                    db.insert_file(
                        &file.filename,
                        &rel_path,
                        &hash,
                        file.capture_time.as_deref(),
                        file.file_size,
                        Some(&file.path.to_string_lossy()),
                        session,
                    )?;
                    return Ok(None);
                }
            }
        }
    }

    let dest_file = unique_dest_path(&candidate);

    // Write to a hidden temp file first so an interrupted copy never leaves a
    // partial file at the final path (which would block future unique_dest_path picks).
    let tmp_file = dest_file.with_file_name(format!(
        ".{}.tmp",
        dest_file.file_name().unwrap_or_default().to_string_lossy()
    ));
    let copy_result = (|| -> Result<()> {
        std::fs::copy(&file.path, &tmp_file)?;
        let dest_hash = hash_file(&tmp_file)?;
        if dest_hash != hash {
            bail!(
                "校验失败: {} 复制后哈希不一致（源: {} 目标: {}）",
                file.filename,
                hash,
                dest_hash
            );
        }
        std::fs::rename(&tmp_file, &dest_file)?;
        Ok(())
    })();
    if copy_result.is_err() {
        let _ = std::fs::remove_file(&tmp_file);
        return Err(copy_result.unwrap_err());
    }

    let rel_path = format!(
        "{}/{}",
        session,
        dest_file.file_name().unwrap_or_default().to_string_lossy()
    );
    db.insert_file(
        &file.filename,
        &rel_path,
        &hash,
        file.capture_time.as_deref(),
        file.file_size,
        Some(&file.path.to_string_lossy()),
        session,
    )?;

    Ok(Some(BackedUpFile {
        source: file.path.clone(),
        dest: dest_file,
        hash,
    }))
}

/// If dest path already exists (filename collision), append _2, _3 etc.
fn unique_dest_path(path: &Path) -> PathBuf {
    if !path.exists() {
        return path.to_path_buf();
    }
    let stem = path.file_stem().unwrap_or_default().to_string_lossy();
    let ext = path
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    let parent = path.parent().unwrap();
    let mut n = 2u32;
    loop {
        let candidate = parent.join(format!("{stem}_{n}{ext}"));
        if !candidate.exists() {
            return candidate;
        }
        n += 1;
    }
}

pub struct SpotCheckResult {
    pub checked: usize,
    pub sessions_covered: usize,
    pub mismatches: Vec<String>,
}

/// 基于 mtime 的抽检：递归扫描每个 session 文件夹（含子目录）下所有文件/目录的修改时间，
/// 若任意一个比上次校验时间新（或从未校验过），则对该 session 数据库记录的全部文件重新哈希。
/// 无变化的 session 直接跳过，不读文件。
pub fn spot_check(
    db: &mut Database,
    media_root: &Path,
    progress_cb: &mut dyn FnMut(usize, usize),
) -> Result<SpotCheckResult> {
    use std::collections::HashMap;
    use std::time::SystemTime;

    let records = db.list_all()?;

    // Group by session; track the latest verified_at per session.
    let mut by_session: HashMap<String, Vec<_>> = HashMap::new();
    for rec in records {
        by_session
            .entry(rec.session_name.clone())
            .or_default()
            .push(rec);
    }

    // Determine which sessions need checking based on mtime.
    let mut sessions_to_check: Vec<(String, Vec<_>)> = Vec::new();
    for (session, files) in by_session {
        let session_dir = media_root.join(&session);

        // Latest verified_at across all files in this session.
        let last_verified: Option<SystemTime> = files
            .iter()
            .filter_map(|f| f.verified_at.as_deref())
            .filter_map(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| SystemTime::from(dt))
            .max();

        if dir_modified_since(&session_dir, last_verified) {
            sessions_to_check.push((session, files));
        }
    }

    let sessions_covered = sessions_to_check.len();
    let total: usize = sessions_to_check.iter().map(|(_, f)| f.len()).sum();
    let mut checked = 0;
    let mut mismatches = Vec::new();

    for (_, files) in &sessions_to_check {
        for record in files {
            progress_cb(checked, total);
            verify_record(record, media_root, db, &mut mismatches);
            checked += 1;
        }
    }

    progress_cb(total, total);
    Ok(SpotCheckResult {
        checked,
        sessions_covered,
        mismatches,
    })
}

/// Returns true if any file or directory under `dir` (recursively) has an mtime
/// newer than `since`. If `since` is None (never verified), always returns true.
fn dir_modified_since(dir: &Path, since: Option<std::time::SystemTime>) -> bool {
    let Some(since) = since else { return true };

    let Ok(entries) = std::fs::read_dir(dir) else {
        return true;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let mtime = entry.metadata().ok().and_then(|m| m.modified().ok());
        if mtime.map(|t| t > since).unwrap_or(true) {
            return true;
        }
        if path.is_dir() && dir_modified_since(&path, Some(since)) {
            return true;
        }
    }
    false
}

/// Check a single DB record against its file on disk. Pushes mismatches if any.
fn verify_record(
    record: &crate::db::FileRecord,
    media_root: &Path,
    db: &mut Database,
    mismatches: &mut Vec<String>,
) {
    let full_path = media_root.join(&record.dest_path);
    if !full_path.exists() {
        mismatches.push(format!("文件不存在: {}", record.dest_path));
        return;
    }
    match hash_file(&full_path) {
        Ok(h) if h == record.hash => {
            let _ = db.update_verified(&record.hash);
        }
        Ok(h) => mismatches.push(format!(
            "哈希不符: {}  期望: {}  实际: {}",
            record.dest_path, record.hash, h
        )),
        Err(e) => mismatches.push(format!("读取失败: {} — {e}", record.dest_path)),
    }
}

/// Verify all DB records against their files on disk.
/// Simpler than `verify_files`: only checks files the DB knows about.
pub fn verify_db_records(
    db: &mut Database,
    media_root: &Path,
    progress_cb: &mut dyn FnMut(usize, usize),
) -> Result<Vec<String>> {
    let records = db.list_all()?;
    let total = records.len();
    let mut mismatches = Vec::new();

    for (i, record) in records.iter().enumerate() {
        progress_cb(i, total);
        verify_record(record, media_root, db, &mut mismatches);
    }

    progress_cb(total, total);
    Ok(mismatches)
}

/// Verify all media files under `media_root` by scanning the filesystem.
///
/// Two-pass check:
/// 1. Scan all media files on disk, hash each one, compare against DB record.
/// 2. Check all DB records to find files that are missing from disk.
///
/// This catches both corrupted files AND files not in the DB (untracked).
pub fn verify_files(
    db: &mut Database,
    media_root: &Path,
    progress_cb: &mut dyn FnMut(usize, usize),
) -> Result<Vec<String>> {
    use std::collections::HashSet;

    // Pass 1: scan filesystem for all media files
    let mut disk_files: Vec<PathBuf> = Vec::new();
    for entry in walkdir::WalkDir::new(media_root)
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
        if crate::scanner::PHOTO_EXTS.contains(&ext.as_str())
            || crate::scanner::VIDEO_EXTS.contains(&ext.as_str())
        {
            disk_files.push(path);
        }
    }

    let records = db.list_all()?;
    // Build a set of (hash → record) for quick lookup
    let record_by_hash: std::collections::HashMap<&str, &crate::db::FileRecord> =
        records.iter().map(|r| (r.hash.as_str(), r)).collect();
    // Track which DB hashes we've seen on disk
    let mut seen_hashes: HashSet<String> = HashSet::new();

    let total = disk_files.len() + records.len(); // disk scan + DB cross-check
    let mut checked = 0;
    let mut mismatches = Vec::new();

    // Pass 1: hash every file on disk, compare with DB
    for path in &disk_files {
        progress_cb(checked, total);
        let rel_path = path
            .strip_prefix(media_root)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string();

        match hash_file(path) {
            Ok(hash) => {
                seen_hashes.insert(hash.clone());
                if let Some(record) = record_by_hash.get(hash.as_str()) {
                    if record.hash != hash {
                        mismatches.push(format!(
                            "哈希不符: {}  期望: {}  实际: {}",
                            rel_path, record.hash, hash
                        ));
                    } else {
                        let _ = db.update_verified(&hash);
                    }
                } else {
                    mismatches.push(format!("未在数据库中: {rel_path}"));
                }
            }
            Err(e) => {
                mismatches.push(format!("读取失败: {rel_path} — {e}"));
            }
        }
        checked += 1;
    }

    // Pass 2: find DB records whose files are missing from disk
    for record in &records {
        progress_cb(checked, total);
        if !seen_hashes.contains(&record.hash) {
            let full_path = media_root.join(&record.dest_path);
            if !full_path.exists() {
                mismatches.push(format!("文件不存在: {}", record.dest_path));
            }
        }
        checked += 1;
    }

    progress_cb(total, total);
    Ok(mismatches)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_name_single_day() {
        let d = NaiveDate::from_ymd_opt(2026, 3, 22).unwrap();
        assert_eq!(session_name(&[d], "新宿"), "20260322_新宿");
    }

    #[test]
    fn test_session_name_multi_day() {
        let d1 = NaiveDate::from_ymd_opt(2026, 3, 20).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2026, 3, 22).unwrap();
        assert_eq!(session_name(&[d1, d2], "东京"), "20260320-20260322_东京");
    }
}
