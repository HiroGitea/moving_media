use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime};
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use walkdir::WalkDir;

#[derive(Debug, Clone, PartialEq)]
pub enum MediaType {
    Photo,
    Video,
}

#[derive(Debug, Clone)]
pub struct MediaFile {
    pub path: PathBuf,
    pub filename: String,
    pub media_type: MediaType,
    pub capture_time: Option<String>,
    pub capture_date: Option<NaiveDate>,
    pub file_size: u64,
}

pub static PHOTO_EXTS: &[&str] = &[
    "arw", "raf", "cr3", "nef", "dng", "jpg", "jpeg", "heic", "ari", "r3d",
];
pub static VIDEO_EXTS: &[&str] = &["mp4", "mov", "mts", "m2ts", "avi", "mxf"];

#[derive(Debug, Clone)]
struct Entry {
    path: PathBuf,
    filename: String,
    media_type: MediaType,
    file_size: u64,
}

pub fn count_media_files(source: &Path) -> usize {
    count_media_files_with_cancel(source, || false).unwrap_or(0)
}

pub fn count_media_files_with_cancel<C>(source: &Path, should_cancel: C) -> Option<usize>
where
    C: Fn() -> bool,
{
    WalkDir::new(source)
        .follow_links(false)
        .into_iter()
        .take_while(|_| !should_cancel())
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| media_type_for_path(e.path()).is_some())
        .count()
        .into()
}

fn collect_candidates(source: &Path) -> Vec<Entry> {
    // Step 1: collect candidate entries serially (WalkDir is not Send).
    WalkDir::new(source)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| {
            let path = e.path().to_path_buf();
            let media_type = media_type_for_path(&path)?;

            let filename = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();

            let file_size = e.metadata().map(|m| m.len()).unwrap_or(0);

            Some(Entry {
                path,
                filename,
                media_type,
                file_size,
            })
        })
        .collect()
}

pub fn scan_source(source: &Path) -> Result<Vec<MediaFile>> {
    Ok(
        scan_source_with_progress_and_cancel(source, |_, _, _| {}, || false)?
            .unwrap_or_default(),
    )
}

pub fn scan_source_with_progress<F>(source: &Path, progress_cb: F) -> Result<Vec<MediaFile>>
where
    F: Fn(usize, usize, &str) + Send + Sync,
{
    Ok(scan_source_with_progress_and_cancel(source, progress_cb, || false)?.unwrap_or_default())
}

pub fn scan_source_with_progress_and_cancel<F, C>(
    source: &Path,
    progress_cb: F,
    should_cancel: C,
) -> Result<Option<Vec<MediaFile>>>
where
    F: Fn(usize, usize, &str) + Send + Sync,
    C: Fn() -> bool + Send + Sync,
{
    let candidates = collect_candidates(source);
    let total = candidates.len();

    // Step 2: read EXIF / mtime in parallel across CPU cores.
    let progress = AtomicUsize::new(0);
    let files: Vec<Option<MediaFile>> = candidates
        .into_par_iter()
        .map(|entry| {
            if should_cancel() {
                return None;
            }
            let capture_time = capture_time_for_path(&entry.path, &entry.media_type);
            let capture_date = capture_time
                .as_deref()
                .and_then(|dt| dt.get(..10))
                .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
            let cur = progress.fetch_add(1, Ordering::Relaxed) + 1;
            progress_cb(cur, total, &entry.filename);
            Some(MediaFile {
                path: entry.path,
                filename: entry.filename,
                media_type: entry.media_type,
                capture_time,
                capture_date,
                file_size: entry.file_size,
            })
        })
        .collect();

    if should_cancel() {
        Ok(None)
    } else {
        Ok(Some(files.into_iter().flatten().collect()))
    }
}

fn media_type_for_path(path: &Path) -> Option<MediaType> {
    let ext = path
        .extension()
        .and_then(|x| x.to_str())
        .map(|x| x.to_lowercase())
        .unwrap_or_default();

    if PHOTO_EXTS.contains(&ext.as_str()) {
        Some(MediaType::Photo)
    } else if VIDEO_EXTS.contains(&ext.as_str()) {
        Some(MediaType::Video)
    } else {
        None
    }
}

pub fn capture_time_for_path(path: &Path, media_type: &MediaType) -> Option<String> {
    match media_type {
        MediaType::Photo => extract_exif_datetime(path),
        MediaType::Video => extract_fs_datetime(path),
    }
}

fn extract_exif_datetime(path: &Path) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    let mut bufreader = std::io::BufReader::new(file);
    let exif = exif::Reader::new()
        .read_from_container(&mut bufreader)
        .ok()?;

    let field = exif.get_field(exif::Tag::DateTimeOriginal, exif::In::PRIMARY)?;
    let dt_str = match &field.value {
        exif::Value::Ascii(v) => v.first()?.iter().map(|&b| b as char).collect::<String>(),
        _ => return None,
    };
    let dt = NaiveDateTime::parse_from_str(dt_str.trim(), "%Y:%m:%d %H:%M:%S").ok()?;
    Some(dt.format("%Y-%m-%d %H:%M:%S").to_string())
}

fn extract_fs_datetime(path: &Path) -> Option<String> {
    use chrono::{DateTime, Local};

    let meta = std::fs::metadata(path).ok()?;
    let ts = meta.created().ok().or_else(|| meta.modified().ok())?;
    let dt: DateTime<Local> = ts.into();
    Some(dt.format("%Y-%m-%d %H:%M:%S").to_string())
}

/// Kept for tests/callers that only need a date.
/// Return sorted unique dates from a list of files.
pub fn unique_dates(files: &[MediaFile]) -> Vec<NaiveDate> {
    let mut dates: Vec<NaiveDate> = files.iter().filter_map(|f| f.capture_date).collect();
    dates.sort();
    dates.dedup();
    dates
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_scan_classifies_by_extension() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::File::create(dir.path().join("photo.ARW"))
            .unwrap()
            .write_all(b"fake")
            .unwrap();
        std::fs::File::create(dir.path().join("video.MP4"))
            .unwrap()
            .write_all(b"fake")
            .unwrap();
        std::fs::File::create(dir.path().join("ignore.txt"))
            .unwrap()
            .write_all(b"fake")
            .unwrap();

        let files = scan_source(dir.path()).unwrap();
        assert_eq!(files.len(), 2);
        let photos: Vec<_> = files
            .iter()
            .filter(|f| f.media_type == MediaType::Photo)
            .collect();
        let videos: Vec<_> = files
            .iter()
            .filter(|f| f.media_type == MediaType::Video)
            .collect();
        assert_eq!(photos.len(), 1);
        assert_eq!(videos.len(), 1);
    }
}
