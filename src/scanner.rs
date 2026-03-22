use anyhow::Result;
use chrono::NaiveDate;
use rayon::prelude::*;
use std::path::{Path, PathBuf};
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
    pub capture_date: Option<NaiveDate>,
    pub file_size: u64,
}

pub static PHOTO_EXTS: &[&str] = &["arw", "raf", "cr3", "nef", "dng", "jpg", "jpeg", "heic", "ari", "r3d"];
pub static VIDEO_EXTS: &[&str] = &["mp4", "mov", "mts", "m2ts", "avi", "mxf"];

pub fn scan_source(source: &Path) -> Result<Vec<MediaFile>> {
    // Step 1: collect candidate entries serially (WalkDir is not Send).
    struct Entry {
        path: PathBuf,
        filename: String,
        media_type: MediaType,
        file_size: u64,
    }

    let candidates: Vec<Entry> = WalkDir::new(source)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| {
            let path = e.path().to_path_buf();
            let ext = path
                .extension()
                .and_then(|x| x.to_str())
                .map(|x| x.to_lowercase())
                .unwrap_or_default();

            let media_type = if PHOTO_EXTS.contains(&ext.as_str()) {
                MediaType::Photo
            } else if VIDEO_EXTS.contains(&ext.as_str()) {
                MediaType::Video
            } else {
                return None;
            };

            let filename = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();

            let file_size = e.metadata().map(|m| m.len()).unwrap_or(0);

            Some(Entry { path, filename, media_type, file_size })
        })
        .collect();

    // Step 2: read EXIF / mtime in parallel across CPU cores.
    let files: Vec<MediaFile> = candidates
        .into_par_iter()
        .map(|entry| {
            let capture_date = match entry.media_type {
                MediaType::Photo => extract_exif_date(&entry.path),
                MediaType::Video => extract_mtime_date(&entry.path),
            };
            MediaFile {
                path: entry.path,
                filename: entry.filename,
                media_type: entry.media_type,
                capture_date,
                file_size: entry.file_size,
            }
        })
        .collect();

    Ok(files)
}

fn extract_exif_date(path: &Path) -> Option<NaiveDate> {
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
    // Format: "2026:03:22 10:30:00"
    let date_part = dt_str.split_whitespace().next()?;
    let parts: Vec<&str> = date_part.split(':').collect();
    if parts.len() != 3 {
        return None;
    }
    let year: i32 = parts[0].parse().ok()?;
    let month: u32 = parts[1].parse().ok()?;
    let day: u32 = parts[2].parse().ok()?;
    NaiveDate::from_ymd_opt(year, month, day)
}

fn extract_mtime_date(path: &Path) -> Option<NaiveDate> {
    use chrono::{DateTime, Local};
    let meta = std::fs::metadata(path).ok()?;
    let mtime = meta.modified().ok()?;
    let dt: DateTime<Local> = mtime.into();
    Some(dt.date_naive())
}

/// Return sorted unique dates from a list of files.
pub fn unique_dates(files: &[MediaFile]) -> Vec<NaiveDate> {
    let mut dates: Vec<NaiveDate> = files
        .iter()
        .filter_map(|f| f.capture_date)
        .collect();
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
        let photos: Vec<_> = files.iter().filter(|f| f.media_type == MediaType::Photo).collect();
        let videos: Vec<_> = files.iter().filter(|f| f.media_type == MediaType::Video).collect();
        assert_eq!(photos.len(), 1);
        assert_eq!(videos.len(), 1);
    }
}
