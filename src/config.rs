use std::path::PathBuf;

pub struct Config {
    pub photos_root: PathBuf,
    pub videos_root: PathBuf,
    pub db_mirror_dir: PathBuf,
    pub last_source: Option<PathBuf>,
}

fn config_base_dir() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| dirs::home_dir().unwrap_or_else(|| PathBuf::from("/tmp")))
        .join("moving_media")
}

/// Path to the persistent config file: ~/.config/moving_media/config
fn config_file_path() -> PathBuf {
    config_base_dir().join("config")
}

fn reindex_checkpoint_path() -> PathBuf {
    config_base_dir().join("reindex.checkpoint")
}

impl Config {
    /// Load config. Priority: env vars > config file > defaults.
    pub fn load() -> Self {
        let (file_photos, file_videos, last_source) = Self::load_from_file();

        let photos_root = std::env::var("MOVING_MEDIA_PHOTOS")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                file_photos.unwrap_or_else(|| PathBuf::from("/Volumes/My_Files/Backup/Media/Photos"))
            });

        let videos_root = std::env::var("MOVING_MEDIA_VIDEOS")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                file_videos.unwrap_or_else(|| PathBuf::from("/Volumes/My_Files/Backup/Media/Videos"))
            });

        let db_mirror_dir = std::env::var("MOVING_MEDIA_DB_BACKUP")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                dirs::home_dir()
                    .unwrap_or_else(|| PathBuf::from("/tmp"))
                    .join(".local/share/moving_media")
            });

        Config { photos_root, videos_root, db_mirror_dir, last_source }
    }

    /// Returns true if both media directories exist on disk.
    pub fn is_ready(&self) -> bool {
        self.photos_root.is_dir() && self.videos_root.is_dir()
    }

    /// Save photos/videos paths and last_source to the config file.
    pub fn save(&self) -> std::io::Result<()> {
        let path = config_file_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut content = format!(
            "photos={}\nvideos={}\n",
            self.photos_root.to_string_lossy(),
            self.videos_root.to_string_lossy()
        );
        if let Some(src) = &self.last_source {
            content.push_str(&format!("last_source={}\n", src.to_string_lossy()));
        }
        std::fs::write(&path, content)
    }

    /// Parse config file, returns (photos_path, videos_path, last_source).
    fn load_from_file() -> (Option<PathBuf>, Option<PathBuf>, Option<PathBuf>) {
        let path = config_file_path();
        let Ok(content) = std::fs::read_to_string(&path) else {
            return (None, None, None);
        };
        let mut photos = None;
        let mut videos = None;
        let mut last_source = None;
        for line in content.lines() {
            if let Some(val) = line.strip_prefix("photos=") {
                photos = Some(PathBuf::from(val.trim()));
            } else if let Some(val) = line.strip_prefix("videos=") {
                videos = Some(PathBuf::from(val.trim()));
            } else if let Some(val) = line.strip_prefix("last_source=") {
                last_source = Some(PathBuf::from(val.trim()));
            }
        }
        (photos, videos, last_source)
    }

    pub fn photos_db(&self) -> PathBuf {
        self.photos_root.join("moving_media.db")
    }

    pub fn videos_db(&self) -> PathBuf {
        self.videos_root.join("moving_media.db")
    }

    pub fn photos_mirror_db(&self) -> PathBuf {
        self.db_mirror_dir.join("photos_moving_media.db")
    }

    pub fn videos_mirror_db(&self) -> PathBuf {
        self.db_mirror_dir.join("videos_moving_media.db")
    }

    pub fn reindex_checkpoint(&self) -> PathBuf {
        reindex_checkpoint_path()
    }

    pub fn load_reindex_checkpoint(path: &std::path::Path) -> Option<String> {
        let content = std::fs::read_to_string(path).ok()?;
        content
            .lines()
            .find_map(|line| line.strip_prefix("last_path=").map(|v| v.trim().to_string()))
            .filter(|v| !v.is_empty())
    }

    pub fn save_reindex_checkpoint(path: &std::path::Path, last_path: &str) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, format!("last_path={last_path}\n"))
    }

    pub fn clear_reindex_checkpoint(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
    }
}
