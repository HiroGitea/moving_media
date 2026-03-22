use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection};
use std::path::Path;

/// Increment this when the schema changes. Add a migration arm in `run_migrations`.
pub const CURRENT_VERSION: u32 = 1;

#[derive(Debug)]
pub enum VersionStatus {
    Ok,
    NeedsUpgrade { db_version: u32, app_version: u32 },
    TooNew { db_version: u32, app_version: u32 },
}

/// Check DB version without opening a full Database (no migration).
pub fn check_version(db_path: &Path) -> Result<VersionStatus> {
    if !db_path.exists() {
        return Ok(VersionStatus::Ok);
    }
    let conn = Connection::open(db_path)
        .with_context(|| format!("打开数据库失败: {}", db_path.display()))?;
    let version: u32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version > CURRENT_VERSION {
        Ok(VersionStatus::TooNew {
            db_version: version,
            app_version: CURRENT_VERSION,
        })
    } else if version < CURRENT_VERSION {
        // version 0 could be fresh or pre-versioning — both are fine
        Ok(VersionStatus::Ok)
    } else {
        Ok(VersionStatus::Ok)
    }
}

pub struct Database {
    conn: Connection,
    mirror_path: Option<std::path::PathBuf>,
    /// When true, `sync_mirror` is deferred. Call `flush_mirror()` to force sync.
    mirror_deferred: bool,
    mirror_dirty: bool,
}

impl Database {
    pub fn open(db_path: &Path, mirror_path: Option<&Path>) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("创建目录失败: {}", parent.display()))?;
        }
        let conn = Connection::open(db_path)
            .with_context(|| format!("打开数据库失败: {}", db_path.display()))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let db = Database {
            conn,
            mirror_path: mirror_path.map(|p| p.to_path_buf()),
            mirror_deferred: false,
            mirror_dirty: false,
        };
        db.migrate()?;
        Ok(db)
    }

    /// Open in read-only mode from mirror when primary is unavailable.
    pub fn open_readonly(mirror_path: &Path) -> Result<Self> {
        let conn = Connection::open(mirror_path)
            .with_context(|| format!("打开镜像数据库失败: {}", mirror_path.display()))?;
        Ok(Database {
            conn,
            mirror_path: None,
            mirror_deferred: false,
            mirror_dirty: false,
        })
    }

    // ── Schema versioning ────────────────────────────────────

    fn get_version(&self) -> Result<u32> {
        Ok(self
            .conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))?)
    }

    fn set_version(&self, v: u32) -> Result<()> {
        // user_version cannot be set via params binding, only string formatting is safe here
        // because v is a u32 (no injection risk).
        self.conn
            .execute_batch(&format!("PRAGMA user_version = {v}"))?;
        Ok(())
    }

    /// Check schema version and run any pending migrations.
    ///
    /// Handles three cases:
    /// - Fresh database (version=0, no tables): creates schema, stamps CURRENT_VERSION.
    /// - Pre-versioning database (version=0, table exists): stamps CURRENT_VERSION, no DDL.
    /// - Versioned database (version>0): runs each migration step in order.
    pub fn migrate(&self) -> Result<()> {
        let version = self.get_version()?;

        if version == 0 {
            let table_exists: bool = self.conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='files'",
                [],
                |r| r.get::<_, i64>(0),
            )? > 0;

            if table_exists {
                // Pre-versioning database: schema is already current, just stamp the version.
                self.set_version(CURRENT_VERSION)?;
                return Ok(());
            }
        }

        if version > CURRENT_VERSION {
            bail!(
                "数据库版本 {version} 高于程序支持的版本 {CURRENT_VERSION}，\
                 请升级程序后再使用。"
            );
        }

        for v in version..CURRENT_VERSION {
            match v {
                0 => self.migration_v1()?,
                // Future migrations:
                // 1 => self.migration_v2()?,
                _ => bail!("未知迁移步骤: {v} → {}", v + 1),
            }
            self.set_version(v + 1)?;
        }

        Ok(())
    }

    /// v0 → v1: initial schema.
    fn migration_v1(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS files (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                filename     TEXT NOT NULL,
                dest_path    TEXT NOT NULL UNIQUE,
                hash         TEXT NOT NULL,
                hash_algo    TEXT NOT NULL DEFAULT 'blake3',
                file_size    INTEGER NOT NULL,
                source_path  TEXT,
                session_name TEXT NOT NULL,
                backed_up_at TEXT NOT NULL,
                verified_at  TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_hash ON files(hash);",
        )?;
        Ok(())
    }

    // ── Queries ──────────────────────────────────────────────

    /// Returns (session_name, dest_path) if this hash is already backed up.
    pub fn find_by_hash(&self, hash: &str) -> Result<Option<(String, String)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT session_name, dest_path FROM files WHERE hash = ?1")?;
        let mut rows = stmt.query(params![hash])?;
        if let Some(row) = rows.next()? {
            Ok(Some((row.get(0)?, row.get(1)?)))
        } else {
            Ok(None)
        }
    }

    /// Remove a record by hash (used when a file is found missing on disk).
    pub fn delete_by_hash(&mut self, hash: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM files WHERE hash = ?1", params![hash])?;
        self.sync_mirror()?;
        Ok(())
    }

    pub fn insert_file(
        &mut self,
        filename: &str,
        dest_path: &str,
        hash: &str,
        file_size: u64,
        source_path: Option<&str>,
        session_name: &str,
    ) -> Result<()> {
        let now = chrono::Local::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO files (filename, dest_path, hash, hash_algo, file_size, source_path, session_name, backed_up_at)
             VALUES (?1, ?2, ?3, 'blake3', ?4, ?5, ?6, ?7)",
            params![filename, dest_path, hash, file_size as i64, source_path, session_name, now],
        )?;
        self.sync_mirror()?;
        Ok(())
    }

    /// Bulk write mode for large imports/reindexing.
    pub fn begin_bulk(&self) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE;")?;
        Ok(())
    }

    pub fn commit_bulk(&mut self) -> Result<()> {
        self.conn.execute_batch("COMMIT;")?;
        self.sync_mirror()?;
        Ok(())
    }

    pub fn rollback_bulk(&self) -> Result<()> {
        self.conn.execute_batch("ROLLBACK;")?;
        Ok(())
    }

    /// Insert a record if its hash is not already present.
    ///
    /// Returns `true` when a new row was inserted, `false` when skipped.
    /// Intended for use inside an explicit bulk transaction, so it does not sync the mirror DB.
    pub fn insert_file_if_missing_bulk(
        &self,
        filename: &str,
        dest_path: &str,
        hash: &str,
        file_size: u64,
        source_path: Option<&str>,
        session_name: &str,
    ) -> Result<bool> {
        let now = chrono::Local::now().to_rfc3339();
        let changed = self.conn.execute(
            "INSERT OR IGNORE INTO files
             (filename, dest_path, hash, hash_algo, file_size, source_path, session_name, backed_up_at)
             VALUES (?1, ?2, ?3, 'blake3', ?4, ?5, ?6, ?7)",
            params![filename, dest_path, hash, file_size as i64, source_path, session_name, now],
        )?;
        Ok(changed > 0)
    }

    pub fn update_verified(&mut self, hash: &str) -> Result<()> {
        let now = chrono::Local::now().to_rfc3339();
        self.conn.execute(
            "UPDATE files SET verified_at = ?1 WHERE hash = ?2",
            params![now, hash],
        )?;
        self.sync_mirror()?;
        Ok(())
    }

    pub fn list_all(&self) -> Result<Vec<FileRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT filename, dest_path, hash, hash_algo, file_size, session_name, backed_up_at, verified_at
             FROM files ORDER BY backed_up_at DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(FileRecord {
                filename: row.get(0)?,
                dest_path: row.get(1)?,
                hash: row.get(2)?,
                hash_algo: row.get(3)?,
                file_size: row.get::<_, i64>(4)? as u64,
                session_name: row.get(5)?,
                backed_up_at: row.get(6)?,
                verified_at: row.get(7)?,
            })
        })?;
        rows.map(|r| r.map_err(Into::into)).collect()
    }

    fn sync_mirror(&mut self) -> Result<()> {
        if self.mirror_deferred {
            self.mirror_dirty = true;
            return Ok(());
        }
        self.do_sync_mirror()
    }

    fn do_sync_mirror(&mut self) -> Result<()> {
        if let Some(ref mirror) = self.mirror_path {
            if let Some(parent) = mirror.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let mirror_str = mirror.to_string_lossy();
            let _ = std::fs::remove_file(mirror.as_path());
            self.conn
                .execute_batch(&format!("VACUUM INTO '{mirror_str}'"))?;
        }
        self.mirror_dirty = false;
        Ok(())
    }

    /// Defer mirror syncs until `flush_mirror()` is called.
    pub fn set_mirror_deferred(&mut self, deferred: bool) {
        self.mirror_deferred = deferred;
    }

    /// Force mirror sync if any writes have occurred since deferring.
    pub fn flush_mirror(&mut self) -> Result<()> {
        if self.mirror_dirty {
            self.do_sync_mirror()?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct FileRecord {
    pub filename: String,
    pub dest_path: String,
    pub hash: String,
    pub hash_algo: String,
    pub file_size: u64,
    pub session_name: String,
    pub backed_up_at: String,
    pub verified_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_find() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let mut db = Database::open(&db_path, None).unwrap();
        db.insert_file(
            "DSC001.ARW",
            "20260322_新宿/DSC001.ARW",
            "abc123",
            1024,
            None,
            "20260322_新宿",
        )
        .unwrap();
        let found = db.find_by_hash("abc123").unwrap();
        assert_eq!(
            found,
            Some((
                "20260322_新宿".to_string(),
                "20260322_新宿/DSC001.ARW".to_string()
            ))
        );
        let missing = db.find_by_hash("nonexistent").unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_mirror_sync() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("main.db");
        let mirror_path = dir.path().join("mirror.db");
        let mut db = Database::open(&db_path, Some(&mirror_path)).unwrap();
        db.insert_file(
            "video.mp4",
            "20260322_新宿/video.mp4",
            "def456",
            2048,
            None,
            "20260322_新宿",
        )
        .unwrap();
        assert!(
            mirror_path.exists(),
            "mirror DB should be created after insert"
        );
    }

    #[test]
    fn test_version_stamped_on_new_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Database::open(&db_path, None).unwrap();
        assert_eq!(db.get_version().unwrap(), CURRENT_VERSION);
    }

    #[test]
    fn test_pre_versioning_db_adopted() {
        // Simulate a database created before versioning was introduced:
        // table exists but user_version = 0.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("old.db");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    dest_path TEXT NOT NULL UNIQUE,
                    hash TEXT NOT NULL,
                    hash_algo TEXT NOT NULL DEFAULT 'blake3',
                    file_size INTEGER NOT NULL,
                    source_path TEXT,
                    session_name TEXT NOT NULL,
                    backed_up_at TEXT NOT NULL,
                    verified_at TEXT
                );
                CREATE UNIQUE INDEX idx_hash ON files(hash);",
            )
            .unwrap();
            // user_version stays 0
        }
        let db = Database::open(&db_path, None).unwrap();
        assert_eq!(db.get_version().unwrap(), CURRENT_VERSION);
    }

    #[test]
    fn test_newer_db_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("future.db");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(&format!("PRAGMA user_version = {}", CURRENT_VERSION + 1))
                .unwrap();
        }
        assert!(Database::open(&db_path, None).is_err());
    }
}
