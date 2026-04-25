//! Persistent SQLite cache — metadata index and small-file BLOB store.
//!
//! # Schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS metadata (
//!     remote_id    TEXT PRIMARY KEY,
//!     inode        INTEGER NOT NULL,
//!     parent_id    TEXT NOT NULL DEFAULT '',
//!     last_fetch   INTEGER NOT NULL,      -- Unix timestamp (seconds)
//!     md5_checksum TEXT,                  -- NULL for folders/Workspace files
//!     is_dirty     INTEGER NOT NULL DEFAULT 0
//! );
//!
//! CREATE TABLE IF NOT EXISTS small_files (
//!     remote_id    TEXT PRIMARY KEY,
//!     data         BLOB NOT NULL
//! );
//!
//! CREATE TABLE IF NOT EXISTS sync_state (
//!     key          TEXT PRIMARY KEY,
//!     value        TEXT NOT NULL
//! );
//! ```
//!
//! # Concurrency
//!
//! WAL mode (Write-Ahead Logging) allows multiple concurrent readers
//! alongside a single writer.  The `r2d2` connection pool (up to
//! `POOL_SIZE` connections) ensures the 16-thread reply-dispatcher pool
//! from Phase 1 never serialises on a single SQLite connection.
//!
//! # Error handling
//!
//! All public methods that write to the database return or silently log
//! errors — they never panic or propagate to the FUSE layer.  Read methods
//! return `Option<T>` / `Vec<T>` so callers can fall through to a
//! network fetch when the DB is unavailable.

use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Number of pooled SQLite connections.
/// Sized to serve the 16-thread reply dispatcher without contention.
const POOL_SIZE: u32 = 8;

/// Cached metadata entry returned from the DB.
#[derive(Debug, Clone)]
pub struct CachedMeta {
    pub remote_id: String,
    /// Stable inode number assigned on first cache insertion.
    #[allow(dead_code)] // populated by all SELECT paths; used by future callers
    pub inode: u64,
    pub parent_id: String,
    /// Display name of the file as stored in Drive.
    pub name: String,
    /// Unix timestamp of the last successful metadata fetch.
    #[allow(dead_code)] // populated by all SELECT paths; used by future cache-validity callers
    pub last_fetch: u64,
    pub md5_checksum: Option<String>,
    pub is_dirty: bool,
}

/// Thread-safe SQLite-backed persistent cache.
///
/// All operations are non-fatal: failures are logged and callers fall
/// through to in-memory caches or live Drive API requests.
pub struct DbManager {
    pool: Pool<SqliteConnectionManager>,
}

impl DbManager {
    /// Open (or create) the SQLite database at `db_path`.
    ///
    /// - Creates the parent directory if missing.
    /// - Activates WAL mode and `PRAGMA synchronous=NORMAL` on every
    ///   connection in the pool.
    /// - Runs `CREATE TABLE IF NOT EXISTS` migrations.
    ///
    /// Returns `Err` only if the pool itself cannot be constructed (e.g.
    /// the path is unwritable).  Individual query errors are silent.
    pub fn new(db_path: &Path) -> Result<Arc<Self>> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create DB dir '{}'", parent.display()))?;
        }

        let manager = SqliteConnectionManager::file(db_path).with_init(|conn| {
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=NORMAL;
                 PRAGMA foreign_keys=ON;",
            )?;
            Ok(())
        });

        let pool = Pool::builder()
            .max_size(POOL_SIZE)
            .build(manager)
            .context("build SQLite connection pool")?;

        let db = Arc::new(Self { pool });
        db.run_migrations()?;
        info!("db: opened '{}' (pool_size={})", db_path.display(), POOL_SIZE);
        Ok(db)
    }

    // ── Schema migrations ─────────────────────────────────────────────────

    fn run_migrations(&self) -> Result<()> {
        let conn = self.pool.get().context("get connection for migrations")?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS metadata (
                remote_id    TEXT    PRIMARY KEY,
                inode        INTEGER NOT NULL,
                parent_id    TEXT    NOT NULL DEFAULT '',
                last_fetch   INTEGER NOT NULL,
                md5_checksum TEXT,
                is_dirty     INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_metadata_inode
                ON metadata(inode);

            CREATE TABLE IF NOT EXISTS small_files (
                remote_id    TEXT    PRIMARY KEY,
                data         BLOB    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sync_state (
                key          TEXT    PRIMARY KEY,
                value        TEXT    NOT NULL
            );
            ",
        )
        .context("run migrations")?;
        // Phase 4: add `name` column for write-back support.
        // ALTER TABLE ignores the error when the column already exists.
        let _ = conn
            .execute_batch("ALTER TABLE metadata ADD COLUMN name TEXT NOT NULL DEFAULT ''");
        debug!("db: migrations complete");
        Ok(())
    }

    // ── Metadata ──────────────────────────────────────────────────────────

    /// Persist file metadata, upserting any existing row.
    pub fn store_metadata(
        &self,
        remote_id: &str,
        inode: u64,
        parent_id: &str,
        name: &str,
        md5_checksum: Option<&str>,
        is_dirty: bool,
    ) -> Result<()> {
        let conn = self.pool.get().context("get connection")?;
        let now = now_unix();
        conn.execute(
            "INSERT INTO metadata
                (remote_id, inode, parent_id, name, last_fetch, md5_checksum, is_dirty)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(remote_id) DO UPDATE SET
                parent_id    = excluded.parent_id,
                name         = excluded.name,
                last_fetch   = excluded.last_fetch,
                md5_checksum = excluded.md5_checksum,
                is_dirty     = excluded.is_dirty
             -- inode intentionally omitted: once assigned it must never change",
            params![
                remote_id,
                inode as i64,
                parent_id,
                name,
                now as i64,
                md5_checksum,
                is_dirty as i32
            ],
        )
        .context("store_metadata upsert")?;
        Ok(())
    }

    /// Look up cached metadata by Drive file ID.  Returns `None` on miss or
    /// DB error.
    pub fn get_metadata(&self, remote_id: &str) -> Option<CachedMeta> {
        let conn = self.pool.get().ok()?;
        conn.query_row(
            "SELECT remote_id, inode, parent_id, name, last_fetch, md5_checksum, is_dirty
             FROM metadata WHERE remote_id = ?1",
            params![remote_id],
            |row| {
                Ok(CachedMeta {
                    remote_id: row.get(0)?,
                    inode: row.get::<_, i64>(1)? as u64,
                    parent_id: row.get(2)?,
                    name: row.get(3)?,
                    last_fetch: row.get::<_, i64>(4)? as u64,
                    md5_checksum: row.get(5)?,
                    is_dirty: row.get::<_, i32>(6)? != 0,
                })
            },
        )
        .ok()
    }

    /// Mark a file as dirty (local modification not yet uploaded to Drive).
    #[allow(dead_code)] // used in tests and available for future write paths
    pub fn mark_dirty(&self, remote_id: &str) {
        if let Ok(conn) = self.pool.get() {
            let _ = conn.execute(
                "UPDATE metadata SET is_dirty = 1 WHERE remote_id = ?1",
                params![remote_id],
            );
        }
    }

    /// Clear the dirty flag after a successful upload.
    #[allow(dead_code)] // used in tests and available for future write paths
    pub fn clear_dirty(&self, remote_id: &str) {
        if let Ok(conn) = self.pool.get() {
            let _ = conn.execute(
                "UPDATE metadata SET is_dirty = 0 WHERE remote_id = ?1",
                params![remote_id],
            );
        }
    }

    /// Delete all DB entries for a Drive file ID (both tables).
    pub fn remove_entry(&self, remote_id: &str) {
        if let Ok(conn) = self.pool.get() {
            let _ = conn.execute(
                "DELETE FROM metadata WHERE remote_id = ?1",
                params![remote_id],
            );
            let _ = conn.execute(
                "DELETE FROM small_files WHERE remote_id = ?1",
                params![remote_id],
            );
        } else {
            error!("db: remove_entry '{}': could not get connection", remote_id);
        }
    }

    /// Load all `(remote_id, inode)` pairs from the `metadata` table.
    ///
    /// Called once at startup by `ObjectManager` to restore stable inode
    /// assignments across remounts.  An inode that was persisted in a
    /// previous session is reused instead of allocating a fresh counter
    /// value — so the same Drive file always gets the same inode number
    /// as long as the SQLite database is retained.
    pub fn load_inode_map(&self) -> Vec<(String, u64)> {
        let Ok(conn) = self.pool.get() else {
            return vec![];
        };
        let Ok(mut stmt) = conn.prepare("SELECT remote_id, inode FROM metadata") else {
            return vec![];
        };
        stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })
        .ok()
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default()
    }

    /// Return all `remote_id` values currently in the `metadata` table.
    /// Retained for unit-test coverage; production GC uses a bulk-query approach.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn list_all_remote_ids(&self) -> Vec<String> {
        let Ok(conn) = self.pool.get() else {
            return vec![];
        };
        let Ok(mut stmt) = conn.prepare("SELECT remote_id FROM metadata") else {
            return vec![];
        };
        stmt.query_map([], |row| row.get(0))
            .ok()
            .map(|rows| rows.filter_map(|r| r.ok()).collect())
            .unwrap_or_default()
    }

    /// Return all metadata entries flagged as dirty (`is_dirty = 1`).
    ///
    /// Used by `UploadManager` at startup (crash-recovery) and on each
    /// wakeup cycle to find files that need to be synchronised with Drive.
    pub fn list_dirty_entries(&self) -> Vec<CachedMeta> {
        let Ok(conn) = self.pool.get() else {
            return vec![];
        };
        let Ok(mut stmt) = conn.prepare(
            "SELECT remote_id, inode, parent_id, name, last_fetch, md5_checksum, is_dirty
             FROM metadata WHERE is_dirty = 1",
        ) else {
            return vec![];
        };
        stmt.query_map([], |row| {
            Ok(CachedMeta {
                remote_id: row.get(0)?,
                inode: row.get::<_, i64>(1)? as u64,
                parent_id: row.get(2)?,
                name: row.get(3)?,
                last_fetch: row.get::<_, i64>(4)? as u64,
                md5_checksum: row.get(5)?,
                is_dirty: row.get::<_, i32>(6)? != 0,
            })
        })
        .ok()
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default()
    }

    /// Permanently remove a dirty metadata entry that cannot be uploaded.
    ///
    /// Called by `UploadManager` when `create_file` returns HTTP 404 —
    /// meaning the parent folder no longer exists on Drive (e.g. it was
    /// deleted in a previous session before the upload could complete).
    /// Retrying is pointless in this case; abandoning prevents the entry
    /// from looping forever on every restart.
    pub fn abandon_dirty_entry(&self, remote_id: &str) {
        if let Ok(conn) = self.pool.get() {
            let _ = conn.execute(
                "DELETE FROM metadata WHERE remote_id = ?1 AND is_dirty = 1",
                params![remote_id],
            );
        }
        warn!("db: abandoned unrecoverable dirty entry '{}'", remote_id);
    }

    /// After a successful Drive upload of an existing file, clear the dirty
    /// flag and persist the server-assigned `md5_checksum`.
    pub fn clear_dirty_after_upload(&self, remote_id: &str, md5: Option<&str>) {
        if let Ok(conn) = self.pool.get() {
            let _ = conn.execute(
                "UPDATE metadata SET is_dirty = 0, md5_checksum = ?2 WHERE remote_id = ?1",
                params![remote_id, md5],
            );
        }
    }

    /// After a successful Drive creation of a new file, swap the temporary
    /// pending `remote_id` for the real Drive file ID, clear the dirty flag,
    /// and persist the `md5_checksum`.
    ///
    /// Also migrates the matching row in the `small_files` table so BLOB
    /// content remains accessible under the CAS key (MD5 when available,
    /// otherwise `real_id`).
    pub fn finalize_new_file(&self, pending_id: &str, real_id: &str, md5: Option<&str>) {
        if let Ok(conn) = self.pool.get() {
            let _ = conn.execute(
                "UPDATE metadata SET remote_id = ?2, is_dirty = 0, md5_checksum = ?3
                 WHERE remote_id = ?1",
                params![pending_id, real_id, md5],
            );
        }
        // Migrate small_files from pending_id to the CAS key (md5 if available,
        // else real_id so content remains findable by cache_key_for).
        let sf_new_key = md5.unwrap_or(real_id);
        if let Ok(conn) = self.pool.get() {
            let _ = conn.execute(
                "UPDATE small_files SET remote_id = ?2 WHERE remote_id = ?1",
                params![pending_id, sf_new_key],
            );
        }
    }

    // ── Small-file BLOB store ─────────────────────────────────────────────

    /// Persist raw file bytes for a small file (≤ `CACHE_RAM_MAX_BYTES`).
    ///
    /// `key` is the cache key — the MD5 checksum for clean files (CAS) or
    /// `remote_id` / `__pending__` ID for dirty/pending files.
    ///
    /// Silently logs on error — callers fall through to a network fetch.
    pub fn store_small_file(&self, key: &str, data: &[u8]) {
        match self.pool.get() {
            Ok(conn) => {
                if let Err(e) = conn.execute(
                    "INSERT INTO small_files (remote_id, data) VALUES (?1, ?2)
                     ON CONFLICT(remote_id) DO UPDATE SET data = excluded.data",
                    params![key, data],
                ) {
                    error!("db: store_small_file '{}': {}", key, e);
                }
            }
            Err(e) => error!("db: pool error in store_small_file: {}", e),
        }
    }

    /// Retrieve BLOB content by cache key.  Returns `None` on cache miss
    /// or DB error.
    pub fn get_small_file(&self, key: &str) -> Option<Vec<u8>> {
        let conn = self.pool.get().ok()?;
        conn.query_row(
            "SELECT data FROM small_files WHERE remote_id = ?1",
            params![key],
            |row| row.get(0),
        )
        .ok()
    }

    /// Migrate a small-file BLOB entry from `old_key` to `new_key`.
    ///
    /// Called by `ObjectManager::migrate_cache_key` after a successful upload
    /// to move the BLOB from a dirty `file_id` key to its MD5 CAS key.
    pub fn migrate_small_file_key(&self, old_key: &str, new_key: &str) {
        if old_key == new_key {
            return;
        }
        if let Ok(conn) = self.pool.get() {
            let copied = conn.execute(
                "INSERT OR REPLACE INTO small_files (remote_id, data)
                 SELECT ?1, data FROM small_files WHERE remote_id = ?2",
                params![new_key, old_key],
            );
            if copied.map(|n| n > 0).unwrap_or(false) {
                let _ = conn.execute(
                    "DELETE FROM small_files WHERE remote_id = ?1",
                    params![old_key],
                );
            }
        }
    }

    // ── Sync state ────────────────────────────────────────────────────────

    /// Retrieve the stored Google Drive changes `startPageToken`.
    /// Returns `None` when not yet initialised or on DB error.
    pub fn get_sync_token(&self) -> Option<String> {
        let conn = self.pool.get().ok()?;
        conn.query_row(
            "SELECT value FROM sync_state WHERE key = 'changes_token'",
            [],
            |row| row.get(0),
        )
        .ok()
    }

    /// Persist the latest Drive changes page token.
    pub fn set_sync_token(&self, token: &str) {
        if let Ok(conn) = self.pool.get() {
            let _ = conn.execute(
                "INSERT INTO sync_state (key, value) VALUES ('changes_token', ?1)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![token],
            );
        }
    }

    // ── Garbage collection ────────────────────────────────────────────────

    /// Delete flat content files in `content_dir` that are no longer
    /// referenced by any row in the `metadata` table.
    ///
    /// With Content-Addressable Storage (CAS) the cache files are named by
    /// MD5 checksum (clean files) or by `remote_id` / `__pending__` ID (dirty
    /// or pending files).  A cache file named `X` is safe to delete when:
    ///
    /// - `X` does **not** appear as `md5_checksum` in any metadata row, AND
    /// - `X` does **not** appear as `remote_id` of any dirty metadata row.
    ///
    /// The check is performed with two bulk queries so the cost is O(1) DB
    /// round-trips regardless of how many files are cached.
    ///
    /// Errors are logged but never propagated — GC is best-effort.
    pub fn run_gc(&self, content_dir: &Path) {
        // Build the set of protected cache keys.
        let mut protected: std::collections::HashSet<String> = Default::default();

        if let Ok(conn) = self.pool.get() {
            // All MD5 checksums currently referenced (CAS-keyed clean files).
            if let Ok(mut stmt) = conn.prepare(
                "SELECT DISTINCT md5_checksum FROM metadata
                 WHERE md5_checksum IS NOT NULL AND md5_checksum != ''",
            ) {
                if let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0)) {
                    for r in rows.filter_map(|x| x.ok()) {
                        protected.insert(r);
                    }
                }
            }
            // All remote_ids of dirty entries (file_id-keyed dirty/pending files).
            if let Ok(mut stmt) =
                conn.prepare("SELECT remote_id FROM metadata WHERE is_dirty = 1")
            {
                if let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0)) {
                    for r in rows.filter_map(|x| x.ok()) {
                        protected.insert(r);
                    }
                }
            }
        }

        let dir_iter = match std::fs::read_dir(content_dir) {
            Ok(d) => d,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!("gc: cannot read '{}': {}", content_dir.display(), e);
                }
                return; // empty content dir on first mount — nothing to GC
            }
        };

        let mut removed = 0usize;
        for entry in dir_iter.flatten() {
            let name = entry.file_name();
            let cache_key = name.to_string_lossy();
            // Skip atomic temp files written by DiskCache::insert.
            if cache_key.ends_with(".tmp") {
                continue;
            }
            if !protected.contains(cache_key.as_ref()) {
                let path = entry.path();
                if let Err(e) = std::fs::remove_file(&path) {
                    warn!("gc: remove '{}': {}", path.display(), e);
                } else {
                    debug!("gc: removed orphan '{}'", cache_key);
                    removed += 1;
                }
            }
        }
        if removed > 0 {
            info!("gc: removed {} orphaned content file(s)", removed);
        } else {
            debug!("gc: no orphans found in '{}'", content_dir.display());
        }
    }

    // ── Duplicates report ─────────────────────────────────────────────────

    /// Return all MD5 hashes that appear for more than one Drive file,
    /// together with the `(remote_id, name)` pairs that share that hash.
    ///
    /// Used by the virtual `/.duplicates` file.  Pending files and entries
    /// without a checksum are excluded.
    pub fn get_md5_duplicates(&self) -> Vec<(String, Vec<(String, String)>)> {
        let conn = match self.pool.get() {
            Ok(c) => c,
            Err(_) => return vec![],
        };
        let sql = "
            SELECT md5_checksum, remote_id, name
            FROM metadata
            WHERE md5_checksum IS NOT NULL
              AND md5_checksum != ''
              AND remote_id NOT LIKE '__pending__%'
            ORDER BY md5_checksum
        ";
        let mut stmt = match conn.prepare(sql) {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        let rows: Vec<(String, String, String)> = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .ok()
            .map(|r| r.filter_map(|x| x.ok()).collect())
            .unwrap_or_default();

        let mut map: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();
        for (md5, id, name) in rows {
            map.entry(md5).or_default().push((id, name));
        }
        let mut result: Vec<(String, Vec<(String, String)>)> = map
            .into_iter()
            .filter(|(_, v)| v.len() > 1)
            .collect();
        result.sort_by(|a, b| a.0.cmp(&b.0));
        result
    }

    /// Resolve the full FUSE path for a Drive file by walking the `parent_id`
    /// chain using a recursive CTE.
    ///
    /// Returns `"/<remote_id>"` as a safe fallback when the path cannot be
    /// fully resolved (e.g. intermediate directory not yet in metadata).
    pub fn resolve_path(&self, remote_id: &str) -> String {
        let conn = match self.pool.get() {
            Ok(c) => c,
            Err(_) => return format!("/{}", remote_id),
        };
        let sql = "
            WITH RECURSIVE path_cte(id, parent_id, name, depth) AS (
                SELECT remote_id, parent_id, name, 0
                FROM   metadata WHERE remote_id = ?1
                UNION ALL
                SELECT m.remote_id, m.parent_id, m.name, pc.depth + 1
                FROM   metadata m
                INNER JOIN path_cte pc ON m.remote_id = pc.parent_id
                WHERE  pc.parent_id IS NOT NULL
                  AND  pc.parent_id != ''
                  AND  pc.parent_id != 'root'
            )
            SELECT name FROM path_cte ORDER BY depth DESC
        ";
        let segments: Vec<String> = match conn.prepare(sql) {
            Ok(mut stmt) => stmt
                .query_map(params![remote_id], |row| row.get(0))
                .ok()
                .map(|r| r.filter_map(|x| x.ok()).collect())
                .unwrap_or_default(),
            Err(_) => return format!("/{}", remote_id),
        };
        if segments.is_empty() {
            return format!("/{}", remote_id);
        }
        format!("/{}", segments.join("/"))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ── Unit Tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_db() -> Arc<DbManager> {
        let tmp = tempdir().unwrap();
        DbManager::new(&tmp.path().join("test.db")).expect("DbManager::new must not fail")
    }

    #[test]
    fn store_and_get_metadata_roundtrip() {
        let db = make_db();
        db.store_metadata("file1", 42, "root", "file1.txt", Some("abc123"), false).unwrap();
        let m = db.get_metadata("file1").expect("must be cached");
        assert_eq!(m.remote_id, "file1");
        assert_eq!(m.inode, 42);
        assert_eq!(m.parent_id, "root");
        assert_eq!(m.name, "file1.txt");
        assert_eq!(m.md5_checksum.as_deref(), Some("abc123"));
        assert!(!m.is_dirty);
    }

    #[test]
    fn get_metadata_missing_returns_none() {
        let db = make_db();
        assert!(db.get_metadata("nonexistent").is_none());
    }

    #[test]
    fn metadata_upsert_updates_existing_row() {
        let db = make_db();
        db.store_metadata("f1", 1, "root", "f1.txt", Some("old_md5"), false).unwrap();
        db.store_metadata("f1", 1, "parent2", "f1.txt", Some("new_md5"), true).unwrap();
        let m = db.get_metadata("f1").unwrap();
        assert_eq!(m.parent_id, "parent2");
        assert_eq!(m.md5_checksum.as_deref(), Some("new_md5"));
        assert!(m.is_dirty);
    }

    #[test]
    fn mark_and_clear_dirty() {
        let db = make_db();
        db.store_metadata("f1", 1, "root", "", None, false).unwrap();
        db.mark_dirty("f1");
        assert!(db.get_metadata("f1").unwrap().is_dirty);
        db.clear_dirty("f1");
        assert!(!db.get_metadata("f1").unwrap().is_dirty);
    }

    #[test]
    fn remove_entry_clears_both_tables() {
        let db = make_db();
        db.store_metadata("f1", 1, "root", "", None, false).unwrap();
        db.store_small_file("f1", b"hello");
        db.remove_entry("f1");
        assert!(db.get_metadata("f1").is_none());
        assert!(db.get_small_file("f1").is_none());
    }

    #[test]
    fn small_file_store_and_get_roundtrip() {
        let db = make_db();
        db.store_small_file("sf1", b"tiny content");
        let got = db.get_small_file("sf1").expect("must be present");
        assert_eq!(got, b"tiny content");
    }

    #[test]
    fn small_file_missing_returns_none() {
        let db = make_db();
        assert!(db.get_small_file("missing").is_none());
    }

    #[test]
    fn small_file_upsert_replaces_data() {
        let db = make_db();
        db.store_small_file("sf1", b"old");
        db.store_small_file("sf1", b"new content");
        assert_eq!(db.get_small_file("sf1").unwrap(), b"new content");
    }

    #[test]
    fn sync_token_set_and_get() {
        let db = make_db();
        assert!(db.get_sync_token().is_none());
        db.set_sync_token("token-abc");
        assert_eq!(db.get_sync_token().as_deref(), Some("token-abc"));
        db.set_sync_token("token-xyz");
        assert_eq!(db.get_sync_token().as_deref(), Some("token-xyz"));
    }

    #[test]
    fn list_all_remote_ids_empty_and_populated() {
        let db = make_db();
        assert!(db.list_all_remote_ids().is_empty());
        db.store_metadata("a", 1, "", "", None, false).unwrap();
        db.store_metadata("b", 2, "", "", None, false).unwrap();
        let mut ids = db.list_all_remote_ids();
        ids.sort();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[test]
    fn gc_removes_orphaned_content_files() {
        let tmp = tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let content_dir = tmp.path().join("content");
        std::fs::create_dir_all(&content_dir).unwrap();

        let db = DbManager::new(&db_path).unwrap();
        // Clean file: stored under its MD5 checksum (CAS key).
        db.store_metadata("clean-id", 1, "", "clean.txt", Some("deadbeef"), false)
            .unwrap();
        std::fs::write(content_dir.join("deadbeef"), b"data").unwrap();
        // Dirty file: stored under its remote_id.
        db.store_metadata("dirty-id", 2, "", "dirty.txt", None, true).unwrap();
        std::fs::write(content_dir.join("dirty-id"), b"dirty").unwrap();
        // Orphan file: no matching metadata row.
        std::fs::write(content_dir.join("orphan-id"), b"data").unwrap();
        // Temp file — must be skipped regardless.
        std::fs::write(content_dir.join("orphan-id.tmp"), b"partial").unwrap();

        db.run_gc(&content_dir);

        assert!(content_dir.join("deadbeef").exists(), "md5-keyed file must be kept");
        assert!(content_dir.join("dirty-id").exists(), "dirty remote_id file must be kept");
        assert!(!content_dir.join("orphan-id").exists(), "orphan-id must be removed");
        assert!(
            content_dir.join("orphan-id.tmp").exists(),
            ".tmp files must be skipped by GC"
        );
    }

    #[test]
    fn gc_on_missing_content_dir_is_silent() {
        let db = make_db();
        // Should not panic or log an error for a non-existent dir.
        db.run_gc(std::path::Path::new("/tmp/this-path-does-not-exist-gdrive-test-xyz"));
    }

    #[test]
    fn list_dirty_entries_returns_only_dirty() {
        let db = make_db();
        db.store_metadata("clean", 1, "p", "clean.txt", Some("md5a"), false).unwrap();
        db.store_metadata("dirty", 2, "p", "dirty.txt", Some("md5b"), true).unwrap();
        let entries = db.list_dirty_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].remote_id, "dirty");
        assert_eq!(entries[0].name, "dirty.txt");
    }

    #[test]
    fn clear_dirty_after_upload_updates_md5() {
        let db = make_db();
        db.store_metadata("f", 1, "p", "f.txt", None, true).unwrap();
        db.clear_dirty_after_upload("f", Some("new_md5"));
        let m = db.get_metadata("f").unwrap();
        assert!(!m.is_dirty);
        assert_eq!(m.md5_checksum.as_deref(), Some("new_md5"));
    }

    #[test]
    fn finalize_new_file_swaps_pending_id() {
        let db = make_db();
        db.store_metadata("__pending__42", 3, "parent", "new.txt", None, true).unwrap();
        db.store_small_file("__pending__42", b"content");
        db.finalize_new_file("__pending__42", "real-drive-id", Some("abc"));
        assert!(db.get_metadata("__pending__42").is_none());
        let m = db.get_metadata("real-drive-id").expect("must exist");
        assert!(!m.is_dirty);
        assert_eq!(m.md5_checksum.as_deref(), Some("abc"));
        // Content is now stored under the MD5 CAS key, not the real Drive ID.
        assert_eq!(db.get_small_file("abc").as_deref(), Some(b"content" as &[u8]));
        assert!(db.get_small_file("real-drive-id").is_none());
    }
}
