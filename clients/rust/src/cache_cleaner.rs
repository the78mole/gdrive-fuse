//! Background disk-cache cleaner — enforces a configurable byte budget on
//! `~/.cache/gdrive-fuse-rs/content/` using a Least-Recently-Used (LRU)
//! eviction policy.
//!
//! # Safety invariant
//!
//! A cache entry is **never** evicted while its `is_dirty` flag in SQLite is
//! `1`.  Evicting an unsynchronised local modification would permanently lose
//! user data.  The cleaner always queries `DbManager` before unlinking a file.
//!
//! # LRU ordering
//!
//! File `atime` (access timestamp) is used as the recency key — the file with
//! the oldest access time is evicted first.  If the filesystem was mounted
//! with `noatime`, `mtime` is used as a fallback.

use crate::db_manager::DbManager;
use log::{debug, error, info, warn};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

/// How often the cleaner wakes to check the cache size.
const CLEAN_INTERVAL: Duration = Duration::from_secs(5 * 60); // 5 minutes

/// Background cache-size enforcer.
pub struct CacheCleaner {
    db: Arc<DbManager>,
    content_dir: PathBuf,
    /// Maximum total byte size of the on-disk content cache before LRU
    /// eviction kicks in.  Set from [`Config::cache.disk_max_bytes`] at
    /// construction time.
    max_disk_bytes: u64,
}

impl CacheCleaner {
    pub fn new(db: Arc<DbManager>, content_dir: PathBuf, max_disk_bytes: u64) -> Self {
        Self { db, content_dir, max_disk_bytes }
    }

    /// Spawn the background `"gdrive-cache-cleaner"` thread.
    pub fn start(self: Arc<Self>) {
        let max_gib = self.max_disk_bytes / (1024 * 1024 * 1024);
        std::thread::Builder::new()
            .name("gdrive-cache-cleaner".to_string())
            .spawn(move || self.run())
            .expect("spawn gdrive-cache-cleaner thread");
        info!(
            "cache-cleaner: started (limit={} GiB, interval={:?})",
            max_gib,
            CLEAN_INTERVAL
        );
    }

    fn run(&self) {
        loop {
            std::thread::sleep(CLEAN_INTERVAL);
            self.clean_once();
        }
    }

    fn clean_once(&self) {
        let entries = match self.scan_content_dir() {
            Ok(e) => e,
            Err(e) => {
                error!("cache-cleaner: scan failed: {:#}", e);
                return;
            }
        };

        let total_bytes: u64 = entries.iter().map(|(_, sz, _)| sz).sum();
        debug!(
            "cache-cleaner: total={} MiB, limit={} GiB",
            total_bytes / (1024 * 1024),
            self.max_disk_bytes / (1024 * 1024 * 1024)
        );

        if total_bytes <= self.max_disk_bytes {
            return;
        }

        let over_by = total_bytes - self.max_disk_bytes;
        info!(
            "cache-cleaner: {} MiB over limit — starting LRU eviction",
            over_by / (1024 * 1024)
        );

        // Sort by atime ascending so the oldest entry is evicted first.
        let mut sorted = entries;
        sorted.sort_by_key(|(_, _, atime)| *atime);

        let mut freed: u64 = 0;
        for (path, size, _atime) in sorted {
            if freed >= over_by {
                break;
            }

            let file_id = path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();

            // Skip atomic temp files produced by DiskCache::insert.
            if file_id.ends_with(".tmp") {
                continue;
            }

            // With CAS, content files are named by MD5 checksum; they have no
            // metadata row keyed to that hash.  CAS-keyed files are always
            // clean and can be freely evicted — falling through is correct.
            // However, __pending__ files must never be evicted until upload.
            if file_id.starts_with("__pending__") {
                debug!("cache-cleaner: skipping pending file '{}'", file_id);
                continue;
            }

            // Safety invariant: never evict a file with pending local writes.
            if let Some(meta) = self.db.get_metadata(&file_id) {
                if meta.is_dirty {
                    debug!("cache-cleaner: skipping dirty file '{}'", file_id);
                    continue;
                }
            }

            match std::fs::remove_file(&path) {
                Ok(()) => {
                    info!("cache-cleaner: evicted '{}' ({} KiB)", file_id, size / 1024);
                    freed += size;
                }
                Err(e) => {
                    warn!("cache-cleaner: remove '{}': {}", path.display(), e);
                }
            }
        }

        if freed > 0 {
            info!("cache-cleaner: freed {} MiB", freed / (1024 * 1024));
        }
    }

    /// Scan the content directory and return `(path, bytes, atime_secs)` for
    /// every non-temp file found.  Returns an empty `Vec` when the directory
    /// does not yet exist.
    fn scan_content_dir(&self) -> anyhow::Result<Vec<(PathBuf, u64, u64)>> {
        if !self.content_dir.exists() {
            return Ok(vec![]);
        }
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(&self.content_dir)?.flatten() {
            let path = entry.path();
            let meta = match path.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            let size = meta.len();
            // Prefer atime; fall back to mtime for `noatime` mounts.
            let atime = meta
                .accessed()
                .or_else(|_| meta.modified())
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            entries.push((path, size, atime));
        }
        Ok(entries)
    }
}
