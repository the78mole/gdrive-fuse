//! Object Manager — owns all cache state for the FUSE filesystem.
//!
//! `ObjectManager` is the single authoritative store for:
//! - inode ↔ Drive-ID mappings
//! - directory listings (with 3-state lifecycle: Fresh → Stale → Invalid)
//! - file metadata
//! - downloaded file content
//! - name index for O(1) `lookup` resolution
//!
//! All data structures are `DashMap` (16-shard RwLocks) so concurrent reads
//! and writes on different keys never contend.  The inode counter uses
//! `AtomicU64` — one CAS per new file.
//!
//! `ObjectManager` has **no knowledge** of the Google Drive API or the Queue
//! Manager.  Workers call `store_*` methods; FUSE callbacks call `get_*`
//! methods.

use crate::gclient::{DirListing, FileInfo};
use dashmap::DashMap;
use fuser::{FileAttr, FileType, INodeNo};
use log::{debug, info};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ── Constants ─────────────────────────────────────────────────────────────

pub const TTL: Duration = Duration::from_secs(30);
pub const ROOT_INO: u64 = 1;

/// Files at or below this size are enqueued for **proactive prefetch** by the
/// dedicated small-file worker pool.  Does not affect what gets cached —
/// see `CACHE_MAX_FILE_BYTES` for that.
pub const PREFETCH_MAX_BYTES: u64 = 512 * 1024; // 64 KiB

/// Files at or below this size are fully downloaded into the in-memory content
/// cache on first read.  Larger files are served via HTTP Range requests and
/// are not stored in the content cache.
pub const CACHE_MAX_FILE_BYTES: u64 = 1024 * 1024; // 1 MiB

/// Maximum total byte footprint of the in-memory content cache.
/// Least-recently-used entries are evicted once this limit is exceeded.
pub const CACHE_MAX_TOTAL_BYTES: u64 = 1024 * 1024 * 1024; // 1 GiB

// ── ContentCache — byte-bounded LRU ─────────────────────────────────────

/// Thread-safe in-memory content cache with byte-granularity FIFO eviction.
///
/// Entries are evicted in insertion order (FIFO) once the total stored bytes
/// exceed `max_bytes`.  A single mutex protects both the map and the running
/// byte counter so all operations are atomic.
pub struct ContentCache {
    inner: Mutex<ContentCacheInner>,
    max_bytes: u64,
}

struct ContentCacheInner {
    map: HashMap<String, Vec<u8>>,
    /// Insertion order used for eviction (FIFO).
    order: VecDeque<String>,
    total_bytes: u64,
}

impl ContentCache {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            inner: Mutex::new(ContentCacheInner {
                map: HashMap::new(),
                order: VecDeque::new(),
                total_bytes: 0,
            }),
            max_bytes,
        }
    }

    /// Look up a cached entry.
    pub fn get(&self, file_id: &str) -> Option<Vec<u8>> {
        self.inner.lock().map.get(file_id).cloned()
    }

    /// Remove an entry from the cache.
    pub fn remove(&self, file_id: &str) {
        let mut inner = self.inner.lock();
        if let Some(old) = inner.map.remove(file_id) {
            inner.total_bytes = inner.total_bytes.saturating_sub(old.len() as u64);
            inner.order.retain(|k| k != file_id);
        }
    }

    /// Insert or replace an entry, evicting oldest entries as needed.
    pub fn insert(&self, file_id: &str, data: Vec<u8>) {
        let new_size = data.len() as u64;
        let mut inner = self.inner.lock();

        // Refund the size of an existing entry that will be replaced.
        if let Some(old) = inner.map.remove(file_id) {
            inner.total_bytes = inner.total_bytes.saturating_sub(old.len() as u64);
            // Remove stale entry from the ordering queue.
            inner.order.retain(|k| k != file_id);
        }

        // Evict oldest entries until there is room for the new one.
        while self.max_bytes > 0 && inner.total_bytes + new_size > self.max_bytes {
            match inner.order.pop_front() {
                Some(evict_id) => {
                    if let Some(evicted) = inner.map.remove(&evict_id) {
                        let freed = evicted.len() as u64;
                        inner.total_bytes = inner.total_bytes.saturating_sub(freed);
                        info!("content-cache: evicted '{}' ({} bytes), total now {} bytes",
                            evict_id, freed, inner.total_bytes);
                    }
                }
                None => break, // cache is empty but new entry is still too big
            }
        }

        inner.total_bytes += new_size;
        inner.order.push_back(file_id.to_string());
        inner.map.insert(file_id.to_string(), data);
        debug!("content-cache: stored '{}' ({} bytes), total {} bytes",
            file_id, new_size, inner.total_bytes);
    }

    /// Number of entries currently in the cache (for testing).
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.lock().map.len()
    }

    /// Total bytes currently stored in the cache.
    #[cfg(test)]
    pub fn total_bytes(&self) -> u64 {
        self.inner.lock().total_bytes
    }
}



#[derive(Clone, PartialEq)]
pub enum DirCacheState {
    Fresh,
    #[allow(dead_code)]
    Stale,
    #[allow(dead_code)]
    Invalid,
}

/// One cached directory listing with explicit state and timestamp.
#[derive(Clone)]
pub struct DirEntry {
    pub files: Vec<FileInfo>,
    pub etag: String,
    pub fetched_at: std::time::Instant,
    pub state: DirCacheState,
    /// `false` while background workers are still fetching additional pages.
    /// FUSE `readdir` may serve a partial listing immediately; the kernel will
    /// ask again (via a new `opendir`/`readdir` cycle) and see the full result
    /// once this flag is `true`.
    pub is_complete: bool,
}

// ── ObjectManager ──────────────────────────────────────────────────────────

pub struct ObjectManager {
    next_ino: AtomicU64,
    ino_to_id: DashMap<u64, String>,
    id_to_ino: DashMap<String, u64>,
    /// Drive file ID → last-known metadata.
    pub metadata: DashMap<String, FileInfo>,
    /// Parent Drive ID → directory listing.
    pub dir_cache: DashMap<String, DirEntry>,
    /// Drive file ID → downloaded bytes (byte-bounded LRU cache).
    pub content_cache: ContentCache,
    /// `"{parent_id}:{display_name}"` → `file_id` — populated by every dir
    /// store so `lookup` can resolve in O(1) without scanning the listing.
    pub name_index: DashMap<String, String>,
}

impl ObjectManager {
    pub fn new() -> Self {
        let ino_to_id = DashMap::new();
        let id_to_ino = DashMap::new();
        ino_to_id.insert(ROOT_INO, "root".to_string());
        id_to_ino.insert("root".to_string(), ROOT_INO);
        Self {
            next_ino: AtomicU64::new(2),
            ino_to_id,
            id_to_ino,
            metadata: DashMap::new(),
            dir_cache: DashMap::new(),
            content_cache: ContentCache::new(CACHE_MAX_TOTAL_BYTES),
            name_index: DashMap::new(),
        }
    }

    // ── Inode management ──────────────────────────────────────────────────

    /// Allocate or reuse a stable inode for `file_id`.
    ///
    /// `DashMap::entry().or_insert_with()` provides shard-level atomicity —
    /// the same inode is never allocated twice even under concurrent calls.
    pub fn get_or_alloc_ino(&self, file_id: &str) -> u64 {
        *self
            .id_to_ino
            .entry(file_id.to_string())
            .or_insert_with(|| {
                let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
                self.ino_to_id.insert(ino, file_id.to_string());
                ino
            })
    }

    pub fn ino_to_drive_id(&self, ino: u64) -> Option<String> {
        self.ino_to_id.get(&ino).map(|r| r.clone())
    }

    // ── Directory cache reads ─────────────────────────────────────────────

    /// Returns the listing only if the entry is **Fresh and within TTL**.
    /// Returns `None` if missing, stale, or expired — the caller must enqueue
    /// a fetch.
    pub fn get_cached_dir(&self, parent_id: &str) -> Option<Vec<FileInfo>> {
        let entry = self.dir_cache.get(parent_id)?;
        if entry.state == DirCacheState::Fresh && entry.fetched_at.elapsed() < TTL {
            Some(entry.files.clone())
        } else {
            None
        }
    }

    /// Returns the listing regardless of TTL — used after a successful fetch
    /// to served the just-stored data.
    pub fn get_dir_files(&self, parent_id: &str) -> Option<Vec<FileInfo>> {
        self.dir_cache.get(parent_id).map(|e| e.files.clone())
    }

    /// Returns `true` if any cache entry (stale or fresh) exists for this dir.
    pub fn has_cache_entry(&self, parent_id: &str) -> bool {
        self.dir_cache.contains_key(parent_id)
    }

    /// Returns the stored ETag if the entry is expired (past TTL) and has a
    /// non-empty ETag — the worker should use `If-None-Match` instead of a
    /// cold fetch.  Returns `None` for missing, fresh, or ETag-less entries.
    pub fn get_stale_etag(&self, parent_id: &str) -> Option<String> {
        let entry = self.dir_cache.get(parent_id)?;
        if entry.fetched_at.elapsed() >= TTL && !entry.etag.is_empty() {
            Some(entry.etag.clone())
        } else {
            None
        }
    }

    // ── Directory cache writes ────────────────────────────────────────────

    /// Store the **first page** of a directory listing as a partial result.
    ///
    /// The entry is marked `is_complete = false`.  Call [`append_dir_listing`]
    /// when all remaining pages have been fetched.
    pub fn store_dir_partial(&self, parent_id: &str, files: Vec<FileInfo>, etag: String) {
        for f in &files {
            self.get_or_alloc_ino(&f.id);
            self.metadata.insert(f.id.clone(), f.clone());
            self.name_index
                .insert(make_name_key(parent_id, &f.name, &f.mime_type), f.id.clone());
        }
        debug!(
            "store_dir_partial('{}'): {} files (partial)",
            parent_id,
            files.len()
        );
        self.dir_cache.insert(
            parent_id.to_string(),
            DirEntry {
                files,
                etag,
                fetched_at: std::time::Instant::now(),
                state: DirCacheState::Fresh,
                is_complete: false,
            },
        );
    }

    /// Replace the directory listing with a complete result fetched across all
    /// pages and mark `is_complete = true`.
    #[allow(dead_code)]
    pub fn append_dir_listing(&self, parent_id: &str, listing: DirListing) {
        self.store_dir_listing(parent_id, listing);
    }

    /// Store a fresh directory listing and populate metadata + name index.
    pub fn store_dir_listing(&self, parent_id: &str, listing: DirListing) {
        for f in &listing.files {
            self.get_or_alloc_ino(&f.id);
            self.metadata.insert(f.id.clone(), f.clone());
            self.name_index
                .insert(make_name_key(parent_id, &f.name, &f.mime_type), f.id.clone());
        }
        debug!(
            "store_dir_listing('{}'): {} files, etag={}",
            parent_id,
            listing.files.len(),
            listing.etag
        );
        self.dir_cache.insert(
            parent_id.to_string(),
            DirEntry {
                files: listing.files,
                etag: listing.etag,
                fetched_at: std::time::Instant::now(),
                state: DirCacheState::Fresh,
                is_complete: true,
            },
        );
    }

    /// Mark an existing entry as fresh without changing its content (304 path).
    ///
    /// Returns the (now refreshed) listing, or `None` if the entry is gone
    /// (concurrent eviction — caller should enqueue a cold fetch).
    pub fn touch_dir(&self, parent_id: &str) -> Option<Vec<FileInfo>> {
        let mut entry = self.dir_cache.get_mut(parent_id)?;
        entry.fetched_at = std::time::Instant::now();
        entry.state = DirCacheState::Fresh;
        entry.is_complete = true;
        Some(entry.files.clone())
    }

    /// Returns `true` when the cached listing is complete (all pages fetched).
    pub fn is_dir_complete(&self, parent_id: &str) -> bool {
        self.dir_cache.get(parent_id).map(|e| e.is_complete).unwrap_or(false)
    }

    // ── Metadata ─────────────────────────────────────────────────────────

    pub fn get_metadata(&self, file_id: &str) -> Option<FileInfo> {
        self.metadata.get(file_id).map(|r| r.clone())
    }

    pub fn store_metadata(&self, info: FileInfo) {
        self.get_or_alloc_ino(&info.id);
        self.metadata.insert(info.id.clone(), info);
    }

    // ── Content cache ─────────────────────────────────────────────────────

    pub fn get_content(&self, file_id: &str) -> Option<Vec<u8>> {
        self.content_cache.get(file_id)
    }

    /// Store downloaded file content.  Files larger than `CACHE_MAX_FILE_BYTES`
    /// should not be passed here — callers in `fuse_ops` already guard this.
    /// LRU eviction happens automatically if the byte budget is exceeded.
    pub fn store_content(&self, file_id: &str, content: Vec<u8>) {
        self.content_cache.insert(file_id, content);
    }

    // ── Write-support helpers ─────────────────────────────────────────────

    /// Remove a directory listing from the cache so the next access triggers
    /// a fresh fetch from the Drive API.
    pub fn invalidate_dir(&self, parent_id: &str) {
        self.dir_cache.remove(parent_id);
        debug!("invalidate_dir('{}')", parent_id);
    }

    /// Evict file metadata and cached content for `file_id`.
    pub fn remove_metadata(&self, file_id: &str) {
        self.metadata.remove(file_id);
        self.content_cache.remove(file_id);
        debug!("remove_metadata('{}')", file_id);
    }

    /// Replace a temporary pending file ID (used for newly created files
    /// before their first upload) with the permanent Drive file ID.
    ///
    /// The inode assigned to `old_id` is reused for `new_info.id` so that
    /// open file handles remain valid across the flush.
    pub fn replace_pending_id(&self, old_id: &str, new_info: FileInfo) {
        if let Some((_, ino)) = self.id_to_ino.remove(old_id) {
            self.ino_to_id.insert(ino, new_info.id.clone());
            self.id_to_ino.insert(new_info.id.clone(), ino);
        }
        self.metadata.remove(old_id);
        self.metadata.insert(new_info.id.clone(), new_info);
        debug!("replace_pending_id: '{}' → recorded new id", old_id);
    }

    // ── FileAttr helpers (shared between fuse_ops and here) ───────────────

    pub fn make_file_attr(ino: u64, info: &FileInfo) -> FileAttr {
        let now = SystemTime::now();
        if info.is_folder {
            FileAttr {
                ino: INodeNo(ino),
                size: 0,
                blocks: 0,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: unsafe { libc::getuid() },
                gid: unsafe { libc::getgid() },
                rdev: 0,
                flags: 0,
                blksize: 512,
            }
        } else {
            let is_ws = is_workspace_type(&info.mime_type);
            let size = if is_ws {
                desktop_content(&info.name, &info.mime_type, &info.id).len() as u64
            } else {
                info.size
            };
            FileAttr {
                ino: INodeNo(ino),
                size,
                blocks: size.div_ceil(512),
                atime: now,
                mtime: now,
                ctime: now,
                crtime: UNIX_EPOCH,
                kind: FileType::RegularFile,
                perm: if is_ws { 0o755 } else { 0o644 },
                nlink: 1,
                uid: unsafe { libc::getuid() },
                gid: unsafe { libc::getgid() },
                rdev: 0,
                flags: 0,
                blksize: 512,
            }
        }
    }
}

// ── Free helpers (used by ObjectManager and fuse_ops) ─────────────────────

/// Build the name-index key for a child entry.
pub fn make_name_key(parent_id: &str, name: &str, mime: &str) -> String {
    if is_workspace_type(mime) {
        format!("{}:{}.desktop", parent_id, name)
    } else {
        format!("{}:{}", parent_id, name)
    }
}

pub fn is_workspace_type(mime: &str) -> bool {
    mime.starts_with("application/vnd.google-apps.")
        && mime != "application/vnd.google-apps.folder"
}

pub fn display_name(name: &str, mime: &str) -> String {
    // FUSE/VFS rejects any dirent whose name contains `/` (verify_dirent_name
    // returns -EIO).  Replace every occurrence with the visually similar
    // Division Slash U+2215 (∕) so the character is preserved for the user
    // while remaining kernel-safe.
    let safe = name.replace('/', "\u{2215}");
    if is_workspace_type(mime) {
        format!("{}.desktop", safe)
    } else {
        safe
    }
}

pub fn desktop_content(name: &str, mime: &str, file_id: &str) -> Vec<u8> {
    let (icon, comment) = workspace_icon_and_label(mime);
    format!(
        "[Desktop Entry]\nType=Link\nName={}\nURL={}\nIcon={}\nComment={}\n",
        name,
        workspace_url(mime, file_id),
        icon,
        comment,
    )
    .into_bytes()
}

fn workspace_url(mime: &str, file_id: &str) -> String {
    match mime {
        "application/vnd.google-apps.document" => {
            format!("https://docs.google.com/document/d/{}/edit", file_id)
        }
        "application/vnd.google-apps.spreadsheet" => {
            format!("https://docs.google.com/spreadsheets/d/{}/edit", file_id)
        }
        "application/vnd.google-apps.presentation" => {
            format!("https://docs.google.com/presentation/d/{}/edit", file_id)
        }
        "application/vnd.google-apps.form" => {
            format!("https://docs.google.com/forms/d/{}/edit", file_id)
        }
        "application/vnd.google-apps.drawing" => {
            format!("https://docs.google.com/drawings/d/{}/edit", file_id)
        }
        _ => format!("https://drive.google.com/open?id={}", file_id),
    }
}

fn workspace_icon_and_label(mime: &str) -> (&'static str, &'static str) {
    match mime {
        "application/vnd.google-apps.document" => ("x-office-document", "Google Docs"),
        "application/vnd.google-apps.spreadsheet" => ("x-office-spreadsheet", "Google Sheets"),
        "application/vnd.google-apps.presentation" => {
            ("x-office-presentation", "Google Slides")
        }
        "application/vnd.google-apps.form" => ("x-office-document", "Google Forms"),
        "application/vnd.google-apps.drawing" => ("image-x-generic", "Google Drawings"),
        "application/vnd.google-apps.map" => ("text-html", "Google Maps"),
        "application/vnd.google-apps.site" => ("text-html", "Google Sites"),
        _ => ("text-html", "Google Drive"),
    }
}

// ── Unit Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gclient::{DirListing, FileInfo};
    use fuser::FileType;
    use std::time::Duration;

    // ── helpers ───────────────────────────────────────────────────────────

    fn file(id: &str, name: &str, mime: &str, size: u64, is_folder: bool) -> FileInfo {
        FileInfo {
            id: id.to_string(),
            name: name.to_string(),
            mime_type: mime.to_string(),
            size,
            modified_time: String::new(),
            is_folder,
        }
    }

    fn listing(files: Vec<FileInfo>, etag: &str) -> DirListing {
        DirListing { files, etag: etag.to_string() }
    }

    fn stale_entry(files: Vec<FileInfo>, etag: &str) -> DirEntry {
        DirEntry {
            files,
            etag: etag.to_string(),
            fetched_at: std::time::Instant::now() - Duration::from_secs(60),
            state: DirCacheState::Fresh,
            is_complete: true,
        }
    }

    // ── inode management ──────────────────────────────────────────────────

    #[test]
    fn root_ino_preloaded() {
        let obj = ObjectManager::new();
        assert_eq!(obj.ino_to_drive_id(ROOT_INO), Some("root".to_string()));
    }

    #[test]
    fn get_or_alloc_ino_idempotent() {
        let obj = ObjectManager::new();
        let a = obj.get_or_alloc_ino("file-abc");
        let b = obj.get_or_alloc_ino("file-abc");
        assert_eq!(a, b);
    }

    #[test]
    fn get_or_alloc_ino_unique_per_id() {
        let obj = ObjectManager::new();
        let a = obj.get_or_alloc_ino("id-a");
        let b = obj.get_or_alloc_ino("id-b");
        assert_ne!(a, b);
    }

    #[test]
    fn ino_to_drive_id_roundtrip() {
        let obj = ObjectManager::new();
        let ino = obj.get_or_alloc_ino("xyz");
        assert_eq!(obj.ino_to_drive_id(ino), Some("xyz".to_string()));
        assert_eq!(obj.ino_to_drive_id(9999), None);
    }

    // ── directory cache ───────────────────────────────────────────────────

    #[test]
    fn store_dir_caches_fresh_listing() {
        let obj = ObjectManager::new();
        let f = file("f1", "test.txt", "text/plain", 100, false);
        obj.store_dir_listing("parent-1", listing(vec![f], "etag-1"));
        let result = obj.get_cached_dir("parent-1");
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn get_cached_dir_missing_returns_none() {
        let obj = ObjectManager::new();
        assert!(obj.get_cached_dir("nonexistent").is_none());
    }

    #[test]
    fn get_cached_dir_expired_returns_none() {
        let obj = ObjectManager::new();
        obj.dir_cache.insert("old".to_string(), stale_entry(vec![], "e"));
        assert!(obj.get_cached_dir("old").is_none());
    }

    #[test]
    fn has_cache_entry_absent_and_present() {
        let obj = ObjectManager::new();
        assert!(!obj.has_cache_entry("xyz"));
        obj.store_dir_listing("xyz", listing(vec![], "e"));
        assert!(obj.has_cache_entry("xyz"));
    }

    #[test]
    fn has_cache_entry_true_even_when_stale() {
        let obj = ObjectManager::new();
        obj.dir_cache.insert("stale".to_string(), stale_entry(vec![], "e"));
        assert!(obj.has_cache_entry("stale"));
    }

    // ── stale ETag ────────────────────────────────────────────────────────

    #[test]
    fn get_stale_etag_fresh_entry_is_none() {
        let obj = ObjectManager::new();
        obj.store_dir_listing("d1", listing(vec![], "my-etag"));
        assert!(obj.get_stale_etag("d1").is_none());
    }

    #[test]
    fn get_stale_etag_expired_with_etag_returns_some() {
        let obj = ObjectManager::new();
        obj.dir_cache.insert("d2".to_string(), stale_entry(vec![], "stale-etag"));
        assert_eq!(obj.get_stale_etag("d2"), Some("stale-etag".to_string()));
    }

    #[test]
    fn get_stale_etag_expired_empty_etag_is_none() {
        // An empty ETag would produce `If-None-Match: ""` which Drive treats as
        // a match even when nothing was cached — must fall back to cold fetch.
        let obj = ObjectManager::new();
        obj.dir_cache.insert("d3".to_string(), stale_entry(vec![], ""));
        assert!(obj.get_stale_etag("d3").is_none());
    }

    // ── touch_dir ─────────────────────────────────────────────────────────

    #[test]
    fn touch_dir_makes_stale_entry_fresh_again() {
        let obj = ObjectManager::new();
        let f = file("fid", "a.txt", "text/plain", 10, false);
        obj.dir_cache.insert("d".to_string(), stale_entry(vec![f], "etag"));
        // Before touch: stale → get_cached_dir returns None, stale_etag returns Some
        assert!(obj.get_cached_dir("d").is_none());
        assert!(obj.get_stale_etag("d").is_some());
        // Touch refreshes the timestamp
        let files = obj.touch_dir("d");
        assert!(files.is_some());
        assert_eq!(files.unwrap().len(), 1);
        // After touch: fresh within TTL
        assert!(obj.get_cached_dir("d").is_some());
        assert!(obj.get_stale_etag("d").is_none());
    }

    #[test]
    fn touch_dir_missing_returns_none() {
        let obj = ObjectManager::new();
        assert!(obj.touch_dir("ghost").is_none());
    }

    // ── store_dir_listing side-effects ────────────────────────────────────

    #[test]
    fn store_dir_populates_metadata_and_name_index() {
        let obj = ObjectManager::new();
        let f = file("id1", "Report", "application/vnd.google-apps.document", 0, false);
        obj.store_dir_listing("parent", listing(vec![f], "e"));
        // metadata populated
        assert!(obj.get_metadata("id1").is_some());
        // name_index: workspace → ".desktop" suffix
        assert!(obj.name_index.contains_key("parent:Report.desktop"));
    }

    #[test]
    fn store_dir_allocates_inodes_for_children() {
        let obj = ObjectManager::new();
        let f = file("child-id", "file.txt", "text/plain", 50, false);
        obj.store_dir_listing("p", listing(vec![f], "e"));
        let ino = obj.get_or_alloc_ino("child-id");
        assert_eq!(obj.ino_to_drive_id(ino), Some("child-id".to_string()));
    }

    // ── metadata ──────────────────────────────────────────────────────────

    #[test]
    fn store_and_get_metadata_roundtrip() {
        let obj = ObjectManager::new();
        let f = file("meta-id", "doc.txt", "text/plain", 512, false);
        obj.store_metadata(f);
        let got = obj.get_metadata("meta-id").expect("must be present");
        assert_eq!(got.name, "doc.txt");
        assert_eq!(got.size, 512);
    }

    #[test]
    fn get_metadata_missing_returns_none() {
        let obj = ObjectManager::new();
        assert!(obj.get_metadata("missing").is_none());
    }

    // ── content cache ─────────────────────────────────────────────────────

    #[test]
    fn store_and_get_content_roundtrip() {
        let obj = ObjectManager::new();
        obj.store_content("f1", vec![1, 2, 3]);
        assert_eq!(obj.get_content("f1"), Some(vec![1u8, 2, 3]));
    }

    #[test]
    fn get_content_missing_returns_none() {
        let obj = ObjectManager::new();
        assert!(obj.get_content("missing").is_none());
    }

    #[test]
    fn store_content_evicts_fifo_on_overflow() {
        // Build a cache that holds at most 10 bytes.
        let cache = ContentCache::new(10);
        // Insert two entries: 4 + 4 = 8 bytes (fits).
        cache.insert("a", vec![0u8; 4]);
        cache.insert("b", vec![0u8; 4]);
        assert_eq!(cache.total_bytes(), 8);
        // Insert 5 bytes — evicts "a" (oldest, FIFO: 4 bytes freed → 4 + 5 = 9 ≤ 10).
        cache.insert("c", vec![0u8; 5]);
        assert!(cache.get("a").is_none(), "a should have been evicted (oldest)");
        assert!(cache.get("b").is_some());
        assert!(cache.get("c").is_some());
        assert!(cache.total_bytes() <= 10);
    }

    #[test]
    fn store_content_replace_updates_size() {
        let cache = ContentCache::new(100);
        cache.insert("f1", vec![0u8; 10]);
        assert_eq!(cache.total_bytes(), 10);
        // Replace with a smaller value — total should shrink.
        cache.insert("f1", vec![0u8; 3]);
        assert_eq!(cache.total_bytes(), 3);
        assert_eq!(cache.len(), 1);
    }

    // ── free helpers ─────────────────────────────────────────────────────

    #[test]
    fn make_name_key_regular_file() {
        assert_eq!(make_name_key("parent", "file.txt", "text/plain"), "parent:file.txt");
    }

    #[test]
    fn make_name_key_workspace_gets_desktop_suffix() {
        assert_eq!(
            make_name_key("p", "Doc", "application/vnd.google-apps.document"),
            "p:Doc.desktop"
        );
    }

    #[test]
    fn make_name_key_folder_no_suffix() {
        assert_eq!(
            make_name_key("p", "Folder", "application/vnd.google-apps.folder"),
            "p:Folder"
        );
    }

    #[test]
    fn is_workspace_type_classification() {
        assert!(is_workspace_type("application/vnd.google-apps.document"));
        assert!(is_workspace_type("application/vnd.google-apps.spreadsheet"));
        assert!(is_workspace_type("application/vnd.google-apps.presentation"));
        assert!(!is_workspace_type("application/vnd.google-apps.folder"));
        assert!(!is_workspace_type("text/plain"));
        assert!(!is_workspace_type("image/png"));
    }

    #[test]
    fn display_name_workspace_adds_desktop() {
        assert_eq!(
            display_name("Sheet", "application/vnd.google-apps.spreadsheet"),
            "Sheet.desktop"
        );
    }

    #[test]
    fn display_name_regular_unchanged() {
        assert_eq!(display_name("file.txt", "text/plain"), "file.txt");
    }

    #[test]
    fn display_name_folder_unchanged() {
        assert_eq!(display_name("Docs", "application/vnd.google-apps.folder"), "Docs");
    }

    #[test]
    fn desktop_content_contains_required_fields() {
        let c =
            desktop_content("My Doc", "application/vnd.google-apps.document", "doc-id-123");
        let s = std::str::from_utf8(&c).unwrap();
        assert!(s.contains("[Desktop Entry]"));
        assert!(s.contains("Type=Link"));
        assert!(s.contains("Name=My Doc"));
        assert!(s.contains("https://docs.google.com/document/d/doc-id-123/edit"));
    }

    // ── make_file_attr ────────────────────────────────────────────────────

    #[test]
    fn make_file_attr_for_folder() {
        let f = file("folder-id", "Folder", "application/vnd.google-apps.folder", 0, true);
        let attr = ObjectManager::make_file_attr(5, &f);
        assert_eq!(attr.ino, INodeNo(5));
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.size, 0);
        assert_eq!(attr.perm, 0o755);
        assert_eq!(attr.nlink, 2);
    }

    #[test]
    fn make_file_attr_for_regular_file() {
        let f = file("reg-id", "image.png", "image/png", 2048, false);
        let attr = ObjectManager::make_file_attr(10, &f);
        assert_eq!(attr.ino, INodeNo(10));
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.size, 2048);
        assert_eq!(attr.perm, 0o644);
        assert_eq!(attr.nlink, 1);
        assert_eq!(attr.blocks, 2048u64.div_ceil(512));
    }

    #[test]
    fn make_file_attr_for_workspace_file() {
        let f = file("ws-id", "Sheet", "application/vnd.google-apps.spreadsheet", 0, false);
        let attr = ObjectManager::make_file_attr(20, &f);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o755);
        let expected =
            desktop_content("Sheet", "application/vnd.google-apps.spreadsheet", "ws-id")
                .len() as u64;
        assert_eq!(attr.size, expected);
    }
}
