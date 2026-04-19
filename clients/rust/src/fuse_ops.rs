//! FUSE filesystem operations — bridges the kernel FUSE interface with
//! `ObjectManager` (cache) and `QueueManager` (Drive API work queue).
//!
//! # Concurrency model
//!
//! `fuser` serialises all FUSE callbacks through `&mut self`, so the handler
//! itself is single-threaded.  All Drive API calls are dispatched to the
//! worker pool via `QueueManager::enqueue_and_wait` (blocking) or
//! `QueueManager::enqueue` (fire-and-forget).  Cache reads go directly to
//! `ObjectManager` — `DashMap` shard-locks handle concurrent access between
//! the FUSE thread and worker threads.
//!
//! # Cache-first strategy
//!
//! Every FUSE callback checks `ObjectManager` first.  Only on a cache miss
//! (or TTL expiry) is a task submitted to `QueueManager`.  After the worker
//! returns the callback re-reads `ObjectManager` — the worker has already
//! written the result there.

use crate::dup_mapping::DupMapping;
use crate::gclient::FileInfo;
use crate::object_manager::{
    ObjectManager, ROOT_INO, SMALL_FILE_MAX_BYTES, TTL, desktop_content, is_workspace_type,
};
use crate::queue_manager::{Priority, QueueManager, TaskKey};
use fuser::{FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, Request};
use log::{debug, error, info};
use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// ── GDriveFuse ─────────────────────────────────────────────────────────────

/// FUSE filesystem handler.
pub struct GDriveFuse {
    obj: Arc<ObjectManager>,
    queue: Arc<QueueManager>,
    dup_map: Arc<DupMapping>,
}

impl GDriveFuse {
    pub fn new(
        obj: Arc<ObjectManager>,
        queue: Arc<QueueManager>,
        dup_map: Arc<DupMapping>,
    ) -> Self {
        // Kick off an eager root prefetch immediately after mount so the root
        // listing is already in cache when the user first runs `ls`.
        {
            let q = Arc::clone(&queue);
            let o = Arc::clone(&obj);
            std::thread::Builder::new()
                .name("startup-prefetch".to_string())
                .spawn(move || {
                    match q.enqueue_and_wait(
                        TaskKey::FetchDir("root".to_string()),
                        Priority::Normal,
                    ) {
                        Ok(_) => {
                            let files = o.get_dir_files("root").unwrap_or_default();
                            let n_dirs = files.iter().filter(|f| f.is_folder).count();
                            let n_small = files
                                .iter()
                                .filter(|f| {
                                    !f.is_folder
                                        && !f.mime_type
                                            .starts_with("application/vnd.google-apps.")
                                        && f.size > 0
                                        && f.size <= SMALL_FILE_MAX_BYTES
                                })
                                .count();
                            info!(
                                "startup prefetch: root has {} entries, \
                                 {} dirs, {} small files",
                                files.len(),
                                n_dirs,
                                n_small
                            );
                            // Prefetch all root subdirectories (Normal priority).
                            for f in files.iter().filter(|f| f.is_folder) {
                                q.enqueue(TaskKey::FetchDir(f.id.clone()), Priority::Normal);
                            }
                            // Pre-download small files so background readers are instant.
                            for f in files.iter().filter(|f| {
                                !f.is_folder
                                    && !f.mime_type
                                        .starts_with("application/vnd.google-apps.")
                                    && f.size > 0
                                    && f.size <= SMALL_FILE_MAX_BYTES
                            }) {
                                q.enqueue(TaskKey::DownloadFile(f.id.clone()), Priority::Low);
                            }
                        }
                        Err(e) => error!("startup prefetch failed: {}", e),
                    }
                })
                .ok();
        }
        Self { obj, queue, dup_map }
    }

    // ── Directory helper ──────────────────────────────────────────────────

    /// Return a fresh directory listing, fetching via the queue if the cache is
    /// missing or expired.  After every cold fetch, child directories that are
    /// not yet cached are enqueued for background prefetch (Low priority).
    fn get_dir(&self, parent_id: &str) -> Option<Vec<FileInfo>> {
        // Fast path — entry still within TTL.
        if let Some(files) = self.obj.get_cached_dir(parent_id) {
            return Some(files);
        }

        // Stale or missing — enqueue a fetch and block until the worker delivers.
        if let Err(e) =
            self.queue.enqueue_and_wait(TaskKey::FetchDir(parent_id.to_string()), Priority::High)
        {
            error!("get_dir '{}': {}", parent_id, e);
            return None;
        }

        // Re-read regardless of TTL — the worker just stored a fresh result.
        let files = self.obj.get_dir_files(parent_id)?;

        // Fire background prefetch for child dirs not yet in cache.
        for f in files.iter().filter(|f| f.is_folder) {
            if !self.obj.has_cache_entry(&f.id) {
                self.queue.enqueue(TaskKey::FetchDir(f.id.clone()), Priority::Low);
            }
        }

        Some(files)
    }

    // ── FileAttr helpers ──────────────────────────────────────────────────

    fn root_attr() -> FileAttr {
        let now = SystemTime::now();
        FileAttr {
            ino: ROOT_INO,
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
    }
}

// ── Filesystem trait ───────────────────────────────────────────────────────

impl Filesystem for GDriveFuse {
    // ── lookup ─────────────────────────────────────────────────────────────

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuser::ReplyEntry) {
        debug!("lookup parent={} name={:?}", parent, name);

        let Some(parent_id) = self.obj.ino_to_drive_id(parent) else {
            reply.error(libc::ENOENT);
            return;
        };
        let name_str = name.to_string_lossy();

        // Fetch (or return cached) listing and scan with duplicate-aware names.
        // The name-index is intentionally bypassed here: it stores raw base
        // names and would return wrong results for suffixed duplicates.
        let Some(files) = self.get_dir(&parent_id) else {
            reply.error(libc::EIO);
            return;
        };
        for (unique_name, f) in self.dup_map.resolve(&files) {
            if unique_name == name_str.as_ref() {
                let ino = self.obj.get_or_alloc_ino(&f.id);
                reply.entry(&TTL, &ObjectManager::make_file_attr(ino, f), 0);
                return;
            }
        }
        reply.error(libc::ENOENT);
    }

    // ── getattr ────────────────────────────────────────────────────────────

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr ino={}", ino);

        if ino == ROOT_INO {
            reply.attr(&TTL, &Self::root_attr());
            return;
        }

        let Some(file_id) = self.obj.ino_to_drive_id(ino) else {
            reply.error(libc::ENOENT);
            return;
        };

        // Fast path: metadata already in cache.
        if let Some(info) = self.obj.get_metadata(&file_id) {
            reply.attr(&TTL, &ObjectManager::make_file_attr(ino, &info));
            return;
        }

        // Metadata miss — fetch via queue.
        if let Err(e) = self
            .queue
            .enqueue_and_wait(TaskKey::GetMetadata(file_id.clone()), Priority::High)
        {
            error!("getattr ino={}: {}", ino, e);
            reply.error(libc::EIO);
            return;
        }
        match self.obj.get_metadata(&file_id) {
            Some(info) => reply.attr(&TTL, &ObjectManager::make_file_attr(ino, &info)),
            None => {
                error!("getattr ino={}: metadata missing after fetch", ino);
                reply.error(libc::EIO);
            }
        }
    }

    // ── readdir ────────────────────────────────────────────────────────────

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("readdir ino={} offset={}", ino, offset);

        let Some(parent_id) = self.obj.ino_to_drive_id(ino) else {
            reply.error(libc::ENOENT);
            return;
        };

        let Some(files) = self.get_dir(&parent_id) else {
            error!("readdir ino={}: get_dir returned None for '{}'", ino, parent_id);
            reply.error(libc::EIO);
            return;
        };

        let mut entries: Vec<(u64, FileType, String)> = vec![
            (ino, FileType::Directory, ".".to_string()),
            (ino, FileType::Directory, "..".to_string()),
        ];

        // Google Drive allows multiple files with the same name in one directory.
        // Duplicate display names in a FUSE readdir response cause the kernel
        // to return EIO.  Names are resolved via DupMapping which assigns
        // stable, persistent suffixes: `Bild.jpg` → `Bild (1).jpg`.
        for (unique_name, f) in self.dup_map.resolve(&files) {
            let child_ino = self.obj.get_or_alloc_ino(&f.id);
            let kind = if f.is_folder { FileType::Directory } else { FileType::RegularFile };
            entries.push((child_ino, kind, unique_name));
        }

        let total = entries.len().saturating_sub(2);
        if offset == 0 {
            info!("readdir: {} entries in folder id={}", total, parent_id);
        }

        let mut added = 0usize;
        let mut stopped_at: Option<usize> = None;
        for (i, (child_ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(*child_ino, (i + 1) as i64, *kind, name) {
                stopped_at = Some(i);
                break;
            }
            added += 1;
        }
        debug!(
            "readdir ino={} offset={}: added {} entries, stopped_at={:?}",
            ino, offset, added, stopped_at
        );
        reply.ok();
    }

    // ── read ───────────────────────────────────────────────────────────────

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        debug!("read ino={} offset={} size={}", ino, offset, size);

        let Some(file_id) = self.obj.ino_to_drive_id(ino) else {
            reply.error(libc::ENOENT);
            return;
        };

        if offset == 0 {
            let name = self
                .obj
                .get_metadata(&file_id)
                .map(|f| f.name.clone())
                .unwrap_or_else(|| file_id.clone());
            info!("read: \"{}\" (id={})", name, file_id);
        }

        // Google Workspace files — synthesise .desktop bytes; no download needed.
        if let Some(info) = self.obj.get_metadata(&file_id) {
            if is_workspace_type(&info.mime_type) {
                let content = desktop_content(&info.name, &info.mime_type, &file_id);
                let start = (offset as usize).min(content.len());
                let end = (start + size as usize).min(content.len());
                reply.data(&content[start..end]);
                return;
            }
        }

        // Content cache hit.
        if let Some(content) = self.obj.get_content(&file_id) {
            let start = (offset as usize).min(content.len());
            let end = (start + size as usize).min(content.len());
            reply.data(&content[start..end]);
            return;
        }

        // Cache miss — download via queue and block.
        if let Err(e) =
            self.queue.enqueue_and_wait(TaskKey::DownloadFile(file_id.clone()), Priority::High)
        {
            error!("read ino={}: {}", ino, e);
            reply.error(libc::EIO);
            return;
        }

        match self.obj.get_content(&file_id) {
            Some(content) => {
                let start = (offset as usize).min(content.len());
                let end = (start + size as usize).min(content.len());
                reply.data(&content[start..end]);
            }
            None => {
                error!("read ino={}: content missing after download", ino);
                reply.error(libc::EIO);
            }
        }
    }
}
