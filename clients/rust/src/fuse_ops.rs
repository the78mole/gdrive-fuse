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
    CACHE_MAX_FILE_BYTES, ObjectManager, PREFETCH_MAX_BYTES, ROOT_INO, TTL, desktop_content,
    is_workspace_type,
};
use crate::queue_manager::{Priority, QueueManager, TaskKey, TaskResult};
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
                    // Use FetchDir (full, all pages) for startup so the root
                    // listing is complete before any user interaction.
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
                                        && f.size <= PREFETCH_MAX_BYTES
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
                            // Pre-download files ≤ PREFETCH_MAX_BYTES (64 KiB) on Low
                            // priority — served by the 24 dedicated small-file workers.
                            for f in files.iter().filter(|f| {
                                !f.is_folder
                                    && !f.mime_type
                                        .starts_with("application/vnd.google-apps.")
                                    && f.size > 0
                                    && f.size <= PREFETCH_MAX_BYTES
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
    /// missing or expired.
    ///
    /// **Progressive strategy**: on a cold miss, only the first page is awaited
    /// (fast, typically < 200 ms).  If more pages exist, a `FetchDirPages`
    /// continuation is enqueued at Low priority so the partial listing is
    /// available to `readdir` immediately.  Subsequent `readdir` calls
    /// (triggered by the file manager's periodic refresh or next `opendir`)
    /// will see a growing listing until `is_complete` is true.
    ///
    /// After every fetch, child directories not yet cached are enqueued for
    /// background prefetch at Low priority.
    fn get_dir(&self, parent_id: &str) -> Option<Vec<FileInfo>> {
        // Fast path 1 — entry still within TTL (partial or complete).
        if let Some(files) = self.obj.get_cached_dir(parent_id) {
            return Some(files);
        }

        // Fast path 2 — stale entry: serve immediately and fire a background
        // ETag revalidation (stale-while-revalidate).  The listing will be
        // refreshed for the next readdir cycle without blocking the user.
        if let Some(files) = self.obj.get_dir_files(parent_id) {
            self.queue
                .enqueue(TaskKey::FetchDir(parent_id.to_string()), Priority::Normal);
            // Prefetch child dirs that are not yet cached.
            for f in files.iter().filter(|f| f.is_folder) {
                if !self.obj.has_cache_entry(&f.id) {
                    self.queue.enqueue(TaskKey::FetchDir(f.id.clone()), Priority::Low);
                }
            }
            return Some(files);
        }

        // Complete miss — fetch the first page and block until it arrives.
        match self
            .queue
            .enqueue_and_wait(TaskKey::FetchDirFirstPage(parent_id.to_string()), Priority::High)
        {
            Ok(TaskResult::DirListingPartial(Some((page_token, etag)))) => {
                // More pages exist — enqueue the continuation at Low priority.
                // fuse_ops owns the queue Arc so we can enqueue directly here.
                self.queue.enqueue(
                    TaskKey::FetchDirPages(parent_id.to_string(), page_token, etag),
                    Priority::Low,
                );
            }
            Ok(TaskResult::DirListing) | Ok(TaskResult::DirListingPartial(None)) => {
                // Single-page or already complete — nothing more to do.
            }
            Err(e) => {
                error!("get_dir '{}': {}", parent_id, e);
                return None;
            }
            Ok(other) => {
                error!("get_dir '{}': unexpected task result {:?}", parent_id, other);
            }
        }

        // Re-read regardless of TTL — the worker just stored a (partial) result.
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
    // ── opendir ────────────────────────────────────────────────────────────

    /// Trigger an eager directory fetch so that the subsequent `readdir` can
    /// return at least the first page immediately.
    ///
    /// We use `opendir` as the prefetch trigger rather than `readdir` because
    /// `opendir` is called **before** the GUI shows the busy indicator and
    /// before any `readdir` calls.  This gives the first-page fetch a head
    /// start: by the time `readdir` arrives, there is a good chance the partial
    /// listing is already in cache.
    ///
    /// The busy indicator remains visible because `readdir` still blocks on
    /// the queue if the cache is not yet warm (< first page arrived).
    fn opendir(
        &mut self,
        _req: &Request,
        ino: u64,
        _flags: i32,
        reply: fuser::ReplyOpen,
    ) {
        debug!("opendir ino={}", ino);
        if let Some(parent_id) = self.obj.ino_to_drive_id(ino) {
            if !self.obj.has_cache_entry(&parent_id) {
                // Cold miss — start eager first-page fetch before readdir arrives.
                self.queue
                    .enqueue(TaskKey::FetchDirFirstPage(parent_id), Priority::High);
            } else if self.obj.get_cached_dir(&parent_id).is_none() {
                // Stale entry — trigger background ETag revalidation.
                // readdir will serve the stale data immediately; once the
                // worker finishes the cache becomes fresh for the next cycle.
                self.queue.enqueue(TaskKey::FetchDir(parent_id), Priority::Normal);
            }
            // Fresh — nothing to do.
        }
        reply.opened(0, 0);
    }

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
        let is_complete = self.obj.is_dir_complete(&parent_id);
        if offset == 0 {
            info!(
                "readdir: {} entries in folder id={} ({})",
                total,
                parent_id,
                if is_complete { "complete" } else { "partial \u{2014} more pages loading" }
            );
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

        // Cache miss — strategy depends on file size:
        //   ≤ CACHE_MAX_FILE_BYTES (1 MiB) → download fully and cache (fast repeat reads)
        //   > CACHE_MAX_FILE_BYTES          → HTTP Range request for exactly the requested
        //     window (no heap allocation for the whole file, no cache pollution)
        let file_size = self
            .obj
            .get_metadata(&file_id)
            .map(|f| f.size)
            .unwrap_or(u64::MAX);

        if file_size <= CACHE_MAX_FILE_BYTES {
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
        } else {
            // Large file — fetch only the requested range.
            debug!(
                "read ino={} range offset={} size={} (file_size={})",
                ino, offset, size, file_size
            );
            match self.queue.enqueue_and_wait(
                TaskKey::DownloadFileRange(file_id.clone(), offset as u64, size),
                Priority::High,
            ) {
                Ok(TaskResult::FileContentRange(bytes)) => reply.data(&bytes),
                Ok(other) => {
                    error!("read ino={}: unexpected task result {:?}", ino, other);
                    reply.error(libc::EIO);
                }
                Err(e) => {
                    error!("read ino={} range: {}", ino, e);
                    reply.error(libc::EIO);
                }
            }
        }
    }

    /// Return ENODATA for every extended attribute.
    ///
    /// This filesystem stores no xattrs.  Returning ENODATA (= ENOATTR on
    /// Linux) is the correct POSIX response and short-circuits any xattr
    /// probing done by thumbnailers or indexers before they fall back to
    /// reading the file content.
    fn getxattr(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        _name: &std::ffi::OsStr,
        _size: u32,
        reply: fuser::ReplyXattr,
    ) {
        reply.error(libc::ENODATA);
    }

    /// Return an empty xattr list — there are no extended attributes here.
    fn listxattr(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        if size == 0 {
            // Caller is querying the required buffer size.
            reply.size(0);
        } else {
            reply.data(&[]);
        }
    }
}
