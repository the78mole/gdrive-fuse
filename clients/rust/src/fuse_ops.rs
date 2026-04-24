//! FUSE filesystem operations — bridges the kernel FUSE interface with
//! `ObjectManager` (cache) and `QueueManager` (Drive API work queue).
//!
//! # Concurrency model
//!
//! `fuser` serves FUSE callbacks concurrently via `&self`, so the handler
//! must be `Send + Sync`.  All Drive API calls are dispatched to the
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
use crate::gclient::{FileInfo, GClient};
use crate::object_manager::{
    ObjectManager, ROOT_INO, TTL, CACHE_STREAM_THRESHOLD_BYTES, desktop_content, is_workspace_type,
};
use crate::queue_manager::{Priority, QueueManager, TaskKey};
use crossbeam_channel::Sender;
use dashmap::DashMap;
use fuser::{FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo, LockOwner, OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, Request};
use log::{debug, error, info};
use std::ffi::OsStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ── WriteEntry ─────────────────────────────────────────────────────────────

/// In-memory write buffer for an open writable file handle.
struct WriteEntry {
    /// Parent directory Drive ID — needed when uploading a new file.
    parent_id: String,
    /// File display name — needed when creating a new file on Drive.
    name: String,
    /// Drive file ID of an *existing* file being overwritten, or `None` for
    /// newly created files that have not yet been uploaded to Drive.
    file_id: Option<String>,
    /// Accumulated write data.  Grows on `write`, uploaded on `release`.
    content: Vec<u8>,
}

// ── GDriveFuse ─────────────────────────────────────────────────────────────

/// FUSE filesystem handler.
pub struct GDriveFuse {
    obj: Arc<ObjectManager>,
    queue: Arc<QueueManager>,
    dup_map: Arc<DupMapping>,
    /// Drive API client — used directly for write operations.
    client: Arc<GClient>,
    /// Dedicated Drive API client for upload threads — owns its own HTTP
    /// connection pool so in-flight downloads never delay uploads.
    upload_client: Arc<GClient>,
    /// Per-file-handle write buffers, keyed by file handle number.
    write_buffers: DashMap<u64, WriteEntry>,
    /// Monotonically increasing file handle counter.
    next_fh: AtomicU64,
    /// Sender end of the dedicated upload thread pool.  Closures submitted
    /// here execute on one of `UPLOAD_THREADS` background threads.
    upload_tx: Sender<Box<dyn FnOnce() + Send + 'static>>,
    /// Number of uploads currently in-flight or queued.  Shared with the
    /// `QueueManager` so Low-prio prefetch workers pause automatically.
    active_uploads: Arc<AtomicUsize>,
    /// Sender end of the reply-dispatcher pool.  FUSE callbacks that would
    /// block (cache miss → Drive API call) move their `reply` handle here
    /// and return immediately, keeping the single FUSE event-loop thread free
    /// to accept the next kernel request.  The pool thread issues the API
    /// call and sends the reply when it is ready.
    reply_tx: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl GDriveFuse {
    pub fn new(
        obj: Arc<ObjectManager>,
        queue: Arc<QueueManager>,
        dup_map: Arc<DupMapping>,
        client: Arc<GClient>,
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
                        Priority::DirPrefetch,
                    ) {
                        Ok(_) => {
                            let files = o.get_dir_files("root").unwrap_or_default();
                            let n_dirs = files.iter().filter(|f| f.is_folder).count();
                            info!(
                                "startup prefetch: root has {} entries, {} dirs",
                                files.len(),
                                n_dirs
                            );
                            // Prefetch root subdirectories at P3 (background dir).
                            for f in files.iter().filter(|f| f.is_folder) {
                                q.enqueue(TaskKey::FetchDir(f.id.clone()), Priority::DirPrefetch);
                            }
                            // Prefetch metadata for non-folder files at P4.
                            for f in files.iter().filter(|f| !f.is_folder) {
                                q.enqueue(
                                    TaskKey::GetMetadata(f.id.clone()),
                                    Priority::MetaPrefetch,
                                );
                            }
                        }
                        Err(e) => error!("startup prefetch failed: {}", e),
                    }
                })
                .ok();
        }
        // ── Upload thread pool ───────────────────────────────────────────────
        // 4 dedicated threads serve asynchronous Drive uploads so FUSE
        // `release()` can return to the kernel immediately.
        const UPLOAD_THREADS: usize = 4;
        let (upload_tx, upload_rx) =
            crossbeam_channel::unbounded::<Box<dyn FnOnce() + Send + 'static>>();
        // Reuse the counter from QueueManager — Low-prio workers check it.
        let active_uploads = Arc::clone(&queue.active_uploads);
        for i in 0..UPLOAD_THREADS {
            let rx = upload_rx.clone();
            std::thread::Builder::new()
                .name(format!("gdrive-upload-{}", i))
                .spawn(move || {
                    for task in rx.iter() {
                        task();
                    }
                    debug!("upload-{}: channel closed, exiting", i);
                })
                .expect("failed to spawn upload thread");
        }
        Self {
            obj,
            queue,
            dup_map,
            upload_client: Arc::new(client.fork()),
            client,
            write_buffers: DashMap::new(),
            next_fh: AtomicU64::new(1),
            upload_tx,
            active_uploads,
            reply_tx: {
                // ── Reply-dispatcher pool ————————————————————————
                // FUSE callbacks that cannot be served from cache move their
                // `reply` handle to one of these threads so the FUSE
                // event-loop thread can immediately pick up the next request
                // from the kernel.  REPLY_THREADS=16 is enough headroom for
                // the kernel's typical burst of parallel readdir/lookup/read
                // calls during an `ls -R` on a large tree.
                const REPLY_THREADS: usize = 16;
                let (reply_tx, reply_rx) =
                    crossbeam_channel::unbounded::<Box<dyn FnOnce() + Send + 'static>>();
                for i in 0..REPLY_THREADS {
                    let rx = reply_rx.clone();
                    std::thread::Builder::new()
                        .name(format!("gdrive-reply-{}", i))
                        .spawn(move || {
                            for task in rx.iter() {
                                task();
                            }
                            debug!("reply-{}: channel closed, exiting", i);
                        })
                        .expect("failed to spawn reply dispatcher thread");
                }
                reply_tx
            },
        }
    }

    // ── Directory helper ──────────────────────────────────────────────────

    /// Return a fresh directory listing, fetching via the queue if the cache is
    /// missing or expired.
    ///
    /// Returns `Arc<Vec<FileInfo>>` — cloning the `Arc` is O(1) and zero-copy
    /// regardless of listing size.
    fn get_dir(&self, parent_id: &str) -> Option<Arc<Vec<FileInfo>>> {
        // Fast path 1 — entry still within TTL (partial or complete).
        if let Some(files) = self.obj.get_cached_dir(parent_id) {
            return Some(files);
        }

        // Fast path 2 — stale entry: serve stale data immediately and fire a
        // background full fetch so the listing is refreshed for the next cycle.
        if let Some(files) = self.obj.get_dir_files(parent_id) {
            let priority = if self.obj.is_dir_complete(parent_id) {
                Priority::DirPrefetch  // stale-while-revalidate — no urgency
            } else {
                Priority::DirUrgent    // incomplete listing — fetch remaining pages now
            };
            self.queue
                .enqueue(TaskKey::FetchDir(parent_id.to_string()), priority);
            // Prefetch child dirs that are not yet cached.
            for f in files.iter().filter(|f| f.is_folder) {
                if !self.obj.has_cache_entry(&f.id) {
                    self.queue.enqueue(TaskKey::FetchDir(f.id.clone()), Priority::DirPrefetch);
                }
            }
            return Some(files);
        }

        // Complete miss — fetch the full listing and block until it arrives.
        if let Err(e) = self
            .queue
            .enqueue_and_wait(TaskKey::FetchDir(parent_id.to_string()), Priority::DirUrgent)
        {
            error!("get_dir '{}': {}", parent_id, e);
            return None;
        }

        // Re-read regardless of TTL — the worker just stored a (partial) result.
        let files = self.obj.get_dir_files(parent_id)?;

        // Fire background prefetch for child dirs not yet in cache.
        for f in files.iter().filter(|f| f.is_folder) {
            if !self.obj.has_cache_entry(&f.id) {
                self.queue.enqueue(TaskKey::FetchDir(f.id.clone()), Priority::DirPrefetch);
            }
        }

        Some(files)
    }

    // ── FileAttr helpers ──────────────────────────────────────────────────

    fn root_attr() -> FileAttr {
        let now = SystemTime::now();
        FileAttr {
            ino: INodeNo(ROOT_INO),
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

// ── readdir helper ─────────────────────────────────────────────────────────

/// Build the `(inode, kind, name)` entry list from a directory listing and
/// stream it into a `ReplyDirectory`.
///
/// Extracted as a free function so it can be called from both the synchronous
/// fast-path and the dispatcher-pool closure without capturing `&self`.
fn serve_readdir(
    files: &[crate::gclient::FileInfo],
    ino: INodeNo,
    parent_id: &str,
    offset: u64,
    obj: &ObjectManager,
    dup_map: &DupMapping,
    mut reply: ReplyDirectory,
) {
    let mut entries: Vec<(u64, FileType, String)> = vec![
        (ino.0, FileType::Directory, ".".to_string()),
        (ino.0, FileType::Directory, "..".to_string()),
    ];
    for (unique_name, f) in dup_map.resolve(files) {
        let child_ino = obj.get_or_alloc_ino(&f.id);
        let kind = if f.is_folder { FileType::Directory } else { FileType::RegularFile };
        entries.push((child_ino, kind, unique_name));
    }
    let total = entries.len().saturating_sub(2);
    if offset == 0 {
        let is_complete = obj.is_dir_complete(parent_id);
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
        if reply.add(INodeNo(*child_ino), (i + 1) as u64, *kind, name) {
            stopped_at = Some(i);
            break;
        }
        added += 1;
    }
    debug!(
        "readdir ino={} offset={}: added {} entries, stopped_at={:?}",
        ino.0, offset, added, stopped_at
    );
    reply.ok();
}

// ── Filesystem trait ───────────────────────────────────────────────────────

impl Filesystem for GDriveFuse {
    // ── init ───────────────────────────────────────────────────────────────

    /// Tell the kernel that this filesystem supports:
    ///
    /// - `FUSE_CAP_ASYNC_READ`       — kernel may send multiple read requests
    ///   concurrently without waiting for each reply.
    /// - `FUSE_PARALLEL_DIROPS`  — kernel may issue `lookup` and `readdir`
    ///   operations in parallel within the same directory.
    ///
    /// Both capabilities are prerequisites for the reply-dispatcher pool to
    /// yield a throughput improvement: if the kernel serialises requests the
    /// pool threads would never execute concurrently.
    fn init(
        &mut self,
        _req: &Request,
        config: &mut fuser::KernelConfig,
    ) -> std::io::Result<()> {
        // Ignore unsupported-flags errors — the kernel simply won't set flags
        // it does not know.
        let _ = config.add_capabilities(
            fuser::InitFlags::FUSE_ASYNC_READ | fuser::InitFlags::FUSE_PARALLEL_DIROPS,
        );
        Ok(())
    }

    // ── opendir ────────────────────────────────────────────────────────────

    /// Trigger an eager directory fetch so that the subsequent `readdir` can
    /// return the complete listing immediately.
    ///
    /// `opendir` fires a full `FetchDir` (all pages, pageSize=1000) on the
    /// P0 worker before `readdir` arrives.  This gives the fetch a head start:
    /// for most directories (< 1000 entries) a single Drive API call suffices,
    /// and by the time `readdir` arrives a few milliseconds later the complete
    /// listing is already in cache.
    fn opendir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _flags: OpenFlags,
        reply: fuser::ReplyOpen,
    ) {
        debug!("opendir ino={}", ino.0);
        if let Some(parent_id) = self.obj.ino_to_drive_id(ino.0) {
            // Fire a full directory fetch whenever the cache is absent, stale,
            // or partial.  Runs on the P0 (DirUrgent) worker, giving the fetch
            // a head start so readdir finds a complete listing already in cache
            // when it arrives a few milliseconds later.
            if self.obj.get_cached_dir(&parent_id).is_none() {
                self.queue.enqueue(TaskKey::FetchDir(parent_id), Priority::DirUrgent);
            }
            // Fresh and complete — nothing to do.
        }
        reply.opened(FileHandle(0), FopenFlags::empty());
    }

    // ── lookup ─────────────────────────────────────────────────────────────

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: fuser::ReplyEntry) {
        debug!("lookup parent={} name={:?}", parent.0, name);

        let Some(parent_id) = self.obj.ino_to_drive_id(parent.0) else {
            reply.error(fuser::Errno::ENOENT);
            return;
        };
        let name_str = name.to_string_lossy().into_owned();

        // Fast path: listing already in cache — serve without touching the queue.
        // The name-index is intentionally bypassed here: it stores raw base
        // names and would return wrong results for suffixed duplicates.
        if let Some(files) = self.obj.get_cached_dir(&parent_id) {
            for (unique_name, f) in self.dup_map.resolve(&files) {
                if unique_name == name_str.as_str() {
                    let ino = self.obj.get_or_alloc_ino(&f.id);
                    reply.entry(&TTL, &ObjectManager::make_file_attr(ino, f), Generation(0));
                    return;
                }
            }
            reply.error(fuser::Errno::ENOENT);
            return;
        }

        // Slow path: cache miss — move reply into the dispatcher pool.
        let obj = Arc::clone(&self.obj);
        let queue = Arc::clone(&self.queue);
        let dup_map = Arc::clone(&self.dup_map);
        self.reply_tx
            .send(Box::new(move || {
                let files = match queue.enqueue_and_wait(
                    TaskKey::FetchDir(parent_id.clone()),
                    Priority::DirUrgent,
                ) {
                    Ok(_) => obj.get_dir_files(&parent_id),
                    Err(e) => {
                        error!("lookup: fetch parent '{}': {}", parent_id, e);
                        None
                    }
                };
                let Some(files) = files else {
                    reply.error(fuser::Errno::EIO);
                    return;
                };
                for (unique_name, f) in dup_map.resolve(&files) {
                    if unique_name == name_str.as_str() {
                        let ino = obj.get_or_alloc_ino(&f.id);
                        reply.entry(&TTL, &ObjectManager::make_file_attr(ino, f), Generation(0));
                        return;
                    }
                }
                reply.error(fuser::Errno::ENOENT);
            }))
            .unwrap_or_else(|_| error!("lookup: reply dispatcher channel closed"));
    }

    // ── getattr ────────────────────────────────────────────────────────────

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        debug!("getattr ino={}", ino.0);

        if ino.0 == ROOT_INO {
            reply.attr(&TTL, &Self::root_attr());
            return;
        }

        let Some(file_id) = self.obj.ino_to_drive_id(ino.0) else {
            reply.error(fuser::Errno::ENOENT);
            return;
        };

        // Fast path: metadata already in cache.
        if let Some(info) = self.obj.get_metadata(&file_id) {
            reply.attr(&TTL, &ObjectManager::make_file_attr(ino.0, &info));
            return;
        }

        // Metadata miss — dispatch to the reply-dispatcher pool so the FUSE
        // event-loop thread can immediately accept the next kernel request.
        let obj = Arc::clone(&self.obj);
        let queue = Arc::clone(&self.queue);
        self.reply_tx
            .send(Box::new(move || {
                if let Err(e) = queue
                    .enqueue_and_wait(TaskKey::GetMetadata(file_id.clone()), Priority::MetaUrgent)
                {
                    error!("getattr ino={}: {}", ino.0, e);
                    reply.error(fuser::Errno::EIO);
                    return;
                }
                match obj.get_metadata(&file_id) {
                    Some(info) => {
                        reply.attr(&TTL, &ObjectManager::make_file_attr(ino.0, &info))
                    }
                    None => {
                        error!("getattr ino={}: metadata missing after fetch", ino.0);
                        reply.error(fuser::Errno::EIO);
                    }
                }
            }))
            .unwrap_or_else(|_| error!("getattr: reply dispatcher channel closed"));
    }

    // ── readdir ────────────────────────────────────────────────────────────

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        debug!("readdir ino={} offset={}", ino.0, offset);

        let Some(parent_id) = self.obj.ino_to_drive_id(ino.0) else {
            reply.error(fuser::Errno::ENOENT);
            return;
        };

        // Fast path: listing is cached — serve synchronously without touching
        // the queue.
        if let Some(files) = self.obj.get_cached_dir(&parent_id) {
            serve_readdir(&files, ino, &parent_id, offset, &self.obj, &self.dup_map, reply);
            return;
        }

        // Slow path: listing missing, stale, or partial — move the reply handle
        // into the dispatcher pool so the FUSE event-loop thread stays unblocked.
        let obj = Arc::clone(&self.obj);
        let queue = Arc::clone(&self.queue);
        let dup_map = Arc::clone(&self.dup_map);
        self.reply_tx
            .send(Box::new(move || {
                let files = match queue.enqueue_and_wait(
                    TaskKey::FetchDir(parent_id.clone()),
                    Priority::DirUrgent,
                ) {
                    Ok(_) => obj.get_dir_files(&parent_id),
                    Err(e) => {
                        error!("readdir: fetch '{}': {}", parent_id, e);
                        reply.error(fuser::Errno::EIO);
                        return;
                    }
                };
                let Some(files) = files else {
                    error!("readdir: no files after fetch for '{}'", parent_id);
                    reply.error(fuser::Errno::EIO);
                    return;
                };
                // Prefetch child dirs not yet in cache.
                for f in files.iter().filter(|f| f.is_folder) {
                    if !obj.has_cache_entry(&f.id) {
                        queue.enqueue(TaskKey::FetchDir(f.id.clone()), Priority::DirPrefetch);
                    }
                }
                serve_readdir(&files, ino, &parent_id, offset, &obj, &dup_map, reply);
            }))
            .unwrap_or_else(|_| error!("readdir: reply dispatcher channel closed"));
    }

    // ── read ───────────────────────────────────────────────────────────────

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock: Option<LockOwner>,
        reply: ReplyData,
    ) {
        debug!("read ino={} offset={} size={}", ino.0, offset, size);

        let Some(file_id) = self.obj.ino_to_drive_id(ino.0) else {
            reply.error(fuser::Errno::ENOENT);
            return;
        };

        let file_name = self
            .obj
            .get_metadata(&file_id)
            .map(|f| f.name.clone())
            .unwrap_or_else(|| file_id.clone());

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

        // Early-return EOF guard for regular files: if the kernel requests bytes
        // at or past the known file size, return an empty slice immediately.
        // This prevents a pointless cache lookup or full-file download when
        // offset ≥ file_size.  Per POSIX read(2): returning 0 bytes is the
        // correct EOF indicator — it is NOT an error.
        let known_size = self
            .obj
            .get_metadata(&file_id)
            .map(|f| f.size)
            .unwrap_or(u64::MAX); // unknown size → do not skip
        if offset >= known_size {
            reply.data(&[]);
            return;
        }

        // 1. RAM cache hit — small files (≤ CACHE_RAM_MAX_BYTES, i.e. 4 KiB).
        if let Some(content) = self.obj.get_content(&file_id) {
            debug!("read (ram-cache): \"{}\" offset={} size={}", file_name, offset, size);
            let start = (offset as usize).min(content.len());
            let end = (start + size as usize).min(content.len());
            reply.data(&content[start..end]);
            return;
        }

        // 2. Disk cache hit — file was downloaded during a previous read().
        //    Only the requested slice is read from disk via seek(); the full
        //    file is never loaded into RAM.
        if let Some(slice) = self.obj.read_disk_slice(&file_id, offset, size) {
            debug!("read (disk-cache): \"{}\" offset={} size={}", file_name, offset, size);
            reply.data(&slice);
            return;
        }

        // 3. Cache miss — move the reply handle into the dispatcher pool so the
        //    FUSE event-loop thread can immediately accept the next kernel request
        //    while this thread downloads the file from Google Drive.
        //
        //    Files larger than CACHE_STREAM_THRESHOLD_BYTES (64 MiB) are served
        //    via an HTTP Range request that delivers *only* the requested window
        //    — the full file is never downloaded.  This prevents OOM when the
        //    user seeks in a multi-gigabyte video while only needing 128 KiB per
        //    `read()` call.  Range-downloaded bytes are NOT stored in any cache.
        //
        //    Smaller files go through the normal full-download → cache → serve
        //    path so subsequent reads within the same file are served from the
        //    disk or RAM cache without a network round trip.
        let file_size = self
            .obj
            .get_metadata(&file_id)
            .map(|f| f.size)
            .unwrap_or(0);

        if file_size > CACHE_STREAM_THRESHOLD_BYTES {
            info!(
                "read (stream-range): \"{}\" (id={}) size={} offset={} len={}",
                file_name, file_id, file_size, offset, size
            );
            let client = Arc::clone(&self.client);
            self.reply_tx
                .send(Box::new(move || {
                    match client.download_file_range(&file_id, offset, size) {
                        Ok(bytes) => reply.data(&bytes),
                        Err(e) => {
                            error!(
                                "read: range download failed for '{}' offset={} len={}: {}",
                                file_id, offset, size, e
                            );
                            reply.error(fuser::Errno::EIO);
                        }
                    }
                }))
                .unwrap_or_else(|_| error!("read: reply dispatcher channel closed"));
        } else {
            info!("read (downloading): \"{}\" (id={})", file_name, file_id);
            let obj = Arc::clone(&self.obj);
            let queue = Arc::clone(&self.queue);
            self.reply_tx
                .send(Box::new(move || {
                    if let Err(e) = queue.enqueue_and_wait(
                        TaskKey::DownloadFile(file_id.clone()),
                        Priority::FileDownload,
                    ) {
                        error!("read: download failed for '{}': {}", file_id, e);
                        reply.error(fuser::Errno::EIO);
                        return;
                    }
                    // 4. Serve from whichever cache tier the worker populated.
                    if let Some(content) = obj.get_content(&file_id) {
                        let start = (offset as usize).min(content.len());
                        let end = (start + size as usize).min(content.len());
                        reply.data(&content[start..end]);
                    } else if let Some(slice) = obj.read_disk_slice(&file_id, offset, size) {
                        reply.data(&slice);
                    } else {
                        error!("read: content missing after download for '{}'", file_id);
                        reply.error(fuser::Errno::EIO);
                    }
                }))
                .unwrap_or_else(|_| error!("read: reply dispatcher channel closed"));
        }
    }

    /// Return ENODATA for every extended attribute.
    ///
    /// This filesystem stores no xattrs.  Returning ENODATA (= ENOATTR on
    /// Linux) is the correct POSIX response and short-circuits any xattr
    /// probing done by thumbnailers or indexers before they fall back to
    /// reading the file content.
    fn getxattr(
        &self,
        _req: &fuser::Request,
        _ino: INodeNo,
        _name: &std::ffi::OsStr,
        _size: u32,
        reply: fuser::ReplyXattr,
    ) {
        reply.error(fuser::Errno::ENODATA);
    }

    /// Return an empty xattr list — there are no extended attributes here.
    fn listxattr(
        &self,
        _req: &fuser::Request,
        _ino: INodeNo,
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

    // ── mkdir ──────────────────────────────────────────────────────────────

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        let name_str = name.to_string_lossy();
        debug!("mkdir parent={} name={:?}", parent.0, name_str);

        let Some(parent_id) = self.obj.ino_to_drive_id(parent.0) else {
            reply.error(fuser::Errno::ENOENT);
            return;
        };

        match self.client.create_folder(&name_str, &parent_id) {
            Ok(info) => {
                self.obj.invalidate_dir(&parent_id);
                let ino = self.obj.get_or_alloc_ino(&info.id);
                self.obj.store_metadata(info.clone());
                reply.entry(&TTL, &ObjectManager::make_file_attr(ino, &info), Generation(0));
            }
            Err(e) => {
                error!("mkdir '{}': {}", name_str, e);
                reply.error(fuser::Errno::EIO);
            }
        }
    }

    // ── unlink ─────────────────────────────────────────────────────────────

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: fuser::ReplyEmpty) {
        let name_str = name.to_string_lossy();
        debug!("unlink parent={} name={:?}", parent.0, name_str);

        let Some(parent_id) = self.obj.ino_to_drive_id(parent.0) else {
            reply.error(fuser::Errno::ENOENT);
            return;
        };

        let Some(files) = self.get_dir(&parent_id) else {
            reply.error(fuser::Errno::EIO);
            return;
        };

        // Resolve through the duplicate-name map so the caller's unique name
        // (e.g. "file (1).txt") is correctly matched to the right Drive file.
        let file_id = self
            .dup_map
            .resolve(&files)
            .into_iter()
            .find(|(unique, f)| unique == name_str.as_ref() && !f.is_folder)
            .map(|(_, f)| f.id.clone());

        let Some(file_id) = file_id else {
            reply.error(fuser::Errno::ENOENT);
            return;
        };

        match self.client.delete_file(&file_id) {
            Ok(()) => {
                self.obj.remove_metadata(&file_id);
                self.obj.invalidate_dir(&parent_id);
                reply.ok();
            }
            Err(e) => {
                error!("unlink '{}': {}", file_id, e);
                reply.error(fuser::Errno::EIO);
            }
        }
    }

    // ── rmdir ──────────────────────────────────────────────────────────────

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: fuser::ReplyEmpty) {
        let name_str = name.to_string_lossy();
        debug!("rmdir parent={} name={:?}", parent.0, name_str);

        let Some(parent_id) = self.obj.ino_to_drive_id(parent.0) else {
            reply.error(fuser::Errno::ENOENT);
            return;
        };

        let Some(files) = self.get_dir(&parent_id) else {
            reply.error(fuser::Errno::EIO);
            return;
        };

        let dir_id = self
            .dup_map
            .resolve(&files)
            .into_iter()
            .find(|(unique, f)| unique == name_str.as_ref() && f.is_folder)
            .map(|(_, f)| f.id.clone());

        let Some(dir_id) = dir_id else {
            reply.error(fuser::Errno::ENOENT);
            return;
        };

        match self.client.delete_file(&dir_id) {
            Ok(()) => {
                self.obj.remove_metadata(&dir_id);
                self.obj.invalidate_dir(&parent_id);
                self.obj.invalidate_dir(&dir_id);
                reply.ok();
            }
            Err(e) => {
                error!("rmdir '{}': {}", dir_id, e);
                reply.error(fuser::Errno::EIO);
            }
        }
    }

    // ── rename ─────────────────────────────────────────────────────────────

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: fuser::RenameFlags,
        reply: fuser::ReplyEmpty,
    ) {
        let name_str = name.to_string_lossy();
        let newname_str = newname.to_string_lossy();
        debug!(
            "rename parent={} name={:?} → newparent={} newname={:?}",
            parent.0, name_str, newparent.0, newname_str
        );

        let (Some(parent_id), Some(newparent_id)) = (
            self.obj.ino_to_drive_id(parent.0),
            self.obj.ino_to_drive_id(newparent.0),
        ) else {
            reply.error(fuser::Errno::ENOENT);
            return;
        };

        let Some(files) = self.get_dir(&parent_id) else {
            reply.error(fuser::Errno::EIO);
            return;
        };

        let file_id = self
            .dup_map
            .resolve(&files)
            .into_iter()
            .find(|(unique, _)| unique == name_str.as_ref())
            .map(|(_, f)| f.id.clone())
            // Fallback: if an intermediate `unlink` (overwrite-copy pattern)
            // wiped the dir-cache via `invalidate_dir`, the pending entry is
            // still registered in the name-index.
            .or_else(|| self.obj.lookup_id_by_parent_and_name(&parent_id, &name_str));

        let Some(raw_file_id) = file_id else {
            reply.error(fuser::Errno::ENOENT);
            return;
        };

        // If the source file was just created and its upload is still in
        // flight, the dir-cache still carries a placeholder ID
        // ("__pending__<fh>").  Wait up to 60 s for replace_pending_id() to
        // swap it for the real Drive ID before calling the Drive rename API.
        let file_id = if raw_file_id.starts_with("__pending__") {
            let ino = self.obj.get_or_alloc_ino(&raw_file_id);
            let deadline = std::time::Instant::now() + Duration::from_secs(60);
            loop {
                if std::time::Instant::now() > deadline {
                    error!("rename: timed out waiting for upload of '{}'", raw_file_id);
                    reply.error(fuser::Errno::EIO);
                    return;
                }
                let current =
                    self.obj.ino_to_drive_id(ino).unwrap_or_default();
                if !current.is_empty() && !current.starts_with("__pending__") {
                    debug!("rename: resolved pending '{}' → '{}'", raw_file_id, current);
                    break current;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        } else {
            raw_file_id
        };

        let (new_parent_arg, old_parent_arg) = if parent_id != newparent_id {
            (Some(newparent_id.as_str()), Some(parent_id.as_str()))
        } else {
            (None, None)
        };

        match self
            .client
            .rename_file(&file_id, &newname_str, new_parent_arg, old_parent_arg)
        {
            Ok(updated) => {
                self.obj.store_metadata(updated);
                self.obj.invalidate_dir(&parent_id);
                if parent_id != newparent_id {
                    self.obj.invalidate_dir(&newparent_id);
                }
                reply.ok();
            }
            Err(e) => {
                error!("rename '{}': {}", file_id, e);
                reply.error(fuser::Errno::EIO);
            }
        }
    }

    // ── create ─────────────────────────────────────────────────────────────

    /// Create and open a new file.  The file is buffered in memory and
    /// uploaded to Drive on `release`.
    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let name_str = name.to_string_lossy().into_owned();
        debug!("create parent={} name={:?}", parent.0, name_str);

        let Some(parent_id) = self.obj.ino_to_drive_id(parent.0) else {
            reply.error(fuser::Errno::ENOENT);
            return;
        };

        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        let pending_id = format!("__pending__{}", fh);

        // Placeholder metadata so getattr works before the first flush.
        let placeholder = FileInfo {
            id: pending_id.clone(),
            name: name_str.clone(),
            mime_type: "application/octet-stream".to_string(),
            size: 0,
            modified_time: String::new(),
            md5_checksum: None,
            is_folder: false,
        };
        self.obj.store_metadata(placeholder.clone());
        // Inject into the parent dir-cache so rename() can locate the file by
        // name before the Drive upload has completed.
        self.obj.inject_pending_into_dir(&parent_id, placeholder.clone());
        let ino = self.obj.get_or_alloc_ino(&pending_id);

        self.write_buffers.insert(
            fh,
            WriteEntry {
                parent_id,
                name: name_str,
                file_id: None,
                content: Vec::new(),
            },
        );

        let attr = ObjectManager::make_file_attr(ino, &placeholder);
        reply.created(&TTL, &attr, Generation(0), FileHandle(fh), FopenFlags::empty());
    }

    // ── open ───────────────────────────────────────────────────────────────

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: fuser::ReplyOpen) {
        debug!("open ino={} flags={:?}", ino.0, flags);

        if ino.0 == ROOT_INO {
            reply.error(fuser::Errno::EISDIR);
            return;
        }

        let Some(file_id) = self.obj.ino_to_drive_id(ino.0) else {
            reply.error(fuser::Errno::ENOENT);
            return;
        };

        let writable = (flags.0 & (libc::O_WRONLY | libc::O_RDWR)) != 0;

        if writable {
            let truncate = (flags.0 & libc::O_TRUNC) != 0;
            let initial_content = if truncate {
                Vec::new()
            } else {
                // Seed the write buffer with the existing content so O_RDWR
                // overwrites work correctly.
                if let Some(cached) = self.obj.get_content(&file_id) {
                    // Small file already in RAM cache — clone out of Arc for write buffer.
                    Arc::unwrap_or_clone(cached)
                } else if let Some(disk) = self.obj.read_full_disk_content(&file_id) {
                    // Large file on disk cache — load into write buffer.
                    disk
                } else {
                    match self
                        .queue
                        .enqueue_and_wait(TaskKey::DownloadFile(file_id.clone()), Priority::FileDownload)
                    {
                        Ok(_) => self
                            .obj
                            .get_content(&file_id)
                            .map(Arc::unwrap_or_clone)
                            .or_else(|| self.obj.read_full_disk_content(&file_id))
                            .unwrap_or_default(),
                        Err(_) => Vec::new(),
                    }
                }
            };

            let name = self
                .obj
                .get_metadata(&file_id)
                .map(|f| f.name.clone())
                .unwrap_or_default();

            let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
            self.write_buffers.insert(
                fh,
                WriteEntry {
                    parent_id: String::new(), // not needed for updates
                    name,
                    file_id: Some(file_id),
                    content: initial_content,
                },
            );
            reply.opened(FileHandle(fh), FopenFlags::empty());
        } else {
            // Read-only open — no write buffer needed.
            reply.opened(FileHandle(0), FopenFlags::empty());
        }
    }

    // ── write ──────────────────────────────────────────────────────────────

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: OpenFlags,
        _lock: Option<LockOwner>,
        reply: fuser::ReplyWrite,
    ) {
        debug!("write fh={} offset={} len={}", fh.0, offset, data.len());

        let Some(mut entry) = self.write_buffers.get_mut(&fh.0) else {
            reply.error(fuser::Errno::EBADF);
            return;
        };

        let start = offset as usize;
        let end = start + data.len();
        if end > entry.content.len() {
            entry.content.resize(end, 0);
        }
        entry.content[start..end].copy_from_slice(data);

        reply.written(data.len() as u32);
    }

    // ── flush ──────────────────────────────────────────────────────────────

    /// Called on each `close(2)` from user space.  Since a file descriptor
    /// can be duplicated, `flush` may arrive multiple times per `open`.  We
    /// defer the actual upload to `release` (called exactly once per open).
    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    // ── fsync ─────────────────────────────────────────────────────────────

    /// Acknowledge `fsync(2)`.  Write data is held in the in-memory
    /// `write_buffers` map and will be uploaded to Drive on `release`.
    /// Returning `ok` here satisfies tools (editors, rsync, …) that call
    /// `fsync` after writing without us needing to start the upload early.
    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    // ── release ────────────────────────────────────────────────────────────

    /// Called once when the last reference to an open file handle is dropped.
    /// Uploads any buffered write data to Google Drive.
    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        debug!("release ino={} fh={}", ino.0, fh.0);

        let Some((_, entry)) = self.write_buffers.remove(&fh.0) else {
            // Read-only file handle — nothing to upload.
            reply.ok();
            return;
        };

        // Acknowledge to the kernel immediately.  The Drive upload is
        // dispatched to the dedicated upload thread pool so tools like `cp`
        // can queue the next file without waiting for the full HTTP round-trip.
        reply.ok();

        let WriteEntry { file_id, name, parent_id, content } = entry;
        let client = Arc::clone(&self.upload_client);
        let obj = Arc::clone(&self.obj);
        let active_uploads = Arc::clone(&self.active_uploads);
        let pending_id = format!("__pending__{}", fh.0);

        // Increment BEFORE sending to the pool so Low-prio workers pause as
        // soon as we know an upload is incoming, not only when it starts.
        active_uploads.fetch_add(1, Ordering::Relaxed);

        let task: Box<dyn FnOnce() + Send + 'static> = if let Some(fid) = file_id {
            Box::new(move || {
                // Invalidate any cached chunks before the upload so a
                // concurrent read doesn't serve stale data while the upload
                // is in progress.
                obj.invalidate_content(&fid);
                match client.update_file_content(&fid, content) {
                    Ok(updated) => obj.store_metadata(updated),
                    Err(e) => error!("upload: update_file_content '{}': {}", fid, e),
                }
                active_uploads.fetch_sub(1, Ordering::Relaxed);
            })
        } else {
            Box::new(move || {
                let size = content.len() as u64;
                match client.create_file(&name, &parent_id, content) {
                    Ok(mut new_info) => {
                        if new_info.size == 0 {
                            new_info.size = size;
                        }
                        // Update dir-cache in-place and swap ino maps;
                        // no extra invalidate needed.
                        obj.replace_pending_id(&pending_id, &parent_id, new_info);
                    }
                    Err(e) => {
                        error!("upload: create_file '{}': {}", name, e);
                        // Remove the stale placeholder from cache and dir listing.
                        obj.remove_pending_from_dir(&parent_id, &pending_id);
                    }
                }
                active_uploads.fetch_sub(1, Ordering::Relaxed);
            })
        };

        self.upload_tx
            .send(task)
            .unwrap_or_else(|_| error!("upload pool channel closed"));
    }

    // ── destroy ────────────────────────────────────────────────────────────

    /// Called by the FUSE kernel module just before unmounting.
    ///
    /// Blocks until every pending background upload has completed so that a
    /// clean `fusermount3 -u` never abandons in-flight writes.
    fn destroy(&mut self) {
        let count = self.active_uploads.load(Ordering::Relaxed);
        if count > 0 {
            info!("destroy: waiting for {} pending upload(s) to finish…", count);
            loop {
                if self.active_uploads.load(Ordering::Acquire) == 0 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            info!("destroy: all uploads complete, unmounting");
        }
    }

    // ── setattr ────────────────────────────────────────────────────────────

    /// Handle attribute changes.  Only `size` (truncation) is acted upon;
    /// all other fields are acknowledged but not persisted (Drive has no
    /// POSIX permission model).
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        debug!("setattr ino={} size={:?} fh={:?}", ino.0, size, fh.map(|h| h.0));

        // Truncate the write buffer if a specific file handle is given.
        if let (Some(new_size), Some(fh)) = (size, fh) {
            if let Some(mut entry) = self.write_buffers.get_mut(&fh.0) {
                entry.content.resize(new_size as usize, 0);
            }
        }

        // Return current (or updated) attributes.
        if ino.0 == ROOT_INO {
            reply.attr(&TTL, &Self::root_attr());
            return;
        }

        let file_id = self.obj.ino_to_drive_id(ino.0);
        let info = file_id.as_deref().and_then(|id| self.obj.get_metadata(id));

        match info {
            Some(info) => {
                let mut attr = ObjectManager::make_file_attr(ino.0, &info);
                if let Some(new_size) = size {
                    attr.size = new_size;
                }
                reply.attr(&TTL, &attr);
            }
            None => {
                reply.attr(&TTL, &Self::root_attr());
            }
        }
    }
}

// ── Unit Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gclient::GClient;
    use crate::object_manager::ObjectManager;
    use httpmock::prelude::*;
    use tempfile::TempDir;

    // ── helpers ───────────────────────────────────────────────────────────

    fn mock_client(server: &MockServer) -> Arc<GClient> {
        let base_url = format!("{}/drive/v3", server.base_url());
        Arc::new(GClient::new_for_test(&base_url, "fake-token"))
    }

    /// Mirror `GDriveFuse::read()` without a live FUSE session.
    /// Checks RAM cache → disk cache → downloads full file → serves slice.
    fn simulate_read(
        obj: &Arc<ObjectManager>,
        client: &Arc<GClient>,
        file_id: &str,
        offset: u64,
        size: u32,
    ) -> Vec<u8> {
        // 1. RAM cache hit.
        if let Some(content) = obj.get_content(file_id) {
            let start = (offset as usize).min(content.len());
            let end = (start + size as usize).min(content.len());
            return content[start..end].to_vec();
        }
        // 2. Disk cache hit.
        if let Some(slice) = obj.read_disk_slice(file_id, offset, size) {
            return slice;
        }
        // 3. Cache miss — download full file, route to RAM or disk.
        let bytes = client.download_file(file_id).expect("download_file failed");
        obj.store_content(file_id, bytes);
        // 4. Serve from whichever cache was populated.
        if let Some(content) = obj.get_content(file_id) {
            let start = (offset as usize).min(content.len());
            let end = (start + size as usize).min(content.len());
            content[start..end].to_vec()
        } else if let Some(slice) = obj.read_disk_slice(file_id, offset, size) {
            slice
        } else {
            panic!("no content after download for file_id={}", file_id);
        }
    }

    fn register_metadata(obj: &Arc<ObjectManager>, file_id: &str, size: u64) {
        use crate::gclient::FileInfo;
        obj.store_metadata(FileInfo {
            id: file_id.to_string(),
            name: format!("{}.bin", file_id),
            mime_type: "application/octet-stream".to_string(),
            size,
            modified_time: String::new(),
            md5_checksum: None,
            is_folder: false,
        });
    }

    // ── Disk cache: large files stored on disk, small files in RAM ─────────────

    /// Files > CACHE_RAM_MAX_BYTES (4 KiB) must be written to disk on first
    /// read and served from disk (via seek+read) on subsequent reads.
    /// Files ≤ 4 KiB must stay in RAM only.
    #[test]
    fn large_file_on_disk_small_file_in_ram() {
        use crate::object_manager::CACHE_RAM_MAX_BYTES;
        let tmp = TempDir::new().unwrap();

        // ── Large file (16 KiB > 4 KiB threshold) ───────────────────────────
        let large_size = (CACHE_RAM_MAX_BYTES * 4) as usize;
        let large_data: Vec<u8> = (0..large_size).map(|i| (i % 251) as u8).collect();

        let server = MockServer::start();
        let large_data_clone = large_data.clone();
        server.mock(|when, then| {
            when.method(GET)
                .path_includes("/files/large-pdf")
                .query_param("alt", "media");
            then.status(200).body(large_data_clone);
        });

        let obj = Arc::new(ObjectManager::new_for_test(tmp.path().to_path_buf()));
        register_metadata(&obj, "large-pdf", large_size as u64);
        let client = mock_client(&server);

        // Non-sequential reads — each triggers a download on first access.
        let reads: &[(u64, u32)] = &[
            (0, 512),
            (4096, 1024),
            (large_size as u64 - 100, 100),
        ];
        for &(offset, size) in reads {
            let got = simulate_read(&obj, &client, "large-pdf", offset, size);
            let s = offset as usize;
            let e = (s + size as usize).min(large_size);
            assert_eq!(got, &large_data[s..e], "mismatch offset={} size={}", offset, size);
        }

        // Large file: on disk, NOT in RAM.
        assert!(obj.get_content("large-pdf").is_none(), "large file must not occupy RAM");
        assert!(obj.has_disk_content("large-pdf"), "large file must be on disk");

        // ── Small file (≤ 4 KiB) ───────────────────────────────────────────
        let small_data = b"tiny file content".to_vec();
        server.mock(|when, then| {
            when.method(GET)
                .path_includes("/files/small-txt")
                .query_param("alt", "media");
            then.status(200).body(small_data.clone());
        });
        register_metadata(&obj, "small-txt", small_data.len() as u64);
        let got = simulate_read(&obj, &client, "small-txt", 0, small_data.len() as u32);
        assert_eq!(got, small_data);

        // Small file: in RAM, NOT on disk.
        assert!(obj.get_content("small-txt").is_some(), "small file must be in RAM");
        assert!(!obj.has_disk_content("small-txt"), "small file must NOT be on disk");
    }
}
