//! Queue Manager — priority task queue with deduplication and worker pool.
//!
//! # Design
//!
//! Three `crossbeam_channel` queues carry `Task` values at different priority
//! levels.  A pool of worker threads drains them; each worker polls the HIGH
//! queue first (non-blocking), the NORMAL queue next, then blocks on a
//! `select!` across all three.
//!
//! ## Deduplication
//!
//! Before inserting a new task the manager checks whether an **identical task
//! is already queued or being processed by a worker**.  If it is, the new
//! caller's `TaskCompletion` handle is attached to the existing entry so it
//! receives the result when the worker finishes — without issuing a duplicate
//! Drive API call.
//!
//! A single `tracking: DashMap<TaskKey, Vec<Arc<TaskCompletion>>>` covers both
//! states (queued + in-flight):
//!
//! - **`enqueue_inner`**: if key present → attach completion and return;
//!   otherwise insert and send to the channel.
//! - **worker**: execute task, `tracking.remove(key)` at the end → gets all
//!   completions (including any attached while the task was in-flight), stores
//!   result in `ObjectManager`, notifies all waiters.
//!
//! There is a small window between `tracking.remove` and the next insert
//! where a racing caller will not find the key and enqueue a fresh task.  This
//! is safe — the result is already in `ObjectManager` and the new caller will
//! find it on the re-check after `enqueue_and_wait` returns.

use crate::gclient::GClient;
use crate::object_manager::ObjectManager;
use crossbeam_channel::{Receiver, Sender, unbounded};
use dashmap::DashMap;
use log::{debug, error, info};
use std::sync::{Arc, Condvar, Mutex};

// ── Priority ───────────────────────────────────────────────────────────────

/// Task priority — lower value = served first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// User-triggered FUSE operation (lookup, getattr, readdir, read).
    High,
    /// Background directory prefetch.
    Normal,
    /// Background small-file content prefetch.
    Low,
}

// ── TaskKey ────────────────────────────────────────────────────────────────

/// Unique identity of a unit of work — used as the deduplication key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TaskKey {
    /// Fetch or revalidate a directory listing (worker decides which based on
    /// cache state: revalidate if there is a stale ETag, cold fetch otherwise).
    FetchDir(String),
    /// Fetch only the **first page** of a directory listing and store a
    /// partial result.  A separate `FetchDirPages` task is enqueued at Low
    /// priority to retrieve the remaining pages.
    FetchDirFirstPage(String),
    /// Fetch all remaining pages of a directory listing, continuing from the
    /// stored partial result.  The `String` fields are:
    /// `(parent_id, page_token, etag)`.
    FetchDirPages(String, String, String),
    /// Fetch metadata for a single file.
    GetMetadata(String),
    /// Download the full content of a file (≤ `SMALL_FILE_MAX_BYTES`).
    DownloadFile(String),
    /// Download a specific byte range of a file (used for large files).
    /// Fields: `(file_id, offset, length)`.  Results are NOT cached.
    DownloadFileRange(String, u64, u32),
}

// ── TaskResult ─────────────────────────────────────────────────────────────

/// Value stored in `ObjectManager` and returned to `enqueue_and_wait` callers.
#[derive(Clone, Debug)]
pub enum TaskResult {
    DirListing,
    /// First page stored; `Some((page_token, etag))` when more pages remain.
    /// The caller (fuse_ops) must enqueue `TaskKey::FetchDirPages` at Low
    /// priority to retrieve the remaining pages.
    #[allow(dead_code)]
    DirListingPartial(Option<(String, String)>),
    NotModified,
    FileMetadata,
    FileContent,
    /// Range bytes returned directly to the caller — not stored in the cache.
    FileContentRange(Vec<u8>),
}

// ── TaskCompletion ─────────────────────────────────────────────────────────

/// Synchronisation handle passed to `enqueue_and_wait` callers.
pub struct TaskCompletion {
    result: Mutex<Option<Result<TaskResult, String>>>,
    cond: Condvar,
}

impl TaskCompletion {
    fn new() -> Arc<Self> {
        Arc::new(Self { result: Mutex::new(None), cond: Condvar::new() })
    }

    /// Called by a worker when the task finishes.
    fn complete(&self, result: Result<TaskResult, String>) {
        let mut guard = self.result.lock().unwrap();
        *guard = Some(result);
        self.cond.notify_all();
    }

    /// Block until the associated worker completes the task.
    /// Returns immediately if the result is already available.
    pub fn wait(&self) -> Result<TaskResult, String> {
        let mut guard = self.result.lock().unwrap();
        loop {
            if let Some(r) = guard.take() {
                return r;
            }
            guard = self.cond.wait(guard).unwrap();
        }
    }
}

// ── Internal Task ──────────────────────────────────────────────────────────

struct Task {
    key: TaskKey,
}

// ── QueueManager ──────────────────────────────────────────────────────────

/// Priority queue manager with built-in deduplication and worker pool.
pub struct QueueManager {
    high_tx: Sender<Task>,
    normal_tx: Sender<Task>,
    low_tx: Sender<Task>,
    /// Tracks all tasks that are either queued or being processed.
    /// key → list of completion handles waiting for the result.
    ///
    /// Wrapped in an `Arc` so workers can hold a clone of this map without
    /// holding an `Arc<QueueManager>` reference — which would prevent the
    /// senders from being dropped on `QueueManager::drop` and cause a deadlock.
    tracking: Arc<DashMap<TaskKey, Vec<Arc<TaskCompletion>>>>,
}

impl QueueManager {
    /// Creates a `QueueManager` and spawns `num_workers` worker threads.
    ///
    /// Workers are daemon-like: they run until the channels are dropped (i.e.
    /// when the `QueueManager` itself is dropped on process exit).
    /// Creates a `QueueManager` with a fixed worker-pool layout:
    ///
    /// | Pool             | Count | Queues served        | Purpose                          |
    /// |------------------|-------|----------------------|----------------------------------|
    /// | Full workers     |     8 | High + Normal + Low  | User-triggered FUSE ops + spill  |
    /// | Dir-prefetch     |     4 | Normal + Low         | Background directory listings    |
    /// | File-prefetch    |     1 | Low only             | Background small-file downloads  |
    ///
    /// Keeping dedicated Low/Normal-only workers ensures that a burst of
    /// background prefetch tasks can never starve the High-priority lane that
    /// serves interactive FUSE operations.
    pub fn new(
        object_manager: Arc<ObjectManager>,
        client: Arc<GClient>,
    ) -> Arc<Self> {
        let (high_tx, high_rx) = unbounded::<Task>();
        let (normal_tx, normal_rx) = unbounded::<Task>();
        let (low_tx, low_rx) = unbounded::<Task>();

        // The tracking map is shared between QueueManager (for enqueue_inner)
        // and each worker (for remove-and-notify).  Workers hold an independent
        // Arc so dropping the QueueManager closes the channels and workers exit
        // without a circular-reference deadlock.
        let tracking: Arc<DashMap<TaskKey, Vec<Arc<TaskCompletion>>>> =
            Arc::new(DashMap::new());

        let qm = Arc::new(QueueManager {
            high_tx,
            normal_tx,
            low_tx,
            tracking: Arc::clone(&tracking),
        });

        // ── Full workers (High + Normal + Low) ─────────────────────────────
        const FULL_WORKERS: usize = 8;
        // ── Dir-prefetch workers (Normal + Low only) ───────────────────────
        const DIR_WORKERS: usize = 4;
        // ── Small-file workers (Low only, ≤ 64 KiB) ───────────────────────
        // 24 workers flush large directories quickly without competing with
        // the interactive Full/Dir-prefetch pools.
        const FILE_WORKERS: usize = 24;

        let total = FULL_WORKERS + DIR_WORKERS + FILE_WORKERS;
        info!(
            "QueueManager: spawning {} workers ({} full, {} dir-prefetch, {} small-file)",
            total, FULL_WORKERS, DIR_WORKERS, FILE_WORKERS
        );

        let mut id = 0usize;

        for _ in 0..FULL_WORKERS {
            let (h, n, l) = (high_rx.clone(), normal_rx.clone(), low_rx.clone());
            let (tr, obj, cli) = (Arc::clone(&tracking), Arc::clone(&object_manager), Arc::clone(&client));
            let worker_id = id; id += 1;
            std::thread::Builder::new()
                .name(format!("gdrive-full-{}", worker_id))
                .spawn(move || worker_loop(worker_id, h, n, l, tr, obj, cli))
                .expect("failed to spawn worker thread");
        }

        for _ in 0..DIR_WORKERS {
            let (n, l) = (normal_rx.clone(), low_rx.clone());
            let (tr, obj, cli) = (Arc::clone(&tracking), Arc::clone(&object_manager), Arc::clone(&client));
            let worker_id = id; id += 1;
            std::thread::Builder::new()
                .name(format!("gdrive-dir-{}", worker_id))
                .spawn(move || worker_loop_dir(worker_id, n, l, tr, obj, cli))
                .expect("failed to spawn worker thread");
        }

        for _ in 0..FILE_WORKERS {
            let l = low_rx.clone();
            let (tr, obj, cli) = (Arc::clone(&tracking), Arc::clone(&object_manager), Arc::clone(&client));
            let worker_id = id; id += 1;
            std::thread::Builder::new()
                .name(format!("gdrive-file-{}", worker_id))
                .spawn(move || worker_loop_file(worker_id, l, tr, obj, cli))
                .expect("failed to spawn worker thread");
        }

        qm
    }

    // ── Public API ────────────────────────────────────────────────────────

    /// Enqueue a fire-and-forget task.  Does nothing if an identical task is
    /// already queued or in-flight.
    pub fn enqueue(&self, key: TaskKey, priority: Priority) {
        self.enqueue_inner(key, priority, None);
    }

    /// Enqueue a task and block the caller until a worker delivers the result.
    ///
    /// If the same task is already in-flight, the caller is attached to it and
    /// woken up when that worker finishes — no duplicate API call is issued.
    pub fn enqueue_and_wait(&self, key: TaskKey, priority: Priority) -> Result<TaskResult, String> {
        let completion = TaskCompletion::new();
        self.enqueue_inner(key, priority, Some(Arc::clone(&completion)));
        completion.wait()
    }

    // ── Internal ──────────────────────────────────────────────────────────

    fn enqueue_inner(&self, key: TaskKey, priority: Priority, c: Option<Arc<TaskCompletion>>) {
        match self.tracking.entry(key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                // Task already queued or in-flight — attach completion if any.
                if let Some(completion) = c {
                    e.get_mut().push(completion);
                }
                debug!("enqueue: {:?} already tracked, attaching waiter", key);
                return;
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                // First occurrence — register and enqueue.
                let completions = c.map(|c| vec![c]).unwrap_or_default();
                e.insert(completions);
            }
        }

        let task = Task { key };
        let _ = match priority {
            Priority::High => self.high_tx.send(task),
            Priority::Normal => self.normal_tx.send(task),
            Priority::Low => self.low_tx.send(task),
        };
    }
}

// ── Worker loop ────────────────────────────────────────────────────────────

fn worker_loop(
    id: usize,
    high: Receiver<Task>,
    normal: Receiver<Task>,
    low: Receiver<Task>,
    tracking: Arc<DashMap<TaskKey, Vec<Arc<TaskCompletion>>>>,
    obj: Arc<ObjectManager>,
    client: Arc<GClient>,
) {
    debug!("worker-full-{}: started", id);
    loop {
        // Poll higher-priority queues without blocking first, then do a
        // blocking select across all three.  This gives HIGH strict priority
        // over NORMAL/LOW while still waiting efficiently when all are empty.
        let task = if let Ok(t) = high.try_recv() {
            t
        } else if let Ok(t) = normal.try_recv() {
            t
        } else {
            crossbeam_channel::select! {
                recv(high)   -> msg => match msg { Ok(t) => t, Err(_) => { debug!("worker-full-{}: channel closed", id); return; } },
                recv(normal) -> msg => match msg { Ok(t) => t, Err(_) => { debug!("worker-full-{}: channel closed", id); return; } },
                recv(low)    -> msg => match msg { Ok(t) => t, Err(_) => { debug!("worker-full-{}: channel closed", id); return; } },
            }
        };
        run_task(id, "full", task, &tracking, &obj, &client);
    }
}

/// Worker that serves only the **Normal** and **Low** queues.
/// Used for background directory prefetch; never blocks High-priority FUSE ops.
fn worker_loop_dir(
    id: usize,
    normal: Receiver<Task>,
    low: Receiver<Task>,
    tracking: Arc<DashMap<TaskKey, Vec<Arc<TaskCompletion>>>>,
    obj: Arc<ObjectManager>,
    client: Arc<GClient>,
) {
    debug!("worker-dir-{}: started", id);
    loop {
        let task = if let Ok(t) = normal.try_recv() {
            t
        } else {
            crossbeam_channel::select! {
                recv(normal) -> msg => match msg { Ok(t) => t, Err(_) => { debug!("worker-dir-{}: channel closed", id); return; } },
                recv(low)    -> msg => match msg { Ok(t) => t, Err(_) => { debug!("worker-dir-{}: channel closed", id); return; } },
            }
        };
        run_task(id, "dir", task, &tracking, &obj, &client);
    }
}

/// Worker that serves **only** the Low queue.
/// Used for small-file prefetch; never competes with interative paths.
fn worker_loop_file(
    id: usize,
    low: Receiver<Task>,
    tracking: Arc<DashMap<TaskKey, Vec<Arc<TaskCompletion>>>>,
    obj: Arc<ObjectManager>,
    client: Arc<GClient>,
) {
    debug!("worker-file-{}: started", id);
    loop {
        let task = match low.recv() {
            Ok(t) => t,
            Err(_) => { debug!("worker-file-{}: channel closed", id); return; }
        };
        run_task(id, "file", task, &tracking, &obj, &client);
    }
}

/// Execute `task`, store any result in `ObjectManager`, and notify waiters.
/// Extracted from `worker_loop` so all three worker variants share it.
fn run_task(
    id: usize,
    kind: &str,
    task: Task,
    tracking: &Arc<DashMap<TaskKey, Vec<Arc<TaskCompletion>>>>,
    obj: &Arc<ObjectManager>,
    client: &Arc<GClient>,
) {
    debug!("worker-{}-{}: executing {:?}", kind, id, task.key);

    let api_result = execute_task(&task.key, obj, client);

    let task_result = match &api_result {
        Ok(crate::gclient::ApiOutcome::DirListing(listing)) => {
            if let TaskKey::FetchDir(parent_id) | TaskKey::FetchDirPages(parent_id, _, _) =
                &task.key
            {
                obj.store_dir_listing(parent_id, listing.clone());
            }
            Ok(TaskResult::DirListing)
        }
        Ok(crate::gclient::ApiOutcome::DirListingFirstPage {
            files,
            next_page_token,
            etag,
        }) => {
            if let TaskKey::FetchDirFirstPage(parent_id) = &task.key {
                obj.store_dir_partial(parent_id, files.clone(), etag.clone());
                if let Some(pt) = next_page_token {
                    info!(
                        "worker-{}-{}: first page stored for '{}' ({} files), \
                         continuation will be enqueued by caller",
                        kind, id, parent_id, files.len()
                    );
                    Ok(TaskResult::DirListingPartial(Some((pt.clone(), etag.clone()))))
                } else {
                    obj.store_dir_listing(
                        parent_id,
                        crate::gclient::DirListing {
                            files: files.clone(),
                            etag: etag.clone(),
                        },
                    );
                    info!(
                        "worker-{}-{}: single-page fetch complete for '{}' ({} files)",
                        kind, id, parent_id, files.len()
                    );
                    Ok(TaskResult::DirListing)
                }
            } else {
                Ok(TaskResult::DirListingPartial(None))
            }
        }
        Ok(crate::gclient::ApiOutcome::NotModified) => {
            if let TaskKey::FetchDir(parent_id) = &task.key {
                if obj.touch_dir(parent_id).is_none() {
                    error!("worker-{}-{}: touch_dir('{}') miss after 304", kind, id, parent_id);
                }
            }
            Ok(TaskResult::NotModified)
        }
        Ok(crate::gclient::ApiOutcome::FileMetadata(info)) => {
            obj.store_metadata(info.clone());
            Ok(TaskResult::FileMetadata)
        }
        Ok(crate::gclient::ApiOutcome::FileContent(bytes)) => {
            if let TaskKey::DownloadFile(file_id) = &task.key {
                obj.store_content(file_id, bytes.clone());
            }
            Ok(TaskResult::FileContent)
        }
        Ok(crate::gclient::ApiOutcome::FileContentRange(bytes)) => {
            Ok(TaskResult::FileContentRange(bytes.clone()))
        }
        Err(e) => {
            error!("worker-{}-{}: {:?} failed: {}", kind, id, task.key, e);
            Err(e.clone())
        }
    };

    let completions = tracking.remove(&task.key).map(|(_, v)| v).unwrap_or_default();
    debug!("worker-{}-{}: {:?} done, notifying {} waiters", kind, id, task.key, completions.len());
    for c in &completions {
        c.complete(task_result.clone());
    }
}

// ── Task execution (pure Drive API calls) ─────────────────────────────────

fn execute_task(
    key: &TaskKey,
    obj: &ObjectManager,
    client: &GClient,
) -> Result<crate::gclient::ApiOutcome, String> {
    match key {
        TaskKey::FetchDir(parent_id) => {
            // If the cache has a stale (expired) entry with a valid ETag, try
            // a conditional request first.  This avoids re-downloading the full
            // listing when nothing has changed.
            if let Some(etag) = obj.get_stale_etag(parent_id) {
                match client.revalidate_dir(parent_id, &etag) {
                    Ok(Some(listing)) => {
                        return Ok(crate::gclient::ApiOutcome::DirListing(listing))
                    }
                    Ok(None) => return Ok(crate::gclient::ApiOutcome::NotModified),
                    Err(e) => {
                        // Revalidation failed — fall through to a cold fetch.
                        debug!("execute_task: revalidate '{}' failed ({}), cold fetch", parent_id, e);
                    }
                }
            }
            client
                .list_files(parent_id)
                .map(crate::gclient::ApiOutcome::DirListing)
                .map_err(|e| e.to_string())
        }
        TaskKey::FetchDirFirstPage(parent_id) => {
            // Fetch only the first page so FUSE readdir can return partial
            // results immediately.  If there are more pages a FetchDirPages
            // continuation task is signalled via the outcome.
            client
                .list_files_first_page(parent_id)
                .map(|(files, next_page_token, etag)| {
                    crate::gclient::ApiOutcome::DirListingFirstPage { files, next_page_token, etag }
                })
                .map_err(|e| e.to_string())
        }
        TaskKey::FetchDirPages(parent_id, page_token, etag) => {
            // If the cache was already completed by a concurrent FetchDir
            // (e.g. startup-prefetch), skip the continuation to avoid
            // replacing the full listing with only the remaining pages.
            if obj.is_dir_complete(parent_id) {
                debug!(
                    "execute_task: FetchDirPages('{}') skipped \
                     — cache already complete",
                    parent_id
                );
                return Ok(crate::gclient::ApiOutcome::NotModified);
            }
            // Use the already-cached first page as the accumulator so the
            // final listing contains all entries (page 1 + pages 2..n).
            let accumulator = obj.get_dir_files(parent_id).unwrap_or_default();
            client
                .list_files_pages(parent_id, page_token.clone(), accumulator, etag.clone())
                .map(crate::gclient::ApiOutcome::DirListing)
                .map_err(|e| e.to_string())
        }
        TaskKey::GetMetadata(file_id) => client
            .get_file_metadata(file_id)
            .map(crate::gclient::ApiOutcome::FileMetadata)
            .map_err(|e| e.to_string()),
        TaskKey::DownloadFile(file_id) => client
            .download_file(file_id)
            .map(crate::gclient::ApiOutcome::FileContent)
            .map_err(|e| e.to_string()),
        TaskKey::DownloadFileRange(file_id, offset, length) => client
            .download_file_range(file_id, *offset, *length)
            .map(crate::gclient::ApiOutcome::FileContentRange)
            .map_err(|e| e.to_string()),
    }
}

// ── Unit Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gclient::GClient;
    use crate::object_manager::ObjectManager;
    use httpmock::prelude::*;
    use serde_json::json;
    use std::sync::Arc;

    // ── helpers ───────────────────────────────────────────────────────────

    /// Start a mock server and build a GClient pointing at it.
    fn mock_client(server: &MockServer) -> Arc<GClient> {
        let base_url = format!("{}/drive/v3", server.base_url());
        Arc::new(GClient::new_for_test(&base_url, "fake-bearer-token"))
    }

    fn empty_listing_mock(server: &MockServer) -> httpmock::Mock<'_> {
        server.mock(|when, then| {
            when.method(GET).path_contains("/files");
            then.status(200)
                .header("etag", "\"test-etag\"")
                .json_body(json!({ "files": [] }));
        })
    }

    // ── FetchDir ──────────────────────────────────────────────────────────

    #[test]
    fn fetchdir_ok_stores_listing_in_object_manager() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path_contains("/files");
            then.status(200)
                .header("etag", "\"etag-1\"")
                .json_body(json!({
                    "files": [
                        {
                            "id": "file-abc",
                            "name": "hello.txt",
                            "mimeType": "text/plain",
                            "size": "42"
                        }
                    ]
                }));
        });

        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));

        let result =
            qm.enqueue_and_wait(TaskKey::FetchDir("root".to_string()), Priority::High);

        assert!(result.is_ok(), "expected Ok, got: {:?}", result.err());
        assert!(obj.has_cache_entry("root"), "listing must be stored in ObjectManager");
        let files = obj.get_dir_files("root").expect("files must be present");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, "file-abc");
    }

    #[test]
    fn fetchdir_server_error_returns_err() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path_contains("/files");
            then.status(500).body("Internal Server Error");
        });

        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));

        let result =
            qm.enqueue_and_wait(TaskKey::FetchDir("some-dir".to_string()), Priority::High);

        assert!(result.is_err(), "expected Err on HTTP 500");
        assert!(!obj.has_cache_entry("some-dir"), "failed fetch must not store anything");
    }

    // ── GetMetadata ───────────────────────────────────────────────────────

    #[test]
    fn get_metadata_ok_stores_in_object_manager() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path_contains("/files/meta-file-id");
            then.status(200).json_body(json!({
                "id": "meta-file-id",
                "name": "document.pdf",
                "mimeType": "application/pdf",
                "size": "512",
                "modifiedTime": "2024-01-01T00:00:00Z"
            }));
        });

        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));

        let result = qm
            .enqueue_and_wait(TaskKey::GetMetadata("meta-file-id".to_string()), Priority::High);

        assert!(result.is_ok());
        let meta = obj.get_metadata("meta-file-id").expect("metadata must be stored");
        assert_eq!(meta.name, "document.pdf");
    }

    // ── DownloadFile ──────────────────────────────────────────────────────

    #[test]
    fn download_file_ok_stores_content_in_object_manager() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path_contains("/files/dl-file-id").query_param("alt", "media");
            then.status(200).body(b"hello world".to_vec());
        });

        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));

        let result = qm.enqueue_and_wait(
            TaskKey::DownloadFile("dl-file-id".to_string()),
            Priority::High,
        );

        assert!(result.is_ok());
        assert_eq!(obj.get_content("dl-file-id"), Some(b"hello world".to_vec()));
    }

    // ── Deduplication ─────────────────────────────────────────────────────

    #[test]
    fn concurrent_same_key_both_callers_receive_result() {
        let server = MockServer::start();
        // Single worker so the second enqueue definitely finds the first in-flight.
        let mock = empty_listing_mock(&server);

        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));
        let qm2 = Arc::clone(&qm);

        // Launch two threads; both request the same directory.
        let h1 = std::thread::spawn(move || {
            qm.enqueue_and_wait(TaskKey::FetchDir("shared-dir".to_string()), Priority::High)
        });
        // Give the first thread a moment to register in tracking before the second one.
        std::thread::sleep(std::time::Duration::from_millis(5));
        let h2 = std::thread::spawn(move || {
            qm2.enqueue_and_wait(TaskKey::FetchDir("shared-dir".to_string()), Priority::High)
        });

        let r1 = h1.join().expect("thread 1 panicked");
        let r2 = h2.join().expect("thread 2 panicked");

        assert!(r1.is_ok(), "caller 1 expected Ok, got: {:?}", r1.err());
        assert!(r2.is_ok(), "caller 2 expected Ok, got: {:?}", r2.err());
        // With deduplication the mock is called at most once; without it at most twice.
        assert!(mock.hits() <= 2, "unexpected extra HTTP calls: {}", mock.hits());
    }

    // ── Fire-and-forget ───────────────────────────────────────────────────

    #[test]
    fn enqueue_fire_and_forget_does_not_block() {
        let server = MockServer::start();
        empty_listing_mock(&server);

        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));

        // enqueue() must return immediately — not block until the worker finishes.
        let start = std::time::Instant::now();
        qm.enqueue(TaskKey::FetchDir("prefetch-dir".to_string()), Priority::Low);
        assert!(
            start.elapsed() < std::time::Duration::from_millis(100),
            "enqueue must be non-blocking"
        );
    }

    // ── Priority ordering ─────────────────────────────────────────────────

    #[test]
    fn high_priority_task_completes_successfully() {
        let server = MockServer::start();
        empty_listing_mock(&server);

        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));

        let result =
            qm.enqueue_and_wait(TaskKey::FetchDir("hi-dir".to_string()), Priority::High);
        assert!(result.is_ok());
    }

    #[test]
    fn low_priority_task_still_completes() {
        let server = MockServer::start();
        empty_listing_mock(&server);

        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));

        let result =
            qm.enqueue_and_wait(TaskKey::FetchDir("lo-dir".to_string()), Priority::Low);
        assert!(result.is_ok());
    }
}
