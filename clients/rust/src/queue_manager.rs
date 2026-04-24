//! Queue Manager — five-priority task queue with deduplication and worker pool.
//!
//! # Design
//!
//! Five independent `crossbeam_channel` queues carry `Task` values at distinct
//! priority levels.  Each priority has exactly **one dedicated worker thread**
//! with its own HTTP connection pool (`GClient`), so a slow file download can
//! never delay a directory listing or a metadata fetch.
//!
//! ## Priority levels
//!
//! | Level | `Priority` variant | Served by | Purpose |
//! |-------|--------------------|-----------|---------|
//! | P0 | `DirUrgent`    | 1 worker | User-triggered opendir / readdir cold miss |
//! | P1 | `MetaUrgent`   | 1 worker | User-triggered getattr cache miss |
//! | P2 | `FileDownload` | 1 worker | User-triggered file read / copy |
//! | P3 | `DirPrefetch`  | 1 worker | Background directory listing prefetch |
//! | P4 | `MetaPrefetch` | 1 worker | Background metadata prefetch |
//!
//! P3 and P4 workers **pause** while Drive uploads are in-flight so that
//! uploads always get full network bandwidth.
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
//! - **worker**: execute task, `tracking.remove(key)` → gets all completions
//!   (including any attached while the task was in-flight), stores result in
//!   `ObjectManager`, notifies all waiters.
//!
//! ## Cache policy
//!
//! - Directory listings: always stored in `ObjectManager`.
//! - File metadata: always stored in `ObjectManager`.
//! - File content ≤ 64 KiB (`CACHE_MAX_FILE_BYTES`): stored in content cache.
//! - File content > 64 KiB: **not cached** — bytes are returned directly to
//!   the FUSE `read()` caller and discarded.  Each `read()` for a large file
//!   issues exactly one HTTP Range request for the bytes the kernel requested.

use crate::gclient::GClient;
use crate::object_manager::ObjectManager;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};
use dashmap::DashMap;
use log::{debug, error, info};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

// ── Priority ───────────────────────────────────────────────────────────────

/// Task priority — determines which queue and worker handles the task.
///
/// Each variant maps to exactly one dedicated worker thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// P0 — full directory fetches (`FetchDir`) and ETag revalidations.
    DirUrgent,
    /// P1 — user-triggered attribute fetch (`getattr` cache miss).
    MetaUrgent,
    /// P2 — user-triggered file content access (file `open` / `read` / `cp`).
    FileDownload,
    /// P3 — background directory listing prefetch.  Pauses during uploads.
    DirPrefetch,
    /// P4 — background metadata prefetch.  Pauses during uploads.
    MetaPrefetch,
}

// ── TaskKey ────────────────────────────────────────────────────────────────

/// Unique identity of a unit of work — used as the deduplication key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TaskKey {
    /// Fetch or revalidate a complete directory listing (all pages, conditional if ETag present).
    FetchDir(String),
    /// Fetch metadata for a single file.
    GetMetadata(String),
    /// Download the full content of a small file (≤ `CACHE_RAM_MAX_BYTES`).
    /// Result is stored in the RAM content cache.
    DownloadFile(String),
}

// ── TaskResult ─────────────────────────────────────────────────────────────

/// Value returned to `enqueue_and_wait` callers.
#[derive(Clone, Debug)]
pub enum TaskResult {
    DirListing,
    NotModified,
    FileMetadata,
    FileContent,
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

    fn complete(&self, result: Result<TaskResult, String>) {
        let mut guard = self.result.lock().unwrap();
        *guard = Some(result);
        self.cond.notify_all();
    }

    /// Block until the associated worker completes the task.
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
    enqueued_at: Instant,
    /// Prefetch epoch at enqueue time.  P3/P4 workers discard tasks whose
    /// epoch is older than the current epoch (set when a `DirUrgent` task
    /// arrives), so a burst of user navigation immediately clears the backlog.
    epoch: u64,
}

// ── QueueManager ──────────────────────────────────────────────────────────

/// Five-priority queue manager with per-priority dedicated workers.
pub struct QueueManager {
    dir_urgent_tx: Sender<Task>,
    meta_urgent_tx: Sender<Task>,
    file_tx: Sender<Task>,
    dir_prefetch_tx: Sender<Task>,
    meta_prefetch_tx: Sender<Task>,
    tracking: Arc<DashMap<TaskKey, Vec<Arc<TaskCompletion>>>>,
    /// Number of Drive uploads currently in progress.  P3/P4 workers pause
    /// while this is > 0 so uploads get uncontested network bandwidth.
    pub active_uploads: Arc<AtomicUsize>,
    /// Monotonically increasing epoch counter.  Incremented every time a
    /// `DirUrgent` task is enqueued (= user actively navigates a directory).
    /// P3/P4 tasks stamped with an older epoch are discarded by their workers
    /// rather than executed, effectively purging the prefetch backlog on
    /// every user-triggered navigation event.
    prefetch_epoch: Arc<AtomicU64>,
}

impl QueueManager {
    /// Create a `QueueManager` and spawn five worker threads — one per
    /// priority.  Each worker gets an independent HTTP connection pool
    /// (forked from `client`).
    pub fn new(object_manager: Arc<ObjectManager>, client: Arc<GClient>) -> Arc<Self> {
        let (dir_urgent_tx, dir_urgent_rx) = unbounded::<Task>();
        let (meta_urgent_tx, meta_urgent_rx) = unbounded::<Task>();
        let (file_tx, file_rx) = unbounded::<Task>();
        let (dir_prefetch_tx, dir_prefetch_rx) = unbounded::<Task>();
        let (meta_prefetch_tx, meta_prefetch_rx) = unbounded::<Task>();

        let tracking: Arc<DashMap<TaskKey, Vec<Arc<TaskCompletion>>>> = Arc::new(DashMap::new());
        let active_uploads = Arc::new(AtomicUsize::new(0));
        let prefetch_epoch = Arc::new(AtomicU64::new(0));

        let qm = Arc::new(QueueManager {
            dir_urgent_tx,
            meta_urgent_tx,
            file_tx,
            dir_prefetch_tx,
            meta_prefetch_tx,
            tracking: Arc::clone(&tracking),
            active_uploads: Arc::clone(&active_uploads),
            prefetch_epoch: Arc::clone(&prefetch_epoch),
        });

        info!(
            "QueueManager: spawning 5 workers \
             (p0-dir-urgent, p1-meta-urgent, p2-file, p3-dir-prefetch, p4-meta-prefetch)"
        );

        // P0 — DirUrgent: dedicated HTTP pool, never paused, no epoch
        spawn_worker("gdrive-p0-dir", dir_urgent_rx,
            Arc::clone(&tracking), Arc::clone(&object_manager),
            Arc::new(client.fork()), None, None);

        // P1 — MetaUrgent: dedicated HTTP pool, never paused, no epoch
        spawn_worker("gdrive-p1-meta", meta_urgent_rx,
            Arc::clone(&tracking), Arc::clone(&object_manager),
            Arc::new(client.fork()), None, None);

        // P2 — FileDownload: dedicated HTTP pool, never paused, no epoch
        spawn_worker("gdrive-p2-file", file_rx,
            Arc::clone(&tracking), Arc::clone(&object_manager),
            Arc::new(client.fork()), None, None);

        // P3 — DirPrefetch: pauses during uploads; discards stale epochs
        spawn_worker("gdrive-p3-dir-prefetch", dir_prefetch_rx,
            Arc::clone(&tracking), Arc::clone(&object_manager),
            Arc::new(client.fork()), Some(Arc::clone(&active_uploads)),
            Some(Arc::clone(&prefetch_epoch)));

        // P4 — MetaPrefetch: pauses during uploads; discards stale epochs
        spawn_worker("gdrive-p4-meta-prefetch", meta_prefetch_rx,
            Arc::clone(&tracking), Arc::clone(&object_manager),
            Arc::new(client.fork()), Some(Arc::clone(&active_uploads)),
            Some(Arc::clone(&prefetch_epoch)));

        qm
    }

    // ── Public API ────────────────────────────────────────────────────────

    /// Enqueue a fire-and-forget task.  Does nothing if an identical task is
    /// already queued or in-flight.
    pub fn enqueue(&self, key: TaskKey, priority: Priority) {
        self.enqueue_inner(key, priority, None);
    }

    /// Enqueue a task and block the caller until a worker delivers the result.
    pub fn enqueue_and_wait(&self, key: TaskKey, priority: Priority) -> Result<TaskResult, String> {
        let completion = TaskCompletion::new();
        self.enqueue_inner(key, priority, Some(Arc::clone(&completion)));
        completion.wait()
    }

    // ── Internal ──────────────────────────────────────────────────────────

    fn enqueue_inner(&self, key: TaskKey, priority: Priority, c: Option<Arc<TaskCompletion>>) {
        // Bump the epoch on every user-initiated navigation event.  This purges
        // stale P3/P4 prefetch tasks that are queued behind the current request.
        if priority == Priority::DirUrgent {
            let prev = self.prefetch_epoch.fetch_add(1, Ordering::Release);
            debug!("enqueue: {:?} — prefetch epoch {} → {}", priority, prev, prev + 1);
        }

        match self.tracking.entry(key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                if let Some(completion) = c {
                    e.get_mut().push(completion);
                }
                debug!("enqueue: {:?} already tracked, attaching waiter", key);
                return;
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                e.insert(c.map(|c| vec![c]).unwrap_or_default());
            }
        }

        // Stamp prefetch tasks with the current epoch so P3/P4 workers can
        // detect and discard tasks that pre-date the latest navigation event.
        let epoch = match priority {
            Priority::DirPrefetch | Priority::MetaPrefetch =>
                self.prefetch_epoch.load(Ordering::Acquire),
            _ => 0,
        };

        let task = Task { key, enqueued_at: Instant::now(), epoch };
        let _ = match priority {
            Priority::DirUrgent    => self.dir_urgent_tx.send(task),
            Priority::MetaUrgent   => self.meta_urgent_tx.send(task),
            Priority::FileDownload => self.file_tx.send(task),
            Priority::DirPrefetch  => self.dir_prefetch_tx.send(task),
            Priority::MetaPrefetch => self.meta_prefetch_tx.send(task),
        };
    }
}

// ── Worker ─────────────────────────────────────────────────────────────────

/// Spawn a single named worker thread.
///
/// `active_uploads` — when `Some`, the worker pauses while the counter is > 0.
/// `epoch_guard`    — when `Some` (P3/P4 only), tasks whose epoch is older
///                    than the current value are discarded rather than executed.
fn spawn_worker(
    name: &'static str,
    rx: Receiver<Task>,
    tracking: Arc<DashMap<TaskKey, Vec<Arc<TaskCompletion>>>>,
    obj: Arc<ObjectManager>,
    client: Arc<GClient>,
    active_uploads: Option<Arc<AtomicUsize>>,
    epoch_guard: Option<Arc<AtomicU64>>,
) {
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            debug!("{}: started", name);
            loop {
                // Pause during uploads if this worker should back off.
                if let Some(ref au) = active_uploads {
                    while au.load(Ordering::Relaxed) > 0 {
                        std::thread::sleep(Duration::from_millis(20));
                    }
                }

                let task = match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(t) => t,
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => {
                        debug!("{}: channel closed, exiting", name);
                        return;
                    }
                };

                // Re-check uploads after waking from recv.
                if let Some(ref au) = active_uploads {
                    while au.load(Ordering::Relaxed) > 0 {
                        std::thread::sleep(Duration::from_millis(20));
                    }
                }

                // Epoch check: discard tasks that pre-date the latest
                // DirUrgent navigation event so stale prefetch backlog is
                // cleared immediately when the user changes directories.
                if let Some(ref eg) = epoch_guard {
                    let current = eg.load(Ordering::Acquire);
                    if task.epoch < current {
                        debug!(
                            "{}: discarding stale {:?} (epoch {} < {})",
                            name, task.key, task.epoch, current
                        );
                        // Notify any waiters (fire-and-forget tasks have none).
                        let completions = tracking
                            .remove(&task.key)
                            .map(|(_, v)| v)
                            .unwrap_or_default();
                        for c in completions {
                            c.complete(Ok(TaskResult::NotModified));
                        }
                        continue;
                    }
                }

                run_task(name, task, &tracking, &obj, &client);
            }
        })
        .expect("failed to spawn worker thread");
}

// ── Task execution ──────────────────────────────────────────────────────────

fn run_task(
    worker: &str,
    task: Task,
    tracking: &Arc<DashMap<TaskKey, Vec<Arc<TaskCompletion>>>>,
    obj: &Arc<ObjectManager>,
    client: &Arc<GClient>,
) {
    let queue_delay = task.enqueued_at.elapsed();
    debug!(
        "{}: executing {:?} (queue_delay={:.1}ms)",
        worker, task.key,
        queue_delay.as_secs_f64() * 1000.0
    );

    let exec_start = Instant::now();
    let api_result = execute_task(&task.key, obj, client);
    let exec_dur = exec_start.elapsed();

    let task_result = match &api_result {
        Ok(crate::gclient::ApiOutcome::DirListing(listing)) => {
            if let TaskKey::FetchDir(parent_id) = &task.key {
                obj.store_dir_listing(parent_id, listing.clone());
            }
            Ok(TaskResult::DirListing)
        }
        Ok(crate::gclient::ApiOutcome::NotModified) => {
            if let TaskKey::FetchDir(parent_id) = &task.key {
                if obj.touch_dir(parent_id).is_none() {
                    error!("{}: touch_dir('{}') miss after 304", worker, parent_id);
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
        Err(e) => {
            error!("{}: {:?} failed: {}", worker, task.key, e);
            Err(e.clone())
        }
    };

    let completions = tracking.remove(&task.key).map(|(_, v)| v).unwrap_or_default();
    debug!(
        "{}: {:?} done in {:.1}ms (queue={:.1}ms), {} waiters",
        worker, task.key,
        exec_dur.as_secs_f64() * 1000.0,
        queue_delay.as_secs_f64() * 1000.0,
        completions.len()
    );
    for c in &completions {
        c.complete(task_result.clone());
    }
}

// ── Drive API calls ────────────────────────────────────────────────────────

fn execute_task(
    key: &TaskKey,
    obj: &ObjectManager,
    client: &GClient,
) -> Result<crate::gclient::ApiOutcome, String> {
    match key {
        TaskKey::FetchDir(parent_id) => {
            if let Some(etag) = obj.get_stale_etag(parent_id) {
                match client.revalidate_dir(parent_id, &etag) {
                    Ok(Some(listing)) => {
                        return Ok(crate::gclient::ApiOutcome::DirListing(listing))
                    }
                    Ok(None) => return Ok(crate::gclient::ApiOutcome::NotModified),
                    Err(e) => {
                        debug!("execute_task: revalidate '{}' failed ({}), cold fetch", parent_id, e);
                    }
                }
            }
            client
                .list_files(parent_id)
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

    fn mock_client(server: &MockServer) -> Arc<GClient> {
        let base_url = format!("{}/drive/v3", server.base_url());
        Arc::new(GClient::new_for_test(&base_url, "fake-bearer-token"))
    }

    fn empty_listing_mock(server: &MockServer) -> httpmock::Mock<'_> {
        server.mock(|when, then| {
            when.method(GET).path_includes("/files");
            then.status(200)
                .header("etag", "\"test-etag\"")
                .json_body(json!({ "files": [] }));
        })
    }

    #[test]
    fn fetchdir_ok_stores_listing_in_object_manager() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path_includes("/files");
            then.status(200)
                .header("etag", "\"etag-1\"")
                .json_body(json!({
                    "files": [{"id":"file-abc","name":"hello.txt","mimeType":"text/plain","size":"42"}]
                }));
        });

        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));

        let result = qm.enqueue_and_wait(TaskKey::FetchDir("root".to_string()), Priority::FileDownload);
        assert!(result.is_ok(), "expected Ok, got: {:?}", result.err());
        assert!(obj.has_cache_entry("root"));
        let files = obj.get_dir_files("root").expect("files must be present");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, "file-abc");
    }

    #[test]
    fn fetchdir_server_error_returns_err() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path_includes("/files");
            then.status(500).body("Internal Server Error");
        });

        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));

        let result = qm.enqueue_and_wait(TaskKey::FetchDir("some-dir".to_string()), Priority::FileDownload);
        assert!(result.is_err(), "expected Err on HTTP 500");
        assert!(!obj.has_cache_entry("some-dir"));
    }

    #[test]
    fn get_metadata_ok_stores_in_object_manager() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path_includes("/files/meta-file-id");
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

        let result = qm.enqueue_and_wait(
            TaskKey::GetMetadata("meta-file-id".to_string()),
            Priority::MetaUrgent,
        );
        assert!(result.is_ok());
        let meta = obj.get_metadata("meta-file-id").expect("metadata must be stored");
        assert_eq!(meta.name, "document.pdf");
    }

    #[test]
    fn download_file_ok_stores_content_in_object_manager() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path_includes("/files/dl-file-id").query_param("alt", "media");
            then.status(200).body(b"hello world".to_vec());
        });

        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));

        let result = qm.enqueue_and_wait(
            TaskKey::DownloadFile("dl-file-id".to_string()),
            Priority::FileDownload,
        );
        assert!(result.is_ok());
        assert_eq!(obj.get_content("dl-file-id").as_deref().map(|v| v.as_slice()), Some(b"hello world".as_slice()));
    }

    #[test]
    fn concurrent_same_key_both_callers_receive_result() {
        let server = MockServer::start();
        let mock = empty_listing_mock(&server);

        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));
        let qm2 = Arc::clone(&qm);

        let h1 = std::thread::spawn(move || {
            qm.enqueue_and_wait(TaskKey::FetchDir("shared-dir".to_string()), Priority::FileDownload)
        });
        std::thread::sleep(std::time::Duration::from_millis(5));
        let h2 = std::thread::spawn(move || {
            qm2.enqueue_and_wait(TaskKey::FetchDir("shared-dir".to_string()), Priority::FileDownload)
        });

        let r1 = h1.join().expect("thread 1 panicked");
        let r2 = h2.join().expect("thread 2 panicked");
        assert!(r1.is_ok(), "caller 1: {:?}", r1.err());
        assert!(r2.is_ok(), "caller 2: {:?}", r2.err());
        assert!(mock.calls() <= 2, "unexpected extra HTTP calls: {}", mock.calls());
    }

    #[test]
    fn enqueue_fire_and_forget_does_not_block() {
        let server = MockServer::start();
        empty_listing_mock(&server);

        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));

        let start = std::time::Instant::now();
        qm.enqueue(TaskKey::FetchDir("prefetch-dir".to_string()), Priority::DirPrefetch);
        assert!(start.elapsed() < std::time::Duration::from_millis(100));
    }

    #[test]
    fn dir_urgent_task_completes_successfully() {
        let server = MockServer::start();
        empty_listing_mock(&server);
        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));
        assert!(qm
            .enqueue_and_wait(TaskKey::FetchDir("d".to_string()), Priority::DirUrgent)
            .is_ok());
    }

    #[test]
    fn meta_urgent_task_completes_successfully() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path_includes("/files/f1");
            then.status(200).json_body(json!({
                "id":"f1","name":"a.txt","mimeType":"text/plain","size":"10"
            }));
        });
        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));
        assert!(qm
            .enqueue_and_wait(TaskKey::GetMetadata("f1".to_string()), Priority::MetaUrgent)
            .is_ok());
    }

    #[test]
    fn dir_prefetch_task_completes_successfully() {
        let server = MockServer::start();
        empty_listing_mock(&server);
        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));
        assert!(qm
            .enqueue_and_wait(TaskKey::FetchDir("lo-dir".to_string()), Priority::DirPrefetch)
            .is_ok());
    }

    #[test]
    fn meta_prefetch_task_completes_successfully() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path_includes("/files/pf1");
            then.status(200).json_body(json!({
                "id":"pf1","name":"b.txt","mimeType":"text/plain","size":"20"
            }));
        });
        let obj = Arc::new(ObjectManager::new());
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));
        assert!(qm
            .enqueue_and_wait(TaskKey::GetMetadata("pf1".to_string()), Priority::MetaPrefetch)
            .is_ok());
    }

    #[test]
    fn download_large_file_stored_on_disk_not_in_ram() {
        use crate::object_manager::CACHE_RAM_MAX_BYTES;
        use tempfile::TempDir;

        // A file clearly above the RAM threshold (16 KiB).
        let file_size = (CACHE_RAM_MAX_BYTES * 4) as usize;
        let payload: Vec<u8> = (0..file_size).map(|i| (i % 251) as u8).collect();

        let server = MockServer::start();
        let payload_clone = payload.clone();
        server.mock(|when, then| {
            when.method(GET)
                .path_includes("/files/large-cached")
                .query_param("alt", "media");
            then.status(200).body(payload_clone);
        });

        let tmp = TempDir::new().unwrap();
        let obj = Arc::new(ObjectManager::new_for_test(tmp.path().to_path_buf()));
        let qm = QueueManager::new(Arc::clone(&obj), mock_client(&server));

        let result = qm.enqueue_and_wait(
            TaskKey::DownloadFile("large-cached".to_string()),
            Priority::FileDownload,
        );
        assert!(result.is_ok());

        // Large file must NOT be in RAM.
        assert!(
            obj.get_content("large-cached").is_none(),
            "large file must not occupy RAM"
        );
        // Large file MUST be on disk.
        assert!(
            obj.has_disk_content("large-cached"),
            "large file must be in disk cache"
        );
        // Verify slice correctness.
        let slice = obj
            .read_disk_slice("large-cached", 1024, 512)
            .expect("slice must be readable");
        assert_eq!(slice, &payload[1024..1536]);
    }
}
