//! Background Write-Back Uploader — watches for dirty cache entries and
//! uploads them to Google Drive asynchronously.
//!
//! # Protocol
//!
//! 1. At startup, `UploadManager` immediately scans the SQLite `metadata`
//!    table for entries with `is_dirty = 1` left over from a previous
//!    session (crash-recovery).
//! 2. The background thread then blocks on a wakeup channel with a
//!    [`UPLOAD_POLL_INTERVAL`] timeout.  Every FUSE `release()` call sends
//!    a wakeup signal so that new writes are uploaded without waiting for
//!    the next scheduled tick.
//! 3. For each dirty entry:
//!    - Load content from the disk cache or SQLite BLOB (written by
//!      `ObjectManager::write_local_dirty`).
//!    - Upload via the Drive API:
//!      - **New file** (`remote_id` starts with `"__pending__"`): call
//!        `GClient::create_file`, then call `ObjectManager::replace_pending_id`
//!        and `DbManager::finalize_new_file` to swap the temporary ID for the
//!        real Drive ID.
//!      - **Existing file**: call `GClient::update_file_content`, then
//!        `DbManager::clear_dirty_after_upload`.
//! 4. Upload failures are logged; the dirty flag is left set so the entry
//!    is retried on the next wakeup or tick.
//!
//! # Race-condition note
//!
//! If the user writes to a file while an upload of an earlier version is
//! in progress, the second `release()` call sets `is_dirty = 1` again.
//! When the first upload completes it calls `clear_dirty_after_upload`,
//! which clears the flag.  The second write is then re-detected on the
//! next poll cycle (≤ `UPLOAD_POLL_INTERVAL`).  This is an accepted
//! trade-off for Phase 4; a future version may use a per-file version
//! counter to avoid the second-write window entirely.

use crate::db_manager::DbManager;
use crate::gclient::GClient;
use crate::object_manager::ObjectManager;
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Returns `true` when `e` wraps an HTTP 404 response from the Drive API.
///
/// Used to distinguish "parent folder deleted" (unrecoverable, must abandon)
/// from transient errors (network timeout, 5xx, …) that should be retried.
fn is_http_not_found(e: &anyhow::Error) -> bool {
    e.downcast_ref::<reqwest::Error>()
        .and_then(|re| re.status())
        .map(|s| s == reqwest::StatusCode::NOT_FOUND)
        .unwrap_or(false)
}

/// Maximum time between automatic dirty-entry polls when no wakeup signal
/// has been received from the FUSE layer.
const UPLOAD_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Background upload manager: dequeues dirty cache entries and synchronises
/// them with Google Drive non-blocking from the FUSE event-loop thread.
pub struct UploadManager {
    db: Arc<DbManager>,
    obj: Arc<ObjectManager>,
    /// Dedicated HTTP client with its own connection pool so uploads never
    /// delay concurrent downloads or metadata fetches.
    client: Arc<GClient>,
    /// Wakeup pings from the FUSE `release()` path signal new dirty entries.
    wakeup_rx: crossbeam_channel::Receiver<()>,
}

impl UploadManager {
    /// Create a new `UploadManager` and return it together with the sender
    /// that the FUSE layer uses to signal newly dirtied files.
    ///
    /// The `base_client` is forked so the uploader owns an independent HTTP
    /// connection pool.
    pub fn new(
        db: Arc<DbManager>,
        obj: Arc<ObjectManager>,
        base_client: &GClient,
    ) -> (Self, crossbeam_channel::Sender<()>) {
        let (wakeup_tx, wakeup_rx) = crossbeam_channel::unbounded();
        let client = Arc::new(base_client.fork());
        let mgr = Self { db, obj, client, wakeup_rx };
        (mgr, wakeup_tx)
    }

    /// Spawn the background `"gdrive-uploader"` thread.
    ///
    /// On startup the thread immediately runs a crash-recovery pass before
    /// entering the event loop, ensuring that any dirty entries left by a
    /// previous session are uploaded without user intervention.
    pub fn start(self: Arc<Self>) {
        std::thread::Builder::new()
            .name("gdrive-uploader".to_string())
            .spawn(move || self.run())
            .expect("spawn gdrive-uploader thread");
        info!(
            "upload-manager: background uploader started (poll_interval={:?})",
            UPLOAD_POLL_INTERVAL
        );
    }

    fn run(&self) {
        // Crash-recovery: upload anything dirty from the previous session.
        self.process_dirty_entries();

        let tick = crossbeam_channel::tick(UPLOAD_POLL_INTERVAL);
        loop {
            crossbeam_channel::select! {
                recv(self.wakeup_rx) -> _ => {
                    debug!("upload-manager: wakeup signal");
                }
                recv(tick) -> _ => {
                    debug!("upload-manager: poll tick");
                }
            }
            self.process_dirty_entries();
        }
    }

    fn process_dirty_entries(&self) {
        let entries = self.db.list_dirty_entries();
        if entries.is_empty() {
            return;
        }
        info!("upload-manager: {} dirty entry/entries to upload", entries.len());
        for meta in &entries {
            self.upload_entry(meta);
        }
    }

    fn upload_entry(&self, meta: &crate::db_manager::CachedMeta) {
        let remote_id = &meta.remote_id;
        let is_new = remote_id.starts_with("__pending__");

        // Load the locally persisted content from whichever cache tier holds it.
        let mut content = if let Some(arc) = self.obj.get_content(remote_id) {
            Arc::unwrap_or_clone(arc)
        } else if let Some(data) = self.obj.read_full_disk_content(remote_id) {
            data
        } else {
            warn!(
                "upload-manager: local content not found for '{}' (name='{}') — skipping",
                remote_id, meta.name
            );
            return;
        };

        // Belt-and-suspenders: get_content() returns the SQLite BLOB when moka
        // misses, but an earlier flush() with 0-byte content may have stored a
        // stale empty BLOB before the real content arrived.  The store_content_at_key
        // fix should normally clear it, but as a safety net: if content is empty
        // yet the disk cache has data, use the disk copy instead.
        if content.is_empty() {
            if let Some(disk_data) = self.obj.read_full_disk_content(remote_id) {
                if !disk_data.is_empty() {
                    debug!(
                        "upload-manager: overriding empty get_content() with disk data ({} bytes) for '{}'",
                        disk_data.len(), remote_id
                    );
                    content = disk_data;
                }
            }
            // For new files: if no real content found anywhere, a genuine
            // 0-byte write (or a race where write_local_dirty hasn't finished
            // yet) landed here.  Defer until the next poll cycle rather than
            // uploading an empty file.
            if is_new && content.is_empty() {
                debug!(
                    "upload-manager: deferring new '{}' ('{}'): no content found yet — retrying on next cycle",
                    remote_id, meta.name
                );
                return;
            }
        }

        if is_new {
            // ── New file: create on Drive ──────────────────────────────────

            // Settling delay for the rename-after-release race.
            //
            // Chrome downloads always close the file handle BEFORE issuing the
            // rename (*.crdownload → *.pdf).  Because notify_tx wakes the
            // UploadManager immediately after release(), the old snapshot from
            // list_dirty_entries() and even the fresh re-read from SQLite can
            // both land BEFORE rename_pending() has updated the DB row.
            //
            // We wait up to NEW_FILE_SETTLE_MS for the row to stabilise.
            // Crash-recovery entries have a last_fetch timestamp from a
            // previous session (age ≥ 2 s) and are exempt from the delay.
            const NEW_FILE_SETTLE_MS: u64 = 1_500;
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let age_secs = now_secs.saturating_sub(meta.last_fetch);
            if age_secs < 2 {
                let sleep_ms = NEW_FILE_SETTLE_MS.saturating_sub(age_secs.saturating_mul(1_000));
                debug!(
                    "upload-manager: settling new '{}' for {}ms (age={}s)",
                    meta.name, sleep_ms, age_secs
                );
                std::thread::sleep(Duration::from_millis(sleep_ms));
            }

            // Re-read the most up-to-date name and parent_id from SQLite right
            // before the API call.  The list_dirty_entries() snapshot may be
            // stale: Chrome's typical pattern is
            //   release("file.crdownload") → rename("file.crdownload", "file.pdf")
            // rename_pending() updates SQLite, but if the UploadManager woke
            // up between write_local_dirty() and rename_pending(), its snapshot
            // still shows "file.crdownload".  Uploading with the wrong name
            // causes Drive to create "file.crdownload" (instead of "file.pdf")
            // and replace_pending_id() then stores the wrong name back into the
            // in-memory caches, making the real PDF invisible after a refresh.
            let (upload_name, upload_parent) = if let Some(fresh) = self.db.get_metadata(remote_id) {
                (fresh.name, fresh.parent_id)
            } else {
                (meta.name.clone(), meta.parent_id.clone())
            };

            // Guard: parent folder is itself still pending (not yet uploaded).
            // Uploading now would send a fake "__pending__<N>" folder ID to
            // Drive and always fail.  Defer until the parent has been resolved.
            if upload_parent.starts_with("__pending__") {
                warn!(
                    "upload-manager: deferring '{}' — parent '{}' is still pending",
                    upload_name, upload_parent
                );
                return;
            }

            debug!(
                "upload-manager: creating '{}' in parent '{}'",
                upload_name, upload_parent
            );
            let size = content.len() as u64;
            match self.client.create_file(&upload_name, &upload_parent, content) {
                Ok(mut new_info) => {
                    if new_info.size == 0 {
                        new_info.size = size;
                    }
                    info!(
                        "upload-manager: created '{}' → id={}",
                        upload_name, new_info.id
                    );
                    // Replace the pending ID with the real Drive ID everywhere.
                    self.db.finalize_new_file(
                        remote_id,
                        &new_info.id,
                        new_info.md5_checksum.as_deref(),
                    );
                    // Migrate moka + disk from pending_id to MD5 CAS key.
                    // The DB side (small_files) is already handled by finalize_new_file.
                    if let Some(md5) = &new_info.md5_checksum {
                        if !md5.is_empty() {
                            self.obj.migrate_cache_key(remote_id, md5);
                        }
                    }
                    self.obj.replace_pending_id(remote_id, &upload_parent, new_info);
                }
                Err(e) => {
                    if is_http_not_found(&e) {
                        // The parent folder no longer exists on Drive (e.g. was
                        // deleted in a previous session before the upload could
                        // complete).  Retrying is pointless — abandon the entry
                        // so it doesn't loop forever on every restart.
                        error!(
                            "upload-manager: create_file '{}': parent '{}' not found on \
                             Drive (404) — abandoning unrecoverable dirty entry",
                            upload_name, upload_parent
                        );
                        self.db.abandon_dirty_entry(remote_id);
                        self.obj.remove_pending_from_dir(&upload_parent, remote_id);
                    } else {
                        error!(
                            "upload-manager: create_file '{}': {:#} — will retry",
                            upload_name, e
                        );
                        // dirty flag intentionally not cleared: retry on next cycle
                    }
                }
            }
        } else {
            // ── Existing file: update content on Drive ─────────────────────
            debug!("upload-manager: updating '{}'", remote_id);
            match self.client.update_file_content(remote_id, content) {
                Ok(updated) => {
                    info!("upload-manager: updated '{}'", remote_id);
                    self.db.clear_dirty_after_upload(
                        remote_id,
                        updated.md5_checksum.as_deref(),
                    );
                    // Migrate content from file_id key to MD5 CAS key now
                    // that the dirty flag has been cleared.
                    if let Some(md5) = &updated.md5_checksum {
                        if !md5.is_empty() {
                            self.db.migrate_small_file_key(remote_id, md5);
                            self.obj.migrate_cache_key(remote_id, md5);
                        }
                    }
                    self.obj.store_metadata(updated);
                }
                Err(e) => {
                    error!(
                        "upload-manager: update_file_content '{}': {:#} — will retry",
                        remote_id, e
                    );
                    // dirty flag intentionally not cleared: retry on next cycle
                }
            }
        }
    }
}
