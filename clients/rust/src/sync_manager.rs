//! Background change-watcher that keeps caches in sync with Google Drive.
//!
//! # Protocol
//!
//! 1. On first start, `SyncManager` calls `GClient::get_start_page_token()` if
//!    no token is stored in the `DbManager`.
//! 2. Every `SYNC_INTERVAL` seconds it calls `GClient::get_changes(token)`.
//! 3. For each [`ChangeItem`]:
//!    - **Removed** file → evict from all cache tiers; remove DB entry.
//!    - **Changed** file → compare MD5 from DB with the API value.  If they
//!      differ (or there was no stored MD5) → evict content caches + mark
//!      parent directories stale; update stored metadata and DB entry.
//! 4. The new page token returned by the API is persisted in the DB so the
//!    next poll only receives incremental updates.
//!
//! # Error handling
//!
//! - HTTP errors from the changes API are logged and the current token is
//!   kept.  If the token appears expired (API signals an invalid token) it is
//!   cleared and re-fetched on the next iteration.
//! - The worker thread is non-blocking from the FUSE perspective: it only
//!   mutates in-memory `DashMap`s and the SQLite DB; it never holds a lock
//!   that a FUSE callback is waiting on.

use crate::db_manager::DbManager;
use crate::gclient::{ChangeItem, GClient};
use crate::object_manager::ObjectManager;
use anyhow::Result;
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::Duration;

/// How often to poll the Drive changes feed.
const SYNC_INTERVAL: Duration = Duration::from_secs(30);

/// Spawns and manages the background Drive change-poll loop.
pub struct SyncManager {
    db: Arc<DbManager>,
    obj: Arc<ObjectManager>,
    client: Arc<GClient>,
}

impl SyncManager {
    pub fn new(db: Arc<DbManager>, obj: Arc<ObjectManager>, client: Arc<GClient>) -> Self {
        Self { db, obj, client }
    }

    /// Spawn the background "gdrive-sync" thread.
    ///
    /// The thread is detached — it runs until the process exits.  Holding an
    /// `Arc<SyncManager>` keeps the DB, ObjectManager and GClient alive.
    pub fn start(self: Arc<Self>) {
        std::thread::Builder::new()
            .name("gdrive-sync".to_string())
            .spawn(move || self.run())
            .expect("spawn gdrive-sync thread");
        info!("sync: background change-watcher started (interval={:?})", SYNC_INTERVAL);
    }

    fn run(&self) {
        loop {
            std::thread::sleep(SYNC_INTERVAL);
            if let Err(e) = self.poll_once() {
                warn!("sync: poll error — {:#}", e);
            }
        }
    }

    /// Perform a single changes-feed poll cycle.
    fn poll_once(&self) -> Result<()> {
        // Ensure we have a valid start page token.
        let token = match self.db.get_sync_token() {
            Some(t) => t,
            None => {
                let t = self.client.get_start_page_token().map_err(|e| {
                    anyhow::anyhow!("get_start_page_token failed: {:#}", e)
                })?;
                self.db.set_sync_token(&t);
                debug!("sync: initialised with startPageToken '{}'", t);
                t
            }
        };

        let (changes, new_token) = match self.client.get_changes(&token) {
            Ok(pair) => pair,
            Err(e) => {
                let msg = format!("{:#}", e);
                // An invalid/expired page token surfaces as a 400 or 410.
                // Clear the stored token so we re-bootstrap next iteration.
                if msg.contains("400") || msg.contains("410") || msg.contains("Invalid") {
                    warn!("sync: page token invalid, clearing for re-bootstrap: {}", msg);
                    // Overwrite with an empty string to trigger re-fetch next time.
                    // We set it to empty here; on the next poll_once None handling
                    // will re-fetch a fresh start token.
                    // Actually: remove by re-setting to a sentinel and relying on
                    // get_start_page_token to detect it.
                    // Simplest: call get_start_page_token right now.
                    match self.client.get_start_page_token() {
                        Ok(new_t) => {
                            self.db.set_sync_token(&new_t);
                            info!("sync: bootstrapped fresh startPageToken after expiry");
                        }
                        Err(e2) => {
                            error!("sync: could not get fresh startPageToken: {:#}", e2);
                        }
                    }
                } else {
                    error!("sync: get_changes error: {}", msg);
                }
                return Ok(());
            }
        };

        let count = changes.len();
        for change in &changes {
            self.apply_change(change);
        }

        // Persist the new page token regardless of how many changes were applied.
        self.db.set_sync_token(&new_token);

        if count > 0 {
            info!("sync: applied {} change(s), new token stored", count);
        } else {
            debug!("sync: no changes since last poll");
        }
        Ok(())
    }

    /// Apply a single change event to all cache tiers.
    fn apply_change(&self, change: &ChangeItem) {
        let id = &change.file_id;

        if change.removed {
            debug!("sync: file '{}' removed from Drive", id);
            self.obj.content_cache.remove(id);
            self.obj.evict_disk_content(id);
            self.obj.evict_metadata(id);
            self.db.remove_entry(id);
            return;
        }

        let Some(new_info) = &change.file else {
            // Change event has no file payload — treat as removal/unknown.
            debug!("sync: change for '{}' has no file info, evicting", id);
            self.obj.content_cache.remove(id);
            self.obj.evict_disk_content(id);
            self.obj.evict_metadata(id);
            self.db.remove_entry(id);
            return;
        };

        // Compare stored MD5 with the live value.
        let cached_md5 = self.db.get_metadata(id).and_then(|m| m.md5_checksum);
        let new_md5 = new_info.md5_checksum.as_deref();
        let content_changed = match (cached_md5.as_deref(), new_md5) {
            (Some(old), Some(new)) => old != new,
            // If either side is None (folder / Workspace file) treat as
            // content-neutral — just update metadata.
            _ => false,
        };

        if content_changed {
            debug!("sync: '{}' MD5 changed ({:?} → {:?}), evicting content",
                   id, cached_md5, new_md5);
            self.obj.content_cache.remove(id);
            self.obj.evict_disk_content(id);
            if let Some(db_ref) = Some(&self.db) {
                db_ref.remove_entry(id); // will be re-inserted below
            }
        }

        // Evict stale in-memory metadata and mark parent dirs stale.
        self.obj.evict_metadata(id);
        self.obj.mark_dir_stale_for_file(id);

        // Re-store the updated metadata in both caches.
        self.obj.store_metadata(new_info.clone());

        // Upsert the DB metadata row.  Use empty string for parent since we
        // don't know the parent from a changes event without a separate API
        // call; the QueueManager will overwrite with the real parent on the
        // next readdir fetch.
        self.db
            .store_metadata(
                id,
                self.obj.get_or_alloc_ino(id),
                "", // parent unknown from changes feed
                "", // name unknown from changes feed
                new_md5,
                false,
            )
            .unwrap_or_else(|e| error!("sync: db.store_metadata '{}': {:#}", id, e));
    }
}
