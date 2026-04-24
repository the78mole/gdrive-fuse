//! Google Drive FUSE filesystem — Rust implementation.
//!
//! Entry point: parses CLI arguments, initialises OAuth2, builds the FUSE
//! operations object and hands control to the `fuser` library.

mod auth;
mod db_manager;
mod dup_mapping;
mod fuse_ops;
mod gclient;
mod object_manager;
mod queue_manager;
mod sync_manager;

use anyhow::{Context, Result};
use clap::Parser;
use log::{info, warn};
use std::path::PathBuf;
use std::sync::Arc;

// Credentials optionally embedded at compile time (set CLIENT_ID / CLIENT_SECRET
// as environment variables when running `cargo build`).
const BUILTIN_CLIENT_ID: Option<&str> = option_env!("CLIENT_ID");
const BUILTIN_CLIENT_SECRET: Option<&str> = option_env!("CLIENT_SECRET");

/// Mount Google Drive as a local filesystem (Rust client).
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// OAuth2 client ID — pass via --client-id, set CLIENT_ID at runtime,
    /// or embed at compile time by setting CLIENT_ID during `cargo build`.
    #[arg(long, env = "CLIENT_ID")]
    client_id: Option<String>,

    /// OAuth2 client secret — pass via --client-secret, set CLIENT_SECRET at runtime,
    /// or embed at compile time by setting CLIENT_SECRET during `cargo build`.
    #[arg(long, env = "CLIENT_SECRET")]
    client_secret: Option<String>,

    /// Mount point directory
    mountpoint: PathBuf,

    /// Enable debug logging (sets RUST_LOG=debug if unset)
    #[arg(long, short)]
    debug: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Initialise logging
    if args.debug && std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    info!("gdrive-fuse-rs starting, mountpoint: {}", args.mountpoint.display());

    // Resolve credentials: runtime arg/env > compiled-in default.
    let client_id = args
        .client_id
        .or_else(|| BUILTIN_CLIENT_ID.map(str::to_string))
        .context(
            "CLIENT_ID is required: pass --client-id, set CLIENT_ID env var, \
             or compile with CLIENT_ID set",
        )?;
    let client_secret = args
        .client_secret
        .or_else(|| BUILTIN_CLIENT_SECRET.map(str::to_string))
        .context(
            "CLIENT_SECRET is required: pass --client-secret, set CLIENT_SECRET env var, \
             or compile with CLIENT_SECRET set",
        )?;

    // Authenticate
    let auth = auth::Auth::new(client_id, client_secret)?;
    let token = auth.get_access_token()?;
    info!("OAuth2 token obtained");

    // Build API client
    let client = Arc::new(gclient::GClient::new(token, auth));

    // Persistent SQLite cache — non-fatal if unavailable.
    let db_path = dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from(".cache"))
        .join("gdrive-fuse-rs")
        .join("metadata.db");
    let content_dir = db_path.parent().unwrap().join("content");

    let maybe_db = db_manager::DbManager::new(&db_path)
        .inspect_err(|e| warn!("persistent cache unavailable: {:#} — running without SQLite", e))
        .ok();

    // Garbage-collect orphaned content files from a previous run.
    if let Some(db) = &maybe_db {
        db.run_gc(&content_dir);
    }

    // Build FUSE filesystem handler
    let obj = Arc::new(match maybe_db.clone() {
        Some(db) => object_manager::ObjectManager::new_with_db(db),
        None => object_manager::ObjectManager::new(),
    });
    let queue = queue_manager::QueueManager::new(Arc::clone(&obj), Arc::clone(&client));

    // Persistent duplicate-name mapping.
    let dup_map_path = std::env::var("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
        .join(".gdrive-fuse-rs/dup-mapping");
    info!("dup-mapping: {:?}", dup_map_path);
    let dup_map = Arc::new(dup_mapping::DupMapping::load(dup_map_path));

    // Keep a reference for the SyncManager before moving obj into GDriveFuse.
    let obj_for_sync = Arc::clone(&obj);
    let fs = fuse_ops::GDriveFuse::new(obj, queue, dup_map, Arc::clone(&client));

    // Start background change-watcher (only when SQLite is available).
    if let Some(db) = maybe_db {
        let sync = Arc::new(sync_manager::SyncManager::new(
            db,
            obj_for_sync,
            Arc::clone(&client),
        ));
        sync.start();
    }

    // Mount options — AllowOther requires `user_allow_other` in /etc/fuse.conf;
    // omit it if the file doesn't have it to avoid a hard startup failure.
    let allow_other = std::fs::read_to_string("/etc/fuse.conf")
        .map(|c| c.lines().any(|l| l.trim() == "user_allow_other"))
        .unwrap_or(false);

    let acl = if allow_other {
        fuser::SessionACL::All
    } else {
        info!(
            "'user_allow_other' not set in /etc/fuse.conf — mounting without \
             allow_other (unmount with: fusermount3 -u {})",
            args.mountpoint.display()
        );
        fuser::SessionACL::Owner
    };

    let mut mount_options = vec![
        fuser::MountOption::FSName("gdrive-fuse-rs".to_string()),
        fuser::MountOption::DefaultPermissions,
    ];
    if allow_other {
        mount_options.push(fuser::MountOption::AutoUnmount);
    }

    let mut config = fuser::Config::default();
    config.mount_options = mount_options;
    config.acl = acl;

    info!("Mounting at {}", args.mountpoint.display());
    fuser::mount2(fs, &args.mountpoint, &config)?;

    Ok(())
}
