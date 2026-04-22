//! Google Drive FUSE filesystem — Rust implementation.
//!
//! Entry point: parses CLI arguments, initialises OAuth2, builds the FUSE
//! operations object and hands control to the `fuser` library.

mod auth;
mod dup_mapping;
mod fuse_ops;
mod gclient;
mod object_manager;
mod queue_manager;

use anyhow::Result;
use clap::Parser;
use log::info;
use std::path::PathBuf;
use std::sync::Arc;

/// Mount Google Drive as a local filesystem (Rust client).
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// OAuth2 client ID (from credentials.json)
    #[arg(long, env = "CLIENT_ID")]
    client_id: String,

    /// OAuth2 client secret (from credentials.json)
    #[arg(long, env = "CLIENT_SECRET")]
    client_secret: String,

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

    // Authenticate
    let auth = auth::Auth::new(args.client_id.clone(), args.client_secret.clone())?;
    let token = auth.get_access_token()?;
    info!("OAuth2 token obtained");

    // Build API client
    let client = Arc::new(gclient::GClient::new(token, auth));

    // Build FUSE filesystem handler
    let obj = std::sync::Arc::new(object_manager::ObjectManager::new());
    let queue = queue_manager::QueueManager::new(Arc::clone(&obj), Arc::clone(&client));

    // Persistent duplicate-name mapping.
    let dup_map_path = std::env::var("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
        .join(".gdrive-fuse-rs/dup-mapping");
    info!("dup-mapping: {:?}", dup_map_path);
    let dup_map = Arc::new(dup_mapping::DupMapping::load(dup_map_path));

    let fs = fuse_ops::GDriveFuse::new(obj, queue, dup_map);

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
        fuser::MountOption::RO,
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
