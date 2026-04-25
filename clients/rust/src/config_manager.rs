//! Centralized runtime configuration for gdrive-fuse-rs.
//!
//! Configuration is stored as a TOML file at
//! `~/.config/gdrive-fuse-rs/config.toml`.  If the file does not exist it is
//! created with the built-in defaults on first run so the user always has a
//! documented starting point.
//!
//! # Priority (highest → lowest)
//!
//! 1. CLI flags / environment variables (`--client-id`, `CLIENT_ID`, …)
//! 2. `config.toml`
//! 3. Compile-time built-in defaults
//!
//! The structs are all `#[derive(Deserialize, Serialize)]` so that TOML
//! round-tripping is lossless.  Unknown keys in the file are silently ignored
//! thanks to `#[serde(default)]` on every field.

use anyhow::{Context, Result};
use log::info;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

// ── Sub-sections ──────────────────────────────────────────────────────────

/// Settings that control the in-memory and on-disk content caches.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct CacheConfig {
    /// Files at or below this size (bytes) are kept in the **RAM** content
    /// cache.  Larger files go to the disk cache or are streamed on demand.
    pub ram_max_bytes: u64,

    /// Total byte capacity of the in-memory Moka content cache.
    pub moka_max_bytes: u64,

    /// Time-to-live for entries in the in-memory Moka cache (seconds).
    pub moka_ttl_secs: u64,

    /// Maximum combined byte size of the on-disk content cache
    /// (`~/.cache/gdrive-fuse-rs/content/`).  The background cleaner
    /// evicts the least-recently-used files when this limit is exceeded.
    pub disk_max_bytes: u64,

    /// Files larger than this threshold are served via HTTP Range requests
    /// on every `read()` call and are never written to the disk cache.
    pub stream_threshold_bytes: u64,

    /// How long FUSE considers directory attributes (TTL) valid before
    /// requesting a revalidation (seconds).
    pub dir_ttl_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ram_max_bytes: 4 * 1024,                   // 4 KiB
            moka_max_bytes: 256 * 1024 * 1024,         // 256 MiB
            moka_ttl_secs: 600,                        // 10 minutes
            disk_max_bytes: 10 * 1024 * 1024 * 1024,  // 10 GiB
            stream_threshold_bytes: 64 * 1024 * 1024, // 64 MiB
            dir_ttl_secs: 30,
        }
    }
}

/// Settings for the background Drive change-watcher.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct SyncConfig {
    /// How often to poll the Drive changes feed (seconds).
    pub interval_secs: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self { interval_secs: 30 }
    }
}

/// Logging settings.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct LogConfig {
    /// Log verbosity level.  Accepted values: `"error"`, `"warn"`, `"info"`,
    /// `"debug"`, `"trace"`.  Overridden by the `RUST_LOG` environment
    /// variable and the `--debug` CLI flag.
    pub level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
        }
    }
}

/// Optional OAuth2 credentials stored in the config file.
///
/// Credentials supplied here are used as a fallback when neither the
/// `--client-id` / `--client-secret` CLI flags nor the `CLIENT_ID` /
/// `CLIENT_SECRET` environment variables are set.  Compile-time embedded
/// credentials (via `option_env!`) take precedence over these.
///
/// **Security:** the config file must not be world-readable.  The application
/// does not enforce this but the token file (`token.json`) is always created
/// with mode `0600`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct OAuthConfig {
    /// Google OAuth2 client ID.
    pub client_id: Option<String>,
    /// Google OAuth2 client secret.
    pub client_secret: Option<String>,
}

// ── Top-level Config ──────────────────────────────────────────────────────

/// Complete runtime configuration for gdrive-fuse-rs.
///
/// Loaded from `~/.config/gdrive-fuse-rs/config.toml` via
/// [`ConfigManager::load_or_create`].
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub cache: CacheConfig,
    pub sync: SyncConfig,
    pub log: LogConfig,
    pub oauth: OAuthConfig,
}

impl Config {
    /// Convenience: `cache.dir_ttl_secs` as a [`Duration`].
    #[inline]
    pub fn dir_ttl(&self) -> Duration {
        Duration::from_secs(self.cache.dir_ttl_secs)
    }

    /// Convenience: `cache.moka_ttl_secs` as a [`Duration`].
    #[inline]
    pub fn moka_ttl(&self) -> Duration {
        Duration::from_secs(self.cache.moka_ttl_secs)
    }

    /// Convenience: `sync.interval_secs` as a [`Duration`].
    #[inline]
    pub fn sync_interval(&self) -> Duration {
        Duration::from_secs(self.sync.interval_secs)
    }
}

// ── ConfigManager ─────────────────────────────────────────────────────────

/// Handles locating, loading, and (first-time) creating the config file.
pub struct ConfigManager;

impl ConfigManager {
    /// Return the canonical config file path:
    /// `~/.config/gdrive-fuse-rs/config.toml`.
    pub fn config_path() -> PathBuf {
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("~/.config"))
            .join("gdrive-fuse-rs")
            .join("config.toml")
    }

    /// Load the config from disk.  If the file does not exist yet it is
    /// created with the built-in defaults and the path is logged.
    ///
    /// Unknown TOML keys are silently ignored so that a newer config file
    /// can be read by an older binary without errors.
    pub fn load_or_create() -> Result<Config> {
        let path = Self::config_path();

        if !path.exists() {
            Self::write_defaults(&path)?;
            info!("config: created default config at {}", path.display());
        }

        let raw = fs::read_to_string(&path)
            .with_context(|| format!("reading config file {}", path.display()))?;

        let cfg: Config = toml::from_str(&raw)
            .with_context(|| format!("parsing config file {}", path.display()))?;

        info!("config: loaded from {}", path.display());
        Ok(cfg)
    }

    // ── private helpers ──────────────────────────────────────────────────

    fn write_defaults(path: &PathBuf) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating config dir {}", parent.display()))?;
        }

        let defaults = Config::default();
        let content = Self::serialize_with_comments(&defaults)?;

        fs::write(path, content)
            .with_context(|| format!("writing default config to {}", path.display()))?;

        Ok(())
    }

    /// Serialize `Config` to TOML and prepend a human-readable header comment
    /// so users opening the file for the first time see explanations.
    fn serialize_with_comments(cfg: &Config) -> Result<String> {
        let body = toml::to_string_pretty(cfg)
            .context("serializing default config to TOML")?;

        let header = "\
# gdrive-fuse-rs configuration
# ──────────────────────────────────────────────────────────────────────────────
# This file is created automatically on first run.  Edit values as needed.
# All sizes are in bytes unless stated otherwise.
#
# Priority (highest → lowest):
#   CLI flags / environment variables  >  this file  >  compiled-in defaults
# ──────────────────────────────────────────────────────────────────────────────

";
        Ok(format!("{}{}", header, body))
    }
}
