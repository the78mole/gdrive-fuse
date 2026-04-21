//! Build script — derives the version string for the binary.
//!
//! Priority:
//!   1. `GDRIVE_FUSE_VERSION` environment variable (set by CI)
//!   2. `git describe --tags --dirty --abbrev=7` (local builds)
//!   3. `CARGO_PKG_VERSION` from Cargo.toml (final fallback)
//!
//! The resolved version overrides `CARGO_PKG_VERSION` so that
//! `clap`'s `#[command(version)]` and `--version` output the full
//! pre-release string (e.g. `1.2.3-pr15-3-gabcdef` or `1.2.3-loc-4-dirty`).

fn main() {
    // Re-run when git state or the env var changes.
    println!("cargo:rerun-if-env-changed=GDRIVE_FUSE_VERSION");
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs");

    let version = std::env::var("GDRIVE_FUSE_VERSION")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(git_version);

    // Override so every place that reads CARGO_PKG_VERSION (incl. clap) gets our string.
    println!("cargo:rustc-env=CARGO_PKG_VERSION={version}");
}

/// Derive a version string from `git describe`.
///
/// Transformations (after stripping leading `v`):
/// | `git describe` output      | result               |
/// |----------------------------|----------------------|
/// | `1.2.3`                    | `1.2.3`              |
/// | `1.2.3-dirty`              | `1.2.3-dirty`        |
/// | `1.2.3-42-gabcdef`         | `1.2.3-loc-42`       |
/// | `1.2.3-42-gabcdef-dirty`   | `1.2.3-loc-42-dirty` |
fn git_version() -> String {
    let fallback = || env!("CARGO_PKG_VERSION").to_string();

    let output = std::process::Command::new("git")
        .args(["describe", "--tags", "--dirty", "--abbrev=7"])
        .output();

    let desc = match output {
        Ok(o) if o.status.success() => {
            String::from_utf8(o.stdout).unwrap_or_default().trim().to_string()
        }
        _ => return fallback(),
    };

    let desc = desc.strip_prefix('v').unwrap_or(&desc);

    // Split on '-' up to 4 parts: base, count, sha, dirty-flag
    let parts: Vec<&str> = desc.splitn(4, '-').collect();
    match parts.as_slice() {
        [base] => base.to_string(),
        [base, "dirty"] => format!("{base}-dirty"),
        [base, count, _sha] => format!("{base}-loc-{count}"),
        [base, count, _sha, "dirty"] => format!("{base}-loc-{count}-dirty"),
        _ => desc.to_string(),
    }
}
