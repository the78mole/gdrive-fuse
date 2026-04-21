//! Persistent duplicate-name mapping for FUSE directory listings.
//!
//! Google Drive permits multiple files or folders with identical names inside
//! the same directory.  FUSE, however, requires every entry returned by
//! `readdir` to carry a **unique** name — otherwise the kernel returns EIO to
//! the caller.
//!
//! `DupMapping` solves this by:
//!
//! 1. Keeping a `file_id → unique_display_name` map in memory.
//! 2. Persisting the map to `~/.gdrive-fuse-rs/dup-mapping` (tab-separated).
//! 3. Assigning stable, deterministic names: the first occurrence of a base
//!    name keeps the original; subsequent duplicates receive a numeric suffix
//!    inserted **before** the extension:
//!
//!    ```text
//!    Bild.jpg          →  Bild.jpg
//!    Bild.jpg          →  Bild (1).jpg
//!    Bild.jpg          →  Bild (2).jpg
//!    Unbenanntes Dok.  →  Unbenanntes Dok.
//!    Unbenanntes Dok.  →  Unbenanntes Dok. (1)
//!    ```
//!
//! Because assignments are persisted, the same file always gets the same name
//! even after remounting — regardless of the order the Drive API returns files.

use crate::gclient::FileInfo;
use crate::object_manager::display_name;
use log::{debug, warn};
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

// ── DupMapping ─────────────────────────────────────────────────────────────

/// Stable, persistent mapping of Drive file-IDs to unique FUSE display names.
pub struct DupMapping {
    path: PathBuf,
    /// file_id → unique display name assigned to this file.
    inner: RwLock<HashMap<String, String>>,
}

impl DupMapping {
    /// Load from `path`.
    ///
    /// Returns an empty mapping (with a warning) if the file does not exist or
    /// cannot be parsed — loading never fails hard.
    pub fn load(path: PathBuf) -> Self {
        let map = if path.exists() {
            match Self::read_file(&path) {
                Ok(m) => {
                    debug!("dup-mapping: loaded {} entries from {:?}", m.len(), path);
                    m
                }
                Err(e) => {
                    warn!("dup-mapping: could not read {:?}: {} — starting fresh", path, e);
                    HashMap::new()
                }
            }
        } else {
            HashMap::new()
        };
        Self { path, inner: RwLock::new(map) }
    }

    /// Resolve unique display names for every entry in `files`.
    ///
    /// Files already recorded in the mapping keep their stored name.  Newly
    /// seen files are assigned a free name (suffixed when necessary) and the
    /// mapping is updated and saved to disk.
    ///
    /// The returned `Vec` preserves the order of `files`.
    pub fn resolve<'a>(&self, files: &'a [FileInfo]) -> Vec<(String, &'a FileInfo)> {
        let mut inner = self.inner.write().expect("dup_map lock poisoned");

        // Collect names that are already assigned to files in this listing so
        // we can avoid conflicts when choosing names for new entries.
        let mut taken: HashMap<String, ()> = inner
            .iter()
            .filter(|(id, _)| files.iter().any(|f| &f.id == *id))
            .map(|(_, name)| (name.clone(), ()))
            .collect();

        let mut result = Vec::with_capacity(files.len());
        let mut changed = false;

        for f in files {
            if let Some(name) = inner.get(&f.id) {
                result.push((name.clone(), f));
            } else {
                // New file — find the next free name.
                let base = display_name(&f.name, &f.mime_type);
                let unique = if !taken.contains_key(&base) {
                    base.clone()
                } else {
                    let mut n = 1u32;
                    loop {
                        let candidate = suffix_name(&base, n);
                        if !taken.contains_key(&candidate) {
                            break candidate;
                        }
                        n += 1;
                    }
                };
                taken.insert(unique.clone(), ());
                inner.insert(f.id.clone(), unique.clone());
                changed = true;
                result.push((unique, f));
            }
        }

        if changed {
            if let Err(e) = Self::write_file(&self.path, &inner) {
                warn!("dup-mapping: could not save to {:?}: {}", self.path, e);
            } else {
                debug!("dup-mapping: saved {} entries", inner.len());
            }
        }

        result
    }

    // ── I/O helpers ───────────────────────────────────────────────────────

    fn read_file(path: &Path) -> io::Result<HashMap<String, String>> {
        let file = fs::File::open(path)?;
        let mut map = HashMap::new();
        for line in io::BufReader::new(file).lines() {
            let line = line?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            match line.split_once('\t') {
                Some((id, name)) => {
                    map.insert(id.to_string(), name.to_string());
                }
                None => warn!("dup-mapping: malformed line: {:?}", line),
            }
        }
        Ok(map)
    }

    fn write_file(path: &Path, map: &HashMap<String, String>) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut f = fs::File::create(path)?;
        writeln!(f, "# gdrive-fuse duplicate name mapping")?;
        writeln!(f, "# file_id<TAB>unique_display_name")?;
        // Sort by file_id for reproducible diffs.
        let mut entries: Vec<_> = map.iter().collect();
        entries.sort_by_key(|(id, _)| id.as_str());
        for (id, name) in entries {
            writeln!(f, "{}\t{}", id, name)?;
        }
        Ok(())
    }
}

// ── Suffix helper ──────────────────────────────────────────────────────────

/// Insert ` (n)` before the last `.`-separated extension, or append when no
/// extension is present.
///
/// `n` is 1-based (first duplicate → 1).
pub(crate) fn suffix_name(name: &str, n: u32) -> String {
    match name.rfind('.') {
        Some(dot) => format!("{} ({}){}", &name[..dot], n, &name[dot..]),
        None => format!("{} ({})", name, n),
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn fi(id: &str, name: &str) -> FileInfo {
        FileInfo {
            id: id.to_string(),
            name: name.to_string(),
            mime_type: "application/octet-stream".to_string(),
            size: 0,
            modified_time: String::new(),
            is_folder: false,
        }
    }

    fn fi_folder(id: &str, name: &str) -> FileInfo {
        FileInfo {
            id: id.to_string(),
            name: name.to_string(),
            mime_type: "application/vnd.google-apps.folder".to_string(),
            size: 0,
            modified_time: String::new(),
            is_folder: true,
        }
    }

    fn tmp_map() -> DupMapping {
        let dir = tempfile::tempdir().expect("tmp dir");
        DupMapping::load(dir.into_path().join("dup-mapping"))
    }

    #[test]
    fn unique_names_unchanged() {
        let dm = tmp_map();
        let files = vec![fi("a", "foo.jpg"), fi("b", "bar.jpg")];
        let names: Vec<_> = dm.resolve(&files).into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, ["foo.jpg", "bar.jpg"]);
    }

    #[test]
    fn first_duplicate_keeps_base_name() {
        let dm = tmp_map();
        let files = vec![fi("a", "Bild.jpg"), fi("b", "Bild.jpg"), fi("c", "Bild.jpg")];
        let names: Vec<_> = dm.resolve(&files).into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, ["Bild.jpg", "Bild (1).jpg", "Bild (2).jpg"]);
    }

    #[test]
    fn extension_suffix_inserted_correctly() {
        let dm = tmp_map();
        let files = vec![fi("a", "doc"), fi("b", "doc")];
        let names: Vec<_> = dm.resolve(&files).into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, ["doc", "doc (1)"]);
    }

    #[test]
    fn folder_duplicate_suffix() {
        let dm = tmp_map();
        let files = vec![fi_folder("a", "Projekte"), fi_folder("b", "Projekte")];
        let names: Vec<_> = dm.resolve(&files).into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, ["Projekte", "Projekte (1)"]);
    }

    #[test]
    fn assignments_stable_across_calls() {
        let dm = tmp_map();
        let files = vec![fi("a", "Bild.jpg"), fi("b", "Bild.jpg")];
        let first: Vec<_> = dm.resolve(&files).into_iter().map(|(n, _)| n).collect();
        // Swap order — should still get the same names per file ID.
        let files_rev = vec![fi("b", "Bild.jpg"), fi("a", "Bild.jpg")];
        let second: Vec<_> = dm.resolve(&files_rev).into_iter().map(|(n, _)| n).collect();
        assert_eq!(first[0], "Bild.jpg");  // id "a"
        assert_eq!(first[1], "Bild (1).jpg"); // id "b"
        // After swap: b first, then a — but mappings are fixed by id
        assert_eq!(second[0], "Bild (1).jpg"); // id "b" still gets (1)
        assert_eq!(second[1], "Bild.jpg");     // id "a" still gets base
    }

    #[test]
    fn persist_and_reload() {
        let dir = tempfile::tempdir().expect("tmp dir");
        let path = dir.path().join("dup-mapping");

        let files = vec![fi("a", "x.txt"), fi("b", "x.txt")];
        {
            let dm1 = DupMapping::load(path.clone());
            let names: Vec<_> =
                dm1.resolve(&files).into_iter().map(|(n, _)| n).collect();
            assert_eq!(names, ["x.txt", "x (1).txt"]);
        }

        // Reload from disk — same results.
        let dm2 = DupMapping::load(path);
        let names: Vec<_> = dm2.resolve(&files).into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, ["x.txt", "x (1).txt"]);
    }
}
