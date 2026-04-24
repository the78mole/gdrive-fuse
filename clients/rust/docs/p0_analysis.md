# Thread-Performance-Analyse: gdrive-fuse Rust-Client

Analysiert gegen Commit `a9d14df` (Stand: April 2026).

---

## 1. Blockierungs-Check

### Strukturelles Problem: Single-threaded FUSE-Dispatch

```rust
// main.rs
fuser::mount2(fs, &args.mountpoint, &config)?;
```

`fuser::mount2` liest Requests sequenziell von `/dev/fuse`. Jede blockierende
FUSE-Callback-Methode hält den einzigen Event-Loop-Thread an — der Kernel kann
keine neuen Requests senden, bis die aktuelle Methode zurückkommt.

### Konkrete Blocking-Punkte

| Methode | Blocking-Call | Wirkung bei `ls -R` |
|---------|---------------|---------------------|
| `readdir` | `enqueue_and_wait(FetchDirFirstPage, P-nav)` | Gesamter FUSE-Thread blockiert, bis erste Seite ankommt |
| `lookup` | `get_dir` → ggf. `enqueue_and_wait(FetchDirFirstPage)` | Serialisiert hinter laufendem `readdir` |
| `getattr` | `enqueue_and_wait(GetMetadata, P1)` | Nur bei Cache-Miss; durch `store_dir_listing` meist abgedeckt |
| `read` | `enqueue_and_wait(DownloadFile, P2)` | Blockiert alle anderen Reads **und** alle anderen FUSE-Ops |
| `read` Step 2 | `File::open + seek + read_exact` | Synchrones I/O im FUSE-Callback-Thread (Disk-Cache-Hit) |

### Empfohlener Fix: Reply-Dispatcher-Pool

`ReplyData`, `ReplyAttr` u. a. sind `Send` — sie können in einen Background-Thread
verschoben werden. Der Kernel wartet auf die Reply, nicht auf den Return aus dem
Callback. Das erlaubt es dem FUSE-Thread, sofort den nächsten Request zu lesen.

```rust
// Neues Feld in GDriveFuse:
reply_tx: Sender<Box<dyn FnOnce() + Send + 'static>>,
```

```rust
fn read(&self, _req: &Request, ino: INodeNo, ..., reply: ReplyData) {
    let file_id = ...;

    // Schneller Pfad: Cache-Hit → direkt servieren (kein Context-Switch)
    if let Some(content) = self.obj.get_content(&file_id) {
        let s = (offset as usize).min(content.len());
        reply.data(&content[s..(s + size as usize).min(content.len())]);
        return;
    }
    if let Some(slice) = self.obj.read_disk_slice(&file_id, offset, size) {
        reply.data(&slice);
        return;
    }

    // Langsamer Pfad: Reply in Dispatcher-Pool auslagern — FUSE-Thread kehrt sofort zurück
    let obj = Arc::clone(&self.obj);
    let queue = Arc::clone(&self.queue);
    self.reply_tx.send(Box::new(move || {
        match queue.enqueue_and_wait(
            TaskKey::DownloadFile(file_id.clone()),
            Priority::FileDownload,
        ) {
            Ok(_) => {
                if let Some(c) = obj.get_content(&file_id) {
                    let s = (offset as usize).min(c.len());
                    reply.data(&c[s..(s + size as usize).min(c.len())]);
                } else if let Some(sl) = obj.read_disk_slice(&file_id, offset, size) {
                    reply.data(&sl);
                } else {
                    reply.error(fuser::Errno::EIO);
                }
            }
            Err(_) => reply.error(fuser::Errno::EIO),
        }
    })).ok();
    // Kein reply.* hier — der Dispatcher-Pool antwortet asynchron
}
```

Dasselbe Muster für `readdir` (bei Cache-Miss) und `getattr`. Ein Pool mit 16
Threads reicht für typische GUI-Last.

---

## 2. Async-Bridge

`reqwest` ist als **blocking client** eingebunden (`features = ["blocking"]`).
Eine Umstellung auf tokio-async würde bedeuten:

1. `reqwest::blocking` → `reqwest` (async)
2. Alle `GClient`-Methoden werden `async fn`
3. Jede Worker-Thread-Iteration: `runtime.block_on(...)` oder echtes `tokio::spawn`

**Empfehlung: Nicht umsteigen.** Der aktuelle Ansatz (synchrone Workers +
Condvar + Reply-Dispatcher) hat denselben Durchsatz wie eine tokio-Lösung ohne
deren Komplexität:

- `block_on` auf einem Sync-Thread blockiert den Thread genauso wie `Condvar::wait` — kein Gewinn
- Echter async würde 6 Worker-Threads in tokio-Tasks umwandeln → 6× weniger
  OS-Threads, mehr Context-Switching bei I/O-gebundenen HTTP-Calls → kein
  messbarer Unterschied bei GDrive-Latenz (~100–300 ms)

**Was sofort hilft**: `FUSE_CAP_ASYNC_READ` + `FUSE_CAP_PARALLEL_DIROPS` im
`init`-Callback setzen, damit der Kernel mehrere Requests parallel senden darf:

```rust
// fuse_ops.rs
fn init(
    &mut self,
    _req: &Request,
    config: &mut fuser::KernelConfig,
) -> Result<(), libc::c_int> {
    config
        .add_capabilities(
            fuser::consts::FUSE_CAP_ASYNC_READ | fuser::consts::FUSE_CAP_PARALLEL_DIROPS,
        )
        .map_err(|_| libc::EIO)
}
```

> **Voraussetzung:** Diese Flags entfalten ihre Wirkung nur, wenn die
> FUSE-Callbacks schnell genug zurückkehren — d. h. der Reply-Dispatcher-Pool
> aus Abschnitt 1 muss zuerst implementiert sein.

---

## 3. Lock-Contention

### Bereits gut: DashMap überall

`dir_cache`, `metadata`, `name_index`, `ino_to_id`, `id_to_ino` sind alle
`DashMap` (16-Shard RwLocks). Concurrent reads auf verschiedene Keys blockieren
einander nicht.

### Problem 1: `ContentCache` — einzelner `parking_lot::Mutex`

```rust
pub struct ContentCache {
    inner: Mutex<ContentCacheInner>,  // ← Ein Lock für alle Reads und Writes
    max_bytes: u64,
}
struct ContentCacheInner {
    map: HashMap<String, Vec<u8>>,
    order: VecDeque<String>,
    total_bytes: u64,
}
```

Jeder `get()` und `insert()` hält denselben Lock. Parallele Reads auf
verschiedene kleine Dateien serialisieren vollständig.

**Fix:** `moka` (concurrent LRU, MIT-Lizenz):

```toml
# Cargo.toml
moka = { version = "0.12", features = ["sync"] }
```

```rust
// object_manager.rs
use moka::sync::Cache;

pub struct ObjectManager {
    // ...
    content_cache: Cache<String, Arc<[u8]>>,  // Sharded, O(1) clone
    // ...
}

// Im new_with_disk_dir():
content_cache: Cache::builder()
    .max_capacity(CACHE_MAX_TOTAL_BYTES)
    .weigher(|_k, v: &Arc<[u8]>| v.len() as u32)
    .build(),
```

Der Gewinn ist bei der aktuellen `CACHE_RAM_MAX_BYTES`-Grenze von 4 KiB moderat.
Relevant wird er, wenn die Grenze erhöht wird (z. B. auf 1 MiB für Thumbnails).

### Problem 2: `Vec<FileInfo>` wird bei jedem Lesen geklont

```rust
// get_cached_dir — aufgerufen aus readdir + lookup + get_dir
pub fn get_cached_dir(&self, parent_id: &str) -> Option<Vec<FileInfo>> {
    let entry = self.dir_cache.get(parent_id)?;
    if ... {
        Some(entry.files.clone())  // ← klont den ganzen Vec bei jedem Aufruf
    }
}
```

Bei `ls -R` auf einem Ordner mit 500 Einträgen wird `get_dir` aus `readdir`
**und** für jedes `lookup` aufgerufen — das sind > 500 Klone desselben Vecs.

**Fix:** `Arc<Vec<FileInfo>>` in `DirEntry` — O(1)-Clone durch Pointer-Kopie:

```rust
pub struct DirEntry {
    pub files: Arc<Vec<FileInfo>>,  // war: Vec<FileInfo>
    // ...
}

pub fn get_cached_dir(&self, parent_id: &str) -> Option<Arc<Vec<FileInfo>>> {
    // ...
    Some(Arc::clone(&entry.files))
}
```

Anpassungen erforderlich in: `store_dir_listing`, `store_dir_partial`,
`append_dir_listing`, `get_dir_files`, `touch_dir` sowie den aufrufenden
Stellen in `fuse_ops.rs` (`readdir`, `lookup`, `get_dir`).

---

## 4. Batching / Request Coalescing

**Bereits vollständig implementiert** — das ist die Stärke des aktuellen Designs.

`tracking: DashMap<TaskKey, Vec<Arc<TaskCompletion>>>` in `QueueManager`
dedupliziert identische Requests transparent:

```
Thread A: enqueue_and_wait(DownloadFile("id123"))  → neu → Worker startet Download
Thread B: enqueue_and_wait(DownloadFile("id123"))  → Duplikat → hängt sich an TaskCompletion an
Thread C: enqueue_and_wait(DownloadFile("id123"))  → Duplikat → hängt sich an TaskCompletion an
Worker:   Download fertig → notifiziert A, B und C gleichzeitig
```

Alle FUSE-`read()`-Calls auf dieselbe Datei, während der Download läuft,
warten auf **denselben** Worker-Task ohne einen zweiten HTTP-Request auszulösen.

### Bekanntes Gap: RAM-Spitze bei Großdateien

Sehr große Dateien (z. B. 500 MB Video) werden vollständig in `Vec<u8>` im
Heap gehalten (`GClient::download_file`), bevor sie auf Disk geschrieben werden.
Das erzeugt eine temporäre RAM-Spitze von Dateigröße × 2 (Download-Buffer +
Disk-Write-Buffer).

**Fix:** Streaming-Download direkt in eine Temp-Datei via
`reqwest::blocking::Response::copy_to()`:

```rust
// gclient.rs — download_file_streaming (neuer Code-Pfad)
pub fn download_file_to_path(&self, file_id: &str, dest: &Path) -> Result<u64> {
    let tmp = dest.with_extension("tmp");
    let mut file = std::fs::File::create(&tmp)?;
    let mut resp = self.http.get(...).send()?;
    let bytes = resp.copy_to(&mut file)?;
    std::fs::rename(&tmp, dest)?;
    Ok(bytes)
}
```

`ObjectManager::store_content` müsste dann für große Dateien den
`download_file_to_path`-Pfad nutzen statt den Byte-Vec in Empfang zu nehmen.
Das erfordert, dass der Worker den Ziel-Pfad kennt — eine einfache Erweiterung
von `TaskKey::DownloadFile(String)` um einen optionalen `dest: PathBuf`.

---

## Priorisierung

| Maßnahme | Aufwand | Gewinn `ls -R` | Gewinn Dateitransfer |
|----------|---------|----------------|----------------------|
| Reply-Dispatcher-Pool (non-blocking callbacks) | mittel | **hoch** | **hoch** |
| `FUSE_CAP_ASYNC_READ` + `PARALLEL_DIROPS` | gering | mittel¹ | mittel¹ |
| `Arc<Vec<FileInfo>>` in `DirEntry` | gering | mittel | – |
| `ContentCache` → `moka` | gering | gering | gering |
| Streaming-Download für Großdateien | mittel | – | mittel (RAM) |

¹ Setzt Reply-Dispatcher-Pool voraus.

**Empfohlene Reihenfolge:**
1. Reply-Dispatcher-Pool — gibt alle anderen Optimierungen frei
2. `FUSE_CAP_ASYNC_READ` + `PARALLEL_DIROPS` in `init()`
3. `Arc<Vec<FileInfo>>` — minimaler Refactor, sofortiger Gewinn für tiefe Verzeichnisbäume
4. `moka` — für die Zukunft, wenn `CACHE_RAM_MAX_BYTES` erhöht wird
5. Streaming-Download — wenn RAM-Auslastung ein Problem wird
