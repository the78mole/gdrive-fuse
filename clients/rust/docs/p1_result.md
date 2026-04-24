# Phase 1 — Non-blocking FUSE Callbacks: Ergebnisbericht

Basierend auf der Analyse in [`p0_analysis.md`](p0_analysis.md).

---

## 1. FUSE-Threading-Modell — Reply-Dispatcher-Pool

### Problem

Der `fuser`-FUSE-Event-Loop läuft auf **einem einzigen Thread** (`fuse_main`). Jeder Callback
(`readdir`, `lookup`, `read`), der einen Cache-Miss hatte und auf die Google-Drive-API warten
musste, hat diesen Thread blockiert. Während eines `ls -R` auf einem großen Verzeichnisbaum
stapelten sich so Dutzende Kernel-Requests in der FUSE-Queue.

### Lösung

Ein unbounded `crossbeam_channel` verbindet den FUSE-Event-Loop-Thread mit einem Pool aus
**16 Worker-Threads** (`gdrive-reply-0` … `gdrive-reply-15`). Callbacks können im Cache-Hit-Fall
synchron antworten (kein Overhead); bei Cache-Miss wird das `reply`-Handle zusammen mit der
benötigten Closure in den Channel gelegt und der FUSE-Thread kehrt sofort zurück.

### Betroffene Module

| Datei | Änderung |
|---|---|
| `src/fuse_ops.rs` | Feld `reply_tx: Sender<Box<dyn FnOnce() + Send + 'static>>` in `GDriveFuse` |
| `src/fuse_ops.rs` | Pool-Initialisierung in `GDriveFuse::new()` |
| `src/fuse_ops.rs` | Callbacks `read()`, `readdir()`, `lookup()` nutzen `reply_tx` auf Cache-Miss-Pfad |

### Pool-Initialisierung (`GDriveFuse::new()`)

```rust
reply_tx: {
    const REPLY_THREADS: usize = 16;
    let (reply_tx, reply_rx) =
        crossbeam_channel::unbounded::<Box<dyn FnOnce() + Send + 'static>>();
    for i in 0..REPLY_THREADS {
        let rx = reply_rx.clone();
        std::thread::Builder::new()
            .name(format!("gdrive-reply-{}", i))
            .spawn(move || {
                for task in rx.iter() { task(); }
                debug!("reply-{}: channel closed, exiting", i);
            })
            .expect("failed to spawn reply dispatcher thread");
    }
    reply_tx
},
```

### Dispatch-Logik (Beispiel: `read()`)

```rust
// 1 + 2: RAM- und Disk-Cache-Hit → synchron, kein Overhead
if let Some(content) = self.obj.get_content(&file_id) { ... return; }
if let Some(slice) = self.obj.read_disk_slice(&file_id, offset, size) { ... return; }

// 3: Cache-Miss → FUSE-Thread gibt sofort frei
let obj = Arc::clone(&self.obj);
let queue = Arc::clone(&self.queue);
self.reply_tx.send(Box::new(move || {
    match queue.enqueue_and_wait(TaskKey::DownloadFile(file_id.clone()), Priority::FileDownload) {
        Err(e) => { reply.error(fuser::Errno::EIO); return; }
        Ok(_) => {}
    }
    // 4: Aus Cache bedienen, nachdem Worker den Download abgeschlossen hat
    if let Some(content) = obj.get_content(&file_id) { reply.data(&content[s..e]); }
    else if let Some(slice) = obj.read_disk_slice(&file_id, offset, size) { reply.data(&slice); }
    else { reply.error(fuser::Errno::EIO); }
})).unwrap_or_else(|_| error!("read: reply dispatcher channel closed"));
```

Das gleiche Muster wurde für `readdir()` und `lookup()` angewendet.

---

## 2. Kernel-Flags (`init()`-Callback)

- [x] `FUSE_ASYNC_READ` — Kernel darf mehrere `read`-Requests gleichzeitig schicken, ohne auf die vorherige Antwort zu warten.
- [x] `FUSE_PARALLEL_DIROPS` — Kernel darf `lookup`- und `readdir`-Calls für dasselbe Verzeichnis parallel senden.

```rust
fn init(&mut self, _req: &Request, config: &mut fuser::KernelConfig) -> std::io::Result<()> {
    let _ = config.add_capabilities(
        fuser::InitFlags::FUSE_ASYNC_READ | fuser::InitFlags::FUSE_PARALLEL_DIROPS,
    );
    Ok(())
}
```

> Beide Flags sind Voraussetzungen dafür, dass der Reply-Dispatcher-Pool tatsächlich parallel
> arbeitet. Ohne sie serialisiert der Kernel alle Requests unabhängig von der Implementierung.

### Problem bei der Implementierung

Die fuser-Dokumentation und ältere Beispiele referenzieren `fuser::consts::FUSE_CAP_ASYNC_READ`
bzw. `FUSE_CAP_PARALLEL_DIROPS` — diese Konstanten **existieren in fuser 0.17 nicht**.

| Erwarteter Pfad (Doku/Beispiele) | Tatsächlicher Pfad (fuser 0.17) |
|---|---|
| `fuser::consts::FUSE_CAP_ASYNC_READ` | `fuser::InitFlags::FUSE_ASYNC_READ` |
| `fuser::consts::FUSE_CAP_PARALLEL_DIROPS` | `fuser::InitFlags::FUSE_PARALLEL_DIROPS` |

Ebenso hat sich der Rückgabetyp von `init()` geändert:

| fuser ≤ 0.14 | fuser 0.17 |
|---|---|
| `Result<(), libc::c_int>` | `std::io::Result<()>` |

---

## 3. Memory-Management & Copy-on-Write mit `Arc<Vec<FileInfo>>`

### Motivation

Vor der Änderung enthielt `DirEntry.files` einen `Vec<FileInfo>`, der bei jedem `readdir`- und
`lookup`-Aufruf vollständig geklont wurde (O(n)). Bei Verzeichnissen mit hunderten Einträgen war
das ein messbarer Allokations-Overhead.

### Umstellung

```rust
// vorher
pub struct DirEntry {
    pub files: Vec<FileInfo>,
    ...
}

// nachher
pub struct DirEntry {
    pub files: Arc<Vec<FileInfo>>,
    ...
}
```

`get_cached_dir()`, `get_dir_files()` und `touch_dir()` geben jetzt `Option<Arc<Vec<FileInfo>>>`
zurück. Der Aufruf `.clone()` auf einem `Arc` ist O(1) (atomarer Referenzzähler-Increment).

### `Arc::make_mut` für sichere Mutationen (Copy-on-Write)

Drei Methoden in `object_manager.rs` mutieren die Verzeichnisliste in-place, werden aber während
einer laufenden Schreiboperation (Upload, Rename, Unlink) aufgerufen — nicht während eines Reads.

| Methode | Stelle | Operation |
|---|---|---|
| `inject_pending_into_dir` | `object_manager.rs:538` | `Arc::make_mut(&mut entry.files).push(info)` |
| `remove_pending_from_dir` | `object_manager.rs:555` | `Arc::make_mut(&mut entry.files).retain(…)` |
| `replace_pending_id` | `object_manager.rs:586` | `Arc::make_mut(&mut entry.files)[pos] = new_info` |

`Arc::make_mut` implementiert CoW: ist der `Arc` der einzige Besitzer, wird direkt mutiert; gibt
es weitere Referenzen (z.B. ein laufendes `readdir`), wird zuerst eine exklusive Kopie angelegt.

### Weitere Anpassungen

- `store_dir_partial()` und `store_dir_listing()` wrappen den `Vec` beim Speichern in `Arc::new(…)`.
- `queue_manager.rs`: `FetchDirPages` kopiert den Accumulator via `(*arc).clone()`, da
  `list_files_pages()` einen eigenen `Vec<FileInfo>` zum Anhängen benötigt.
- Test-Helper `stale_entry()` in `object_manager.rs` wrappte das `files`-Argument in `Arc::new(…)`.

---

## 4. Verbleibende Gaps (nicht implementiert)

Die folgenden Punkte aus `p0_analysis.md` sind noch **nicht** umgesetzt:

- [ ] **Streaming-Download** (`read` Byte-für-Byte, ohne vollständige Datei in RAM/Disk vorzuhalten)  
  Aufwändig: erfordert Umbau von `GClient::download_file` auf einen chunk-basierten Iterator
  oder einen `tokio`-Stream und Anpassung des Disk-Cache-Writers.

- [ ] **`moka`-Cache-Integration** (TTL-gesteuerte Eviction mit Größenlimit für RAM-Cache)  
  Aktuell wird der RAM-Cache (`content_cache` in `ObjectManager`) nie evicted; `moka` würde
  automatische Größen- und TTL-Grenzen liefern. Erfordert Umbau von `DashMap<String, Vec<u8>>`
  auf `moka::sync::Cache<String, Arc<Vec<u8>>>`.

- [ ] **`getattr` non-blocking** auf Metadaten-Miss-Pfad  
  Aktuell blockiert `getattr` bei einem Metadaten-Cache-Miss (`enqueue_and_wait`). Der Impact
  ist gering (Metadaten sind klein und schnell abgerufen), aber für Vollständigkeit könnte
  der gleiche Dispatcher-Pool-Ansatz angewendet werden.

- [ ] **`write`/`flush` non-blocking**  
  Upload-Pfade blockieren ebenfalls. Diese wurden bewusst ausgeklammert, da Upload-Serialisierung
  per Design gewünscht ist (kein konkurrentes Schreiben auf dieselbe Datei).

---

## 5. Testergebnis

```
test result: ok. 51 passed; 0 failed; 0 ignored; finished in 6.93s
```

Alle 51 Tests bestehen nach den Änderungen unverändert.
