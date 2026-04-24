//! Google Drive REST API client — mirrors `GClient` from the C++ implementation.
//!
//! All methods are **blocking** (synchronous) so they can be called directly
//! from FUSE callbacks without any async runtime overhead.

use crate::auth::Auth;
use anyhow::Result;
use log::{debug, error, info, warn};
use reqwest::blocking::Client;
use serde::Deserialize;
use std::sync::Arc;

const API_BASE: &str = "https://www.googleapis.com/drive/v3";

/// Metadata for a file or folder — mirrors `GClient::FileInfo`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileInfo {
    pub id: String,
    pub name: String,
    pub mime_type: String,
    #[serde(default, deserialize_with = "de_size")]
    pub size: u64,
    #[serde(default)]
    #[allow(dead_code)]
    pub modified_time: String,
    #[serde(skip)]
    pub is_folder: bool,
}

/// Result of a directory listing including the ETag for conditional requests.
#[derive(Debug, Clone)]
pub struct DirListing {
    pub files: Vec<FileInfo>,
    pub etag: String,
}

/// Unified return type from a single Drive API call, used by the worker pool.
#[derive(Clone)]
pub enum ApiOutcome {
    DirListing(DirListing),
    /// First page of a directory listing.  `next_page_token` is `Some` when
    /// there are additional pages to fetch.
    DirListingFirstPage {
        files: Vec<FileInfo>,
        next_page_token: Option<String>,
        etag: String,
    },
    NotModified,
    FileMetadata(FileInfo),
    FileContent(Vec<u8>),
}

/// Google Drive API wrapper — thread-safe through `Arc<Auth>`.
pub struct GClient {
    auth: Arc<Auth>,
    http: Client,
    /// Base URL for the Drive API.  Overridable in tests to point at a mock
    /// server without changing production code.
    base_url: String,
    /// Test-only: when set, bypasses `Auth::get_access_token()` entirely.
    #[cfg(test)]
    test_token: Option<String>,
}

impl GClient {
    pub fn new(token: String, auth: Auth) -> Self {
        let _ = token; // token obtained on first request via auth
        Self {
            auth: Arc::new(auth),
            http: Client::new(),
            base_url: API_BASE.to_string(),
            #[cfg(test)]
            test_token: None,
        }
    }

    /// Create a sibling client that shares the same `Auth` (and therefore the
    /// same token-refresh lock) but owns an **independent** HTTP connection
    /// pool.
    ///
    /// Use this to give dedicated threads (upload workers, navigation workers)
    /// their own pool so that in-flight downloads never delay other requests
    /// waiting for a connection.
    pub fn fork(&self) -> Self {
        Self {
            auth: Arc::clone(&self.auth),
            http: Client::new(),
            base_url: self.base_url.clone(),
            #[cfg(test)]
            test_token: self.test_token.clone(),
        }
    }

    /// Test-only constructor: points at a local mock server and uses a fixed
    /// bearer token so `Auth::get_access_token()` is never called.
    #[cfg(test)]
    pub(crate) fn new_for_test(base_url: &str, fake_token: &str) -> Self {
        let auth =
            crate::auth::Auth::new("test-id".to_string(), "test-secret".to_string())
                .expect("Auth::new must not fail");
        Self {
            auth: Arc::new(auth),
            http: Client::new(),
            base_url: base_url.to_string(),
            test_token: Some(fake_token.to_string()),
        }
    }

    /// Return a bearer token, bypassing auth in test mode.
    fn get_token(&self) -> Result<String> {
        #[cfg(test)]
        if let Some(t) = &self.test_token {
            return Ok(t.clone());
        }
        self.auth.get_access_token()
    }

    /// List files in a directory. Mirrors `GClient::listFiles()`.
    ///
    /// Uses `pageSize=1000` and follows `nextPageToken` pages until exhausted
    /// so that directories with > 100 files are returned completely.
    pub fn list_files(&self, parent_id: &str) -> Result<DirListing> {
        let token = self.get_token()?;
        let query = format!("'{}' in parents and trashed = false", parent_id);
        let base_url = format!(
            "{}/files?q={}&fields=nextPageToken,files(id,name,mimeType,size,modifiedTime)&pageSize=1000",
            self.base_url,
            urlencoding::encode(&query)
        );

        let mut all_files: Vec<FileInfo> = Vec::new();
        let mut page_token: Option<String> = None;
        let mut etag = String::new();

        loop {
            let url = match &page_token {
                Some(pt) => format!("{}&pageToken={}", base_url, urlencoding::encode(pt)),
                None => base_url.clone(),
            };
            let resp = self
                .http
                .get(&url)
                .bearer_auth(&token)
                .send()?
                .error_for_status()?;

            if etag.is_empty() {
                etag = resp
                    .headers()
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
            }
            let body: serde_json::Value = resp.json()?;
            all_files.extend(parse_file_list(&body));
            match body["nextPageToken"].as_str() {
                Some(pt) => page_token = Some(pt.to_string()),
                None => break,
            }
        }

        debug!(
            "list_files('{}'): {} files (all pages), ETag={}",
            parent_id,
            all_files.len(),
            etag
        );
        Ok(DirListing { files: all_files, etag })
    }

    /// Fetch only the **first page** of a directory listing (10 entries).
    ///
    /// Uses a small `pageSize=10` so the first results appear in the GUI
    /// file explorer with minimal latency.  Returns
    /// `(first_page_files, next_page_token, etag)`.  When
    /// `next_page_token` is `Some`, the caller should continue with
    /// [`list_files_pages`] to retrieve the remaining files.  This allows
    /// FUSE `readdir` to return partial results immediately while background
    /// workers fetch the rest.
    pub fn list_files_first_page(
        &self,
        parent_id: &str,
    ) -> Result<(Vec<FileInfo>, Option<String>, String)> {
        let token = self.get_token()?;
        let query = format!("'{}' in parents and trashed = false", parent_id);
        let url = format!(
            "{}/files?q={}&fields=nextPageToken,files(id,name,mimeType,size,modifiedTime)&pageSize=10",
            self.base_url,
            urlencoding::encode(&query)
        );

        let resp = self
            .http
            .get(&url)
            .bearer_auth(&token)
            .send()?
            .error_for_status()?;

        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let body: serde_json::Value = resp.json()?;
        let files = parse_file_list(&body);
        let next = body["nextPageToken"].as_str().map(|s| s.to_string());

        debug!(
            "list_files_first_page('{}'): {} files, has_more={}",
            parent_id,
            files.len(),
            next.is_some()
        );
        Ok((files, next, etag))
    }

    /// Fetch all remaining pages of a directory listing starting from
    /// `page_token`.  Appends to `accumulator` and returns the final merged
    /// `DirListing`.
    pub fn list_files_pages(
        &self,
        parent_id: &str,
        mut page_token: String,
        mut accumulator: Vec<FileInfo>,
        etag: String,
    ) -> Result<DirListing> {
        let token = self.get_token()?;
        let query = format!("'{}' in parents and trashed = false", parent_id);
        let base_url = format!(
            "{}/files?q={}&fields=nextPageToken,files(id,name,mimeType,size,modifiedTime)&pageSize=1000",
            self.base_url,
            urlencoding::encode(&query)
        );

        loop {
            let url = format!("{}&pageToken={}", base_url, urlencoding::encode(&page_token));
            let resp = self
                .http
                .get(&url)
                .bearer_auth(&token)
                .send()?
                .error_for_status()?;
            let body: serde_json::Value = resp.json()?;
            accumulator.extend(parse_file_list(&body));
            match body["nextPageToken"].as_str() {
                Some(pt) => page_token = pt.to_string(),
                None => break,
            }
        }

        debug!(
            "list_files_pages('{}'): {} files total (all remaining pages)",
            parent_id,
            accumulator.len()
        );
        Ok(DirListing { files: accumulator, etag })
    }

    /// Conditional GET with If-None-Match. Returns `None` on 304.
    /// Mirrors `GClient::revalidateDir()`.
    ///
    /// On a 200 response follows `nextPageToken` pages (with `pageSize=1000`)
    /// so the refreshed listing is always complete.
    pub fn revalidate_dir(&self, parent_id: &str, etag: &str) -> Result<Option<DirListing>> {
        let token = self.get_token()?;
        let query = format!("'{}' in parents and trashed = false", parent_id);
        let base_url = format!(
            "{}/files?q={}&fields=nextPageToken,files(id,name,mimeType,size,modifiedTime)&pageSize=1000",
            self.base_url,
            urlencoding::encode(&query)
        );

        // First page — send If-None-Match to detect 304
        let first_resp = self
            .http
            .get(&base_url)
            .bearer_auth(&token)
            .header("If-None-Match", etag)
            .send()?;

        if first_resp.status().as_u16() == 304 {
            debug!("revalidate_dir('{}'): 304 Not Modified", parent_id);
            return Ok(None);
        }

        first_resp.error_for_status_ref()?;
        let new_etag = first_resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let body: serde_json::Value = first_resp.json()?;
        let mut all_files = parse_file_list(&body);
        let mut page_token = body["nextPageToken"].as_str().map(|s| s.to_string());

        // Follow remaining pages without If-None-Match
        while let Some(pt) = page_token {
            let url = format!("{}&pageToken={}", base_url, urlencoding::encode(&pt));
            let resp = self
                .http
                .get(&url)
                .bearer_auth(&token)
                .send()?
                .error_for_status()?;
            let body: serde_json::Value = resp.json()?;
            all_files.extend(parse_file_list(&body));
            page_token = body["nextPageToken"].as_str().map(|s| s.to_string());
        }

        debug!(
            "revalidate_dir('{}'): changed, {} files (all pages)",
            parent_id,
            all_files.len()
        );
        Ok(Some(DirListing { files: all_files, etag: new_etag }))
    }

    /// Fetch metadata for a single file. Mirrors `GClient::getFileMetadata()`.
    pub fn get_file_metadata(&self, file_id: &str) -> Result<FileInfo> {
        let token = self.get_token()?;
        let url = format!(
            "{}/files/{}?fields=id,name,mimeType,size,modifiedTime",
            self.base_url, file_id
        );

        let mut info: FileInfo = self
            .http
            .get(&url)
            .bearer_auth(&token)
            .send()?
            .error_for_status()?
            .json()?;

        info.is_folder = info.mime_type == "application/vnd.google-apps.folder";
        debug!("get_file_metadata('{}'): {:?}", file_id, info.name);
        Ok(info)
    }

    /// Download the raw bytes of a file. Mirrors `GClient::downloadFile()`.
    pub fn download_file(&self, file_id: &str) -> Result<Vec<u8>> {
        let token = self.get_token()?;
        let url = format!("{}/files/{}?alt=media", self.base_url, file_id);

        let t0 = std::time::Instant::now();
        let resp = self
            .http
            .get(&url)
            .bearer_auth(&token)
            .send()?
            .error_for_status()?;

        let bytes = resp.bytes()?.to_vec();
        debug!("download_file('{}'): {} bytes in {:.1}ms", file_id, bytes.len(), t0.elapsed().as_secs_f64() * 1000.0);
        Ok(bytes)
    }

    /// Download a byte range of a file using an HTTP `Range` request.
    ///
    /// Download a byte range of a file using an HTTP `Range` request.
    ///
    /// Used for files larger than `CACHE_MAX_FILE_BYTES` so that `fuse_ops::read`
    /// serves exactly the bytes requested by the kernel without fetching and
    /// caching the entire file.
    ///
    /// **Handles both 206 and 200 responses correctly:**
    /// * `206 Partial Content` — server honoured the Range header; the returned
    ///   bytes start at `offset`.  Returned as-is.
    /// * `200 OK` — server ignored the Range header and returned the full file
    ///   (can happen when a download redirect strips the Range header).  The
    /// correct window `[offset, offset+length)` is sliced out before returning
    ///   so callers always receive the bytes they asked for, regardless of which
    ///   status code was used.
    #[allow(dead_code)]
    pub fn download_file_range(
        &self,
        file_id: &str,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>> {
        let token = self.get_token()?;
        let url = format!("{}/files/{}?alt=media", self.base_url, file_id);
        // RFC 7233 byte-range: "bytes=<first>-<last>" — both endpoints inclusive.
        let last = offset.saturating_add(length as u64).saturating_sub(1);
        let range_header = format!("bytes={}-{}", offset, last);

        let t0 = std::time::Instant::now();
        let resp = self
            .http
            .get(&url)
            .bearer_auth(&token)
            .header("Range", &range_header)
            .send()?
            .error_for_status()?;

        let status = resp.status();
        let bytes = resp.bytes()?.to_vec();

        let result = if status == reqwest::StatusCode::PARTIAL_CONTENT {
            // 206: bytes start at `offset` exactly — return as-is.
            bytes
        } else {
            // 200 OK (or any other 2xx): server returned the full file.
            // Slice out the requested window so the caller gets the right bytes.
            warn!(
                "download_file_range('{}', {}..={}): server returned {} \
                 instead of 206; slicing [{}..]",
                file_id, offset, last, status, offset
            );
            let start = offset as usize;
            let end = start.saturating_add(length as usize).min(bytes.len());
            if start >= bytes.len() {
                vec![]
            } else {
                bytes[start..end].to_vec()
            }
        };

        info!(
            "download_file_range('{}', {}..={}): {} bytes (status={}) in {:.1}ms",
            file_id,
            offset,
            last,
            result.len(),
            status,
            t0.elapsed().as_secs_f64() * 1000.0
        );
        Ok(result)
    }

    // ── Write operations ──────────────────────────────────────────────────

    /// Create a new folder on Google Drive.
    pub fn create_folder(&self, name: &str, parent_id: &str) -> Result<FileInfo> {
        let token = self.get_token()?;
        let url = format!(
            "{}/files?fields=id,name,mimeType,size,modifiedTime",
            self.base_url
        );
        let metadata = serde_json::json!({
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        });
        let resp = self
            .http
            .post(&url)
            .bearer_auth(&token)
            .json(&metadata)
            .send()?
            .error_for_status()?;
        let mut info: FileInfo = resp.json()?;
        info.is_folder = true;
        debug!("create_folder '{}' in '{}' → id={}", name, parent_id, info.id);
        Ok(info)
    }

    /// Upload a new file to Google Drive using multipart upload.
    ///
    /// `content` is moved in and streamed directly into the multipart body
    /// via a `Cursor` chain — no extra heap copy.
    pub fn create_file(&self, name: &str, parent_id: &str, content: Vec<u8>) -> Result<FileInfo> {
        let token = self.get_token()?;
        // Derive the upload endpoint from base_url.
        let upload_base = self.base_url.replace("/drive/v3", "/upload/drive/v3");
        let url = format!(
            "{}/files?uploadType=multipart&fields=id,name,mimeType,size,modifiedTime",
            upload_base
        );
        let boundary = "gdrive_fuse_boundary_4a2f8b1c3e7d9";
        let metadata = serde_json::json!({ "name": name, "parents": [parent_id] });
        let size = content.len() as u64;

        // Build the multipart body as a single contiguous Vec<u8> so reqwest
        // can set a proper Content-Length header.  Chunked transfer encoding
        // (the fallback when body size is unknown) causes noticeably higher
        // latency on the Google Drive upload endpoint.
        let prefix = format!(
            "--{b}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n\
             {meta}\r\n--{b}\r\nContent-Type: application/octet-stream\r\n\r\n",
            b = boundary,
            meta = metadata,
        )
        .into_bytes();
        let suffix = format!("\r\n--{}--", boundary).into_bytes();
        let mut body = prefix;
        body.extend_from_slice(&content);
        body.extend_from_slice(&suffix);

        let resp = self
            .http
            .post(&url)
            .bearer_auth(&token)
            .header(
                "Content-Type",
                format!("multipart/related; boundary={}", boundary),
            )
            .body(body)
            .send()?
            .error_for_status()?;
        let mut info: FileInfo = resp.json()?;
        info.is_folder = false;
        info.size = size;
        debug!("create_file '{}' in '{}' → id={}", name, parent_id, info.id);
        Ok(info)
    }

    /// Update the content of an existing file on Google Drive (media upload).
    ///
    /// `content` is moved in and sent directly as the request body — no copy.
    pub fn update_file_content(&self, file_id: &str, content: Vec<u8>) -> Result<FileInfo> {
        let token = self.get_token()?;
        let upload_base = self.base_url.replace("/drive/v3", "/upload/drive/v3");
        let url = format!(
            "{}/files/{}?uploadType=media&fields=id,name,mimeType,size,modifiedTime",
            upload_base, file_id
        );
        let size = content.len() as u64;
        let resp = self
            .http
            .patch(&url)
            .bearer_auth(&token)
            .header("Content-Type", "application/octet-stream")
            .body(content)
            .send()?
            .error_for_status()?;
        let mut info: FileInfo = resp.json()?;
        info.is_folder = false;
        info.size = size;
        debug!("update_file_content '{}': {} bytes", file_id, size);
        Ok(info)
    }

    /// Permanently delete a file or folder from Google Drive.
    pub fn delete_file(&self, file_id: &str) -> Result<()> {
        let token = self.get_token()?;
        let url = format!("{}/files/{}", self.base_url, file_id);
        self.http
            .delete(&url)
            .bearer_auth(&token)
            .send()?
            .error_for_status()?;
        debug!("delete_file '{}'", file_id);
        Ok(())
    }

    /// Rename and/or move a file or folder.
    ///
    /// Supply `new_parent_id` and `old_parent_id` to move to a different
    /// parent directory; leave both as `None` to rename in place.
    pub fn rename_file(
        &self,
        file_id: &str,
        new_name: &str,
        new_parent_id: Option<&str>,
        old_parent_id: Option<&str>,
    ) -> Result<FileInfo> {
        let token = self.get_token()?;
        let mut url = format!(
            "{}/files/{}?fields=id,name,mimeType,size,modifiedTime",
            self.base_url, file_id
        );
        if let (Some(new_p), Some(old_p)) = (new_parent_id, old_parent_id) {
            url.push_str(&format!("&addParents={}&removeParents={}", new_p, old_p));
        }
        let body = serde_json::json!({ "name": new_name });
        let resp = self
            .http
            .patch(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()?
            .error_for_status()?;
        let mut info: FileInfo = resp.json()?;
        info.is_folder = info.mime_type == "application/vnd.google-apps.folder";
        debug!(
            "rename_file '{}' → '{}' (parent: {:?} → {:?})",
            file_id, new_name, old_parent_id, new_parent_id
        );
        Ok(info)
    }
}

// ── helpers ────────────────────────────────────────────────────────────────

fn parse_file_list(json: &serde_json::Value) -> Vec<FileInfo> {
    let Some(arr) = json["files"].as_array() else {
        error!("parse_file_list: 'files' field missing");
        return vec![];
    };

    arr.iter()
        .filter_map(|v| {
            let mut info: FileInfo = serde_json::from_value(v.clone()).ok()?;
            info.is_folder = info.mime_type == "application/vnd.google-apps.folder";
            Some(info)
        })
        .collect()
}

/// Custom deserializer: Drive API returns `size` as a JSON string.
fn de_size<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<u64, D::Error> {
    use serde::de::Error;
    let s = String::deserialize(d).unwrap_or_default();
    s.parse().map_err(|_| D::Error::custom(format!("invalid size: {}", s)))
}
