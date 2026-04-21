//! Google Drive REST API client — mirrors `GClient` from the C++ implementation.
//!
//! All methods are **blocking** (synchronous) so they can be called directly
//! from FUSE callbacks without any async runtime overhead.

use crate::auth::Auth;
use anyhow::Result;
use log::{debug, error, info};
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
    /// Result of a Range request — bytes are returned directly to the caller
    /// and are NOT stored in the content cache.
    FileContentRange(Vec<u8>),
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

        let resp = self
            .http
            .get(&url)
            .bearer_auth(&token)
            .send()?
            .error_for_status()?;

        let bytes = resp.bytes()?.to_vec();
        debug!("download_file('{}'): {} bytes", file_id, bytes.len());
        Ok(bytes)
    }

    /// Download a byte range of a file using an HTTP `Range` request.
    ///
    /// Used for files larger than `SMALL_FILE_MAX_BYTES` so that `fuse_ops::read`
    /// serves exactly the bytes requested by the kernel without fetching and
    /// caching the entire file.  Returns the bytes actually received, which may
    /// be shorter than `length` at end-of-file (206 Partial Content) or equal
    /// to the full file when the server ignores the Range header (200 OK).
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

        let resp = self
            .http
            .get(&url)
            .bearer_auth(&token)
            .header("Range", &range_header)
            .send()?
            .error_for_status()?;

        let bytes = resp.bytes()?.to_vec();
        info!(
            "download_file_range('{}', {}..={}): {} bytes",
            file_id,
            offset,
            last,
            bytes.len()
        );
        Ok(bytes)
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
