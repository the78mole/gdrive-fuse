//! OAuth2 Authorization Code Flow for Google Drive.
//!
//! Stores and reloads tokens from `~/.gdrive_tokens_rs.json`.
//! Thread-safe: the access token is guarded by a `parking_lot::Mutex`.

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use log::{debug, info};
use parking_lot::Mutex;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";
const AUTH_ENDPOINT: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const REDIRECT_URI: &str = "http://localhost:8080/callback";
const SCOPES: &str = "https://www.googleapis.com/auth/drive";

/// Persisted token data (mirrors the C++ implementation's `.gdrive_tokens.json`).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TokenData {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: DateTime<Utc>,
}

/// OAuth2 client — mirrors `Auth` from the C++ implementation.
pub struct Auth {
    client_id: String,
    client_secret: String,
    token: Mutex<Option<TokenData>>,
    http: Client,
    token_path: PathBuf,
}

impl Auth {
    /// Construct a new `Auth` object and attempt to load saved tokens.
    pub fn new(client_id: String, client_secret: String) -> Result<Self> {
        let token_path = dirs::home_dir()
            .unwrap_or_default()
            .join(".gdrive_tokens_rs.json");

        let token = Self::load_tokens(&token_path).ok();

        Ok(Self {
            client_id,
            client_secret,
            token: Mutex::new(token),
            http: Client::new(),
            token_path,
        })
    }

    /// Return a valid access token, refreshing or re-authorising as needed.
    pub fn get_access_token(&self) -> Result<String> {
        let mut guard = self.token.lock();

        if let Some(ref t) = *guard {
            if Utc::now() < t.expires_at - chrono::Duration::seconds(60) {
                debug!("Using cached access token");
                return Ok(t.access_token.clone());
            }
            // Token expired — refresh
            debug!("Access token expired, refreshing");
            let refreshed = self.refresh_token(&t.refresh_token)?;
            *guard = Some(refreshed.clone());
            self.save_tokens(&refreshed)?;
            return Ok(refreshed.access_token);
        }

        // No token yet — full Authorization Code flow
        info!("No token found, starting OAuth2 authorization flow");
        let token_data = self.authorize()?;
        *guard = Some(token_data.clone());
        self.save_tokens(&token_data)?;
        Ok(token_data.access_token)
    }

    // ── private helpers ────────────────────────────────────────────────────

    fn authorize(&self) -> Result<TokenData> {
        // Build the authorization URL
        let auth_url = format!(
            "{}?client_id={}&redirect_uri={}&response_type=code&scope={}&access_type=offline&prompt=consent",
            AUTH_ENDPOINT,
            self.client_id,
            urlencoding::encode(REDIRECT_URI),
            urlencoding::encode(SCOPES),
        );

        // Try to open the browser automatically; fall back to manual copy-paste
        if std::process::Command::new("xdg-open")
            .arg(&auth_url)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .is_err()
        {
            info!("Could not open browser automatically.");
        }
        info!("Open this URL in your browser to authorize:");
        info!("{}", auth_url);

        // Minimal local HTTP server to capture the redirect
        let code = listen_for_code()?;

        self.exchange_code(&code)
    }

    fn exchange_code(&self, code: &str) -> Result<TokenData> {
        let resp = self
            .http
            .post(TOKEN_ENDPOINT)
            .form(&[
                ("code", code),
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
                ("redirect_uri", REDIRECT_URI),
                ("grant_type", "authorization_code"),
            ])
            .send()?
            .error_for_status()?
            .json::<serde_json::Value>()?;

        parse_token_response(&resp)
    }

    fn refresh_token(&self, refresh_token: &str) -> Result<TokenData> {
        let resp = self
            .http
            .post(TOKEN_ENDPOINT)
            .form(&[
                ("refresh_token", refresh_token),
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
                ("grant_type", "refresh_token"),
            ])
            .send()?
            .error_for_status()?
            .json::<serde_json::Value>()?;

        // Refresh responses may omit the refresh_token field; keep the old one.
        let access_token = resp["access_token"]
            .as_str()
            .ok_or_else(|| anyhow!("missing access_token in refresh response"))?
            .to_string();
        let expires_in = resp["expires_in"].as_i64().unwrap_or(3600);

        Ok(TokenData {
            access_token,
            refresh_token: refresh_token.to_string(),
            expires_at: Utc::now() + chrono::Duration::seconds(expires_in),
        })
    }

    fn load_tokens(path: &PathBuf) -> Result<TokenData> {
        let data = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&data)?)
    }

    fn save_tokens(&self, token: &TokenData) -> Result<()> {
        // Never log the token content
        fs::write(&self.token_path, serde_json::to_string_pretty(token)?)?;
        debug!("Tokens saved to {}", self.token_path.display());
        Ok(())
    }
}

// ── helpers ────────────────────────────────────────────────────────────────

fn listen_for_code() -> Result<String> {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;

    // Bind on both IPv4 and IPv6 loopback — localhost may resolve to either
    let (tx, rx) = mpsc::sync_channel::<std::net::TcpStream>(1);
    let mut bound = false;

    for addr in &["127.0.0.1:8080", "[::1]:8080"] {
        match TcpListener::bind(addr) {
            Ok(listener) => {
                bound = true;
                debug!("OAuth2 listener bound on {}", addr);
                let tx2 = tx.clone();
                std::thread::spawn(move || {
                    if let Ok((stream, _)) = listener.accept() {
                        let _ = tx2.send(stream);
                    }
                });
            }
            Err(e) => debug!("Could not bind {}: {}", addr, e),
        }
    }
    drop(tx); // close sender so channel errors if no listener bound

    if !bound {
        return Err(anyhow!("Could not bind to port 8080 on any loopback address"));
    }

    info!("Waiting for OAuth2 redirect on http://localhost:8080/callback …");

    let mut stream = rx
        .recv()
        .map_err(|_| anyhow!("No connection received on port 8080"))?;

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;

    // GET /callback?[iss=...&]code=<code>[&...] HTTP/1.1  — param order is not guaranteed
    let code = request_line
        .split_whitespace()
        .nth(1)
        .and_then(|path| path.split_once('?').map(|(_, qs)| qs))
        .and_then(|qs| {
            qs.split('&')
                .find_map(|kv| kv.strip_prefix("code=").map(str::to_string))
        })
        .ok_or_else(|| anyhow!("could not extract code from redirect URI: {}", request_line))?;

    let body = "<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
  <title>gdrive-fuse — Authorization</title>
  <style>
    body { font-family: sans-serif; display: flex; justify-content: center;
           align-items: center; height: 100vh; margin: 0;
           background: #f0f4f8; color: #1a202c; }
    .card { background: white; border-radius: 12px; padding: 2.5rem 3rem;
            box-shadow: 0 4px 24px rgba(0,0,0,.1); text-align: center;
            max-width: 420px; }
    .check { font-size: 3.5rem; color: #38a169; }
    h1 { font-size: 1.4rem; margin: .75rem 0 .4rem; }
    p  { font-size: .95rem; color: #4a5568; margin: 0; }
  </style>
</head>
<body>
  <div class=\"card\">
    <div class=\"check\">&#10003;</div>
    <h1>Authorization successful</h1>
    <p>You may close this tab &mdash; gdrive-fuse is now connected.</p>
  </div>
</body>
</html>";
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=UTF-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(), body
    );
    stream.write_all(response.as_bytes())?;

    Ok(code)
}

fn parse_token_response(resp: &serde_json::Value) -> Result<TokenData> {
    let access_token = resp["access_token"]
        .as_str()
        .ok_or_else(|| anyhow!("missing access_token"))?
        .to_string();
    let refresh_token = resp["refresh_token"]
        .as_str()
        .ok_or_else(|| anyhow!("missing refresh_token"))?
        .to_string();
    let expires_in = resp["expires_in"].as_i64().unwrap_or(3600);

    Ok(TokenData {
        access_token,
        refresh_token,
        expires_at: Utc::now() + chrono::Duration::seconds(expires_in),
    })
}
