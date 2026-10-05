use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use mirrarium_protocol::CaptureMetadata;
use rusqlite::{params, Connection};
use serde::Serialize;
use sha2::{Digest, Sha256};
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivacyClass {
    Public,
    Private,
    Unknown,
}

impl PrivacyClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Private => "private",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StoreStats {
    pub captures: u64,
    pub public_captures: u64,
    pub private_captures: u64,
    pub unknown_captures: u64,
    pub objects: u64,
    pub object_bytes: u64,
    pub public_objects: u64,
    pub private_objects: u64,
    pub unknown_objects: u64,
    pub captured_body_bytes: u64,
    pub body_errors: u64,
}

struct InFlightCapture {
    metadata: CaptureMetadata,
    temp_path: PathBuf,
    writer: BufWriter<File>,
    hasher: Sha256,
    bytes: u64,
    next_sequence: u32,
}

pub struct CaptureStore {
    root: PathBuf,
    connection: Connection,
    in_flight: HashMap<String, InFlightCapture>,
}

impl CaptureStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).with_context(|| format!("creating {}", root.display()))?;
        fs::create_dir_all(root.join(".incoming"))?;
        fs::create_dir_all(root.join("public/objects"))?;
        fs::create_dir_all(root.join("private/objects"))?;
        fs::create_dir_all(root.join("unknown/objects"))?;
        harden_directory(&root)?;
        harden_directory(&root.join(".incoming"))?;
        harden_directory(&root.join("private"))?;

        let database_path = root.join("ledger.sqlite3");
        let connection = Connection::open(&database_path)
            .with_context(|| format!("opening {}", database_path.display()))?;
        harden_file(&database_path)?;

        connection.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS objects (
                storage_class TEXT NOT NULL,
                hash TEXT NOT NULL,
                bytes INTEGER NOT NULL,
                relative_path TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                PRIMARY KEY (storage_class, hash)
            );

            CREATE TABLE IF NOT EXISTS captures (
                capture_id TEXT PRIMARY KEY,
                captured_at_ms INTEGER NOT NULL,
                tab_id INTEGER NOT NULL,
                request_id TEXT NOT NULL,
                method TEXT NOT NULL,
                url TEXT NOT NULL,
                status INTEGER NOT NULL,
                mime_type TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                privacy_class TEXT NOT NULL,
                body_hash TEXT,
                body_bytes INTEGER NOT NULL,
                encoded_data_length INTEGER,
                etag TEXT,
                last_modified TEXT,
                cache_control TEXT,
                body_error TEXT
            );

            CREATE INDEX IF NOT EXISTS captures_url_idx
                ON captures(url);
            CREATE INDEX IF NOT EXISTS captures_request_idx
                ON captures(tab_id, request_id);
            CREATE INDEX IF NOT EXISTS captures_class_idx
                ON captures(privacy_class);
            "#,
        )?;

        Ok(Self {
            root,
            connection,
            in_flight: HashMap::new(),
        })
    }

    pub fn begin(&mut self, metadata: CaptureMetadata) -> Result<()> {
        anyhow::ensure!(
            !self.in_flight.contains_key(&metadata.capture_id),
            "capture already in flight: {}",
            metadata.capture_id
        );

        let temp_name = format!("{}.part", sha256_hex(metadata.capture_id.as_bytes()));
        let temp_path = self.root.join(".incoming").join(temp_name);
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)
            .with_context(|| format!("opening {}", temp_path.display()))?;
        harden_file(&temp_path)?;

        self.in_flight.insert(
            metadata.capture_id.clone(),
            InFlightCapture {
                metadata,
                temp_path,
                writer: BufWriter::new(file),
                hasher: Sha256::new(),
                bytes: 0,
                next_sequence: 0,
            },
        );

        Ok(())
    }

    pub fn append_chunk(
        &mut self,
        capture_id: &str,
        sequence: u32,
        data_base64: &str,
    ) -> Result<()> {
        let capture = self
            .in_flight
            .get_mut(capture_id)
            .with_context(|| format!("unknown capture: {capture_id}"))?;

        anyhow::ensure!(
            sequence == capture.next_sequence,
            "out-of-order chunk for {capture_id}: expected {}, got {sequence}",
            capture.next_sequence
        );

        let bytes = BASE64
            .decode(data_base64)
            .with_context(|| format!("decoding chunk {sequence} for {capture_id}"))?;
        capture.writer.write_all(&bytes)?;
        capture.hasher.update(&bytes);
        capture.bytes = capture
            .bytes
            .checked_add(bytes.len() as u64)
            .context("capture byte count overflow")?;
        capture.next_sequence = capture
            .next_sequence
            .checked_add(1)
            .context("capture sequence overflow")?;

        Ok(())
    }

    pub fn finish(
        &mut self,
        capture_id: &str,
        encoded_data_length: Option<u64>,
        body_error: Option<&str>,
    ) -> Result<()> {
        let mut capture = self
            .in_flight
            .remove(capture_id)
            .with_context(|| format!("unknown capture: {capture_id}"))?;

        capture.writer.flush()?;
        drop(capture.writer);

        let privacy = classify(&capture.metadata);
        let captured_at_ms = now_ms()?;

        if let Some(error) = body_error {
            let _ = fs::remove_file(&capture.temp_path);
            self.insert_capture(
                &capture.metadata,
                privacy,
                None,
                capture.bytes,
                encoded_data_length,
                Some(error),
                captured_at_ms,
            )?;
            return Ok(());
        }

        let body_hash = format!("{:x}", capture.hasher.finalize());
        let relative_path = object_relative_path(privacy, &body_hash);
        let final_path = self.root.join(&relative_path);

        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent)?;
            if privacy == PrivacyClass::Private {
                harden_directory(parent)?;
            }
        }

        if final_path.exists() {
            fs::remove_file(&capture.temp_path)?;
        } else {
            fs::rename(&capture.temp_path, &final_path).with_context(|| {
                format!(
                    "moving {} to {}",
                    capture.temp_path.display(),
                    final_path.display()
                )
            })?;
            if privacy == PrivacyClass::Private {
                harden_file(&final_path)?;
            }
        }

        self.connection.execute(
            r#"
            INSERT OR IGNORE INTO objects
                (storage_class, hash, bytes, relative_path, created_at_ms)
            VALUES
                (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                privacy.as_str(),
                body_hash,
                capture.bytes as i64,
                relative_path.to_string_lossy(),
                captured_at_ms as i64,
            ],
        )?;

        self.insert_capture(
            &capture.metadata,
            privacy,
            Some(&body_hash),
            capture.bytes,
            encoded_data_length,
            None,
            captured_at_ms,
        )?;

        Ok(())
    }

    pub fn stats(&self) -> Result<StoreStats> {
        Ok(StoreStats {
            captures: scalar_u64(&self.connection, "SELECT COUNT(*) FROM captures")?,
            public_captures: scalar_u64(
                &self.connection,
                "SELECT COUNT(*) FROM captures WHERE privacy_class = 'public'",
            )?,
            private_captures: scalar_u64(
                &self.connection,
                "SELECT COUNT(*) FROM captures WHERE privacy_class = 'private'",
            )?,
            unknown_captures: scalar_u64(
                &self.connection,
                "SELECT COUNT(*) FROM captures WHERE privacy_class = 'unknown'",
            )?,
            objects: scalar_u64(&self.connection, "SELECT COUNT(*) FROM objects")?,
            object_bytes: scalar_u64(
                &self.connection,
                "SELECT COALESCE(SUM(bytes), 0) FROM objects",
            )?,
            public_objects: scalar_u64(
                &self.connection,
                "SELECT COUNT(*) FROM objects WHERE storage_class = 'public'",
            )?,
            private_objects: scalar_u64(
                &self.connection,
                "SELECT COUNT(*) FROM objects WHERE storage_class = 'private'",
            )?,
            unknown_objects: scalar_u64(
                &self.connection,
                "SELECT COUNT(*) FROM objects WHERE storage_class = 'unknown'",
            )?,
            captured_body_bytes: scalar_u64(
                &self.connection,
                "SELECT COALESCE(SUM(body_bytes), 0) FROM captures",
            )?,
            body_errors: scalar_u64(
                &self.connection,
                "SELECT COUNT(*) FROM captures WHERE body_error IS NOT NULL",
            )?,
        })
    }

    fn insert_capture(
        &self,
        metadata: &CaptureMetadata,
        privacy: PrivacyClass,
        body_hash: Option<&str>,
        body_bytes: u64,
        encoded_data_length: Option<u64>,
        body_error: Option<&str>,
        captured_at_ms: u64,
    ) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO captures (
                capture_id,
                captured_at_ms,
                tab_id,
                request_id,
                method,
                url,
                status,
                mime_type,
                resource_type,
                privacy_class,
                body_hash,
                body_bytes,
                encoded_data_length,
                etag,
                last_modified,
                cache_control,
                body_error
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9,
                ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17
            )
            "#,
            params![
                metadata.capture_id,
                captured_at_ms as i64,
                metadata.tab_id,
                metadata.request_id,
                metadata.method,
                metadata.url,
                metadata.status,
                metadata.mime_type,
                metadata.resource_type,
                privacy.as_str(),
                body_hash,
                body_bytes as i64,
                encoded_data_length.map(|value| value as i64),
                metadata.etag,
                metadata.last_modified,
                metadata.cache_control,
                body_error,
            ],
        )?;
        Ok(())
    }
}

pub fn classify(metadata: &CaptureMetadata) -> PrivacyClass {
    let Ok(url) = Url::parse(&metadata.url) else {
        return PrivacyClass::Unknown;
    };

    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    let path = url.path().to_ascii_lowercase();
    let resource_type = metadata.resource_type.to_ascii_lowercase();

    let static_type = matches!(
        resource_type.as_str(),
        "script" | "stylesheet" | "font"
    );
    let static_host = host == "cdn.oaistatic.com"
        || host.ends_with(".oaistatic.com")
        || host == "static.openai.com";
    let static_path = path.starts_with("/_next/static/");

    if static_path || (static_type && (static_host || is_chatgpt_host(&host))) {
        return PrivacyClass::Public;
    }

    if is_chatgpt_host(&host) {
        return PrivacyClass::Private;
    }

    PrivacyClass::Unknown
}

fn is_chatgpt_host(host: &str) -> bool {
    host == "chatgpt.com"
        || host.ends_with(".chatgpt.com")
        || host == "chat.openai.com"
}

fn object_relative_path(class: PrivacyClass, hash: &str) -> PathBuf {
    PathBuf::from(class.as_str())
        .join("objects")
        .join(&hash[..2])
        .join(hash)
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn now_ms() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis()
        .try_into()
        .context("timestamp overflow")?)
}

fn scalar_u64(connection: &Connection, sql: &str) -> Result<u64> {
    let value: i64 = connection.query_row(sql, [], |row| row.get(0))?;
    value.try_into().context("negative SQLite aggregate")
}

#[cfg(unix)]
fn harden_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("hardening directory {}", path.display()))
}

#[cfg(not(unix))]
fn harden_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn harden_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("hardening file {}", path.display()))
}

#[cfg(not(unix))]
fn harden_file(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrarium_protocol::CaptureMetadata;
    use tempfile::tempdir;

    fn metadata(capture_id: &str, url: &str, resource_type: &str) -> CaptureMetadata {
        CaptureMetadata {
            capture_id: capture_id.to_owned(),
            tab_id: 7,
            request_id: format!("request-{capture_id}"),
            method: "GET".to_owned(),
            url: url.to_owned(),
            status: 200,
            mime_type: "application/json".to_owned(),
            resource_type: resource_type.to_owned(),
            etag: None,
            last_modified: None,
            cache_control: None,
        }
    }

    #[test]
    fn deduplicates_only_within_a_privacy_class() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        let body = BASE64.encode(b"same bytes");

        for capture_id in ["one", "two"] {
            store
                .begin(metadata(
                    capture_id,
                    "https://chatgpt.com/backend-api/conversation/example",
                    "Fetch",
                ))
                .unwrap();
            store.append_chunk(capture_id, 0, &body).unwrap();
            store.finish(capture_id, Some(10), None).unwrap();
        }

        store
            .begin(metadata(
                "public-copy",
                "https://chatgpt.com/_next/static/example.js",
                "Script",
            ))
            .unwrap();
        store.append_chunk("public-copy", 0, &body).unwrap();
        store.finish("public-copy", Some(10), None).unwrap();

        let stats = store.stats().unwrap();
        assert_eq!(stats.captures, 3);
        assert_eq!(stats.private_captures, 2);
        assert_eq!(stats.public_captures, 1);
        assert_eq!(stats.private_objects, 1);
        assert_eq!(stats.public_objects, 1);
        assert_eq!(stats.objects, 2);
    }

    #[test]
    fn rejects_out_of_order_chunks() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        store
            .begin(metadata(
                "ordered",
                "https://chatgpt.com/backend-api/test",
                "Fetch",
            ))
            .unwrap();

        let error = store
            .append_chunk("ordered", 1, &BASE64.encode(b"oops"))
            .unwrap_err();
        assert!(error.to_string().contains("out-of-order"));
    }

    #[test]
    fn records_body_failures_without_creating_an_object() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        store
            .begin(metadata(
                "failed",
                "https://chatgpt.com/backend-api/test",
                "Fetch",
            ))
            .unwrap();
        store
            .finish("failed", None, Some("Network.getResponseBody failed"))
            .unwrap();

        let stats = store.stats().unwrap();
        assert_eq!(stats.captures, 1);
        assert_eq!(stats.objects, 0);
        assert_eq!(stats.body_errors, 1);
    }
}
