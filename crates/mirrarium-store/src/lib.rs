use std::{
    collections::{BTreeMap, HashMap},
    env,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use mirrarium_protocol::{CaptureMetadata, CaptureProvenance, RequestBodyMetadata};
use rusqlite::{params, types::Type, Connection};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use url::Url;

const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

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

    fn parse(value: &str) -> Option<Self> {
        match value {
            "public" => Some(Self::Public),
            "private" => Some(Self::Private),
            "unknown" => Some(Self::Unknown),
            _ => None,
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
    pub suppressed_bodies: u64,
    pub request_bodies: u64,
    pub request_body_bytes: u64,
    pub request_body_errors: u64,
    pub suppressed_request_bodies: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CaptureSummary {
    pub captured_at_ms: u64,
    pub method: String,
    pub url: String,
    pub status: i64,
    pub mime_type: String,
    pub resource_type: String,
    pub privacy_class: String,
    pub body_hash: Option<String>,
    pub body_bytes: u64,
    pub body_error: Option<String>,
    pub request_body_hash: Option<String>,
    pub request_body_bytes: u64,
    pub request_body_error: Option<String>,
    pub request_body_kind: Option<String>,
    pub request_body_content_type: Option<String>,
    pub request_body_has_post_data: Option<bool>,
    pub request_body_post_data_entry_count: Option<u32>,
    pub request_body_declared_content_length: Option<u64>,
    pub provenance: CaptureProvenance,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerifyReport {
    pub checked_objects: u64,
    pub corrupt_objects: u64,
    pub errors: Vec<String>,
}

struct RequestBodyCapture {
    metadata: RequestBodyMetadata,
    bytes: Vec<u8>,
    next_sequence: u32,
    finished: bool,
    error: Option<String>,
}

struct InFlightCapture {
    metadata: CaptureMetadata,
    temp_path: PathBuf,
    writer: BufWriter<File>,
    hasher: Sha256,
    bytes: u64,
    next_sequence: u32,
    suppressed_reason: Option<String>,
    request_body: Option<RequestBodyCapture>,
}

enum ResponseBodyDisposition {
    KeepRaw,
    Replace(Vec<u8>),
    Suppress(&'static str),
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
                body_error TEXT,
                provenance_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS captures_url_idx
                ON captures(url);
            CREATE INDEX IF NOT EXISTS captures_request_idx
                ON captures(tab_id, request_id);
            CREATE INDEX IF NOT EXISTS captures_class_idx
                ON captures(privacy_class);

            CREATE TABLE IF NOT EXISTS request_bodies (
                capture_id TEXT PRIMARY KEY
                    REFERENCES captures(capture_id) ON DELETE CASCADE,
                content_type TEXT,
                body_hash TEXT,
                body_bytes INTEGER NOT NULL,
                body_error TEXT,
                body_kind TEXT NOT NULL DEFAULT 'unknown',
                has_post_data INTEGER NOT NULL DEFAULT 0,
                post_data_entry_count INTEGER,
                declared_content_length INTEGER
            );

            CREATE TABLE IF NOT EXISTS cache_replay_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at_ms INTEGER NOT NULL,
                url TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                outcome TEXT NOT NULL,
                body_bytes INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS cache_replay_events_outcome_idx
                ON cache_replay_events(outcome);
            "#,
        )?;

        ensure_table_column(
            &connection,
            "captures",
            "provenance_json",
            "TEXT NOT NULL DEFAULT '{}'",
        )?;
        ensure_table_column(
            &connection,
            "request_bodies",
            "body_kind",
            "TEXT NOT NULL DEFAULT 'unknown'",
        )?;
        ensure_table_column(
            &connection,
            "request_bodies",
            "has_post_data",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_table_column(
            &connection,
            "request_bodies",
            "post_data_entry_count",
            "INTEGER",
        )?;
        ensure_table_column(
            &connection,
            "request_bodies",
            "declared_content_length",
            "INTEGER",
        )?;

        Ok(Self {
            root,
            connection,
            in_flight: HashMap::new(),
        })
    }

    pub fn begin(&mut self, mut metadata: CaptureMetadata) -> Result<()> {
        anyhow::ensure!(
            !self.in_flight.contains_key(&metadata.capture_id),
            "capture already in flight: {}",
            metadata.capture_id
        );

        let suppressed_reason = credential_endpoint_reason(&metadata.url).map(str::to_owned);
        metadata.url = sanitize_url_for_storage(&metadata.url);
        sanitize_provenance(&mut metadata.provenance);

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
                suppressed_reason,
                request_body: None,
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

        if capture.suppressed_reason.is_some() {
            capture.next_sequence = capture
                .next_sequence
                .checked_add(1)
                .context("capture sequence overflow")?;
            return Ok(());
        }

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

    pub fn begin_request_body(
        &mut self,
        capture_id: &str,
        metadata: RequestBodyMetadata,
    ) -> Result<()> {
        let capture = self
            .in_flight
            .get_mut(capture_id)
            .with_context(|| format!("unknown capture: {capture_id}"))?;

        anyhow::ensure!(
            capture.request_body.is_none(),
            "request body already started for {capture_id}"
        );

        let error = capture
            .suppressed_reason
            .as_ref()
            .map(|reason| format!("suppressed:{reason}"));

        capture.request_body = Some(RequestBodyCapture {
            metadata,
            bytes: Vec::new(),
            next_sequence: 0,
            finished: false,
            error,
        });
        Ok(())
    }

    pub fn append_request_body_chunk(
        &mut self,
        capture_id: &str,
        sequence: u32,
        data_base64: &str,
    ) -> Result<()> {
        let capture = self
            .in_flight
            .get_mut(capture_id)
            .with_context(|| format!("unknown capture: {capture_id}"))?;
        let request_body = capture
            .request_body
            .as_mut()
            .with_context(|| format!("request body not started for {capture_id}"))?;

        anyhow::ensure!(
            !request_body.finished,
            "request body already finished for {capture_id}"
        );
        anyhow::ensure!(
            sequence == request_body.next_sequence,
            "out-of-order request-body chunk for {capture_id}: expected {}, got {sequence}",
            request_body.next_sequence
        );

        request_body.next_sequence = request_body
            .next_sequence
            .checked_add(1)
            .context("request-body sequence overflow")?;

        if request_body.error.is_some() {
            return Ok(());
        }

        let bytes = BASE64
            .decode(data_base64)
            .with_context(|| format!("decoding request-body chunk {sequence} for {capture_id}"))?;

        if request_body.bytes.len().saturating_add(bytes.len()) > MAX_REQUEST_BODY_BYTES {
            request_body.bytes.clear();
            request_body.error = Some("suppressed:request_body_too_large".to_owned());
            return Ok(());
        }

        request_body.bytes.extend_from_slice(&bytes);
        Ok(())
    }

    pub fn finish_request_body(
        &mut self,
        capture_id: &str,
        body_error: Option<&str>,
    ) -> Result<()> {
        let capture = self
            .in_flight
            .get_mut(capture_id)
            .with_context(|| format!("unknown capture: {capture_id}"))?;
        let request_body = capture
            .request_body
            .as_mut()
            .with_context(|| format!("request body not started for {capture_id}"))?;

        anyhow::ensure!(
            !request_body.finished,
            "request body already finished for {capture_id}"
        );

        if let Some(error) = body_error {
            request_body.bytes.clear();
            request_body.error = Some(error.to_owned());
        } else if request_body.error.is_none() {
            match sanitize_request_body(
                request_body.metadata.content_type.as_deref(),
                &request_body.bytes,
            ) {
                Ok(bytes) => request_body.bytes = bytes,
                Err(reason) => {
                    request_body.bytes.clear();
                    request_body.error = Some(format!("suppressed:{reason}"));
                }
            }
        }

        request_body.finished = true;
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

        if let Some(reason) = capture.suppressed_reason.as_deref() {
            let marker = format!("suppressed:{reason}");
            let _ = fs::remove_file(&capture.temp_path);
            self.insert_capture(
                &capture.metadata,
                privacy,
                None,
                0,
                encoded_data_length,
                Some(&marker),
                captured_at_ms,
            )?;
            self.persist_request_body(
                &capture.metadata.capture_id,
                capture.request_body.take(),
                captured_at_ms,
            )?;
            return Ok(());
        }

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
            self.persist_request_body(
                &capture.metadata.capture_id,
                capture.request_body.take(),
                captured_at_ms,
            )?;
            return Ok(());
        }

        let body_hash = match sanitize_response_body(
            privacy,
            &capture.metadata.mime_type,
            &capture.temp_path,
        )? {
            ResponseBodyDisposition::KeepRaw => format!("{:x}", capture.hasher.finalize()),
            ResponseBodyDisposition::Replace(bytes) => {
                fs::write(&capture.temp_path, &bytes)
                    .with_context(|| format!("rewriting {}", capture.temp_path.display()))?;
                harden_file(&capture.temp_path)?;
                capture.bytes = bytes.len() as u64;
                sha256_hex(&bytes)
            }
            ResponseBodyDisposition::Suppress(reason) => {
                let marker = format!("suppressed:{reason}");
                let _ = fs::remove_file(&capture.temp_path);
                self.insert_capture(
                    &capture.metadata,
                    privacy,
                    None,
                    0,
                    encoded_data_length,
                    Some(&marker),
                    captured_at_ms,
                )?;
                self.persist_request_body(
                    &capture.metadata.capture_id,
                    capture.request_body.take(),
                    captured_at_ms,
                )?;
                return Ok(());
            }
        };
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
        self.persist_request_body(
            &capture.metadata.capture_id,
            capture.request_body.take(),
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
                "SELECT COUNT(*) FROM captures WHERE body_error IS NOT NULL AND body_error NOT LIKE 'suppressed:%'",
            )?,
            suppressed_bodies: scalar_u64(
                &self.connection,
                "SELECT COUNT(*) FROM captures WHERE body_error LIKE 'suppressed:%'",
            )?,
            request_bodies: scalar_u64(
                &self.connection,
                "SELECT COUNT(*) FROM request_bodies",
            )?,
            request_body_bytes: scalar_u64(
                &self.connection,
                "SELECT COALESCE(SUM(body_bytes), 0) FROM request_bodies",
            )?,
            request_body_errors: scalar_u64(
                &self.connection,
                "SELECT COUNT(*) FROM request_bodies WHERE body_error IS NOT NULL AND body_error NOT LIKE 'suppressed:%'",
            )?,
            suppressed_request_bodies: scalar_u64(
                &self.connection,
                "SELECT COUNT(*) FROM request_bodies WHERE body_error LIKE 'suppressed:%'",
            )?,
        })
    }

    pub fn record_cache_replay_outcome(
        &self,
        raw_url: &str,
        resource_type: &str,
        outcome: &str,
        body_bytes: u64,
    ) -> Result<()> {
        let url = Url::parse(raw_url).context("parsing replay telemetry URL")?;
        let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
        anyhow::ensure!(
            url.scheme() == "https"
                && (host == "chatgpt.com" || host == "chat.openai.com")
                && url.path().starts_with("/_next/static/")
                && url.query().is_none()
                && url.fragment().is_none(),
            "refusing replay telemetry for non-static ChatGPT URL"
        );
        anyhow::ensure!(
            matches!(
                resource_type.to_ascii_lowercase().as_str(),
                "script" | "stylesheet" | "image"
            ),
            "refusing replay telemetry for unsupported resource type"
        );
        anyhow::ensure!(
            matches!(
                outcome,
                "hit" | "miss" | "lookup_error" | "timeout" | "fulfill_error"
            ),
            "invalid replay telemetry outcome"
        );
        anyhow::ensure!(
            outcome == "hit" || body_bytes == 0,
            "non-hit replay telemetry must not report replayed bytes"
        );

        self.connection.execute(
            r#"
            INSERT INTO cache_replay_events (
                observed_at_ms,
                url,
                resource_type,
                outcome,
                body_bytes
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                now_ms()? as i64,
                sanitize_url_for_storage(raw_url),
                resource_type,
                outcome,
                body_bytes as i64,
            ],
        )?;
        Ok(())
    }

    pub fn recent_captures(&self, limit: u64) -> Result<Vec<CaptureSummary>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT
                captured_at_ms,
                method,
                url,
                status,
                mime_type,
                resource_type,
                privacy_class,
                captures.body_hash,
                captures.body_bytes,
                captures.body_error,
                request_bodies.body_hash,
                COALESCE(request_bodies.body_bytes, 0),
                request_bodies.body_error,
                request_bodies.body_kind,
                request_bodies.content_type,
                request_bodies.has_post_data,
                request_bodies.post_data_entry_count,
                request_bodies.declared_content_length,
                captures.provenance_json
            FROM captures
            LEFT JOIN request_bodies USING (capture_id)
            ORDER BY captured_at_ms DESC
            LIMIT ?1
            "#,
        )?;

        let rows = statement.query_map([limit as i64], |row| {
            Ok(CaptureSummary {
                captured_at_ms: row.get::<_, i64>(0)? as u64,
                method: row.get(1)?,
                url: row.get(2)?,
                status: row.get(3)?,
                mime_type: row.get(4)?,
                resource_type: row.get(5)?,
                privacy_class: row.get(6)?,
                body_hash: row.get(7)?,
                body_bytes: row.get::<_, i64>(8)? as u64,
                body_error: row.get(9)?,
                request_body_hash: row.get(10)?,
                request_body_bytes: row.get::<_, i64>(11)? as u64,
                request_body_error: row.get(12)?,
                request_body_kind: row.get(13)?,
                request_body_content_type: row.get(14)?,
                request_body_has_post_data: row
                    .get::<_, Option<i64>>(15)?
                    .map(|value| value != 0),
                request_body_post_data_entry_count: row
                    .get::<_, Option<i64>>(16)?
                    .and_then(|value| u32::try_from(value).ok()),
                request_body_declared_content_length: row
                    .get::<_, Option<i64>>(17)?
                    .and_then(|value| u64::try_from(value).ok()),
                provenance: serde_json::from_str(&row.get::<_, String>(18)?).map_err(
                    |error| rusqlite::Error::FromSqlConversionFailure(
                        18,
                        Type::Text,
                        Box::new(error),
                    ),
                )?,
            })
        })?;

        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("reading recent captures")
    }

    pub fn verify(&self) -> Result<VerifyReport> {
        let mut statement = self.connection.prepare(
            "SELECT storage_class, hash, bytes, relative_path FROM objects ORDER BY storage_class, hash",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;

        let mut report = VerifyReport {
            checked_objects: 0,
            corrupt_objects: 0,
            errors: Vec::new(),
        };

        for row in rows {
            let (storage_class, hash, expected_bytes, indexed_relative_path) = row?;
            report.checked_objects += 1;

            let Some(class) = PrivacyClass::parse(&storage_class) else {
                report.corrupt_objects += 1;
                report
                    .errors
                    .push(format!("{storage_class}/{hash}: invalid storage class"));
                continue;
            };

            if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                report.corrupt_objects += 1;
                report
                    .errors
                    .push(format!("{storage_class}/{hash}: invalid SHA-256 key"));
                continue;
            }

            if expected_bytes < 0 {
                report.corrupt_objects += 1;
                report.errors.push(format!(
                    "{storage_class}/{hash}: negative byte count {expected_bytes}"
                ));
                continue;
            }

            let expected_relative_path = object_relative_path(class, &hash);
            if Path::new(&indexed_relative_path) != expected_relative_path {
                report.corrupt_objects += 1;
                report.errors.push(format!(
                    "{storage_class}/{hash}: ledger path mismatch: {indexed_relative_path}"
                ));
                continue;
            }

            let path = self.root.join(&expected_relative_path);
            let file = match File::open(&path) {
                Ok(file) => file,
                Err(error) => {
                    report.corrupt_objects += 1;
                    report.errors.push(format!(
                        "{storage_class}/{hash}: cannot open {}: {error}",
                        path.display()
                    ));
                    continue;
                }
            };

            let mut reader = BufReader::new(file);
            let mut hasher = Sha256::new();
            let mut actual_bytes = 0_u64;
            let mut buffer = [0_u8; 64 * 1024];

            loop {
                let read = reader.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
                actual_bytes = actual_bytes
                    .checked_add(read as u64)
                    .context("verified byte count overflow")?;
            }

            let actual_hash = format!("{:x}", hasher.finalize());
            if actual_hash != hash || actual_bytes != expected_bytes as u64 {
                report.corrupt_objects += 1;
                report.errors.push(format!(
                    "{storage_class}/{hash}: expected {expected_bytes} bytes/{hash}, got {actual_bytes} bytes/{actual_hash}"
                ));
            }
        }

        Ok(report)
    }

    fn persist_request_body(
        &self,
        capture_id: &str,
        request_body: Option<RequestBodyCapture>,
        captured_at_ms: u64,
    ) -> Result<()> {
        let Some(mut request_body) = request_body else {
            return Ok(());
        };

        if !request_body.finished && request_body.error.is_none() {
            request_body.bytes.clear();
            request_body.error = Some("incomplete:request_body_not_finished".to_owned());
        }

        let mut body_hash: Option<String> = None;
        let mut body_bytes = 0_u64;

        if request_body.error.is_none() {
            body_bytes = request_body.bytes.len() as u64;
            let hash = sha256_hex(&request_body.bytes);
            let relative_path = object_relative_path(PrivacyClass::Private, &hash);
            let final_path = self.root.join(&relative_path);

            if let Some(parent) = final_path.parent() {
                fs::create_dir_all(parent)?;
                harden_directory(parent)?;
            }

            if !final_path.exists() {
                let temp_name = format!(
                    "{}.request.part",
                    sha256_hex(format!("{capture_id}:request-body").as_bytes())
                );
                let temp_path = self.root.join(".incoming").join(temp_name);
                let mut file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&temp_path)
                    .with_context(|| format!("creating {}", temp_path.display()))?;
                harden_file(&temp_path)?;
                file.write_all(&request_body.bytes)?;
                file.flush()?;
                drop(file);

                if final_path.exists() {
                    fs::remove_file(&temp_path)?;
                } else {
                    fs::rename(&temp_path, &final_path).with_context(|| {
                        format!(
                            "moving {} to {}",
                            temp_path.display(),
                            final_path.display()
                        )
                    })?;
                    harden_file(&final_path)?;
                }
            }

            self.connection.execute(
                r#"
                INSERT OR IGNORE INTO objects
                    (storage_class, hash, bytes, relative_path, created_at_ms)
                VALUES
                    ('private', ?1, ?2, ?3, ?4)
                "#,
                params![
                    hash,
                    body_bytes as i64,
                    relative_path.to_string_lossy(),
                    captured_at_ms as i64,
                ],
            )?;

            body_hash = Some(hash);
        }

        let body_kind = request_body_kind(request_body.metadata.content_type.as_deref());
        self.connection.execute(
            r#"
            INSERT INTO request_bodies (
                capture_id,
                content_type,
                body_hash,
                body_bytes,
                body_error,
                body_kind,
                has_post_data,
                post_data_entry_count,
                declared_content_length
            )
            VALUES
                (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            params![
                capture_id,
                request_body.metadata.content_type,
                body_hash,
                body_bytes as i64,
                request_body.error,
                body_kind,
                if request_body.metadata.has_post_data { 1_i64 } else { 0_i64 },
                request_body.metadata.post_data_entry_count.map(i64::from),
                request_body
                    .metadata
                    .declared_content_length
                    .and_then(|value| i64::try_from(value).ok()),
            ],
        )?;

        Ok(())
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
                body_error,
                provenance_json
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9,
                ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18
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
                serde_json::to_string(&metadata.provenance)?,
            ],
        )?;
        Ok(())
    }
}

fn ensure_table_column(
    connection: &Connection,
    table_name: &str,
    column_name: &str,
    definition: &str,
) -> Result<()> {
    for identifier in [table_name, column_name] {
        anyhow::ensure!(
            identifier
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "invalid SQLite identifier"
        );
    }

    let mut statement = connection.prepare(&format!("PRAGMA table_info({table_name})"))?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;

    for column in columns {
        if column? == column_name {
            return Ok(());
        }
    }

    connection.execute(
        &format!("ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}"),
        [],
    )?;
    Ok(())
}

fn sanitize_provenance(provenance: &mut CaptureProvenance) {
    if let Some(document_url) = provenance.document_url.as_mut() {
        *document_url = sanitize_url_for_storage(document_url);
    }
    if let Some(redirected_from_url) = provenance.redirected_from_url.as_mut() {
        *redirected_from_url = sanitize_url_for_storage(redirected_from_url);
    }
    provenance.request_headers =
        sanitize_headers(std::mem::take(&mut provenance.request_headers));
    provenance.response_headers =
        sanitize_headers(std::mem::take(&mut provenance.response_headers));
}

fn sanitize_headers(headers: BTreeMap<String, String>) -> BTreeMap<String, String> {
    headers
        .into_iter()
        .map(|(name, value)| {
            let normalized = name.trim().to_ascii_lowercase().replace('-', "_");
            let value = if is_sensitive_header_name(&normalized) {
                "[REDACTED]".to_owned()
            } else if matches!(
                normalized.as_str(),
                "referer" | "referrer" | "location" | "content_location"
            ) {
                sanitize_url_for_storage(&value)
            } else {
                value
            };
            (name, value)
        })
        .collect()
}

fn is_sensitive_header_name(normalized: &str) -> bool {
    matches!(
        normalized,
        "authorization"
            | "proxy_authorization"
            | "cookie"
            | "set_cookie"
            | "authentication_info"
            | "proxy_authenticate"
            | "www_authenticate"
            | "x_csrf_token"
            | "x_xsrf_token"
            | "x_auth_token"
            | "x_api_key"
    ) || normalized.ends_with("_token")
        || normalized.contains("credential")
}

pub fn default_data_root() -> Result<PathBuf> {
    if let Some(path) = env::var_os("MIRRARIUM_DATA_DIR") {
        return Ok(PathBuf::from(path));
    }

    if let Some(path) = env::var_os("XDG_DATA_HOME") {
        return Ok(PathBuf::from(path).join("mirrarium"));
    }

    if let Some(home) = env::var_os("HOME") {
        return Ok(PathBuf::from(home).join(".local/share/mirrarium"));
    }

    anyhow::bail!("set MIRRARIUM_DATA_DIR, XDG_DATA_HOME, or HOME")
}

fn request_body_kind(content_type: Option<&str>) -> &'static str {
    let content_type = content_type.unwrap_or("").to_ascii_lowercase();
    if content_type.contains("multipart/form-data") {
        "multipart"
    } else if content_type.contains("json") {
        "json"
    } else if content_type.contains("application/x-www-form-urlencoded") {
        "form"
    } else if content_type.is_empty() {
        "unknown"
    } else {
        "opaque"
    }
}

fn sanitize_response_body(
    privacy: PrivacyClass,
    mime_type: &str,
    path: &Path,
) -> Result<ResponseBodyDisposition> {
    if privacy == PrivacyClass::Public {
        return Ok(ResponseBodyDisposition::KeepRaw);
    }

    let mime_type = mime_type.to_ascii_lowercase();
    if !mime_type.contains("json") && !mime_type.contains("text/event-stream") {
        return Ok(ResponseBodyDisposition::KeepRaw);
    }

    let bytes = fs::read(path)
        .with_context(|| format!("reading structured response body {}", path.display()))?;

    if mime_type.contains("json") {
        let mut value = match serde_json::from_slice::<Value>(&bytes) {
            Ok(value) => value,
            Err(_) => {
                return Ok(ResponseBodyDisposition::Suppress(
                    "unparseable_json_response_body",
                ))
            }
        };

        if redact_json_secrets(&mut value) {
            let sanitized = serde_json::to_vec(&value)
                .context("serializing sanitized JSON response body")?;
            return Ok(ResponseBodyDisposition::Replace(sanitized));
        }

        return Ok(ResponseBodyDisposition::KeepRaw);
    }

    sanitize_sse_response_body(&bytes)
}

fn sanitize_sse_response_body(bytes: &[u8]) -> Result<ResponseBodyDisposition> {
    let text = match std::str::from_utf8(bytes) {
        Ok(text) => text,
        Err(_) => {
            return Ok(ResponseBodyDisposition::Suppress(
                "non_utf8_event_stream_response_body",
            ))
        }
    };

    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    let mut changed = false;
    let mut sanitized_blocks = Vec::new();

    for block in normalized.split("\n\n") {
        if block.is_empty() {
            sanitized_blocks.push(String::new());
            continue;
        }

        let lines: Vec<&str> = block.split('\n').collect();
        let data_lines: Vec<&str> = lines
            .iter()
            .filter_map(|line| {
                let (field, value) = line.split_once(':')?;
                (field == "data").then_some(value.strip_prefix(' ').unwrap_or(value))
            })
            .collect();

        if data_lines.is_empty() {
            if contains_credential_marker(block) {
                return Ok(ResponseBodyDisposition::Suppress(
                    "credential_marker_in_event_stream",
                ));
            }
            sanitized_blocks.push(block.to_owned());
            continue;
        }

        let data = data_lines.join("\n");
        if data.trim() == "[DONE]" {
            sanitized_blocks.push(block.to_owned());
            continue;
        }

        match serde_json::from_str::<Value>(&data) {
            Ok(mut value) => {
                if redact_json_secrets(&mut value) {
                    changed = true;
                    let sanitized_data = serde_json::to_string(&value)
                        .context("serializing sanitized SSE JSON event")?;
                    let mut block_lines = Vec::new();
                    let mut inserted_data = false;

                    for line in lines {
                        let is_data = line
                            .split_once(':')
                            .is_some_and(|(field, _)| field == "data");
                        if is_data {
                            if !inserted_data {
                                block_lines.push(format!("data: {sanitized_data}"));
                                inserted_data = true;
                            }
                        } else {
                            block_lines.push(line.to_owned());
                        }
                    }

                    sanitized_blocks.push(block_lines.join("\n"));
                } else {
                    sanitized_blocks.push(block.to_owned());
                }
            }
            Err(_) => {
                if contains_credential_marker(&data) {
                    return Ok(ResponseBodyDisposition::Suppress(
                        "unparseable_credential_event_stream",
                    ));
                }
                sanitized_blocks.push(block.to_owned());
            }
        }
    }

    if !changed {
        return Ok(ResponseBodyDisposition::KeepRaw);
    }

    let mut sanitized = sanitized_blocks.join("\n\n");
    if text.ends_with("\n\n") && !sanitized.ends_with("\n\n") {
        sanitized.push_str("\n\n");
    }

    Ok(ResponseBodyDisposition::Replace(sanitized.into_bytes()))
}

fn contains_credential_marker(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase();
    [
        "access_token=",
        "refresh_token=",
        "id_token=",
        "session_token=",
        "authorization=",
        "x-amz-signature=",
        "x-amz-credential=",
        "x-amz-security-token=",
        "signature=",
        "bearer ",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
}

fn sanitize_structured_string(value: &str) -> Option<String> {
    let looks_url_like = value.starts_with("http://")
        || value.starts_with("https://")
        || value.starts_with("//")
        || value.starts_with('/')
        || value.starts_with('?');

    if !looks_url_like {
        return None;
    }

    let sanitized = sanitize_url_for_storage(value);
    (sanitized != value).then_some(sanitized)
}

fn sanitize_request_body(
    content_type: Option<&str>,
    bytes: &[u8],
) -> std::result::Result<Vec<u8>, &'static str> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let content_type = content_type.unwrap_or("").to_ascii_lowercase();

    if content_type.contains("multipart/form-data") {
        return Err("multipart_request_body_not_archived");
    }

    let looks_json = content_type.contains("json")
        || bytes
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            .is_some_and(|byte| byte == b'{' || byte == b'[');

    if looks_json {
        let mut value: Value =
            serde_json::from_slice(bytes).map_err(|_| "unparseable_json_request_body")?;
        if redact_json_secrets(&mut value) {
            return serde_json::to_vec(&value).map_err(|_| "json_request_body_reserialize_failed");
        }
        return Ok(bytes.to_vec());
    }

    if content_type.contains("application/x-www-form-urlencoded") {
        let pairs: Vec<(String, String)> = url::form_urlencoded::parse(bytes)
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        let mut changed = false;
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());

        for (key, value) in pairs {
            if is_sensitive_body_key(&key) {
                serializer.append_pair(&key, "[REDACTED]");
                changed = true;
            } else {
                serializer.append_pair(&key, &value);
            }
        }

        if changed {
            return Ok(serializer.finish().into_bytes());
        }
        return Ok(bytes.to_vec());
    }

    Err("unsupported_request_body_content_type")
}

fn redact_json_secrets(value: &mut Value) -> bool {
    match value {
        Value::Object(map) => {
            let mut changed = false;
            for (key, child) in map {
                if is_sensitive_body_key(key) {
                    if child.as_str() != Some("[REDACTED]") {
                        *child = Value::String("[REDACTED]".to_owned());
                    }
                    changed = true;
                } else {
                    changed |= redact_json_secrets(child);
                }
            }
            changed
        }
        Value::Array(values) => {
            let mut changed = false;
            for child in values {
                changed |= redact_json_secrets(child);
            }
            changed
        }
        Value::String(text) => {
            if let Some(sanitized) = sanitize_structured_string(text) {
                *text = sanitized;
                true
            } else {
                false
            }
        }
        _ => false,
    }
}

fn is_sensitive_body_key(key: &str) -> bool {
    let normalized = key.trim().to_ascii_lowercase().replace('-', "_");
    matches!(
        normalized.as_str(),
        "authorization"
            | "password"
            | "passwd"
            | "secret"
            | "client_secret"
            | "access_token"
            | "refresh_token"
            | "id_token"
            | "session_token"
            | "auth_token"
            | "api_key"
            | "apikey"
            | "cookie"
            | "csrf_token"
    ) || normalized.ends_with("_token")
        || normalized.contains("credential")
}

pub fn sanitize_url_for_storage(raw_url: &str) -> String {
    let Ok(mut url) = Url::parse(raw_url) else {
        return sanitize_relative_url_for_storage(raw_url);
    };

    let query_pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();

    url.set_fragment(None);
    let _ = url.set_username("");
    let _ = url.set_password(None);

    if !query_pairs.is_empty() {
        url.set_query(None);
        let mut query = url.query_pairs_mut();
        for (key, value) in query_pairs {
            if is_sensitive_query_key(&key) {
                query.append_pair(&key, "[REDACTED]");
            } else {
                query.append_pair(&key, &value);
            }
        }
    }

    url.to_string()
}

fn sanitize_relative_url_for_storage(raw_url: &str) -> String {
    let fragmentless = raw_url.split_once('#').map_or(raw_url, |(head, _)| head);
    let Some((prefix, raw_query)) = fragmentless.split_once('?') else {
        return fragmentless.to_owned();
    };

    let pairs: Vec<(String, String)> = url::form_urlencoded::parse(raw_query.as_bytes())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();

    if pairs.is_empty() {
        return prefix.to_owned();
    }

    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in pairs {
        if is_sensitive_query_key(&key) {
            serializer.append_pair(&key, "[REDACTED]");
        } else {
            serializer.append_pair(&key, &value);
        }
    }

    format!("{prefix}?{}", serializer.finish())
}

pub fn credential_endpoint_reason(raw_url: &str) -> Option<&'static str> {
    let url = Url::parse(raw_url).ok()?;
    if !is_chatgpt_host(&url.host_str().unwrap_or_default().to_ascii_lowercase()) {
        return None;
    }

    let path = url.path().to_ascii_lowercase();
    if path == "/api/auth"
        || path.starts_with("/api/auth/")
        || path == "/auth"
        || path.starts_with("/auth/")
        || path.starts_with("/backend-api/auth/")
        || path.contains("/oauth/")
        || path.ends_with("/oauth")
        || path.contains("/login")
    {
        return Some("credential_endpoint");
    }

    None
}

fn is_sensitive_query_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "token"
            | "access_token"
            | "id_token"
            | "refresh_token"
            | "session"
            | "session_token"
            | "jwt"
            | "code"
            | "state"
            | "sig"
            | "signature"
            | "authorization"
            | "key"
            | "api_key"
            | "apikey"
            | "x-amz-signature"
            | "x-amz-credential"
            | "x-amz-security-token"
            | "policy"
            | "key-pair-id"
    )
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
    use mirrarium_protocol::{CaptureMetadata, CaptureProvenance};
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
            provenance: CaptureProvenance::default(),
        }
    }

    #[test]
    fn replay_telemetry_accepts_only_static_public_scope() {
        let directory = tempdir().unwrap();
        let store = CaptureStore::open(directory.path()).unwrap();

        store
            .record_cache_replay_outcome(
                "https://chatgpt.com/_next/static/app.js",
                "Script",
                "hit",
                42,
            )
            .unwrap();
        store
            .record_cache_replay_outcome(
                "https://chatgpt.com/_next/static/app.css",
                "Stylesheet",
                "miss",
                0,
            )
            .unwrap();

        assert!(store
            .record_cache_replay_outcome(
                "https://chatgpt.com/backend-api/conversation/x",
                "Fetch",
                "miss",
                0,
            )
            .is_err());
        assert!(store
            .record_cache_replay_outcome(
                "https://chatgpt.com/_next/static/app.js?token=secret",
                "Script",
                "miss",
                0,
            )
            .is_err());
        assert!(store
            .record_cache_replay_outcome(
                "https://chatgpt.com/_next/static/app.js",
                "Script",
                "miss",
                1,
            )
            .is_err());

        let (count, bytes): (i64, i64) = store
            .connection
            .query_row(
                "SELECT COUNT(*), SUM(body_bytes) FROM cache_replay_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(bytes, 42);
    }

    #[test]
    fn deduplicates_only_within_a_privacy_class() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        let body = BASE64.encode(b"same bytes");

        for capture_id in ["one", "two"] {
            let mut item = metadata(
                capture_id,
                "https://chatgpt.com/backend-api/conversation/example",
                "Fetch",
            );
            item.mime_type = "text/plain".to_owned();
            store.begin(item).unwrap();
            store.append_chunk(capture_id, 0, &body).unwrap();
            store.finish(capture_id, Some(10), None).unwrap();
        }

        let mut public_item = metadata(
            "public-copy",
            "https://chatgpt.com/_next/static/example.js",
            "Script",
        );
        public_item.mime_type = "application/javascript".to_owned();
        store
            .begin(public_item)
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

        let captures = store.recent_captures(2).unwrap();
        assert_eq!(captures.len(), 2);

        let report = store.verify().unwrap();
        assert_eq!(report.checked_objects, 2);
        assert_eq!(report.corrupt_objects, 0);
    }

    #[test]
    fn verifier_detects_corruption() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        let body = BASE64.encode(b"original");
        let mut item = metadata(
            "corrupt-me",
            "https://chatgpt.com/backend-api/test",
            "Fetch",
        );
        item.mime_type = "text/plain".to_owned();
        store.begin(item).unwrap();
        store.append_chunk("corrupt-me", 0, &body).unwrap();
        store.finish("corrupt-me", Some(8), None).unwrap();

        let capture = store.recent_captures(1).unwrap().pop().unwrap();
        let hash = capture.body_hash.unwrap();
        let path = directory
            .path()
            .join(object_relative_path(PrivacyClass::Private, &hash));
        fs::write(path, b"tampered").unwrap();

        let report = store.verify().unwrap();
        assert_eq!(report.checked_objects, 1);
        assert_eq!(report.corrupt_objects, 1);
    }

    #[test]
    fn credential_endpoint_body_is_never_persisted() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        store
            .begin(metadata(
                "auth-session",
                "https://chatgpt.com/api/auth/session?access_token=secret#fragment",
                "Fetch",
            ))
            .unwrap();
        store
            .append_chunk("auth-session", 0, &BASE64.encode(b"reusable credential"))
            .unwrap();
        store.finish("auth-session", Some(19), None).unwrap();

        let stats = store.stats().unwrap();
        assert_eq!(stats.captures, 1);
        assert_eq!(stats.objects, 0);
        assert_eq!(stats.suppressed_bodies, 1);
        assert_eq!(stats.body_errors, 0);

        let capture = store.recent_captures(1).unwrap().pop().unwrap();
        assert!(!capture.url.contains("secret"));
        assert!(!capture.url.contains("#fragment"));
        assert!(capture.url.contains("access_token=%5BREDACTED%5D"));
    }

    #[test]
    fn relative_url_secrets_are_redacted() {
        let sanitized = sanitize_url_for_storage(
            "/backend-api/redirect-final?token=fixture-secret&keep=yes#fragment",
        );

        assert!(sanitized.starts_with("/backend-api/redirect-final?"));
        assert!(!sanitized.contains("fixture-secret"));
        assert!(!sanitized.contains("#fragment"));
        assert!(sanitized.contains("keep=yes"));
        assert!(sanitized.contains("token=%5BREDACTED%5D"));
    }

    #[test]
    fn signed_url_secrets_are_redacted_without_suppressing_normal_content() {
        let sanitized = sanitize_url_for_storage(
            "https://files.example.test/object?x=1&X-Amz-Signature=abc&token=def#secret",
        );
        assert!(sanitized.contains("x=1"));
        assert!(!sanitized.contains("abc"));
        assert!(!sanitized.contains("def"));
        assert!(!sanitized.contains("#secret"));
        assert!(sanitized.matches("%5BREDACTED%5D").count() >= 2);
    }

    #[test]
    fn private_json_response_is_scrubbed_before_cas() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        store
            .begin(metadata(
                "json-response-secrets",
                "https://chatgpt.com/backend-api/conversation/attachment",
                "Fetch",
            ))
            .unwrap();

        let body = br#"{"id":"conversation-a","access_token":"top-secret","attachment":{"download_url":"https://files.example.test/object?keep=yes&X-Amz-Signature=signed-secret&token=query-secret"}}"#;
        store
            .append_chunk("json-response-secrets", 0, &BASE64.encode(body))
            .unwrap();
        store
            .finish("json-response-secrets", Some(body.len() as u64), None)
            .unwrap();

        let capture = store.recent_captures(1).unwrap().pop().unwrap();
        let hash = capture.body_hash.unwrap();
        let path = directory
            .path()
            .join(object_relative_path(PrivacyClass::Private, &hash));
        let stored = fs::read_to_string(path).unwrap();

        assert!(!stored.contains("top-secret"));
        assert!(!stored.contains("signed-secret"));
        assert!(!stored.contains("query-secret"));
        assert!(stored.contains("[REDACTED]"));
        assert!(stored.contains("keep=yes"));
    }

    #[test]
    fn private_sse_json_is_scrubbed_before_cas() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        let mut item = metadata(
            "sse-response-secrets",
            "https://chatgpt.com/backend-api/conversation/stream",
            "Fetch",
        );
        item.mime_type = "text/event-stream".to_owned();
        store.begin(item).unwrap();

        let body = concat!(
            "event: message\n",
            "data: {\"conversation_id\":\"c1\",\"delta\":\"hello\",",
            "\"download_url\":\"https://files.example.test/o?X-Amz-Signature=sse-secret\"}\n\n",
            "data: [DONE]\n\n"
        );
        store
            .append_chunk("sse-response-secrets", 0, &BASE64.encode(body.as_bytes()))
            .unwrap();
        store
            .finish(
                "sse-response-secrets",
                Some(body.len() as u64),
                None,
            )
            .unwrap();

        let capture = store.recent_captures(1).unwrap().pop().unwrap();
        let hash = capture.body_hash.unwrap();
        let path = directory
            .path()
            .join(object_relative_path(PrivacyClass::Private, &hash));
        let stored = fs::read_to_string(path).unwrap();

        assert!(!stored.contains("sse-secret"));
        assert!(stored.contains("%5BREDACTED%5D"));
        assert!(stored.contains("\"delta\":\"hello\""));
        assert!(stored.contains("[DONE]"));
    }

    #[test]
    fn malformed_private_json_response_is_suppressed() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        store
            .begin(metadata(
                "malformed-json-response",
                "https://chatgpt.com/backend-api/conversation/broken",
                "Fetch",
            ))
            .unwrap();

        let body = br#"{"download_url":"https://files.example.test/o?token=secret""#;
        store
            .append_chunk("malformed-json-response", 0, &BASE64.encode(body))
            .unwrap();
        store
            .finish(
                "malformed-json-response",
                Some(body.len() as u64),
                None,
            )
            .unwrap();

        let stats = store.stats().unwrap();
        assert_eq!(stats.objects, 0);
        assert_eq!(stats.suppressed_bodies, 1);

        let capture = store.recent_captures(1).unwrap().pop().unwrap();
        assert!(capture.body_hash.is_none());
        assert_eq!(
            capture.body_error.as_deref(),
            Some("suppressed:unparseable_json_response_body")
        );
    }

    #[test]
    fn request_body_is_private_and_redacted_before_cas() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        let mut meta = metadata(
            "request-body",
            "https://chatgpt.com/backend-api/conversation",
            "Fetch",
        );
        meta.method = "POST".to_owned();
        store.begin(meta).unwrap();
        store
            .begin_request_body(
                "request-body",
                RequestBodyMetadata {
                    content_type: Some("application/json".to_owned()),
                    has_post_data: true,
                    post_data_entry_count: Some(1),
                    declared_content_length: Some(150),
                },
            )
            .unwrap();
        store
            .append_request_body_chunk(
                "request-body",
                0,
                &BASE64.encode(
                    br#"{"message":"hello from request body","access_token":"fixture-secret-token","nested":[{"id_token":"first-secret"},{"refresh_token":"second-secret"}]}"#,
                ),
            )
            .unwrap();
        store.finish_request_body("request-body", None).unwrap();
        store
            .finish("request-body", Some(2), Some("fixture response unavailable"))
            .unwrap();

        let stats = store.stats().unwrap();
        assert_eq!(stats.request_bodies, 1);
        assert_eq!(stats.request_body_errors, 0);
        assert_eq!(stats.suppressed_request_bodies, 0);

        let capture = store.recent_captures(1).unwrap().pop().unwrap();
        assert_eq!(capture.request_body_kind.as_deref(), Some("json"));
        assert_eq!(
            capture.request_body_content_type.as_deref(),
            Some("application/json")
        );
        assert_eq!(capture.request_body_has_post_data, Some(true));
        assert_eq!(capture.request_body_post_data_entry_count, Some(1));
        assert_eq!(capture.request_body_declared_content_length, Some(150));
        let hash = capture.request_body_hash.unwrap();
        let path = directory
            .path()
            .join(object_relative_path(PrivacyClass::Private, &hash));
        let persisted = fs::read_to_string(path).unwrap();
        assert!(persisted.contains("hello from request body"));
        assert!(persisted.contains("[REDACTED]"));
        assert!(!persisted.contains("fixture-secret-token"));
        assert!(!persisted.contains("first-secret"));
        assert!(!persisted.contains("second-secret"));
    }

    #[test]
    fn unsupported_request_body_never_reaches_cas() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        store
            .begin(metadata(
                "opaque-request",
                "https://chatgpt.com/backend-api/upload",
                "Fetch",
            ))
            .unwrap();
        store
            .begin_request_body(
                "opaque-request",
                RequestBodyMetadata {
                    content_type: Some("application/octet-stream".to_owned()),
                    has_post_data: true,
                    post_data_entry_count: Some(2),
                    declared_content_length: Some(4096),
                },
            )
            .unwrap();
        store
            .append_request_body_chunk(
                "opaque-request",
                0,
                &BASE64.encode(b"opaque secret-bearing bytes"),
            )
            .unwrap();
        store.finish_request_body("opaque-request", None).unwrap();
        store
            .finish("opaque-request", Some(2), Some("fixture response unavailable"))
            .unwrap();

        let stats = store.stats().unwrap();
        assert_eq!(stats.request_bodies, 1);
        assert_eq!(stats.suppressed_request_bodies, 1);

        let capture = store.recent_captures(1).unwrap().pop().unwrap();
        assert!(capture.request_body_hash.is_none());
        assert_eq!(capture.request_body_bytes, 0);
        assert_eq!(capture.request_body_kind.as_deref(), Some("opaque"));
        assert_eq!(capture.request_body_post_data_entry_count, Some(2));
        assert_eq!(capture.request_body_declared_content_length, Some(4096));
    }

    #[test]
    fn provenance_is_sanitized_before_it_reaches_the_ledger() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        let mut item = metadata(
            "provenance",
            "https://chatgpt.com/backend-api/test",
            "Fetch",
        );
        item.provenance.document_url =
            Some("https://chatgpt.com/c/test?access_token=secret#fragment".to_owned());
        item.provenance.lifecycle_id = Some("lifecycle-1".to_owned());
        item.provenance.redirect_hop = Some(1);
        item.provenance.redirected_from_url =
            Some("https://chatgpt.com/start?token=redirect-secret".to_owned());
        item.provenance
            .request_headers
            .insert("Authorization".to_owned(), "Bearer secret".to_owned());
        item.provenance
            .response_headers
            .insert("Set-Cookie".to_owned(), "session=secret".to_owned());
        item.provenance.response_headers.insert(
            "Location".to_owned(),
            "https://chatgpt.com/next?token=secret".to_owned(),
        );
        item.provenance.response_protocol = Some("h2".to_owned());
        item.provenance.from_disk_cache = true;

        store.begin(item).unwrap();
        store
            .append_chunk("provenance", 0, &BASE64.encode(b"body"))
            .unwrap();
        store.finish("provenance", Some(4), None).unwrap();

        let provenance = store
            .recent_captures(1)
            .unwrap()
            .pop()
            .unwrap()
            .provenance;
        assert_eq!(
            provenance.request_headers["Authorization"],
            "[REDACTED]"
        );
        assert_eq!(provenance.response_headers["Set-Cookie"], "[REDACTED]");
        assert!(!provenance.document_url.unwrap().contains("secret"));
        assert!(!provenance.redirected_from_url.unwrap().contains("redirect-secret"));
        assert_eq!(provenance.lifecycle_id.as_deref(), Some("lifecycle-1"));
        assert_eq!(provenance.redirect_hop, Some(1));
        assert!(!provenance.response_headers["Location"].contains("secret"));
        assert_eq!(provenance.response_protocol.as_deref(), Some("h2"));
        assert!(provenance.from_disk_cache);
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
