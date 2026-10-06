use std::{
    collections::{BTreeMap, HashMap},
    env,
    fs::{self, File, OpenOptions},
    io::{BufWriter, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chacha20poly1305::{
    aead::{Aead, Payload},
    KeyInit, XChaCha20Poly1305, XNonce,
};
use fs2::FileExt;
use hkdf::Hkdf;
use mirrarium_protocol::{CaptureMetadata, CaptureProvenance, RequestBodyMetadata};
use rand_core::{OsRng, RngCore};
use rusqlite::{params, types::Type, Connection, OpenFlags};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use url::Url;

const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;
const PRIVATE_OBJECT_MAGIC: &[u8; 8] = b"MIRRPV01";
const PRIVATE_STREAM_MAGIC: &[u8; 8] = b"MIRRPV02";
const PRIVATE_KEY_BYTES: usize = 32;
const PRIVATE_NONCE_BYTES: usize = 24;
const PRIVATE_STREAM_NONCE_PREFIX_BYTES: usize = 16;
const PRIVATE_STREAM_TAG_BYTES: usize = 16;
const PRIVATE_STREAM_REWRITE_CHUNK_BYTES: usize = 256 * 1024;
const SQLITE_PLAINTEXT_HEADER: &[u8; 16] = b"SQLite format 3\0";
const LEDGER_KEY_PURPOSE: &str = "ledger-sqlcipher-v1";
const READ_ONLY_LEDGER_BUSY_TIMEOUT_MS: u64 = 250;

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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IncomingMaintenanceStatus {
    pub incoming_exists: bool,
    pub writer_active: bool,
    pub incomplete_capture_files: u64,
    pub incomplete_capture_bytes: u64,
    pub inflight_capture_files: u64,
    pub inflight_capture_bytes: u64,
    pub abandoned_capture_files: u64,
    pub abandoned_capture_bytes: u64,
    pub ledger_recovery_files: u64,
    pub ledger_recovery_bytes: u64,
    pub unexpected_files: u64,
    pub unexpected_bytes: u64,
    pub unexpected_non_file_entries: u64,
    pub cleanup_on_next_writer_start: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PrivateStorageStatus {
    pub key_path: String,
    pub key_exists: bool,
    pub ledger_exists: bool,
    pub ledger_encrypted: bool,
    pub ledger_plaintext_legacy: bool,
    pub private_objects: u64,
    pub encrypted_private_objects: u64,
    pub legacy_plaintext_private_objects: u64,
    pub missing_or_invalid_private_objects: u64,
    pub migration_needed: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PrivateMigrationReport {
    pub key_path: String,
    pub migrated_objects: u64,
    pub migrated_body_bytes: u64,
    pub already_encrypted_objects: u64,
    pub ledger_migrated: bool,
    pub ledger_already_encrypted: bool,
    pub ledger_plaintext_bytes: u64,
}

#[derive(Debug)]
struct PrivateObjectMigrationReport {
    migrated_objects: u64,
    migrated_body_bytes: u64,
    already_encrypted_objects: u64,
}

#[derive(Debug)]
struct LedgerMigrationReport {
    migrated: bool,
    already_encrypted: bool,
    plaintext_bytes: u64,
}

struct RequestBodyCapture {
    metadata: RequestBodyMetadata,
    bytes: Vec<u8>,
    next_sequence: u32,
    finished: bool,
    error: Option<String>,
}

enum ResponseBodySink {
    Plain(BufWriter<File>),
    PrivateEncrypted(PrivateStreamWriter),
}

impl ResponseBodySink {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        match self {
            Self::Plain(writer) => writer.write_all(bytes).context("writing response temp body"),
            Self::PrivateEncrypted(writer) => writer.write_chunk(bytes),
        }
    }

    fn flush(&mut self) -> Result<()> {
        match self {
            Self::Plain(writer) => writer.flush().context("flushing response temp body"),
            Self::PrivateEncrypted(writer) => writer.flush(),
        }
    }
}

struct InFlightCapture {
    metadata: CaptureMetadata,
    temp_path: PathBuf,
    writer: ResponseBodySink,
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
    writer_lock: Option<File>,
}

impl CaptureStore {
    pub fn open_read_only(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let connection = open_raw_ledger_connection(&root, true)?;
        Ok(Self {
            root,
            connection,
            in_flight: HashMap::new(),
            writer_lock: None,
        })
    }

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

        let writer_lock = acquire_writer_lock(&root)?;
        purge_abandoned_capture_parts(&root)?;

        let database_path = root.join("ledger.sqlite3");
        let connection = open_raw_ledger_connection(&root, false)?;
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

            CREATE TABLE IF NOT EXISTS private_revalidation_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at_ms INTEGER NOT NULL,
                outcome TEXT NOT NULL,
                body_bytes INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS private_revalidation_events_outcome_idx
                ON private_revalidation_events(outcome);
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
            writer_lock: Some(writer_lock),
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

        let privacy = classify(&metadata);
        let writer = if privacy == PrivacyClass::Private && suppressed_reason.is_none() {
            let key = load_or_create_private_key(&self.root)?;
            ResponseBodySink::PrivateEncrypted(PrivateStreamWriter::new(
                BufWriter::new(file),
                key,
            )?)
        } else {
            ResponseBodySink::Plain(BufWriter::new(file))
        };

        self.in_flight.insert(
            metadata.capture_id.clone(),
            InFlightCapture {
                metadata,
                temp_path,
                writer,
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
        capture.writer.write_chunk(&bytes)?;
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
            &self.root,
            privacy,
            &capture.metadata.mime_type,
            &capture.temp_path,
        )? {
            ResponseBodyDisposition::KeepRaw => format!("{:x}", capture.hasher.finalize()),
            ResponseBodyDisposition::Replace(bytes) => {
                if privacy == PrivacyClass::Private {
                    let key = load_existing_private_key(&self.root)?;
                    rewrite_private_stream_file(&capture.temp_path, key, &bytes)?;
                } else {
                    fs::write(&capture.temp_path, &bytes)
                        .with_context(|| format!("rewriting {}", capture.temp_path.display()))?;
                    harden_file(&capture.temp_path)?;
                }
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
            if privacy == PrivacyClass::Private {
                let raw = fs::read(&capture.temp_path)
                    .with_context(|| format!("verifying private temp object {}", capture.temp_path.display()))?;
                anyhow::ensure!(
                    raw.starts_with(PRIVATE_STREAM_MAGIC),
                    "private response temp object is not MIRRPV02 encrypted"
                );
            }
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

    pub fn private_storage_status(&self) -> Result<PrivateStorageStatus> {
        let key_path = private_key_path(&self.root)?;
        let ledger_path = self.root.join("ledger.sqlite3");
        let ledger_exists = ledger_path.is_file();
        let ledger_plaintext_legacy =
            ledger_exists && database_has_plaintext_sqlite_header(&ledger_path)?;
        let ledger_encrypted = ledger_exists && !ledger_plaintext_legacy;
        let mut statement = self.connection.prepare(
            "SELECT hash, relative_path FROM objects WHERE storage_class = 'private' ORDER BY hash",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut private_objects = 0_u64;
        let mut encrypted_private_objects = 0_u64;
        let mut legacy_plaintext_private_objects = 0_u64;
        let mut missing_or_invalid_private_objects = 0_u64;

        for row in rows {
            let (hash, indexed_relative_path) = row?;
            private_objects = private_objects
                .checked_add(1)
                .context("private object count overflow")?;

            if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                missing_or_invalid_private_objects += 1;
                continue;
            }

            let expected_relative_path = object_relative_path(PrivacyClass::Private, &hash);
            if Path::new(&indexed_relative_path) != expected_relative_path {
                missing_or_invalid_private_objects += 1;
                continue;
            }

            match fs::read(self.root.join(&expected_relative_path)) {
                Ok(bytes) if is_encrypted_private_object(&bytes) => {
                    encrypted_private_objects += 1;
                }
                Ok(_) => {
                    legacy_plaintext_private_objects += 1;
                }
                Err(_) => {
                    missing_or_invalid_private_objects += 1;
                }
            }
        }

        Ok(PrivateStorageStatus {
            key_path: key_path.to_string_lossy().into_owned(),
            key_exists: key_path.is_file(),
            ledger_exists,
            ledger_encrypted,
            ledger_plaintext_legacy,
            private_objects,
            encrypted_private_objects,
            legacy_plaintext_private_objects,
            missing_or_invalid_private_objects,
            migration_needed: legacy_plaintext_private_objects > 0 || ledger_plaintext_legacy,
        })
    }

    fn migrate_private_objects(&self) -> Result<PrivateObjectMigrationReport> {
        let mut statement = self.connection.prepare(
            "SELECT hash, bytes, relative_path FROM objects WHERE storage_class = 'private' ORDER BY hash",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;

        let mut migrated_objects = 0_u64;
        let mut migrated_body_bytes = 0_u64;
        let mut already_encrypted_objects = 0_u64;
        let mut key: Option<[u8; PRIVATE_KEY_BYTES]> = None;

        for row in rows {
            let (hash, expected_bytes, indexed_relative_path) = row?;
            anyhow::ensure!(
                hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "invalid private object hash {hash:?}"
            );
            anyhow::ensure!(expected_bytes >= 0, "negative private object byte count");

            let relative_path = object_relative_path(PrivacyClass::Private, &hash);
            anyhow::ensure!(
                Path::new(&indexed_relative_path) == relative_path,
                "private object path mismatch for {hash}"
            );
            let final_path = self.root.join(&relative_path);
            let plaintext = fs::read(&final_path)
                .with_context(|| format!("reading legacy private object {}", final_path.display()))?;

            if is_encrypted_private_object(&plaintext) {
                already_encrypted_objects = already_encrypted_objects
                    .checked_add(1)
                    .context("encrypted private object count overflow")?;
                continue;
            }

            anyhow::ensure!(
                plaintext.len() as i64 == expected_bytes,
                "legacy private object {hash} has unexpected byte count"
            );
            anyhow::ensure!(
                sha256_hex(&plaintext) == hash,
                "legacy private object {hash} failed SHA-256 verification"
            );

            let key_ref = if let Some(existing) = key.as_ref() {
                existing
            } else {
                key.insert(load_or_create_private_key(&self.root)?)
            };
            let envelope = encrypt_private_object_bytes(key_ref, &hash, &plaintext)?;

            let temp_path = self
                .root
                .join(".incoming")
                .join(format!("{hash}.private-migration.part"));
            if temp_path.exists() {
                fs::remove_file(&temp_path)
                    .with_context(|| format!("removing stale migration file {}", temp_path.display()))?;
            }
            fs::write(&temp_path, &envelope)
                .with_context(|| format!("writing private migration file {}", temp_path.display()))?;
            harden_file(&temp_path)?;
            fs::rename(&temp_path, &final_path).with_context(|| {
                format!(
                    "replacing legacy private object {} with encrypted envelope",
                    final_path.display()
                )
            })?;
            harden_file(&final_path)?;

            migrated_objects = migrated_objects
                .checked_add(1)
                .context("migrated private object count overflow")?;
            migrated_body_bytes = migrated_body_bytes
                .checked_add(plaintext.len() as u64)
                .context("migrated private byte count overflow")?;
        }

        Ok(PrivateObjectMigrationReport {
            migrated_objects,
            migrated_body_bytes,
            already_encrypted_objects,
        })
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
        let allowed_path = match host.as_str() {
            "chatgpt.com" | "chat.openai.com" => url.path().starts_with("/_next/static/"),
            "cdn.oaistatic.com" => true,
            _ => false,
        };
        anyhow::ensure!(
            url.scheme() == "https"
                && allowed_path
                && url.query().is_none()
                && url.fragment().is_none(),
            "refusing replay telemetry for non-static public URL"
        );
        anyhow::ensure!(
            matches!(
                resource_type.to_ascii_lowercase().as_str(),
                "script" | "stylesheet" | "image" | "font"
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

    pub fn record_private_revalidation_outcome(
        &self,
        outcome: &str,
        body_bytes: u64,
    ) -> Result<()> {
        anyhow::ensure!(
            matches!(outcome, "not_modified" | "refreshed" | "fulfill_error"),
            "invalid private revalidation outcome"
        );
        anyhow::ensure!(
            outcome == "not_modified" || body_bytes == 0,
            "only not_modified private revalidation may report saved bytes"
        );

        self.connection.execute(
            r#"
            INSERT INTO private_revalidation_events (
                observed_at_ms,
                outcome,
                body_bytes
            ) VALUES (?1, ?2, ?3)
            "#,
            params![now_ms()? as i64, outcome, body_bytes as i64],
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

            match read_verified_object(&self.root, &storage_class, &hash) {
                Ok(bytes) if bytes.len() as i64 == expected_bytes => {}
                Ok(bytes) => {
                    report.corrupt_objects += 1;
                    report.errors.push(format!(
                        "{storage_class}/{hash}: expected {expected_bytes} logical bytes, got {}",
                        bytes.len()
                    ));
                }
                Err(error) => {
                    report.corrupt_objects += 1;
                    report
                        .errors
                        .push(format!("{storage_class}/{hash}: {error:#}"));
                }
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
                let key = load_or_create_private_key(&self.root)?;
                let envelope = encrypt_private_object_bytes(&key, &hash, &request_body.bytes)?;
                file.write_all(&envelope)?;
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

fn database_has_plaintext_sqlite_header(path: &Path) -> Result<bool> {
    let mut file = File::open(path)
        .with_context(|| format!("opening database header {}", path.display()))?;
    let mut header = [0_u8; SQLITE_PLAINTEXT_HEADER.len()];
    match file.read_exact(&mut header) {
        Ok(()) => Ok(&header == SQLITE_PLAINTEXT_HEADER),
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error)
            .with_context(|| format!("reading database header {}", path.display())),
    }
}

fn open_raw_ledger_connection(root: &Path, read_only: bool) -> Result<Connection> {
    let database = root.join("ledger.sqlite3");
    if read_only {
        anyhow::ensure!(
            database.is_file(),
            "raw ledger does not exist: {}",
            database.display()
        );
    }

    let existed = database.is_file();
    let plaintext = existed && database_has_plaintext_sqlite_header(&database)?;
    let connection = if read_only {
        Connection::open_with_flags(
            &database,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
    } else {
        Connection::open(&database)
    }
    .with_context(|| format!("opening raw ledger {}", database.display()))?;

    if read_only {
        connection
            .busy_timeout(Duration::from_millis(READ_ONLY_LEDGER_BUSY_TIMEOUT_MS))
            .context("configuring read-only raw-ledger busy timeout")?;
    }

    if !plaintext {
        apply_private_database_key(
            &connection,
            root,
            LEDGER_KEY_PURPOSE,
            !existed,
        )
        .context("opening encrypted raw ledger")?;
    }

    Ok(connection)
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn database_user_table_counts(connection: &Connection) -> Result<BTreeMap<String, u64>> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let tables = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut counts = BTreeMap::new();
    for table in tables {
        let quoted = table.replace('"', "\"\"");
        let count: i64 = connection.query_row(
            &format!("SELECT COUNT(*) FROM \"{quoted}\""),
            [],
            |row| row.get(0),
        )?;
        let count: u64 = count
            .try_into()
            .with_context(|| format!("negative row count in table {table:?}"))?;
        counts.insert(table, count);
    }
    Ok(counts)
}

fn database_schema_fingerprint(
    connection: &Connection,
) -> Result<Vec<(String, String, String, Option<String>)>> {
    let mut statement = connection.prepare(
        r#"
        SELECT type, name, tbl_name, sql
        FROM sqlite_master
        WHERE name NOT LIKE 'sqlite_%'
        ORDER BY type, name
        "#,
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<String>>(3)?,
        ))
    })?;
    let fingerprint = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(fingerprint)
}
fn verify_database_integrity(connection: &Connection) -> Result<()> {
    let result: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .context("running SQLite integrity_check")?;
    anyhow::ensure!(
        result.eq_ignore_ascii_case("ok"),
        "SQLite integrity_check failed: {result}"
    );
    Ok(())
}

fn remove_database_sidecars(database: &Path) -> Result<()> {
    for path in [
        PathBuf::from(format!("{}-wal", database.display())),
        PathBuf::from(format!("{}-shm", database.display())),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("removing database sidecar {}", path.display()));
            }
        }
    }
    Ok(())
}

fn recover_interrupted_ledger_migration(root: &Path) -> Result<()> {
    let database = root.join("ledger.sqlite3");
    let backup = root.join(".incoming/ledger.sqlite3.plaintext-backup");
    let encrypted_part = root.join(".incoming/ledger.sqlite3.encrypted.part");

    if !database.exists() && backup.exists() {
        fs::rename(&backup, &database).with_context(|| {
            format!(
                "restoring interrupted ledger migration backup {}",
                backup.display()
            )
        })?;
        remove_database_sidecars(&encrypted_part)?;
        let _ = fs::remove_file(&encrypted_part);
        return Ok(());
    }

    if database.exists() && backup.exists() {
        if !database_has_plaintext_sqlite_header(&database)?
            && open_raw_ledger_connection(root, true).is_ok()
        {
            fs::remove_file(&backup).with_context(|| {
                format!("removing completed ledger migration backup {}", backup.display())
            })?;
        } else {
            anyhow::bail!(
                "raw ledger migration backup {} exists; refusing to overwrite recovery evidence",
                backup.display()
            );
        }
    }

    Ok(())
}

fn migrate_raw_ledger(root: &Path) -> Result<LedgerMigrationReport> {
    recover_interrupted_ledger_migration(root)?;

    let database = root.join("ledger.sqlite3");
    anyhow::ensure!(
        database.is_file(),
        "raw ledger does not exist: {}",
        database.display()
    );

    if !database_has_plaintext_sqlite_header(&database)? {
        let connection = open_raw_ledger_connection(root, true)?;
        verify_database_integrity(&connection)?;
        return Ok(LedgerMigrationReport {
            migrated: false,
            already_encrypted: true,
            plaintext_bytes: 0,
        });
    }

    let plaintext_bytes = fs::metadata(&database)
        .with_context(|| format!("reading raw ledger metadata {}", database.display()))?
        .len();

    let source = Connection::open(&database)
        .with_context(|| format!("opening plaintext raw ledger {}", database.display()))?;
    verify_database_integrity(&source)?;

    let checkpoint: (i64, i64, i64) = source
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .context("checkpointing plaintext raw ledger before migration")?;
    anyhow::ensure!(
        checkpoint.0 == 0,
        "raw ledger WAL checkpoint is busy; close other Mirrarium processes before migration"
    );

    let source_counts = database_user_table_counts(&source)?;
    let source_schema = database_schema_fingerprint(&source)?;
    let source_user_version: i64 =
        source.pragma_query_value(None, "user_version", |row| row.get(0))?;

    let target = root.join(".incoming/ledger.sqlite3.encrypted.part");
    let backup = root.join(".incoming/ledger.sqlite3.plaintext-backup");
    anyhow::ensure!(
        !backup.exists(),
        "raw ledger migration backup {} already exists",
        backup.display()
    );
    let _ = fs::remove_file(&target);
    remove_database_sidecars(&target)?;

    let key = private_database_key(root, LEDGER_KEY_PURPOSE, true)?;
    let key_literal = sqlcipher_raw_key_literal(&key);
    let target_literal = sql_string_literal(&target.to_string_lossy());

    source
        .execute_batch(&format!(
            "ATTACH DATABASE {target_literal} AS encrypted KEY \"{key_literal}\";"
        ))
        .context("attaching encrypted raw-ledger migration target")?;
    let export_result = source.query_row(
        "SELECT sqlcipher_export('encrypted')",
        [],
        |_row| Ok(()),
    );
    if let Err(error) = export_result {
        let _ = source.execute_batch("DETACH DATABASE encrypted;");
        return Err(error).context("exporting plaintext raw ledger into SQLCipher");
    }
    source
        .execute_batch(&format!(
            "PRAGMA encrypted.user_version = {source_user_version};"
        ))
        .context("copying raw ledger user_version")?;
    source
        .execute_batch("DETACH DATABASE encrypted;")
        .context("detaching encrypted raw-ledger migration target")?;
    harden_file(&target)?;

    let target_connection = Connection::open_with_flags(
        &target,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening encrypted migration target {}", target.display()))?;
    apply_private_database_key(
        &target_connection,
        root,
        LEDGER_KEY_PURPOSE,
        false,
    )
    .context("verifying encrypted raw-ledger migration target")?;
    verify_database_integrity(&target_connection)?;
    let target_counts = database_user_table_counts(&target_connection)?;
    let target_schema = database_schema_fingerprint(&target_connection)?;
    let target_user_version: i64 =
        target_connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    anyhow::ensure!(
        target_counts == source_counts,
        "encrypted raw-ledger migration changed table row counts"
    );
    anyhow::ensure!(
        target_schema == source_schema,
        "encrypted raw-ledger migration changed database schema"
    );
    anyhow::ensure!(
        target_user_version == source_user_version,
        "encrypted raw-ledger migration changed user_version"
    );
    drop(target_connection);
    drop(source);

    remove_database_sidecars(&database)?;
    fs::rename(&database, &backup).with_context(|| {
        format!(
            "moving plaintext raw ledger {} to migration backup {}",
            database.display(),
            backup.display()
        )
    })?;

    if let Err(error) = fs::rename(&target, &database) {
        let _ = fs::rename(&backup, &database);
        return Err(error).with_context(|| {
            format!(
                "installing encrypted raw ledger {}",
                database.display()
            )
        });
    }
    harden_file(&database)?;

    let final_connection = match open_raw_ledger_connection(root, true) {
        Ok(connection) => connection,
        Err(error) => {
            let _ = fs::remove_file(&database);
            let _ = fs::rename(&backup, &database);
            return Err(error).context("verifying installed encrypted raw ledger");
        }
    };
    verify_database_integrity(&final_connection)?;
    anyhow::ensure!(
        database_user_table_counts(&final_connection)? == source_counts,
        "installed encrypted raw ledger changed table row counts"
    );
    drop(final_connection);

    fs::remove_file(&backup).with_context(|| {
        format!(
            "removing plaintext raw-ledger migration backup {}",
            backup.display()
        )
    })?;

    Ok(LedgerMigrationReport {
        migrated: true,
        already_encrypted: false,
        plaintext_bytes,
    })
}

pub fn migrate_private_storage(root: impl AsRef<Path>) -> Result<PrivateMigrationReport> {
    let root = root.as_ref();
    let key_path = private_key_path(root)?;

    if root.join("ledger.sqlite3").is_file() {
        let reader = CaptureStore::open_read_only(root)?;
        let has_objects_table: i64 = reader.connection.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'objects'",
            [],
            |row| row.get(0),
        )?;
        if has_objects_table > 0 {
            let status = reader.private_storage_status()?;
            if status.legacy_plaintext_private_objects == 0
                && status.missing_or_invalid_private_objects == 0
                && status.ledger_encrypted
                && !status.ledger_plaintext_legacy
            {
                return Ok(PrivateMigrationReport {
                    key_path: status.key_path,
                    migrated_objects: 0,
                    migrated_body_bytes: 0,
                    already_encrypted_objects: status.encrypted_private_objects,
                    ledger_migrated: false,
                    ledger_already_encrypted: true,
                    ledger_plaintext_bytes: 0,
                });
            }
        }
    }

    let mut store = CaptureStore::open(root)?;
    let objects = store.migrate_private_objects()?;
    let writer_lock = store.writer_lock.take();
    drop(store);
    let ledger = migrate_raw_ledger(root)?;
    drop(writer_lock);

    Ok(PrivateMigrationReport {
        key_path: key_path.to_string_lossy().into_owned(),
        migrated_objects: objects.migrated_objects,
        migrated_body_bytes: objects.migrated_body_bytes,
        already_encrypted_objects: objects.already_encrypted_objects,
        ledger_migrated: ledger.migrated,
        ledger_already_encrypted: ledger.already_encrypted,
        ledger_plaintext_bytes: ledger.plaintext_bytes,
    })
}

fn acquire_writer_lock(root: &Path) -> Result<File> {
    let path = root.join(".writer.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .with_context(|| format!("opening Mirrarium writer lock {}", path.display()))?;
    harden_file(&path)?;
    file.try_lock_exclusive().with_context(|| {
        format!(
            "another Mirrarium writer is already using data root {}",
            root.display()
        )
    })?;
    Ok(file)
}

fn is_abandoned_capture_part_name(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".part") else {
        return false;
    };

    let hash = stem
        .strip_suffix(".request")
        .or_else(|| stem.strip_suffix(".private-migration"))
        .unwrap_or(stem);
    hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_ledger_recovery_file_name(name: &str) -> bool {
    matches!(
        name,
        "ledger.sqlite3.encrypted.part"
            | "ledger.sqlite3.encrypted.part-wal"
            | "ledger.sqlite3.encrypted.part-shm"
            | "ledger.sqlite3.plaintext-backup"
            | "ledger.sqlite3.plaintext-backup-wal"
            | "ledger.sqlite3.plaintext-backup-shm"
    )
}

fn writer_lock_is_active(root: &Path) -> Result<bool> {
    let path = root.join(".writer.lock");
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("opening Mirrarium writer lock {}", path.display()));
        }
    };

    match FileExt::try_lock_shared(&file) {
        Ok(()) => {
            FileExt::unlock(&file)
                .with_context(|| format!("unlocking Mirrarium writer lock {}", path.display()))?;
            Ok(false)
        }
        Err(error) if error.kind() == ErrorKind::WouldBlock => Ok(true),
        Err(error) => Err(error)
            .with_context(|| format!("probing Mirrarium writer lock {}", path.display())),
    }
}

fn add_maintenance_file(
    files: &mut u64,
    bytes: &mut u64,
    file_bytes: u64,
    label: &str,
) -> Result<()> {
    *files = files
        .checked_add(1)
        .with_context(|| format!("{label} file count overflow"))?;
    *bytes = bytes
        .checked_add(file_bytes)
        .with_context(|| format!("{label} byte count overflow"))?;
    Ok(())
}

pub fn incoming_maintenance_status(
    root: impl AsRef<Path>,
) -> Result<IncomingMaintenanceStatus> {
    let root = root.as_ref();
    let writer_active = writer_lock_is_active(root)?;
    let incoming = root.join(".incoming");

    if !incoming.exists() {
        return Ok(IncomingMaintenanceStatus {
            incoming_exists: false,
            writer_active,
            incomplete_capture_files: 0,
            incomplete_capture_bytes: 0,
            inflight_capture_files: 0,
            inflight_capture_bytes: 0,
            abandoned_capture_files: 0,
            abandoned_capture_bytes: 0,
            ledger_recovery_files: 0,
            ledger_recovery_bytes: 0,
            unexpected_files: 0,
            unexpected_bytes: 0,
            unexpected_non_file_entries: 0,
            cleanup_on_next_writer_start: false,
        });
    }

    anyhow::ensure!(
        incoming.is_dir(),
        "incoming path is not a directory: {}",
        incoming.display()
    );

    let mut incomplete_capture_files = 0_u64;
    let mut incomplete_capture_bytes = 0_u64;
    let mut ledger_recovery_files = 0_u64;
    let mut ledger_recovery_bytes = 0_u64;
    let mut unexpected_files = 0_u64;
    let mut unexpected_bytes = 0_u64;
    let mut unexpected_non_file_entries = 0_u64;

    for entry in fs::read_dir(&incoming)
        .with_context(|| format!("reading incoming directory {}", incoming.display()))?
    {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_file() {
            unexpected_non_file_entries = unexpected_non_file_entries
                .checked_add(1)
                .context("unexpected incoming entry count overflow")?;
            continue;
        }

        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            let bytes = entry.metadata()?.len();
            add_maintenance_file(
                &mut unexpected_files,
                &mut unexpected_bytes,
                bytes,
                "unexpected incoming",
            )?;
            continue;
        };
        let bytes = entry.metadata()?.len();

        if is_abandoned_capture_part_name(name) {
            add_maintenance_file(
                &mut incomplete_capture_files,
                &mut incomplete_capture_bytes,
                bytes,
                "incomplete capture",
            )?;
        } else if is_ledger_recovery_file_name(name) {
            add_maintenance_file(
                &mut ledger_recovery_files,
                &mut ledger_recovery_bytes,
                bytes,
                "ledger recovery",
            )?;
        } else {
            add_maintenance_file(
                &mut unexpected_files,
                &mut unexpected_bytes,
                bytes,
                "unexpected incoming",
            )?;
        }
    }

    let (inflight_capture_files, inflight_capture_bytes) = if writer_active {
        (incomplete_capture_files, incomplete_capture_bytes)
    } else {
        (0, 0)
    };
    let (abandoned_capture_files, abandoned_capture_bytes) = if writer_active {
        (0, 0)
    } else {
        (incomplete_capture_files, incomplete_capture_bytes)
    };

    Ok(IncomingMaintenanceStatus {
        incoming_exists: true,
        writer_active,
        incomplete_capture_files,
        incomplete_capture_bytes,
        inflight_capture_files,
        inflight_capture_bytes,
        abandoned_capture_files,
        abandoned_capture_bytes,
        ledger_recovery_files,
        ledger_recovery_bytes,
        unexpected_files,
        unexpected_bytes,
        unexpected_non_file_entries,
        cleanup_on_next_writer_start: !writer_active && incomplete_capture_files > 0,
    })
}

fn purge_abandoned_capture_parts(root: &Path) -> Result<(u64, u64)> {
    let incoming = root.join(".incoming");
    let mut removed_files = 0_u64;
    let mut removed_bytes = 0_u64;

    for entry in fs::read_dir(&incoming)
        .with_context(|| format!("reading incoming directory {}", incoming.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !is_abandoned_capture_part_name(name) {
            continue;
        }

        let path = entry.path();
        let bytes = entry.metadata()?.len();
        fs::remove_file(&path)
            .with_context(|| format!("removing abandoned capture part {}", path.display()))?;
        removed_files = removed_files
            .checked_add(1)
            .context("abandoned capture file count overflow")?;
        removed_bytes = removed_bytes
            .checked_add(bytes)
            .context("abandoned capture byte count overflow")?;
    }

    Ok((removed_files, removed_bytes))
}

pub fn open_raw_ledger_read_only(root: impl AsRef<Path>) -> Result<Connection> {
    open_raw_ledger_connection(root.as_ref(), true)
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
    root: &Path,
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

    let stored = fs::read(path)
        .with_context(|| format!("reading structured response body {}", path.display()))?;
    let bytes = if privacy == PrivacyClass::Private {
        anyhow::ensure!(
            stored.starts_with(PRIVATE_STREAM_MAGIC),
            "private structured response temp object is not MIRRPV02 encrypted"
        );
        let key = load_existing_private_key(root)?;
        decrypt_private_stream_bytes(&key, &stored)?
    } else {
        stored
    };

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

fn private_key_path(root: &Path) -> Result<PathBuf> {
    if let Some(path) = env::var_os("MIRRARIUM_PRIVATE_KEY_FILE") {
        return Ok(PathBuf::from(path));
    }

    // Library callers (tests, fixtures, embeddings) may pass an isolated root
    // directly instead of using Mirrarium's configured application data root.
    // Keep those roots self-contained so independent instances never share a
    // production key implicitly.
    if default_data_root()
        .map(|configured| configured != root)
        .unwrap_or(true)
    {
        return Ok(root.join(".keys/private.key"));
    }

    if let Some(path) = env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(path).join("mirrarium/private.key"));
    }
    if let Some(home) = env::var_os("HOME") {
        return Ok(PathBuf::from(home).join(".config/mirrarium/private.key"));
    }
    Ok(root.join(".keys/private.key"))
}

fn read_private_key(path: &Path) -> Result<[u8; PRIVATE_KEY_BYTES]> {
    let bytes = fs::read(path)
        .with_context(|| format!("reading Mirrarium private key {}", path.display()))?;
    anyhow::ensure!(
        bytes.len() == PRIVATE_KEY_BYTES,
        "Mirrarium private key {} must be exactly {} bytes",
        path.display(),
        PRIVATE_KEY_BYTES
    );
    let mut key = [0_u8; PRIVATE_KEY_BYTES];
    key.copy_from_slice(&bytes);
    Ok(key)
}

fn load_or_create_private_key(root: &Path) -> Result<[u8; PRIVATE_KEY_BYTES]> {
    let path = private_key_path(root)?;
    match read_private_key(&path) {
        Ok(key) => return Ok(key),
        Err(error) if path.exists() => return Err(error),
        Err(_) => {}
    }

    anyhow::ensure!(
        !encrypted_private_objects_exist(root)?,
        "encrypted private objects already exist but Mirrarium key {} is missing; restore that key instead of generating a replacement",
        path.display()
    );

    let parent = path
        .parent()
        .context("Mirrarium private key path has no parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("creating private-key directory {}", parent.display()))?;
    harden_directory(parent)?;

    let mut key = [0_u8; PRIVATE_KEY_BYTES];
    OsRng.fill_bytes(&mut key);
    match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(mut file) => {
            harden_file(&path)?;
            file.write_all(&key)?;
            file.flush()?;
            Ok(key)
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => read_private_key(&path),
        Err(error) => Err(error)
            .with_context(|| format!("creating Mirrarium private key {}", path.display())),
    }
}

fn encrypted_private_objects_exist(root: &Path) -> Result<bool> {
    let objects_root = root.join("private/objects");
    if !objects_root.exists() {
        return Ok(false);
    }

    let mut directories = vec![objects_root];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(&directory)
            .with_context(|| format!("reading private object directory {}", directory.display()))?
        {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                directories.push(entry.path());
                continue;
            }
            if !file_type.is_file() {
                continue;
            }

            let mut file = File::open(entry.path())?;
            let mut prefix = [0_u8; PRIVATE_OBJECT_MAGIC.len()];
            match file.read_exact(&mut prefix) {
                Ok(()) if is_encrypted_private_object(&prefix) => return Ok(true),
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::UnexpectedEof => {}
                Err(error) => return Err(error.into()),
            }
        }
    }

    Ok(false)
}

fn load_existing_private_key(root: &Path) -> Result<[u8; PRIVATE_KEY_BYTES]> {
    let path = private_key_path(root)?;
    read_private_key(&path).with_context(|| {
        format!(
            "private object is encrypted but Mirrarium key {} is unavailable",
            path.display()
        )
    })
}

pub fn private_database_key(
    root: impl AsRef<Path>,
    purpose: &str,
    create_master_if_missing: bool,
) -> Result<[u8; PRIVATE_KEY_BYTES]> {
    anyhow::ensure!(
        !purpose.is_empty()
            && purpose.len() <= 128
            && purpose
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "invalid private database key purpose"
    );

    let root = root.as_ref();
    let master = if create_master_if_missing {
        load_or_create_private_key(root)?
    } else {
        load_existing_private_key(root)?
    };
    let hkdf = Hkdf::<Sha256>::new(Some(b"mirrarium-private-database-v1"), &master);
    let mut derived = [0_u8; PRIVATE_KEY_BYTES];
    hkdf.expand(purpose.as_bytes(), &mut derived)
        .map_err(|_| anyhow::anyhow!("deriving private database key failed"))?;
    Ok(derived)
}

fn sqlcipher_raw_key_literal(key: &[u8; PRIVATE_KEY_BYTES]) -> String {
    let mut hex = String::with_capacity(PRIVATE_KEY_BYTES * 2);
    for byte in key {
        use std::fmt::Write as _;
        write!(&mut hex, "{byte:02x}").expect("writing to String cannot fail");
    }
    format!("x'{hex}'")
}

pub fn apply_private_database_key(
    connection: &Connection,
    root: impl AsRef<Path>,
    purpose: &str,
    create_master_if_missing: bool,
) -> Result<()> {
    let key = private_database_key(root, purpose, create_master_if_missing)?;
    let literal = sqlcipher_raw_key_literal(&key);
    connection
        .execute_batch(&format!("PRAGMA key = \"{literal}\";"))
        .with_context(|| format!("applying SQLCipher key for {purpose}"))?;

    let cipher_version: String = connection
        .pragma_query_value(None, "cipher_version", |row| row.get(0))
        .context("this Mirrarium build does not provide SQLCipher")?;
    anyhow::ensure!(
        !cipher_version.trim().is_empty(),
        "SQLCipher cipher_version is empty"
    );
    connection
        .query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get::<_, i64>(0))
        .with_context(|| format!("verifying SQLCipher key for {purpose}"))?;
    Ok(())
}

fn private_object_aad(hash: &str) -> Vec<u8> {
    let mut aad = b"mirrarium-private-object-v1\0".to_vec();
    aad.extend_from_slice(hash.as_bytes());
    aad
}

struct PrivateStreamWriter {
    writer: BufWriter<File>,
    key: [u8; PRIVATE_KEY_BYTES],
    nonce_prefix: [u8; PRIVATE_STREAM_NONCE_PREFIX_BYTES],
    next_frame: u64,
}

impl PrivateStreamWriter {
    fn new(
        mut writer: BufWriter<File>,
        key: [u8; PRIVATE_KEY_BYTES],
    ) -> Result<Self> {
        let mut nonce_prefix = [0_u8; PRIVATE_STREAM_NONCE_PREFIX_BYTES];
        OsRng.fill_bytes(&mut nonce_prefix);
        writer.write_all(PRIVATE_STREAM_MAGIC)?;
        writer.write_all(&nonce_prefix)?;
        writer.flush()?;
        Ok(Self {
            writer,
            key,
            nonce_prefix,
            next_frame: 0,
        })
    }

    fn write_chunk(&mut self, plaintext: &[u8]) -> Result<()> {
        let sequence = self.next_frame;
        let nonce = private_stream_nonce(&self.nonce_prefix, sequence);
        let aad = private_stream_aad(&self.nonce_prefix, sequence);
        let cipher = XChaCha20Poly1305::new((&self.key).into());
        let ciphertext = cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| anyhow::anyhow!("encrypting private stream frame failed"))?;
        let frame_len: u32 = ciphertext
            .len()
            .try_into()
            .context("private stream frame exceeds u32 length")?;
        self.writer.write_all(&frame_len.to_be_bytes())?;
        self.writer.write_all(&ciphertext)?;
        self.next_frame = self
            .next_frame
            .checked_add(1)
            .context("private stream frame sequence overflow")?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush().context("flushing private stream writer")
    }
}

fn private_stream_nonce(
    prefix: &[u8; PRIVATE_STREAM_NONCE_PREFIX_BYTES],
    sequence: u64,
) -> [u8; PRIVATE_NONCE_BYTES] {
    let mut nonce = [0_u8; PRIVATE_NONCE_BYTES];
    nonce[..PRIVATE_STREAM_NONCE_PREFIX_BYTES].copy_from_slice(prefix);
    nonce[PRIVATE_STREAM_NONCE_PREFIX_BYTES..].copy_from_slice(&sequence.to_be_bytes());
    nonce
}

fn private_stream_aad(
    prefix: &[u8; PRIVATE_STREAM_NONCE_PREFIX_BYTES],
    sequence: u64,
) -> Vec<u8> {
    let mut aad = b"mirrarium-private-stream-v2\0".to_vec();
    aad.extend_from_slice(prefix);
    aad.extend_from_slice(&sequence.to_be_bytes());
    aad
}

fn decrypt_private_stream_bytes(
    key: &[u8; PRIVATE_KEY_BYTES],
    envelope: &[u8],
) -> Result<Vec<u8>> {
    let header_len = PRIVATE_STREAM_MAGIC.len() + PRIVATE_STREAM_NONCE_PREFIX_BYTES;
    anyhow::ensure!(
        envelope.len() >= header_len,
        "encrypted private stream envelope is truncated"
    );
    anyhow::ensure!(
        envelope.starts_with(PRIVATE_STREAM_MAGIC),
        "private stream envelope magic mismatch"
    );

    let mut nonce_prefix = [0_u8; PRIVATE_STREAM_NONCE_PREFIX_BYTES];
    nonce_prefix.copy_from_slice(
        &envelope[PRIVATE_STREAM_MAGIC.len()..header_len],
    );

    let cipher = XChaCha20Poly1305::new(key.into());
    let mut plaintext = Vec::new();
    let mut cursor = header_len;
    let mut sequence = 0_u64;

    while cursor < envelope.len() {
        anyhow::ensure!(
            envelope.len() - cursor >= 4,
            "encrypted private stream frame length is truncated"
        );
        let frame_len = u32::from_be_bytes(
            envelope[cursor..cursor + 4]
                .try_into()
                .expect("four-byte frame length slice"),
        ) as usize;
        cursor += 4;
        anyhow::ensure!(
            frame_len >= PRIVATE_STREAM_TAG_BYTES,
            "encrypted private stream frame is too short"
        );
        let frame_end = cursor
            .checked_add(frame_len)
            .context("private stream frame length overflow")?;
        anyhow::ensure!(
            frame_end <= envelope.len(),
            "encrypted private stream frame is truncated"
        );

        let nonce = private_stream_nonce(&nonce_prefix, sequence);
        let aad = private_stream_aad(&nonce_prefix, sequence);
        let frame = cipher
            .decrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: &envelope[cursor..frame_end],
                    aad: &aad,
                },
            )
            .map_err(|_| anyhow::anyhow!("decrypting private stream frame failed"))?;
        plaintext.extend_from_slice(&frame);
        cursor = frame_end;
        sequence = sequence
            .checked_add(1)
            .context("private stream frame sequence overflow")?;
    }

    Ok(plaintext)
}

fn rewrite_private_stream_file(
    path: &Path,
    key: [u8; PRIVATE_KEY_BYTES],
    plaintext: &[u8],
) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("opening private stream rewrite {}", path.display()))?;
    let mut writer = PrivateStreamWriter::new(BufWriter::new(file), key)?;
    for chunk in plaintext.chunks(PRIVATE_STREAM_REWRITE_CHUNK_BYTES) {
        writer.write_chunk(chunk)?;
    }
    writer.flush()?;
    harden_file(path)
}

fn is_encrypted_private_object(bytes: &[u8]) -> bool {
    bytes.starts_with(PRIVATE_OBJECT_MAGIC) || bytes.starts_with(PRIVATE_STREAM_MAGIC)
}

fn encrypt_private_object_bytes(
    key: &[u8; PRIVATE_KEY_BYTES],
    hash: &str,
    plaintext: &[u8],
) -> Result<Vec<u8>> {
    let cipher = XChaCha20Poly1305::new(key.into());
    let mut nonce = [0_u8; PRIVATE_NONCE_BYTES];
    OsRng.fill_bytes(&mut nonce);
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &private_object_aad(hash),
            },
        )
        .map_err(|_| anyhow::anyhow!("encrypting private object failed"))?;

    let mut envelope = Vec::with_capacity(
        PRIVATE_OBJECT_MAGIC.len() + PRIVATE_NONCE_BYTES + ciphertext.len(),
    );
    envelope.extend_from_slice(PRIVATE_OBJECT_MAGIC);
    envelope.extend_from_slice(&nonce);
    envelope.extend_from_slice(&ciphertext);
    Ok(envelope)
}

fn decrypt_private_object_bytes(
    key: &[u8; PRIVATE_KEY_BYTES],
    hash: &str,
    envelope: &[u8],
) -> Result<Vec<u8>> {
    anyhow::ensure!(
        envelope.len() >= PRIVATE_OBJECT_MAGIC.len() + PRIVATE_NONCE_BYTES + 16,
        "encrypted private object envelope is truncated"
    );
    anyhow::ensure!(
        envelope.starts_with(PRIVATE_OBJECT_MAGIC),
        "private object envelope magic mismatch"
    );

    let nonce_start = PRIVATE_OBJECT_MAGIC.len();
    let nonce_end = nonce_start + PRIVATE_NONCE_BYTES;
    let cipher = XChaCha20Poly1305::new(key.into());
    cipher
        .decrypt(
            XNonce::from_slice(&envelope[nonce_start..nonce_end]),
            Payload {
                msg: &envelope[nonce_end..],
                aad: &private_object_aad(hash),
            },
        )
        .map_err(|_| anyhow::anyhow!("decrypting private object failed"))
}

pub fn read_verified_object(
    root: impl AsRef<Path>,
    storage_class: &str,
    hash: &str,
) -> Result<Vec<u8>> {
    let class = PrivacyClass::parse(storage_class)
        .with_context(|| format!("invalid storage class {storage_class:?}"))?;
    anyhow::ensure!(
        hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "invalid SHA-256 object key {hash:?}"
    );

    let relative_path = object_relative_path(class, hash);
    let root = root.as_ref();
    let path = root.join(&relative_path);
    let stored = fs::read(&path)
        .with_context(|| format!("reading stored object {}", path.display()))?;
    let bytes = if class == PrivacyClass::Private && stored.starts_with(PRIVATE_OBJECT_MAGIC) {
        let key = load_existing_private_key(root)?;
        decrypt_private_object_bytes(&key, hash, &stored)?
    } else if class == PrivacyClass::Private && stored.starts_with(PRIVATE_STREAM_MAGIC) {
        let key = load_existing_private_key(root)?;
        decrypt_private_stream_bytes(&key, &stored)?
    } else {
        stored
    };
    let actual_hash = sha256_hex(&bytes);
    anyhow::ensure!(
        actual_hash == hash,
        "stored object hash verification failed for {storage_class}/{hash}"
    );
    Ok(bytes)
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
        store
            .record_cache_replay_outcome(
                "https://cdn.oaistatic.com/assets/app.js",
                "Script",
                "hit",
                7,
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
        assert_eq!(count, 3);
        assert_eq!(bytes, 49);
    }

    #[test]
    fn private_revalidation_telemetry_is_aggregate_only() {
        let directory = tempdir().unwrap();
        let store = CaptureStore::open(directory.path()).unwrap();

        store
            .record_private_revalidation_outcome("not_modified", 128)
            .unwrap();
        store
            .record_private_revalidation_outcome("refreshed", 0)
            .unwrap();
        store
            .record_private_revalidation_outcome("fulfill_error", 0)
            .unwrap();

        assert!(store
            .record_private_revalidation_outcome("refreshed", 1)
            .is_err());
        assert!(store
            .record_private_revalidation_outcome("unknown", 0)
            .is_err());

        let (count, bytes): (i64, i64) = store
            .connection
            .query_row(
                "SELECT COUNT(*), SUM(body_bytes) FROM private_revalidation_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 3);
        assert_eq!(bytes, 128);
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
    fn missing_master_key_is_not_replaced_when_encrypted_private_objects_exist() {
        let directory = tempdir().unwrap();
        let plaintext = b"existing encrypted bytes";
        let hash = sha256_hex(plaintext);
        let path = directory
            .path()
            .join(object_relative_path(PrivacyClass::Private, &hash));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let envelope =
            encrypt_private_object_bytes(&[9_u8; PRIVATE_KEY_BYTES], &hash, plaintext).unwrap();
        fs::write(&path, envelope).unwrap();

        let key_path = private_key_path(directory.path()).unwrap();
        assert!(!key_path.exists());
        let error = load_or_create_private_key(directory.path()).unwrap_err();
        assert!(error.to_string().contains("restore that key"));
        assert!(!key_path.exists());
    }

    #[test]
    fn writable_store_excludes_second_writer_but_allows_read_only_inspection() {
        let directory = tempdir().unwrap();
        let writer = CaptureStore::open(directory.path()).unwrap();

        let error = CaptureStore::open(directory.path())
            .err()
            .expect("second writable store should be rejected");
        assert!(error.to_string().contains("another Mirrarium writer"));

        let reader = CaptureStore::open_read_only(directory.path()).unwrap();
        assert_eq!(reader.stats().unwrap().captures, 0);
        drop(reader);
        drop(writer);

        CaptureStore::open(directory.path()).unwrap();
    }

    #[test]
    fn abandoned_capture_part_purge_preserves_ledger_recovery_files() {
        let directory = tempdir().unwrap();
        let incoming = directory.path().join(".incoming");
        fs::create_dir_all(&incoming).unwrap();
        let hash = "a".repeat(64);
        let response = incoming.join(format!("{hash}.part"));
        let request = incoming.join(format!("{hash}.request.part"));
        let object_migration = incoming.join(format!("{hash}.private-migration.part"));
        let ledger_target = incoming.join("ledger.sqlite3.encrypted.part");
        let ledger_backup = incoming.join("ledger.sqlite3.plaintext-backup");

        fs::write(&response, b"legacy plaintext response").unwrap();
        fs::write(&request, b"encrypted request temp").unwrap();
        fs::write(&object_migration, b"encrypted object migration temp").unwrap();
        fs::write(&ledger_target, b"ledger target").unwrap();
        fs::write(&ledger_backup, b"ledger backup").unwrap();

        let (files, bytes) = purge_abandoned_capture_parts(directory.path()).unwrap();
        assert_eq!(files, 3);
        assert!(bytes > 0);
        assert!(!response.exists());
        assert!(!request.exists());
        assert!(!object_migration.exists());
        assert!(ledger_target.exists());
        assert!(ledger_backup.exists());
    }

    #[test]
    fn incoming_maintenance_status_distinguishes_abandoned_and_recovery_artifacts() {
        let directory = tempdir().unwrap();
        let incoming = directory.path().join(".incoming");
        fs::create_dir_all(&incoming).unwrap();
        let hash = "b".repeat(64);

        fs::write(incoming.join(format!("{hash}.part")), b"response").unwrap();
        fs::write(incoming.join(format!("{hash}.request.part")), b"request").unwrap();
        fs::write(
            incoming.join(format!("{hash}.private-migration.part")),
            b"migration",
        )
        .unwrap();
        fs::write(
            incoming.join("ledger.sqlite3.encrypted.part"),
            b"ledger target",
        )
        .unwrap();
        fs::write(
            incoming.join("ledger.sqlite3.plaintext-backup"),
            b"ledger backup",
        )
        .unwrap();
        fs::write(incoming.join("unexpected.bin"), b"unknown").unwrap();
        fs::create_dir(incoming.join("unexpected-directory")).unwrap();

        let status = incoming_maintenance_status(directory.path()).unwrap();
        assert!(status.incoming_exists);
        assert!(!status.writer_active);
        assert_eq!(status.incomplete_capture_files, 3);
        assert_eq!(status.abandoned_capture_files, 3);
        assert_eq!(status.inflight_capture_files, 0);
        assert_eq!(status.ledger_recovery_files, 2);
        assert_eq!(status.unexpected_files, 1);
        assert_eq!(status.unexpected_non_file_entries, 1);
        assert!(status.cleanup_on_next_writer_start);
    }

    #[test]
    fn incoming_maintenance_status_marks_parts_inflight_while_writer_is_live() {
        let directory = tempdir().unwrap();
        let writer = CaptureStore::open(directory.path()).unwrap();
        let incoming = directory.path().join(".incoming");
        let hash = "c".repeat(64);
        fs::write(incoming.join(format!("{hash}.part")), b"active").unwrap();

        let live = incoming_maintenance_status(directory.path()).unwrap();
        assert!(live.writer_active);
        assert_eq!(live.incomplete_capture_files, 1);
        assert_eq!(live.inflight_capture_files, 1);
        assert_eq!(live.abandoned_capture_files, 0);
        assert!(!live.cleanup_on_next_writer_start);

        drop(writer);

        let abandoned = incoming_maintenance_status(directory.path()).unwrap();
        assert!(!abandoned.writer_active);
        assert_eq!(abandoned.inflight_capture_files, 0);
        assert_eq!(abandoned.abandoned_capture_files, 1);
        assert!(abandoned.cleanup_on_next_writer_start);
    }

    #[test]
    fn read_only_raw_ledger_uses_bounded_busy_timeout() {
        let directory = tempdir().unwrap();
        let writer = CaptureStore::open(directory.path()).unwrap();
        drop(writer);

        let connection = open_raw_ledger_read_only(directory.path()).unwrap();
        let busy_timeout_ms: u64 = connection
            .pragma_query_value(None, "busy_timeout", |row| row.get(0))
            .unwrap();
        assert_eq!(busy_timeout_ms, READ_ONLY_LEDGER_BUSY_TIMEOUT_MS);
    }

    #[test]
    fn read_only_capture_store_does_not_mutate_live_ledger() {
        let directory = tempdir().unwrap();
        let mut writer = CaptureStore::open(directory.path()).unwrap();
        let first_metadata = metadata(
            "read-only-live",
            "https://chatgpt.com/backend-api/read-only",
            "Fetch",
        );
        writer.begin(first_metadata).unwrap();
        writer
            .append_chunk("read-only-live", 0, &BASE64.encode(b"{\"version\":1}"))
            .unwrap();
        writer.finish("read-only-live", None, None).unwrap();

        let reader = CaptureStore::open_read_only(directory.path()).unwrap();
        assert_eq!(reader.recent_captures(10).unwrap().len(), 1);

        let second_metadata = metadata(
            "read-only-live-2",
            "https://chatgpt.com/backend-api/read-only",
            "Fetch",
        );
        writer.begin(second_metadata).unwrap();
        writer
            .append_chunk("read-only-live-2", 0, &BASE64.encode(b"{\"version\":2}"))
            .unwrap();
        writer.finish("read-only-live-2", None, None).unwrap();

        assert_eq!(CaptureStore::open_read_only(directory.path()).unwrap().recent_captures(10).unwrap().len(), 2);
    }

    #[test]
    fn new_capture_store_creates_encrypted_raw_ledger() {
        let directory = tempdir().unwrap();
        let store = CaptureStore::open(directory.path()).unwrap();
        let ledger = directory.path().join("ledger.sqlite3");
        let raw = fs::read(&ledger).unwrap();
        assert_ne!(
            raw.get(..SQLITE_PLAINTEXT_HEADER.len()),
            Some(SQLITE_PLAINTEXT_HEADER.as_slice())
        );

        let status = store.private_storage_status().unwrap();
        assert!(status.ledger_exists);
        assert!(status.ledger_encrypted);
        assert!(!status.ledger_plaintext_legacy);

        let read_only = open_raw_ledger_read_only(directory.path()).unwrap();
        let table_count: i64 = read_only
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(table_count > 0);
    }

    #[test]
    fn legacy_plaintext_raw_ledger_remains_readable() {
        let directory = tempdir().unwrap();
        let ledger = directory.path().join("ledger.sqlite3");
        let plain = Connection::open(&ledger).unwrap();
        plain
            .execute_batch("CREATE TABLE legacy_probe (value TEXT); INSERT INTO legacy_probe VALUES ('ok');")
            .unwrap();
        drop(plain);

        assert!(database_has_plaintext_sqlite_header(&ledger).unwrap());
        let read_only = open_raw_ledger_read_only(directory.path()).unwrap();
        let value: String = read_only
            .query_row("SELECT value FROM legacy_probe", [], |row| row.get(0))
            .unwrap();
        assert_eq!(value, "ok");
    }

    #[test]
    fn private_database_keys_are_stable_and_domain_separated() {
        let directory = tempdir().unwrap();
        let one = private_database_key(directory.path(), "corpus-sqlcipher-v1", true).unwrap();
        let two = private_database_key(directory.path(), "corpus-sqlcipher-v1", false).unwrap();
        let other = private_database_key(directory.path(), "ledger-sqlcipher-v1", false).unwrap();

        assert_eq!(one, two);
        assert_ne!(one, other);
        assert!(private_database_key(directory.path(), "", false).is_err());
        assert!(private_database_key(directory.path(), "bad purpose", false).is_err());
    }

    #[test]
    fn private_stream_envelope_round_trips_multiple_frames() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("stream-v2");
        let file = File::create(&path).unwrap();
        let key = [0x5a_u8; PRIVATE_KEY_BYTES];
        let mut writer = PrivateStreamWriter::new(BufWriter::new(file), key).unwrap();
        writer.write_chunk(b"alpha-").unwrap();
        writer.write_chunk(b"beta-").unwrap();
        writer.write_chunk(b"gamma").unwrap();
        writer.flush().unwrap();
        drop(writer);

        let raw = fs::read(&path).unwrap();
        assert!(raw.starts_with(PRIVATE_STREAM_MAGIC));
        assert!(!String::from_utf8_lossy(&raw).contains("alpha-beta-gamma"));
        assert_eq!(
            decrypt_private_stream_bytes(&key, &raw).unwrap(),
            b"alpha-beta-gamma"
        );
    }

    #[test]
    fn private_stream_envelope_rejects_wrong_key_tamper_and_truncation() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("stream-v2-errors");
        let file = File::create(&path).unwrap();
        let key = [0x31_u8; PRIVATE_KEY_BYTES];
        let mut writer = PrivateStreamWriter::new(BufWriter::new(file), key).unwrap();
        writer.write_chunk(b"first").unwrap();
        writer.write_chunk(b"second").unwrap();
        writer.flush().unwrap();
        drop(writer);

        let raw = fs::read(&path).unwrap();
        assert!(decrypt_private_stream_bytes(&[0x32_u8; PRIVATE_KEY_BYTES], &raw).is_err());

        let mut tampered = raw.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 0x01;
        assert!(decrypt_private_stream_bytes(&key, &tampered).is_err());

        assert!(decrypt_private_stream_bytes(&key, &raw[..raw.len() - 1]).is_err());
    }

    #[test]
    fn private_object_envelope_round_trips_and_binds_hash() {
        let key = [7_u8; PRIVATE_KEY_BYTES];
        let plaintext = b"private fixture bytes";
        let hash = sha256_hex(plaintext);
        let envelope = encrypt_private_object_bytes(&key, &hash, plaintext).unwrap();

        assert!(envelope.starts_with(PRIVATE_OBJECT_MAGIC));
        assert_ne!(envelope.as_slice(), plaintext);
        assert!(!String::from_utf8_lossy(&envelope).contains("private fixture bytes"));

        let decoded = decrypt_private_object_bytes(&key, &hash, &envelope).unwrap();
        assert_eq!(decoded, plaintext);

        let wrong_hash = sha256_hex(b"different plaintext");
        assert!(decrypt_private_object_bytes(&key, &wrong_hash, &envelope).is_err());
    }

    #[test]
    fn private_object_envelope_rejects_wrong_key_and_truncation() {
        let key = [3_u8; PRIVATE_KEY_BYTES];
        let wrong_key = [4_u8; PRIVATE_KEY_BYTES];
        let plaintext = b"private fixture bytes";
        let hash = sha256_hex(plaintext);
        let envelope = encrypt_private_object_bytes(&key, &hash, plaintext).unwrap();

        assert!(decrypt_private_object_bytes(&wrong_key, &hash, &envelope).is_err());
        assert!(decrypt_private_object_bytes(
            &key,
            &hash,
            &envelope[..PRIVATE_OBJECT_MAGIC.len() + PRIVATE_NONCE_BYTES]
        )
        .is_err());
    }

    #[test]
    fn verified_object_reader_keeps_legacy_private_plaintext_compatible() {
        let directory = tempdir().unwrap();
        let plaintext = b"legacy private bytes";
        let hash = sha256_hex(plaintext);
        let path = directory
            .path()
            .join(object_relative_path(PrivacyClass::Private, &hash));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, plaintext).unwrap();

        assert_eq!(
            read_verified_object(directory.path(), "private", &hash).unwrap(),
            plaintext
        );
    }

    #[test]
    fn verified_object_reader_rejects_corruption() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        let body = BASE64.encode(b"verified object");
        let mut item = metadata(
            "verified-object",
            "https://chatgpt.com/backend-api/test",
            "Fetch",
        );
        item.mime_type = "text/plain".to_owned();
        store.begin(item).unwrap();
        store.append_chunk("verified-object", 0, &body).unwrap();
        store.finish("verified-object", Some(15), None).unwrap();

        let capture = store.recent_captures(1).unwrap().pop().unwrap();
        let hash = capture.body_hash.unwrap();
        assert_eq!(
            read_verified_object(directory.path(), "private", &hash).unwrap(),
            b"verified object"
        );

        let path = directory
            .path()
            .join(object_relative_path(PrivacyClass::Private, &hash));
        fs::write(path, b"tampered").unwrap();
        assert!(read_verified_object(directory.path(), "private", &hash).is_err());
    }

    #[test]
    fn private_storage_migration_encrypts_legacy_objects_idempotently() {
        let directory = tempdir().unwrap();
        let store = CaptureStore::open(directory.path()).unwrap();
        let plaintext = b"legacy migration bytes";
        let hash = sha256_hex(plaintext);
        let relative_path = object_relative_path(PrivacyClass::Private, &hash);
        let path = directory.path().join(&relative_path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, plaintext).unwrap();
        store
            .connection
            .execute(
                "INSERT INTO objects (storage_class, hash, bytes, relative_path, created_at_ms) VALUES ('private', ?1, ?2, ?3, 1)",
                params![
                    hash,
                    plaintext.len() as i64,
                    relative_path.to_string_lossy().to_string()
                ],
            )
            .unwrap();

        let before = store.private_storage_status().unwrap();
        assert_eq!(before.private_objects, 1);
        assert_eq!(before.legacy_plaintext_private_objects, 1);
        assert!(before.migration_needed);

        let report = store.migrate_private_objects().unwrap();
        assert_eq!(report.migrated_objects, 1);
        assert_eq!(report.migrated_body_bytes, plaintext.len() as u64);

        let raw = fs::read(&path).unwrap();
        assert!(raw.starts_with(PRIVATE_OBJECT_MAGIC));
        assert_eq!(
            read_verified_object(directory.path(), "private", &hash).unwrap(),
            plaintext
        );

        let after = store.private_storage_status().unwrap();
        assert_eq!(after.encrypted_private_objects, 1);
        assert_eq!(after.legacy_plaintext_private_objects, 0);
        assert!(!after.migration_needed);

        let second = store.migrate_private_objects().unwrap();
        assert_eq!(second.migrated_objects, 0);
        assert_eq!(second.already_encrypted_objects, 1);
    }

    #[test]
    fn fully_encrypted_migration_is_read_only_while_writer_is_live() {
        let directory = tempdir().unwrap();
        let mut writer = CaptureStore::open(directory.path()).unwrap();
        let mut item = metadata(
            "live-migration-noop",
            "https://chatgpt.com/backend-api/conversation/live",
            "Fetch",
        );
        item.mime_type = "text/plain".to_owned();
        writer.begin(item).unwrap();
        writer
            .append_chunk(
                "live-migration-noop",
                0,
                &BASE64.encode(b"already encrypted"),
            )
            .unwrap();
        writer.finish("live-migration-noop", None, None).unwrap();

        let report = migrate_private_storage(directory.path()).unwrap();
        assert_eq!(report.migrated_objects, 0);
        assert_eq!(report.already_encrypted_objects, 1);
        assert!(!report.ledger_migrated);
        assert!(report.ledger_already_encrypted);
    }

    #[test]
    fn private_storage_migration_exports_plaintext_ledger_to_sqlcipher() {
        let directory = tempdir().unwrap();
        let ledger = directory.path().join("ledger.sqlite3");
        let plain = Connection::open(&ledger).unwrap();
        plain
            .execute_batch(
                "PRAGMA user_version = 7;
                 CREATE TABLE probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO probe (value) VALUES ('alpha'), ('beta');",
            )
            .unwrap();
        drop(plain);
        assert!(database_has_plaintext_sqlite_header(&ledger).unwrap());

        let report = migrate_private_storage(directory.path()).unwrap();
        assert!(report.ledger_migrated);
        assert!(!report.ledger_already_encrypted);
        assert!(report.ledger_plaintext_bytes > 0);
        assert!(!database_has_plaintext_sqlite_header(&ledger).unwrap());

        let encrypted = open_raw_ledger_read_only(directory.path()).unwrap();
        let values: i64 = encrypted
            .query_row("SELECT COUNT(*) FROM probe", [], |row| row.get(0))
            .unwrap();
        assert_eq!(values, 2);
        let version: i64 = encrypted
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 7);
        drop(encrypted);

        let second = migrate_private_storage(directory.path()).unwrap();
        assert!(!second.ledger_migrated);
        assert!(second.ledger_already_encrypted);
        assert_eq!(second.ledger_plaintext_bytes, 0);
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
    fn private_response_incoming_file_is_encrypted_from_first_chunk() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        let capture_id = "encrypted-incoming";
        let mut item = metadata(
            capture_id,
            "https://chatgpt.com/backend-api/conversation/incoming",
            "Fetch",
        );
        item.mime_type = "text/plain".to_owned();
        store.begin(item).unwrap();

        let temp_path = directory
            .path()
            .join(".incoming")
            .join(format!("{}.part", sha256_hex(capture_id.as_bytes())));
        let initial = fs::read(&temp_path).unwrap();
        assert!(initial.starts_with(PRIVATE_STREAM_MAGIC));

        let plaintext = b"private-incoming-plaintext";
        store
            .append_chunk(capture_id, 0, &BASE64.encode(plaintext))
            .unwrap();
        store
            .in_flight
            .get_mut(capture_id)
            .unwrap()
            .writer
            .flush()
            .unwrap();

        let raw = fs::read(&temp_path).unwrap();
        assert!(raw.starts_with(PRIVATE_STREAM_MAGIC));
        assert!(!raw.windows(plaintext.len()).any(|window| window == plaintext));
        let key = load_existing_private_key(directory.path()).unwrap();
        assert_eq!(decrypt_private_stream_bytes(&key, &raw).unwrap(), plaintext);

        store
            .finish(capture_id, Some(plaintext.len() as u64), None)
            .unwrap();
        let capture = store.recent_captures(1).unwrap().pop().unwrap();
        let hash = capture.body_hash.unwrap();
        let final_path = directory
            .path()
            .join(object_relative_path(PrivacyClass::Private, &hash));
        let final_raw = fs::read(final_path).unwrap();
        assert!(final_raw.starts_with(PRIVATE_STREAM_MAGIC));
        assert_eq!(
            read_verified_object(directory.path(), "private", &hash).unwrap(),
            plaintext
        );
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
        let raw = fs::read(&path).unwrap();
        assert!(raw.starts_with(PRIVATE_STREAM_MAGIC));
        assert!(!String::from_utf8_lossy(&raw).contains("top-secret"));
        let stored = String::from_utf8(
            read_verified_object(directory.path(), "private", &hash).unwrap(),
        )
        .unwrap();

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
        let raw = fs::read(&path).unwrap();
        assert!(raw.starts_with(PRIVATE_STREAM_MAGIC));
        let stored = String::from_utf8(
            read_verified_object(directory.path(), "private", &hash).unwrap(),
        )
        .unwrap();

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
        let raw = fs::read(&path).unwrap();
        assert!(raw.starts_with(PRIVATE_OBJECT_MAGIC));
        assert!(!String::from_utf8_lossy(&raw).contains("hello from request body"));
        let persisted = String::from_utf8(
            read_verified_object(directory.path(), "private", &hash).unwrap(),
        )
        .unwrap();
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
