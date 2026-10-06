use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use mirrarium_store::{private_database_key, read_verified_object};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, Transaction};
use serde::Serialize;
use serde_json::Value;
use url::Url;

const CORPUS_SCHEMA_VERSION: i64 = 1;

#[derive(Debug, Clone, Serialize)]
pub struct CorpusStats {
    pub stream_captures: u64,
    pub stream_events: u64,
    pub json_stream_events: u64,
    pub stream_message_revisions: u64,
    pub conversation_snapshots: u64,
    pub message_observations: u64,
    pub stream_reconstructions: u64,
    pub attachment_observations: u64,
    pub attachment_downloads: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AttachmentObservationView {
    pub capture_id: String,
    pub sequence: u64,
    pub conversation_id: Option<String>,
    pub message_id: Option<String>,
    pub attachment_id: Option<String>,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub size_bytes: Option<u64>,
    pub sanitized_url: Option<String>,
    pub source_url: String,
    pub json_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AttachmentDownloadView {
    pub download_capture_id: String,
    pub source_url: String,
    pub mime_type: String,
    pub privacy_class: String,
    pub body_hash: String,
    pub body_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AttachmentView {
    pub observation: AttachmentObservationView,
    pub downloads: Vec<AttachmentDownloadView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConversationSummary {
    pub conversation_id: String,
    pub title: Option<String>,
    pub snapshot_count: u64,
    pub message_observation_count: u64,
    pub stream_reconstruction_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageObservationView {
    pub capture_id: String,
    pub source_kind: String,
    pub sequence: u64,
    pub conversation_id: Option<String>,
    pub message_id: Option<String>,
    pub role: Option<String>,
    pub content_text: Option<String>,
    pub source_url: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamMessageRevisionView {
    pub capture_id: String,
    pub sequence: u64,
    pub conversation_id: String,
    pub message_id: String,
    pub parent_id: Option<String>,
    pub role: Option<String>,
    pub content_text: Option<String>,
    pub source_url: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamReconstructionView {
    pub capture_id: String,
    pub conversation_id: String,
    pub message_id: Option<String>,
    pub source_url: String,
    pub text: String,
    pub fragment_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConversationView {
    pub summary: ConversationSummary,
    pub messages: Vec<MessageObservationView>,
    pub streams: Vec<StreamReconstructionView>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CanonicalMessageView {
    pub message_id: Option<String>,
    pub parent_id: Option<String>,
    pub role: Option<String>,
    pub content_text: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CanonicalConversationView {
    pub conversation_id: String,
    pub title: Option<String>,
    pub basis_capture_id: String,
    pub basis_source_url: String,
    pub basis_source_body_hash: String,
    pub basis_kind: String,
    pub current_node: Option<String>,
    pub messages: Vec<CanonicalMessageView>,
    pub linked_streams: Vec<StreamReconstructionView>,
    pub unlinked_streams: Vec<StreamReconstructionView>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SseEvent {
    event_name: Option<String>,
    data: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MessageObservation {
    conversation_id: Option<String>,
    message_id: Option<String>,
    role: Option<String>,
    content_text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConversationExtraction {
    conversation_id: String,
    title: Option<String>,
    messages: Vec<MessageObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AttachmentObservation {
    conversation_id: Option<String>,
    message_id: Option<String>,
    attachment_id: Option<String>,
    file_name: Option<String>,
    mime_type: Option<String>,
    size_bytes: Option<u64>,
    sanitized_url: Option<String>,
    json_path: String,
}

#[derive(Debug, Clone)]
struct DownloadSource {
    capture_id: String,
    source_url: String,
    mime_type: String,
    privacy_class: String,
    body_hash: String,
    body_bytes: u64,
}

#[derive(Debug, Clone)]
struct StreamRevisionCandidate {
    message_id: String,
    parent_id: Option<String>,
    role: Option<String>,
    content_text: Option<String>,
    valid: bool,
}

#[derive(Debug, Default)]
struct StreamAccumulator {
    text: String,
    fragment_count: u64,
    message_id: Option<String>,
    linkable: bool,
}

pub fn rebuild(raw_root: impl AsRef<Path>) -> Result<CorpusStats> {
    let raw_root = raw_root.as_ref();
    let raw_database = raw_root.join("ledger.sqlite3");
    anyhow::ensure!(
        raw_database.is_file(),
        "raw ledger does not exist: {}",
        raw_database.display()
    );

    let derived_root = raw_root.join("derived");
    fs::create_dir_all(&derived_root)
        .with_context(|| format!("creating {}", derived_root.display()))?;
    harden_directory(&derived_root)?;

    let corpus_database = derived_root.join("corpus.sqlite3");
    remove_corpus_database_files(&corpus_database)?;
    let mut corpus = Connection::open(&corpus_database)
        .with_context(|| format!("opening {}", corpus_database.display()))?;
    apply_corpus_database_key(&corpus, raw_root, true)?;
    harden_file(&corpus_database)?;

    corpus.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA foreign_keys = ON;

        DROP TABLE IF EXISTS attachment_downloads;
        DROP TABLE IF EXISTS attachment_observations;
        DROP TABLE IF EXISTS message_observations;
        DROP TABLE IF EXISTS conversation_snapshots;
        DROP TABLE IF EXISTS stream_reconstructions;
        DROP TABLE IF EXISTS stream_message_revisions;
        DROP TABLE IF EXISTS stream_events;
        DROP TABLE IF EXISTS stream_captures;

        CREATE TABLE stream_captures (
            capture_id TEXT PRIMARY KEY,
            source_url TEXT NOT NULL,
            privacy_class TEXT NOT NULL,
            source_body_hash TEXT NOT NULL,
            event_count INTEGER NOT NULL
        );

        CREATE TABLE stream_events (
            capture_id TEXT NOT NULL
                REFERENCES stream_captures(capture_id) ON DELETE CASCADE,
            sequence INTEGER NOT NULL,
            event_name TEXT,
            data TEXT NOT NULL,
            json_valid INTEGER NOT NULL,
            PRIMARY KEY (capture_id, sequence)
        );

        CREATE TABLE stream_message_revisions (
            capture_id TEXT NOT NULL
                REFERENCES stream_captures(capture_id) ON DELETE CASCADE,
            sequence INTEGER NOT NULL,
            conversation_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            parent_id TEXT,
            role TEXT,
            content_text TEXT,
            source_url TEXT NOT NULL,
            PRIMARY KEY (capture_id, sequence, message_id)
        );

        CREATE INDEX stream_message_revisions_conversation_idx
            ON stream_message_revisions(conversation_id);
        CREATE INDEX stream_message_revisions_message_idx
            ON stream_message_revisions(message_id);

        CREATE TABLE conversation_snapshots (
            capture_id TEXT PRIMARY KEY,
            conversation_id TEXT NOT NULL,
            title TEXT,
            source_url TEXT NOT NULL,
            privacy_class TEXT NOT NULL,
            source_body_hash TEXT NOT NULL
        );

        CREATE TABLE message_observations (
            capture_id TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            conversation_id TEXT,
            message_id TEXT,
            role TEXT,
            content_text TEXT,
            source_url TEXT NOT NULL,
            PRIMARY KEY (capture_id, source_kind, sequence)
        );

        CREATE INDEX message_observations_conversation_idx
            ON message_observations(conversation_id);
        CREATE INDEX message_observations_message_idx
            ON message_observations(message_id);

        CREATE TABLE attachment_observations (
            capture_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            conversation_id TEXT,
            message_id TEXT,
            attachment_id TEXT,
            file_name TEXT,
            mime_type TEXT,
            size_bytes INTEGER,
            sanitized_url TEXT,
            url_identity TEXT,
            source_url TEXT NOT NULL,
            json_path TEXT NOT NULL,
            PRIMARY KEY (capture_id, sequence)
        );

        CREATE INDEX attachment_observations_conversation_idx
            ON attachment_observations(conversation_id);
        CREATE INDEX attachment_observations_message_idx
            ON attachment_observations(message_id);
        CREATE INDEX attachment_observations_url_idx
            ON attachment_observations(url_identity);

        CREATE TABLE attachment_downloads (
            attachment_capture_id TEXT NOT NULL,
            attachment_sequence INTEGER NOT NULL,
            download_capture_id TEXT NOT NULL,
            source_url TEXT NOT NULL,
            mime_type TEXT NOT NULL,
            privacy_class TEXT NOT NULL,
            body_hash TEXT NOT NULL,
            body_bytes INTEGER NOT NULL,
            PRIMARY KEY (
                attachment_capture_id,
                attachment_sequence,
                download_capture_id
            ),
            FOREIGN KEY (attachment_capture_id, attachment_sequence)
                REFERENCES attachment_observations(capture_id, sequence)
                ON DELETE CASCADE
        );

        CREATE TABLE stream_reconstructions (
            capture_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            message_id TEXT,
            source_url TEXT NOT NULL,
            text TEXT NOT NULL,
            fragment_count INTEGER NOT NULL,
            PRIMARY KEY (capture_id, conversation_id)
        );
        "#,
    )?;
    corpus.pragma_update(None, "user_version", CORPUS_SCHEMA_VERSION)?;

    let raw = Connection::open_with_flags(
        &raw_database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening raw ledger {}", raw_database.display()))?;

    let stream_sources = collect_sources(
        &raw,
        r#"
        SELECT capture_id, url, privacy_class, body_hash
        FROM captures
        WHERE body_hash IS NOT NULL
          AND lower(mime_type) LIKE 'text/event-stream%'
        ORDER BY captured_at_ms, capture_id
        "#,
    )?;

    let json_sources = collect_sources(
        &raw,
        r#"
        SELECT capture_id, url, privacy_class, body_hash
        FROM captures
        WHERE body_hash IS NOT NULL
          AND lower(mime_type) LIKE '%json%'
        ORDER BY captured_at_ms, capture_id
        "#,
    )?;

    let download_sources = collect_download_sources(&raw)?;

    let transaction = corpus.transaction()?;

    for (capture_id, source_url, privacy_class, body_hash) in stream_sources {
        derive_stream_capture(
            &transaction,
            raw_root,
            &capture_id,
            &source_url,
            &privacy_class,
            &body_hash,
        )?;
    }

    for (capture_id, source_url, privacy_class, body_hash) in json_sources {
        derive_json_capture(
            &transaction,
            raw_root,
            &capture_id,
            &source_url,
            &privacy_class,
            &body_hash,
        )?;
    }

    correlate_attachment_downloads(&transaction, &download_sources)?;

    transaction.commit()?;
    stats(raw_root)
}

pub fn stats(raw_root: impl AsRef<Path>) -> Result<CorpusStats> {
    let database = raw_root.as_ref().join("derived/corpus.sqlite3");
    anyhow::ensure!(
        database.is_file(),
        "derived corpus does not exist; run 'mirrarium corpus rebuild'"
    );
    let connection = Connection::open_with_flags(
        &database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening {}", database.display()))?;
    apply_corpus_database_key(&connection, raw_root.as_ref(), false)
        .context("opening encrypted derived corpus; restore the Mirrarium private key or run 'mirrarium corpus rebuild'")?;
    validate_corpus_schema(&connection)?;

    for table in [
        "stream_captures",
        "stream_events",
        "stream_message_revisions",
        "conversation_snapshots",
        "message_observations",
        "stream_reconstructions",
        "attachment_observations",
        "attachment_downloads",
    ] {
        anyhow::ensure!(
            table_exists(&connection, table)?,
            "derived corpus schema is out of date; run 'mirrarium corpus rebuild'"
        );
    }

    Ok(CorpusStats {
        stream_captures: scalar_u64(
            &connection,
            "SELECT COUNT(*) FROM stream_captures",
        )?,
        stream_events: scalar_u64(&connection, "SELECT COUNT(*) FROM stream_events")?,
        json_stream_events: scalar_u64(
            &connection,
            "SELECT COUNT(*) FROM stream_events WHERE json_valid = 1",
        )?,
        stream_message_revisions: scalar_u64(
            &connection,
            "SELECT COUNT(*) FROM stream_message_revisions",
        )?,
        conversation_snapshots: scalar_u64(
            &connection,
            "SELECT COUNT(*) FROM conversation_snapshots",
        )?,
        message_observations: scalar_u64(
            &connection,
            "SELECT COUNT(*) FROM message_observations",
        )?,
        stream_reconstructions: scalar_u64(
            &connection,
            "SELECT COUNT(*) FROM stream_reconstructions",
        )?,
        attachment_observations: scalar_u64(
            &connection,
            "SELECT COUNT(*) FROM attachment_observations",
        )?,
        attachment_downloads: scalar_u64(
            &connection,
            "SELECT COUNT(*) FROM attachment_downloads",
        )?,
    })
}

pub fn conversations(
    raw_root: impl AsRef<Path>,
    limit: u64,
) -> Result<Vec<ConversationSummary>> {
    anyhow::ensure!(limit > 0, "conversation limit must be greater than zero");
    let connection = open_corpus_read_only(raw_root)?;

    let mut statement = connection.prepare(
        r#"
        SELECT conversation_id
        FROM (
            SELECT conversation_id
            FROM conversation_snapshots
            UNION
            SELECT conversation_id
            FROM message_observations
            WHERE conversation_id IS NOT NULL
            UNION
            SELECT conversation_id
            FROM stream_reconstructions
        )
        ORDER BY conversation_id
        LIMIT ?1
        "#,
    )?;
    let ids = statement
        .query_map([limit as i64], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    ids.into_iter()
        .map(|conversation_id| {
            conversation_summary(&connection, &conversation_id)?
                .context("conversation disappeared while reading corpus")
        })
        .collect()
}

pub fn conversation(
    raw_root: impl AsRef<Path>,
    conversation_id: &str,
    message_limit: u64,
) -> Result<Option<ConversationView>> {
    anyhow::ensure!(
        !conversation_id.trim().is_empty(),
        "conversation id must not be empty"
    );
    anyhow::ensure!(
        message_limit > 0,
        "message observation limit must be greater than zero"
    );

    let connection = open_corpus_read_only(raw_root)?;
    let Some(summary) = conversation_summary(&connection, conversation_id)? else {
        return Ok(None);
    };

    let mut message_statement = connection.prepare(
        r#"
        SELECT
            capture_id,
            source_kind,
            sequence,
            conversation_id,
            message_id,
            role,
            content_text,
            source_url
        FROM message_observations
        WHERE conversation_id = ?1
        ORDER BY rowid
        LIMIT ?2
        "#,
    )?;
    let messages = message_statement
        .query_map(params![conversation_id, message_limit as i64], |row| {
            Ok(MessageObservationView {
                capture_id: row.get(0)?,
                source_kind: row.get(1)?,
                sequence: row.get::<_, i64>(2)? as u64,
                conversation_id: row.get(3)?,
                message_id: row.get(4)?,
                role: row.get(5)?,
                content_text: row.get(6)?,
                source_url: row.get(7)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let streams = stream_views_for_conversation(&connection, conversation_id)?;

    Ok(Some(ConversationView {
        summary,
        messages,
        streams,
    }))
}


pub fn stream_message_revisions(
    raw_root: impl AsRef<Path>,
    conversation_id: &str,
    limit: u64,
) -> Result<Vec<StreamMessageRevisionView>> {
    anyhow::ensure!(
        !conversation_id.trim().is_empty(),
        "conversation id must not be empty"
    );
    anyhow::ensure!(limit > 0, "stream revision limit must be greater than zero");

    let connection = open_corpus_read_only(raw_root)?;
    stream_message_revisions_for_connection(&connection, conversation_id, limit as i64)
}

fn stream_message_revisions_for_connection(
    connection: &Connection,
    conversation_id: &str,
    limit: i64,
) -> Result<Vec<StreamMessageRevisionView>> {
    let mut statement = connection.prepare(
        r#"
        SELECT
            capture_id,
            sequence,
            conversation_id,
            message_id,
            parent_id,
            role,
            content_text,
            source_url
        FROM stream_message_revisions
        WHERE conversation_id = ?1
        ORDER BY rowid
        LIMIT ?2
        "#,
    )?;

    let revisions = statement
        .query_map(params![conversation_id, limit], |row| {
            Ok(StreamMessageRevisionView {
                capture_id: row.get(0)?,
                sequence: row.get::<_, i64>(1)? as u64,
                conversation_id: row.get(2)?,
                message_id: row.get(3)?,
                parent_id: row.get(4)?,
                role: row.get(5)?,
                content_text: row.get(6)?,
                source_url: row.get(7)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(revisions)
}

pub fn attachments(
    raw_root: impl AsRef<Path>,
    conversation_id: Option<&str>,
    limit: u64,
) -> Result<Vec<AttachmentView>> {
    anyhow::ensure!(limit > 0, "attachment limit must be greater than zero");
    let connection = open_corpus_read_only(raw_root)?;

    let mut statement = connection.prepare(
        r#"
        SELECT
            capture_id,
            sequence,
            conversation_id,
            message_id,
            attachment_id,
            file_name,
            mime_type,
            size_bytes,
            sanitized_url,
            source_url,
            json_path
        FROM attachment_observations
        WHERE (?1 IS NULL OR conversation_id = ?1)
        ORDER BY rowid
        LIMIT ?2
        "#,
    )?;
    let observations = statement
        .query_map(params![conversation_id, limit as i64], |row| {
            Ok(AttachmentObservationView {
                capture_id: row.get(0)?,
                sequence: row.get::<_, i64>(1)? as u64,
                conversation_id: row.get(2)?,
                message_id: row.get(3)?,
                attachment_id: row.get(4)?,
                file_name: row.get(5)?,
                mime_type: row.get(6)?,
                size_bytes: row
                    .get::<_, Option<i64>>(7)?
                    .and_then(|value| value.try_into().ok()),
                sanitized_url: row.get(8)?,
                source_url: row.get(9)?,
                json_path: row.get(10)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    observations
        .into_iter()
        .map(|observation| {
            let mut download_statement = connection.prepare(
                r#"
                SELECT
                    download_capture_id,
                    source_url,
                    mime_type,
                    privacy_class,
                    body_hash,
                    body_bytes
                FROM attachment_downloads
                WHERE attachment_capture_id = ?1
                  AND attachment_sequence = ?2
                ORDER BY rowid
                "#,
            )?;
            let downloads = download_statement
                .query_map(
                    params![observation.capture_id, observation.sequence as i64],
                    |row| {
                        Ok(AttachmentDownloadView {
                            download_capture_id: row.get(0)?,
                            source_url: row.get(1)?,
                            mime_type: row.get(2)?,
                            privacy_class: row.get(3)?,
                            body_hash: row.get(4)?,
                            body_bytes: row.get::<_, i64>(5)? as u64,
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            Ok(AttachmentView {
                observation,
                downloads,
            })
        })
        .collect()
}

pub fn canonical(
    raw_root: impl AsRef<Path>,
    conversation_id: &str,
) -> Result<Option<CanonicalConversationView>> {
    anyhow::ensure!(
        !conversation_id.trim().is_empty(),
        "conversation id must not be empty"
    );

    let raw_root = raw_root.as_ref();
    let connection = open_corpus_read_only(raw_root)?;
    let snapshot = connection
        .query_row(
            r#"
            SELECT
                capture_id,
                title,
                source_url,
                privacy_class,
                source_body_hash
            FROM conversation_snapshots
            WHERE conversation_id = ?1
            ORDER BY rowid DESC
            LIMIT 1
            "#,
            [conversation_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()?;

    let Some((capture_id, title, source_url, privacy_class, source_body_hash)) = snapshot else {
        return Ok(None);
    };

    let bytes = read_verified_object(raw_root, &privacy_class, &source_body_hash)
        .context("reading canonical snapshot object")?;
    let value: Value = serde_json::from_slice(&bytes)
        .context("canonical snapshot is not JSON")?;

    let (basis_kind, current_node, mut messages, mut warnings) =
        canonical_messages_from_snapshot(&value);

    if let Some(snapshot_id) = conversation_id_from_value(&value)
        .or_else(|| value.get("id").and_then(Value::as_str).map(str::to_owned))
        .or_else(|| conversation_id_from_url(&source_url))
    {
        if snapshot_id != conversation_id {
            warnings.push(format!(
                "snapshot conversation id {snapshot_id:?} does not match requested id {conversation_id:?}"
            ));
        }
    }

    let raw_connection = open_raw_ledger_read_only(raw_root)?;
    let basis_order = capture_row_order(&raw_connection, &capture_id)?
        .context("canonical basis capture is missing from the raw ledger")?;

    let all_revisions =
        stream_message_revisions_for_connection(&connection, conversation_id, -1)?;
    let mut revisions = Vec::new();
    let mut excluded_revision_count = 0_u64;
    for revision in all_revisions {
        match capture_row_order(&raw_connection, &revision.capture_id)? {
            Some(order) if order > basis_order => revisions.push(revision),
            _ => excluded_revision_count += 1,
        }
    }
    if excluded_revision_count > 0 {
        warnings.push(format!(
            "{excluded_revision_count} stream message revision(s) do not follow the canonical basis snapshot and were excluded from canonical merging"
        ));
    }
    warnings.extend(merge_stream_revisions_into_canonical(
        &mut messages,
        &revisions,
    ));

    let all_streams = stream_views_for_conversation(&connection, conversation_id)?;
    let mut eligible_streams = Vec::new();
    let mut unlinked_streams = Vec::new();
    let mut excluded_stream_count = 0_u64;
    for stream in all_streams {
        match capture_row_order(&raw_connection, &stream.capture_id)? {
            Some(order) if order > basis_order => eligible_streams.push(stream),
            _ => {
                excluded_stream_count += 1;
                unlinked_streams.push(stream);
            }
        }
    }
    if excluded_stream_count > 0 {
        warnings.push(format!(
            "{excluded_stream_count} stream reconstruction(s) do not follow the canonical basis snapshot and remain unlinked"
        ));
    }

    let (linked_streams, newly_unlinked_streams, stream_warnings) =
        merge_exact_id_streams(&mut messages, eligible_streams);
    unlinked_streams.extend(newly_unlinked_streams);
    warnings.extend(stream_warnings);
    if !unlinked_streams.is_empty() {
        warnings.push(
            "some stream reconstructions remain separate because no safe exact-ID merge was possible"
                .to_owned(),
        );
    }

    Ok(Some(CanonicalConversationView {
        conversation_id: conversation_id.to_owned(),
        title,
        basis_capture_id: capture_id,
        basis_source_url: source_url,
        basis_source_body_hash: source_body_hash,
        basis_kind,
        current_node,
        messages,
        linked_streams,
        unlinked_streams,
        warnings,
    }))
}

fn stream_views_for_conversation(
    connection: &Connection,
    conversation_id: &str,
) -> Result<Vec<StreamReconstructionView>> {
    let mut stream_statement = connection.prepare(
        r#"
        SELECT
            capture_id,
            conversation_id,
            message_id,
            source_url,
            text,
            fragment_count
        FROM stream_reconstructions
        WHERE conversation_id = ?1
        ORDER BY rowid
        "#,
    )?;
    let rows = stream_statement.query_map([conversation_id], |row| {
        Ok(StreamReconstructionView {
            capture_id: row.get(0)?,
            conversation_id: row.get(1)?,
            message_id: row.get(2)?,
            source_url: row.get(3)?,
            text: row.get(4)?,
            fragment_count: row.get::<_, i64>(5)? as u64,
        })
    })?;
    let streams = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(streams)
}

fn compatible_optional_text(
    current: &mut Option<String>,
    incoming: Option<&str>,
) -> bool {
    let Some(incoming) = incoming else {
        return true;
    };

    match current.as_deref() {
        None => {
            *current = Some(incoming.to_owned());
            true
        }
        Some(existing) if existing == incoming => true,
        Some(existing) if incoming.starts_with(existing) => {
            *current = Some(incoming.to_owned());
            true
        }
        Some(existing) if existing.starts_with(incoming) => true,
        Some(_) => false,
    }
}

fn collapse_stream_revision_candidates(
    revisions: &[StreamMessageRevisionView],
) -> (BTreeMap<String, StreamRevisionCandidate>, Vec<String>) {
    let mut candidates: BTreeMap<String, StreamRevisionCandidate> = BTreeMap::new();
    let mut warnings = Vec::new();

    for revision in revisions {
        let candidate = candidates
            .entry(revision.message_id.clone())
            .or_insert_with(|| StreamRevisionCandidate {
                message_id: revision.message_id.clone(),
                parent_id: revision.parent_id.clone(),
                role: revision.role.clone(),
                content_text: revision.content_text.clone(),
                valid: true,
            });

        if candidate.parent_id.is_none() {
            candidate.parent_id = revision.parent_id.clone();
        } else if let Some(parent_id) = revision.parent_id.as_deref() {
            if candidate.parent_id.as_deref() != Some(parent_id) {
                candidate.valid = false;
                warnings.push(format!(
                    "stream revisions for message {:?} disagree on parent id; refusing to canonicalize them",
                    revision.message_id
                ));
            }
        }

        if candidate.role.is_none() {
            candidate.role = revision.role.clone();
        } else if let Some(role) = revision.role.as_deref() {
            if candidate.role.as_deref() != Some(role) {
                candidate.valid = false;
                warnings.push(format!(
                    "stream revisions for message {:?} disagree on role; refusing to canonicalize them",
                    revision.message_id
                ));
            }
        }

        if !compatible_optional_text(
            &mut candidate.content_text,
            revision.content_text.as_deref(),
        ) {
            candidate.valid = false;
            warnings.push(format!(
                "stream revisions for message {:?} contain incompatible text revisions; refusing to canonicalize them",
                revision.message_id
            ));
        }
    }

    (candidates, warnings)
}

fn merge_stream_revisions_into_canonical(
    messages: &mut Vec<CanonicalMessageView>,
    revisions: &[StreamMessageRevisionView],
) -> Vec<String> {
    let (candidates, mut warnings) = collapse_stream_revision_candidates(revisions);
    let mut applied = BTreeSet::new();

    for candidate in candidates.values().filter(|candidate| candidate.valid) {
        let matches: Vec<usize> = messages
            .iter()
            .enumerate()
            .filter_map(|(index, message)| {
                (message.message_id.as_deref() == Some(candidate.message_id.as_str()))
                    .then_some(index)
            })
            .collect();

        if matches.len() != 1 {
            continue;
        }

        let target = &mut messages[matches[0]];
        if let (Some(snapshot_parent), Some(stream_parent)) = (
            target.parent_id.as_deref(),
            candidate.parent_id.as_deref(),
        ) {
            if snapshot_parent != stream_parent {
                warnings.push(format!(
                    "stream revisions for message {:?} disagree with canonical parent {:?}; refusing to merge",
                    candidate.message_id, snapshot_parent
                ));
                continue;
            }
        }

        if let (Some(snapshot_role), Some(stream_role)) =
            (target.role.as_deref(), candidate.role.as_deref())
        {
            if snapshot_role != stream_role {
                warnings.push(format!(
                    "stream revisions for message {:?} disagree with canonical role {:?}; refusing to merge",
                    candidate.message_id, snapshot_role
                ));
                continue;
            }
        }

        if !compatible_optional_text(
            &mut target.content_text,
            candidate.content_text.as_deref(),
        ) {
            warnings.push(format!(
                "stream revisions for message {:?} conflict with canonical snapshot content; refusing to merge",
                candidate.message_id
            ));
            continue;
        }

        if target.parent_id.is_none() {
            target.parent_id = candidate.parent_id.clone();
        }
        if target.role.is_none() {
            target.role = candidate.role.clone();
        }
        applied.insert(candidate.message_id.clone());
    }

    let mut canonical_ids: BTreeSet<String> = messages
        .iter()
        .filter_map(|message| message.message_id.clone())
        .collect();
    let mut seen_tail_ids = canonical_ids.clone();

    loop {
        let Some(tail_id) = messages
            .last()
            .and_then(|message| message.message_id.clone())
        else {
            if candidates.values().any(|candidate| {
                candidate.valid && !applied.contains(&candidate.message_id)
            }) {
                warnings.push(
                    "canonical tail has no message id; refusing to append streamed children"
                        .to_owned(),
                );
            }
            break;
        };

        let children: Vec<&StreamRevisionCandidate> = candidates
            .values()
            .filter(|candidate| {
                !applied.contains(&candidate.message_id)
                    && !canonical_ids.contains(&candidate.message_id)
                    && candidate.parent_id.as_deref() == Some(tail_id.as_str())
            })
            .collect();

        match children.as_slice() {
            [] => break,
            [child] => {
                if !child.valid {
                    warnings.push(format!(
                        "stream tail child {:?} has conflicting revision evidence; refusing to append it",
                        child.message_id
                    ));
                    break;
                }
                if child.role.is_none() && child.content_text.is_none() {
                    warnings.push(format!(
                        "stream tail child {:?} has no role or content evidence; refusing to append it",
                        child.message_id
                    ));
                    break;
                }
                if !seen_tail_ids.insert(child.message_id.clone()) {
                    warnings.push(format!(
                        "stream tail cycle detected at message {:?}; refusing to continue",
                        child.message_id
                    ));
                    break;
                }

                messages.push(CanonicalMessageView {
                    message_id: Some(child.message_id.clone()),
                    parent_id: child.parent_id.clone(),
                    role: child.role.clone(),
                    content_text: child.content_text.clone(),
                });
                canonical_ids.insert(child.message_id.clone());
                applied.insert(child.message_id.clone());
            }
            _ => {
                warnings.push(format!(
                    "stream tail parent {tail_id:?} has {} distinct child messages; refusing to guess a branch",
                    children.len()
                ));
                break;
            }
        }
    }

    let unapplied = candidates
        .values()
        .filter(|candidate| candidate.valid && !applied.contains(&candidate.message_id))
        .count();
    if unapplied > 0 {
        warnings.push(format!(
            "{unapplied} stream message candidate(s) remain outside the canonical branch"
        ));
    }

    warnings
}

fn merge_exact_id_streams(
    messages: &mut [CanonicalMessageView],
    streams: Vec<StreamReconstructionView>,
) -> (
    Vec<StreamReconstructionView>,
    Vec<StreamReconstructionView>,
    Vec<String>,
) {
    let mut linked = Vec::new();
    let mut unlinked = Vec::new();
    let mut warnings = Vec::new();

    for stream in streams {
        let Some(message_id) = stream.message_id.as_deref() else {
            unlinked.push(stream);
            continue;
        };

        let matching_indices: Vec<usize> = messages
            .iter()
            .enumerate()
            .filter_map(|(index, message)| {
                (message.message_id.as_deref() == Some(message_id)).then_some(index)
            })
            .collect();

        if matching_indices.len() != 1 {
            warnings.push(format!(
                "stream for message {message_id:?} matched {} canonical messages; refusing to merge",
                matching_indices.len()
            ));
            unlinked.push(stream);
            continue;
        }

        let target = &mut messages[matching_indices[0]];
        let compatible = match target.content_text.as_deref() {
            None => {
                target.content_text = Some(stream.text.clone());
                true
            }
            Some(snapshot_text) if snapshot_text == stream.text => true,
            Some(snapshot_text) if stream.text.starts_with(snapshot_text) => {
                target.content_text = Some(stream.text.clone());
                true
            }
            Some(snapshot_text) if snapshot_text.starts_with(&stream.text) => true,
            Some(_) => false,
        };

        if compatible {
            linked.push(stream);
        } else {
            warnings.push(format!(
                "stream text for message {message_id:?} conflicts with canonical snapshot content; refusing to merge"
            ));
            unlinked.push(stream);
        }
    }

    (linked, unlinked, warnings)
}

fn canonical_messages_from_snapshot(
    value: &Value,
) -> (
    String,
    Option<String>,
    Vec<CanonicalMessageView>,
    Vec<String>,
) {
    let mut warnings = Vec::new();

    if let Some(mapping) = value.get("mapping").and_then(Value::as_object) {
        let current_node = value
            .get("current_node")
            .and_then(Value::as_str)
            .map(str::to_owned);

        let Some(mut cursor) = current_node.clone() else {
            warnings.push(
                "mapping snapshot has no current_node; refusing to guess a canonical branch"
                    .to_owned(),
            );
            return (
                "mapping_without_current_node".to_owned(),
                None,
                Vec::new(),
                warnings,
            );
        };

        let mut seen = BTreeSet::new();
        let mut reversed = Vec::new();

        loop {
            if !seen.insert(cursor.clone()) {
                warnings.push(format!(
                    "mapping parent cycle detected at node {cursor:?}; branch truncated"
                ));
                break;
            }

            let Some(node) = mapping.get(&cursor) else {
                warnings.push(format!(
                    "current branch references missing node {cursor:?}; branch truncated"
                ));
                break;
            };

            let parent_id = node
                .get("parent")
                .and_then(Value::as_str)
                .map(str::to_owned);

            if let Some(message) = extract_message(node, Some(&cursor)) {
                reversed.push(CanonicalMessageView {
                    message_id: message.message_id,
                    parent_id: parent_id.clone(),
                    role: message.role,
                    content_text: message.content_text,
                });
            }

            let Some(parent_id) = parent_id else {
                break;
            };
            cursor = parent_id;
        }

        reversed.reverse();
        return (
            "mapping_current_node".to_owned(),
            current_node,
            reversed,
            warnings,
        );
    }

    if let Some(items) = value.get("messages").and_then(Value::as_array) {
        let messages = items
            .iter()
            .filter_map(|item| extract_message(item, None))
            .map(|message| CanonicalMessageView {
                message_id: message.message_id,
                parent_id: None,
                role: message.role,
                content_text: message.content_text,
            })
            .collect();
        return (
            "messages_array".to_owned(),
            None,
            messages,
            warnings,
        );
    }

    warnings.push("snapshot has neither mapping nor messages array".to_owned());
    (
        "unsupported_snapshot_shape".to_owned(),
        None,
        Vec::new(),
        warnings,
    )
}

fn open_raw_ledger_read_only(raw_root: impl AsRef<Path>) -> Result<Connection> {
    let database = raw_root.as_ref().join("ledger.sqlite3");
    anyhow::ensure!(
        database.is_file(),
        "raw ledger does not exist: {}",
        database.display()
    );
    Connection::open_with_flags(
        &database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening raw ledger {}", database.display()))
}

fn capture_row_order(connection: &Connection, capture_id: &str) -> Result<Option<i64>> {
    connection
        .query_row(
            "SELECT rowid FROM captures WHERE capture_id = ?1",
            [capture_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
}

fn open_corpus_read_only(raw_root: impl AsRef<Path>) -> Result<Connection> {
    let database = raw_root.as_ref().join("derived/corpus.sqlite3");
    anyhow::ensure!(
        database.is_file(),
        "derived corpus does not exist; run 'mirrarium corpus rebuild'"
    );
    let connection = Connection::open_with_flags(
        &database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening {}", database.display()))?;
    validate_corpus_schema(&connection)?;

    for table in [
        "stream_captures",
        "stream_events",
        "stream_message_revisions",
        "conversation_snapshots",
        "message_observations",
        "stream_reconstructions",
        "attachment_observations",
        "attachment_downloads",
    ] {
        anyhow::ensure!(
            table_exists(&connection, table)?,
            "derived corpus schema is out of date; run 'mirrarium corpus rebuild'"
        );
    }

    Ok(connection)
}

fn conversation_summary(
    connection: &Connection,
    conversation_id: &str,
) -> Result<Option<ConversationSummary>> {
    let snapshot_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM conversation_snapshots WHERE conversation_id = ?1",
        [conversation_id],
        |row| row.get(0),
    )?;
    let message_observation_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM message_observations WHERE conversation_id = ?1",
        [conversation_id],
        |row| row.get(0),
    )?;
    let stream_reconstruction_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM stream_reconstructions WHERE conversation_id = ?1",
        [conversation_id],
        |row| row.get(0),
    )?;

    if snapshot_count == 0
        && message_observation_count == 0
        && stream_reconstruction_count == 0
    {
        return Ok(None);
    }

    let title = connection
        .query_row(
            r#"
            SELECT title
            FROM conversation_snapshots
            WHERE conversation_id = ?1
              AND title IS NOT NULL
            ORDER BY rowid DESC
            LIMIT 1
            "#,
            [conversation_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;

    Ok(Some(ConversationSummary {
        conversation_id: conversation_id.to_owned(),
        title,
        snapshot_count: snapshot_count.try_into().context("negative snapshot count")?,
        message_observation_count: message_observation_count
            .try_into()
            .context("negative message observation count")?,
        stream_reconstruction_count: stream_reconstruction_count
            .try_into()
            .context("negative stream reconstruction count")?,
    }))
}


fn collect_download_sources(connection: &Connection) -> Result<Vec<DownloadSource>> {
    let mut statement = connection.prepare(
        r#"
        SELECT
            capture_id,
            url,
            mime_type,
            privacy_class,
            body_hash,
            body_bytes
        FROM captures
        WHERE body_hash IS NOT NULL
        ORDER BY captured_at_ms, capture_id
        "#,
    )?;
    let rows = statement.query_map([], |row| {
        Ok(DownloadSource {
            capture_id: row.get(0)?,
            source_url: row.get(1)?,
            mime_type: row.get(2)?,
            privacy_class: row.get(3)?,
            body_hash: row.get(4)?,
            body_bytes: row.get::<_, i64>(5)? as u64,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn correlate_attachment_downloads(
    transaction: &Transaction<'_>,
    downloads: &[DownloadSource],
) -> Result<()> {
    for download in downloads {
        let Some(url_identity) = attachment_url_identity(&download.source_url, None) else {
            continue;
        };

        transaction.execute(
            r#"
            INSERT OR IGNORE INTO attachment_downloads (
                attachment_capture_id,
                attachment_sequence,
                download_capture_id,
                source_url,
                mime_type,
                privacy_class,
                body_hash,
                body_bytes
            )
            SELECT
                capture_id,
                sequence,
                ?1,
                ?2,
                ?3,
                ?4,
                ?5,
                ?6
            FROM attachment_observations
            WHERE url_identity = ?7
            "#,
            params![
                download.capture_id,
                download.source_url,
                download.mime_type,
                download.privacy_class,
                download.body_hash,
                download.body_bytes as i64,
                url_identity,
            ],
        )?;
    }

    Ok(())
}

fn collect_sources(
    connection: &Connection,
    sql: &str,
) -> Result<Vec<(String, String, String, String)>> {
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn derive_stream_capture(
    transaction: &Transaction<'_>,
    raw_root: &Path,
    capture_id: &str,
    source_url: &str,
    privacy_class: &str,
    body_hash: &str,
) -> Result<()> {
    let bytes = read_verified_object(raw_root, privacy_class, body_hash)
        .context("reading stream body object")?;
    let text = std::str::from_utf8(&bytes)
        .context("stream body is not UTF-8")?;
    let events = parse_sse(text);

    transaction.execute(
        r#"
        INSERT INTO stream_captures
            (capture_id, source_url, privacy_class, source_body_hash, event_count)
        VALUES
            (?1, ?2, ?3, ?4, ?5)
        "#,
        params![
            capture_id,
            source_url,
            privacy_class,
            body_hash,
            events.len() as i64,
        ],
    )?;

    let mut reconstructions: BTreeMap<String, StreamAccumulator> = BTreeMap::new();

    for (sequence, event) in events.into_iter().enumerate() {
        let parsed = serde_json::from_str::<Value>(&event.data).ok();
        transaction.execute(
            r#"
            INSERT INTO stream_events
                (capture_id, sequence, event_name, data, json_valid)
            VALUES
                (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                capture_id,
                sequence as i64,
                event.event_name,
                event.data,
                if parsed.is_some() { 1_i64 } else { 0_i64 },
            ],
        )?;

        let Some(value) = parsed else {
            continue;
        };

        let conversation_id = conversation_id_from_value(&value)
            .or_else(|| conversation_id_from_url(source_url));

        if let (Some(conversation_id), Some(delta)) = (
            conversation_id.as_deref(),
            value.get("delta").and_then(Value::as_str),
        ) {
            let explicit_message_id = stream_message_id(&value);
            let entry = reconstructions
                .entry(conversation_id.to_owned())
                .or_insert_with(|| StreamAccumulator {
                    message_id: explicit_message_id.clone(),
                    linkable: explicit_message_id.is_some(),
                    ..StreamAccumulator::default()
                });

            if entry.fragment_count > 0
                && entry.linkable
                && entry.message_id.as_deref() != explicit_message_id.as_deref()
            {
                entry.linkable = false;
                entry.message_id = None;
            }

            entry.text.push_str(delta);
            entry.fragment_count += 1;
        }

        let message_value = value.get("message").unwrap_or(&value);
        if let Some(mut message) = extract_message(message_value, None) {
            if message.conversation_id.is_none() {
                message.conversation_id = conversation_id;
            }

            if let (Some(conversation_id), Some(message_id)) = (
                message.conversation_id.as_deref(),
                message.message_id.as_deref(),
            ) {
                transaction.execute(
                    r#"
                    INSERT INTO stream_message_revisions (
                        capture_id,
                        sequence,
                        conversation_id,
                        message_id,
                        parent_id,
                        role,
                        content_text,
                        source_url
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8
                    )
                    "#,
                    params![
                        capture_id,
                        sequence as i64,
                        conversation_id,
                        message_id,
                        parent_message_id_from_stream_value(&value, message_value),
                        message.role.as_deref(),
                        message.content_text.as_deref(),
                        source_url,
                    ],
                )?;
            }

            insert_message_observation(
                transaction,
                capture_id,
                "sse",
                sequence as u64,
                source_url,
                &message,
            )?;
        }
    }

    for (conversation_id, reconstruction) in reconstructions {
        let message_id = if reconstruction.linkable {
            reconstruction.message_id.as_deref()
        } else {
            None
        };
        transaction.execute(
            r#"
            INSERT INTO stream_reconstructions
                (capture_id, conversation_id, message_id, source_url, text, fragment_count)
            VALUES
                (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            params![
                capture_id,
                conversation_id,
                message_id,
                source_url,
                reconstruction.text,
                reconstruction.fragment_count as i64,
            ],
        )?;
    }

    Ok(())
}

fn parent_message_id_from_stream_value(
    event: &Value,
    message: &Value,
) -> Option<String> {
    message
        .get("parent_id")
        .and_then(Value::as_str)
        .or_else(|| message.get("parentId").and_then(Value::as_str))
        .or_else(|| event.get("parent_id").and_then(Value::as_str))
        .or_else(|| event.get("parentId").and_then(Value::as_str))
        .or_else(|| event.get("parent_message_id").and_then(Value::as_str))
        .or_else(|| event.get("parentMessageId").and_then(Value::as_str))
        .or_else(|| {
            message
                .pointer("/metadata/parent_id")
                .and_then(Value::as_str)
        })
        .map(str::to_owned)
}

fn stream_message_id(value: &Value) -> Option<String> {
    value
        .pointer("/message/id")
        .and_then(Value::as_str)
        .or_else(|| value.get("message_id").and_then(Value::as_str))
        .or_else(|| value.get("messageId").and_then(Value::as_str))
        .map(str::to_owned)
}

fn derive_json_capture(
    transaction: &Transaction<'_>,
    raw_root: &Path,
    capture_id: &str,
    source_url: &str,
    privacy_class: &str,
    body_hash: &str,
) -> Result<()> {
    let bytes = read_verified_object(raw_root, privacy_class, body_hash)
        .context("reading JSON body object")?;
    let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
        return Ok(());
    };

    let root_conversation_id = conversation_id_from_value(&value)
        .or_else(|| value.get("id").and_then(Value::as_str).map(str::to_owned))
        .or_else(|| conversation_id_from_url(source_url));
    let attachments = extract_attachment_observations(
        &value,
        source_url,
        root_conversation_id.as_deref(),
    );
    for (sequence, attachment) in attachments.iter().enumerate() {
        insert_attachment_observation(
            transaction,
            capture_id,
            sequence as u64,
            source_url,
            attachment,
        )?;
    }

    let Some(extraction) = extract_conversation(&value, source_url) else {
        return Ok(());
    };

    transaction.execute(
        r#"
        INSERT INTO conversation_snapshots
            (capture_id, conversation_id, title, source_url, privacy_class, source_body_hash)
        VALUES
            (?1, ?2, ?3, ?4, ?5, ?6)
        "#,
        params![
            capture_id,
            extraction.conversation_id,
            extraction.title,
            source_url,
            privacy_class,
            body_hash,
        ],
    )?;

    for (sequence, message) in extraction.messages.iter().enumerate() {
        insert_message_observation(
            transaction,
            capture_id,
            "json_snapshot",
            sequence as u64,
            source_url,
            message,
        )?;
    }

    Ok(())
}


fn insert_attachment_observation(
    transaction: &Transaction<'_>,
    capture_id: &str,
    sequence: u64,
    source_url: &str,
    attachment: &AttachmentObservation,
) -> Result<()> {
    let url_identity = attachment
        .sanitized_url
        .as_deref()
        .and_then(|url| attachment_url_identity(url, Some(source_url)));

    transaction.execute(
        r#"
        INSERT INTO attachment_observations (
            capture_id,
            sequence,
            conversation_id,
            message_id,
            attachment_id,
            file_name,
            mime_type,
            size_bytes,
            sanitized_url,
            url_identity,
            source_url,
            json_path
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12
        )
        "#,
        params![
            capture_id,
            sequence as i64,
            attachment.conversation_id.as_deref(),
            attachment.message_id.as_deref(),
            attachment.attachment_id.as_deref(),
            attachment.file_name.as_deref(),
            attachment.mime_type.as_deref(),
            attachment.size_bytes.map(|value| value as i64),
            attachment.sanitized_url.as_deref(),
            url_identity,
            source_url,
            attachment.json_path,
        ],
    )?;
    Ok(())
}

fn extract_attachment_observations(
    value: &Value,
    source_url: &str,
    root_conversation_id: Option<&str>,
) -> Vec<AttachmentObservation> {
    let mut observations = Vec::new();
    walk_attachment_values(
        value,
        source_url,
        "$",
        root_conversation_id.map(str::to_owned),
        None,
        &mut observations,
    );
    observations
}

fn walk_attachment_values(
    value: &Value,
    source_url: &str,
    path: &str,
    inherited_conversation_id: Option<String>,
    inherited_message_id: Option<String>,
    observations: &mut Vec<AttachmentObservation>,
) {
    match value {
        Value::Object(map) => {
            let conversation_id =
                conversation_id_from_value(value).or(inherited_conversation_id);
            let message_id = message_id_for_context(value).or(inherited_message_id);

            if let Some(observation) = attachment_from_object(
                map,
                source_url,
                path,
                conversation_id.clone(),
                message_id.clone(),
            ) {
                observations.push(observation);
            }

            for (key, child) in map {
                let child_path = format!("{path}/{}", escape_json_pointer(key));
                walk_attachment_values(
                    child,
                    source_url,
                    &child_path,
                    conversation_id.clone(),
                    message_id.clone(),
                    observations,
                );
            }
        }
        Value::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                walk_attachment_values(
                    child,
                    source_url,
                    &format!("{path}/{index}"),
                    inherited_conversation_id.clone(),
                    inherited_message_id.clone(),
                    observations,
                );
            }
        }
        _ => {}
    }
}

fn attachment_from_object(
    map: &serde_json::Map<String, Value>,
    source_url: &str,
    path: &str,
    conversation_id: Option<String>,
    message_id: Option<String>,
) -> Option<AttachmentObservation> {
    let explicit_id = first_string(
        map,
        &[
            "file_id",
            "fileId",
            "attachment_id",
            "attachmentId",
            "asset_pointer",
        ],
    );
    let file_name = first_string(map, &["file_name", "fileName", "filename", "name"]);
    let mime_type = first_string(
        map,
        &["mime_type", "mimeType", "content_type", "contentType"],
    );
    let size_bytes = first_u64(map, &["size_bytes", "sizeBytes", "size"]);
    let raw_url = first_string(
        map,
        &[
            "download_url",
            "downloadUrl",
            "download_link",
            "downloadLink",
            "url",
        ],
    );

    let path_lower = path.to_ascii_lowercase();
    let attachment_context = path_lower.contains("attachment")
        || path_lower.contains("/files")
        || path_lower.contains("/file")
        || map.contains_key("download_url")
        || map.contains_key("downloadUrl")
        || map.contains_key("file_id")
        || map.contains_key("fileId")
        || map.contains_key("attachment_id")
        || map.contains_key("attachmentId")
        || map.contains_key("asset_pointer");

    if !attachment_context {
        return None;
    }

    if explicit_id.is_none()
        && file_name.is_none()
        && mime_type.is_none()
        && size_bytes.is_none()
        && raw_url.is_none()
    {
        return None;
    }

    let attachment_id = explicit_id.or_else(|| {
        map.get("id")
            .and_then(Value::as_str)
            .map(str::to_owned)
    });
    let sanitized_url = raw_url
        .as_deref()
        .and_then(|url| attachment_url_identity(url, Some(source_url)));

    Some(AttachmentObservation {
        conversation_id,
        message_id,
        attachment_id,
        file_name,
        mime_type,
        size_bytes,
        sanitized_url,
        json_path: path.to_owned(),
    })
}

fn first_string(
    map: &serde_json::Map<String, Value>,
    keys: &[&str],
) -> Option<String> {
    keys.iter()
        .find_map(|key| map.get(*key).and_then(Value::as_str))
        .map(str::to_owned)
}

fn first_u64(
    map: &serde_json::Map<String, Value>,
    keys: &[&str],
) -> Option<u64> {
    keys.iter().find_map(|key| {
        let value = map.get(*key)?;
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
    })
}

fn message_id_for_context(value: &Value) -> Option<String> {
    let role = value
        .get("role")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/author/role").and_then(Value::as_str));
    role?;
    value
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn attachment_url_identity(raw_url: &str, base_url: Option<&str>) -> Option<String> {
    let mut url = Url::parse(raw_url).ok().or_else(|| {
        let base = Url::parse(base_url?).ok()?;
        base.join(raw_url).ok()
    })?;

    url.set_fragment(None);
    let query_pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();

    if !query_pairs.is_empty() {
        url.set_query(None);
        let mut query = url.query_pairs_mut();
        for (key, value) in query_pairs {
            if is_sensitive_url_key(&key) {
                query.append_pair(&key, "[REDACTED]");
            } else {
                query.append_pair(&key, &value);
            }
        }
    }

    Some(url.to_string())
}

fn is_sensitive_url_key(key: &str) -> bool {
    let normalized = key.trim().to_ascii_lowercase().replace('-', "_");
    matches!(
        normalized.as_str(),
        "token"
            | "access_token"
            | "id_token"
            | "refresh_token"
            | "session"
            | "session_token"
            | "auth"
            | "authorization"
            | "signature"
            | "x_amz_signature"
            | "x_goog_signature"
            | "key"
            | "api_key"
            | "apikey"
            | "code"
    ) || normalized.ends_with("_token")
        || normalized.ends_with("_signature")
        || normalized.contains("credential")
}

fn escape_json_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn insert_message_observation(
    transaction: &Transaction<'_>,
    capture_id: &str,
    source_kind: &str,
    sequence: u64,
    source_url: &str,
    message: &MessageObservation,
) -> Result<()> {
    if message.message_id.is_none() && message.role.is_none() && message.content_text.is_none() {
        return Ok(());
    }

    transaction.execute(
        r#"
        INSERT INTO message_observations (
            capture_id,
            source_kind,
            sequence,
            conversation_id,
            message_id,
            role,
            content_text,
            source_url
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8
        )
        "#,
        params![
            capture_id,
            source_kind,
            sequence as i64,
            message.conversation_id.as_deref(),
            message.message_id.as_deref(),
            message.role.as_deref(),
            message.content_text.as_deref(),
            source_url,
        ],
    )?;
    Ok(())
}

fn extract_conversation(value: &Value, source_url: &str) -> Option<ConversationExtraction> {
    let conversation_id = conversation_id_from_value(value)
        .or_else(|| value.get("id").and_then(Value::as_str).map(str::to_owned))
        .or_else(|| conversation_id_from_url(source_url))?;
    let title = value
        .get("title")
        .and_then(Value::as_str)
        .map(str::to_owned);

    let mut messages = Vec::new();

    if let Some(items) = value.get("messages").and_then(Value::as_array) {
        for item in items {
            if let Some(mut message) = extract_message(item, None) {
                if message.conversation_id.is_none() {
                    message.conversation_id = Some(conversation_id.clone());
                }
                messages.push(message);
            }
        }
    }

    if let Some(mapping) = value.get("mapping").and_then(Value::as_object) {
        for (node_id, node) in mapping {
            if let Some(mut message) = extract_message(node, Some(node_id)) {
                if message.conversation_id.is_none() {
                    message.conversation_id = Some(conversation_id.clone());
                }
                messages.push(message);
            }
        }
    }

    Some(ConversationExtraction {
        conversation_id,
        title,
        messages,
    })
}

fn extract_message(value: &Value, fallback_id: Option<&str>) -> Option<MessageObservation> {
    let message = value.get("message").unwrap_or(value);
    if !message.is_object() {
        return None;
    }

    let conversation_id = conversation_id_from_value(message);
    let message_id = message
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| fallback_id.map(str::to_owned));
    let role = message
        .get("role")
        .and_then(Value::as_str)
        .or_else(|| message.pointer("/author/role").and_then(Value::as_str))
        .map(str::to_owned);
    let content_text = message
        .get("content")
        .and_then(extract_text)
        .or_else(|| message.get("text").and_then(extract_text));

    if message_id.is_none() && role.is_none() && content_text.is_none() {
        return None;
    }

    Some(MessageObservation {
        conversation_id,
        message_id,
        role,
        content_text,
    })
}

fn extract_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Array(values) => {
            let parts: Vec<String> = values.iter().filter_map(extract_text).collect();
            (!parts.is_empty()).then(|| parts.join(""))
        }
        Value::Object(map) => {
            if let Some(text) = map.get("text").and_then(Value::as_str) {
                return Some(text.to_owned());
            }

            if let Some(parts) = map.get("parts").and_then(Value::as_array) {
                let text: Vec<String> = parts.iter().filter_map(extract_text).collect();
                if !text.is_empty() {
                    return Some(text.join(""));
                }
            }

            None
        }
        _ => None,
    }
}

fn conversation_id_from_value(value: &Value) -> Option<String> {
    value
        .get("conversation_id")
        .and_then(Value::as_str)
        .or_else(|| value.get("conversationId").and_then(Value::as_str))
        .map(str::to_owned)
}

fn conversation_id_from_url(source_url: &str) -> Option<String> {
    let url = Url::parse(source_url).ok()?;
    let segments: Vec<&str> = url.path_segments()?.collect();

    for window in segments.windows(2) {
        if matches!(window[0], "conversation" | "conversations")
            && !matches!(window[1], "" | "post" | "stream")
        {
            return Some(window[1].to_owned());
        }
    }

    None
}

fn parse_sse(input: &str) -> Vec<SseEvent> {
    let normalized = input.replace("\r\n", "\n").replace('\r', "\n");
    let mut events = Vec::new();
    let mut event_name: Option<String> = None;
    let mut data_lines: Vec<String> = Vec::new();

    let flush = |events: &mut Vec<SseEvent>,
                 event_name: &mut Option<String>,
                 data_lines: &mut Vec<String>| {
        if data_lines.is_empty() {
            *event_name = None;
            return;
        }

        events.push(SseEvent {
            event_name: event_name.take(),
            data: data_lines.join("\n"),
        });
        data_lines.clear();
    };

    for line in normalized.split('\n') {
        if line.is_empty() {
            flush(&mut events, &mut event_name, &mut data_lines);
            continue;
        }

        if line.starts_with(':') {
            continue;
        }

        let (field, value) = match line.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (line, ""),
        };

        match field {
            "event" => event_name = Some(value.to_owned()),
            "data" => data_lines.push(value.to_owned()),
            _ => {}
        }
    }

    flush(&mut events, &mut event_name, &mut data_lines);
    events
}

fn object_path(root: &Path, privacy_class: &str, hash: &str) -> Result<PathBuf> {
    anyhow::ensure!(
        matches!(privacy_class, "public" | "private" | "unknown"),
        "invalid storage class {privacy_class:?}"
    );
    anyhow::ensure!(
        hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "invalid SHA-256 object key {hash:?}"
    );

    Ok(root
        .join(privacy_class)
        .join("objects")
        .join(&hash[..2])
        .join(hash))
}

fn sqlcipher_raw_key_literal(key: &[u8; 32]) -> String {
    let mut hex = String::with_capacity(64);
    for byte in key {
        use std::fmt::Write as _;
        write!(&mut hex, "{byte:02x}").expect("writing to String cannot fail");
    }
    format!("x'{hex}'")
}

fn apply_corpus_database_key(
    connection: &Connection,
    raw_root: &Path,
    create_master_if_missing: bool,
) -> Result<()> {
    let key = private_database_key(
        raw_root,
        "corpus-sqlcipher-v1",
        create_master_if_missing,
    )?;
    let literal = sqlcipher_raw_key_literal(&key);
    connection
        .execute_batch(&format!("PRAGMA key = \"{literal}\";"))
        .context("applying SQLCipher corpus key")?;

    let cipher_version: String = connection
        .pragma_query_value(None, "cipher_version", |row| row.get(0))
        .context("this Mirrarium build does not provide SQLCipher")?;
    anyhow::ensure!(
        !cipher_version.trim().is_empty(),
        "SQLCipher cipher_version is empty"
    );

    connection
        .query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get::<_, i64>(0))
        .context("verifying SQLCipher corpus key")?;
    Ok(())
}

fn remove_corpus_database_files(database: &Path) -> Result<()> {
    for path in [
        database.to_path_buf(),
        PathBuf::from(format!("{}-wal", database.display())),
        PathBuf::from(format!("{}-shm", database.display())),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("removing old corpus database file {}", path.display()));
            }
        }
    }
    Ok(())
}

fn validate_corpus_schema(connection: &Connection) -> Result<()> {
    let version: i64 =
        connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    anyhow::ensure!(
        version == CORPUS_SCHEMA_VERSION,
        "derived corpus schema version {version} is out of date (expected {CORPUS_SCHEMA_VERSION}); run 'mirrarium corpus rebuild'"
    );
    Ok(())
}

fn table_exists(connection: &Connection, table: &str) -> Result<bool> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [table],
        |row| row.get(0),
    )?;
    Ok(count == 1)
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

    #[test]
    fn sqlcipher_raw_key_literal_is_exact_32_byte_hex_blob() {
        let literal = sqlcipher_raw_key_literal(&[0xab; 32]);
        assert_eq!(literal.len(), 67);
        assert!(literal.starts_with("x'"));
        assert!(literal.ends_with('''));
        assert_eq!(&literal[2..66], &"ab".repeat(32));
    }

    #[test]
    fn rejects_outdated_derived_corpus_schema() {
        let connection = Connection::open_in_memory().unwrap();
        let error = validate_corpus_schema(&connection).unwrap_err();
        assert!(error.to_string().contains("run 'mirrarium corpus rebuild'"));

        connection
            .pragma_update(None, "user_version", CORPUS_SCHEMA_VERSION)
            .unwrap();
        validate_corpus_schema(&connection).unwrap();
    }

    #[test]
    fn parses_sse_fields_multiline_data_and_done_marker() {
        let input = concat!(
            ": keepalive\r\n",
            "event: message\r\n",
            "data: {\"a\":1,\r\n",
            "data: \"b\":2}\r\n",
            "\r\n",
            "data: [DONE]\r\n",
            "\r\n"
        );

        assert_eq!(
            parse_sse(input),
            vec![
                SseEvent {
                    event_name: Some("message".to_owned()),
                    data: "{\"a\":1,\n\"b\":2}".to_owned(),
                },
                SseEvent {
                    event_name: None,
                    data: "[DONE]".to_owned(),
                },
            ]
        );
    }


    #[test]
    fn extracts_attachment_metadata_and_sanitizes_signed_url_identity() {
        let value = serde_json::json!({
            "conversation_id": "conversation-a",
            "mapping": {
                "message-node": {
                    "message": {
                        "id": "message-1",
                        "author": {"role": "user"},
                        "content": {
                            "attachments": [{
                                "file_id": "file-123",
                                "filename": "notes.txt",
                                "mime_type": "text/plain",
                                "size_bytes": 42,
                                "download_url": "/backend-api/files/file-123/download?token=secret&keep=yes"
                            }]
                        }
                    }
                }
            }
        });

        let observations = extract_attachment_observations(
            &value,
            "https://chatgpt.com/backend-api/conversation/conversation-a",
            Some("conversation-a"),
        );
        assert_eq!(observations.len(), 1);
        let attachment = &observations[0];
        assert_eq!(attachment.conversation_id.as_deref(), Some("conversation-a"));
        assert_eq!(attachment.message_id.as_deref(), Some("message-1"));
        assert_eq!(attachment.attachment_id.as_deref(), Some("file-123"));
        assert_eq!(attachment.file_name.as_deref(), Some("notes.txt"));
        assert_eq!(attachment.mime_type.as_deref(), Some("text/plain"));
        assert_eq!(attachment.size_bytes, Some(42));
        let url = attachment.sanitized_url.as_deref().unwrap();
        assert!(url.contains("/backend-api/files/file-123/download"));
        assert!(url.contains("keep=yes"));
        assert!(!url.contains("secret"));
        assert!(url.contains("%5BREDACTED%5D"));
    }

    #[test]
    fn extracts_stream_parent_message_identity() {
        let event = serde_json::json!({
            "conversation_id": "conversation-a",
            "parent_message_id": "user-1",
            "message": {
                "id": "assistant-1",
                "author": {"role": "assistant"},
                "content": {"parts": ["hello"]}
            }
        });

        assert_eq!(
            parent_message_id_from_stream_value(&event, &event["message"]).as_deref(),
            Some("user-1")
        );
    }

    #[test]
    fn stream_revisions_append_unambiguous_exact_parent_tail() {
        let mut messages = vec![CanonicalMessageView {
            message_id: Some("user-1".to_owned()),
            parent_id: Some("root".to_owned()),
            role: Some("user".to_owned()),
            content_text: Some("question".to_owned()),
        }];
        let revisions = vec![
            StreamMessageRevisionView {
                capture_id: "stream".to_owned(),
                sequence: 0,
                conversation_id: "conversation-a".to_owned(),
                message_id: "assistant-1".to_owned(),
                parent_id: Some("user-1".to_owned()),
                role: Some("assistant".to_owned()),
                content_text: Some("hello".to_owned()),
                source_url: "https://chatgpt.com/backend-api/conversation/stream".to_owned(),
            },
            StreamMessageRevisionView {
                capture_id: "stream".to_owned(),
                sequence: 1,
                conversation_id: "conversation-a".to_owned(),
                message_id: "assistant-1".to_owned(),
                parent_id: Some("user-1".to_owned()),
                role: Some("assistant".to_owned()),
                content_text: Some("hello world".to_owned()),
                source_url: "https://chatgpt.com/backend-api/conversation/stream".to_owned(),
            },
        ];

        let warnings = merge_stream_revisions_into_canonical(&mut messages, &revisions);
        assert!(warnings.is_empty());
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[1].message_id.as_deref(), Some("assistant-1"));
        assert_eq!(messages[1].parent_id.as_deref(), Some("user-1"));
        assert_eq!(messages[1].content_text.as_deref(), Some("hello world"));
    }

    #[test]
    fn stream_revisions_refuse_ambiguous_tail_branch() {
        let mut messages = vec![CanonicalMessageView {
            message_id: Some("user-1".to_owned()),
            parent_id: Some("root".to_owned()),
            role: Some("user".to_owned()),
            content_text: Some("question".to_owned()),
        }];
        let revisions = vec![
            StreamMessageRevisionView {
                capture_id: "stream".to_owned(),
                sequence: 0,
                conversation_id: "conversation-a".to_owned(),
                message_id: "assistant-a".to_owned(),
                parent_id: Some("user-1".to_owned()),
                role: Some("assistant".to_owned()),
                content_text: Some("branch a".to_owned()),
                source_url: "https://chatgpt.com/backend-api/conversation/stream".to_owned(),
            },
            StreamMessageRevisionView {
                capture_id: "stream".to_owned(),
                sequence: 1,
                conversation_id: "conversation-a".to_owned(),
                message_id: "assistant-b".to_owned(),
                parent_id: Some("user-1".to_owned()),
                role: Some("assistant".to_owned()),
                content_text: Some("branch b".to_owned()),
                source_url: "https://chatgpt.com/backend-api/conversation/stream".to_owned(),
            },
        ];

        let warnings = merge_stream_revisions_into_canonical(&mut messages, &revisions);
        assert_eq!(messages.len(), 1);
        assert!(warnings.iter().any(|warning| warning.contains("refusing to guess a branch")));
    }

    #[test]
    fn conflicting_sibling_still_blocks_stream_tail_branch_choice() {
        let mut messages = vec![CanonicalMessageView {
            message_id: Some("user-1".to_owned()),
            parent_id: Some("root".to_owned()),
            role: Some("user".to_owned()),
            content_text: Some("question".to_owned()),
        }];
        let revisions = vec![
            StreamMessageRevisionView {
                capture_id: "stream".to_owned(),
                sequence: 0,
                conversation_id: "conversation-a".to_owned(),
                message_id: "assistant-a".to_owned(),
                parent_id: Some("user-1".to_owned()),
                role: Some("assistant".to_owned()),
                content_text: Some("safe branch".to_owned()),
                source_url: "https://chatgpt.com/backend-api/conversation/stream".to_owned(),
            },
            StreamMessageRevisionView {
                capture_id: "stream".to_owned(),
                sequence: 1,
                conversation_id: "conversation-a".to_owned(),
                message_id: "assistant-b".to_owned(),
                parent_id: Some("user-1".to_owned()),
                role: Some("assistant".to_owned()),
                content_text: Some("conflict one".to_owned()),
                source_url: "https://chatgpt.com/backend-api/conversation/stream".to_owned(),
            },
            StreamMessageRevisionView {
                capture_id: "stream".to_owned(),
                sequence: 2,
                conversation_id: "conversation-a".to_owned(),
                message_id: "assistant-b".to_owned(),
                parent_id: Some("user-1".to_owned()),
                role: Some("assistant".to_owned()),
                content_text: Some("different conflict".to_owned()),
                source_url: "https://chatgpt.com/backend-api/conversation/stream".to_owned(),
            },
        ];

        let warnings = merge_stream_revisions_into_canonical(&mut messages, &revisions);
        assert_eq!(messages.len(), 1);
        assert!(warnings.iter().any(|warning| warning.contains("refusing to guess a branch")));
    }

    #[test]
    fn exact_id_stream_extends_prefix_compatible_canonical_message() {
        let mut messages = vec![CanonicalMessageView {
            message_id: Some("assistant-1".to_owned()),
            parent_id: Some("user-1".to_owned()),
            role: Some("assistant".to_owned()),
            content_text: Some("hello".to_owned()),
        }];
        let streams = vec![StreamReconstructionView {
            capture_id: "stream-capture".to_owned(),
            conversation_id: "conversation-a".to_owned(),
            message_id: Some("assistant-1".to_owned()),
            source_url: "https://chatgpt.com/backend-api/conversation/stream".to_owned(),
            text: "hello world".to_owned(),
            fragment_count: 2,
        }];

        let (linked, unlinked, warnings) =
            merge_exact_id_streams(&mut messages, streams);
        assert_eq!(messages[0].content_text.as_deref(), Some("hello world"));
        assert_eq!(linked.len(), 1);
        assert!(unlinked.is_empty());
        assert!(warnings.is_empty());
    }

    #[test]
    fn conflicting_exact_id_stream_stays_unlinked() {
        let mut messages = vec![CanonicalMessageView {
            message_id: Some("assistant-1".to_owned()),
            parent_id: Some("user-1".to_owned()),
            role: Some("assistant".to_owned()),
            content_text: Some("snapshot answer".to_owned()),
        }];
        let streams = vec![StreamReconstructionView {
            capture_id: "stream-capture".to_owned(),
            conversation_id: "conversation-a".to_owned(),
            message_id: Some("assistant-1".to_owned()),
            source_url: "https://chatgpt.com/backend-api/conversation/stream".to_owned(),
            text: "different answer".to_owned(),
            fragment_count: 2,
        }];

        let (linked, unlinked, warnings) =
            merge_exact_id_streams(&mut messages, streams);
        assert_eq!(
            messages[0].content_text.as_deref(),
            Some("snapshot answer")
        );
        assert!(linked.is_empty());
        assert_eq!(unlinked.len(), 1);
        assert_eq!(warnings.len(), 1);
    }

    #[test]
    fn extracts_simple_conversation_snapshot() {
        let value = serde_json::json!({
            "id": "ignored-root-id",
            "conversation_id": "conversation-a",
            "title": "Fixture",
            "messages": [
                {"id": "m1", "role": "user", "content": "hello"},
                {"id": "m2", "author": {"role": "assistant"}, "content": {"parts": ["hi", " there"]}}
            ]
        });

        let extracted = extract_conversation(
            &value,
            "https://chatgpt.com/backend-api/conversation/fallback",
        )
        .unwrap();
        assert_eq!(extracted.conversation_id, "conversation-a");
        assert_eq!(extracted.title.as_deref(), Some("Fixture"));
        assert_eq!(extracted.messages.len(), 2);
        assert_eq!(extracted.messages[0].content_text.as_deref(), Some("hello"));
        assert_eq!(
            extracted.messages[1].content_text.as_deref(),
            Some("hi there")
        );
    }

    #[test]
    fn prefers_root_conversation_id_over_url_fallback() {
        let value = serde_json::json!({
            "id": "root-conversation",
            "title": "Root ID",
            "messages": [{"role": "user", "content": "hello"}]
        });

        let extracted = extract_conversation(
            &value,
            "https://chatgpt.com/backend-api/conversation/url-fallback",
        )
        .unwrap();
        assert_eq!(extracted.conversation_id, "root-conversation");
    }

    #[test]
    fn canonical_mapping_follows_current_node_and_preserves_branch_choice() {
        let value = serde_json::json!({
            "id": "branch-test",
            "current_node": "assistant-b",
            "mapping": {
                "root": {
                    "parent": null,
                    "children": ["user-1"],
                    "message": null
                },
                "user-1": {
                    "parent": "root",
                    "children": ["assistant-a", "assistant-b"],
                    "message": {
                        "id": "user-1",
                        "author": {"role": "user"},
                        "content": {"parts": ["question"]}
                    }
                },
                "assistant-a": {
                    "parent": "user-1",
                    "children": [],
                    "message": {
                        "id": "assistant-a",
                        "author": {"role": "assistant"},
                        "content": {"parts": ["discarded"]}
                    }
                },
                "assistant-b": {
                    "parent": "user-1",
                    "children": [],
                    "message": {
                        "id": "assistant-b",
                        "author": {"role": "assistant"},
                        "content": {"parts": ["chosen"]}
                    }
                }
            }
        });

        let (basis, current_node, messages, warnings) =
            canonical_messages_from_snapshot(&value);
        assert_eq!(basis, "mapping_current_node");
        assert_eq!(current_node.as_deref(), Some("assistant-b"));
        assert!(warnings.is_empty());
        assert_eq!(
            messages,
            vec![
                CanonicalMessageView {
                    message_id: Some("user-1".to_owned()),
                    parent_id: Some("root".to_owned()),
                    role: Some("user".to_owned()),
                    content_text: Some("question".to_owned()),
                },
                CanonicalMessageView {
                    message_id: Some("assistant-b".to_owned()),
                    parent_id: Some("user-1".to_owned()),
                    role: Some("assistant".to_owned()),
                    content_text: Some("chosen".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn extracts_mapping_messages_and_url_conversation_id() {
        let value = serde_json::json!({
            "title": "Mapped",
            "mapping": {
                "node-a": {
                    "message": {
                        "author": {"role": "assistant"},
                        "content": {"parts": ["mapped"]}
                    }
                }
            }
        });

        let extracted = extract_conversation(
            &value,
            "https://chatgpt.com/backend-api/conversation/url-conversation",
        )
        .unwrap();
        assert_eq!(extracted.conversation_id, "url-conversation");
        assert_eq!(extracted.messages.len(), 1);
        assert_eq!(extracted.messages[0].message_id.as_deref(), Some("node-a"));
        assert_eq!(extracted.messages[0].role.as_deref(), Some("assistant"));
    }
}
