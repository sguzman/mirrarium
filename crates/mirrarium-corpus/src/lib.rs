use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, Transaction};
use serde::Serialize;
use serde_json::Value;
use url::Url;

#[derive(Debug, Clone, Serialize)]
pub struct CorpusStats {
    pub stream_captures: u64,
    pub stream_events: u64,
    pub json_stream_events: u64,
    pub conversation_snapshots: u64,
    pub message_observations: u64,
    pub stream_reconstructions: u64,
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
pub struct StreamReconstructionView {
    pub capture_id: String,
    pub conversation_id: String,
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
    let mut corpus = Connection::open(&corpus_database)
        .with_context(|| format!("opening {}", corpus_database.display()))?;
    harden_file(&corpus_database)?;

    corpus.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA foreign_keys = ON;

        DROP TABLE IF EXISTS message_observations;
        DROP TABLE IF EXISTS conversation_snapshots;
        DROP TABLE IF EXISTS stream_reconstructions;
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

        CREATE TABLE stream_reconstructions (
            capture_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            source_url TEXT NOT NULL,
            text TEXT NOT NULL,
            fragment_count INTEGER NOT NULL,
            PRIMARY KEY (capture_id, conversation_id)
        );
        "#,
    )?;

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

    for table in [
        "stream_captures",
        "stream_events",
        "conversation_snapshots",
        "message_observations",
        "stream_reconstructions",
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

    let mut stream_statement = connection.prepare(
        r#"
        SELECT
            capture_id,
            conversation_id,
            source_url,
            text,
            fragment_count
        FROM stream_reconstructions
        WHERE conversation_id = ?1
        ORDER BY rowid
        "#,
    )?;
    let streams = stream_statement
        .query_map([conversation_id], |row| {
            Ok(StreamReconstructionView {
                capture_id: row.get(0)?,
                conversation_id: row.get(1)?,
                source_url: row.get(2)?,
                text: row.get(3)?,
                fragment_count: row.get::<_, i64>(4)? as u64,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok(Some(ConversationView {
        summary,
        messages,
        streams,
    }))
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

    for table in [
        "stream_captures",
        "stream_events",
        "conversation_snapshots",
        "message_observations",
        "stream_reconstructions",
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
    let source_path = object_path(raw_root, privacy_class, body_hash)?;
    let bytes = fs::read(&source_path)
        .with_context(|| format!("reading stream body {}", source_path.display()))?;
    let text = std::str::from_utf8(&bytes)
        .with_context(|| format!("stream body is not UTF-8: {}", source_path.display()))?;
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

    let mut reconstructions: BTreeMap<String, (String, u64)> = BTreeMap::new();

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
            let entry = reconstructions
                .entry(conversation_id.to_owned())
                .or_insert_with(|| (String::new(), 0));
            entry.0.push_str(delta);
            entry.1 += 1;
        }

        let message_value = value.get("message").unwrap_or(&value);
        if let Some(mut message) = extract_message(message_value, None) {
            if message.conversation_id.is_none() {
                message.conversation_id = conversation_id;
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

    for (conversation_id, (text, fragment_count)) in reconstructions {
        transaction.execute(
            r#"
            INSERT INTO stream_reconstructions
                (capture_id, conversation_id, source_url, text, fragment_count)
            VALUES
                (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                capture_id,
                conversation_id,
                source_url,
                text,
                fragment_count as i64,
            ],
        )?;
    }

    Ok(())
}

fn derive_json_capture(
    transaction: &Transaction<'_>,
    raw_root: &Path,
    capture_id: &str,
    source_url: &str,
    privacy_class: &str,
    body_hash: &str,
) -> Result<()> {
    let source_path = object_path(raw_root, privacy_class, body_hash)?;
    let bytes = fs::read(&source_path)
        .with_context(|| format!("reading JSON body {}", source_path.display()))?;
    let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
        return Ok(());
    };

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
