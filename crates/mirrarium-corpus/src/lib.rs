use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OpenFlags};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct CorpusStats {
    pub stream_captures: u64,
    pub stream_events: u64,
    pub json_stream_events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SseEvent {
    event_name: Option<String>,
    data: String,
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

        CREATE TABLE IF NOT EXISTS stream_captures (
            capture_id TEXT PRIMARY KEY,
            source_url TEXT NOT NULL,
            privacy_class TEXT NOT NULL,
            source_body_hash TEXT NOT NULL,
            event_count INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS stream_events (
            capture_id TEXT NOT NULL
                REFERENCES stream_captures(capture_id) ON DELETE CASCADE,
            sequence INTEGER NOT NULL,
            event_name TEXT,
            data TEXT NOT NULL,
            json_valid INTEGER NOT NULL,
            PRIMARY KEY (capture_id, sequence)
        );
        "#,
    )?;

    let raw = Connection::open_with_flags(
        &raw_database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening raw ledger {}", raw_database.display()))?;

    let sources = {
        let mut statement = raw.prepare(
            r#"
            SELECT capture_id, url, privacy_class, body_hash
            FROM captures
            WHERE body_hash IS NOT NULL
              AND lower(mime_type) LIKE 'text/event-stream%'
            ORDER BY captured_at_ms, capture_id
            "#,
        )?;

        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;

        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };

    let transaction = corpus.transaction()?;
    transaction.execute("DELETE FROM stream_events", [])?;
    transaction.execute("DELETE FROM stream_captures", [])?;

    for (capture_id, source_url, privacy_class, body_hash) in sources {
        let source_path = object_path(raw_root, &privacy_class, &body_hash)?;
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

        for (sequence, event) in events.into_iter().enumerate() {
            let json_valid = serde_json::from_str::<Value>(&event.data).is_ok();
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
                    if json_valid { 1_i64 } else { 0_i64 },
                ],
            )?;
        }
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
    })
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
}
