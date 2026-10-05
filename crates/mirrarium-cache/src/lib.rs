use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CacheCandidate {
    pub url: String,
    pub resource_type: String,
    pub mime_type: String,
    pub body_hash: String,
    pub body_bytes: u64,
    pub cache_control: Option<String>,
    pub capture_count: u64,
    pub distinct_body_hashes: u64,
    pub eligible: bool,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CacheStats {
    pub observed_public_get_captures: u64,
    pub unique_urls: u64,
    pub eligible_urls: u64,
    pub ineligible_urls: u64,
    pub conflicting_urls: u64,
}

#[derive(Debug, Clone)]
struct Aggregate {
    resource_type: String,
    mime_type: String,
    body_hash: String,
    body_bytes: u64,
    cache_control: Option<String>,
    capture_count: u64,
    body_hashes: BTreeSet<String>,
}

pub fn candidates(raw_root: impl AsRef<Path>, limit: u64) -> Result<Vec<CacheCandidate>> {
    anyhow::ensure!(limit > 0, "cache candidate limit must be greater than zero");
    let mut candidates = build_inventory(raw_root)?;
    candidates.sort_by(|left, right| {
        right
            .eligible
            .cmp(&left.eligible)
            .then_with(|| left.url.cmp(&right.url))
    });
    candidates.truncate(limit.try_into().unwrap_or(usize::MAX));
    Ok(candidates)
}

pub fn stats(raw_root: impl AsRef<Path>) -> Result<CacheStats> {
    let candidates = build_inventory(raw_root)?;
    let observed_public_get_captures = candidates.iter().map(|item| item.capture_count).sum();
    let eligible_urls = candidates.iter().filter(|item| item.eligible).count() as u64;
    let conflicting_urls = candidates
        .iter()
        .filter(|item| item.distinct_body_hashes > 1)
        .count() as u64;
    let unique_urls = candidates.len() as u64;

    Ok(CacheStats {
        observed_public_get_captures,
        unique_urls,
        eligible_urls,
        ineligible_urls: unique_urls.saturating_sub(eligible_urls),
        conflicting_urls,
    })
}

fn build_inventory(raw_root: impl AsRef<Path>) -> Result<Vec<CacheCandidate>> {
    let database = raw_root.as_ref().join("ledger.sqlite3");
    anyhow::ensure!(
        database.is_file(),
        "raw ledger does not exist: {}",
        database.display()
    );

    let connection = Connection::open_with_flags(
        &database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening raw ledger {}", database.display()))?;

    let mut statement = connection.prepare(
        r#"
        SELECT
            url,
            resource_type,
            mime_type,
            body_hash,
            body_bytes,
            cache_control
        FROM captures
        WHERE privacy_class = 'public'
          AND lower(method) = 'get'
          AND status = 200
          AND body_hash IS NOT NULL
          AND body_error IS NULL
        ORDER BY url, captured_at_ms, rowid
        "#,
    )?;

    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, Option<String>>(5)?,
        ))
    })?;

    let mut aggregates: BTreeMap<String, Aggregate> = BTreeMap::new();
    for row in rows {
        let (url, resource_type, mime_type, body_hash, body_bytes, cache_control) = row?;
        let body_bytes: u64 = body_bytes.try_into().context("negative body byte count")?;
        let aggregate = aggregates.entry(url).or_insert_with(|| Aggregate {
            resource_type: resource_type.clone(),
            mime_type: mime_type.clone(),
            body_hash: body_hash.clone(),
            body_bytes,
            cache_control: cache_control.clone(),
            capture_count: 0,
            body_hashes: BTreeSet::new(),
        });

        aggregate.resource_type = resource_type;
        aggregate.mime_type = mime_type;
        aggregate.body_hash = body_hash.clone();
        aggregate.body_bytes = body_bytes;
        aggregate.cache_control = cache_control;
        aggregate.capture_count = aggregate
            .capture_count
            .checked_add(1)
            .context("cache capture count overflow")?;
        aggregate.body_hashes.insert(body_hash);
    }

    Ok(aggregates
        .into_iter()
        .map(|(url, aggregate)| candidate_from_aggregate(url, aggregate))
        .collect())
}

fn candidate_from_aggregate(url: String, aggregate: Aggregate) -> CacheCandidate {
    let mut reasons = Vec::new();
    let resource_type = aggregate.resource_type.to_ascii_lowercase();
    if !matches!(
        resource_type.as_str(),
        "script" | "stylesheet" | "font" | "image"
    ) {
        reasons.push("resource_type_not_static".to_owned());
    }

    let cache_control_lower = aggregate
        .cache_control
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let directives: Vec<&str> = cache_control_lower
        .split(',')
        .map(str::trim)
        .filter(|directive| !directive.is_empty())
        .collect();

    if directives
        .iter()
        .any(|directive| *directive == "no-store" || directive.starts_with("private"))
    {
        reasons.push("cache_control_forbids_replay".to_owned());
    }
    if !directives.iter().any(|directive| *directive == "immutable") {
        reasons.push("cache_control_not_immutable".to_owned());
    }

    let distinct_body_hashes = aggregate.body_hashes.len() as u64;
    if distinct_body_hashes != 1 {
        reasons.push("url_observed_with_multiple_body_hashes".to_owned());
    }

    CacheCandidate {
        url,
        resource_type: aggregate.resource_type,
        mime_type: aggregate.mime_type,
        body_hash: aggregate.body_hash,
        body_bytes: aggregate.body_bytes,
        cache_control: aggregate.cache_control,
        capture_count: aggregate.capture_count,
        distinct_body_hashes,
        eligible: reasons.is_empty(),
        reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use tempfile::tempdir;

    fn open_fixture() -> (tempfile::TempDir, Connection) {
        let directory = tempdir().unwrap();
        let database = directory.path().join("ledger.sqlite3");
        let connection = Connection::open(database).unwrap();
        connection
            .execute_batch(
                r#"
                CREATE TABLE captures (
                    capture_id TEXT PRIMARY KEY,
                    captured_at_ms INTEGER NOT NULL,
                    method TEXT NOT NULL,
                    url TEXT NOT NULL,
                    status INTEGER NOT NULL,
                    mime_type TEXT NOT NULL,
                    resource_type TEXT NOT NULL,
                    privacy_class TEXT NOT NULL,
                    body_hash TEXT,
                    body_bytes INTEGER NOT NULL,
                    cache_control TEXT,
                    body_error TEXT
                );
                "#,
            )
            .unwrap();
        (directory, connection)
    }

    fn insert_capture(
        connection: &Connection,
        id: &str,
        timestamp: i64,
        url: &str,
        hash: &str,
        cache_control: Option<&str>,
    ) {
        connection
            .execute(
                r#"
                INSERT INTO captures (
                    capture_id,
                    captured_at_ms,
                    method,
                    url,
                    status,
                    mime_type,
                    resource_type,
                    privacy_class,
                    body_hash,
                    body_bytes,
                    cache_control,
                    body_error
                ) VALUES (?1, ?2, 'GET', ?3, 200, 'application/javascript',
                          'Script', 'public', ?4, 12, ?5, NULL)
                "#,
                params![id, timestamp, url, hash, cache_control],
            )
            .unwrap();
    }

    #[test]
    fn stable_immutable_static_url_is_eligible() {
        let (directory, connection) = open_fixture();
        insert_capture(
            &connection,
            "one",
            1,
            "https://chatgpt.com/_next/static/app.js",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("public, max-age=31536000, immutable"),
        );
        insert_capture(
            &connection,
            "two",
            2,
            "https://chatgpt.com/_next/static/app.js",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("public, max-age=31536000, immutable"),
        );

        let items = candidates(directory.path(), 20).unwrap();
        assert_eq!(items.len(), 1);
        assert!(items[0].eligible);
        assert_eq!(items[0].capture_count, 2);
        assert_eq!(items[0].distinct_body_hashes, 1);
        assert!(items[0].reasons.is_empty());
    }

    #[test]
    fn same_url_with_multiple_hashes_is_never_eligible() {
        let (directory, connection) = open_fixture();
        insert_capture(
            &connection,
            "one",
            1,
            "https://chatgpt.com/_next/static/app.js",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("public, max-age=31536000, immutable"),
        );
        insert_capture(
            &connection,
            "two",
            2,
            "https://chatgpt.com/_next/static/app.js",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            Some("public, max-age=31536000, immutable"),
        );

        let item = candidates(directory.path(), 20).unwrap().pop().unwrap();
        assert!(!item.eligible);
        assert_eq!(item.distinct_body_hashes, 2);
        assert!(item
            .reasons
            .iter()
            .any(|reason| reason == "url_observed_with_multiple_body_hashes"));
    }

    #[test]
    fn immutable_directive_is_required() {
        let (directory, connection) = open_fixture();
        insert_capture(
            &connection,
            "one",
            1,
            "https://chatgpt.com/_next/static/app.js",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("public, max-age=3600"),
        );

        let item = candidates(directory.path(), 20).unwrap().pop().unwrap();
        assert!(!item.eligible);
        assert!(item
            .reasons
            .iter()
            .any(|reason| reason == "cache_control_not_immutable"));
    }
}
