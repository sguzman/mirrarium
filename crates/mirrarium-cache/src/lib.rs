use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use sha2::{Digest, Sha256};
use url::Url;

const MAX_REPLAY_BODY_BYTES: u64 = 16 * 1024 * 1024;

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
pub struct ReplayEntry {
    pub url: String,
    pub resource_type: String,
    pub mime_type: String,
    pub body_hash: String,
    pub body_bytes: u64,
    pub cache_control: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    #[serde(skip)]
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ReplayStats {
    pub attempts: u64,
    pub hits: u64,
    pub misses: u64,
    pub lookup_errors: u64,
    pub timeouts: u64,
    pub fulfill_errors: u64,
    pub replayed_bytes: u64,
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

pub fn replay_stats(raw_root: impl AsRef<Path>) -> Result<ReplayStats> {
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

    let table_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'cache_replay_events'",
        [],
        |row| row.get(0),
    )?;
    if table_count == 0 {
        return Ok(ReplayStats::default());
    }

    let mut stats = ReplayStats::default();
    let mut statement = connection.prepare(
        r#"
        SELECT outcome, COUNT(*), COALESCE(SUM(body_bytes), 0)
        FROM cache_replay_events
        GROUP BY outcome
        "#,
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;
    for row in rows {
        let (outcome, count, bytes) = row?;
        let count: u64 = count.try_into().context("negative replay event count")?;
        let bytes: u64 = bytes.try_into().context("negative replay byte count")?;
        stats.attempts = stats
            .attempts
            .checked_add(count)
            .context("replay attempt count overflow")?;
        match outcome.as_str() {
            "hit" => {
                stats.hits = count;
                stats.replayed_bytes = bytes;
            }
            "miss" => stats.misses = count,
            "lookup_error" => stats.lookup_errors = count,
            "timeout" => stats.timeouts = count,
            "fulfill_error" => stats.fulfill_errors = count,
            _ => {}
        }
    }
    Ok(stats)
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

pub fn lookup(
    raw_root: impl AsRef<Path>,
    raw_url: &str,
    resource_type: &str,
) -> Result<Option<ReplayEntry>> {
    if !matches!(
        resource_type.to_ascii_lowercase().as_str(),
        "script" | "stylesheet"
    ) {
        return Ok(None);
    }

    let Some(url) = replayable_url(raw_url) else {
        return Ok(None);
    };
    let root = raw_root.as_ref();
    let database = root.join("ledger.sqlite3");
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

    let mut hashes = BTreeSet::new();
    let mut statement = connection.prepare(
        r#"
        SELECT body_hash
        FROM captures
        WHERE url = ?1
          AND privacy_class = 'public'
          AND lower(method) = 'get'
          AND status = 200
          AND body_hash IS NOT NULL
          AND body_error IS NULL
        ORDER BY captured_at_ms, rowid
        "#,
    )?;
    let rows = statement.query_map([url.as_str()], |row| row.get::<_, String>(0))?;
    for row in rows {
        hashes.insert(row?);
    }
    if hashes.len() != 1 {
        return Ok(None);
    }

    let row = connection
        .query_row(
            r#"
            SELECT
                resource_type,
                mime_type,
                body_hash,
                body_bytes,
                cache_control,
                etag,
                last_modified
            FROM captures
            WHERE url = ?1
              AND privacy_class = 'public'
              AND lower(method) = 'get'
              AND status = 200
              AND body_hash IS NOT NULL
              AND body_error IS NULL
            ORDER BY captured_at_ms DESC, rowid DESC
            LIMIT 1
            "#,
            [url.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            },
        )
        .optional()?;
    let Some((
        stored_resource_type,
        mime_type,
        body_hash,
        body_bytes,
        cache_control,
        etag,
        last_modified,
    )) = row
    else {
        return Ok(None);
    };

    if !stored_resource_type.eq_ignore_ascii_case(resource_type) {
        return Ok(None);
    }
    if !cache_control_allows_replay(cache_control.as_deref()) {
        return Ok(None);
    }

    let body_bytes: u64 = body_bytes.try_into().context("negative body byte count")?;
    if body_bytes > MAX_REPLAY_BODY_BYTES {
        return Ok(None);
    }
    if body_hash.len() != 64 || !body_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("invalid public CAS hash for replay candidate");
    }

    let expected_relative_path = public_object_relative_path(&body_hash);
    let indexed_object = connection
        .query_row(
            r#"
            SELECT bytes, relative_path
            FROM objects
            WHERE storage_class = 'public'
              AND hash = ?1
            "#,
            [&body_hash],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let Some((indexed_bytes, indexed_relative_path)) = indexed_object else {
        anyhow::bail!("public replay object is missing from object index");
    };
    let indexed_bytes: u64 = indexed_bytes
        .try_into()
        .context("negative indexed object byte count")?;
    anyhow::ensure!(
        indexed_bytes == body_bytes,
        "public replay object byte count disagrees with capture"
    );
    anyhow::ensure!(
        Path::new(&indexed_relative_path) == expected_relative_path,
        "public replay object path disagrees with CAS layout"
    );
    anyhow::ensure!(
        expected_relative_path
            .components()
            .all(|component| !matches!(component, Component::ParentDir | Component::RootDir | Component::Prefix(_))),
        "invalid public replay object path"
    );

    let object_path = root.join(&expected_relative_path);
    let body = fs::read(&object_path)
        .with_context(|| format!("reading replay object {}", object_path.display()))?;
    anyhow::ensure!(
        body.len() as u64 == body_bytes,
        "public replay object length verification failed"
    );
    let actual_hash = format!("{:x}", Sha256::digest(&body));
    anyhow::ensure!(
        actual_hash == body_hash,
        "public replay object hash verification failed"
    );

    Ok(Some(ReplayEntry {
        url: url.to_string(),
        resource_type: stored_resource_type,
        mime_type,
        body_hash,
        body_bytes,
        cache_control,
        etag,
        last_modified,
        body,
    }))
}

fn replayable_url(raw_url: &str) -> Option<Url> {
    let url = Url::parse(raw_url).ok()?;
    if url.scheme() != "https"
        || !matches!(url.host_str(), Some("chatgpt.com" | "chat.openai.com"))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    Some(url)
}

fn cache_control_allows_replay(cache_control: Option<&str>) -> bool {
    let normalized = cache_control.unwrap_or_default().to_ascii_lowercase();
    let directives: Vec<&str> = normalized
        .split(',')
        .map(str::trim)
        .filter(|directive| !directive.is_empty())
        .collect();
    !directives
        .iter()
        .any(|directive| *directive == "no-store" || directive.starts_with("private"))
        && directives.iter().any(|directive| *directive == "immutable")
}

fn public_object_relative_path(hash: &str) -> PathBuf {
    PathBuf::from("public")
        .join("objects")
        .join(&hash[..2])
        .join(hash)
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
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    fn open_fixture() -> (tempfile::TempDir, Connection) {
        let directory = tempdir().unwrap();
        let database = directory.path().join("ledger.sqlite3");
        let connection = Connection::open(database).unwrap();
        connection
            .execute_batch(
                r#"
                CREATE TABLE objects (
                    storage_class TEXT NOT NULL,
                    hash TEXT NOT NULL,
                    bytes INTEGER NOT NULL,
                    relative_path TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (storage_class, hash)
                );
                CREATE TABLE cache_replay_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    observed_at_ms INTEGER NOT NULL,
                    url TEXT NOT NULL,
                    resource_type TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    body_bytes INTEGER NOT NULL
                );
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
                    etag TEXT,
                    last_modified TEXT,
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
                    etag,
                    last_modified,
                    body_error
                ) VALUES (?1, ?2, 'GET', ?3, 200, 'application/javascript',
                          'Script', 'public', ?4, 12, ?5, '"fixture-etag"', NULL, NULL)
                "#,
                params![id, timestamp, url, hash, cache_control],
            )
            .unwrap();
    }

    #[test]
    fn replay_stats_aggregate_outcomes_and_bytes() {
        let (directory, connection) = open_fixture();
        for (outcome, bytes) in [
            ("hit", 120_i64),
            ("hit", 80),
            ("miss", 0),
            ("timeout", 0),
            ("lookup_error", 0),
            ("fulfill_error", 0),
        ] {
            connection
                .execute(
                    "INSERT INTO cache_replay_events (observed_at_ms, url, resource_type, outcome, body_bytes) VALUES (1, 'https://chatgpt.com/_next/static/app.js', 'Script', ?1, ?2)",
                    params![outcome, bytes],
                )
                .unwrap();
        }

        assert_eq!(
            replay_stats(directory.path()).unwrap(),
            ReplayStats {
                attempts: 6,
                hits: 2,
                misses: 1,
                lookup_errors: 1,
                timeouts: 1,
                fulfill_errors: 1,
                replayed_bytes: 200,
            }
        );
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
    fn replayable_url_accepts_exact_chatgpt_https_without_query() {
        assert!(replayable_url("https://chatgpt.com/_next/static/replay.js").is_some());
        assert!(replayable_url("https://chat.openai.com/_next/static/replay.css").is_some());
        assert!(replayable_url("http://chatgpt.com/_next/static/replay.js").is_none());
        assert!(replayable_url("https://chatgpt.com/_next/static/replay.js?v=1").is_none());
    }

    #[test]
    fn lookup_reads_and_verifies_public_replay_object() {
        let (directory, connection) = open_fixture();
        let body = b"fixture body";
        let hash = format!("{:x}", Sha256::digest(body));
        let relative_path = public_object_relative_path(&hash);
        let object_path = directory.path().join(&relative_path);
        fs::create_dir_all(object_path.parent().unwrap()).unwrap();
        fs::write(&object_path, body).unwrap();
        connection
            .execute(
                "INSERT INTO objects (storage_class, hash, bytes, relative_path, created_at_ms) VALUES ('public', ?1, ?2, ?3, 1)",
                params![hash, body.len() as i64, relative_path.to_string_lossy().to_string()],
            )
            .unwrap();
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
                    etag,
                    last_modified,
                    body_error
                ) VALUES (
                    'replay',
                    1,
                    'GET',
                    'https://chatgpt.com/_next/static/replay.js',
                    200,
                    'application/javascript',
                    'Script',
                    'public',
                    ?1,
                    ?2,
                    'public, max-age=31536000, immutable',
                    '"fixture-etag"',
                    NULL,
                    NULL
                )
                "#,
                params![hash, body.len() as i64],
            )
            .unwrap();

        let replay = lookup(
            directory.path(),
            "https://chatgpt.com/_next/static/replay.js",
            "Script",
        )
        .unwrap()
        .unwrap();
        assert_eq!(replay.body, body);
        assert_eq!(replay.body_hash, hash);
        assert_eq!(replay.etag.as_deref(), Some("\"fixture-etag\""));
    }

    #[test]
    fn lookup_rejects_query_urls_and_resource_type_mismatch() {
        let (directory, _connection) = open_fixture();
        assert!(lookup(
            directory.path(),
            "https://chatgpt.com/_next/static/replay.js?v=1",
            "Script"
        )
        .unwrap()
        .is_none());
        assert!(lookup(
            directory.path(),
            "https://chatgpt.com/_next/static/replay.js",
            "Fetch"
        )
        .unwrap()
        .is_none());
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
