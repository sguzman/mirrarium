use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;
use mirrarium_store::{open_raw_ledger_read_only, read_verified_object};
use url::Url;

const MAX_REPLAY_BODY_BYTES: u64 = 16 * 1024 * 1024;

pub struct CacheReader {
    root: PathBuf,
    connection: Connection,
}

impl CacheReader {
    pub fn open(raw_root: impl AsRef<Path>) -> Result<Self> {
        let root = raw_root.as_ref().to_path_buf();
        let connection = open_raw_ledger_read_only(&root)?;
        Ok(Self { root, connection })
    }

    pub fn private_lookup(
    raw_root: impl AsRef<Path>,
    raw_url: &str,
) -> Result<Option<PrivateReadEntry>> {
    CacheReader::open(raw_root)?.private_lookup(raw_url)
}

fn private_lookup_with_connection(
    root: &Path,
    connection: &Connection,
    raw_url: &str,
) -> Result<Option<PrivateReadEntry>> {
    let Some(url) = private_revalidation_url(raw_url) else {
        return false;
    };
    match resource_type.to_ascii_lowercase().as_str() {
        "document" => true,
        "fetch" | "xhr" => url.path().to_ascii_lowercase().starts_with("/backend-api/"),
        _ => false,
    }
}

pub fn private_reads(
    raw_root: impl AsRef<Path>,
    limit: u64,
) -> Result<Vec<PrivateReadProfile>> {
    anyhow::ensure!(limit > 0, "private-read limit must be greater than zero");
    let connection = open_raw_ledger_read_only(raw_root.as_ref())?;

    let mut statement = connection.prepare(
        r#"
        SELECT
            captured_at_ms,
            url,
            mime_type,
            body_hash,
            body_bytes,
            etag,
            last_modified,
            cache_control
        FROM captures
        WHERE privacy_class = 'private'
          AND lower(method) = 'get'
          AND status = 200
          AND body_hash IS NOT NULL
          AND body_error IS NULL
          AND (
              lower(mime_type) LIKE '%json%'
              OR lower(mime_type) LIKE 'text/html%'
          )
        ORDER BY url, captured_at_ms, rowid
        "#,
    )?;

    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
        ))
    })?;

    let mut aggregates: BTreeMap<String, PrivateReadAggregate> = BTreeMap::new();
    for row in rows {
        let (
            captured_at_ms,
            url,
            mime_type,
            body_hash,
            body_bytes,
            etag,
            last_modified,
            cache_control,
        ) = row?;
        let captured_at_ms: u64 = captured_at_ms
            .try_into()
            .context("negative private-read timestamp")?;
        let body_bytes: u64 = body_bytes
            .try_into()
            .context("negative private-read body byte count")?;

        let aggregate = aggregates.entry(url).or_insert_with(|| PrivateReadAggregate {
            mime_type: mime_type.clone(),
            capture_count: 0,
            body_hashes: BTreeSet::new(),
            latest_body_hash: body_hash.clone(),
            latest_body_bytes: body_bytes,
            latest_captured_at_ms: captured_at_ms,
            latest_etag: etag.clone(),
            latest_last_modified: last_modified.clone(),
            latest_cache_control: cache_control.clone(),
        });
        aggregate.capture_count = aggregate
            .capture_count
            .checked_add(1)
            .context("private-read capture count overflow")?;
        aggregate.body_hashes.insert(body_hash.clone());

        if captured_at_ms >= aggregate.latest_captured_at_ms {
            aggregate.mime_type = mime_type;
            aggregate.latest_body_hash = body_hash;
            aggregate.latest_body_bytes = body_bytes;
            aggregate.latest_captured_at_ms = captured_at_ms;
            aggregate.latest_etag = etag;
            aggregate.latest_last_modified = last_modified;
            aggregate.latest_cache_control = cache_control;
        }
    }

    let mut profiles: Vec<PrivateReadProfile> = aggregates
        .into_iter()
        .map(|(url, aggregate)| private_read_profile(url, aggregate))
        .collect();
    profiles.sort_by(|left, right| {
        right
            .revalidation_candidate
            .cmp(&left.revalidation_candidate)
            .then_with(|| right.capture_count.cmp(&left.capture_count))
            .then_with(|| left.url.cmp(&right.url))
    });
    profiles.truncate(limit.try_into().unwrap_or(usize::MAX));
    Ok(profiles)
}

fn private_read_profile(
    url: String,
    aggregate: PrivateReadAggregate,
) -> PrivateReadProfile {
    let has_validator =
        aggregate.latest_etag.is_some() || aggregate.latest_last_modified.is_some();
    let mut reasons = Vec::new();

    if !has_validator {
        reasons.push("missing_http_validator".to_owned());
    }

    let cache_control = aggregate
        .latest_cache_control
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if cache_control
        .split(',')
        .map(str::trim)
        .any(|directive| directive == "no-store")
    {
        reasons.push("cache_control_no_store".to_owned());
    }

    match Url::parse(&url) {
        Ok(parsed) => {
            if parsed.scheme() != "https"
                || !matches!(parsed.host_str(), Some("chatgpt.com" | "chat.openai.com"))
            {
                reasons.push("unsupported_private_origin".to_owned());
            }
            if parsed
                .query_pairs()
                .any(|(_, value)| value.as_ref() == "[REDACTED]")
            {
                reasons.push("redacted_query_identity".to_owned());
            }
        }
        Err(_) => reasons.push("invalid_url".to_owned()),
    }

    let distinct_body_hashes = aggregate.body_hashes.len() as u64;
    PrivateReadProfile {
        url,
        mime_type: aggregate.mime_type,
        capture_count: aggregate.capture_count,
        distinct_body_hashes,
        latest_body_hash: aggregate.latest_body_hash,
        latest_body_bytes: aggregate.latest_body_bytes,
        latest_captured_at_ms: aggregate.latest_captured_at_ms,
        latest_etag: aggregate.latest_etag,
        latest_last_modified: aggregate.latest_last_modified,
        latest_cache_control: aggregate.latest_cache_control,
        has_validator,
        stable_so_far: distinct_body_hashes == 1,
        revalidation_candidate: reasons.is_empty(),
        reasons,
    }
}

pub fn private_lookup(
    raw_root: impl AsRef<Path>,
    raw_url: &str,
) -> Result<Option<PrivateReadEntry>> {
    let Some(url) = private_revalidation_url(raw_url) else {
        return Ok(None);
    };
    let row = connection
        .query_row(
            r#"
            SELECT
                captured_at_ms,
                mime_type,
                body_hash,
                body_bytes,
                cache_control,
                etag,
                last_modified
            FROM captures
            WHERE url = ?1
              AND privacy_class = 'private'
              AND lower(method) = 'get'
              AND status = 200
              AND body_hash IS NOT NULL
              AND body_error IS NULL
              AND (
                  lower(mime_type) LIKE '%json%'
                  OR lower(mime_type) LIKE 'text/html%'
              )
            ORDER BY captured_at_ms DESC, rowid DESC
            LIMIT 1
            "#,
            [url.as_str()],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
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
        captured_at_ms,
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

    if etag.is_none() && last_modified.is_none() {
        return Ok(None);
    }
    if cache_control_has_no_store(cache_control.as_deref()) {
        return Ok(None);
    }

    let captured_at_ms: u64 = captured_at_ms
        .try_into()
        .context("negative private-read capture timestamp")?;
    let body_bytes: u64 = body_bytes
        .try_into()
        .context("negative private-read body byte count")?;
    if body_bytes > MAX_REPLAY_BODY_BYTES {
        return Ok(None);
    }
    if body_hash.len() != 64 || !body_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("invalid private CAS hash for revalidation candidate");
    }

    let expected_relative_path = private_object_relative_path(&body_hash);
    let indexed_object = connection
        .query_row(
            r#"
            SELECT bytes, relative_path
            FROM objects
            WHERE storage_class = 'private'
              AND hash = ?1
            "#,
            [&body_hash],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let Some((indexed_bytes, indexed_relative_path)) = indexed_object else {
        anyhow::bail!("private revalidation object is missing from object index");
    };
    let indexed_bytes: u64 = indexed_bytes
        .try_into()
        .context("negative indexed private object byte count")?;
    anyhow::ensure!(
        indexed_bytes == body_bytes,
        "private revalidation object byte count disagrees with capture"
    );
    anyhow::ensure!(
        Path::new(&indexed_relative_path) == expected_relative_path,
        "private revalidation object path disagrees with CAS layout"
    );
    anyhow::ensure!(
        expected_relative_path
            .components()
            .all(|component| !matches!(component, Component::ParentDir | Component::RootDir | Component::Prefix(_))),
        "invalid private revalidation object path"
    );

    let body = read_verified_object(root, "private", &body_hash)?;
    anyhow::ensure!(
        body.len() as u64 == body_bytes,
        "private revalidation object length verification failed"
    );

    Ok(Some(PrivateReadEntry {
        url: url.to_string(),
        mime_type,
        body_hash,
        body_bytes,
        captured_at_ms,
        cache_control,
        etag,
        last_modified,
        body,
    }))
}

fn private_revalidation_url(raw_url: &str) -> Option<Url> {
    let url = Url::parse(raw_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    let path = url.path().to_ascii_lowercase();

    if url.scheme() != "https"
        || !matches!(host.as_str(), "chatgpt.com" | "chat.openai.com")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || !private_query_identity_is_safe(&url)
        || path == "/api/auth"
        || path.starts_with("/api/auth/")
        || path == "/auth"
        || path.starts_with("/auth/")
        || path.starts_with("/backend-api/auth/")
        || path.contains("/oauth/")
        || path.ends_with("/oauth")
        || path.contains("/login")
    {
        return None;
    }

    Some(url)
}

fn private_query_identity_is_safe(url: &Url) -> bool {
    url.query_pairs().all(|(key, value)| {
        !is_sensitive_query_key(&key)
            && value.as_ref() != "[REDACTED]"
    })
}

fn is_sensitive_query_key(key: &str) -> bool {
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

fn cache_control_has_no_store(cache_control: Option<&str>) -> bool {
    cache_control
        .unwrap_or_default()
        .to_ascii_lowercase()
        .split(',')
        .map(|directive| directive.trim().to_owned())
        .any(|directive| directive == "no-store")
}

pub fn lookup(
    raw_root: impl AsRef<Path>,
    raw_url: &str,
    resource_type: &str,
) -> Result<Option<ReplayEntry>> {
    CacheReader::open(raw_root)?.lookup(raw_url, resource_type)
}

fn lookup_with_connection(
    root: &Path,
    connection: &Connection,
    raw_url: &str,
    resource_type: &str,
) -> Result<Option<ReplayEntry>> {
    if !matches!(
        resource_type.to_ascii_lowercase().as_str(),
        "script" | "stylesheet" | "image" | "font"
    ) {
        return Ok(None);
    }

    let Some(url) = replayable_url(raw_url) else {
        return Ok(None);
    };
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

    let body = read_verified_object(root, "public", &body_hash)?;
    anyhow::ensure!(
        body.len() as u64 == body_bytes,
        "public replay object length verification failed"
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
    let host = url.host_str()?.to_ascii_lowercase();
    let allowed_path = match host.as_str() {
        "chatgpt.com" | "chat.openai.com" => url.path().starts_with("/_next/static/"),
        "cdn.oaistatic.com" => true,
        _ => false,
    };

    if url.scheme() != "https"
        || !allowed_path
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

fn private_object_relative_path(hash: &str) -> PathBuf {
    PathBuf::from("private")
        .join("objects")
        .join(&hash[..2])
        .join(hash)
}

fn public_object_relative_path(hash: &str) -> PathBuf {
    PathBuf::from("public")
        .join("objects")
        .join(&hash[..2])
        .join(hash)
}

fn build_inventory(raw_root: impl AsRef<Path>) -> Result<Vec<CacheCandidate>> {
    let connection = open_raw_ledger_read_only(raw_root.as_ref())?;

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

    let eligible = reasons.is_empty();
    let mut policy_reasons = Vec::new();
    if eligible {
        if aggregate.body_bytes > MAX_REPLAY_BODY_BYTES {
            policy_reasons.push("body_exceeds_replay_limit".to_owned());
        }
        if replayable_url(&url).is_none() {
            policy_reasons.push("outside_current_replay_scope".to_owned());
        }
    }
    let replay_supported = eligible && policy_reasons.is_empty();
    let expansion_candidate = eligible
        && aggregate.body_bytes <= MAX_REPLAY_BODY_BYTES
        && policy_reasons.len() == 1
        && policy_reasons[0] == "outside_current_replay_scope";

    CacheCandidate {
        url,
        resource_type: aggregate.resource_type,
        mime_type: aggregate.mime_type,
        body_hash: aggregate.body_hash,
        body_bytes: aggregate.body_bytes,
        cache_control: aggregate.cache_control,
        capture_count: aggregate.capture_count,
        distinct_body_hashes,
        eligible,
        replay_supported,
        expansion_candidate,
        reasons,
        policy_reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

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
                CREATE TABLE private_revalidation_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    observed_at_ms INTEGER NOT NULL,
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
    fn private_revalidation_stats_aggregate_saved_bytes() {
        let (directory, connection) = open_fixture();
        connection
            .execute(
                "INSERT INTO private_revalidation_events (observed_at_ms, outcome, body_bytes) VALUES (1, 'not_modified', 80)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO private_revalidation_events (observed_at_ms, outcome, body_bytes) VALUES (2, 'refreshed', 0)",
                [],
            )
            .unwrap();

        let stats = private_revalidation_stats(directory.path()).unwrap();
        assert_eq!(stats.attempts, 2);
        assert_eq!(stats.not_modified, 1);
        assert_eq!(stats.refreshed, 1);
        assert_eq!(stats.fulfill_errors, 0);
        assert_eq!(stats.saved_body_bytes, 80);
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
        assert!(items[0].replay_supported);
        assert!(!items[0].expansion_candidate);
        assert_eq!(items[0].capture_count, 2);
        assert_eq!(items[0].distinct_body_hashes, 1);
        assert!(items[0].reasons.is_empty());
        assert!(items[0].policy_reasons.is_empty());
    }

    #[test]
    fn immutable_static_asset_outside_current_scope_is_an_expansion_candidate() {
        let (directory, connection) = open_fixture();
        insert_capture(
            &connection,
            "outside-scope",
            1,
            "https://static.openai.com/assets/app.js",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("public, max-age=31536000, immutable"),
        );

        let item = candidates(directory.path(), 20).unwrap().pop().unwrap();
        assert!(item.eligible);
        assert!(!item.replay_supported);
        assert!(item.expansion_candidate);
        assert_eq!(
            item.policy_reasons,
            vec!["outside_current_replay_scope".to_owned()]
        );

        let stats = stats(directory.path()).unwrap();
        assert_eq!(stats.eligible_urls, 1);
        assert_eq!(stats.replay_supported_urls, 0);
        assert_eq!(stats.expansion_candidate_urls, 1);
        assert_eq!(stats.expansion_candidate_body_bytes, 12);
    }

    #[test]
    fn opportunity_summary_combines_policy_coverage_and_runtime_savings() {
        let (directory, connection) = open_fixture();
        insert_capture(
            &connection,
            "public-supported",
            1,
            "https://chatgpt.com/_next/static/app.js",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("public, max-age=31536000, immutable"),
        );
        insert_capture(
            &connection,
            "public-expansion",
            2,
            "https://static.openai.com/assets/app.js",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            Some("public, max-age=31536000, immutable"),
        );
        connection
            .execute(
                r#"
                INSERT INTO captures (
                    capture_id, captured_at_ms, method, url, status, mime_type,
                    resource_type, privacy_class, body_hash, body_bytes,
                    cache_control, etag, last_modified, body_error
                ) VALUES (
                    'private-current', 3, 'GET',
                    'https://chatgpt.com/backend-api/conversation/a', 200,
                    'application/json', 'Fetch', 'private',
                    'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
                    100, 'private, max-age=0, must-revalidate',
                    '"private-v1"', NULL, NULL
                )
                "#,
                [],
            )
            .unwrap();
        connection
            .execute(
                r#"
                INSERT INTO captures (
                    capture_id, captured_at_ms, method, url, status, mime_type,
                    resource_type, privacy_class, body_hash, body_bytes,
                    cache_control, etag, last_modified, body_error
                ) VALUES (
                    'private-expansion', 4, 'GET',
                    'https://chatgpt.com/backend-api/plain', 200,
                    'text/plain', 'Fetch', 'private',
                    'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
                    300, 'private, max-age=0, must-revalidate',
                    '"plain-v1"', NULL, NULL
                )
                "#,
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO cache_replay_events (observed_at_ms, url, resource_type, outcome, body_bytes) VALUES (5, 'https://chatgpt.com/_next/static/app.js', 'Script', 'hit', 50)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO private_revalidation_events (observed_at_ms, outcome, body_bytes) VALUES (6, 'not_modified', 60)",
                [],
            )
            .unwrap();

        let summary = opportunities(directory.path()).unwrap();
        assert_eq!(summary.public_eligible_body_bytes, 24);
        assert_eq!(summary.public_replay_supported_body_bytes, 12);
        assert_eq!(summary.public_expansion_candidate_body_bytes, 12);
        assert_eq!(summary.private_observed_captures, 2);
        assert_eq!(summary.private_observed_body_bytes, 400);
        assert_eq!(summary.private_validator_body_bytes, 400);
        assert_eq!(summary.private_current_policy_body_bytes, 100);
        assert_eq!(summary.private_expansion_candidate_body_bytes, 300);
        assert_eq!(
            summary.top_public_expansion,
            Some(PublicExpansionLead {
                host: "static.openai.com".to_owned(),
                candidate_urls: 1,
                body_bytes: 12,
                blocking_gate: "public_scope".to_owned(),
            })
        );
        assert_eq!(
            summary.top_private_expansion,
            Some(PrivateExpansionLead {
                resource_type: "Fetch".to_owned(),
                mime_type: "text/plain".to_owned(),
                candidate_captures: 1,
                unique_urls: 1,
                body_bytes: 300,
                blocking_gate: "private_mime_family".to_owned(),
            })
        );
        assert_eq!(summary.runtime_public_replayed_bytes, 50);
        assert_eq!(summary.runtime_private_revalidated_saved_body_bytes, 60);
        assert_eq!(summary.runtime_total_saved_body_bytes, 110);
    }

    #[test]
    fn public_coverage_surfaces_supported_and_scope_expansion_bytes() {
        let (directory, connection) = open_fixture();
        insert_capture(
            &connection,
            "supported",
            1,
            "https://chatgpt.com/_next/static/app.js",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("public, max-age=31536000, immutable"),
        );
        insert_capture(
            &connection,
            "expansion",
            2,
            "https://static.openai.com/assets/app.js",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            Some("public, max-age=31536000, immutable"),
        );

        let profiles = public_coverage(directory.path()).unwrap();
        let chatgpt = profiles
            .iter()
            .find(|item| item.host == "chatgpt.com")
            .unwrap();
        assert_eq!(chatgpt.replay_supported_urls, 1);
        assert_eq!(chatgpt.replay_supported_body_bytes, 12);
        assert_eq!(chatgpt.expansion_candidate_urls, 0);

        let static_openai = profiles
            .iter()
            .find(|item| item.host == "static.openai.com")
            .unwrap();
        assert_eq!(static_openai.eligible_urls, 1);
        assert_eq!(static_openai.replay_supported_urls, 0);
        assert_eq!(static_openai.expansion_candidate_urls, 1);
        assert_eq!(static_openai.expansion_candidate_body_bytes, 12);
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

    fn insert_private_json_capture(
        connection: &Connection,
        id: &str,
        timestamp: i64,
        url: &str,
        hash: &str,
        etag: Option<&str>,
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
                ) VALUES (
                    ?1, ?2, 'GET', ?3, 200, 'application/json', 'Fetch',
                    'private', ?4, 24, ?5, ?6, NULL, NULL
                )
                "#,
                params![id, timestamp, url, hash, cache_control, etag],
            )
            .unwrap();
    }

    #[test]
    fn private_coverage_separates_current_policy_and_expansion_candidates() {
        let (directory, connection) = open_fixture();
        for (
            id,
            url,
            resource_type,
            mime_type,
            bytes,
            etag,
            cache_control,
        ) in [
            (
                "json-current",
                "https://chatgpt.com/backend-api/conversation/a",
                "Fetch",
                "application/json",
                100_i64,
                Some("\"json-v1\""),
                Some("private, max-age=0, must-revalidate"),
            ),
            (
                "html-current",
                "https://chatgpt.com/",
                "Document",
                "text/html",
                200_i64,
                Some("\"html-v1\""),
                Some("private, max-age=0, must-revalidate"),
            ),
            (
                "text-expansion",
                "https://chatgpt.com/backend-api/plain",
                "Fetch",
                "text/plain",
                300_i64,
                Some("\"plain-v1\""),
                Some("private, max-age=0, must-revalidate"),
            ),
            (
                "stream-no-store",
                "https://chatgpt.com/backend-api/stream",
                "Fetch",
                "text/event-stream",
                400_i64,
                Some("\"stream-v1\""),
                Some("no-store"),
            ),
        ] {
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
                        ?1, 1, 'GET', ?2, 200, ?3, ?4, 'private',
                        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                        ?5, ?6, ?7, NULL, NULL
                    )
                    "#,
                    params![id, url, mime_type, resource_type, bytes, cache_control, etag],
                )
                .unwrap();
        }

        let profiles = private_coverage(directory.path()).unwrap();
        let json = profiles
            .iter()
            .find(|item| item.mime_type == "application/json")
            .unwrap();
        assert_eq!(json.current_policy_captures, 1);
        assert_eq!(json.expansion_candidate_captures, 0);

        let html = profiles
            .iter()
            .find(|item| item.mime_type == "text/html")
            .unwrap();
        assert_eq!(html.current_policy_captures, 1);
        assert_eq!(html.current_policy_body_bytes, 200);

        let plain = profiles
            .iter()
            .find(|item| item.mime_type == "text/plain")
            .unwrap();
        assert_eq!(plain.current_policy_captures, 0);
        assert_eq!(plain.expansion_candidate_captures, 1);
        assert_eq!(plain.expansion_candidate_body_bytes, 300);

        let stream = profiles
            .iter()
            .find(|item| item.mime_type == "text/event-stream")
            .unwrap();
        assert_eq!(stream.validator_captures, 1);
        assert_eq!(stream.no_store_captures, 1);
        assert_eq!(stream.expansion_candidate_captures, 0);
    }

    #[test]
    fn private_read_inventory_identifies_revalidation_candidates() {
        let (directory, connection) = open_fixture();
        insert_private_json_capture(
            &connection,
            "private-one",
            1,
            "https://chatgpt.com/backend-api/conversation/a",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("\"v1\""),
            Some("private, max-age=0, must-revalidate"),
        );
        insert_private_json_capture(
            &connection,
            "private-two",
            2,
            "https://chatgpt.com/backend-api/conversation/a",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            Some("\"v2\""),
            Some("private, max-age=0, must-revalidate"),
        );

        let profiles = private_reads(directory.path(), 20).unwrap();
        assert_eq!(profiles.len(), 1);
        let profile = &profiles[0];
        assert!(profile.revalidation_candidate);
        assert!(profile.has_validator);
        assert!(!profile.stable_so_far);
        assert_eq!(profile.capture_count, 2);
        assert_eq!(profile.distinct_body_hashes, 2);
        assert_eq!(profile.latest_etag.as_deref(), Some("\"v2\""));
        assert!(profile.reasons.is_empty());
    }

    #[test]
    fn private_read_inventory_rejects_no_store_and_redacted_identity() {
        let (directory, connection) = open_fixture();
        insert_private_json_capture(
            &connection,
            "private-no-store",
            1,
            "https://chatgpt.com/backend-api/items?token=%5BREDACTED%5D",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("\"v1\""),
            Some("no-store"),
        );

        let profile = private_reads(directory.path(), 20).unwrap().pop().unwrap();
        assert!(!profile.revalidation_candidate);
        assert!(profile
            .reasons
            .iter()
            .any(|reason| reason == "cache_control_no_store"));
        assert!(profile
            .reasons
            .iter()
            .any(|reason| reason == "redacted_query_identity"));
    }

    #[test]
    fn replayable_url_accepts_exact_chatgpt_https_without_query() {
        assert!(replayable_url("https://chatgpt.com/_next/static/replay.js").is_some());
        assert!(replayable_url("https://chat.openai.com/_next/static/replay.css").is_some());
        assert!(replayable_url("https://cdn.oaistatic.com/assets/replay.js").is_some());
        assert!(replayable_url("https://chatgpt.com/backend-api/conversation/x").is_none());
        assert!(replayable_url("https://oaistatic.com/assets/replay.js").is_none());
        assert!(replayable_url("https://other.oaistatic.com/assets/replay.js").is_none());
        assert!(replayable_url("http://chatgpt.com/_next/static/replay.js").is_none());
        assert!(replayable_url("https://chatgpt.com/_next/static/replay.js?v=1").is_none());
        assert!(replayable_url("https://cdn.oaistatic.com/assets/replay.js?v=1").is_none());
    }

    #[test]
    fn private_lookup_reads_verified_validator_backed_json() {
        let (directory, connection) = open_fixture();
        let body = br#"{"id":"conversation-a"}"#;
        let hash = format!("{:x}", Sha256::digest(body));
        let relative_path = private_object_relative_path(&hash);
        let object_path = directory.path().join(&relative_path);
        fs::create_dir_all(object_path.parent().unwrap()).unwrap();
        fs::write(&object_path, body).unwrap();
        connection
            .execute(
                "INSERT INTO objects (storage_class, hash, bytes, relative_path, created_at_ms) VALUES ('private', ?1, ?2, ?3, 1)",
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
                    'private-lookup',
                    10,
                    'GET',
                    'https://chatgpt.com/backend-api/conversation/a',
                    200,
                    'application/json',
                    'Fetch',
                    'private',
                    ?1,
                    ?2,
                    'private, max-age=0, must-revalidate',
                    '"fixture-v1"',
                    NULL,
                    NULL
                )
                "#,
                params![hash, body.len() as i64],
            )
            .unwrap();

        let entry = private_lookup(
            directory.path(),
            "https://chatgpt.com/backend-api/conversation/a",
        )
        .unwrap()
        .unwrap();
        assert_eq!(entry.body, body);
        assert_eq!(entry.body_hash, hash);
        assert_eq!(entry.etag.as_deref(), Some("\"fixture-v1\""));
        assert_eq!(entry.captured_at_ms, 10);
    }

    #[test]
    fn private_lookup_accepts_verified_html_document() {
        let (directory, connection) = open_fixture();
        let body = b"<!doctype html><title>cached document</title>";
        let hash = format!("{:x}", Sha256::digest(body));
        let relative_path = private_object_relative_path(&hash);
        let object_path = directory.path().join(&relative_path);
        fs::create_dir_all(object_path.parent().unwrap()).unwrap();
        fs::write(&object_path, body).unwrap();
        connection
            .execute(
                "INSERT INTO objects (storage_class, hash, bytes, relative_path, created_at_ms) VALUES ('private', ?1, ?2, ?3, 1)",
                params![
                    hash,
                    body.len() as i64,
                    relative_path.to_string_lossy().to_string()
                ],
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
                    'private-html',
                    10,
                    'GET',
                    'https://chatgpt.com/',
                    200,
                    'text/html; charset=utf-8',
                    'Document',
                    'private',
                    ?1,
                    ?2,
                    'private, max-age=0, must-revalidate',
                    '"document-v1"',
                    NULL,
                    NULL
                )
                "#,
                params![hash, body.len() as i64],
            )
            .unwrap();

        let entry = private_lookup(directory.path(), "https://chatgpt.com/")
            .unwrap()
            .unwrap();
        assert_eq!(entry.body, body);
        assert_eq!(entry.mime_type, "text/html; charset=utf-8");
        assert_eq!(entry.etag.as_deref(), Some("\"document-v1\""));
    }

    #[test]
    fn private_lookup_rejects_no_store_missing_validator_and_query_identity() {
        let (directory, connection) = open_fixture();
        insert_private_json_capture(
            &connection,
            "private-no-store",
            1,
            "https://chatgpt.com/backend-api/conversation/no-store",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("\"v1\""),
            Some("no-store"),
        );
        insert_private_json_capture(
            &connection,
            "private-no-validator",
            2,
            "https://chatgpt.com/backend-api/conversation/no-validator",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            None,
            Some("private, max-age=0"),
        );

        assert!(private_lookup(
            directory.path(),
            "https://chatgpt.com/backend-api/conversation/no-store"
        )
        .unwrap()
        .is_none());
        assert!(private_lookup(
            directory.path(),
            "https://chatgpt.com/backend-api/conversation/no-validator"
        )
        .unwrap()
        .is_none());
        assert!(private_revalidation_url(
            "https://chatgpt.com/backend-api/conversation/a?cursor=1&limit=20"
        )
        .is_some());
        assert!(private_revalidation_url(
            "https://chatgpt.com/backend-api/conversation/a?token=secret"
        )
        .is_none());
        assert!(private_revalidation_url(
            "https://chatgpt.com/backend-api/conversation/a?cursor=%5BREDACTED%5D"
        )
        .is_none());
        assert!(private_lookup(
            directory.path(),
            "https://chatgpt.com/api/auth/session"
        )
        .unwrap()
        .is_none());
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
