use std::{
    env, fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use mirrarium_cache as cache;
use mirrarium_protocol::{CaptureMessageStage, HostRequest, HostResponse};
use mirrarium_store::{default_data_root, CaptureStore};
use sha2::{Digest, Sha256};

const MAX_NATIVE_REQUEST_BYTES: usize = 64 * 1024 * 1024;
const MAX_NATIVE_RESPONSE_BYTES: usize = 1024 * 1024;
const REPLAY_RAW_CHUNK_BYTES: usize = 384 * 1024;
const EXTENSION_ID: &str = "oodcefibmdmabgepkcpanjpjolnbignk";

fn open_cache_reader_if_present(root: &Path) -> Result<Option<cache::CacheReader>> {
    if !root.join("ledger.sqlite3").is_file() {
        return Ok(None);
    }
    cache::CacheReader::open(root).map(Some)
}

fn capture_commit_status(
    root: &Path,
    writable: Option<&CaptureStore>,
    capture_id: &str,
) -> Result<bool> {
    anyhow::ensure!(
        !capture_id.trim().is_empty() && capture_id.len() <= 256,
        "capture commit probe requires a bounded nonempty capture id"
    );
    if let Some(store) = writable {
        return Ok(store.capture_by_id(capture_id)?.is_some());
    }
    let ledger = root.join("ledger.sqlite3");
    match fs::symlink_metadata(&ledger) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error).with_context(|| format!("inspecting {}", ledger.display())),
        Ok(_) => {}
    }
    let read_only = CaptureStore::open_read_only(root)?;
    Ok(read_only.capture_by_id(capture_id)?.is_some())
}

fn main() -> Result<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();

    if arguments.first().map(String::as_str) == Some("--stats") {
        let store = CaptureStore::open_read_only(default_data_root()?)?;
        println!("{}", serde_json::to_string_pretty(&store.stats()?)?);
        return Ok(());
    }

    run_native_host()
}

fn run_native_host() -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
    let root = default_data_root()?;
    let mut store: Option<CaptureStore> = None;
    let mut cache_reader: Option<cache::CacheReader> = None;

    while let Some(payload) = read_native_message(&mut input)? {
        match serde_json::from_slice::<HostRequest>(&payload) {
            Ok(HostRequest::Ping) => {
                write_native_response(&mut output, &HostResponse::Pong)?;
            }
            Ok(HostRequest::ExtensionInstallState) => {
                let response = match installed_extension_build_id() {
                    Ok(build_id) => HostResponse::ExtensionInstallState { build_id },
                    Err(error) => HostResponse::Error {
                        capture_id: None,
                        message: format!("extension install state unavailable: {error:#}"),
                    },
                };
                write_native_response(&mut output, &response)?;
            }
            Ok(HostRequest::ExtensionRuntimeState { build_id }) => {
                let response = match publish_extension_runtime_state(&build_id) {
                    Ok(()) => HostResponse::Ack { capture_id: None },
                    Err(error) => HostResponse::Error {
                        capture_id: None,
                        message: format!("extension runtime state unavailable: {error:#}"),
                    },
                };
                write_native_response(&mut output, &response)?;
            }
            Ok(HostRequest::CaptureAbort { capture_id }) => {
                // An abort on a fresh host is a harmless no-op. Never create
                // a writable ledger only to discard an unknown capture.
                let result = match store.as_mut() {
                    Some(writer) => writer.abort(&capture_id),
                    None => Ok(false),
                };
                let response = match result {
                    Ok(discarded) => HostResponse::CaptureAborted {
                        capture_id,
                        discarded,
                    },
                    Err(error) => HostResponse::Error {
                        capture_id: Some(capture_id),
                        message: format!("discarding incomplete capture failed: {error:#}"),
                    },
                };
                write_native_response(&mut output, &response)?;
            }
            Ok(HostRequest::CaptureCommitProbe { capture_id }) => {
                let response = match capture_commit_status(&root, store.as_ref(), &capture_id) {
                    Ok(committed) => HostResponse::CaptureCommitStatus {
                        capture_id,
                        committed,
                    },
                    Err(error) => HostResponse::Error {
                        capture_id: Some(capture_id),
                        message: format!("capture commit status unavailable: {error:#}"),
                    },
                };
                write_native_response(&mut output, &response)?;
            }
            Ok(HostRequest::CacheLookup {
                lookup_id,
                url,
                resource_type,
            }) => {
                if cache_reader.is_none() {
                    match open_cache_reader_if_present(&root) {
                        Ok(Some(reader)) => cache_reader = Some(reader),
                        Ok(None) => {
                            write_native_response(
                                &mut output,
                                &HostResponse::CacheMiss { lookup_id },
                            )?;
                            continue;
                        }
                        Err(error) => {
                            write_native_response(
                                &mut output,
                                &HostResponse::CacheLookupError {
                                    lookup_id,
                                    message: format!("{error:#}"),
                                },
                            )?;
                            continue;
                        }
                    }
                }
                write_cache_lookup_responses(
                    &mut output,
                    cache_reader
                        .as_ref()
                        .expect("cache reader initialized above"),
                    lookup_id,
                    &url,
                    &resource_type,
                )?;
            }
            Ok(HostRequest::PrivateReadLookup { lookup_id, url }) => {
                if cache_reader.is_none() {
                    match open_cache_reader_if_present(&root) {
                        Ok(Some(reader)) => cache_reader = Some(reader),
                        Ok(None) => {
                            write_native_response(
                                &mut output,
                                &HostResponse::PrivateReadMiss { lookup_id },
                            )?;
                            continue;
                        }
                        Err(error) => {
                            write_native_response(
                                &mut output,
                                &HostResponse::PrivateReadLookupError {
                                    lookup_id,
                                    message: format!("{error:#}"),
                                },
                            )?;
                            continue;
                        }
                    }
                }
                write_private_read_lookup_responses(
                    &mut output,
                    cache_reader
                        .as_ref()
                        .expect("cache reader initialized above"),
                    lookup_id,
                    &url,
                )?;
            }
            Ok(request) => {
                if store.is_none() {
                    match CaptureStore::open(&root) {
                        Ok(opened) => store = Some(opened),
                        Err(error) => {
                            write_native_response(
                                &mut output,
                                &HostResponse::Error {
                                    capture_id: request_capture_id(&request),
                                    message: format!("opening writable capture store failed: {error:#}"),
                                },
                            )?;
                            continue;
                        }
                    }
                }
                let response = handle_request(
                    store.as_mut().expect("capture store initialized above"),
                    request,
                );
                write_native_response(&mut output, &response)?;
            }
            Err(error) => {
                write_native_response(
                    &mut output,
                    &HostResponse::Error {
                        capture_id: None,
                        message: format!("invalid request: {error}"),
                    },
                )?;
            }
        }
    }

    Ok(())
}

fn write_cache_lookup_responses(
    output: &mut impl Write,
    cache_reader: &cache::CacheReader,
    lookup_id: String,
    url: &str,
    resource_type: &str,
) -> Result<()> {
    let entry = match cache_reader.lookup(url, resource_type) {
        Ok(entry) => entry,
        Err(error) => {
            return write_native_response(
                output,
                &HostResponse::CacheLookupError {
                    lookup_id,
                    message: format!("{error:#}"),
                },
            );
        }
    };

    let Some(entry) = entry else {
        return write_native_response(output, &HostResponse::CacheMiss { lookup_id });
    };

    write_native_response(
        output,
        &HostResponse::CacheHitStart {
            lookup_id: lookup_id.clone(),
            mime_type: entry.mime_type,
            body_hash: entry.body_hash,
            body_bytes: entry.body_bytes,
            cache_control: entry.cache_control,
            etag: entry.etag,
            last_modified: entry.last_modified,
        },
    )?;

    for (sequence, chunk) in entry.body.chunks(REPLAY_RAW_CHUNK_BYTES).enumerate() {
        write_native_response(
            output,
            &HostResponse::CacheHitChunk {
                lookup_id: lookup_id.clone(),
                sequence: sequence
                    .try_into()
                    .context("replay chunk sequence exceeds u32")?,
                data_base64: BASE64.encode(chunk),
            },
        )?;
    }

    write_native_response(output, &HostResponse::CacheHitFinish { lookup_id })
}

fn write_private_read_lookup_responses(
    output: &mut impl Write,
    cache_reader: &cache::CacheReader,
    lookup_id: String,
    url: &str,
) -> Result<()> {
    let entry = match cache_reader.private_lookup(url) {
        Ok(entry) => entry,
        Err(error) => {
            return write_native_response(
                output,
                &HostResponse::PrivateReadLookupError {
                    lookup_id,
                    message: format!("{error:#}"),
                },
            );
        }
    };

    let Some(entry) = entry else {
        return write_native_response(
            output,
            &HostResponse::PrivateReadMiss { lookup_id },
        );
    };

    write_native_response(
        output,
        &HostResponse::PrivateReadHitStart {
            lookup_id: lookup_id.clone(),
            mime_type: entry.mime_type,
            body_hash: entry.body_hash,
            body_bytes: entry.body_bytes,
            captured_at_ms: entry.captured_at_ms,
            cache_control: entry.cache_control,
            etag: entry.etag,
            last_modified: entry.last_modified,
        },
    )?;

    for (sequence, chunk) in entry.body.chunks(REPLAY_RAW_CHUNK_BYTES).enumerate() {
        write_native_response(
            output,
            &HostResponse::PrivateReadHitChunk {
                lookup_id: lookup_id.clone(),
                sequence: sequence
                    .try_into()
                    .context("private-read chunk sequence exceeds u32")?,
                data_base64: BASE64.encode(chunk),
            },
        )?;
    }

    write_native_response(
        output,
        &HostResponse::PrivateReadHitFinish { lookup_id },
    )
}

fn write_native_response(writer: &mut impl Write, response: &HostResponse) -> Result<()> {
    write_native_message(writer, &serde_json::to_vec(response)?)
}

fn request_capture_id(request: &HostRequest) -> Option<String> {
    match request {
        HostRequest::CaptureStart { metadata } => Some(metadata.capture_id.clone()),
        HostRequest::CaptureChunk { capture_id, .. }
        | HostRequest::RequestBodyStart { capture_id, .. }
        | HostRequest::RequestBodyChunk { capture_id, .. }
        | HostRequest::RequestBodyFinish { capture_id, .. }
        | HostRequest::CaptureFinish { capture_id, .. }
        | HostRequest::CaptureAbort { capture_id } => Some(capture_id.clone()),
        HostRequest::Ping
        | HostRequest::ExtensionInstallState
        | HostRequest::ExtensionRuntimeState { .. }
        | HostRequest::CacheLookup { .. }
        | HostRequest::PrivateReadLookup { .. }
        | HostRequest::CaptureCommitProbe { .. }
        | HostRequest::CacheReplayOutcome { .. }
        | HostRequest::PrivateRevalidationOutcome { .. } => None,
    }
}

fn handle_request(store: &mut CaptureStore, request: HostRequest) -> HostResponse {
    let terminal_capture = matches!(&request, HostRequest::CaptureFinish { .. });
    let progress = match &request {
        HostRequest::CaptureStart { .. } => Some((CaptureMessageStage::CaptureStart, None)),
        HostRequest::CaptureChunk { sequence, .. } => {
            Some((CaptureMessageStage::CaptureChunk, Some(*sequence)))
        }
        HostRequest::RequestBodyStart { .. } => {
            Some((CaptureMessageStage::RequestBodyStart, None))
        }
        HostRequest::RequestBodyChunk { sequence, .. } => {
            Some((CaptureMessageStage::RequestBodyChunk, Some(*sequence)))
        }
        HostRequest::RequestBodyFinish { .. } => {
            Some((CaptureMessageStage::RequestBodyFinish, None))
        }
        _ => None,
    };
    let capture_id = request_capture_id(&request);

    let result = match request {
        HostRequest::Ping => return HostResponse::Pong,
        HostRequest::ExtensionInstallState => {
            return match installed_extension_build_id() {
                Ok(build_id) => HostResponse::ExtensionInstallState { build_id },
                Err(error) => HostResponse::Error {
                    capture_id: None,
                    message: format!("extension install state unavailable: {error:#}"),
                },
            };
        }
        HostRequest::ExtensionRuntimeState { build_id } => {
            return match publish_extension_runtime_state(&build_id) {
                Ok(()) => HostResponse::Ack { capture_id: None },
                Err(error) => HostResponse::Error {
                    capture_id: None,
                    message: format!("extension runtime state unavailable: {error:#}"),
                },
            };
        }
        HostRequest::CacheLookup { lookup_id, .. } => {
            return HostResponse::CacheLookupError {
                lookup_id,
                message: "cache lookup must be handled by the streaming response path".to_owned(),
            };
        }
        HostRequest::PrivateReadLookup { lookup_id, .. } => {
            return HostResponse::PrivateReadLookupError {
                lookup_id,
                message: "private-read lookup must be handled by the streaming response path"
                    .to_owned(),
            };
        }
        HostRequest::CaptureCommitProbe { capture_id } => {
            return HostResponse::Error {
                capture_id: Some(capture_id),
                message: "capture commit probe must use the read-only path".to_owned(),
            };
        }
        HostRequest::CaptureAbort { capture_id } => {
            return HostResponse::Error {
                capture_id: Some(capture_id),
                message: "capture abort must use the direct native path".to_owned(),
            };
        }
        HostRequest::CacheReplayOutcome {
            url,
            resource_type,
            outcome,
            body_bytes,
        } => store.record_cache_replay_outcome(
            &url,
            &resource_type,
            &outcome,
            body_bytes,
        ),
        HostRequest::PrivateRevalidationOutcome {
            outcome,
            body_bytes,
        } => store.record_private_revalidation_outcome(&outcome, body_bytes),
        HostRequest::CaptureStart { metadata } => store.begin(metadata),
        HostRequest::CaptureChunk {
            capture_id,
            sequence,
            data_base64,
        } => store.append_chunk(&capture_id, sequence, &data_base64),
        HostRequest::RequestBodyStart {
            capture_id,
            metadata,
        } => store.begin_request_body(&capture_id, metadata),
        HostRequest::RequestBodyChunk {
            capture_id,
            sequence,
            data_base64,
        } => store.append_request_body_chunk(&capture_id, sequence, &data_base64),
        HostRequest::RequestBodyFinish {
            capture_id,
            body_error,
        } => store.finish_request_body(&capture_id, body_error.as_deref()),
        HostRequest::CaptureFinish {
            capture_id,
            encoded_data_length,
            body_error,
        } => store.finish(
            &capture_id,
            encoded_data_length,
            body_error.as_deref(),
        ),
    };

    match result {
        Ok(()) if terminal_capture => HostResponse::CaptureCommitted {
            capture_id: capture_id.expect("capture finish always has a capture ID"),
        },
        Ok(()) if progress.is_some() => {
            let (stage, sequence) = progress.expect("checked above");
            HostResponse::CaptureMessageAck {
                capture_id: capture_id.expect("capture progress always has a capture ID"),
                stage,
                sequence,
            }
        }
        Ok(()) => HostResponse::Ack { capture_id },
        Err(error) => {
            // Once any capture protocol operation fails, its staged stream
            // can no longer be trusted. Discard it locally before replying,
            // rather than waiting for the asynchronous extension to abort.
            // A completed capture is not removed by this operation.
            let cleanup_error = capture_id
                .as_deref()
                .and_then(|id| store.abort(id).err());
            let message = match cleanup_error {
                Some(cleanup) => format!("{error:#}; abort cleanup failed: {cleanup:#}"),
                None => format!("{error:#}"),
            };
            HostResponse::Error { capture_id, message }
        }
    }
}

fn extension_state_path() -> Result<PathBuf> {
    if let Some(path) = env::var_os("MIRRARIUM_EXTENSION_STATE_FILE") {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(path).join("mirrarium/extension-install.json"));
    }
    let home = env::var_os("HOME")
        .context("set HOME, XDG_CONFIG_HOME, or MIRRARIUM_EXTENSION_STATE_FILE")?;
    Ok(PathBuf::from(home).join(".config/mirrarium/extension-install.json"))
}

fn extension_runtime_state_path() -> Result<PathBuf> {
    let install_state_path = extension_state_path()?;
    let parent = install_state_path
        .parent()
        .context("extension install-state path has no parent directory")?;
    Ok(parent.join("extension-runtime.json"))
}

fn publish_extension_runtime_state(build_id: &str) -> Result<()> {
    anyhow::ensure!(
        !build_id.is_empty() && build_id.len() <= 256,
        "extension runtime build_id is invalid"
    );

    let state_path = extension_runtime_state_path()?;
    let parent = state_path
        .parent()
        .context("extension runtime-state path has no parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("creating extension runtime-state directory {}", parent.display()))?;

    let state = serde_json::json!({
        "schema_version": 1,
        "extension_id": EXTENSION_ID,
        "build_id": build_id,
    });
    let temp = parent.join(format!(
        ".extension-runtime.{}.tmp",
        std::process::id()
    ));
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)
        .with_context(|| format!("opening extension runtime state {}", temp.display()))?;
    file.write_all(&serde_json::to_vec_pretty(&state)?)
        .with_context(|| format!("writing extension runtime state {}", temp.display()))?;
    file.sync_all()
        .with_context(|| format!("syncing extension runtime state {}", temp.display()))?;
    drop(file);
    fs::rename(&temp, &state_path).with_context(|| {
        format!(
            "installing extension runtime state {} to {}",
            temp.display(),
            state_path.display()
        )
    })?;
    sync_directory(parent)?;
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)
        .with_context(|| format!("opening directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("syncing directory {}", path.display()))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn extension_tree_hash(path: &Path) -> Result<String> {
    let mut files = Vec::<(String, PathBuf)>::new();
    collect_extension_tree_files(path, path, &mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));

    let mut hasher = Sha256::new();
    for (relative, file_path) in files {
        let relative_bytes = relative.as_bytes();
        let bytes = fs::read(&file_path)
            .with_context(|| format!("reading extension tree file {}", file_path.display()))?;
        hasher.update((relative_bytes.len() as u64).to_le_bytes());
        hasher.update(relative_bytes);
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn collect_extension_tree_files(
    root: &Path,
    directory: &Path,
    files: &mut Vec<(String, PathBuf)>,
) -> Result<()> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("reading extension tree {}", directory.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        anyhow::ensure!(
            !metadata.file_type().is_symlink(),
            "extension tree contains unsupported symlink: {}",
            path.display()
        );
        if metadata.is_dir() {
            collect_extension_tree_files(root, &path, files)?;
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .context("extension tree file escaped root")?
                .to_string_lossy()
                .replace('\\', "/");
            files.push((relative, path));
        } else {
            anyhow::bail!("extension tree contains unsupported entry: {}", path.display());
        }
    }
    Ok(())
}

fn installed_extension_build_id() -> Result<Option<String>> {
    read_extension_build_id_from_state(&extension_state_path()?)
}

fn read_extension_build_id_from_state(path: &Path) -> Result<Option<String>> {
    if !path.is_file() {
        return Ok(None);
    }

    let state: serde_json::Value = serde_json::from_slice(
        &fs::read(path).with_context(|| format!("reading {}", path.display()))?,
    )
    .with_context(|| format!("parsing {}", path.display()))?;

    anyhow::ensure!(
        state.get("schema_version").and_then(serde_json::Value::as_u64) == Some(1),
        "unsupported extension install-state schema"
    );
    anyhow::ensure!(
        state.get("extension_id").and_then(serde_json::Value::as_str) == Some(EXTENSION_ID),
        "extension install-state id mismatch"
    );
    let build_id = state
        .get("build_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .context("extension install-state build_id is missing")?;
    let install_path = state
        .get("install_path")
        .and_then(serde_json::Value::as_str)
        .map(PathBuf::from)
        .context("extension install-state install_path is missing")?;
    let manifest_path = install_path.join("manifest.json");
    if !manifest_path.is_file() {
        return Ok(None);
    }

    let manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(&manifest_path)
            .with_context(|| format!("reading installed manifest {}", manifest_path.display()))?,
    )
    .with_context(|| format!("parsing installed manifest {}", manifest_path.display()))?;
    let manifest_build_id = manifest
        .get("version_name")
        .and_then(serde_json::Value::as_str);
    if manifest_build_id != Some(build_id) {
        return Ok(None);
    }

    if let Some(expected_tree_hash) = state.get("tree_hash").and_then(serde_json::Value::as_str) {
        let actual_tree_hash = extension_tree_hash(&install_path)?;
        if actual_tree_hash != expected_tree_hash {
            return Ok(None);
        }
    }

    Ok(Some(build_id.to_owned()))
}

fn read_native_message(reader: &mut impl Read) -> Result<Option<Vec<u8>>> {
    let mut header = [0_u8; 4];
    let mut read = 0;

    while read < header.len() {
        match reader.read(&mut header[read..])? {
            0 if read == 0 => return Ok(None),
            0 => anyhow::bail!("unexpected EOF while reading native-message header"),
            n => read += n,
        }
    }

    let len = u32::from_ne_bytes(header) as usize;
    anyhow::ensure!(
        len <= MAX_NATIVE_REQUEST_BYTES,
        "native request message too large: {len} bytes"
    );

    let mut payload = vec![0_u8; len];
    reader
        .read_exact(&mut payload)
        .context("reading native-message payload")?;

    Ok(Some(payload))
}

fn write_native_message(writer: &mut impl Write, payload: &[u8]) -> Result<()> {
    anyhow::ensure!(
        payload.len() <= MAX_NATIVE_RESPONSE_BYTES,
        "native response exceeds Chrome's 1 MiB limit: {} bytes",
        payload.len()
    );
    let len = u32::try_from(payload.len()).context("native response exceeds u32 framing")?;
    writer.write_all(&len.to_ne_bytes())?;
    writer.write_all(payload)?;
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrarium_protocol::{CaptureMetadata, CaptureProvenance};
    use tempfile::tempdir;

    #[test]
    fn read_only_commit_probe_distinguishes_absent_and_committed_capture() {
        let directory = tempdir().unwrap();
        assert!(!capture_commit_status(directory.path(), None, "not-present").unwrap());
        assert!(!directory.path().join("ledger.sqlite3").exists());
        let mut store = CaptureStore::open(directory.path()).unwrap();
        assert!(!capture_commit_status(directory.path(), Some(&store), "pending").unwrap());
        let metadata = CaptureMetadata {
            capture_id: "pending".to_owned(),
            tab_id: 1,
            request_id: "pending-request".to_owned(),
            method: "GET".to_owned(),
            url: "https://chatgpt.com/backend-api/pending".to_owned(),
            status: 204,
            mime_type: "application/json".to_owned(),
            resource_type: "Fetch".to_owned(),
            etag: None,
            last_modified: None,
            cache_control: None,
            provenance: CaptureProvenance::default(),
        };
        store.begin(metadata).unwrap();
        assert!(!capture_commit_status(directory.path(), Some(&store), "pending").unwrap());
        store.finish("pending", None, Some("suppressed:no_response_body_expected")).unwrap();
        assert!(capture_commit_status(directory.path(), Some(&store), "pending").unwrap());
        assert!(capture_commit_status(directory.path(), None, "pending").unwrap());
        assert!(!capture_commit_status(directory.path(), None, "other").unwrap());
        assert!(capture_commit_status(directory.path(), None, "").is_err());
    }

    #[test]
    fn terminal_receipt_is_emitted_only_after_capture_commit() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        let capture_id = "receipt-proof".to_owned();
        let metadata = CaptureMetadata {
            capture_id: capture_id.clone(),
            tab_id: 1,
            request_id: "request-receipt-proof".to_owned(),
            method: "GET".to_owned(),
            url: "https://chatgpt.com/backend-api/conversation/receipt".to_owned(),
            status: 200,
            mime_type: "application/json".to_owned(),
            resource_type: "Fetch".to_owned(),
            etag: None,
            last_modified: None,
            cache_control: None,
            provenance: CaptureProvenance::default(),
        };
        assert!(matches!(
            handle_request(&mut store, HostRequest::CaptureStart { metadata }),
            HostResponse::CaptureMessageAck {
                capture_id: ref id,
                stage: CaptureMessageStage::CaptureStart,
                sequence: None,
            } if id == &capture_id
        ));
        assert!(matches!(
            handle_request(
                &mut store,
                HostRequest::CaptureChunk {
                    capture_id: capture_id.clone(),
                    sequence: 0,
                    data_base64: BASE64.encode(br#"{"id":"receipt"}"#),
                },
            ),
            HostResponse::CaptureMessageAck {
                capture_id: ref id,
                stage: CaptureMessageStage::CaptureChunk,
                sequence: Some(0),
            } if id == &capture_id
        ));
        let committed = handle_request(
            &mut store,
            HostRequest::CaptureFinish {
                capture_id: capture_id.clone(),
                encoded_data_length: None,
                body_error: None,
            },
        );
        assert!(matches!(
            committed,
            HostResponse::CaptureCommitted { ref capture_id } if capture_id == "receipt-proof"
        ));
        assert_eq!(
            serde_json::to_value(&HostResponse::CaptureMessageAck {
                capture_id: capture_id.clone(),
                stage: CaptureMessageStage::CaptureChunk,
                sequence: Some(7),
            }).unwrap(),
            serde_json::json!({
                "type": "capture_message_ack",
                "capture_id": "receipt-proof",
                "stage": "capture_chunk",
                "sequence": 7
            }),
        );
        assert_eq!(
            serde_json::to_value(&committed).unwrap(),
            serde_json::json!({
                "type": "capture_committed",
                "capture_id": "receipt-proof"
            }),
        );
        assert_eq!(store.stats().unwrap().captures, 1);
        assert!(matches!(
            handle_request(
                &mut store,
                HostRequest::CaptureFinish {
                    capture_id: capture_id.clone(),
                    encoded_data_length: None,
                    body_error: None,
                },
            ),
            HostResponse::Error { capture_id: Some(ref id), .. } if id == &capture_id
        ));
        assert_eq!(store.stats().unwrap().captures, 1);
    }

    #[test]
    fn cache_lookup_streams_hit_in_sub_megabyte_native_messages() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        let body = vec![b'x'; REPLAY_RAW_CHUNK_BYTES + 17];
        let metadata = CaptureMetadata {
            capture_id: "public-script".to_owned(),
            tab_id: 1,
            request_id: "request-public-script".to_owned(),
            method: "GET".to_owned(),
            url: "https://chatgpt.com/_next/static/replay.js".to_owned(),
            status: 200,
            mime_type: "application/javascript".to_owned(),
            resource_type: "Script".to_owned(),
            etag: Some("\"fixture-etag\"".to_owned()),
            last_modified: None,
            cache_control: Some("public, max-age=31536000, immutable".to_owned()),
            provenance: CaptureProvenance::default(),
        };
        store.begin(metadata).unwrap();
        store
            .append_chunk("public-script", 0, &BASE64.encode(&body))
            .unwrap();
        store.finish("public-script", None, None).unwrap();
        drop(store);

        let cache_reader = cache::CacheReader::open(directory.path()).unwrap();
        let mut output = Vec::new();
        write_cache_lookup_responses(
            &mut output,
            &cache_reader,
            "lookup-1".to_owned(),
            "https://chatgpt.com/_next/static/replay.js",
            "Script",
        )
        .unwrap();

        let mut input = output.as_slice();
        let mut responses = Vec::new();
        while let Some(payload) = read_native_message(&mut input).unwrap() {
            assert!(payload.len() <= MAX_NATIVE_RESPONSE_BYTES);
            responses.push(serde_json::from_slice::<HostResponse>(&payload).unwrap());
        }

        assert!(matches!(
            responses.first(),
            Some(HostResponse::CacheHitStart { lookup_id, .. }) if lookup_id == "lookup-1"
        ));
        assert!(matches!(
            responses.last(),
            Some(HostResponse::CacheHitFinish { lookup_id }) if lookup_id == "lookup-1"
        ));

        let mut reconstructed = Vec::new();
        let mut expected_sequence = 0_u32;
        for response in &responses {
            if let HostResponse::CacheHitChunk {
                sequence,
                data_base64,
                ..
            } = response
            {
                assert_eq!(*sequence, expected_sequence);
                expected_sequence += 1;
                reconstructed.extend(BASE64.decode(data_base64).unwrap());
            }
        }
        assert_eq!(expected_sequence, 2);
        assert_eq!(reconstructed, body);
    }

    #[test]
    fn private_read_lookup_streams_verified_private_json() {
        let directory = tempdir().unwrap();
        let mut store = CaptureStore::open(directory.path()).unwrap();
        let body = br#"{"id":"private-conversation"}"#;
        let metadata = CaptureMetadata {
            capture_id: "private-json".to_owned(),
            tab_id: 1,
            request_id: "request-private-json".to_owned(),
            method: "GET".to_owned(),
            url: "https://chatgpt.com/backend-api/conversation/private".to_owned(),
            status: 200,
            mime_type: "application/json".to_owned(),
            resource_type: "Fetch".to_owned(),
            etag: Some("\"private-v1\"".to_owned()),
            last_modified: None,
            cache_control: Some("private, max-age=0, must-revalidate".to_owned()),
            provenance: CaptureProvenance::default(),
        };
        store.begin(metadata).unwrap();
        store
            .append_chunk("private-json", 0, &BASE64.encode(body))
            .unwrap();
        store.finish("private-json", None, None).unwrap();
        drop(store);

        let cache_reader = cache::CacheReader::open(directory.path()).unwrap();
        let mut output = Vec::new();
        write_private_read_lookup_responses(
            &mut output,
            &cache_reader,
            "private-lookup-1".to_owned(),
            "https://chatgpt.com/backend-api/conversation/private",
        )
        .unwrap();

        let mut input = output.as_slice();
        let mut responses = Vec::new();
        while let Some(payload) = read_native_message(&mut input).unwrap() {
            assert!(payload.len() <= MAX_NATIVE_RESPONSE_BYTES);
            responses.push(serde_json::from_slice::<HostResponse>(&payload).unwrap());
        }

        assert!(matches!(
            responses.first(),
            Some(HostResponse::PrivateReadHitStart {
                lookup_id,
                etag: Some(etag),
                ..
            }) if lookup_id == "private-lookup-1" && etag == "\"private-v1\""
        ));
        assert!(matches!(
            responses.last(),
            Some(HostResponse::PrivateReadHitFinish { lookup_id })
                if lookup_id == "private-lookup-1"
        ));

        let mut reconstructed = Vec::new();
        for response in &responses {
            if let HostResponse::PrivateReadHitChunk { data_base64, .. } = response {
                reconstructed.extend(BASE64.decode(data_base64).unwrap());
            }
        }
        assert_eq!(reconstructed, body);
    }

    #[test]
    fn missing_ledger_is_a_cache_miss_not_an_open_error() {
        let directory = tempdir().unwrap();
        assert!(!directory.path().join("ledger.sqlite3").exists());
        assert!(open_cache_reader_if_present(directory.path()).unwrap().is_none());
        assert!(!directory.path().join("ledger.sqlite3").exists());
    }

    #[test]
    fn read_only_native_requests_do_not_require_writer_ownership() {
        let directory = tempdir().unwrap();
        let writer = CaptureStore::open(directory.path()).unwrap();

        let state = directory.path().join("missing-extension-state.json");
        assert!(read_extension_build_id_from_state(&state).unwrap().is_none());

        let reader = cache::CacheReader::open(directory.path()).unwrap();
        assert!(reader
            .lookup(
                "https://chatgpt.com/_next/static/missing.js",
                "Script",
            )
            .unwrap()
            .is_none());

        drop(reader);
        drop(writer);
    }

    #[test]
    fn extension_install_state_requires_matching_installed_manifest() {
        let directory = tempdir().unwrap();
        let install = directory.path().join("extension");
        fs::create_dir_all(&install).unwrap();
        fs::write(
            install.join("manifest.json"),
            serde_json::to_vec(&serde_json::json!({
                "manifest_version": 3,
                "name": "Mirrarium",
                "version": "0.1.0",
                "version_name": "0.1.0+abc"
            }))
            .unwrap(),
        )
        .unwrap();

        let state = directory.path().join("state.json");
        fs::write(
            &state,
            serde_json::to_vec(&serde_json::json!({
                "schema_version": 1,
                "extension_id": EXTENSION_ID,
                "build_id": "0.1.0+abc",
                "install_path": install
            }))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            read_extension_build_id_from_state(&state).unwrap().as_deref(),
            Some("0.1.0+abc")
        );

        let tree_hash = extension_tree_hash(&install).unwrap();
        let mut state_value: serde_json::Value =
            serde_json::from_slice(&fs::read(&state).unwrap()).unwrap();
        state_value["tree_hash"] = serde_json::Value::String(tree_hash);
        fs::write(&state, serde_json::to_vec(&state_value).unwrap()).unwrap();
        assert_eq!(
            read_extension_build_id_from_state(&state).unwrap().as_deref(),
            Some("0.1.0+abc")
        );
        fs::write(install.join("extra.js"), b"mixed generation").unwrap();
        assert!(read_extension_build_id_from_state(&state).unwrap().is_none());
        fs::remove_file(install.join("extra.js")).unwrap();

        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(directory.path().join("extension/manifest.json")).unwrap())
                .unwrap();
        manifest["version_name"] = serde_json::Value::String("0.1.0+different".to_owned());
        fs::write(
            directory.path().join("extension/manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        assert!(read_extension_build_id_from_state(&state).unwrap().is_none());
    }

    #[test]
    fn native_message_round_trip() {
        let payload = br#"{"type":"ping"}"#;
        let mut encoded = Vec::new();
        write_native_message(&mut encoded, payload).unwrap();

        let mut input = encoded.as_slice();
        assert_eq!(read_native_message(&mut input).unwrap().unwrap(), payload);
        assert!(read_native_message(&mut input).unwrap().is_none());
    }
}
