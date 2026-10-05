use std::io::{self, Read, Write};

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use mirrarium_cache as cache;
use mirrarium_protocol::{HostRequest, HostResponse};
use mirrarium_store::{default_data_root, CaptureStore};

const MAX_NATIVE_REQUEST_BYTES: usize = 64 * 1024 * 1024;
const MAX_NATIVE_RESPONSE_BYTES: usize = 1024 * 1024;
const REPLAY_RAW_CHUNK_BYTES: usize = 384 * 1024;

fn main() -> Result<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();

    if arguments.first().map(String::as_str) == Some("--stats") {
        let store = CaptureStore::open(default_data_root()?)?;
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
    let mut store = CaptureStore::open(&root)?;

    while let Some(payload) = read_native_message(&mut input)? {
        match serde_json::from_slice::<HostRequest>(&payload) {
            Ok(HostRequest::CacheLookup {
                lookup_id,
                url,
                resource_type,
            }) => {
                write_cache_lookup_responses(
                    &mut output,
                    &root,
                    lookup_id,
                    &url,
                    &resource_type,
                )?;
            }
            Ok(request) => {
                let response = handle_request(&mut store, request);
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
    root: &std::path::Path,
    lookup_id: String,
    url: &str,
    resource_type: &str,
) -> Result<()> {
    let entry = match cache::lookup(root, url, resource_type) {
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

fn write_native_response(writer: &mut impl Write, response: &HostResponse) -> Result<()> {
    write_native_message(writer, &serde_json::to_vec(response)?)
}

fn handle_request(store: &mut CaptureStore, request: HostRequest) -> HostResponse {
    let capture_id = match &request {
        HostRequest::CaptureStart { metadata } => Some(metadata.capture_id.clone()),
        HostRequest::CaptureChunk { capture_id, .. }
        | HostRequest::RequestBodyStart { capture_id, .. }
        | HostRequest::RequestBodyChunk { capture_id, .. }
        | HostRequest::RequestBodyFinish { capture_id, .. }
        | HostRequest::CaptureFinish { capture_id, .. } => Some(capture_id.clone()),
        HostRequest::Ping | HostRequest::CacheLookup { .. } => None,
    };

    let result = match request {
        HostRequest::Ping => return HostResponse::Pong,
        HostRequest::CacheLookup { lookup_id, .. } => {
            return HostResponse::CacheLookupError {
                lookup_id,
                message: "cache lookup must be handled by the streaming response path".to_owned(),
            };
        }
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
        Ok(()) => HostResponse::Ack { capture_id },
        Err(error) => HostResponse::Error {
            capture_id,
            message: format!("{error:#}"),
        },
    }
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

        let mut output = Vec::new();
        write_cache_lookup_responses(
            &mut output,
            directory.path(),
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
    fn native_message_round_trip() {
        let payload = br#"{"type":"ping"}"#;
        let mut encoded = Vec::new();
        write_native_message(&mut encoded, payload).unwrap();

        let mut input = encoded.as_slice();
        assert_eq!(read_native_message(&mut input).unwrap().unwrap(), payload);
        assert!(read_native_message(&mut input).unwrap().is_none());
    }
}
