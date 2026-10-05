use std::{
    env,
    io::{self, Read, Write},
    path::PathBuf,
};

use anyhow::{Context, Result};
use mirrarium_protocol::{HostRequest, HostResponse};
use mirrarium_store::CaptureStore;

const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

fn main() -> Result<()> {
    let arguments: Vec<String> = env::args().skip(1).collect();

    if arguments.first().map(String::as_str) == Some("--stats") {
        let store = CaptureStore::open(data_root()?)?;
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
    let mut store = CaptureStore::open(data_root()?)?;

    while let Some(payload) = read_native_message(&mut input)? {
        let response = match serde_json::from_slice::<HostRequest>(&payload) {
            Ok(request) => handle_request(&mut store, request),
            Err(error) => HostResponse::Error {
                capture_id: None,
                message: format!("invalid request: {error}"),
            },
        };

        write_native_message(&mut output, &serde_json::to_vec(&response)?)?;
    }

    Ok(())
}

fn handle_request(store: &mut CaptureStore, request: HostRequest) -> HostResponse {
    let capture_id = match &request {
        HostRequest::CaptureStart { metadata } => Some(metadata.capture_id.clone()),
        HostRequest::CaptureChunk { capture_id, .. }
        | HostRequest::CaptureFinish { capture_id, .. } => Some(capture_id.clone()),
        HostRequest::Ping => None,
    };

    let result = match request {
        HostRequest::Ping => return HostResponse::Pong,
        HostRequest::CaptureStart { metadata } => store.begin(metadata),
        HostRequest::CaptureChunk {
            capture_id,
            sequence,
            data_base64,
        } => store.append_chunk(&capture_id, sequence, &data_base64),
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

fn data_root() -> Result<PathBuf> {
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
        len <= MAX_MESSAGE_BYTES,
        "native message too large: {len} bytes"
    );

    let mut payload = vec![0_u8; len];
    reader
        .read_exact(&mut payload)
        .context("reading native-message payload")?;

    Ok(Some(payload))
}

fn write_native_message(writer: &mut impl Write, payload: &[u8]) -> Result<()> {
    let len = u32::try_from(payload.len()).context("native response exceeds u32 framing")?;
    writer.write_all(&len.to_ne_bytes())?;
    writer.write_all(payload)?;
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
