use std::io::{self, Read, Write};

use anyhow::{Context, Result};
use mirrarium_protocol::{HostRequest, HostResponse};

const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

fn main() -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();

    while let Some(payload) = read_native_message(&mut input)? {
        let response = match serde_json::from_slice::<HostRequest>(&payload) {
            Ok(HostRequest::Ping) => HostResponse::Pong,
            Ok(HostRequest::ObserveResponse { request_id, .. }) => {
                HostResponse::Ack { request_id }
            }
            Err(error) => HostResponse::Error {
                message: format!("invalid request: {error}"),
            },
        };

        write_native_message(&mut output, &serde_json::to_vec(&response)?)?;
    }

    Ok(())
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

    let len = u32::from_le_bytes(header) as usize;
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
    writer.write_all(&len.to_le_bytes())?;
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
