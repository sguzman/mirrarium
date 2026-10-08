use std::{
    fs,
    io::{BufReader, Read, Write},
    path::Path,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use mirrarium_protocol::{
    CaptureMetadata, CaptureProvenance, HostRequest, HostResponse,
};

struct NativeHost {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

impl NativeHost {
    fn spawn(root: &Path, key_path: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_mirrariumd"))
            .env("MIRRARIUM_DATA_DIR", root)
            .env("MIRRARIUM_PRIVATE_KEY_FILE", key_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn mirrariumd");

        let stdin = child.stdin.take().expect("daemon stdin");
        let stdout = child.stdout.take().expect("daemon stdout");
        Self {
            child,
            stdin: Some(stdin),
            stdout: BufReader::new(stdout),
        }
    }

    fn send(&mut self, request: &HostRequest) -> HostResponse {
        let payload = serde_json::to_vec(request).expect("serialize native request");
        let length = u32::try_from(payload.len()).expect("native request length");

        let stdin = self.stdin.as_mut().expect("native host stdin open");
        stdin
            .write_all(&length.to_ne_bytes())
            .expect("write native header");
        stdin.write_all(&payload).expect("write native payload");
        stdin.flush().expect("flush native request");

        let mut header = [0_u8; 4];
        self.stdout
            .read_exact(&mut header)
            .expect("read native response header");
        let response_len = u32::from_ne_bytes(header) as usize;
        let mut response = vec![0_u8; response_len];
        self.stdout
            .read_exact(&mut response)
            .expect("read native response payload");
        serde_json::from_slice(&response).expect("deserialize native response")
    }

    fn kill(mut self) {
        self.child.kill().expect("kill native host");
        self.child.wait().expect("wait for killed native host");
    }

    fn close(mut self) {
        drop(self.stdin.take());
        let status = self.child.wait().expect("wait for native host");
        assert!(status.success(), "native host did not exit cleanly: {status}");
    }
}

fn metadata(capture_id: &str, url: &str) -> CaptureMetadata {
    CaptureMetadata {
        capture_id: capture_id.to_owned(),
        tab_id: 1,
        request_id: format!("request-{capture_id}"),
        method: "GET".to_owned(),
        url: url.to_owned(),
        status: 200,
        mime_type: "application/json".to_owned(),
        resource_type: "Fetch".to_owned(),
        etag: None,
        last_modified: None,
        cache_control: None,
        provenance: CaptureProvenance::default(),
    }
}

fn expect_ack(response: HostResponse, capture_id: &str) {
    match response {
        HostResponse::Ack {
            capture_id: Some(actual),
        } => assert_eq!(actual, capture_id),
        other => panic!("expected capture ack for {capture_id}, got {other:?}"),
    }
}

fn expect_committed(response: HostResponse, capture_id: &str) {
    match response {
        HostResponse::CaptureCommitted { capture_id: actual } => {
            assert_eq!(actual, capture_id)
        }
        other => panic!("expected durable capture receipt for {capture_id}, got {other:?}"),
    }
}

fn incoming_parts(root: &Path) -> Vec<String> {
    let incoming = root.join(".incoming");
    if !incoming.is_dir() {
        return Vec::new();
    }
    let mut parts = fs::read_dir(incoming)
        .expect("read incoming")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".part"))
        .collect::<Vec<_>>();
    parts.sort();
    parts
}

#[test]
fn daemon_restart_purges_abandoned_capture_and_accepts_new_capture() {
    let directory = tempfile::tempdir().expect("temp data root");
    let root = directory.path();
    let key_path = root.join("private.key");

    let mut first = NativeHost::spawn(root, &key_path);

    expect_ack(
        first.send(&HostRequest::CaptureStart {
            metadata: metadata(
                "committed-before-crash",
                "https://chatgpt.com/backend-api/committed-before-crash",
            ),
        }),
        "committed-before-crash",
    );
    let committed_body = br#"{"committed":true}"#;
    expect_ack(
        first.send(&HostRequest::CaptureChunk {
            capture_id: "committed-before-crash".to_owned(),
            sequence: 0,
            data_base64: BASE64.encode(committed_body),
        }),
        "committed-before-crash",
    );
    expect_committed(
        first.send(&HostRequest::CaptureFinish {
            capture_id: "committed-before-crash".to_owned(),
            encoded_data_length: Some(committed_body.len() as u64),
            body_error: None,
        }),
        "committed-before-crash",
    );
    expect_ack(
        first.send(&HostRequest::CaptureStart {
            metadata: metadata(
                "crash-capture",
                "https://chatgpt.com/backend-api/crash-before-finish",
            ),
        }),
        "crash-capture",
    );

    let partial_body = br#"{"partial":true}"#;
    expect_ack(
        first.send(&HostRequest::CaptureChunk {
            capture_id: "crash-capture".to_owned(),
            sequence: 0,
            data_base64: BASE64.encode(partial_body),
        }),
        "crash-capture",
    );

    let abandoned = incoming_parts(root);
    assert_eq!(abandoned.len(), 1, "expected one in-flight capture part");

    first.kill();

    let mut second = NativeHost::spawn(root, &key_path);
    expect_ack(
        second.send(&HostRequest::CaptureStart {
            metadata: metadata(
                "recovered-capture",
                "https://chatgpt.com/backend-api/recovered-after-crash",
            ),
        }),
        "recovered-capture",
    );

    let recovered_body = br#"{"ok":true}"#;
    expect_ack(
        second.send(&HostRequest::CaptureChunk {
            capture_id: "recovered-capture".to_owned(),
            sequence: 0,
            data_base64: BASE64.encode(recovered_body),
        }),
        "recovered-capture",
    );
    expect_committed(
        second.send(&HostRequest::CaptureFinish {
            capture_id: "recovered-capture".to_owned(),
            encoded_data_length: Some(recovered_body.len() as u64),
            body_error: None,
        }),
        "recovered-capture",
    );

    match second.send(&HostRequest::Ping) {
        HostResponse::Pong => {}
        other => panic!("expected pong after restart, got {other:?}"),
    }
    second.close();

    assert!(
        incoming_parts(root).is_empty(),
        "abandoned or recovered capture staging survived daemon restart"
    );

    let stats_output = Command::new(env!("CARGO_BIN_EXE_mirrariumd"))
        .arg("--stats")
        .env("MIRRARIUM_DATA_DIR", root)
        .env("MIRRARIUM_PRIVATE_KEY_FILE", &key_path)
        .output()
        .expect("run recovered daemon stats");
    assert!(
        stats_output.status.success(),
        "recovered stats failed: {}",
        String::from_utf8_lossy(&stats_output.stderr)
    );
    let stats: serde_json::Value =
        serde_json::from_slice(&stats_output.stdout).expect("parse recovered stats");
    assert_eq!(stats["captures"], 2);
    assert_eq!(stats["private_captures"], 2);
    assert_eq!(stats["private_objects"], 2);
    assert_eq!(stats["body_errors"], 0);
    assert_eq!(
        stats["captured_body_bytes"],
        (committed_body.len() + recovered_body.len()) as u64
    );
}
