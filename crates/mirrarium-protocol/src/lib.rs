use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureMetadata {
    pub capture_id: String,
    pub tab_id: i64,
    pub request_id: String,
    pub method: String,
    pub url: String,
    pub status: i64,
    pub mime_type: String,
    pub resource_type: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub cache_control: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostRequest {
    Ping,
    CaptureStart {
        metadata: CaptureMetadata,
    },
    CaptureChunk {
        capture_id: String,
        sequence: u32,
        data_base64: String,
    },
    CaptureFinish {
        capture_id: String,
        encoded_data_length: Option<u64>,
        body_error: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostResponse {
    Pong,
    Ack {
        capture_id: Option<String>,
    },
    Error {
        capture_id: Option<String>,
        message: String,
    },
}
