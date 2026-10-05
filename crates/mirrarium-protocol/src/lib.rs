use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CaptureProvenance {
    pub frame_id: Option<String>,
    pub loader_id: Option<String>,
    pub lifecycle_id: Option<String>,
    pub redirect_hop: Option<u32>,
    pub redirected_from_url: Option<String>,
    pub document_url: Option<String>,
    pub initiator_type: Option<String>,
    pub request_wall_time_ms: Option<u64>,
    pub response_time_ms: Option<u64>,
    pub response_protocol: Option<String>,
    pub served_from_cache: bool,
    pub from_disk_cache: bool,
    pub from_service_worker: bool,
    pub from_prefetch_cache: bool,
    pub request_headers: BTreeMap<String, String>,
    pub response_headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RequestBodyMetadata {
    pub content_type: Option<String>,
    pub has_post_data: bool,
    pub post_data_entry_count: Option<u32>,
    pub declared_content_length: Option<u64>,
}

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
    pub provenance: CaptureProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostRequest {
    Ping,
    CacheLookup {
        lookup_id: String,
        url: String,
        resource_type: String,
    },
    CaptureStart { metadata: CaptureMetadata },
    CaptureChunk {
        capture_id: String,
        sequence: u32,
        data_base64: String,
    },
    RequestBodyStart {
        capture_id: String,
        metadata: RequestBodyMetadata,
    },
    RequestBodyChunk {
        capture_id: String,
        sequence: u32,
        data_base64: String,
    },
    RequestBodyFinish {
        capture_id: String,
        body_error: Option<String>,
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
    CacheMiss {
        lookup_id: String,
    },
    CacheHitStart {
        lookup_id: String,
        mime_type: String,
        body_hash: String,
        body_bytes: u64,
        cache_control: Option<String>,
        etag: Option<String>,
        last_modified: Option<String>,
    },
    CacheHitChunk {
        lookup_id: String,
        sequence: u32,
        data_base64: String,
    },
    CacheHitFinish {
        lookup_id: String,
    },
    CacheLookupError {
        lookup_id: String,
        message: String,
    },
    Ack { capture_id: Option<String> },
    Error {
        capture_id: Option<String>,
        message: String,
    },
}
