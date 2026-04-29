//! Pluggable message decoders.
//!
//! The CLI currently exposes only [`json::JsonDecoder`]. The trait split is
//! kept so that step ≥ N can add Avro / Protobuf without churning callers.

pub mod json;

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;

use crate::error::Result;

/// Decode raw Kafka message payloads into Arrow `RecordBatch`es.
pub trait MessageDecoder: Send + Sync {
    /// Build a schema from the first N messages (caller's choice of N).
    fn infer_schema(&self, samples: &[Vec<u8>]) -> Result<Arc<Schema>>;

    /// Decode `messages` against `schema` into one `RecordBatch`.
    fn decode_batch(&self, schema: Arc<Schema>, messages: &[Vec<u8>]) -> Result<RecordBatch>;
}

pub use json::JsonDecoder;
