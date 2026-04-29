//! JSON message decoder + schema inference.
//!
//! Powered by `arrow-json`'s `Decoder`/`infer_json_schema_from_iterator`. Each
//! message is expected to be a self-contained JSON object on the wire (one
//! object per Kafka record). Newlines inside the JSON are tolerated.

use std::io::{BufReader, Cursor};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow_json::reader::{infer_json_schema, ReaderBuilder};

use crate::app::decode::MessageDecoder;
use crate::error::{Error, Result};

/// JSON decoder. Stateless; can be cloned freely.
#[derive(Debug, Default, Clone)]
pub struct JsonDecoder;

impl JsonDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl MessageDecoder for JsonDecoder {
    fn infer_schema(&self, samples: &[Vec<u8>]) -> Result<Arc<Schema>> {
        if samples.is_empty() {
            return Err(Error::DecodeSchemaInferEmpty);
        }
        let combined = join_with_newlines(samples);
        if combined.iter().all(|b| b.is_ascii_whitespace()) {
            return Err(Error::DecodeSchemaInferEmpty);
        }
        let mut reader = BufReader::new(Cursor::new(&combined));
        let (schema, _records_read) = infer_json_schema(&mut reader, Some(samples.len()))?;
        Ok(Arc::new(schema))
    }

    fn decode_batch(&self, schema: Arc<Schema>, messages: &[Vec<u8>]) -> Result<RecordBatch> {
        if messages.is_empty() {
            // Build an empty RecordBatch with the right schema.
            let empty = RecordBatch::new_empty(schema);
            return Ok(empty);
        }
        let combined = join_with_newlines(messages);
        let mut decoder = ReaderBuilder::new(schema)
            .with_batch_size(messages.len())
            .build_decoder()?;
        decoder.decode(&combined)?;
        decoder
            .flush()?
            .ok_or_else(|| Error::Decode("decoder produced no batch from non-empty input".into()))
    }
}

fn join_with_newlines(messages: &[Vec<u8>]) -> Vec<u8> {
    let total = messages.iter().map(|m| m.len() + 1).sum();
    let mut out = Vec::with_capacity(total);
    for m in messages {
        out.extend_from_slice(m);
        if !m.ends_with(b"\n") {
            out.push(b'\n');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn schema_inference_basic_types() {
        let dec = JsonDecoder::new();
        let samples = vec![
            b(r#"{"id": 1, "price": 1.5, "side": "buy"}"#),
            b(r#"{"id": 2, "price": 2.5, "side": "sell"}"#),
        ];
        let schema = dec.infer_schema(&samples).unwrap();
        let id_field = schema.field_with_name("id").unwrap();
        let price_field = schema.field_with_name("price").unwrap();
        let side_field = schema.field_with_name("side").unwrap();
        assert!(matches!(id_field.data_type(), DataType::Int64));
        assert!(matches!(price_field.data_type(), DataType::Float64));
        assert!(matches!(side_field.data_type(), DataType::Utf8));
    }

    #[test]
    fn schema_inference_empty_errors() {
        let dec = JsonDecoder::new();
        let err = dec.infer_schema(&[]).unwrap_err();
        assert!(matches!(err, Error::DecodeSchemaInferEmpty));
    }

    #[test]
    fn schema_inference_whitespace_only_errors() {
        let dec = JsonDecoder::new();
        let err = dec.infer_schema(&[b("   "), b("\n\n")]).unwrap_err();
        assert!(matches!(err, Error::DecodeSchemaInferEmpty));
    }

    #[test]
    fn decode_batch_round_trip() {
        let dec = JsonDecoder::new();
        let samples = vec![
            b(r#"{"id": 1, "side": "buy"}"#),
            b(r#"{"id": 2, "side": "sell"}"#),
            b(r#"{"id": 3, "side": "buy"}"#),
        ];
        let schema = dec.infer_schema(&samples).unwrap();
        let batch = dec.decode_batch(schema.clone(), &samples).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema().fields().len(), schema.fields().len());
    }

    #[test]
    fn decode_batch_empty_returns_empty_batch() {
        let dec = JsonDecoder::new();
        let samples = vec![b(r#"{"id": 1}"#)];
        let schema = dec.infer_schema(&samples).unwrap();
        let batch = dec.decode_batch(schema.clone(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn decode_batch_handles_already_newline_terminated() {
        let dec = JsonDecoder::new();
        let samples = vec![b("{\"id\": 1}\n"), b("{\"id\": 2}\n")];
        let schema = dec.infer_schema(&samples).unwrap();
        let batch = dec.decode_batch(schema, &samples).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }
}
