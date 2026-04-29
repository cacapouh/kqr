//! Accumulate `RecordBatch`es and convert to a DataFusion `MemTable`.
//!
//! Used by step 5+ when registering a topic as a SQL table in a
//! `SessionContext`.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::MemTable;

use crate::error::Result;

/// Accumulator that builds a single `MemTable` from one or more
/// `RecordBatch`es sharing a schema.
#[derive(Debug)]
pub struct TableBuilder {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl TableBuilder {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: Vec::new(),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub fn push(&mut self, batch: RecordBatch) {
        self.batches.push(batch);
    }

    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Consume the builder and produce a [`MemTable`] suitable for
    /// `SessionContext::register_table`.
    pub fn build(self) -> Result<MemTable> {
        // MemTable expects a Vec<Vec<RecordBatch>> (outer = partitions).
        // We use a single partition; DataFusion will still parallelise via
        // the default execution plan.
        let partitions = vec![self.batches];
        let table = MemTable::try_new(self.schema, partitions)?;
        Ok(table)
    }
}

/// Sanitise a Kafka topic name into a valid SQL identifier.
///
/// Replaces non `[A-Za-z0-9_]` bytes with `_`. Returns the sanitised name
/// alongside a `bool` flag indicating whether the original needed
/// rewriting (callers should warn the user when so).
pub fn sql_table_name(topic: &str) -> (String, bool) {
    let mut out = String::with_capacity(topic.len());
    let mut changed = false;
    let mut chars = topic.chars();
    if let Some(first) = chars.next() {
        if first.is_ascii_alphabetic() || first == '_' {
            out.push(first);
        } else {
            out.push('_');
            changed = true;
            if first.is_ascii_alphanumeric() {
                out.push(first);
            }
        }
    }
    for c in chars {
        if c.is_ascii_alphanumeric() || c == '_' {
            out.push(c);
        } else {
            out.push('_');
            changed = true;
        }
    }
    if out.is_empty() {
        out.push('_');
        changed = true;
    }
    (out, changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch(rows: &[i64]) -> RecordBatch {
        let arr = Int64Array::from(rows.to_vec());
        RecordBatch::try_new(schema(), vec![Arc::new(arr)]).unwrap()
    }

    #[test]
    fn builder_accumulates_rows() {
        let mut b = TableBuilder::new(schema());
        assert_eq!(b.num_rows(), 0);
        b.push(batch(&[1, 2, 3]));
        b.push(batch(&[4, 5]));
        assert_eq!(b.num_rows(), 5);
    }

    #[test]
    fn builder_builds_memtable() {
        let mut b = TableBuilder::new(schema());
        b.push(batch(&[1, 2]));
        let _table = b.build().unwrap();
        // We don't run a query here (that's step 5); just confirm construction.
    }

    #[test]
    fn sql_table_name_preserves_valid() {
        assert_eq!(sql_table_name("orders"), ("orders".to_string(), false));
        assert_eq!(
            sql_table_name("ord_42_v2"),
            ("ord_42_v2".to_string(), false)
        );
    }

    #[test]
    fn sql_table_name_replaces_dashes() {
        assert_eq!(
            sql_table_name("user-events"),
            ("user_events".to_string(), true)
        );
    }

    #[test]
    fn sql_table_name_replaces_dots_and_slashes() {
        assert_eq!(
            sql_table_name("svc.order/v1"),
            ("svc_order_v1".to_string(), true)
        );
    }

    #[test]
    fn sql_table_name_handles_leading_digit() {
        let (n, changed) = sql_table_name("9lives");
        assert_eq!(n, "_9lives");
        assert!(changed);
    }
}
