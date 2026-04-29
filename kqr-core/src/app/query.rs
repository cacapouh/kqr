//! DataFusion `SessionContext` wrapper.
//!
//! Holds registered `MemTable`s for each topic the CLI has consumed and
//! executes user SQL against them. `--explain` short-circuits to printing
//! logical + physical plans instead of running the query.

use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use crate::error::{Error, Result};

pub struct QueryEngine {
    ctx: SessionContext,
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryEngine {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    /// Register `table` under `name`. Subsequent SQL can reference it by name.
    pub fn register_table(&self, name: &str, table: MemTable) -> Result<()> {
        self.ctx.register_table(name, Arc::new(table))?;
        Ok(())
    }

    /// Execute SQL and collect all results into memory. The CLI drives output
    /// formatting from the returned batches.
    pub async fn execute(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(sql).await?;
        Ok(df.collect().await?)
    }

    /// Return DataFusion's `EXPLAIN` output as a single human-readable string.
    /// Does not run the query.
    pub async fn explain(&self, sql: &str) -> Result<String> {
        let df = self.ctx.sql(&format!("EXPLAIN {sql}")).await?;
        let batches = df.collect().await?;
        format_explain(&batches)
    }
}

fn format_explain(batches: &[RecordBatch]) -> Result<String> {
    let mut out = String::new();
    for batch in batches {
        if batch.num_columns() < 2 {
            return Err(Error::Query("EXPLAIN returned unexpected schema".into()));
        }
        let plan_type = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::Query("EXPLAIN plan_type not Utf8".into()))?;
        let plan = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::Query("EXPLAIN plan not Utf8".into()))?;
        for i in 0..batch.num_rows() {
            if !plan_type.is_null(i) {
                out.push_str(&format!("=== {} ===\n", plan_type.value(i)));
            }
            if !plan.is_null(i) {
                out.push_str(plan.value(i));
                out.push_str("\n\n");
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::app::table::TableBuilder;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn one_table() -> MemTable {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let arr = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)]).unwrap();
        let mut b = TableBuilder::new(schema);
        b.push(batch);
        b.build().unwrap()
    }

    #[tokio::test]
    async fn execute_select_count() {
        let q = QueryEngine::new();
        q.register_table("t", one_table()).unwrap();
        let batches = q.execute("select count(*) as n from t").await.unwrap();
        assert_eq!(batches.len(), 1);
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 5);
    }

    #[tokio::test]
    async fn execute_sum() {
        let q = QueryEngine::new();
        q.register_table("t", one_table()).unwrap();
        let batches = q.execute("select sum(v) as s from t").await.unwrap();
        let s = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(s, 15);
    }

    #[tokio::test]
    async fn explain_returns_text() {
        let q = QueryEngine::new();
        q.register_table("t", one_table()).unwrap();
        let plan = q.explain("select count(*) from t").await.unwrap();
        assert!(plan.contains("logical_plan") || plan.to_lowercase().contains("plan"));
    }
}
