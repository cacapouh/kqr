//! Format `RecordBatch`es into one of the four supported output flavours
//! (table / json / ndjson / csv).
//!
//! `OutputFormat::Table` uses comfy-table; the CLI is expected to fall back
//! to CSV when stdout isn't a TTY (see `kqr-cli`).

use std::io::Write;

use arrow::array::{Array, RecordBatch};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow_csv::WriterBuilder as CsvWriterBuilder;
use arrow_json::writer::{JsonArray, LineDelimited, WriterBuilder};
use comfy_table::presets::UTF8_BORDERS_ONLY;
use comfy_table::Table;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
    Ndjson,
    Csv,
}

/// Write `batches` to `out` in the requested format. Empty input writes
/// only headers (or nothing for non-tabular formats).
pub fn write_batches<W: Write>(
    batches: &[RecordBatch],
    format: OutputFormat,
    out: &mut W,
) -> Result<()> {
    match format {
        OutputFormat::Table => write_table(batches, out),
        OutputFormat::Json => write_json(batches, out, false),
        OutputFormat::Ndjson => write_json(batches, out, true),
        OutputFormat::Csv => write_csv(batches, out),
    }
}

fn write_table<W: Write>(batches: &[RecordBatch], out: &mut W) -> Result<()> {
    let mut table = Table::new();
    table.load_preset(UTF8_BORDERS_ONLY);
    if let Some(first) = batches.first() {
        table.set_header(first.schema().fields().iter().map(|f| f.name().to_string()));
    } else {
        writeln!(out, "(empty result)")?;
        return Ok(());
    }
    let opts = FormatOptions::default().with_null("");
    for batch in batches {
        let formatters: Vec<ArrayFormatter> = (0..batch.num_columns())
            .map(|c| ArrayFormatter::try_new(batch.column(c).as_ref(), &opts))
            .collect::<std::result::Result<_, _>>()?;
        for r in 0..batch.num_rows() {
            let row: Vec<String> = formatters
                .iter()
                .enumerate()
                .map(|(c, f)| {
                    if batch.column(c).is_null(r) {
                        String::new()
                    } else {
                        f.value(r).to_string()
                    }
                })
                .collect();
            table.add_row(row);
        }
    }
    writeln!(out, "{table}")?;
    Ok(())
}

fn write_json<W: Write>(batches: &[RecordBatch], out: &mut W, line_delimited: bool) -> Result<()> {
    if line_delimited {
        let mut w = WriterBuilder::new().build::<_, LineDelimited>(out);
        for batch in batches {
            w.write(batch)?;
        }
        w.finish()?;
    } else {
        let mut w = WriterBuilder::new().build::<_, JsonArray>(out);
        for batch in batches {
            w.write(batch)?;
        }
        w.finish()?;
    }
    Ok(())
}

fn write_csv<W: Write>(batches: &[RecordBatch], out: &mut W) -> Result<()> {
    let mut w = CsvWriterBuilder::new().with_header(true).build(out);
    for batch in batches {
        w.write(batch)
            .map_err(|e| Error::Output(format!("csv: {e}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("side", DataType::Utf8, false),
        ]));
        let id = Int64Array::from(vec![1, 2, 3]);
        let side = StringArray::from(vec!["buy", "sell", "buy"]);
        RecordBatch::try_new(schema, vec![Arc::new(id), Arc::new(side)]).unwrap()
    }

    #[test]
    fn csv_round_trip() {
        let mut buf = Vec::new();
        write_batches(&[batch()], OutputFormat::Csv, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.starts_with("id,side\n"));
        assert!(s.contains("1,buy"));
        assert!(s.contains("3,buy"));
    }

    #[test]
    fn ndjson_round_trip() {
        let mut buf = Vec::new();
        write_batches(&[batch()], OutputFormat::Ndjson, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = s.trim_end().lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("\"id\":1"));
        assert!(lines[2].contains("\"side\":\"buy\""));
    }

    #[test]
    fn json_array_round_trip() {
        let mut buf = Vec::new();
        write_batches(&[batch()], OutputFormat::Json, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.starts_with('['));
        assert!(s.trim_end().ends_with(']'));
    }

    #[test]
    fn table_emits_unicode_borders() {
        let mut buf = Vec::new();
        write_batches(&[batch()], OutputFormat::Table, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("id"));
        assert!(s.contains("side"));
        assert!(s.contains("buy"));
    }

    #[test]
    fn empty_table_says_so() {
        let mut buf = Vec::new();
        write_batches(&[], OutputFormat::Table, &mut buf).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "(empty result)\n");
    }
}
