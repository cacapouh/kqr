//! `kqr schema` — sample N messages from a topic and print the inferred
//! Arrow schema.

use kqr_core::app::decode::{JsonDecoder, MessageDecoder};
use kqr_core::infra::config::Profile;
use kqr_core::infra::kafka::{KafkaSource, RawMessage, RdkafkaSource};
use tokio::sync::mpsc;

use crate::cli::SchemaArgs;
use crate::commands::resolve_window;

pub async fn run(args: SchemaArgs, profile: &Profile) -> anyhow::Result<()> {
    let window = resolve_window(&args.window)?;
    let sample_size = args.consume.schema_sample.max(1) as u64;

    let source = RdkafkaSource::new(profile, args.consume.consumer_group_id.as_deref())?;
    if args.consume.consumer_group_id.is_some() {
        eprintln!(
            "[kqr] --consumer-group-id set: offsets will be committed to group '{}'",
            args.consume.consumer_group_id.as_deref().unwrap()
        );
    }

    let (tx, mut rx) = mpsc::channel::<RawMessage>(256);

    let topic = args.topic.clone();
    let consume_task =
        tokio::spawn(async move { source.consume(&topic, &window, Some(sample_size), tx).await });

    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(sample_size as usize);
    while let Some(msg) = rx.recv().await {
        payloads.push(msg.value);
        if payloads.len() as u64 >= sample_size {
            break;
        }
    }
    drop(rx);
    let _ = consume_task.await?;

    if payloads.is_empty() {
        anyhow::bail!(
            "no messages observed in window — expand the time window or check broker connectivity"
        );
    }

    let decoder = JsonDecoder::new();
    let schema = decoder.infer_schema(&payloads)?;
    print_schema(&schema, payloads.len());
    Ok(())
}

fn print_schema(schema: &arrow::datatypes::Schema, sampled: usize) {
    println!("inferred from {sampled} samples:");
    let name_col = schema
        .fields()
        .iter()
        .map(|f| f.name().len())
        .max()
        .unwrap_or(4);
    let type_col = schema
        .fields()
        .iter()
        .map(|f| format!("{}", f.data_type()).len())
        .max()
        .unwrap_or(8);

    let header = format!(
        "  {:<name_col$}  {:<type_col$}  {}",
        "field",
        "type",
        "nullable",
        name_col = name_col,
        type_col = type_col,
    );
    println!("{header}");
    println!("  {:-<width$}", "", width = name_col + 2 + type_col + 2 + 8);

    for f in schema.fields() {
        println!(
            "  {:<name_col$}  {:<type_col$}  {}",
            f.name(),
            format!("{}", f.data_type()),
            if f.is_nullable() { "yes" } else { "no" },
            name_col = name_col,
            type_col = type_col,
        );
    }
}
