//! `kqr query` — consume topics, register them as SQL tables, run user SQL.

use std::io::{IsTerminal, Write};
use std::sync::Arc;
use std::time::Duration;

use kqr_core::app::cache::ParquetCache;
use kqr_core::app::decode::{JsonDecoder, MessageDecoder};
use kqr_core::app::output::{write_batches, OutputFormat};
use kqr_core::app::query::QueryEngine;
use kqr_core::app::table::{sql_table_name, TableBuilder};
use kqr_core::infra::config::Profile;
use kqr_core::infra::kafka::{KafkaSource, RawMessage, RdkafkaSource, TimeWindow};
use tokio::sync::mpsc;
use tracing::warn;

use crate::cli::{OutputFormat as CliOutputFormat, QueryArgs};
use crate::commands::resolve_window;

/// Default cache TTL when config doesn't specify (= DESIGN.md default 1h).
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(3600);

pub async fn run(args: QueryArgs, profile: &Profile) -> anyhow::Result<()> {
    if args.topics.is_empty() {
        anyhow::bail!("at least one --topic is required");
    }
    let window = resolve_window(&args.window)?;

    if args.consume.consumer_group_id.is_some() {
        eprintln!(
            "[kqr] --consumer-group-id set: offsets will be committed to group '{}'",
            args.consume.consumer_group_id.as_deref().unwrap()
        );
    }

    let cache = if args.reuse && !args.no_reuse {
        Some(ParquetCache::new(
            ParquetCache::default_root(),
            DEFAULT_CACHE_TTL,
        ))
    } else {
        None
    };

    let engine = QueryEngine::new();
    for topic in &args.topics {
        let (sql_name, was_changed) = sql_table_name(topic);
        if was_changed {
            eprintln!(
                "[kqr] topic '{}' is not a valid SQL identifier; using '{}' as the table name",
                topic, sql_name
            );
        }
        register_topic(
            &engine,
            profile,
            &args,
            topic,
            &sql_name,
            &window,
            cache.as_ref(),
        )
        .await?;
    }

    if args.explain {
        let explained = engine.explain(&args.sql).await?;
        print!("{explained}");
        return Ok(());
    }

    let batches = engine.execute(&args.sql).await?;
    let format = pick_format(args.format.format);
    let stdout = std::io::stdout();
    let mut lock = stdout.lock();
    write_batches(&batches, format, &mut lock)?;
    lock.flush()?;
    Ok(())
}

async fn register_topic(
    engine: &QueryEngine,
    profile: &Profile,
    args: &QueryArgs,
    topic: &str,
    sql_name: &str,
    window: &TimeWindow,
    cache: Option<&ParquetCache>,
) -> anyhow::Result<()> {
    // Try cache first if enabled.
    if let Some(c) = cache {
        let key = c.key_path(&profile.brokers, topic, window);
        if let Some(batches) = c.read(&key)? {
            tracing::info!(topic = topic, "cache hit at {}", key.display());
            if let Some(first) = batches.first() {
                let schema = first.schema();
                let mut builder = TableBuilder::new(schema);
                for b in batches {
                    builder.push(b);
                }
                engine.register_table(sql_name, builder.build()?)?;
                return Ok(());
            }
        }
    }

    let source = RdkafkaSource::new(profile, args.consume.consumer_group_id.as_deref())?;
    let (tx, mut rx) = mpsc::channel::<RawMessage>(1024);

    let topic_owned = topic.to_string();
    let window_owned = window.clone();
    let consume_task =
        tokio::spawn(async move { source.consume(&topic_owned, &window_owned, None, tx).await });

    let mut payloads: Vec<Vec<u8>> = Vec::new();
    while let Some(msg) = rx.recv().await {
        payloads.push(msg.value);
    }
    let stats = consume_task.await??;
    tracing::debug!(
        topic = topic,
        messages = stats.messages,
        bytes = stats.bytes,
        "consumed for table build"
    );

    if payloads.is_empty() {
        anyhow::bail!(
            "no messages observed for topic '{topic}' in the window; expand --last or check brokers"
        );
    }

    let decoder = JsonDecoder::new();
    let sample_n = args.consume.schema_sample.min(payloads.len()).max(1);
    let schema = decoder.infer_schema(&payloads[..sample_n])?;
    let batch = decoder.decode_batch(Arc::clone(&schema), &payloads)?;

    // Persist to cache before building MemTable so a cache failure doesn't
    // poison the live query.
    if let Some(c) = cache {
        let key = c.key_path(&profile.brokers, topic, window);
        if let Err(e) = c.write(&key, Arc::clone(&schema), std::slice::from_ref(&batch)) {
            warn!("cache write failed at {}: {e}", key.display());
        }
    }

    let mut builder = TableBuilder::new(schema);
    builder.push(batch);
    let table = builder.build()?;
    engine.register_table(sql_name, table)?;
    Ok(())
}

fn pick_format(requested: CliOutputFormat) -> OutputFormat {
    match requested {
        CliOutputFormat::Table => {
            if std::io::stdout().is_terminal() {
                OutputFormat::Table
            } else {
                // Bare table is hard to consume in a pipe — fall back to CSV.
                warn!("stdout is not a TTY; --format table → csv");
                OutputFormat::Csv
            }
        }
        CliOutputFormat::Json => OutputFormat::Json,
        CliOutputFormat::Ndjson => OutputFormat::Ndjson,
        CliOutputFormat::Csv => OutputFormat::Csv,
    }
}
