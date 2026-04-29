//! Subcommand handlers. Thin shims that translate clap args into core calls
//! and format results — the layer rule keeps business logic in `kqr-core`.

pub mod query;
pub mod repl;
pub mod sample;
pub mod schema;
pub mod topics;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use kqr_core::app::cache::ParquetCache;
use kqr_core::app::decode::{JsonDecoder, MessageDecoder};
use kqr_core::app::query::QueryEngine;
use kqr_core::app::table::TableBuilder;
use kqr_core::infra::config::Profile;
use kqr_core::infra::kafka::{
    KafkaSource, OffsetStart, RawMessage, RdkafkaSource, TimeWindow, WindowParse,
};
use tokio::sync::mpsc;
use tracing::warn;

use crate::cli::{OffsetStartArg, WindowArgs};

/// Translate clap [`WindowArgs`] into a resolved [`TimeWindow`].
pub fn resolve_window(args: &WindowArgs) -> anyhow::Result<TimeWindow> {
    let parsed = WindowParse {
        last: args.last.as_deref(),
        since: args.since.as_deref(),
        from: args.from.as_deref(),
        to: args.to.as_deref(),
        offset: args.offset.map(|o| match o {
            OffsetStartArg::Earliest => OffsetStart::Earliest,
            OffsetStartArg::Latest => OffsetStart::Latest,
        }),
        limit: args.limit,
    };
    Ok(parsed.resolve()?)
}

pub fn parse_progress_interval(s: &str) -> anyhow::Result<Duration> {
    humantime::parse_duration(s).with_context(|| format!("invalid --progress-interval '{s}'"))
}

/// Common path for `query` and `repl`: consume a topic, decode, optionally
/// cache, and register as a SQL table on `engine`.
#[allow(clippy::too_many_arguments)]
pub async fn register_topic(
    engine: &QueryEngine,
    profile: &Profile,
    topic: &str,
    sql_name: &str,
    window: &TimeWindow,
    consumer_group_id: Option<&str>,
    schema_sample: usize,
    cache: Option<&ParquetCache>,
) -> anyhow::Result<()> {
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

    let source = RdkafkaSource::new(profile, consumer_group_id)?;
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
    let sample_n = schema_sample.min(payloads.len()).max(1);
    let schema = decoder.infer_schema(&payloads[..sample_n])?;
    let batch = decoder.decode_batch(Arc::clone(&schema), &payloads)?;

    if let Some(c) = cache {
        let key = c.key_path(&profile.brokers, topic, window);
        if let Err(e) = c.write(&key, Arc::clone(&schema), std::slice::from_ref(&batch)) {
            warn!("cache write failed at {}: {e}", key.display());
        }
    }

    let mut builder = TableBuilder::new(schema);
    builder.push(batch);
    engine.register_table(sql_name, builder.build()?)?;
    Ok(())
}
