//! `kqr query` — consume topics, register them as SQL tables, run user SQL.

use std::io::{IsTerminal, Write};
use std::time::Duration;

use kqr_core::app::cache::ParquetCache;
use kqr_core::app::output::{write_batches, OutputFormat};
use kqr_core::app::query::QueryEngine;
use kqr_core::app::table::sql_table_name;
use kqr_core::infra::config::Profile;
use tracing::warn;

use crate::cli::{OutputFormat as CliOutputFormat, QueryArgs};
use crate::commands::{register_topic, resolve_window};

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
            topic,
            &sql_name,
            &window,
            args.consume.consumer_group_id.as_deref(),
            args.consume.schema_sample,
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

fn pick_format(requested: CliOutputFormat) -> OutputFormat {
    match requested {
        CliOutputFormat::Table => {
            if std::io::stdout().is_terminal() {
                OutputFormat::Table
            } else {
                warn!("stdout is not a TTY; --format table → csv");
                OutputFormat::Csv
            }
        }
        CliOutputFormat::Json => OutputFormat::Json,
        CliOutputFormat::Ndjson => OutputFormat::Ndjson,
        CliOutputFormat::Csv => OutputFormat::Csv,
    }
}
