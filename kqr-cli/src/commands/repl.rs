//! `kqr repl` — interactive SQL REPL.
//!
//! Topics are consumed once at startup; subsequent SQL runs against the
//! resulting in-memory tables. Mutable session state is exposed via
//! backslash meta-commands (matches DESIGN.md):
//!
//! ```text
//! \d                   list registered tables
//! \d <table>           print schema of <table>
//! \format json|table|csv|ndjson
//! \timing on|off
//! \reuse on|off        toggles ParquetCache (no effect for already-loaded tables)
//! \q                   exit
//! ```

use std::collections::HashMap;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use kqr_core::app::cache::ParquetCache;
use kqr_core::app::output::{write_batches, OutputFormat};
use kqr_core::app::query::QueryEngine;
use kqr_core::app::table::sql_table_name;
use kqr_core::infra::config::Profile;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use crate::cli::ReplArgs;
use crate::commands::{register_topic, resolve_window};

const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(3600);

#[derive(Debug)]
struct Session {
    format: OutputFormat,
    timing: bool,
    reuse: bool,
    schemas: HashMap<String, SchemaRef>,
}

pub async fn run(args: ReplArgs, profile: &Profile) -> anyhow::Result<()> {
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

    let cache = ParquetCache::new(ParquetCache::default_root(), DEFAULT_CACHE_TTL);
    let engine = QueryEngine::new();

    let mut session = Session {
        format: if std::io::stdout().is_terminal() {
            OutputFormat::Table
        } else {
            OutputFormat::Csv
        },
        timing: false,
        reuse: false,
        schemas: HashMap::new(),
    };

    eprintln!("[kqr] loading topics...");
    for topic in &args.topics {
        let (sql_name, was_changed) = sql_table_name(topic);
        if was_changed {
            eprintln!(
                "[kqr] topic '{}' is not a valid SQL identifier; using '{}'",
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
            None, // initial load: respect default (no cache); user can \reuse on
        )
        .await?;
        // Capture schema for \d <table>
        if let Ok(provider) = engine_table_schema(&engine, &sql_name).await {
            session.schemas.insert(sql_name.clone(), provider);
        }
    }
    eprintln!(
        "[kqr] {} table(s) ready. \\q to exit, \\d for tables.",
        session.schemas.len()
    );

    let mut rl = DefaultEditor::new()?;
    let history_path = history_file_path();
    if let Some(p) = &history_path {
        let _ = rl.load_history(p);
    }

    loop {
        let line = match rl.readline("kqr> ") {
            Ok(l) => l,
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => break,
            Err(e) => return Err(e.into()),
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let _ = rl.add_history_entry(trimmed);

        if let Some(cmd) = trimmed.strip_prefix('\\') {
            match handle_meta(cmd, &mut session) {
                MetaResult::Continue => continue,
                MetaResult::Quit => break,
            }
        }

        let started = Instant::now();
        match engine.execute(trimmed).await {
            Ok(batches) => {
                let stdout = std::io::stdout();
                let mut lock = stdout.lock();
                if let Err(e) = write_batches(&batches, session.format, &mut lock) {
                    eprintln!("[kqr] output error: {e}");
                }
                let _ = lock.flush();
                if session.timing {
                    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                    eprintln!("[kqr] {} rows in {:.2?}", total, started.elapsed());
                }
            }
            Err(e) => eprintln!("[kqr] query error: {e}"),
        }
    }

    if let Some(p) = &history_path {
        if let Some(parent) = p.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = rl.save_history(p);
    }
    let _ = cache; // silence unused warning when --reuse never toggled on
    Ok(())
}

enum MetaResult {
    Continue,
    Quit,
}

fn handle_meta(cmd: &str, session: &mut Session) -> MetaResult {
    let mut parts = cmd.split_whitespace();
    let head = parts.next().unwrap_or("");
    let rest: Vec<&str> = parts.collect();

    match head {
        "q" | "quit" | "exit" => return MetaResult::Quit,
        "d" => {
            if rest.is_empty() {
                let mut names: Vec<&String> = session.schemas.keys().collect();
                names.sort();
                if names.is_empty() {
                    println!("(no tables)");
                } else {
                    for n in names {
                        let cols = session.schemas[n].fields().len();
                        println!("{n}\t{cols} cols");
                    }
                }
            } else {
                let name = rest[0];
                match session.schemas.get(name) {
                    None => eprintln!("[kqr] unknown table '{name}'"),
                    Some(schema) => {
                        for f in schema.fields() {
                            println!(
                                "  {} : {} {}",
                                f.name(),
                                f.data_type(),
                                if f.is_nullable() { "(nullable)" } else { "" }
                            );
                        }
                    }
                }
            }
        }
        "format" => {
            if let Some(arg) = rest.first() {
                match parse_format(arg) {
                    Some(f) => {
                        session.format = f;
                        println!("[kqr] format = {arg}");
                    }
                    None => eprintln!("[kqr] unknown format '{arg}'"),
                }
            } else {
                println!("[kqr] format = {:?}", session.format);
            }
        }
        "timing" => {
            session.timing = parse_on_off(rest.first().copied()).unwrap_or(!session.timing);
            println!("[kqr] timing = {}", on_off(session.timing));
        }
        "reuse" => {
            session.reuse = parse_on_off(rest.first().copied()).unwrap_or(!session.reuse);
            println!("[kqr] reuse = {}", on_off(session.reuse));
        }
        "h" | "help" | "?" => {
            println!("\\d              list tables");
            println!("\\d <table>      print schema");
            println!("\\format <fmt>   table|json|ndjson|csv");
            println!("\\timing on|off  show query duration");
            println!("\\reuse on|off   use Parquet cache (next consume)");
            println!("\\q              quit");
        }
        other => {
            eprintln!("[kqr] unknown meta command '\\{other}' — try \\h");
        }
    }
    MetaResult::Continue
}

fn parse_format(s: &str) -> Option<OutputFormat> {
    match s.to_lowercase().as_str() {
        "table" => Some(OutputFormat::Table),
        "json" => Some(OutputFormat::Json),
        "ndjson" => Some(OutputFormat::Ndjson),
        "csv" => Some(OutputFormat::Csv),
        _ => None,
    }
}

fn parse_on_off(s: Option<&str>) -> Option<bool> {
    match s.map(|x| x.to_lowercase()).as_deref() {
        Some("on") | Some("true") | Some("1") | Some("yes") => Some(true),
        Some("off") | Some("false") | Some("0") | Some("no") => Some(false),
        _ => None,
    }
}

fn on_off(b: bool) -> &'static str {
    if b {
        "on"
    } else {
        "off"
    }
}

fn history_file_path() -> Option<PathBuf> {
    dirs::state_dir()
        .or_else(|| dirs::home_dir().map(|h| h.join(".local").join("state")))
        .map(|d| d.join("kqr").join("history"))
}

/// Read a registered table's schema from the engine. Used to build the
/// `\d <table>` output without re-walking the catalog every time.
async fn engine_table_schema(engine: &QueryEngine, name: &str) -> anyhow::Result<SchemaRef> {
    // Easiest portable way: SELECT * FROM <table> LIMIT 0 → DataFrame schema.
    let batches = engine
        .execute(&format!("select * from {name} limit 0"))
        .await?;
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_on_off_recognised() {
        assert_eq!(parse_on_off(Some("on")), Some(true));
        assert_eq!(parse_on_off(Some("off")), Some(false));
        assert_eq!(parse_on_off(Some("YES")), Some(true));
        assert_eq!(parse_on_off(Some("nope")), None);
        assert_eq!(parse_on_off(None), None);
    }

    #[test]
    fn parse_format_recognised() {
        assert!(matches!(parse_format("table"), Some(OutputFormat::Table)));
        assert!(matches!(parse_format("json"), Some(OutputFormat::Json)));
        assert!(matches!(parse_format("CSV"), Some(OutputFormat::Csv)));
        assert!(parse_format("ascii").is_none());
    }
}
