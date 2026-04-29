//! Top-level CLI surface (clap derive).
//!
//! Common flags (`--profile`, `--config`, `--brokers`, `-v`) are global so
//! every subcommand sees them. Subcommand-specific args live in [`Command`].

use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "kqr",
    version,
    about = "Query Kafka topics with SQL",
    long_about = "kqr — query Kafka topics with SQL. Bounded ad-hoc queries \
                  powered by Apache Arrow + DataFusion."
)]
pub struct Cli {
    /// Connection profile from ~/.config/kqr/config.toml.
    #[arg(long, global = true, env = "KQR_PROFILE")]
    pub profile: Option<String>,

    /// Override config file path. Default: ~/.config/kqr/config.toml.
    #[arg(long, global = true, env = "KQR_CONFIG", value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Comma-separated bootstrap brokers (overrides profile.brokers).
    #[arg(long, global = true, env = "KQR_BROKERS")]
    pub brokers: Option<String>,

    /// Verbose logs (-v = INFO, -vv = DEBUG, -vvv = TRACE).
    #[arg(short, long, global = true, action = ArgAction::Count)]
    pub verbose: u8,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// Run a one-shot SQL query against one or more topics.
    Query(QueryArgs),
    /// Start the interactive SQL REPL.
    Repl(ReplArgs),
    /// Print the inferred schema for a topic.
    Schema(SchemaArgs),
    /// Sample raw messages from a topic.
    Sample(SampleArgs),
    /// List Kafka topics.
    Topics,
}

/// Time-window flags shared by `query` / `repl` / `schema` / `sample`.
///
/// `--last`, `--since`, `--from`, `--offset` are mutually exclusive
/// (clap arg group `window`). `--to` may accompany `--from` / `--since`.
#[derive(Args, Debug, Clone, Default)]
pub struct WindowArgs {
    /// Read messages from the last DURATION (e.g. "10m", "2h").
    #[arg(long, value_name = "DURATION", group = "window_lower")]
    pub last: Option<String>,

    /// Read messages since DURATION-ago or absolute RFC3339 time.
    #[arg(long, value_name = "DUR_OR_TIME", group = "window_lower")]
    pub since: Option<String>,

    /// Absolute lower bound (RFC3339, e.g. "2026-01-02T03:04:05Z").
    #[arg(long, value_name = "TIME", group = "window_lower")]
    pub from: Option<String>,

    /// Absolute upper bound (RFC3339). Pairs with --from / --since.
    #[arg(long, value_name = "TIME")]
    pub to: Option<String>,

    /// Read from earliest or latest. Requires --limit.
    #[arg(long, value_enum, group = "window_lower")]
    pub offset: Option<OffsetStartArg>,

    /// Hard cap on messages consumed.
    #[arg(long, value_name = "N")]
    pub limit: Option<u64>,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum OffsetStartArg {
    Earliest,
    Latest,
}

/// Output format flags shared by `query` and `sample`.
#[derive(Args, Debug, Clone, Default)]
pub struct FormatArgs {
    /// Output format. Falls back from `table` to `csv` when stdout is not a TTY.
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(ValueEnum, Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OutputFormat {
    #[default]
    Table,
    Json,
    Ndjson,
    Csv,
}

/// Progress-output flags shared by long-running commands.
#[derive(Args, Debug, Clone, Default)]
pub struct ProgressArgs {
    /// Show consumption progress (TTY: spinner; non-TTY: periodic stderr lines).
    #[arg(long, default_value_t = false)]
    pub progress: bool,

    /// Minimum interval between progress lines on non-TTY output.
    #[arg(long, value_name = "DURATION", default_value = "5s")]
    pub progress_interval: String,
}

/// Shared kafka-consumption knobs.
#[derive(Args, Debug, Clone, Default)]
pub struct ConsumeArgs {
    /// Opt-in: join this consumer group (commits offsets).
    /// Default: ad-hoc assign without a group, no offset commit.
    #[arg(long, value_name = "ID")]
    pub consumer_group_id: Option<String>,

    /// Limit decoder schema-inference sample to first N messages.
    #[arg(long, value_name = "N", default_value_t = 1000)]
    pub schema_sample: usize,
}

#[derive(Args, Debug, Clone)]
pub struct QueryArgs {
    /// Kafka topic(s). May repeat: `-t bids -t wins` enables JOIN.
    #[arg(short, long = "topic", value_name = "TOPIC", required = true, num_args = 1..)]
    pub topics: Vec<String>,

    /// SQL to execute. Topic name is the table name.
    #[arg(value_name = "SQL")]
    pub sql: String,

    /// Print DataFusion logical/physical plan instead of executing.
    #[arg(long)]
    pub explain: bool,

    /// Re-use cached RecordBatches if present (Parquet under ~/.cache/kqr).
    #[arg(long, conflicts_with = "no_reuse")]
    pub reuse: bool,

    #[arg(long)]
    pub no_reuse: bool,

    #[command(flatten)]
    pub window: WindowArgs,

    #[command(flatten)]
    pub format: FormatArgs,

    #[command(flatten)]
    pub progress: ProgressArgs,

    #[command(flatten)]
    pub consume: ConsumeArgs,
}

#[derive(Args, Debug, Clone)]
pub struct ReplArgs {
    #[arg(short, long = "topic", value_name = "TOPIC", required = true, num_args = 1..)]
    pub topics: Vec<String>,

    #[command(flatten)]
    pub window: WindowArgs,

    #[command(flatten)]
    pub progress: ProgressArgs,

    #[command(flatten)]
    pub consume: ConsumeArgs,
}

#[derive(Args, Debug, Clone)]
pub struct SchemaArgs {
    #[arg(short, long = "topic", value_name = "TOPIC", required = true)]
    pub topic: String,

    #[command(flatten)]
    pub window: WindowArgs,

    #[command(flatten)]
    pub consume: ConsumeArgs,
}

#[derive(Args, Debug, Clone)]
pub struct SampleArgs {
    #[arg(short, long = "topic", value_name = "TOPIC", required = true)]
    pub topic: String,

    /// Print at most N messages.
    #[arg(short = 'n', long, value_name = "N", default_value_t = 10)]
    pub n: u64,

    #[command(flatten)]
    pub window: WindowArgs,

    #[command(flatten)]
    pub consume: ConsumeArgs,
}
