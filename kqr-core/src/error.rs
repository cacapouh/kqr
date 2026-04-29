//! Crate-wide error type. All public APIs in `kqr-core` return `Result<T>`.

use std::path::PathBuf;

/// Top-level error type for `kqr-core`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // ---- config ---------------------------------------------------------
    #[error("config file I/O at {path}: {source}")]
    ConfigIo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("config TOML parse: {0}")]
    ConfigParse(#[from] toml::de::Error),
    #[error("config: profile '{0}' not found")]
    ConfigProfileMissing(String),
    #[error("config: env var '{0}' is referenced but not set")]
    ConfigEnvMissing(String),
    #[error("config: malformed ${{...}} env reference")]
    ConfigEnvSyntax,
    #[error("config: home directory unavailable")]
    ConfigHomeUnavailable,
    #[error("config: invalid duration '{0}': {1}")]
    ConfigDuration(String, humantime::DurationError),

    // ---- time window ----------------------------------------------------
    #[error("time window: invalid duration '{0}': {1}")]
    WindowDuration(String, humantime::DurationError),
    #[error("time window: invalid timestamp '{0}': {1}")]
    WindowTimestamp(String, chrono::ParseError),
    #[error("time window: only one of --last / --since / --from / --offset may be specified")]
    WindowConflict,
    #[error("time window: --to requires --from or --since absolute time")]
    WindowToWithoutFrom,
    #[error("time window: 'to' must be later than 'from'")]
    WindowToBeforeFrom,
    #[error("time window: --offset requires --limit")]
    WindowOffsetWithoutLimit,

    // ---- kafka ----------------------------------------------------------
    #[error("kafka client: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("kafka topic '{0}' not found")]
    KafkaTopicMissing(String),
    #[error("kafka: brokers not configured (set --brokers or profile.brokers)")]
    KafkaBrokersMissing,
    #[error("kafka: time window resolved to empty range (no offsets)")]
    KafkaEmptyWindow,

    // ---- decode ---------------------------------------------------------
    #[error("decode: {0}")]
    Decode(String),
    #[error("decode: schema inference failed (no usable JSON samples)")]
    DecodeSchemaInferEmpty,
    #[error("decode: not valid JSON: {0}")]
    DecodeJson(#[from] serde_json::Error),

    // ---- arrow / datafusion ---------------------------------------------
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("datafusion: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    // ---- query / output -------------------------------------------------
    #[error("query: {0}")]
    Query(String),
    #[error("output: {0}")]
    Output(String),

    // ---- cache / parquet ------------------------------------------------
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    // ---- internal -------------------------------------------------------
    #[error("background task: {0}")]
    TaskJoin(String),

    // ---- I/O ------------------------------------------------------------
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl From<tokio::task::JoinError> for Error {
    fn from(e: tokio::task::JoinError) -> Self {
        Error::TaskJoin(e.to_string())
    }
}

/// Convenience alias for `Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;
