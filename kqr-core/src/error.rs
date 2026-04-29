//! Crate-wide error type. All public APIs in `kqr-core` return `Result<T>`.
//!
//! Variants are added as their subsystem lands; current variants cover step 2
//! (config). Step 3+ will extend this with kafka/arrow/datafusion/parquet
//! variants.

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

    // ---- I/O ------------------------------------------------------------
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Convenience alias for `Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;
