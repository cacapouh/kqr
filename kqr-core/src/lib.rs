//! Core library for `kqr` — query Kafka topics with SQL.
//!
//! Two layers:
//! - [`app`] — pure logic (decode, table, query, output, cache).
//! - [`infra`] — external I/O. `rdkafka` and config-file I/O live here.
//!
//! The application layer talks to Kafka only through traits exposed by
//! `infra::kafka` so that an audit of `infra/` is sufficient to know what
//! external interactions kqr performs.

pub mod app;
pub mod error;
pub mod infra;

pub use error::{Error, Result};

/// Returns the crate version, useful for `--version` reporting.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_set() {
        assert!(!version().is_empty());
    }
}
