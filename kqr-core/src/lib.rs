//! Core library for `kqr` — query Kafka topics with SQL.
//!
//! Modules will be added in subsequent implementation steps:
//! `config`, `kafka`, `decode`, `table`, `cache`, `query`, `output`, `error`.

pub mod error;

pub use error::Error;

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
