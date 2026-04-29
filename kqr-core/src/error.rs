//! Crate-wide error type.
//!
//! Concrete variants are added as each subsystem (config, kafka, decode, …)
//! lands in later implementation steps.

/// Top-level error returned by `kqr-core` APIs.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Placeholder variant kept until real variants are introduced.
    /// Removed once the first real subsystem lands.
    #[error("kqr-core not yet implemented: {0}")]
    NotImplemented(&'static str),
}

/// Convenience alias for `Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;
