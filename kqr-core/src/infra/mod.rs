//! Infrastructure layer — external I/O.
//!
//! - [`config`]: reads `~/.config/kqr/config.toml` from disk and resolves
//!   `${ENV}` placeholders.
//! - `kafka` (added in step 3): the only place in the workspace that imports
//!   from `rdkafka`. Application code calls [`kafka::KafkaSource`] traits.

pub mod config;
