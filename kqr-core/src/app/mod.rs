//! Application layer — pure logic with no external I/O.
//!
//! Modules here only call into [`crate::infra`] through traits, never into
//! `rdkafka`, the filesystem, or the network directly. This makes the infra
//! layer the single audit surface for external interactions.
