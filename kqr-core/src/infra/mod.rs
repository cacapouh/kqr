//! Infrastructure layer — external I/O.
//!
//! Layer rule (see DESIGN.md §A): every external interaction lives here. The
//! application layer reaches Kafka only via traits exposed in [`kafka`]. An
//! audit reading just `infra/` is therefore complete with respect to network
//! and disk side effects.

pub mod config;
pub mod kafka;
