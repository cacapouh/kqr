//! Top-level subcommand definitions.
//!
//! Argument groups (time-window flags, output format, profile, etc.) and the
//! actual command bodies are filled in by later implementation steps. This
//! module currently only declares the surface area so `kqr --help` exposes the
//! intended UX.

use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run a one-shot SQL query against one or more topics.
    Query,
    /// Start the interactive SQL REPL.
    Repl,
    /// Print the inferred schema for a topic.
    Schema,
    /// Sample raw messages from a topic.
    Sample,
    /// List Kafka topics.
    Topics,
}

impl Command {
    /// Short, kebab-case-ish name for diagnostics.
    pub fn name(&self) -> &'static str {
        match self {
            Command::Query => "query",
            Command::Repl => "repl",
            Command::Schema => "schema",
            Command::Sample => "sample",
            Command::Topics => "topics",
        }
    }
}
