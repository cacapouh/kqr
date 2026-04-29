//! Subcommand handlers. Thin shims that translate clap args into core calls
//! and format results — the layer rule keeps business logic in `kqr-core`.

pub mod sample;
pub mod topics;

use std::time::Duration;

use anyhow::Context;
use kqr_core::infra::kafka::{OffsetStart, TimeWindow, WindowParse};

use crate::cli::{OffsetStartArg, WindowArgs};

/// Translate clap [`WindowArgs`] into a resolved [`TimeWindow`].
pub fn resolve_window(args: &WindowArgs) -> anyhow::Result<TimeWindow> {
    let parsed = WindowParse {
        last: args.last.as_deref(),
        since: args.since.as_deref(),
        from: args.from.as_deref(),
        to: args.to.as_deref(),
        offset: args.offset.map(|o| match o {
            OffsetStartArg::Earliest => OffsetStart::Earliest,
            OffsetStartArg::Latest => OffsetStart::Latest,
        }),
        limit: args.limit,
    };
    Ok(parsed.resolve()?)
}

pub fn parse_progress_interval(s: &str) -> anyhow::Result<Duration> {
    humantime::parse_duration(s).with_context(|| format!("invalid --progress-interval '{s}'"))
}
