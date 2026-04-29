use anyhow::{bail, Context};
use clap::Parser;
use tracing_subscriber::EnvFilter;

mod cli;

use cli::Cli;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    // Resolve the active profile, if any, so we can fail early on bad config.
    let profile = resolve_profile(&cli)?;
    if let Some(p) = &profile {
        tracing::debug!(brokers = %p.brokers, "profile resolved");
    }

    match cli.command {
        Some(cmd) => bail!(
            "`kqr {}` is not implemented yet (will be added in step 3+)",
            cmd.name()
        ),
        None => {
            println!(
                "kqr {} (kqr-core {}) — run `kqr --help`",
                env!("CARGO_PKG_VERSION"),
                kqr_core::version()
            );
            Ok(())
        }
    }
}

fn init_tracing(verbose: u8) {
    let level = match verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("kqr={level},kqr_core={level}")));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

/// Returns `Some(Profile)` if a config exists or brokers/--profile is set,
/// else `None` (commands that don't touch Kafka can run without a profile).
fn resolve_profile(cli: &Cli) -> anyhow::Result<Option<kqr_core::infra::config::Profile>> {
    use kqr_core::infra::config::{Config, Profile};

    let cfg = match &cli.config {
        Some(path) => Some(
            Config::load_from(path)
                .with_context(|| format!("loading config from {}", path.display()))?,
        ),
        None => match Config::load_default() {
            Ok(c) if !c.profiles.is_empty() => Some(c),
            Ok(_) => None,
            Err(e) => {
                tracing::warn!("could not load default config: {e}");
                None
            }
        },
    };

    // If --brokers was given without a profile, synthesize an inline one so
    // every code path downstream sees a Profile.
    if let Some(brokers) = &cli.brokers {
        return Ok(Some(Profile {
            brokers: brokers.clone(),
            ..Profile::default()
        }));
    }

    let Some(cfg) = cfg else {
        return Ok(None);
    };
    let profile = cfg.select_profile(cli.profile.as_deref())?;
    Ok(Some(profile))
}
