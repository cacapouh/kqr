use anyhow::{bail, Context};
use clap::Parser;
use tracing_subscriber::EnvFilter;

mod cli;
mod commands;
mod progress;

use cli::{Cli, Command};
use kqr_core::infra::config::Profile;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    let profile = resolve_profile(&cli)?;
    let exit = match cli.command.clone() {
        Some(Command::Sample(args)) => {
            commands::sample::run(args, &require_profile(profile, "sample")?).await
        }
        Some(Command::Schema(args)) => {
            commands::schema::run(args, &require_profile(profile, "schema")?).await
        }
        Some(Command::Topics) => commands::topics::run(&require_profile(profile, "topics")?).await,
        Some(Command::Query(_)) | Some(Command::Repl(_)) => {
            bail!("subcommand not implemented yet (will land in step 5-7)")
        }
        None => {
            println!(
                "kqr {} (kqr-core {}) — run `kqr --help`",
                env!("CARGO_PKG_VERSION"),
                kqr_core::version()
            );
            return Ok(());
        }
    };
    exit
}

fn require_profile(p: Option<Profile>, cmd: &str) -> anyhow::Result<Profile> {
    p.ok_or_else(|| {
        anyhow::anyhow!(
            "`kqr {cmd}` needs Kafka brokers — pass --brokers or set up a profile in ~/.config/kqr/config.toml"
        )
    })
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

fn resolve_profile(cli: &Cli) -> anyhow::Result<Option<Profile>> {
    use kqr_core::infra::config::Config;

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

    if let Some(brokers) = &cli.brokers {
        // CLI override beats profile entirely.
        let mut base = match (&cfg, &cli.profile) {
            (Some(c), name) => c.select_profile(name.as_deref()).unwrap_or_default(),
            _ => Profile::default(),
        };
        base.brokers = brokers.clone();
        return Ok(Some(base));
    }

    let Some(cfg) = cfg else {
        return Ok(None);
    };
    let profile = cfg.select_profile(cli.profile.as_deref())?;
    Ok(Some(profile))
}
