use anyhow::bail;
use clap::Parser;

mod cli;

use cli::Command;

#[derive(Debug, Parser)]
#[command(
    name = "kqr",
    version,
    about = "Query Kafka topics with SQL",
    long_about = "kqr — query Kafka topics with SQL. \
                  Bounded ad-hoc queries powered by Apache Arrow + DataFusion."
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Some(cmd) => bail!("`kqr {}` is not implemented yet (scaffold)", cmd.name()),
        None => {
            println!(
                "kqr {} (kqr-core {}) — scaffold; run `kqr --help`",
                env!("CARGO_PKG_VERSION"),
                kqr_core::version()
            );
            Ok(())
        }
    }
}
