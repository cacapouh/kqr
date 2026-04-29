//! `kqr topics` — list all user-visible topics on the cluster.

use kqr_core::infra::config::Profile;
use kqr_core::infra::kafka::{KafkaSource, RdkafkaSource};

pub async fn run(profile: &Profile) -> anyhow::Result<()> {
    let source = RdkafkaSource::new(profile, None)?;
    let topics = source.list_topics().await?;
    for t in topics {
        println!("{t}");
    }
    Ok(())
}
