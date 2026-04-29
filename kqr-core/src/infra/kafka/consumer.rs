//! `rdkafka`-backed implementation of [`super::KafkaSource`].
//!
//! This is the **only** module in the crate that imports from `rdkafka`. The
//! shape of [`super::KafkaSource`] is therefore the complete public surface
//! through which application code can reach Kafka.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::error::{Error, Result};
use crate::infra::config::Profile;

use super::window::{OffsetStart, TimeWindow};
use super::{ConsumeStats, KafkaSource, RawMessage};

const META_TIMEOUT: Duration = Duration::from_secs(10);

/// Production [`KafkaSource`] backed by `rdkafka`.
pub struct RdkafkaSource {
    base_config: ClientConfig,
    explicit_group: bool,
}

impl RdkafkaSource {
    /// Build a source from a resolved [`Profile`]. If `group_id` is `Some`,
    /// the consumer joins that group and commits offsets at end-of-consume
    /// (callers are expected to surface a stderr warning to the user).
    /// When `None`, a unique throwaway group id is generated and offsets are
    /// not committed — the no-side-effects default.
    pub fn new(profile: &Profile, group_id: Option<&str>) -> Result<Self> {
        if profile.brokers.trim().is_empty() {
            return Err(Error::KafkaBrokersMissing);
        }
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", profile.brokers.trim());

        let gid = match group_id {
            Some(s) => s.to_string(),
            None => generate_throwaway_group_id(),
        };
        cfg.set("group.id", &gid);
        cfg.set(
            "enable.auto.commit",
            if group_id.is_some() { "true" } else { "false" },
        );
        if group_id.is_none() {
            cfg.set("enable.auto.offset.store", "false");
        }
        cfg.set("auto.offset.reset", "earliest");

        if let (Some(mech), Some(user), Some(pass)) = (
            profile.sasl_mechanism.as_ref(),
            profile.sasl_username.as_ref(),
            profile.sasl_password.as_ref(),
        ) {
            cfg.set("security.protocol", "SASL_PLAINTEXT");
            cfg.set("sasl.mechanism", mech);
            cfg.set("sasl.username", user);
            cfg.set("sasl.password", pass);
        }

        Ok(Self {
            base_config: cfg,
            explicit_group: group_id.is_some(),
        })
    }

    fn make_consumer(&self) -> Result<StreamConsumer> {
        Ok(self.base_config.create()?)
    }
}

fn generate_throwaway_group_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("kqr-{}-{}", std::process::id(), nanos)
}

#[async_trait]
impl KafkaSource for RdkafkaSource {
    async fn list_topics(&self) -> Result<Vec<String>> {
        let consumer = self.make_consumer()?;
        let topics = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let md = consumer.fetch_metadata(None, META_TIMEOUT)?;
            let mut names: Vec<String> = md
                .topics()
                .iter()
                .map(|t| t.name().to_string())
                .filter(|n| !n.starts_with("__"))
                .collect();
            names.sort();
            Ok(names)
        })
        .await??;
        Ok(topics)
    }

    async fn consume(
        &self,
        topic: &str,
        window: &TimeWindow,
        limit: Option<u64>,
        sink: mpsc::Sender<RawMessage>,
    ) -> Result<ConsumeStats> {
        let consumer = Arc::new(self.make_consumer()?);
        let topic_owned = topic.to_string();
        let window_owned = window.clone();

        // Resolve TPL on a blocking thread (offsets_for_times / fetch_watermarks
        // are synchronous in librdkafka).
        let assignments = {
            let consumer = Arc::clone(&consumer);
            let topic = topic_owned.clone();
            tokio::task::spawn_blocking(move || {
                resolve_assignments(&consumer, &topic, &window_owned)
            })
            .await??
        };

        if assignments.is_empty() {
            return Err(Error::KafkaEmptyWindow);
        }

        let tpl = build_tpl(&topic_owned, &assignments)?;
        consumer.assign(&tpl)?;

        let mut remaining: HashMap<i32, i64> = assignments
            .iter()
            .map(|a| (a.partition, a.end_offset - a.start_offset))
            .collect();
        let total_remaining: u64 = remaining.values().map(|r| (*r).max(0) as u64).sum();
        let mut left_to_send: u64 = match limit {
            Some(l) => l.min(total_remaining),
            None => total_remaining,
        };

        debug!(
            ?assignments,
            total = total_remaining,
            limit = left_to_send,
            "starting consume"
        );

        let mut stats = ConsumeStats::default();
        let mut stream = consumer.stream();

        while left_to_send > 0 && remaining.values().any(|r| *r > 0) {
            let msg = match stream.next().await {
                Some(Ok(m)) => m,
                Some(Err(e)) => {
                    if matches!(e, rdkafka::error::KafkaError::PartitionEOF(_)) {
                        continue;
                    }
                    return Err(Error::Kafka(e));
                }
                None => break,
            };
            let partition = msg.partition();
            let offset = msg.offset();

            // Defensive: skip if past planned end (live-tail messages can
            // arrive after we've resolved bounds).
            match remaining.get(&partition) {
                Some(rem) if *rem <= 0 => continue,
                None => continue,
                _ => {}
            }
            let end_off = assignments
                .iter()
                .find(|a| a.partition == partition)
                .map(|a| a.end_offset)
                .unwrap_or(i64::MAX);
            if offset >= end_off {
                if let Some(r) = remaining.get_mut(&partition) {
                    *r = 0;
                }
                continue;
            }

            let key = msg.key().map(|k| k.to_vec());
            let value = msg.payload().map(|p| p.to_vec()).unwrap_or_default();
            let bytes = value.len() as u64 + key.as_ref().map(|k| k.len() as u64).unwrap_or(0);
            let raw = RawMessage {
                partition,
                offset,
                timestamp_ms: msg.timestamp().to_millis(),
                key,
                value,
            };

            if sink.send(raw).await.is_err() {
                debug!("sink closed, ending consume");
                break;
            }
            stats.messages += 1;
            stats.bytes += bytes;
            if let Some(r) = remaining.get_mut(&partition) {
                *r -= 1;
            }
            left_to_send -= 1;
        }

        if self.explicit_group {
            if let Err(e) = consumer.commit_consumer_state(CommitMode::Async) {
                warn!("commit_consumer_state failed: {e}");
            }
        }
        let _ = consumer.unassign();
        Ok(stats)
    }
}

#[derive(Debug, Clone)]
struct Assignment {
    partition: i32,
    start_offset: i64,
    /// Exclusive upper bound. Messages with offset >= end_offset are not consumed.
    end_offset: i64,
}

fn build_tpl(topic: &str, assignments: &[Assignment]) -> Result<TopicPartitionList> {
    let mut tpl = TopicPartitionList::with_capacity(assignments.len());
    for a in assignments {
        tpl.add_partition_offset(topic, a.partition, Offset::Offset(a.start_offset))?;
    }
    Ok(tpl)
}

fn resolve_assignments(
    consumer: &StreamConsumer,
    topic: &str,
    window: &TimeWindow,
) -> Result<Vec<Assignment>> {
    let md = consumer.fetch_metadata(Some(topic), META_TIMEOUT)?;
    let topic_md = md
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| Error::KafkaTopicMissing(topic.to_string()))?;
    if topic_md.partitions().is_empty() {
        return Err(Error::KafkaTopicMissing(topic.to_string()));
    }
    let partitions: Vec<i32> = topic_md.partitions().iter().map(|p| p.id()).collect();

    let mut watermarks: HashMap<i32, (i64, i64)> = HashMap::new();
    for p in &partitions {
        let (low, high) = consumer.fetch_watermarks(topic, *p, META_TIMEOUT)?;
        watermarks.insert(*p, (low, high));
    }

    let assignments = match window {
        TimeWindow::Last(d) => {
            let from_ms = chrono::Utc::now().timestamp_millis() - d.as_millis() as i64;
            resolve_time_based(consumer, topic, &partitions, &watermarks, from_ms, None)?
        }
        TimeWindow::Range { from, to } => {
            let from_ms = from.timestamp_millis();
            let to_ms = to.as_ref().map(|t| t.timestamp_millis());
            resolve_time_based(consumer, topic, &partitions, &watermarks, from_ms, to_ms)?
        }
        TimeWindow::Offset { start, limit } => {
            resolve_offset_based(*start, *limit, &partitions, &watermarks)
        }
    };

    Ok(assignments
        .into_iter()
        .filter(|a| a.end_offset > a.start_offset)
        .collect())
}

fn resolve_time_based(
    consumer: &StreamConsumer,
    topic: &str,
    partitions: &[i32],
    watermarks: &HashMap<i32, (i64, i64)>,
    from_ms: i64,
    to_ms: Option<i64>,
) -> Result<Vec<Assignment>> {
    let from_resolved = lookup_offsets_for_time(consumer, topic, partitions, from_ms)?;
    let to_resolved = match to_ms {
        Some(t) => Some(lookup_offsets_for_time(consumer, topic, partitions, t)?),
        None => None,
    };

    let mut out = Vec::with_capacity(partitions.len());
    for &p in partitions {
        let (low, high) = watermarks.get(&p).copied().unwrap_or((0, 0));
        let from_off = clamp_resolved_offset(&from_resolved, topic, p, low, high);
        let to_off = match &to_resolved {
            Some(r) => clamp_resolved_offset(r, topic, p, low, high),
            None => high,
        };
        out.push(Assignment {
            partition: p,
            start_offset: from_off,
            end_offset: to_off,
        });
    }
    Ok(out)
}

fn lookup_offsets_for_time(
    consumer: &StreamConsumer,
    topic: &str,
    partitions: &[i32],
    timestamp_ms: i64,
) -> Result<TopicPartitionList> {
    let mut tpl = TopicPartitionList::with_capacity(partitions.len());
    for &p in partitions {
        tpl.add_partition_offset(topic, p, Offset::Offset(timestamp_ms))?;
    }
    Ok(consumer.offsets_for_times(tpl, META_TIMEOUT)?)
}

fn clamp_resolved_offset(
    tpl: &TopicPartitionList,
    topic: &str,
    partition: i32,
    low: i64,
    high: i64,
) -> i64 {
    match tpl.find_partition(topic, partition).map(|el| el.offset()) {
        Some(Offset::Offset(n)) if n >= 0 => n.max(low).min(high),
        _ => high,
    }
}

fn resolve_offset_based(
    start: OffsetStart,
    limit: u64,
    partitions: &[i32],
    watermarks: &HashMap<i32, (i64, i64)>,
) -> Vec<Assignment> {
    let n_parts = partitions.len().max(1) as u64;
    let per_part = (limit / n_parts).max(1) as i64;
    partitions
        .iter()
        .map(|&p| {
            let (low, high) = watermarks.get(&p).copied().unwrap_or((0, 0));
            let (s, e) = match start {
                OffsetStart::Earliest => (low, (low + per_part).min(high)),
                OffsetStart::Latest => ((high - per_part).max(low), high),
            };
            Assignment {
                partition: p,
                start_offset: s,
                end_offset: e,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wm(parts: &[(i32, i64, i64)]) -> HashMap<i32, (i64, i64)> {
        parts.iter().map(|(p, l, h)| (*p, (*l, *h))).collect()
    }

    #[test]
    fn offset_earliest_distributes_limit() {
        let parts = vec![0, 1, 2];
        let watermarks = wm(&[(0, 0, 100), (1, 0, 100), (2, 0, 100)]);
        let a = resolve_offset_based(OffsetStart::Earliest, 30, &parts, &watermarks);
        assert_eq!(a.len(), 3);
        for asg in &a {
            assert_eq!(asg.start_offset, 0);
            assert_eq!(asg.end_offset, 10); // 30 / 3 partitions
        }
    }

    #[test]
    fn offset_latest_picks_tail() {
        let parts = vec![0, 1];
        let watermarks = wm(&[(0, 0, 100), (1, 0, 100)]);
        let a = resolve_offset_based(OffsetStart::Latest, 10, &parts, &watermarks);
        for asg in &a {
            assert_eq!(asg.end_offset, 100);
            assert_eq!(asg.start_offset, 95); // 100 - 10/2
        }
    }

    #[test]
    fn offset_clamps_to_low_watermark() {
        let parts = vec![0];
        let watermarks = wm(&[(0, 50, 60)]);
        let a = resolve_offset_based(OffsetStart::Latest, 1000, &parts, &watermarks);
        assert_eq!(a[0].start_offset, 50); // clamped, not negative
        assert_eq!(a[0].end_offset, 60);
    }

    #[test]
    fn empty_window_filters_out_zero_range() {
        let parts = vec![0];
        let watermarks = wm(&[(0, 5, 5)]); // empty partition
        let a = resolve_offset_based(OffsetStart::Earliest, 10, &parts, &watermarks);
        // raw assignments will be (5, 5); the higher-level resolver filters them
        // here we just sanity-check the math
        assert_eq!(a[0].start_offset, 5);
        assert_eq!(a[0].end_offset, 5);
    }

    #[test]
    fn missing_brokers_errors() {
        let p = Profile {
            brokers: "  ".to_string(),
            ..Profile::default()
        };
        match RdkafkaSource::new(&p, None) {
            Err(Error::KafkaBrokersMissing) => {}
            other => panic!("expected KafkaBrokersMissing, got {:?}", other.err()),
        }
    }

    #[test]
    fn throwaway_group_ids_are_unique() {
        let a = generate_throwaway_group_id();
        std::thread::sleep(Duration::from_nanos(1));
        let b = generate_throwaway_group_id();
        assert_ne!(a, b);
    }
}
