//! Kafka adapter — the **only** module in the workspace allowed to import
//! `rdkafka`. Application code reaches Kafka through the [`KafkaSource`]
//! trait so that this directory is the complete audit surface for what
//! kqr does to a Kafka cluster.

pub mod consumer;
pub mod window;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::error::Result;

pub use consumer::RdkafkaSource;
pub use window::{OffsetStart, TimeWindow, WindowParse};

/// One raw Kafka record handed to the application layer.
#[derive(Debug, Clone)]
pub struct RawMessage {
    pub partition: i32,
    pub offset: i64,
    /// Producer-set or broker-set timestamp (ms since epoch), if any.
    pub timestamp_ms: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
}

/// Aggregate counters returned at the end of a [`KafkaSource::consume`] run.
#[derive(Debug, Default, Clone, Copy)]
pub struct ConsumeStats {
    pub messages: u64,
    pub bytes: u64,
}

/// Port: anything that can list topics and produce raw bytes from a topic
/// over a bounded time window.
///
/// The only production impl is [`RdkafkaSource`]. Tests can stub this with a
/// `Vec<RawMessage>`-driven mock without pulling rdkafka into the test crate.
#[async_trait]
pub trait KafkaSource: Send + Sync {
    /// Names of all user topics on the cluster (sorted, internals filtered).
    async fn list_topics(&self) -> Result<Vec<String>>;

    /// Consume `topic` over `window`. Each message is sent to `sink`. Returns
    /// when:
    /// - every assigned partition has reached its resolved end offset, or
    /// - `limit` is `Some(n)` and `n` messages have been sent.
    ///
    /// `sink`'s capacity provides backpressure.
    async fn consume(
        &self,
        topic: &str,
        window: &TimeWindow,
        limit: Option<u64>,
        sink: mpsc::Sender<RawMessage>,
    ) -> Result<ConsumeStats>;
}
