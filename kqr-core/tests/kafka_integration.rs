//! End-to-end integration test against a real Kafka broker booted via
//! testcontainers. Validates the full pipeline:
//!
//!     produce JSON → RdkafkaSource::consume → JsonDecoder → MemTable
//!     → QueryEngine::execute → assertions
//!
//! Marked `#[ignore]` so `cargo test` skips this in normal dev loops; run
//! with `cargo test --test kafka_integration -- --ignored` (see also
//! `scripts/check.sh --integration`).

use std::sync::Arc;
use std::time::Duration;

use kqr_core::app::decode::{JsonDecoder, MessageDecoder};
use kqr_core::app::query::QueryEngine;
use kqr_core::app::table::TableBuilder;
use kqr_core::infra::config::Profile;
use kqr_core::infra::kafka::{KafkaSource, RawMessage, RdkafkaSource, TimeWindow};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};
use tokio::sync::mpsc;

#[tokio::test]
#[ignore = "requires docker; run via scripts/check.sh --integration"]
async fn end_to_end_query_against_real_kafka() {
    let kafka = Kafka::default()
        .start()
        .await
        .expect("start kafka container");

    let port = kafka
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("kafka port");
    let brokers = format!("127.0.0.1:{port}");

    let topic = "kqr_it_orders";
    produce_json(&brokers, topic).await;

    let profile = Profile {
        brokers: brokers.clone(),
        ..Profile::default()
    };
    let source = RdkafkaSource::new(&profile, None).expect("source");

    let window = TimeWindow::Range {
        from: chrono::Utc::now() - chrono::Duration::hours(1),
        to: None,
    };
    let (tx, mut rx) = mpsc::channel::<RawMessage>(256);
    let consume = tokio::spawn(async move { source.consume(topic, &window, None, tx).await });

    let mut payloads: Vec<Vec<u8>> = Vec::new();
    while let Some(m) = rx.recv().await {
        payloads.push(m.value);
    }
    let stats = consume.await.unwrap().expect("consume succeeds");
    assert_eq!(
        stats.messages, 6,
        "produced 6 records; consumed {}",
        stats.messages
    );

    let decoder = JsonDecoder::new();
    let schema = decoder.infer_schema(&payloads).expect("infer");
    let batch = decoder
        .decode_batch(Arc::clone(&schema), &payloads)
        .expect("decode");

    let engine = QueryEngine::new();
    let mut builder = TableBuilder::new(schema);
    builder.push(batch);
    engine
        .register_table("orders", builder.build().expect("memtable"))
        .expect("register");

    // count(*)
    let batches = engine
        .execute("select count(*) as n from orders")
        .await
        .expect("count");
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 6);

    // group by side
    let batches = engine
        .execute(
            "select side, count(*) as n, sum(price) as total \
             from orders group by side order by side",
        )
        .await
        .expect("group by");
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(row_count, 2, "expected 'buy' and 'sell' rows");

    let side = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let n = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let total = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();

    assert_eq!(side.value(0), "buy");
    assert_eq!(side.value(1), "sell");
    assert_eq!(n.value(0), 3);
    assert_eq!(n.value(1), 3);
    // 1.0 + 3.0 + 5.0 = 9.0 / 2.0 + 4.0 + 6.0 = 12.0
    assert!((total.value(0) - 9.0).abs() < 1e-6);
    assert!((total.value(1) - 12.0).abs() < 1e-6);
}

async fn produce_json(brokers: &str, topic: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");

    // 3 buys (id=1,3,5) and 3 sells (id=2,4,6) so group-by has both sides.
    let messages = [
        r#"{"id":1,"price":1.0,"side":"buy"}"#,
        r#"{"id":2,"price":2.0,"side":"sell"}"#,
        r#"{"id":3,"price":3.0,"side":"buy"}"#,
        r#"{"id":4,"price":4.0,"side":"sell"}"#,
        r#"{"id":5,"price":5.0,"side":"buy"}"#,
        r#"{"id":6,"price":6.0,"side":"sell"}"#,
    ];

    for (i, m) in messages.iter().enumerate() {
        let key = format!("{i}");
        let record = FutureRecord::to(topic).payload(*m).key(&key);
        producer
            .send(record, Duration::from_secs(10))
            .await
            .expect("produce");
    }
}
