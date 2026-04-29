//! `kqr sample` — peek at raw messages from a topic.
//!
//! Output is ndjson on stdout, one record per line:
//! `{"partition":0,"offset":42,"timestamp":"...","key":"...","value":"..."}`.
//!
//! Binary keys/values that are not valid UTF-8 are emitted as base64-ish hex
//! under a `"value_hex"` field instead.

use std::time::Instant;

use kqr_core::infra::config::Profile;
use kqr_core::infra::kafka::{KafkaSource, RawMessage, RdkafkaSource};
use serde_json::json;
use tokio::sync::mpsc;

use crate::cli::SampleArgs;
use crate::commands::{parse_progress_interval, resolve_window};
use crate::progress;

pub async fn run(args: SampleArgs, profile: &Profile) -> anyhow::Result<()> {
    let window = resolve_window(&args.window)?;

    // For `sample`, --n is the user-facing cap. Honor it via consume's limit.
    let limit = Some(args.n);

    let source = RdkafkaSource::new(profile, args.consume.consumer_group_id.as_deref())?;
    if args.consume.consumer_group_id.is_some() {
        eprintln!(
            "[kqr] --consumer-group-id set: offsets will be committed to group '{}'",
            args.consume.consumer_group_id.as_deref().unwrap()
        );
    }

    let (tx, mut rx) = mpsc::channel::<RawMessage>(256);

    let interval = parse_progress_interval("5s")?;
    let mut reporter = progress::build("sample", false, interval); // sample is bounded; no progress by default

    let started = Instant::now();
    let topic = args.topic.clone();
    let consume_task =
        tokio::spawn(async move { source.consume(&topic, &window, limit, tx).await });

    let mut total_msgs = 0u64;
    let mut total_bytes = 0u64;
    while let Some(msg) = rx.recv().await {
        emit_ndjson(&msg)?;
        total_msgs += 1;
        total_bytes +=
            msg.value.len() as u64 + msg.key.as_ref().map(|k| k.len() as u64).unwrap_or(0);
        reporter.record(total_msgs, total_bytes);
    }

    let stats = consume_task.await??;
    reporter.finish(stats.messages, stats.bytes, started.elapsed());
    Ok(())
}

fn emit_ndjson(msg: &RawMessage) -> anyhow::Result<()> {
    let key_field = match msg.key.as_ref() {
        None => json!(null),
        Some(k) => match std::str::from_utf8(k) {
            Ok(s) => json!(s),
            Err(_) => json!({ "hex": hex_encode(k) }),
        },
    };
    let value_field = match std::str::from_utf8(&msg.value) {
        Ok(s) => match serde_json::from_str::<serde_json::Value>(s) {
            Ok(v) => v,
            Err(_) => json!(s),
        },
        Err(_) => json!({ "hex": hex_encode(&msg.value) }),
    };
    let timestamp = msg.timestamp_ms.and_then(|ms| {
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms).map(|d| d.to_rfc3339())
    });
    let envelope = json!({
        "partition": msg.partition,
        "offset": msg.offset,
        "timestamp": timestamp,
        "key": key_field,
        "value": value_field,
    });
    println!("{}", envelope);
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}
