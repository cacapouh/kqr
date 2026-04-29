//! Parquet cache layer for `--reuse`.
//!
//! Layout: `<root>/<broker_hash>/<topic>/<window_hash>.parquet`. Each entry's
//! freshness is decided by file mtime vs the configured TTL — older = stale,
//! re-fetch from Kafka.
//!
//! The cache is intentionally local to one user (default `~/.cache/kqr/`) and
//! never shared across machines.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use sha2::{Digest, Sha256};

use crate::error::Result;
use crate::infra::kafka::TimeWindow;

/// Filesystem-backed Parquet cache.
#[derive(Debug, Clone)]
pub struct ParquetCache {
    root: PathBuf,
    ttl: Duration,
}

impl ParquetCache {
    /// Build a cache rooted at `root`. Creates `root` lazily on first write.
    pub fn new(root: PathBuf, ttl: Duration) -> Self {
        Self { root, ttl }
    }

    /// Default location: `~/.cache/kqr/`. Falls back to `./` if the cache dir
    /// can't be resolved (TempDir setups, weird WSL).
    pub fn default_root() -> PathBuf {
        dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("kqr")
    }

    pub fn key_path(&self, brokers: &str, topic: &str, window: &TimeWindow) -> PathBuf {
        let broker_hash = short_hash(brokers);
        let window_hash = short_hash(&window.cache_token());
        self.root
            .join(broker_hash)
            .join(safe_segment(topic))
            .join(format!("{window_hash}.parquet"))
    }

    /// Returns `Some(batches)` on a fresh hit, `None` on miss or stale.
    pub fn read(&self, key: &Path) -> Result<Option<Vec<RecordBatch>>> {
        if !key.exists() {
            return Ok(None);
        }
        if !self.is_fresh(key)? {
            return Ok(None);
        }
        let file = fs::File::open(key)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        let mut out = Vec::new();
        for batch in reader {
            out.push(batch?);
        }
        Ok(Some(out))
    }

    /// Atomically write `batches` to `key` (write to `.tmp` then rename).
    pub fn write(&self, key: &Path, schema: SchemaRef, batches: &[RecordBatch]) -> Result<()> {
        if let Some(parent) = key.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = key.with_extension("parquet.tmp");
        {
            let file = fs::File::create(&tmp)?;
            let mut writer = ArrowWriter::try_new(file, schema, None)?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.close()?;
        }
        fs::rename(&tmp, key)?;
        Ok(())
    }

    /// Whether the cache file at `key` is younger than the configured TTL.
    pub fn is_fresh(&self, key: &Path) -> Result<bool> {
        let meta = fs::metadata(key)?;
        let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let age = SystemTime::now()
            .duration_since(mtime)
            .unwrap_or(Duration::ZERO);
        Ok(age <= self.ttl)
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }
}

fn short_hash(input: &str) -> String {
    let mut h = Sha256::new();
    h.update(input.as_bytes());
    let digest = h.finalize();
    hex::encode(&digest[..8])
}

/// Sanitise a topic name for use as a directory segment. No path separators,
/// nul bytes, or whitespace; `_` substitution.
fn safe_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push('_');
    }
    out
}

/// Convenience helper: read-or-fetch via a closure.
///
/// Returns the batches and a flag indicating whether the cache was hit.
pub async fn read_or_fetch<F, Fut>(
    cache: &ParquetCache,
    key: &Path,
    schema: SchemaRef,
    fetch: F,
) -> Result<(Vec<RecordBatch>, bool)>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Vec<RecordBatch>>>,
{
    if let Some(batches) = cache.read(key)? {
        return Ok((batches, true));
    }
    let batches = fetch().await?;
    if !batches.is_empty() {
        let _ = std::fs::create_dir_all(key.parent().unwrap_or_else(|| Path::new(".")));
        if let Err(e) = cache.write(key, Arc::clone(&schema), &batches) {
            tracing::warn!("cache write failed at {}: {e}", key.display());
        }
    }
    Ok((batches, false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch(rows: &[i64]) -> RecordBatch {
        let arr = Int64Array::from(rows.to_vec());
        RecordBatch::try_new(schema(), vec![Arc::new(arr)]).unwrap()
    }

    #[test]
    fn key_path_is_stable() {
        let cache = ParquetCache::new(PathBuf::from("/tmp/x"), Duration::from_secs(3600));
        let w = TimeWindow::Last(Duration::from_secs(600));
        let p1 = cache.key_path("h:9092", "orders", &w);
        let p2 = cache.key_path("h:9092", "orders", &w);
        assert_eq!(p1, p2);
    }

    #[test]
    fn key_path_changes_with_brokers() {
        let cache = ParquetCache::new(PathBuf::from("/tmp/x"), Duration::from_secs(3600));
        let w = TimeWindow::Last(Duration::from_secs(600));
        let p1 = cache.key_path("a:9092", "orders", &w);
        let p2 = cache.key_path("b:9092", "orders", &w);
        assert_ne!(p1, p2);
    }

    #[test]
    fn key_path_changes_with_window() {
        let cache = ParquetCache::new(PathBuf::from("/tmp/x"), Duration::from_secs(3600));
        let w1 = TimeWindow::Last(Duration::from_secs(600));
        let w2 = TimeWindow::Last(Duration::from_secs(900));
        let p1 = cache.key_path("h:9092", "orders", &w1);
        let p2 = cache.key_path("h:9092", "orders", &w2);
        assert_ne!(p1, p2);
    }

    #[test]
    fn write_then_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ParquetCache::new(dir.path().to_path_buf(), Duration::from_secs(3600));
        let key = cache.key_path(
            "h:9092",
            "orders",
            &TimeWindow::Last(Duration::from_secs(60)),
        );
        cache.write(&key, schema(), &[batch(&[1, 2, 3])]).unwrap();
        let read = cache.read(&key).unwrap().unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].num_rows(), 3);
    }

    #[test]
    fn read_returns_none_on_miss() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ParquetCache::new(dir.path().to_path_buf(), Duration::from_secs(3600));
        let key = cache.key_path(
            "h:9092",
            "missing",
            &TimeWindow::Last(Duration::from_secs(60)),
        );
        assert!(cache.read(&key).unwrap().is_none());
    }

    #[test]
    fn stale_entries_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ParquetCache::new(dir.path().to_path_buf(), Duration::from_nanos(1));
        let key = cache.key_path("h", "t", &TimeWindow::Last(Duration::from_secs(60)));
        cache.write(&key, schema(), &[batch(&[1])]).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        assert!(cache.read(&key).unwrap().is_none());
    }

    #[test]
    fn safe_segment_replaces_path_chars() {
        assert_eq!(safe_segment("ord/v1"), "ord_v1");
        assert_eq!(safe_segment("ord:test"), "ord_test");
        assert_eq!(safe_segment("user-events"), "user-events");
    }
}
