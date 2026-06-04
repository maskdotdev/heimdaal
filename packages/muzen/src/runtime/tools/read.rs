use std::sync::atomic::Ordering;
use std::sync::Arc;

use moka::future::Cache;

use crate::runtime::contracts::{stable_id, RuntimeError, RuntimeLimits, RuntimeResult};
use crate::runtime::repo::{FileMeta, RepoSnapshot};

use super::metrics::ConcurrentAtomicCounters;

#[derive(Debug)]
pub(crate) struct ReadService {
    snapshot: Arc<RepoSnapshot>,
    limits: Arc<RuntimeLimits>,
    file_cache: Cache<String, Arc<Vec<u8>>>,
    counters: Arc<ConcurrentAtomicCounters>,
}

impl ReadService {
    pub(super) fn new(
        snapshot: Arc<RepoSnapshot>,
        limits: Arc<RuntimeLimits>,
        counters: Arc<ConcurrentAtomicCounters>,
    ) -> Self {
        Self {
            snapshot,
            limits,
            file_cache: Cache::new(10_000),
            counters,
        }
    }

    pub(super) async fn read_file(&self, file: &FileMeta) -> RuntimeResult<ReadResult> {
        let key = stable_id(&[
            &self.snapshot.snapshot_id.0,
            &file.file_id.0.to_string(),
            &file.fingerprint,
        ]);
        if let Some(bytes) = self.file_cache.get(&key).await {
            self.counters
                .read_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            return decode_read(bytes.as_ref(), false);
        }
        let (bytes, truncated) = self
            .snapshot
            .read_bounded(file.file_id, self.limits.max_file_bytes_read)?;
        self.counters
            .read_file_reads
            .fetch_add(1, Ordering::Relaxed);
        let bytes = Arc::new(bytes);
        self.file_cache.insert(key, Arc::clone(&bytes)).await;
        decode_read(bytes.as_ref(), truncated)
    }
}

#[derive(Debug)]
pub(super) struct ReadResult {
    pub(super) content: String,
    pub(super) truncated: bool,
}

fn decode_read(bytes: &[u8], truncated: bool) -> RuntimeResult<ReadResult> {
    let content = String::from_utf8(bytes.to_vec())
        .map_err(|_| RuntimeError::InvalidInput("file is not valid UTF-8".to_string()))?;
    Ok(ReadResult { content, truncated })
}
