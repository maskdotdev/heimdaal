use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;

use crate::concurrent::contracts::{
    CacheStatus, ConcurrentCounters, ToolId, ToolMetricKey, ToolMetricsSnapshot, ToolResultEnvelope,
};

#[derive(Debug, Default)]
pub(crate) struct ConcurrentAtomicCounters {
    pub(super) search_scans: AtomicUsize,
    pub(super) search_dedupe_waiters: AtomicUsize,
    pub(super) search_cache_hits: AtomicUsize,
    pub(super) read_cache_hits: AtomicUsize,
    pub(super) read_file_reads: AtomicUsize,
    pub(super) tool_errors: AtomicUsize,
    pub(super) artifact_cache_hits: AtomicUsize,
}

impl ConcurrentAtomicCounters {
    pub(super) fn snapshot(&self) -> ConcurrentCounters {
        ConcurrentCounters {
            search_scans: self.search_scans.load(Ordering::Relaxed),
            search_dedupe_waiters: self.search_dedupe_waiters.load(Ordering::Relaxed),
            search_cache_hits: self.search_cache_hits.load(Ordering::Relaxed),
            read_cache_hits: self.read_cache_hits.load(Ordering::Relaxed),
            read_file_reads: self.read_file_reads.load(Ordering::Relaxed),
            tool_errors: self.tool_errors.load(Ordering::Relaxed),
            artifact_cache_hits: self.artifact_cache_hits.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ConcurrentToolMetricsStore {
    by_tool: DashMap<String, Arc<ConcurrentToolMetricCounters>>,
}

impl ConcurrentToolMetricsStore {
    pub(super) fn record(&self, result: &ToolResultEnvelope) {
        let counters = self
            .by_tool
            .entry(result.tool_name.as_str().to_string())
            .or_insert_with(|| Arc::new(ConcurrentToolMetricCounters::default()))
            .clone();
        counters.calls.fetch_add(1, Ordering::Relaxed);
        if result.ok {
            counters.successes.fetch_add(1, Ordering::Relaxed);
        } else {
            counters.errors.fetch_add(1, Ordering::Relaxed);
        }
        if result.cache.status == CacheStatus::Hit {
            counters.cache_hits.fetch_add(1, Ordering::Relaxed);
        }
        if result.cache.status == CacheStatus::Deduped {
            counters.deduped.fetch_add(1, Ordering::Relaxed);
        }
        counters
            .output_bytes
            .fetch_add(result.limits.output_bytes, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> BTreeMap<ToolMetricKey, ToolMetricsSnapshot> {
        let mut snapshot = BTreeMap::new();
        for entry in self.by_tool.iter() {
            let tool_id = ToolId::parse(entry.key())
                .expect("tool metrics keys are recorded from validated tool ids");
            snapshot.insert(tool_id, entry.value().snapshot());
        }
        snapshot
    }
}

#[derive(Debug, Default)]
struct ConcurrentToolMetricCounters {
    calls: AtomicUsize,
    successes: AtomicUsize,
    errors: AtomicUsize,
    cache_hits: AtomicUsize,
    deduped: AtomicUsize,
    output_bytes: AtomicUsize,
}

impl ConcurrentToolMetricCounters {
    fn snapshot(&self) -> ToolMetricsSnapshot {
        ToolMetricsSnapshot {
            calls: self.calls.load(Ordering::Relaxed),
            successes: self.successes.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            deduped: self.deduped.load(Ordering::Relaxed),
            output_bytes: self.output_bytes.load(Ordering::Relaxed),
        }
    }
}
