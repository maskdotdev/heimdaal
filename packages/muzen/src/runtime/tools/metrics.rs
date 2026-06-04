use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;

use crate::runtime::contracts::{
    CacheStatus, ConcurrentCounters, ToolErrorCode, ToolMetricKey, ToolMetricsSnapshot,
    ToolProviderHealthSnapshot, ToolProviderHealthState, ToolProviderId, ToolResultEnvelope,
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
    by_provider: DashMap<String, Arc<ConcurrentProviderHealthCounters>>,
}

impl ConcurrentToolMetricsStore {
    pub(super) fn record(&self, result: &ToolResultEnvelope) {
        let counters = self
            .by_tool
            .entry(
                ToolMetricKey::new(&result.provider_id, &result.tool_name)
                    .as_str()
                    .to_string(),
            )
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
        if let Some(error) = &result.error {
            match error.code {
                ToolErrorCode::Timeout => {
                    counters.timeouts.fetch_add(1, Ordering::Relaxed);
                }
                ToolErrorCode::Cancelled => {
                    counters.cancellations.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
        }
        if result.artifact_id.is_some() {
            counters.artifacts.fetch_add(1, Ordering::Relaxed);
        }
        counters
            .input_bytes
            .fetch_add(result.limits.input_bytes, Ordering::Relaxed);
        counters
            .output_bytes
            .fetch_add(result.limits.output_bytes, Ordering::Relaxed);
        counters
            .latency_ms
            .fetch_add(result.limits.latency_ms as usize, Ordering::Relaxed);
        record_max(&counters.max_latency_ms, result.limits.latency_ms as usize);
        counters
            .queue_wait_ms
            .fetch_add(result.limits.queue_wait_ms as usize, Ordering::Relaxed);
        record_max(
            &counters.max_queue_wait_ms,
            result.limits.queue_wait_ms as usize,
        );

        let provider = self
            .by_provider
            .entry(result.provider_id.as_str().to_string())
            .or_insert_with(|| Arc::new(ConcurrentProviderHealthCounters::default()))
            .clone();
        provider.record(result);
    }

    pub(super) fn snapshot(&self) -> BTreeMap<ToolMetricKey, ToolMetricsSnapshot> {
        let mut snapshot = BTreeMap::new();
        for entry in self.by_tool.iter() {
            snapshot.insert(
                ToolMetricKey::from_encoded(entry.key().clone()),
                entry.value().snapshot(),
            );
        }
        snapshot
    }

    pub(super) fn provider_health_snapshot(&self) -> Vec<ToolProviderHealthSnapshot> {
        let mut snapshot = self
            .by_provider
            .iter()
            .map(|entry| entry.value().snapshot(entry.key()))
            .collect::<Vec<_>>();
        snapshot.sort_by(|left, right| left.provider_id.cmp(&right.provider_id));
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
    timeouts: AtomicUsize,
    cancellations: AtomicUsize,
    artifacts: AtomicUsize,
    input_bytes: AtomicUsize,
    output_bytes: AtomicUsize,
    latency_ms: AtomicUsize,
    max_latency_ms: AtomicUsize,
    queue_wait_ms: AtomicUsize,
    max_queue_wait_ms: AtomicUsize,
}

impl ConcurrentToolMetricCounters {
    fn snapshot(&self) -> ToolMetricsSnapshot {
        ToolMetricsSnapshot {
            calls: self.calls.load(Ordering::Relaxed),
            successes: self.successes.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            deduped: self.deduped.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            cancellations: self.cancellations.load(Ordering::Relaxed),
            artifacts: self.artifacts.load(Ordering::Relaxed),
            input_bytes: self.input_bytes.load(Ordering::Relaxed),
            output_bytes: self.output_bytes.load(Ordering::Relaxed),
            latency_ms: self.latency_ms.load(Ordering::Relaxed) as u64,
            max_latency_ms: self.max_latency_ms.load(Ordering::Relaxed) as u64,
            queue_wait_ms: self.queue_wait_ms.load(Ordering::Relaxed) as u64,
            max_queue_wait_ms: self.max_queue_wait_ms.load(Ordering::Relaxed) as u64,
        }
    }
}

#[derive(Debug, Default)]
struct ConcurrentProviderHealthCounters {
    calls: AtomicUsize,
    errors: AtomicUsize,
    timeouts: AtomicUsize,
    cancellations: AtomicUsize,
    consecutive_errors: AtomicUsize,
}

impl ConcurrentProviderHealthCounters {
    fn record(&self, result: &ToolResultEnvelope) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if result.ok {
            self.consecutive_errors.store(0, Ordering::Relaxed);
            return;
        }

        self.errors.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.fetch_add(1, Ordering::Relaxed);
        if let Some(error) = &result.error {
            match error.code {
                ToolErrorCode::Timeout => {
                    self.timeouts.fetch_add(1, Ordering::Relaxed);
                }
                ToolErrorCode::Cancelled => {
                    self.cancellations.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
        }
    }

    fn snapshot(&self, provider_id: &str) -> ToolProviderHealthSnapshot {
        let errors = self.errors.load(Ordering::Relaxed);
        let consecutive_errors = self.consecutive_errors.load(Ordering::Relaxed);
        let state = if consecutive_errors >= 3 {
            ToolProviderHealthState::Unhealthy
        } else if errors > 0 {
            ToolProviderHealthState::Degraded
        } else {
            ToolProviderHealthState::Healthy
        };
        ToolProviderHealthSnapshot {
            provider_id: ToolProviderId::parse(provider_id)
                .unwrap_or_else(|_| ToolProviderId::runtime()),
            state,
            calls: self.calls.load(Ordering::Relaxed),
            errors,
            timeouts: self.timeouts.load(Ordering::Relaxed),
            cancellations: self.cancellations.load(Ordering::Relaxed),
            consecutive_errors,
        }
    }
}

fn record_max(counter: &AtomicUsize, value: usize) {
    let mut current = counter.load(Ordering::Relaxed);
    while value > current {
        match counter.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}
