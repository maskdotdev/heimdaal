use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use rayon::prelude::*;
use serde_json::json;
#[cfg(test)]
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::contracts::ToolName;
use crate::runtime::contracts::*;
use crate::runtime::repo::RepoSnapshot;

use super::metrics::ConcurrentAtomicCounters;
use super::redaction::Redactor;
use super::store::ConcurrentArtifactStore;

#[derive(Debug)]
pub(crate) struct SearchCoordinator {
    snapshot: Arc<RepoSnapshot>,
    limits: Arc<RuntimeLimits>,
    redactor: Arc<Redactor>,
    artifacts: Arc<ConcurrentArtifactStore>,
    pool: Arc<rayon::ThreadPool>,
    search_permits: Arc<Semaphore>,
    counters: Arc<ConcurrentAtomicCounters>,
}

impl SearchCoordinator {
    pub(super) fn new(
        snapshot: Arc<RepoSnapshot>,
        limits: Arc<RuntimeLimits>,
        redactor: Arc<Redactor>,
        artifacts: Arc<ConcurrentArtifactStore>,
        counters: Arc<ConcurrentAtomicCounters>,
    ) -> RuntimeResult<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(limits.search_threads.max(1))
            .thread_name(|index| format!("heimdaal-search-{index}"))
            .build()
            .map_err(|_| RuntimeError::Invariant("failed to build search pool"))?;
        Ok(Self {
            snapshot,
            limits: Arc::clone(&limits),
            redactor,
            artifacts,
            pool: Arc::new(pool),
            search_permits: Arc::new(Semaphore::new(limits.max_search_jobs_global.max(1))),
            counters,
        })
    }

    pub(super) async fn search(
        &self,
        query: String,
        fs_scope: &FsScope,
        scope_key: &ScopeKey,
        cancel: CancellationToken,
    ) -> RuntimeResult<ToolResultEnvelope> {
        if query.trim().is_empty() {
            return Err(RuntimeError::InvalidInput("empty search query".to_string()));
        }
        if query.len() > self.limits.max_search_pattern_bytes {
            return Err(RuntimeError::LimitExceeded {
                kind: "search_pattern_bytes",
            });
        }
        let _permit = tokio::select! {
            _ = cancel.cancelled() => return Err(RuntimeError::Cancelled),
            permit = self.search_permits.clone().acquire_owned() => {
                permit.map_err(|_| RuntimeError::Cancelled)?
            }
        };
        if cancel.is_cancelled() {
            return Err(RuntimeError::Cancelled);
        }
        let started = Instant::now();
        let needles = query
            .split('|')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if needles.is_empty() {
            return Err(RuntimeError::InvalidInput("empty search query".to_string()));
        }
        let candidates = self
            .snapshot
            .manifest
            .files
            .iter()
            .filter(|file| file.is_text_candidate && fs_scope.allows(&file.rel_path))
            .map(|file| file.file_id)
            .collect::<Vec<_>>();
        let snapshot = Arc::clone(&self.snapshot);
        let max_bytes = self.limits.max_file_bytes_search;
        let max_matches = self.limits.max_search_matches;
        let pool = Arc::clone(&self.pool);
        let cancel_for_scan = cancel.clone();
        let scan = tokio::task::spawn_blocking(move || {
            pool.install(|| {
                candidates
                    .par_iter()
                    .map(|file_id| {
                        if cancel_for_scan.is_cancelled() {
                            return SearchFileResult::cancelled();
                        }
                        scan_file(&snapshot, *file_id, max_bytes, &needles, max_matches)
                    })
                    .collect::<Vec<_>>()
            })
        })
        .await
        .map_err(|_| RuntimeError::Invariant("search task join failed"))?;
        if cancel.is_cancelled() {
            return Err(RuntimeError::Cancelled);
        }
        self.counters.search_scans.fetch_add(1, Ordering::Relaxed);
        let mut matches = Vec::new();
        let mut searched_files = 0usize;
        let mut skipped_files = 0usize;
        let mut bytes_scanned = 0usize;
        let mut truncated = false;
        for result in scan {
            if let Some(error) = result.error {
                return Err(error);
            }
            searched_files += result.searched_files;
            skipped_files += result.skipped_files;
            bytes_scanned += result.bytes_scanned;
            for item in result.matches {
                if matches.len() >= max_matches {
                    truncated = true;
                    break;
                }
                matches.push(item);
            }
            if matches.len() >= max_matches {
                truncated = true;
                break;
            }
        }
        matches.sort();
        let redacted = matches
            .iter()
            .map(|line| self.redactor.redact(line))
            .collect::<Vec<_>>();
        let first_match = matches.first().and_then(|line| parse_search_match(line));
        let content = redacted.join("\n");
        let artifact_id = self.artifacts.insert_views(
            ArtifactKey(stable_id(&[
                &self.snapshot.snapshot_id.0,
                "search_text",
                scope_key.as_str(),
                &query,
                &max_matches.to_string(),
            ])),
            matches.join("\n"),
            content,
        );
        Ok(ToolResultEnvelope {
            ok: true,
            tool_call_id: ToolCallId("search-result-template".to_string()),
            tool_name: ToolId::from(ToolName::SearchText),
            provider_id: ToolProviderId::builtin_review(),
            snapshot_id: self.snapshot.snapshot_id.clone(),
            artifact_id: Some(artifact_id),
            cache: CacheInfo {
                status: CacheStatus::Miss,
                key_hash: Some(stable_id(&[&query])),
            },
            limits: LimitInfo {
                truncated,
                output_bytes: redacted.iter().map(String::len).sum(),
                searched_files,
                skipped_files,
                bytes_scanned,
                ..LimitInfo::default()
            },
            data: Some(json!({
                "query": query,
                "searchedFiles": searched_files,
                "skippedFiles": skipped_files,
                "bytesScanned": bytes_scanned,
                "returnedMatches": redacted.len(),
                "truncated": truncated,
                "matches": redacted,
                "firstMatch": first_match.map(|item| json!({
                    "path": item.path,
                    "line": item.line,
                })),
                "elapsedMs": started.elapsed().as_millis() as u64,
            })),
            error: None,
        })
    }

    #[cfg(test)]
    pub(crate) async fn acquire_search_permit_for_test(&self) -> OwnedSemaphorePermit {
        self.search_permits
            .clone()
            .acquire_owned()
            .await
            .expect("search semaphore should be open")
    }
}

#[derive(Debug)]
struct SearchFileResult {
    matches: Vec<String>,
    searched_files: usize,
    skipped_files: usize,
    bytes_scanned: usize,
    error: Option<RuntimeError>,
}

impl SearchFileResult {
    fn cancelled() -> Self {
        Self {
            matches: Vec::new(),
            searched_files: 0,
            skipped_files: 0,
            bytes_scanned: 0,
            error: None,
        }
    }

    fn skipped() -> Self {
        Self {
            matches: Vec::new(),
            searched_files: 0,
            skipped_files: 1,
            bytes_scanned: 0,
            error: None,
        }
    }

    fn error(error: RuntimeError) -> Self {
        Self {
            matches: Vec::new(),
            searched_files: 0,
            skipped_files: 0,
            bytes_scanned: 0,
            error: Some(error),
        }
    }
}

fn scan_file(
    snapshot: &RepoSnapshot,
    file_id: crate::runtime::contracts::FileId,
    max_bytes: usize,
    needles: &[String],
    max_matches: usize,
) -> SearchFileResult {
    let Ok(file) = snapshot.file(file_id) else {
        return SearchFileResult::skipped();
    };
    let (bytes, _) = match snapshot.read_bounded(file_id, max_bytes) {
        Ok(read) => read,
        Err(RuntimeError::SnapshotStale { path }) => {
            return SearchFileResult::error(RuntimeError::SnapshotStale { path });
        }
        Err(_) => return SearchFileResult::skipped(),
    };
    let content = match String::from_utf8(bytes) {
        Ok(content) => content,
        Err(_) => return SearchFileResult::skipped(),
    };
    let bytes_scanned = content.len();
    let mut matches = Vec::new();
    for (index, line) in content.lines().enumerate() {
        if needles.iter().any(|needle| line.contains(needle)) {
            matches.push(format!(
                "{}:{}:{}",
                file.rel_path.display(),
                index + 1,
                line.trim()
            ));
            if matches.len() >= max_matches {
                break;
            }
        }
    }
    SearchFileResult {
        matches,
        searched_files: 1,
        skipped_files: 0,
        bytes_scanned,
        error: None,
    }
}

struct SearchMatchAnchor {
    path: String,
    line: usize,
}

fn parse_search_match(value: &str) -> Option<SearchMatchAnchor> {
    let mut parts = value.splitn(3, ':');
    let path = parts.next()?.to_string();
    let line = parts.next()?.parse::<usize>().ok()?;
    if path.is_empty() || line == 0 {
        return None;
    }
    Some(SearchMatchAnchor { path, line })
}
