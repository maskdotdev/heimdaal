use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use moka::future::Cache;
use parking_lot::Mutex;
use rayon::prelude::*;
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{Mutex as TokioMutex, Notify, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::concurrent::contracts::*;
use crate::concurrent::repo::{FileMeta, RepoSnapshot};
use crate::contracts::{ToolCounts, ToolMask, ToolName};

#[derive(Debug)]
pub(crate) struct ToolEngine {
    pub(crate) snapshot: Arc<RepoSnapshot>,
    pub(crate) artifacts: Arc<ConcurrentArtifactStore>,
    pub(crate) findings: Arc<ConcurrentFindingStore>,
    pub(crate) read: Arc<ReadService>,
    pub(crate) search: Arc<SearchCoordinator>,
    pub(crate) limits: Arc<RuntimeLimits>,
    pub(crate) redactor: Arc<Redactor>,
    result_cache: Cache<String, Arc<ToolResultEnvelope>>,
    inflight: Mutex<HashMap<String, Arc<InflightToolResult>>>,
    read_permits: Arc<Semaphore>,
    pub(crate) counters: Arc<ConcurrentAtomicCounters>,
}

#[derive(Debug, Default)]
struct InflightToolResult {
    result: TokioMutex<Option<Arc<ToolResultEnvelope>>>,
    notify: Notify,
}

impl ToolEngine {
    pub(crate) fn new(
        snapshot: Arc<RepoSnapshot>,
        limits: Arc<RuntimeLimits>,
    ) -> RuntimeResult<Self> {
        let counters = Arc::new(ConcurrentAtomicCounters::default());
        let redactor = Arc::new(Redactor::new()?);
        let artifacts = Arc::new(ConcurrentArtifactStore::default());
        let read = Arc::new(ReadService::new(
            Arc::clone(&snapshot),
            Arc::clone(&limits),
            Arc::clone(&counters),
        ));
        let search = Arc::new(SearchCoordinator::new(
            Arc::clone(&snapshot),
            Arc::clone(&limits),
            Arc::clone(&redactor),
            Arc::clone(&artifacts),
            Arc::clone(&counters),
        )?);
        Ok(Self {
            snapshot,
            artifacts,
            findings: Arc::new(ConcurrentFindingStore::default()),
            read,
            search,
            result_cache: Cache::new(limits.search_result_cache_bytes.max(1)),
            inflight: Mutex::new(HashMap::new()),
            read_permits: Arc::new(Semaphore::new(limits.max_read_concurrency_global.max(1))),
            limits,
            redactor,
            counters,
        })
    }

    pub(crate) async fn execute_batch(
        &self,
        session_id: SessionId,
        turn_id: TurnId,
        calls: Vec<crate::concurrent::contracts::ModelToolCall>,
        allowed_tools: ToolMask,
        cancel: CancellationToken,
    ) -> Vec<ToolResultEnvelope> {
        if calls.len() > self.limits.max_tool_calls_per_turn {
            return calls
                .into_iter()
                .map(|call| {
                    self.error_result(
                        call.call_id,
                        call.name,
                        ToolErrorCode::TooManyMatches,
                        "too many tool calls in one model turn",
                        false,
                    )
                })
                .collect();
        }
        if calls.len() > 1 && calls.iter().any(|call| call.name == ToolName::Finish) {
            return calls
                .into_iter()
                .map(|call| {
                    self.error_result(
                        call.call_id,
                        call.name,
                        ToolErrorCode::InvalidArgs,
                        "finish must be called alone",
                        false,
                    )
                })
                .collect();
        }

        let per_session = Arc::new(Semaphore::new(
            self.limits.max_tool_parallelism_per_session.max(1),
        ));
        let mut futures = FuturesUnordered::new();
        for call in calls {
            let engine = self;
            let per_session = Arc::clone(&per_session);
            let cancel = cancel.clone();
            let session_id = session_id.clone();
            futures.push(async move {
                let original_index = call.index;
                let result = match validate_invocation(session_id, turn_id, call, allowed_tools) {
                    Ok(invocation) => {
                        let Ok(_permit) = per_session.acquire_owned().await else {
                            return (
                                original_index,
                                engine.error_result(
                                    invocation.call_id,
                                    invocation.name,
                                    ToolErrorCode::Internal,
                                    "tool semaphore closed",
                                    false,
                                ),
                            );
                        };
                        engine.execute_invocation(invocation, cancel).await
                    }
                    Err((call_id, name, error)) => {
                        engine.error_result(call_id, name, error, "invalid tool invocation", false)
                    }
                };
                (original_index, result)
            });
        }

        let mut ordered = Vec::new();
        while let Some(result) = futures.next().await {
            ordered.push(result);
        }
        ordered.sort_by_key(|(index, _)| *index);
        ordered.into_iter().map(|(_, result)| result).collect()
    }

    async fn execute_invocation(
        &self,
        invocation: ToolInvocation,
        cancel: CancellationToken,
    ) -> ToolResultEnvelope {
        if cancel.is_cancelled() {
            return self.error_result(
                invocation.call_id,
                invocation.name,
                ToolErrorCode::Cancelled,
                "tool call cancelled",
                false,
            );
        }
        if !tool_allowed(invocation.allowed_tools, invocation.name) {
            return self.error_result(
                invocation.call_id,
                invocation.name,
                ToolErrorCode::ToolNotAllowed,
                "tool is not allowed for this session",
                false,
            );
        }
        if let Some(key) = self.cache_key(&invocation) {
            return self.execute_cacheable(key, invocation, cancel).await;
        }
        self.compute_uncached(invocation, cancel, CacheStatus::NotCacheable)
            .await
    }

    async fn execute_cacheable(
        &self,
        key: String,
        invocation: ToolInvocation,
        cancel: CancellationToken,
    ) -> ToolResultEnvelope {
        if let Some(hit) = self.result_cache.get(&key).await {
            self.counters
                .artifact_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            return hit.for_call(invocation.call_id, invocation.name, CacheStatus::Hit);
        }
        let (cell, owner) = {
            let mut inflight = self.inflight.lock();
            if let Some(cell) = inflight.get(&key) {
                (Arc::clone(cell), false)
            } else {
                let cell = Arc::new(InflightToolResult::default());
                inflight.insert(key.clone(), Arc::clone(&cell));
                (cell, true)
            }
        };
        if owner {
            let result = Arc::new(
                self.compute_uncached(invocation.clone(), cancel, CacheStatus::Miss)
                    .await,
            );
            *cell.result.lock().await = Some(Arc::clone(&result));
            cell.notify.notify_waiters();
            if result.ok {
                self.result_cache
                    .insert(key.clone(), Arc::clone(&result))
                    .await;
            }
            self.inflight.lock().remove(&key);
            result.as_ref().clone()
        } else {
            self.counters.search_dedupe_waiters.fetch_add(
                (invocation.name == ToolName::SearchText) as usize,
                Ordering::Relaxed,
            );
            loop {
                if let Some(result) = cell.result.lock().await.clone() {
                    break result.for_call(
                        invocation.call_id,
                        invocation.name,
                        CacheStatus::Deduped,
                    );
                }
                cell.notify.notified().await;
            }
        }
    }

    async fn compute_uncached(
        &self,
        invocation: ToolInvocation,
        cancel: CancellationToken,
        cache_status: CacheStatus,
    ) -> ToolResultEnvelope {
        match invocation.name {
            ToolName::ReadDiff => self.read_diff(invocation.call_id, cache_status),
            ToolName::ListFiles => self.list_files(invocation.call_id, cache_status),
            ToolName::ReadFile | ToolName::ReadHeadFile => {
                let ToolArgs::ReadFile { path } = invocation.args else {
                    return self.error_result(
                        invocation.call_id,
                        invocation.name,
                        ToolErrorCode::InvalidArgs,
                        "read_file requires path",
                        false,
                    );
                };
                self.read_file(invocation.call_id, invocation.name, path, cache_status)
                    .await
            }
            ToolName::SearchText => {
                let ToolArgs::SearchText { query } = invocation.args else {
                    return self.error_result(
                        invocation.call_id,
                        invocation.name,
                        ToolErrorCode::InvalidArgs,
                        "search_text requires query",
                        false,
                    );
                };
                self.search_text(invocation.call_id, query, cancel, cache_status)
                    .await
            }
            ToolName::RecordFinding => {
                let ToolArgs::RecordFinding { title, claim } = invocation.args else {
                    return self.error_result(
                        invocation.call_id,
                        invocation.name,
                        ToolErrorCode::InvalidArgs,
                        "record_finding requires title and claim",
                        false,
                    );
                };
                self.record_finding(invocation.call_id, title, claim)
            }
            ToolName::Finish => {
                let reason = match invocation.args {
                    ToolArgs::Finish { reason } => reason,
                    _ => "finished".to_string(),
                };
                self.finish(invocation.call_id, reason)
            }
            _ => self.error_result(
                invocation.call_id,
                invocation.name,
                ToolErrorCode::UnknownTool,
                "tool is not implemented in concurrent runtime",
                false,
            ),
        }
    }

    fn cache_key(&self, invocation: &ToolInvocation) -> Option<String> {
        match &invocation.args {
            ToolArgs::Empty
                if matches!(invocation.name, ToolName::ReadDiff | ToolName::ListFiles) =>
            {
                Some(stable_id(&[
                    &self.snapshot.snapshot_id.0,
                    invocation.name.as_str(),
                    &CONCURRENT_CONTRACT_VERSION.to_string(),
                    &REDACTION_POLICY_VERSION.to_string(),
                ]))
            }
            ToolArgs::ReadFile { path } => Some(stable_id(&[
                &self.snapshot.snapshot_id.0,
                invocation.name.as_str(),
                &path.display(),
                &CONCURRENT_CONTRACT_VERSION.to_string(),
                &REDACTION_POLICY_VERSION.to_string(),
            ])),
            ToolArgs::SearchText { query } => Some(stable_id(&[
                &self.snapshot.snapshot_id.0,
                invocation.name.as_str(),
                query,
                &self.limits.max_search_matches.to_string(),
                &CONCURRENT_CONTRACT_VERSION.to_string(),
                &REDACTION_POLICY_VERSION.to_string(),
            ])),
            _ => None,
        }
    }

    fn read_diff(&self, call_id: ToolCallId, cache_status: CacheStatus) -> ToolResultEnvelope {
        let content = self.redactor.redact(&self.snapshot.diff.content);
        let artifact_id = self.artifacts.insert(
            ArtifactKey(stable_id(&[
                &self.snapshot.snapshot_id.0,
                "read_diff",
                &self.snapshot.diff.content_hash,
            ])),
            content.clone(),
        );
        ToolResultEnvelope {
            ok: true,
            tool_call_id: call_id,
            tool_name: ToolName::ReadDiff,
            snapshot_id: self.snapshot.snapshot_id.clone(),
            artifact_id: Some(artifact_id),
            cache: CacheInfo {
                status: cache_status,
                key_hash: Some(self.snapshot.diff.content_hash.clone()),
            },
            limits: LimitInfo {
                output_bytes: content.len(),
                ..LimitInfo::default()
            },
            data: Some(json!({
                "content": content,
                "contentHash": self.snapshot.diff.content_hash,
            })),
            error: None,
        }
    }

    fn list_files(&self, call_id: ToolCallId, cache_status: CacheStatus) -> ToolResultEnvelope {
        let files = self.snapshot.list_files();
        let content = files
            .iter()
            .take(300)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        let artifact_id = self.artifacts.insert(
            ArtifactKey(stable_id(&[&self.snapshot.snapshot_id.0, "list_files"])),
            content,
        );
        ToolResultEnvelope {
            ok: true,
            tool_call_id: call_id,
            tool_name: ToolName::ListFiles,
            snapshot_id: self.snapshot.snapshot_id.clone(),
            artifact_id: Some(artifact_id),
            cache: CacheInfo {
                status: cache_status,
                key_hash: None,
            },
            limits: LimitInfo {
                truncated: files.len() > 300,
                output_bytes: files.iter().map(String::len).sum(),
                ..LimitInfo::default()
            },
            data: Some(json!({
                "files": files.into_iter().take(300).collect::<Vec<_>>(),
            })),
            error: None,
        }
    }

    async fn read_file(
        &self,
        call_id: ToolCallId,
        tool_name: ToolName,
        path: RepoPath,
        cache_status: CacheStatus,
    ) -> ToolResultEnvelope {
        let Ok(file) = self.snapshot.lookup(&path).cloned() else {
            return self.error_result(
                call_id,
                tool_name,
                ToolErrorCode::PathDenied,
                "path is not present in the repo manifest",
                false,
            );
        };
        let Ok(_permit) = self.read_permits.clone().acquire_owned().await else {
            return self.error_result(
                call_id,
                tool_name,
                ToolErrorCode::Internal,
                "read semaphore closed",
                false,
            );
        };
        match self.read.read_file(&file).await {
            Ok(read) => {
                let content = self.redactor.redact(&read.content);
                let artifact_id = self.artifacts.insert(
                    ArtifactKey(stable_id(&[
                        &self.snapshot.snapshot_id.0,
                        tool_name.as_str(),
                        &file.fingerprint,
                        &path.display(),
                    ])),
                    content.clone(),
                );
                ToolResultEnvelope {
                    ok: true,
                    tool_call_id: call_id,
                    tool_name,
                    snapshot_id: self.snapshot.snapshot_id.clone(),
                    artifact_id: Some(artifact_id.clone()),
                    cache: CacheInfo {
                        status: cache_status,
                        key_hash: Some(file.fingerprint.clone()),
                    },
                    limits: LimitInfo {
                        truncated: read.truncated,
                        output_bytes: content.len(),
                        ..LimitInfo::default()
                    },
                    data: Some(json!({
                        "path": path.display(),
                        "content": content,
                        "evidenceId": stable_id(&[
                            &self.snapshot.snapshot_id.0,
                            &file.file_id.0.to_string(),
                            &file.fingerprint,
                            &artifact_id.0,
                        ]),
                    })),
                    error: None,
                }
            }
            Err(error) => self.runtime_error_result(call_id, tool_name, error),
        }
    }

    async fn search_text(
        &self,
        call_id: ToolCallId,
        query: String,
        cancel: CancellationToken,
        cache_status: CacheStatus,
    ) -> ToolResultEnvelope {
        match self.search.search(query, cancel).await {
            Ok(mut result) => {
                result.tool_call_id = call_id;
                result.cache.status = cache_status;
                result
            }
            Err(error) => self.runtime_error_result(call_id, ToolName::SearchText, error),
        }
    }

    fn record_finding(
        &self,
        call_id: ToolCallId,
        title: String,
        claim: String,
    ) -> ToolResultEnvelope {
        let finding_id = self.findings.insert(title.clone(), claim.clone());
        ToolResultEnvelope {
            ok: true,
            tool_call_id: call_id,
            tool_name: ToolName::RecordFinding,
            snapshot_id: self.snapshot.snapshot_id.clone(),
            artifact_id: None,
            cache: CacheInfo {
                status: CacheStatus::NotCacheable,
                key_hash: None,
            },
            limits: LimitInfo::default(),
            data: Some(json!({
                "findingId": finding_id,
                "title": title,
                "claim": claim,
            })),
            error: None,
        }
    }

    fn finish(&self, call_id: ToolCallId, reason: String) -> ToolResultEnvelope {
        ToolResultEnvelope {
            ok: true,
            tool_call_id: call_id,
            tool_name: ToolName::Finish,
            snapshot_id: self.snapshot.snapshot_id.clone(),
            artifact_id: None,
            cache: CacheInfo {
                status: CacheStatus::NotCacheable,
                key_hash: None,
            },
            limits: LimitInfo::default(),
            data: Some(json!({ "reason": reason })),
            error: None,
        }
    }

    pub(crate) fn error_result(
        &self,
        call_id: ToolCallId,
        tool_name: ToolName,
        code: ToolErrorCode,
        message: &str,
        retryable: bool,
    ) -> ToolResultEnvelope {
        self.counters.tool_errors.fetch_add(1, Ordering::Relaxed);
        ToolResultEnvelope {
            ok: false,
            tool_call_id: call_id,
            tool_name,
            snapshot_id: self.snapshot.snapshot_id.clone(),
            artifact_id: None,
            cache: CacheInfo {
                status: CacheStatus::NotCacheable,
                key_hash: None,
            },
            limits: LimitInfo::default(),
            data: None,
            error: Some(ToolErrorInfo {
                code,
                message: message.to_string(),
                retryable,
                partial: false,
            }),
        }
    }

    fn runtime_error_result(
        &self,
        call_id: ToolCallId,
        tool_name: ToolName,
        error: RuntimeError,
    ) -> ToolResultEnvelope {
        match error {
            RuntimeError::RepoAccessDenied => self.error_result(
                call_id,
                tool_name,
                ToolErrorCode::PathDenied,
                "path denied by repo policy",
                false,
            ),
            RuntimeError::LimitExceeded { .. } => self.error_result(
                call_id,
                tool_name,
                ToolErrorCode::TooLarge,
                "resource limit exceeded",
                false,
            ),
            RuntimeError::Cancelled => self.error_result(
                call_id,
                tool_name,
                ToolErrorCode::Cancelled,
                "operation cancelled",
                true,
            ),
            RuntimeError::InvalidInput(_) => self.error_result(
                call_id,
                tool_name,
                ToolErrorCode::InvalidArgs,
                "invalid tool input",
                false,
            ),
            _ => self.error_result(
                call_id,
                tool_name,
                ToolErrorCode::Internal,
                "internal tool error",
                false,
            ),
        }
    }

    pub(crate) fn snapshot_counters(&self) -> ConcurrentCounters {
        self.counters.snapshot()
    }
}

#[derive(Debug)]
pub(crate) struct ReadService {
    snapshot: Arc<RepoSnapshot>,
    limits: Arc<RuntimeLimits>,
    file_cache: Cache<String, Arc<Vec<u8>>>,
    counters: Arc<ConcurrentAtomicCounters>,
}

impl ReadService {
    fn new(
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

    async fn read_file(&self, file: &FileMeta) -> RuntimeResult<ReadResult> {
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
struct ReadResult {
    content: String,
    truncated: bool,
}

fn decode_read(bytes: &[u8], truncated: bool) -> RuntimeResult<ReadResult> {
    let content = String::from_utf8(bytes.to_vec())
        .map_err(|_| RuntimeError::InvalidInput("file is not valid UTF-8".to_string()))?;
    Ok(ReadResult { content, truncated })
}

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
    fn new(
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

    async fn search(
        &self,
        query: String,
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
        let _permit = self
            .search_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RuntimeError::Cancelled)?;
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
            .filter(|file| file.is_text_candidate)
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
        let content = redacted.join("\n");
        let artifact_id = self.artifacts.insert(
            ArtifactKey(stable_id(&[
                &self.snapshot.snapshot_id.0,
                "search_text",
                &query,
                &max_matches.to_string(),
            ])),
            content,
        );
        Ok(ToolResultEnvelope {
            ok: true,
            tool_call_id: ToolCallId("search-result-template".to_string()),
            tool_name: ToolName::SearchText,
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
            },
            data: Some(json!({
                "query": query,
                "searchedFiles": searched_files,
                "skippedFiles": skipped_files,
                "bytesScanned": bytes_scanned,
                "returnedMatches": redacted.len(),
                "truncated": truncated,
                "matches": redacted,
                "elapsedMs": started.elapsed().as_millis() as u64,
            })),
            error: None,
        })
    }
}

#[derive(Debug)]
struct SearchFileResult {
    matches: Vec<String>,
    searched_files: usize,
    skipped_files: usize,
    bytes_scanned: usize,
}

impl SearchFileResult {
    fn cancelled() -> Self {
        Self {
            matches: Vec::new(),
            searched_files: 0,
            skipped_files: 0,
            bytes_scanned: 0,
        }
    }
}

fn scan_file(
    snapshot: &RepoSnapshot,
    file_id: crate::concurrent::contracts::FileId,
    max_bytes: usize,
    needles: &[String],
    max_matches: usize,
) -> SearchFileResult {
    let Ok(file) = snapshot.file(file_id) else {
        return SearchFileResult {
            matches: Vec::new(),
            searched_files: 0,
            skipped_files: 1,
            bytes_scanned: 0,
        };
    };
    let Ok((bytes, _)) = snapshot.read_bounded(file_id, max_bytes) else {
        return SearchFileResult {
            matches: Vec::new(),
            searched_files: 0,
            skipped_files: 1,
            bytes_scanned: 0,
        };
    };
    let content = match String::from_utf8(bytes) {
        Ok(content) => content,
        Err(_) => {
            return SearchFileResult {
                matches: Vec::new(),
                searched_files: 0,
                skipped_files: 1,
                bytes_scanned: 0,
            };
        }
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
    }
}

#[derive(Debug, Default)]
pub(crate) struct ConcurrentArtifactStore {
    by_id: DashMap<String, Arc<ConcurrentArtifact>>,
    order: Mutex<Vec<String>>,
}

impl ConcurrentArtifactStore {
    fn insert(&self, key: ArtifactKey, content: String) -> ArtifactId {
        let content_hash = stable_id(&[&content]);
        let artifact_id = ArtifactId(format!("art_{}", stable_id(&[&key.0, &content_hash])));
        if self
            .by_id
            .insert(
                artifact_id.0.clone(),
                Arc::new(ConcurrentArtifact {
                    artifact_id: artifact_id.clone(),
                    bytes: content.len(),
                    content_hash,
                    content,
                }),
            )
            .is_none()
        {
            self.order.lock().push(artifact_id.0.clone());
        }
        artifact_id
    }

    pub(crate) fn stats(&self) -> (usize, usize) {
        let artifacts = self.by_id.iter().collect::<Vec<_>>();
        let bytes = artifacts.iter().map(|item| item.bytes).sum();
        (artifacts.len(), bytes)
    }
}

#[derive(Debug)]
struct ConcurrentArtifact {
    artifact_id: ArtifactId,
    bytes: usize,
    content_hash: String,
    content: String,
}

#[derive(Debug, Default)]
pub(crate) struct ConcurrentFindingStore {
    by_id: DashMap<String, (String, String)>,
    order: Mutex<Vec<String>>,
}

impl ConcurrentFindingStore {
    fn insert(&self, title: String, claim: String) -> String {
        let id = format!("finding_{}", stable_id(&[&title, &claim]));
        if self.by_id.insert(id.clone(), (title, claim)).is_none() {
            self.order.lock().push(id.clone());
        }
        id
    }

    pub(crate) fn len(&self) -> usize {
        self.by_id.len()
    }
}

#[derive(Debug)]
pub(crate) struct Redactor {
    patterns: Vec<Regex>,
}

impl Redactor {
    fn new() -> RuntimeResult<Self> {
        let patterns = [
            r"AKIA[0-9A-Z]{16}",
            r"github_pat_[A-Za-z0-9_]{20,}",
            r"ghp_[A-Za-z0-9_]{20,}",
            r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}",
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----",
        ];
        let mut compiled = Vec::new();
        for pattern in patterns {
            compiled.push(Regex::new(pattern).map_err(|_| {
                RuntimeError::Invariant("failed to compile built-in redaction regex")
            })?);
        }
        Ok(Self { patterns: compiled })
    }

    fn redact(&self, input: &str) -> String {
        let mut output = input.to_string();
        for pattern in &self.patterns {
            output = pattern.replace_all(&output, "[REDACTED]").into_owned();
        }
        output
    }
}

#[derive(Debug, Default)]
pub(crate) struct ConcurrentAtomicCounters {
    search_scans: AtomicUsize,
    search_dedupe_waiters: AtomicUsize,
    search_cache_hits: AtomicUsize,
    read_cache_hits: AtomicUsize,
    read_file_reads: AtomicUsize,
    tool_errors: AtomicUsize,
    artifact_cache_hits: AtomicUsize,
}

impl ConcurrentAtomicCounters {
    fn snapshot(&self) -> ConcurrentCounters {
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadFileArgs {
    path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchTextArgs {
    query: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordFindingArgs {
    title: String,
    claim: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FinishArgs {
    reason: Option<String>,
}

pub(crate) fn validate_invocation(
    session_id: SessionId,
    turn_id: TurnId,
    call: crate::concurrent::contracts::ModelToolCall,
    allowed_tools: ToolMask,
) -> Result<ToolInvocation, (ToolCallId, ToolName, ToolErrorCode)> {
    let args = match call.name {
        ToolName::ReadDiff | ToolName::ListFiles => ToolArgs::Empty,
        ToolName::ReadFile | ToolName::ReadHeadFile => {
            let parsed: ReadFileArgs = serde_json::from_str(&call.raw_arguments)
                .map_err(|_| (call.call_id.clone(), call.name, ToolErrorCode::InvalidArgs))?;
            let path = RepoPath::parse(&parsed.path)
                .map_err(|_| (call.call_id.clone(), call.name, ToolErrorCode::PathDenied))?;
            ToolArgs::ReadFile { path }
        }
        ToolName::SearchText => {
            let parsed: SearchTextArgs = serde_json::from_str(&call.raw_arguments)
                .map_err(|_| (call.call_id.clone(), call.name, ToolErrorCode::InvalidArgs))?;
            ToolArgs::SearchText {
                query: parsed.query,
            }
        }
        ToolName::RecordFinding => {
            let parsed: RecordFindingArgs = serde_json::from_str(&call.raw_arguments)
                .map_err(|_| (call.call_id.clone(), call.name, ToolErrorCode::InvalidArgs))?;
            ToolArgs::RecordFinding {
                title: parsed.title,
                claim: parsed.claim,
            }
        }
        ToolName::Finish => {
            let parsed: FinishArgs = serde_json::from_str(&call.raw_arguments)
                .map_err(|_| (call.call_id.clone(), call.name, ToolErrorCode::InvalidArgs))?;
            ToolArgs::Finish {
                reason: parsed.reason.unwrap_or_else(|| "finished".to_string()),
            }
        }
        _ => return Err((call.call_id, call.name, ToolErrorCode::UnknownTool)),
    };
    Ok(ToolInvocation {
        session_id,
        turn_id,
        original_index: call.index,
        call_id: call.call_id,
        name: call.name,
        args,
        allowed_tools,
    })
}

pub(crate) fn tool_allowed(mask: ToolMask, tool: ToolName) -> bool {
    match tool {
        ToolName::ListChangedFiles => mask.list_changed_files,
        ToolName::ReadDiff => mask.read_diff,
        ToolName::ListFiles => mask.list_files,
        ToolName::ReadFile => mask.read_file,
        ToolName::ReadBaseFile => mask.read_base_file,
        ToolName::ReadHeadFile => mask.read_head_file,
        ToolName::SearchText => mask.search_text,
        ToolName::FindRelatedFiles => mask.find_related_files,
        ToolName::FindTestsForFile => mask.find_tests_for_file,
        ToolName::ListImports => mask.list_imports,
        ToolName::RecordFinding => mask.record_finding,
        ToolName::ChallengeFinding => mask.challenge_finding,
        ToolName::Finish => mask.finish,
    }
}

pub(crate) fn count_tool_result(counts: &mut ToolCounts, result: &ToolResultEnvelope) {
    if result.ok {
        counts.increment(result.tool_name);
    }
}
