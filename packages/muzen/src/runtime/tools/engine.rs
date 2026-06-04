use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use moka::future::Cache;
use parking_lot::Mutex;
use serde_json::json;
use tokio::sync::{Mutex as TokioMutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::contracts::{ByteRangeV1, EvidenceRefV1, ToolName};
use crate::runtime::contracts::*;
use crate::runtime::repo::RepoSnapshot;
use crate::util::redaction_none;

use super::authorization::ToolAuthorizer;
use super::metrics::{ConcurrentAtomicCounters, ConcurrentToolMetricsStore};
use super::provider::{
    BuiltinReviewToolProvider, InProcessToolProvider, JsonRpcToolProvider, ToolProvider,
};
use super::read::ReadService;
use super::redaction::Redactor;
use super::registry::{CustomToolContext, CustomToolOutput, ToolRegistry};
use super::search::SearchCoordinator;
use super::store::{
    artifact_kind_for_tool, evidence_line_range, evidence_location, evidence_revision_for_tool,
    finding_id_for_call, ConcurrentArtifactStore, ConcurrentFindingStore,
};
use super::validation::validate_invocation;

pub(crate) struct ToolEngine {
    pub(crate) snapshot: Arc<RepoSnapshot>,
    pub(crate) artifacts: Arc<ConcurrentArtifactStore>,
    pub(crate) findings: Arc<ConcurrentFindingStore>,
    pub(crate) read: Arc<ReadService>,
    pub(crate) search: Arc<SearchCoordinator>,
    pub(crate) registry: Arc<ToolRegistry>,
    pub(crate) limits: Arc<RuntimeLimits>,
    pub(crate) redactor: Arc<Redactor>,
    result_cache: Cache<String, Arc<ToolResultEnvelope>>,
    inflight: Mutex<HashMap<String, Arc<InflightToolResult>>>,
    read_permits: Arc<Semaphore>,
    authorizer: ToolAuthorizer,
    provider_permits: DashMap<String, Arc<Semaphore>>,
    providers: Vec<Arc<dyn ToolProvider>>,
    pub(crate) counters: Arc<ConcurrentAtomicCounters>,
    pub(crate) metrics: Arc<ConcurrentToolMetricsStore>,
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
        Self::with_registry(snapshot, limits, Arc::new(ToolRegistry::review_defaults()?))
    }

    pub(crate) fn with_registry(
        snapshot: Arc<RepoSnapshot>,
        limits: Arc<RuntimeLimits>,
        registry: Arc<ToolRegistry>,
    ) -> RuntimeResult<Self> {
        let counters = Arc::new(ConcurrentAtomicCounters::default());
        let metrics = Arc::new(ConcurrentToolMetricsStore::default());
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
        let mut providers: Vec<Arc<dyn ToolProvider>> = vec![
            Arc::new(BuiltinReviewToolProvider),
            Arc::new(InProcessToolProvider),
        ];
        providers.extend(registry.jsonrpc_transports().into_iter().map(
            |(provider_id, transport)| {
                Arc::new(JsonRpcToolProvider::new(provider_id, transport)) as Arc<dyn ToolProvider>
            },
        ));
        Ok(Self {
            snapshot,
            artifacts,
            findings: Arc::new(ConcurrentFindingStore::default()),
            read,
            search,
            registry,
            result_cache: Cache::new(limits.search_result_cache_bytes.max(1)),
            inflight: Mutex::new(HashMap::new()),
            read_permits: Arc::new(Semaphore::new(limits.max_read_concurrency_global.max(1))),
            authorizer: ToolAuthorizer::new(),
            provider_permits: DashMap::new(),
            providers,
            limits,
            redactor,
            counters,
            metrics,
        })
    }

    pub(crate) fn record_finding_result(
        &self,
        session_id: &SessionId,
        result: &ToolResultEnvelope,
        evidence_results: &[ToolResultEnvelope],
        revision_id: &str,
    ) -> Option<String> {
        let evidence = self.evidence_refs(evidence_results, revision_id);
        self.findings
            .insert_from_tool_result(session_id, result, evidence)
    }

    pub(crate) async fn execute_batch(
        &self,
        scope: SessionScope,
        turn_id: TurnId,
        calls: Vec<crate::runtime::contracts::ModelToolCall>,
        cancel: CancellationToken,
    ) -> Vec<ToolResultEnvelope> {
        if calls.len() > self.limits.max_tool_calls_per_turn {
            let results = calls
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
                .collect::<Vec<_>>();
            self.record_batch_metrics(&results);
            return results;
        }
        if calls.len() > 1
            && calls
                .iter()
                .any(|call| call.name.as_builtin() == Some(ToolName::Finish))
        {
            let results = calls
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
                .collect::<Vec<_>>();
            self.record_batch_metrics(&results);
            return results;
        }

        let mut seen_calls = HashSet::new();
        let mut duplicate_results = Vec::new();
        let mut calls_to_execute = Vec::new();
        for call in calls {
            let key = stable_id(&[call.name.as_str(), &call.raw_arguments]);
            if matches!(
                call.name.as_builtin(),
                Some(ToolName::RecordFinding | ToolName::Finish)
            ) || seen_calls.insert(key)
            {
                calls_to_execute.push(call);
            } else {
                duplicate_results.push((
                    call.index,
                    self.error_result(
                        call.call_id,
                        call.name,
                        ToolErrorCode::InvalidArgs,
                        "duplicate tool call in same model turn",
                        false,
                    ),
                ));
            }
        }

        let per_session = Arc::new(Semaphore::new(
            self.limits.max_tool_parallelism_per_session.max(1),
        ));
        let scope_key = scope
            .capabilities
            .fs_scope
            .scope_key(&self.snapshot.snapshot_id);
        let scope = Arc::new(scope);
        let mut futures = FuturesUnordered::new();
        for call in calls_to_execute {
            let engine = self;
            let per_session = Arc::clone(&per_session);
            let cancel = cancel.clone();
            let session_id = scope.id.clone();
            let capabilities = scope.capabilities.clone();
            let scope_key = scope_key.clone();
            futures.push(async move {
                let original_index = call.index;
                let result = match validate_invocation(
                    session_id,
                    turn_id,
                    call,
                    capabilities,
                    scope_key,
                    &engine.registry,
                ) {
                    Ok(invocation) => {
                        let Ok(_permit) = per_session.acquire_owned().await else {
                            return (
                                original_index,
                                engine.error_result(
                                    invocation.call_id,
                                    invocation.tool_id.clone(),
                                    ToolErrorCode::Internal,
                                    "tool semaphore closed",
                                    false,
                                ),
                            );
                        };
                        let input_bytes = invocation.input_bytes;
                        let started = Instant::now();
                        let mut result = engine.execute_invocation(invocation, cancel).await;
                        result.limits.input_bytes = input_bytes;
                        result.limits.latency_ms = elapsed_ms(started);
                        if matches!(result.cache.status, CacheStatus::Hit | CacheStatus::Deduped) {
                            result.limits.queue_wait_ms = 0;
                        }
                        result
                    }
                    Err((call_id, name, error)) => engine.error_result(
                        call_id,
                        name,
                        error,
                        validation_error_message(error),
                        false,
                    ),
                };
                (original_index, result)
            });
        }

        let mut ordered = Vec::new();
        ordered.extend(duplicate_results);
        while let Some(result) = futures.next().await {
            ordered.push(result);
        }
        ordered.sort_by_key(|(index, _)| *index);
        let results = ordered
            .into_iter()
            .map(|(_, result)| result)
            .collect::<Vec<_>>();
        self.record_batch_metrics(&results);
        results
    }

    async fn execute_invocation(
        &self,
        invocation: ToolInvocation,
        cancel: CancellationToken,
    ) -> ToolResultEnvelope {
        if cancel.is_cancelled() {
            return self.error_result(
                invocation.call_id,
                invocation.tool_id,
                ToolErrorCode::Cancelled,
                "tool call cancelled",
                false,
            );
        }
        let Some(definition) = self.registry.definition(&invocation.tool_id) else {
            return self.error_result(
                invocation.call_id,
                invocation.tool_id,
                ToolErrorCode::UnknownTool,
                "tool is not registered",
                false,
            );
        };
        if let Err(error) = self.authorizer.authorize(&invocation, definition) {
            return self.error_result(
                invocation.call_id,
                invocation.tool_id,
                error.code,
                error.message,
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
            return hit.for_call(invocation.call_id, invocation.tool_id, CacheStatus::Hit);
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
                (invocation.builtin_name == Some(ToolName::SearchText)) as usize,
                Ordering::Relaxed,
            );
            loop {
                if let Some(result) = cell.result.lock().await.clone() {
                    return result.for_call(
                        invocation.call_id,
                        invocation.tool_id,
                        CacheStatus::Deduped,
                    );
                }
                tokio::select! {
                    _ = cancel.cancelled() => {
                        return self.error_result(
                            invocation.call_id,
                            invocation.tool_id,
                            ToolErrorCode::Cancelled,
                            "operation cancelled",
                            true,
                        );
                    }
                    _ = cell.notify.notified() => {}
                }
            }
        }
    }

    async fn compute_uncached(
        &self,
        invocation: ToolInvocation,
        cancel: CancellationToken,
        cache_status: CacheStatus,
    ) -> ToolResultEnvelope {
        let call_id = invocation.call_id.clone();
        let tool_id = invocation.tool_id.clone();
        let Some(definition) = self.registry.definition(&tool_id) else {
            return self.error_result(
                call_id,
                tool_id,
                ToolErrorCode::UnknownTool,
                "tool is not registered",
                false,
            );
        };
        let Some(provider) = self
            .providers
            .iter()
            .find(|provider| provider.accepts(definition))
        else {
            return self.error_result(
                call_id,
                tool_id,
                ToolErrorCode::UnknownTool,
                "tool has no provider",
                false,
            );
        };
        let provider_id = provider.id();
        let timeout = Duration::from_millis(self.limits.max_tool_provider_ms.max(1));
        let queue_started = Instant::now();
        let permit = match self.acquire_provider_permit(&provider_id, timeout).await {
            Ok(permit) => permit,
            Err(error) => {
                let mut result = self.runtime_error_result(call_id, tool_id, error);
                result.limits.queue_wait_ms = elapsed_ms_allow_zero(queue_started);
                return result;
            }
        };
        let queue_wait_ms = elapsed_ms_allow_zero(queue_started);
        let _permit = permit;
        let mut result = match tokio::time::timeout(
            timeout,
            provider.execute(self, invocation, cancel, cache_status),
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => self.runtime_error_result(call_id, tool_id, error),
            Err(_) => self.runtime_error_result(call_id, tool_id, RuntimeError::Timeout),
        };
        result.limits.queue_wait_ms = queue_wait_ms;
        result
    }

    async fn acquire_provider_permit(
        &self,
        provider_id: &ToolProviderId,
        timeout: Duration,
    ) -> RuntimeResult<OwnedSemaphorePermit> {
        let permits = self
            .provider_permits
            .entry(provider_id.as_str().to_string())
            .or_insert_with(|| {
                Arc::new(Semaphore::new(
                    self.limits
                        .max_tool_provider_concurrency_per_provider
                        .max(1),
                ))
            })
            .clone();
        tokio::time::timeout(timeout, permits.acquire_owned())
            .await
            .map_err(|_| RuntimeError::Timeout)?
            .map_err(|_| RuntimeError::Cancelled)
    }

    pub(super) async fn execute_builtin_tool(
        &self,
        invocation: ToolInvocation,
        cancel: CancellationToken,
        cache_status: CacheStatus,
    ) -> ToolResultEnvelope {
        match invocation.builtin_name {
            Some(ToolName::ListChangedFiles) => self.list_changed_files(
                invocation.call_id,
                &invocation.capabilities.fs_scope,
                &invocation.scope_key,
                cache_status,
            ),
            Some(ToolName::ReadDiff) => self.read_diff(invocation.call_id, cache_status),
            Some(ToolName::ListFiles) => self.list_files(
                invocation.call_id,
                &invocation.capabilities.fs_scope,
                &invocation.scope_key,
                cache_status,
            ),
            Some(ToolName::ReadBaseFile) => {
                let ToolArgs::ReadFile { path } = invocation.args else {
                    return self.error_result(
                        invocation.call_id,
                        invocation.tool_id,
                        ToolErrorCode::InvalidArgs,
                        "read_base_file requires path",
                        false,
                    );
                };
                if !invocation.capabilities.fs_scope.allows(&path) {
                    return self.error_result(
                        invocation.call_id,
                        invocation.tool_id,
                        ToolErrorCode::PathDenied,
                        "path is outside the session filesystem scope",
                        false,
                    );
                }
                self.base_snapshot_unavailable(invocation.call_id, path, cache_status)
            }
            Some(ToolName::ReadFile | ToolName::ReadHeadFile) => {
                let ToolArgs::ReadFile { path } = invocation.args else {
                    return self.error_result(
                        invocation.call_id,
                        invocation.tool_id,
                        ToolErrorCode::InvalidArgs,
                        "read_file requires path",
                        false,
                    );
                };
                if !invocation.capabilities.fs_scope.allows(&path) {
                    return self.error_result(
                        invocation.call_id,
                        invocation.tool_id,
                        ToolErrorCode::PathDenied,
                        "path is outside the session filesystem scope",
                        false,
                    );
                }
                self.read_file(
                    invocation.call_id,
                    invocation
                        .builtin_name
                        .expect("read_file branch has builtin tool"),
                    path,
                    cache_status,
                )
                .await
            }
            Some(ToolName::SearchText) => {
                let ToolArgs::SearchText { query } = invocation.args else {
                    return self.error_result(
                        invocation.call_id,
                        invocation.tool_id,
                        ToolErrorCode::InvalidArgs,
                        "search_text requires query",
                        false,
                    );
                };
                self.search_text(
                    invocation.call_id,
                    query,
                    &invocation.capabilities.fs_scope,
                    &invocation.scope_key,
                    cancel,
                    cache_status,
                )
                .await
            }
            Some(ToolName::FindRelatedFiles | ToolName::FindTestsForFile) => {
                let ToolArgs::ReadFile { path } = invocation.args else {
                    return self.error_result(
                        invocation.call_id,
                        invocation.tool_id,
                        ToolErrorCode::InvalidArgs,
                        "related/test file tools require path",
                        false,
                    );
                };
                if !invocation.capabilities.fs_scope.allows(&path) {
                    return self.error_result(
                        invocation.call_id,
                        invocation.tool_id,
                        ToolErrorCode::PathDenied,
                        "path is outside the session filesystem scope",
                        false,
                    );
                }
                self.find_files_for_path(
                    invocation.call_id,
                    invocation
                        .builtin_name
                        .expect("related/test branch has builtin tool"),
                    path,
                    &invocation.capabilities.fs_scope,
                    &invocation.scope_key,
                    cache_status,
                )
            }
            Some(ToolName::ListImports) => {
                let ToolArgs::ReadFile { path } = invocation.args else {
                    return self.error_result(
                        invocation.call_id,
                        invocation.tool_id,
                        ToolErrorCode::InvalidArgs,
                        "list_imports requires path",
                        false,
                    );
                };
                if !invocation.capabilities.fs_scope.allows(&path) {
                    return self.error_result(
                        invocation.call_id,
                        invocation.tool_id,
                        ToolErrorCode::PathDenied,
                        "path is outside the session filesystem scope",
                        false,
                    );
                }
                self.list_imports(invocation.call_id, path, cache_status)
                    .await
            }
            Some(ToolName::RecordFinding) => {
                let ToolArgs::RecordFinding { title, claim } = invocation.args else {
                    return self.error_result(
                        invocation.call_id,
                        invocation.tool_id,
                        ToolErrorCode::InvalidArgs,
                        "record_finding requires title and claim",
                        false,
                    );
                };
                self.record_finding(invocation.call_id, title, claim)
            }
            Some(ToolName::ChallengeFinding) => {
                let ToolArgs::ChallengeFinding {
                    finding_id,
                    rationale,
                } = invocation.args
                else {
                    return self.error_result(
                        invocation.call_id,
                        invocation.tool_id,
                        ToolErrorCode::InvalidArgs,
                        "challenge_finding requires finding_id and rationale",
                        false,
                    );
                };
                self.challenge_finding(invocation.call_id, finding_id, rationale)
            }
            Some(ToolName::Finish) => {
                let reason = match invocation.args {
                    ToolArgs::Finish { reason } => reason,
                    _ => "finished".to_string(),
                };
                self.finish(invocation.call_id, reason)
            }
            None => self.error_result(
                invocation.call_id,
                invocation.tool_id,
                ToolErrorCode::UnknownTool,
                "tool is not registered as a built-in provider tool",
                false,
            ),
        }
    }

    fn cache_key(&self, invocation: &ToolInvocation) -> Option<String> {
        match &invocation.args {
            ToolArgs::Empty
                if matches!(
                    invocation.builtin_name,
                    Some(ToolName::ListChangedFiles | ToolName::ReadDiff | ToolName::ListFiles)
                ) =>
            {
                Some(stable_id(&[
                    &self.snapshot.snapshot_id.0,
                    invocation.tool_id.as_str(),
                    invocation.scope_key.as_str(),
                    &CONCURRENT_CONTRACT_VERSION.to_string(),
                    &REDACTION_POLICY_VERSION.to_string(),
                ]))
            }
            ToolArgs::ReadFile { path } => Some(stable_id(&[
                &self.snapshot.snapshot_id.0,
                invocation.tool_id.as_str(),
                invocation.scope_key.as_str(),
                &path.display(),
                &CONCURRENT_CONTRACT_VERSION.to_string(),
                &REDACTION_POLICY_VERSION.to_string(),
            ])),
            ToolArgs::SearchText { query } => Some(stable_id(&[
                &self.snapshot.snapshot_id.0,
                invocation.tool_id.as_str(),
                invocation.scope_key.as_str(),
                query,
                &self.limits.max_search_matches.to_string(),
                &CONCURRENT_CONTRACT_VERSION.to_string(),
                &REDACTION_POLICY_VERSION.to_string(),
            ])),
            ToolArgs::Raw(raw) => self
                .registry
                .definition(&invocation.tool_id)
                .filter(|definition| definition.cacheable)
                .map(|_| {
                    stable_id(&[
                        &self.snapshot.snapshot_id.0,
                        invocation.tool_id.as_str(),
                        invocation.scope_key.as_str(),
                        &raw.to_string(),
                        &CONCURRENT_CONTRACT_VERSION.to_string(),
                        &REDACTION_POLICY_VERSION.to_string(),
                    ])
                }),
            _ => None,
        }
    }

    fn read_diff(&self, call_id: ToolCallId, cache_status: CacheStatus) -> ToolResultEnvelope {
        let content = self.redactor.redact(&self.snapshot.diff.content);
        let artifact_id = self.artifacts.insert_views(
            ArtifactKey(stable_id(&[
                &self.snapshot.snapshot_id.0,
                "read_diff",
                &self.snapshot.diff.content_hash,
            ])),
            self.snapshot.diff.content.clone(),
            content.clone(),
        );
        ToolResultEnvelope {
            ok: true,
            tool_call_id: call_id,
            tool_name: ToolId::from(ToolName::ReadDiff),
            provider_id: ToolProviderId::builtin_review(),
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

    fn list_changed_files(
        &self,
        call_id: ToolCallId,
        fs_scope: &FsScope,
        scope_key: &ScopeKey,
        cache_status: CacheStatus,
    ) -> ToolResultEnvelope {
        let files = self
            .snapshot
            .manifest
            .changed_file_entries
            .iter()
            .filter(|file| fs_scope.allows(&file.rel_path))
            .map(|file| file.summary.clone())
            .collect::<Vec<_>>();
        let content = files.join("\n");
        let artifact_id = self.artifacts.insert(
            ArtifactKey(stable_id(&[
                &self.snapshot.snapshot_id.0,
                "list_changed_files",
                scope_key.as_str(),
            ])),
            content,
        );
        ToolResultEnvelope {
            ok: true,
            tool_call_id: call_id,
            tool_name: ToolId::from(ToolName::ListChangedFiles),
            provider_id: ToolProviderId::builtin_review(),
            snapshot_id: self.snapshot.snapshot_id.clone(),
            artifact_id: Some(artifact_id),
            cache: CacheInfo {
                status: cache_status,
                key_hash: None,
            },
            limits: LimitInfo {
                output_bytes: files.iter().map(String::len).sum(),
                ..LimitInfo::default()
            },
            data: Some(json!({
                "changedFiles": files,
            })),
            error: None,
        }
    }

    fn list_files(
        &self,
        call_id: ToolCallId,
        fs_scope: &FsScope,
        scope_key: &ScopeKey,
        cache_status: CacheStatus,
    ) -> ToolResultEnvelope {
        let mut files = self
            .snapshot
            .manifest
            .files
            .iter()
            .filter(|file| file.is_text_candidate && fs_scope.allows(&file.rel_path))
            .map(|file| file.rel_path.display())
            .collect::<Vec<_>>();
        files.sort();
        let content = files
            .iter()
            .take(300)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        let artifact_id = self.artifacts.insert(
            ArtifactKey(stable_id(&[
                &self.snapshot.snapshot_id.0,
                "list_files",
                scope_key.as_str(),
            ])),
            content,
        );
        ToolResultEnvelope {
            ok: true,
            tool_call_id: call_id,
            tool_name: ToolId::from(ToolName::ListFiles),
            provider_id: ToolProviderId::builtin_review(),
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

    fn base_snapshot_unavailable(
        &self,
        call_id: ToolCallId,
        path: RepoPath,
        cache_status: CacheStatus,
    ) -> ToolResultEnvelope {
        let content = format!("base snapshot unavailable for {}", path.display());
        let artifact_id = self.artifacts.insert(
            ArtifactKey(stable_id(&[
                &self.snapshot.snapshot_id.0,
                "read_base_file",
                &path.display(),
            ])),
            content.clone(),
        );
        ToolResultEnvelope {
            ok: true,
            tool_call_id: call_id,
            tool_name: ToolId::from(ToolName::ReadBaseFile),
            provider_id: ToolProviderId::builtin_review(),
            snapshot_id: self.snapshot.snapshot_id.clone(),
            artifact_id: Some(artifact_id),
            cache: CacheInfo {
                status: cache_status,
                key_hash: None,
            },
            limits: LimitInfo {
                output_bytes: content.len(),
                ..LimitInfo::default()
            },
            data: Some(json!({
                "path": path.display(),
                "available": false,
                "errorCode": "BASE_SNAPSHOT_UNAVAILABLE",
                "message": content,
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
                tool_name.into(),
                ToolErrorCode::PathDenied,
                "path is not present in the repo manifest",
                false,
            );
        };
        let Ok(_permit) = self.read_permits.clone().acquire_owned().await else {
            return self.error_result(
                call_id,
                tool_name.into(),
                ToolErrorCode::Internal,
                "read semaphore closed",
                false,
            );
        };
        match self.read.read_file(&file).await {
            Ok(read) => {
                let content = self.redactor.redact(&read.content);
                let line_count = read.content.lines().count().max(1);
                let tool_id = ToolId::from(tool_name);
                let artifact_id = self.artifacts.insert_views(
                    ArtifactKey(stable_id(&[
                        &self.snapshot.snapshot_id.0,
                        tool_name.as_str(),
                        &file.fingerprint,
                        &path.display(),
                    ])),
                    read.content.clone(),
                    content.clone(),
                );
                ToolResultEnvelope {
                    ok: true,
                    tool_call_id: call_id,
                    tool_name: tool_id,
                    provider_id: ToolProviderId::builtin_review(),
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
                        "lineRange": {
                            "startLine": 1,
                            "endLine": line_count,
                        },
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
            Err(error) => self.runtime_error_result(call_id, tool_name.into(), error),
        }
    }

    fn find_files_for_path(
        &self,
        call_id: ToolCallId,
        tool_name: ToolName,
        path: RepoPath,
        fs_scope: &FsScope,
        scope_key: &ScopeKey,
        cache_status: CacheStatus,
    ) -> ToolResultEnvelope {
        let stem = path
            .as_path()
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_string();
        let mut files = self
            .snapshot
            .manifest
            .files
            .iter()
            .filter(|file| file.is_text_candidate && fs_scope.allows(&file.rel_path))
            .filter(|file| {
                if tool_name == ToolName::FindRelatedFiles {
                    file.rel_path.as_path() != path.as_path()
                        && !stem.is_empty()
                        && file.rel_path.display().contains(&stem)
                } else {
                    let text = file.rel_path.display();
                    (text.contains("test") || text.contains("spec"))
                        && (stem.is_empty() || text.contains(&stem))
                }
            })
            .map(|file| file.rel_path.display())
            .take(80)
            .collect::<Vec<_>>();
        files.sort();
        let truncated = files.len() >= 80;
        let content = files.join("\n");
        let artifact_id = self.artifacts.insert(
            ArtifactKey(stable_id(&[
                &self.snapshot.snapshot_id.0,
                tool_name.as_str(),
                scope_key.as_str(),
                &path.display(),
            ])),
            content,
        );
        ToolResultEnvelope {
            ok: true,
            tool_call_id: call_id,
            tool_name: ToolId::from(tool_name),
            provider_id: ToolProviderId::builtin_review(),
            snapshot_id: self.snapshot.snapshot_id.clone(),
            artifact_id: Some(artifact_id),
            cache: CacheInfo {
                status: cache_status,
                key_hash: Some(stable_id(&[&path.display()])),
            },
            limits: LimitInfo {
                truncated,
                output_bytes: files.iter().map(String::len).sum(),
                ..LimitInfo::default()
            },
            data: Some(json!({
                "path": path.display(),
                "files": files,
            })),
            error: None,
        }
    }

    async fn list_imports(
        &self,
        call_id: ToolCallId,
        path: RepoPath,
        cache_status: CacheStatus,
    ) -> ToolResultEnvelope {
        let Ok(file) = self.snapshot.lookup(&path).cloned() else {
            return self.error_result(
                call_id,
                ToolName::ListImports.into(),
                ToolErrorCode::PathDenied,
                "path is not present in the repo manifest",
                false,
            );
        };
        let Ok(_permit) = self.read_permits.clone().acquire_owned().await else {
            return self.error_result(
                call_id,
                ToolName::ListImports.into(),
                ToolErrorCode::Internal,
                "read semaphore closed",
                false,
            );
        };
        match self.read.read_file(&file).await {
            Ok(read) => {
                let imports = read
                    .content
                    .lines()
                    .enumerate()
                    .filter(|(_, line)| {
                        let trimmed = line.trim_start();
                        trimmed.starts_with("use ")
                            || trimmed.starts_with("import ")
                            || trimmed.starts_with("export ")
                            || trimmed.starts_with("require(")
                            || trimmed.starts_with("from ")
                    })
                    .map(|(index, line)| {
                        format!("{}:{}:{}", path.display(), index + 1, line.trim())
                    })
                    .collect::<Vec<_>>();
                let raw_content = imports.join("\n");
                let content = self.redactor.redact(&raw_content);
                let artifact_id = self.artifacts.insert_views(
                    ArtifactKey(stable_id(&[
                        &self.snapshot.snapshot_id.0,
                        "list_imports",
                        &file.fingerprint,
                        &path.display(),
                    ])),
                    raw_content,
                    content,
                );
                ToolResultEnvelope {
                    ok: true,
                    tool_call_id: call_id,
                    tool_name: ToolId::from(ToolName::ListImports),
                    provider_id: ToolProviderId::builtin_review(),
                    snapshot_id: self.snapshot.snapshot_id.clone(),
                    artifact_id: Some(artifact_id),
                    cache: CacheInfo {
                        status: cache_status,
                        key_hash: Some(file.fingerprint.clone()),
                    },
                    limits: LimitInfo {
                        truncated: read.truncated,
                        output_bytes: imports.iter().map(String::len).sum(),
                        ..LimitInfo::default()
                    },
                    data: Some(json!({
                        "path": path.display(),
                        "imports": imports,
                    })),
                    error: None,
                }
            }
            Err(error) => self.runtime_error_result(call_id, ToolName::ListImports.into(), error),
        }
    }

    async fn search_text(
        &self,
        call_id: ToolCallId,
        query: String,
        fs_scope: &FsScope,
        scope_key: &ScopeKey,
        cancel: CancellationToken,
        cache_status: CacheStatus,
    ) -> ToolResultEnvelope {
        match self.search.search(query, fs_scope, scope_key, cancel).await {
            Ok(mut result) => {
                result.tool_call_id = call_id;
                result.cache.status = cache_status;
                result
            }
            Err(error) => self.runtime_error_result(call_id, ToolName::SearchText.into(), error),
        }
    }

    fn challenge_finding(
        &self,
        call_id: ToolCallId,
        finding_id: String,
        rationale: String,
    ) -> ToolResultEnvelope {
        let content = format!("{finding_id}: {rationale}");
        let artifact_id = self.artifacts.insert_views(
            ArtifactKey(stable_id(&[
                &self.snapshot.snapshot_id.0,
                "challenge_finding",
                &finding_id,
                &rationale,
            ])),
            content.clone(),
            self.redactor.redact(&content),
        );
        ToolResultEnvelope {
            ok: true,
            tool_call_id: call_id,
            tool_name: ToolId::from(ToolName::ChallengeFinding),
            provider_id: ToolProviderId::builtin_review(),
            snapshot_id: self.snapshot.snapshot_id.clone(),
            artifact_id: Some(artifact_id),
            cache: CacheInfo {
                status: CacheStatus::NotCacheable,
                key_hash: None,
            },
            limits: LimitInfo {
                output_bytes: content.len(),
                ..LimitInfo::default()
            },
            data: Some(json!({
                "findingId": finding_id,
                "rationale": rationale,
            })),
            error: None,
        }
    }

    fn record_finding(
        &self,
        call_id: ToolCallId,
        title: String,
        claim: String,
    ) -> ToolResultEnvelope {
        let finding_id = finding_id_for_call(&call_id, &title, &claim);
        ToolResultEnvelope {
            ok: true,
            tool_call_id: call_id,
            tool_name: ToolId::from(ToolName::RecordFinding),
            provider_id: ToolProviderId::builtin_review(),
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

    fn evidence_refs(
        &self,
        results: &[ToolResultEnvelope],
        revision_id: &str,
    ) -> Vec<EvidenceRefV1> {
        results
            .iter()
            .filter(|result| result.ok)
            .filter_map(|result| {
                let tool = result.tool_name.as_builtin()?;
                let kind = artifact_kind_for_tool(tool)?;
                let artifact_id = result.artifact_id.as_ref()?;
                let artifact = self.artifacts.get(artifact_id)?;
                Some(EvidenceRefV1 {
                    evidence_id: format!("evidence-{}", artifact_id.0),
                    artifact_id: artifact_id.0.clone(),
                    kind,
                    revision: evidence_revision_for_tool(tool),
                    revision_id: revision_id.to_string(),
                    location: evidence_location(result, &self.snapshot),
                    line_range: evidence_line_range(result),
                    byte_range: Some(ByteRangeV1 {
                        start_byte: 0,
                        end_byte: artifact.bytes,
                    }),
                    diff_anchor: None,
                    content_hash: artifact.content_hash,
                    redaction: redaction_none(),
                    producing_tool_call_id: result.tool_call_id.0.clone(),
                })
            })
            .collect()
    }

    fn finish(&self, call_id: ToolCallId, reason: String) -> ToolResultEnvelope {
        ToolResultEnvelope {
            ok: true,
            tool_call_id: call_id,
            tool_name: ToolId::from(ToolName::Finish),
            provider_id: ToolProviderId::builtin_review(),
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

    pub(super) async fn execute_in_process_tool(
        &self,
        invocation: ToolInvocation,
        cancel: CancellationToken,
        cache_status: CacheStatus,
    ) -> ToolResultEnvelope {
        let call_id = invocation.call_id.clone();
        let tool_id = invocation.tool_id.clone();
        let Some(definition) = self.registry.definition(&invocation.tool_id).cloned() else {
            return self.error_result(
                call_id,
                tool_id,
                ToolErrorCode::UnknownTool,
                "tool is not registered",
                false,
            );
        };
        let provider_id = definition.provider_id.clone();
        let Some(handler) = definition.handler else {
            return self.error_result(
                call_id,
                tool_id,
                ToolErrorCode::UnknownTool,
                "tool has no handler",
                false,
            );
        };
        let ToolArgs::Raw(args) = invocation.args else {
            return self.error_result(
                call_id,
                tool_id,
                ToolErrorCode::InvalidArgs,
                "custom tool requires raw JSON arguments",
                false,
            );
        };
        let context = CustomToolContext {
            session_id: invocation.session_id,
            turn_id: invocation.turn_id,
            call_id: call_id.clone(),
            tool_id: tool_id.clone(),
            snapshot_id: self.snapshot.snapshot_id.clone(),
            provider_resources: definition.provider_resources.clone(),
        };
        let output =
            match tokio::spawn(async move { handler.execute(context, args, cancel).await }).await {
                Ok(Ok(output)) => output,
                Ok(Err(error)) => return self.runtime_error_result(call_id, tool_id, error),
                Err(error) if error.is_panic() => {
                    return self.error_result(
                        call_id,
                        tool_id,
                        ToolErrorCode::Internal,
                        "in-process tool provider panicked",
                        false,
                    )
                }
                Err(_) => {
                    return self.error_result(
                        call_id,
                        tool_id,
                        ToolErrorCode::Internal,
                        "in-process tool provider task failed",
                        false,
                    )
                }
            };
        self.provider_output_result(
            call_id,
            tool_id,
            provider_id,
            cache_status,
            &invocation.capabilities,
            output,
        )
    }

    pub(super) fn provider_output_result(
        &self,
        call_id: ToolCallId,
        tool_id: ToolId,
        provider_id: ToolProviderId,
        cache_status: CacheStatus,
        capabilities: &CapabilitySet,
        output: CustomToolOutput,
    ) -> ToolResultEnvelope {
        let mut limits = output.limits;
        let data = output.data.map(|data| self.redactor.redact_value(data));
        let data_bytes = data
            .as_ref()
            .and_then(|data| serde_json::to_vec(data).ok())
            .map_or(0, |bytes| bytes.len());
        let prepared_artifact = match output.artifact {
            Some(artifact) => {
                if !capabilities.artifact_access.write {
                    return self.error_result(
                        call_id,
                        tool_id,
                        ToolErrorCode::ToolNotAllowed,
                        "tool artifact write is not allowed for this session",
                        false,
                    );
                }
                if artifact.content.len() > self.limits.max_tool_artifact_bytes {
                    return self.error_result(
                        call_id,
                        tool_id,
                        ToolErrorCode::TooLarge,
                        "tool artifact exceeds runtime artifact limit",
                        false,
                    );
                }
                let raw_content = artifact.content;
                let redacted_content = self.redactor.redact(&raw_content);
                Some((artifact.key, raw_content, redacted_content))
            }
            None => None,
        };
        let artifact_bytes = prepared_artifact
            .as_ref()
            .map_or(0, |(_, _, redacted_content)| redacted_content.len());
        let output_bytes = data_bytes.saturating_add(artifact_bytes);
        if output_bytes > self.limits.max_tool_output_bytes {
            return self.error_result(
                call_id,
                tool_id,
                ToolErrorCode::TooLarge,
                "tool output exceeds runtime output limit",
                false,
            );
        }
        let artifact_id = prepared_artifact.map(|(key, raw_content, redacted_content)| {
            self.artifacts
                .insert_views(key, raw_content, redacted_content)
        });
        limits.output_bytes = output_bytes;
        ToolResultEnvelope {
            ok: true,
            tool_call_id: call_id,
            tool_name: tool_id,
            provider_id,
            snapshot_id: self.snapshot.snapshot_id.clone(),
            artifact_id,
            cache: CacheInfo {
                status: cache_status,
                key_hash: None,
            },
            limits,
            data,
            error: None,
        }
    }

    pub(crate) fn error_result(
        &self,
        call_id: ToolCallId,
        tool_name: ToolId,
        code: ToolErrorCode,
        message: &str,
        retryable: bool,
    ) -> ToolResultEnvelope {
        self.counters.tool_errors.fetch_add(1, Ordering::Relaxed);
        ToolResultEnvelope {
            ok: false,
            tool_call_id: call_id,
            provider_id: self.provider_id_for_tool(&tool_name),
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
        tool_name: ToolId,
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
            RuntimeError::SnapshotStale { .. } => self.error_result(
                call_id,
                tool_name,
                ToolErrorCode::SnapshotStale,
                "snapshot content changed after capture",
                false,
            ),
            RuntimeError::Cancelled => self.error_result(
                call_id,
                tool_name,
                ToolErrorCode::Cancelled,
                "operation cancelled",
                true,
            ),
            RuntimeError::Timeout => self.error_result(
                call_id,
                tool_name,
                ToolErrorCode::Timeout,
                "tool provider timed out",
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

    fn provider_id_for_tool(&self, tool_name: &ToolId) -> ToolProviderId {
        self.registry
            .definition(tool_name)
            .map(|definition| definition.provider_id.clone())
            .unwrap_or_else(ToolProviderId::runtime)
    }

    pub(crate) fn snapshot_counters(&self) -> ConcurrentCounters {
        self.counters.snapshot()
    }

    pub(crate) fn snapshot_tool_metrics(&self) -> BTreeMap<ToolMetricKey, ToolMetricsSnapshot> {
        self.metrics.snapshot()
    }

    pub(crate) fn snapshot_provider_health(&self) -> Vec<ToolProviderHealthSnapshot> {
        self.metrics.provider_health_snapshot()
    }

    pub(crate) fn record_tool_metrics(&self, results: &[ToolResultEnvelope]) {
        self.record_batch_metrics(results);
    }

    #[cfg(test)]
    pub(crate) fn inflight_tool_count_for_test(&self) -> usize {
        self.inflight.lock().len()
    }

    fn record_batch_metrics(&self, results: &[ToolResultEnvelope]) {
        for result in results {
            self.metrics.record(result);
        }
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    (started.elapsed().as_micros().div_ceil(1000) as u64).max(1)
}

fn elapsed_ms_allow_zero(started: Instant) -> u64 {
    started.elapsed().as_micros().div_ceil(1000) as u64
}

fn validation_error_message(code: ToolErrorCode) -> &'static str {
    match code {
        ToolErrorCode::ToolNotAllowed => "tool is not allowed for this session",
        ToolErrorCode::PathDenied => "path is outside the session filesystem scope",
        ToolErrorCode::TooLarge => "tool arguments exceed this session input capability",
        ToolErrorCode::UnknownTool => "tool is not registered",
        ToolErrorCode::InvalidArgs => "invalid tool invocation",
        _ => "tool invocation failed validation",
    }
}
