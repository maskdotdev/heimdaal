use std::collections::BTreeMap;
use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use crate::contracts::{AgentBudget, Role, TokenUsage, ToolCounts};
use crate::runtime::contracts::{
    ArtifactId, ArtifactKey, ArtifactView, CapabilitySet, ConcurrentCounters, ConcurrentRunReport,
    ConversationItem, FsScope, LimitInfo, ModelCostEstimate, ModelMetricsSnapshot, ModelToolCall,
    ModelTurn, ProviderResourceId, ProviderResourceScope, RepoPath, RuntimeError, RuntimeEvent,
    RuntimeEventContext, RuntimeEventSink, RuntimeLimits, RuntimeResult, SessionId, SessionScope,
    SnapshotCaptureStatus, SnapshotId, SnapshotObjectStore, SnapshotStorageMode,
    SnapshotStoragePolicy, ToolCallId, ToolEffects, ToolErrorCode, ToolGrant, ToolId,
    ToolMetricKey, ToolMetricsSnapshot, ToolProviderHealthSnapshot, ToolProviderHealthState,
    ToolProviderId, TurnId,
};
use crate::runtime::dispatch::RuntimeEventDispatcher;
use crate::runtime::model::{
    openai_provider_canary_protocols, ConcurrentModelClient as RuntimeModelClient,
    ConcurrentModelRouter as RuntimeModelRouter, EnvCredentialResolver, ModelLimiter,
    ModelProviderCanaryEvidence, ModelProviderCanaryGate, ModelProviderCanaryStatus,
    ProfileModelRouter, StaticModelRouter,
};
use crate::runtime::tools::{
    ConcurrentArtifactStore as RuntimeArtifactStore, CustomToolArtifact, CustomToolContext,
    CustomToolHandler, CustomToolOptions, CustomToolOutput, JsonRpcToolRegistration,
    JsonRpcToolTransport, ToolRegistry as RuntimeToolRegistry,
};
pub use tokio_util::sync::CancellationToken as Cancellation;

use crate::contracts::{
    ChangeKind as ContractChangeKind, ChangeScopeV1, ChangedFileEntryV1,
    ChangedFileStatus as ContractChangedFileStatus, EventLevel, EventType, FindingV1,
    ModelApiProtocol, PathPolicyV1, Publishability, RenameDetection as ContractRenameDetection,
    ReviewOutcomeV1, ReviewRunJobV1, ReviewRunResultV1, ReviewRuntimeV1,
    SnapshotMode as ContractSnapshotMode, ToolMask, ToolName,
};
use crate::events::{EventEmitter, EventRecord};
use crate::job::{effective_personas, tool_allowed, validate_job};
use crate::runtime::contracts::stable_id;
use crate::runtime::job_runtime::{
    benchmark_failures as runtime_benchmark_failures, JobRuntime, SessionSpec,
};
pub use crate::runtime::policy::ReviewerPolicy;
use crate::runtime::repo::{remote_content_addressed_uri, RepoSnapshot, SnapshotContentRef};
use crate::runtime::tools::ToolEngine;
use crate::util::{timestamp_utc, SCHEMA_VERSION};

pub mod model_adapters {
    pub use crate::runtime::contracts::{ModelCostEstimate, ModelMetricsSnapshot};
    pub use crate::runtime::model::{
        ConcurrentModelClient as ModelClient, ConcurrentModelRouter as ModelRouter,
        CredentialResolver, EnvCredentialResolver, ModelLimiter, StaticModelRouter,
    };
}

pub mod tool_adapters {
    pub use crate::runtime::contracts::{
        ProviderResourceId, ProviderResourceScope, ToolErrorCode, ToolErrorInfo, ToolMetricKey,
        ToolMetricsSnapshot, ToolProviderHealthSnapshot, ToolProviderHealthState, ToolProviderId,
    };
    pub use crate::runtime::tools::ConcurrentArtifactStore as ArtifactStore;
    pub use crate::runtime::tools::{
        CustomToolArtifact, CustomToolContext, CustomToolHandler, CustomToolOptions,
        CustomToolOutput, HttpJsonRpcToolTransport, JsonRpcToolRegistration, JsonRpcToolRequest,
        JsonRpcToolResponse, JsonRpcToolTransport, ToolAliasTable, ToolDefinition, ToolRegistry,
        ToolSchema,
    };
}

pub mod capabilities {
    pub use crate::runtime::contracts::{
        ArtifactAccessPolicy, CapabilitySet, FsScope, ModelOutputPolicy, RuntimeAuthorityPolicy,
        ScopeKey, ToolEffects, ToolGrant, ToolInputPolicy,
    };
}

pub mod metrics {
    pub use crate::runtime::contracts::{
        CacheInfo, CacheStatus, ConcurrentCounters, ConcurrentRunReport, LimitInfo,
        SnapshotMetricsSnapshot,
    };
}

pub mod ids {
    pub use crate::runtime::contracts::{
        ArtifactId, EvidenceId, SessionId, SnapshotId, ToolCallId, ToolId,
    };
}

pub mod artifacts {
    pub use crate::runtime::contracts::{ArtifactId, ArtifactKey, ArtifactView, EvidenceId};
}

pub mod paths {
    pub use crate::runtime::contracts::RepoPath;
}

pub mod storage {
    pub use crate::runtime::contracts::{
        SnapshotCaptureStatus, SnapshotObjectStore, SnapshotStorageMode, SnapshotStoragePolicy,
    };

    pub use super::{
        export_remote_object_store_canary_evidence, run_remote_artifact_object_store_canary,
        run_remote_snapshot_object_store_canary, HttpRemoteObjectClient,
        RemoteObjectStoreCanaryEvidence, RemoteObjectStoreCanaryEvidenceExport,
        RemoteObjectStoreCanaryGate, RemoteObjectStoreCanaryStatus, RemoteObjectStoreCanaryStep,
        RemoteObjectStoreCanaryStepKind, RemoteObjectStoreCanaryTarget,
        REMOTE_OBJECT_STORE_CANARY_SCHEMA_VERSION,
    };
}

pub mod canaries {
    pub use crate::contracts::ModelApiProtocol;
    pub use crate::runtime::model::{
        export_model_provider_canary_evidence, load_model_provider_canary_evidence,
        openai_provider_canary_protocols, run_openai_provider_canaries, CredentialResolver,
        EnvCredentialResolver, ModelProviderCanaryEvidence, ModelProviderCanaryEvidenceExport,
        ModelProviderCanaryGate, ModelProviderCanaryReport, ModelProviderCanaryStatus,
        OpenAiProviderCanaryConfig, MODEL_PROVIDER_CANARY_EVIDENCE_SCHEMA_VERSION,
    };

    pub use super::{
        export_canary_evidence_manifest, export_remote_object_store_canary_evidence,
        load_canary_evidence_manifest, load_remote_object_store_canary_evidence,
        run_remote_artifact_object_store_canary, run_remote_snapshot_object_store_canary,
        CanaryEvidenceFreshnessPolicy, CanaryEvidenceGate, CanaryEvidenceManifest,
        CanaryEvidenceManifestExport, CanaryEvidenceStatusReport, CanaryEvidenceStatusSummary,
        CanaryModelProviderEvidenceStatus, CanaryRemoteObjectStoreEvidenceStatus,
        RemoteObjectStoreCanaryEvidence, RemoteObjectStoreCanaryEvidenceExport,
        RemoteObjectStoreCanaryGate, RemoteObjectStoreCanaryStatus, RemoteObjectStoreCanaryStep,
        RemoteObjectStoreCanaryStepKind, RemoteObjectStoreCanaryTarget,
        CANARY_EVIDENCE_MANIFEST_SCHEMA_VERSION, REMOTE_OBJECT_STORE_CANARY_SCHEMA_VERSION,
    };
}

pub mod runtime {
    pub use crate::runtime::contracts::{RuntimeError, RuntimeLimits, RuntimeResult};
}

#[derive(Debug, Clone)]
pub struct RunSpec {
    pub run_id: String,
    pub snapshots: Vec<SnapshotSpec>,
    pub sessions: Vec<ReviewSessionSpec>,
    pub limits: ReviewRunLimits,
}

impl RunSpec {
    pub fn single_snapshot(
        run_id: impl Into<String>,
        snapshot: SnapshotSpec,
        sessions: Vec<ReviewSessionSpec>,
        limits: impl Into<ReviewRunLimits>,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            snapshots: vec![snapshot],
            sessions,
            limits: limits.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewRunLimits {
    limits: RuntimeLimits,
}

impl ReviewRunLimits {
    pub fn standard(sessions: usize, max_file_bytes: usize, max_search_matches: usize) -> Self {
        Self {
            limits: RuntimeLimits::standard(sessions, max_file_bytes, max_search_matches),
        }
    }

    pub fn from_runtime_limits(limits: runtime::RuntimeLimits) -> Self {
        Self { limits }
    }

    pub fn as_runtime_limits(&self) -> &runtime::RuntimeLimits {
        &self.limits
    }

    fn into_runtime_limits(self) -> RuntimeLimits {
        self.limits
    }
}

impl From<RuntimeLimits> for ReviewRunLimits {
    fn from(value: RuntimeLimits) -> Self {
        Self::from_runtime_limits(value)
    }
}

#[derive(Debug, Clone)]
pub struct ReviewSessionSpec {
    id: SessionId,
    role: Role,
    objective: String,
    snapshot_id: Option<SnapshotId>,
    model_profile_id: Option<String>,
    capabilities: CapabilitySet,
    budget: AgentBudget,
}

impl ReviewSessionSpec {
    pub fn review_read_only(
        id: impl Into<String>,
        role: Role,
        objective: impl Into<String>,
        budget: AgentBudget,
    ) -> Self {
        Self {
            id: SessionId(id.into()),
            role,
            objective: objective.into(),
            snapshot_id: None,
            model_profile_id: None,
            capabilities: CapabilitySet::review_read_only(),
            budget,
        }
    }

    pub fn with_snapshot_id(mut self, snapshot_id: SnapshotId) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    pub fn with_model_profile_id(mut self, model_profile_id: impl Into<String>) -> Self {
        self.model_profile_id = Some(model_profile_id.into());
        self
    }

    pub fn with_capabilities(mut self, capabilities: capabilities::CapabilitySet) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn deny_tool(mut self, tool_id: ToolId) -> Self {
        self.capabilities.tool_grants.remove(&tool_id);
        self
    }

    pub fn grant_custom_read_only_tool(mut self, tool_id: ToolId) -> Self {
        self.capabilities.runtime_authority.host_read = true;
        self.capabilities
            .grant_tool(tool_id, ToolGrant::allow_custom_read_only());
        self
    }

    pub fn grant_provider_read_only_tool(
        mut self,
        provider_id: ToolProviderId,
        tool_id: ToolId,
    ) -> Self {
        allow_runtime_provider(&mut self.capabilities, provider_id);
        self.capabilities.grant_tool(
            tool_id,
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::review_read_only(),
            },
        );
        self
    }

    pub fn grant_provider_read_only_tool_for_resources(
        mut self,
        provider_id: ToolProviderId,
        tool_id: ToolId,
        provider_resources: Vec<ProviderResourceId>,
    ) -> Self {
        allow_runtime_provider(&mut self.capabilities, provider_id.clone());
        allow_runtime_provider_resources(&mut self.capabilities, provider_id, provider_resources);
        self.capabilities.grant_tool(
            tool_id,
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::review_read_only(),
            },
        );
        self
    }

    pub fn grant_provider_network_read_tool(
        mut self,
        provider_id: ToolProviderId,
        tool_id: ToolId,
    ) -> Self {
        self.capabilities.runtime_authority.network_read = true;
        allow_runtime_provider(&mut self.capabilities, provider_id);
        self.capabilities.grant_tool(
            tool_id,
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: provider_network_read_effects(),
            },
        );
        self
    }

    pub fn grant_provider_network_read_tool_for_resources(
        mut self,
        provider_id: ToolProviderId,
        tool_id: ToolId,
        provider_resources: Vec<ProviderResourceId>,
    ) -> Self {
        self.capabilities.runtime_authority.network_read = true;
        allow_runtime_provider(&mut self.capabilities, provider_id.clone());
        allow_runtime_provider_resources(&mut self.capabilities, provider_id, provider_resources);
        self.capabilities.grant_tool(
            tool_id,
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: provider_network_read_effects(),
            },
        );
        self
    }

    pub fn grant_custom_read_only_tool_for_resources(
        mut self,
        tool_id: ToolId,
        provider_resources: Vec<ProviderResourceId>,
    ) -> Self {
        self.capabilities.runtime_authority.host_read = true;
        let scopes = provider_resources
            .into_iter()
            .map(|resource_id| {
                ProviderResourceScope::new(ToolProviderId::in_process(), resource_id)
            })
            .collect::<Vec<_>>();
        match &mut self
            .capabilities
            .runtime_authority
            .allowed_provider_resources
        {
            Some(existing) => existing.extend(scopes),
            None => {
                self.capabilities
                    .runtime_authority
                    .allowed_provider_resources = Some(scopes)
            }
        }
        self.capabilities
            .grant_tool(tool_id, ToolGrant::allow_custom_read_only());
        self
    }

    fn into_session_scope(self) -> SessionScope {
        SessionScope {
            id: self.id,
            role: self.role,
            objective: self.objective,
            snapshot_id: self.snapshot_id,
            model_profile_id: self.model_profile_id,
            capabilities: self.capabilities,
            budget: self.budget,
        }
    }
}

impl From<SessionScope> for ReviewSessionSpec {
    fn from(value: SessionScope) -> Self {
        Self {
            id: value.id,
            role: value.role,
            objective: value.objective,
            snapshot_id: value.snapshot_id,
            model_profile_id: value.model_profile_id,
            capabilities: value.capabilities,
            budget: value.budget,
        }
    }
}

fn allow_runtime_provider(capabilities: &mut CapabilitySet, provider_id: ToolProviderId) {
    let providers = capabilities
        .runtime_authority
        .allowed_provider_ids
        .get_or_insert_with(|| {
            vec![
                ToolProviderId::builtin_review(),
                ToolProviderId::in_process(),
            ]
        });
    if !providers.iter().any(|allowed| allowed == &provider_id) {
        providers.push(provider_id);
    }
}

fn allow_runtime_provider_resources(
    capabilities: &mut CapabilitySet,
    provider_id: ToolProviderId,
    provider_resources: Vec<ProviderResourceId>,
) {
    let resources = capabilities
        .runtime_authority
        .allowed_provider_resources
        .get_or_insert_with(Vec::new);
    for resource_id in provider_resources {
        let scope = ProviderResourceScope::new(provider_id.clone(), resource_id);
        if !resources.iter().any(|allowed| allowed == &scope) {
            resources.push(scope);
        }
    }
}

fn provider_network_read_effects() -> ToolEffects {
    ToolEffects {
        network_read: true,
        ..ToolEffects::review_read_only()
    }
}

#[async_trait]
pub trait ReviewModel: Send + Sync {
    async fn complete_review(
        &self,
        request: ReviewModelRequest,
        cancel: Cancellation,
    ) -> RuntimeResult<ReviewModelTurn>;

    fn estimate_cost(&self, _usage: &TokenUsage) -> Option<ModelCostEstimate> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct ReviewModelRequest {
    pub session_id: String,
    pub role: Role,
    pub objective: String,
    pub snapshot_id: Option<SnapshotId>,
    pub model_profile_id: Option<String>,
    pub turn: u32,
    pub transcript: Vec<ReviewTranscriptItem>,
}

impl ReviewModelRequest {
    fn from_runtime(
        scope: &SessionScope,
        transcript: &[ConversationItem],
        turn_id: TurnId,
    ) -> Self {
        Self {
            session_id: scope.id.0.clone(),
            role: scope.role,
            objective: scope.objective.clone(),
            snapshot_id: scope.snapshot_id.clone(),
            model_profile_id: scope.model_profile_id.clone(),
            turn: turn_id.0,
            transcript: transcript
                .iter()
                .map(ReviewTranscriptItem::from_conversation_item)
                .collect(),
        }
    }

    pub fn transcript_item_count(&self) -> usize {
        self.transcript.len()
    }

    pub fn tool_result_count(&self) -> usize {
        self.transcript
            .iter()
            .filter(|item| matches!(item, ReviewTranscriptItem::ToolResult { .. }))
            .count()
    }

    pub fn tool_call_id(&self, suffix: impl AsRef<str>) -> String {
        format!("{}-{}-{}", self.session_id, self.turn, suffix.as_ref())
    }
}

#[derive(Debug, Clone)]
pub enum ReviewTranscriptItem {
    System {
        content: String,
    },
    User {
        content: String,
    },
    AssistantText {
        content: String,
    },
    AssistantToolCalls {
        calls: Vec<ReviewToolCall>,
    },
    ToolResult {
        call_id: String,
        tool_id: String,
        ok: bool,
        artifact_id: Option<ArtifactId>,
        data: Option<serde_json::Value>,
        error_code: Option<ToolErrorCode>,
    },
}

impl ReviewTranscriptItem {
    fn from_conversation_item(item: &ConversationItem) -> Self {
        match item {
            ConversationItem::System { content } => Self::System {
                content: content.clone(),
            },
            ConversationItem::User { content } => Self::User {
                content: content.clone(),
            },
            ConversationItem::AssistantText { content } => Self::AssistantText {
                content: content.clone(),
            },
            ConversationItem::AssistantToolCalls { calls } => Self::AssistantToolCalls {
                calls: calls
                    .iter()
                    .map(ReviewToolCall::from_model_tool_call)
                    .collect(),
            },
            ConversationItem::ToolResult {
                call_id,
                name,
                content,
            } => Self::ToolResult {
                call_id: call_id.0.clone(),
                tool_id: name.as_str().to_string(),
                ok: content.ok,
                artifact_id: content.artifact_id.clone(),
                data: content.data.clone(),
                error_code: content.error.as_ref().map(|error| error.code),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReviewToolCall {
    pub call_id: Option<String>,
    pub tool_id: String,
    pub arguments: serde_json::Value,
}

impl ReviewToolCall {
    pub fn new(tool_id: impl Into<String>, arguments: serde_json::Value) -> Self {
        Self {
            call_id: None,
            tool_id: tool_id.into(),
            arguments,
        }
    }

    pub fn with_call_id(mut self, call_id: impl Into<String>) -> Self {
        self.call_id = Some(call_id.into());
        self
    }

    fn from_model_tool_call(call: &ModelToolCall) -> Self {
        let arguments = serde_json::from_str(&call.raw_arguments)
            .unwrap_or_else(|_| serde_json::Value::String(call.raw_arguments.clone()));
        Self {
            call_id: Some(call.call_id.0.clone()),
            tool_id: call.name.as_str().to_string(),
            arguments,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ReviewModelTurn {
    Text {
        content: String,
        usage: TokenUsage,
    },
    ToolCalls {
        calls: Vec<ReviewToolCall>,
        usage: TokenUsage,
    },
}

struct ReviewModelClientAdapter {
    model: Arc<dyn ReviewModel>,
}

impl ReviewModelClientAdapter {
    fn new(model: Arc<dyn ReviewModel>) -> Self {
        Self { model }
    }
}

#[async_trait]
impl RuntimeModelClient for ReviewModelClientAdapter {
    async fn complete(
        &self,
        scope: &SessionScope,
        transcript: &[ConversationItem],
        turn_id: TurnId,
        cancel: Cancellation,
    ) -> RuntimeResult<ModelTurn> {
        let request = ReviewModelRequest::from_runtime(scope, transcript, turn_id);
        match self.model.complete_review(request, cancel).await? {
            ReviewModelTurn::Text { content, usage } => Ok(ModelTurn::Text { content, usage }),
            ReviewModelTurn::ToolCalls { calls, usage } => {
                let mut model_calls = Vec::with_capacity(calls.len());
                for (index, call) in calls.into_iter().enumerate() {
                    model_calls.push(ModelToolCall {
                        call_id: ToolCallId(
                            call.call_id
                                .unwrap_or_else(|| format!("{}-{}-{index}", scope.id.0, turn_id.0)),
                        ),
                        index,
                        name: ToolId::parse(&call.tool_id)?,
                        raw_arguments: call.arguments.to_string(),
                    });
                }
                Ok(ModelTurn::ToolCalls {
                    calls: model_calls,
                    usage,
                })
            }
        }
    }

    fn estimate_cost(&self, usage: &TokenUsage) -> Option<ModelCostEstimate> {
        self.model.estimate_cost(usage)
    }
}

pub fn review_model_router(model: Arc<dyn ReviewModel>) -> Arc<dyn model_adapters::ModelRouter> {
    Arc::new(StaticModelRouter::new(Arc::new(
        ReviewModelClientAdapter::new(model),
    )))
}

#[derive(Clone)]
pub struct ReviewToolRegistry {
    inner: RuntimeToolRegistry,
}

pub struct ReviewJsonRpcReadOnlyToolRegistration {
    pub provider_id: ToolProviderId,
    pub id: String,
    pub description: String,
    pub parameters: serde_json::Value,
    pub cacheable: bool,
    pub provider_resources: Vec<ProviderResourceId>,
    pub transport: Arc<dyn JsonRpcToolTransport>,
}

pub struct ReviewJsonRpcNetworkReadToolRegistration {
    pub provider_id: ToolProviderId,
    pub id: String,
    pub description: String,
    pub parameters: serde_json::Value,
    pub cacheable: bool,
    pub provider_resources: Vec<ProviderResourceId>,
    pub transport: Arc<dyn JsonRpcToolTransport>,
}

struct ReviewJsonRpcRuntimeToolRegistration {
    provider_id: ToolProviderId,
    id: String,
    description: String,
    parameters: serde_json::Value,
    cacheable: bool,
    provider_resources: Vec<ProviderResourceId>,
    effects: ToolEffects,
    transport: Arc<dyn JsonRpcToolTransport>,
}

impl std::fmt::Debug for ReviewToolRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReviewToolRegistry").finish_non_exhaustive()
    }
}

impl ReviewToolRegistry {
    pub fn review_defaults() -> RuntimeResult<Self> {
        Ok(Self {
            inner: RuntimeToolRegistry::review_defaults()?,
        })
    }

    pub fn register_read_only_tool(
        &mut self,
        id: impl AsRef<str>,
        description: impl Into<String>,
        parameters: serde_json::Value,
        cacheable: bool,
        handler: Arc<dyn ReviewToolHandler>,
    ) -> RuntimeResult<ToolId> {
        let id = ToolId::parse(id.as_ref())?;
        self.inner.register_custom(
            id.clone(),
            description,
            parameters,
            cacheable,
            Arc::new(ReviewToolHandlerAdapter::new(handler)),
        )?;
        Ok(id)
    }

    pub fn register_scoped_read_only_tool(
        &mut self,
        id: impl AsRef<str>,
        description: impl Into<String>,
        parameters: serde_json::Value,
        cacheable: bool,
        provider_resources: Vec<ProviderResourceId>,
        handler: Arc<dyn ReviewToolHandler>,
    ) -> RuntimeResult<ToolId> {
        let id = ToolId::parse(id.as_ref())?;
        self.inner.register_custom_with_alias_and_effects(
            id.clone(),
            id.clone(),
            description,
            parameters,
            CustomToolOptions {
                cacheable,
                effects: ToolEffects::custom_read_only(),
                provider_resources,
            },
            Arc::new(ReviewToolHandlerAdapter::new(handler)),
        )?;
        Ok(id)
    }

    pub fn register_jsonrpc_read_only_tool(
        &mut self,
        provider_id: ToolProviderId,
        id: impl AsRef<str>,
        description: impl Into<String>,
        parameters: serde_json::Value,
        cacheable: bool,
        transport: Arc<dyn JsonRpcToolTransport>,
    ) -> RuntimeResult<ToolId> {
        self.register_scoped_jsonrpc_read_only_tool(ReviewJsonRpcReadOnlyToolRegistration {
            provider_id,
            id: id.as_ref().to_string(),
            description: description.into(),
            parameters,
            cacheable,
            provider_resources: Vec::new(),
            transport,
        })
    }

    pub fn register_scoped_jsonrpc_read_only_tool(
        &mut self,
        registration: ReviewJsonRpcReadOnlyToolRegistration,
    ) -> RuntimeResult<ToolId> {
        self.register_jsonrpc_tool_registration(ReviewJsonRpcRuntimeToolRegistration {
            provider_id: registration.provider_id,
            id: registration.id,
            description: registration.description,
            parameters: registration.parameters,
            cacheable: registration.cacheable,
            provider_resources: registration.provider_resources,
            effects: ToolEffects::review_read_only(),
            transport: registration.transport,
        })
    }

    pub fn register_jsonrpc_network_read_tool(
        &mut self,
        provider_id: ToolProviderId,
        id: impl AsRef<str>,
        description: impl Into<String>,
        parameters: serde_json::Value,
        cacheable: bool,
        transport: Arc<dyn JsonRpcToolTransport>,
    ) -> RuntimeResult<ToolId> {
        self.register_scoped_jsonrpc_network_read_tool(ReviewJsonRpcNetworkReadToolRegistration {
            provider_id,
            id: id.as_ref().to_string(),
            description: description.into(),
            parameters,
            cacheable,
            provider_resources: Vec::new(),
            transport,
        })
    }

    pub fn register_scoped_jsonrpc_network_read_tool(
        &mut self,
        registration: ReviewJsonRpcNetworkReadToolRegistration,
    ) -> RuntimeResult<ToolId> {
        self.register_jsonrpc_tool_registration(ReviewJsonRpcRuntimeToolRegistration {
            provider_id: registration.provider_id,
            id: registration.id,
            description: registration.description,
            parameters: registration.parameters,
            cacheable: registration.cacheable,
            provider_resources: registration.provider_resources,
            effects: provider_network_read_effects(),
            transport: registration.transport,
        })
    }

    fn register_jsonrpc_tool_registration(
        &mut self,
        registration: ReviewJsonRpcRuntimeToolRegistration,
    ) -> RuntimeResult<ToolId> {
        let id = ToolId::parse(registration.id.as_ref())?;
        self.inner
            .register_jsonrpc_tool_with_alias(JsonRpcToolRegistration {
                provider_id: registration.provider_id,
                id: id.clone(),
                model_alias: id.clone(),
                description: registration.description,
                parameters: registration.parameters,
                options: CustomToolOptions {
                    cacheable: registration.cacheable,
                    effects: registration.effects,
                    provider_resources: registration.provider_resources,
                },
                transport: registration.transport,
            })?;
        Ok(id)
    }

    fn into_tool_registry(self) -> RuntimeToolRegistry {
        self.inner
    }
}

#[async_trait]
pub trait ReviewToolHandler: Send + Sync {
    async fn execute_review_tool(
        &self,
        context: ReviewToolContext,
        arguments: serde_json::Value,
        cancel: Cancellation,
    ) -> RuntimeResult<ReviewToolOutput>;
}

#[derive(Debug, Clone)]
pub struct ReviewToolContext {
    pub session_id: String,
    pub turn: u32,
    pub call_id: String,
    pub tool_id: String,
    pub snapshot_id: SnapshotId,
    pub provider_resources: Vec<ProviderResourceId>,
}

impl ReviewToolContext {
    fn from_custom_tool_context(context: CustomToolContext) -> Self {
        Self {
            session_id: context.session_id.0,
            turn: context.turn_id.0,
            call_id: context.call_id.0,
            tool_id: context.tool_id.as_str().to_string(),
            snapshot_id: context.snapshot_id,
            provider_resources: context.provider_resources,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReviewToolOutput {
    pub data: Option<serde_json::Value>,
    pub artifact: Option<ReviewToolArtifact>,
}

#[derive(Debug, Clone)]
pub struct ReviewToolArtifact {
    pub key: String,
    pub content: String,
}

struct ReviewToolHandlerAdapter {
    handler: Arc<dyn ReviewToolHandler>,
}

impl ReviewToolHandlerAdapter {
    fn new(handler: Arc<dyn ReviewToolHandler>) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl CustomToolHandler for ReviewToolHandlerAdapter {
    async fn execute(
        &self,
        context: CustomToolContext,
        args: serde_json::Value,
        cancel: Cancellation,
    ) -> RuntimeResult<CustomToolOutput> {
        let output = self
            .handler
            .execute_review_tool(
                ReviewToolContext::from_custom_tool_context(context),
                args,
                cancel,
            )
            .await?;
        Ok(CustomToolOutput {
            data: output.data,
            artifact: output.artifact.map(|artifact| CustomToolArtifact {
                key: ArtifactKey(artifact.key),
                content: artifact.content,
            }),
            limits: LimitInfo::default(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotSpec {
    pub snapshot_id: Option<SnapshotId>,
    pub repo_root: PathBuf,
    pub default_cwd: Option<PathBuf>,
    pub change: ChangeSpec,
    pub path_policy: SnapshotPathPolicy,
    pub storage_policy: SnapshotStoragePolicy,
}

impl SnapshotSpec {
    pub fn new(repo_root: impl Into<PathBuf>, change: ChangeSpec) -> Self {
        Self {
            snapshot_id: None,
            repo_root: repo_root.into(),
            default_cwd: None,
            change,
            path_policy: SnapshotPathPolicy::default(),
            storage_policy: SnapshotStoragePolicy::default(),
        }
    }

    pub fn with_default_cwd(mut self, default_cwd: impl Into<PathBuf>) -> Self {
        self.default_cwd = Some(default_cwd.into());
        self
    }

    pub fn with_snapshot_id(mut self, snapshot_id: SnapshotId) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    pub fn with_path_policy(mut self, path_policy: SnapshotPathPolicy) -> Self {
        self.path_policy = path_policy;
        self
    }

    pub fn with_storage_policy(mut self, storage_policy: SnapshotStoragePolicy) -> Self {
        self.storage_policy = storage_policy;
        self
    }

    pub fn with_memory_storage_limit(mut self, max_captured_text_bytes: usize) -> Self {
        self.storage_policy = SnapshotStoragePolicy::memory(max_captured_text_bytes);
        self
    }

    pub fn with_content_addressed_storage(
        mut self,
        root: impl Into<PathBuf>,
        max_captured_text_bytes: usize,
    ) -> Self {
        self.storage_policy =
            SnapshotStoragePolicy::content_addressed_directory(root, max_captured_text_bytes);
        self
    }

    pub fn with_remote_object_storage(
        mut self,
        base_uri: impl Into<String>,
        max_captured_text_bytes: usize,
        object_store: Arc<dyn SnapshotObjectStore>,
    ) -> RuntimeResult<Self> {
        self.storage_policy = SnapshotStoragePolicy::remote_object_store(
            base_uri,
            max_captured_text_bytes,
            object_store,
        )?;
        Ok(self)
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotPathPolicy {
    pub allowed_roots: Vec<PathBuf>,
    pub denied_globs: Vec<String>,
    pub allowed_globs: Option<Vec<String>>,
    pub allow_dot_git: bool,
    pub follow_symlinks: bool,
    pub max_file_bytes: usize,
    pub max_diff_bytes: usize,
    pub max_search_results: usize,
    pub max_directory_entries: usize,
}

impl SnapshotPathPolicy {
    pub fn standard(max_file_bytes: usize, max_search_results: usize) -> Self {
        Self {
            max_file_bytes,
            max_diff_bytes: max_file_bytes,
            max_search_results,
            ..Self::default()
        }
    }
}

impl Default for SnapshotPathPolicy {
    fn default() -> Self {
        Self {
            allowed_roots: vec![PathBuf::from(".")],
            denied_globs: vec![
                ".git".to_string(),
                "node_modules".to_string(),
                "target".to_string(),
                ".venv".to_string(),
                "dist".to_string(),
                "build".to_string(),
                ".next".to_string(),
            ],
            allowed_globs: None,
            allow_dot_git: false,
            follow_symlinks: false,
            max_file_bytes: 200 * 1024,
            max_diff_bytes: 200 * 1024,
            max_search_results: 120,
            max_directory_entries: 20_000,
        }
    }
}

impl From<SnapshotPathPolicy> for PathPolicyV1 {
    fn from(value: SnapshotPathPolicy) -> Self {
        Self {
            allowed_roots: value.allowed_roots,
            denied_globs: value.denied_globs,
            allowed_globs: value.allowed_globs,
            allow_dot_git: value.allow_dot_git,
            follow_symlinks: value.follow_symlinks,
            max_file_bytes: value.max_file_bytes,
            max_diff_bytes: value.max_diff_bytes,
            max_search_results: value.max_search_results,
            max_directory_entries: value.max_directory_entries,
        }
    }
}

impl From<PathPolicyV1> for SnapshotPathPolicy {
    fn from(value: PathPolicyV1) -> Self {
        Self {
            allowed_roots: value.allowed_roots,
            denied_globs: value.denied_globs,
            allowed_globs: value.allowed_globs,
            allow_dot_git: value.allow_dot_git,
            follow_symlinks: value.follow_symlinks,
            max_file_bytes: value.max_file_bytes,
            max_diff_bytes: value.max_diff_bytes,
            max_search_results: value.max_search_results,
            max_directory_entries: value.max_directory_entries,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChangeSpec {
    pub kind: ChangeKind,
    pub change_id: String,
    pub source_ref: String,
    pub target_ref: String,
    pub base_revision_id: String,
    pub head_revision_id: String,
    pub merge_base_revision_id: Option<String>,
    pub snapshot_mode: SnapshotMode,
    pub rename_detection: RenameDetection,
    pub changed_files: Vec<ChangedFileSpec>,
}

impl ChangeSpec {
    pub fn local(
        change_id: impl Into<String>,
        head_revision_id: impl Into<String>,
        changed_files: Vec<ChangedFileSpec>,
    ) -> Self {
        Self {
            kind: ChangeKind::LocalDiff,
            change_id: change_id.into(),
            source_ref: "head".to_string(),
            target_ref: "base".to_string(),
            base_revision_id: "base".to_string(),
            head_revision_id: head_revision_id.into(),
            merge_base_revision_id: None,
            snapshot_mode: SnapshotMode::WorktreeHead,
            rename_detection: RenameDetection::None,
            changed_files,
        }
    }
}

impl From<ChangeSpec> for ChangeScopeV1 {
    fn from(value: ChangeSpec) -> Self {
        Self {
            kind: value.kind.into(),
            change_id: value.change_id,
            source_ref: value.source_ref,
            target_ref: value.target_ref,
            base_revision_id: value.base_revision_id,
            head_revision_id: value.head_revision_id,
            merge_base_revision_id: value.merge_base_revision_id,
            changed_files_manifest_ref: None,
            diff_manifest_ref: None,
            snapshot_mode: value.snapshot_mode.into(),
            rename_detection: value.rename_detection.into(),
            changed_files: value
                .changed_files
                .into_iter()
                .map(ChangedFileEntryV1::from)
                .collect(),
        }
    }
}

impl From<ChangeScopeV1> for ChangeSpec {
    fn from(value: ChangeScopeV1) -> Self {
        Self {
            kind: value.kind.into(),
            change_id: value.change_id,
            source_ref: value.source_ref,
            target_ref: value.target_ref,
            base_revision_id: value.base_revision_id,
            head_revision_id: value.head_revision_id,
            merge_base_revision_id: value.merge_base_revision_id,
            snapshot_mode: value.snapshot_mode.into(),
            rename_detection: value.rename_detection.into(),
            changed_files: value
                .changed_files
                .into_iter()
                .map(ChangedFileSpec::from)
                .collect(),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ChangeKind {
    PullRequest,
    MergeRequest,
    LocalDiff,
}

impl From<ChangeKind> for ContractChangeKind {
    fn from(value: ChangeKind) -> Self {
        match value {
            ChangeKind::PullRequest => Self::PullRequest,
            ChangeKind::MergeRequest => Self::MergeRequest,
            ChangeKind::LocalDiff => Self::LocalDiff,
        }
    }
}

impl From<ContractChangeKind> for ChangeKind {
    fn from(value: ContractChangeKind) -> Self {
        match value {
            ContractChangeKind::PullRequest => Self::PullRequest,
            ContractChangeKind::MergeRequest => Self::MergeRequest,
            ContractChangeKind::LocalDiff => Self::LocalDiff,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SnapshotMode {
    WorktreeHead,
    BaseHeadManifests,
}

impl From<SnapshotMode> for ContractSnapshotMode {
    fn from(value: SnapshotMode) -> Self {
        match value {
            SnapshotMode::WorktreeHead => Self::WorktreeHead,
            SnapshotMode::BaseHeadManifests => Self::BaseHeadManifests,
        }
    }
}

impl From<ContractSnapshotMode> for SnapshotMode {
    fn from(value: ContractSnapshotMode) -> Self {
        match value {
            ContractSnapshotMode::WorktreeHead => Self::WorktreeHead,
            ContractSnapshotMode::BaseHeadManifests => Self::BaseHeadManifests,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RenameDetection {
    None,
    AppManifest,
}

impl From<RenameDetection> for ContractRenameDetection {
    fn from(value: RenameDetection) -> Self {
        match value {
            RenameDetection::None => Self::None,
            RenameDetection::AppManifest => Self::AppManifest,
        }
    }
}

impl From<ContractRenameDetection> for RenameDetection {
    fn from(value: ContractRenameDetection) -> Self {
        match value {
            ContractRenameDetection::None => Self::None,
            ContractRenameDetection::AppManifest => Self::AppManifest,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChangedFileSpec {
    pub status: ChangedFileStatus,
    pub old_path: Option<PathBuf>,
    pub new_path: Option<PathBuf>,
    pub old_content_hash: Option<String>,
    pub new_content_hash: Option<String>,
    pub is_binary: bool,
    pub is_generated: bool,
}

impl ChangedFileSpec {
    pub fn modified(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        Self {
            status: ChangedFileStatus::Modified,
            old_path: Some(path.clone()),
            new_path: Some(path),
            old_content_hash: None,
            new_content_hash: None,
            is_binary: false,
            is_generated: false,
        }
    }
}

impl From<ChangedFileSpec> for ChangedFileEntryV1 {
    fn from(value: ChangedFileSpec) -> Self {
        Self {
            status: value.status.into(),
            old_path: value.old_path,
            new_path: value.new_path,
            old_content_hash: value.old_content_hash,
            new_content_hash: value.new_content_hash,
            is_binary: value.is_binary,
            is_generated: value.is_generated,
        }
    }
}

impl From<ChangedFileEntryV1> for ChangedFileSpec {
    fn from(value: ChangedFileEntryV1) -> Self {
        Self {
            status: value.status.into(),
            old_path: value.old_path,
            new_path: value.new_path,
            old_content_hash: value.old_content_hash,
            new_content_hash: value.new_content_hash,
            is_binary: value.is_binary,
            is_generated: value.is_generated,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ChangedFileStatus {
    Added,
    Modified,
    Deleted,
    Renamed,
    Copied,
    TypeChanged,
}

impl From<ChangedFileStatus> for ContractChangedFileStatus {
    fn from(value: ChangedFileStatus) -> Self {
        match value {
            ChangedFileStatus::Added => Self::Added,
            ChangedFileStatus::Modified => Self::Modified,
            ChangedFileStatus::Deleted => Self::Deleted,
            ChangedFileStatus::Renamed => Self::Renamed,
            ChangedFileStatus::Copied => Self::Copied,
            ChangedFileStatus::TypeChanged => Self::TypeChanged,
        }
    }
}

impl From<ContractChangedFileStatus> for ChangedFileStatus {
    fn from(value: ContractChangedFileStatus) -> Self {
        match value {
            ContractChangedFileStatus::Added => Self::Added,
            ContractChangedFileStatus::Modified => Self::Modified,
            ContractChangedFileStatus::Deleted => Self::Deleted,
            ContractChangedFileStatus::Renamed => Self::Renamed,
            ContractChangedFileStatus::Copied => Self::Copied,
            ContractChangedFileStatus::TypeChanged => Self::TypeChanged,
        }
    }
}

pub struct RunBuilder {
    spec: RunSpec,
    model_router: Option<Arc<dyn RuntimeModelRouter>>,
    tool_registry: Option<Arc<RuntimeToolRegistry>>,
    reviewer_policy: Option<Arc<ReviewerPolicy>>,
    event_sink: Option<Arc<dyn RuntimeEventSink>>,
    legacy_event_emitter: Option<Arc<EventEmitter>>,
}

impl RunBuilder {
    pub fn new(spec: RunSpec) -> Self {
        Self {
            spec,
            model_router: None,
            tool_registry: None,
            reviewer_policy: None,
            event_sink: None,
            legacy_event_emitter: None,
        }
    }

    pub fn model_router(mut self, model_router: Arc<dyn model_adapters::ModelRouter>) -> Self {
        self.model_router = Some(model_router);
        self
    }

    pub fn review_model(mut self, model: Arc<dyn ReviewModel>) -> Self {
        self.model_router = Some(review_model_router(model));
        self
    }

    pub fn tool_registry(mut self, tool_registry: tool_adapters::ToolRegistry) -> Self {
        self.tool_registry = Some(Arc::new(tool_registry));
        self
    }

    pub fn review_tool_registry(mut self, tool_registry: ReviewToolRegistry) -> Self {
        self.tool_registry = Some(Arc::new(tool_registry.into_tool_registry()));
        self
    }

    pub fn shared_tool_registry(mut self, tool_registry: Arc<tool_adapters::ToolRegistry>) -> Self {
        self.tool_registry = Some(tool_registry);
        self
    }

    pub fn reviewer_policy(mut self, reviewer_policy: Arc<ReviewerPolicy>) -> Self {
        self.reviewer_policy = Some(reviewer_policy);
        self
    }

    pub fn event_sink(mut self, event_sink: Arc<dyn runtime_events::EventSink>) -> Self {
        self.event_sink = Some(event_sink);
        self
    }

    pub fn review_event_sink(mut self, event_sink: Arc<dyn ReviewEventSink>) -> Self {
        self.event_sink = Some(Arc::new(ReviewEventSinkAdapter::new(event_sink)));
        self
    }

    pub(crate) fn legacy_event_emitter(mut self, emitter: Option<Arc<EventEmitter>>) -> Self {
        self.legacy_event_emitter = emitter;
        self
    }

    pub fn build(self) -> RuntimeResult<Run> {
        let model_router = self
            .model_router
            .ok_or_else(|| RuntimeError::InvalidInput("run requires a model router".to_string()))?;
        let registry = match self.tool_registry {
            Some(registry) => registry,
            None => Arc::new(RuntimeToolRegistry::review_defaults()?),
        };
        let reviewer_policy = self
            .reviewer_policy
            .unwrap_or_else(|| Arc::new(ReviewerPolicy::new()));
        let limits = Arc::new(self.spec.limits.into_runtime_limits());
        let mut shards = Vec::new();
        for snapshot_spec in self.spec.snapshots {
            let change: ChangeScopeV1 = snapshot_spec.change.clone().into();
            let path_policy: PathPolicyV1 = snapshot_spec.path_policy.into();
            let mut snapshot = RepoSnapshot::build_with_storage(
                &snapshot_spec.repo_root,
                &path_policy,
                &change,
                snapshot_spec.storage_policy,
            )?;
            if let Some(snapshot_id) = snapshot_spec.snapshot_id {
                Arc::get_mut(&mut snapshot)
                    .ok_or(RuntimeError::Invariant("snapshot unexpectedly shared"))?
                    .snapshot_id = snapshot_id;
            }
            let tools = Arc::new(ToolEngine::with_registry(
                Arc::clone(&snapshot),
                Arc::clone(&limits),
                Arc::clone(&registry),
            )?);
            shards.push(RunShard {
                snapshot_handle: SnapshotHandle {
                    snapshot_id: snapshot.snapshot_id.clone(),
                },
                snapshot,
                tools,
                review_revision_id: change.head_revision_id,
                sessions: Vec::new(),
            });
        }
        if shards.is_empty() {
            return Err(RuntimeError::InvalidInput("missing snapshot".to_string()));
        }
        let default_snapshot_id = shards[0].snapshot_handle.snapshot_id.clone();
        for session in self.spec.sessions {
            let session = session.into_session_scope();
            let target_snapshot_id = session
                .snapshot_id
                .clone()
                .unwrap_or_else(|| default_snapshot_id.clone());
            let Some(shard) = shards
                .iter_mut()
                .find(|shard| shard.snapshot_handle.snapshot_id == target_snapshot_id)
            else {
                return Err(RuntimeError::InvalidInput(format!(
                    "unknown session snapshot id {}",
                    target_snapshot_id.0
                )));
            };
            shard.sessions.push(session);
        }
        let snapshot_handles = shards
            .iter()
            .map(|shard| shard.snapshot_handle.clone())
            .collect::<Vec<_>>();
        Ok(Run {
            run_id: self.spec.run_id,
            snapshot_handles,
            shards,
            limits,
            model_router,
            reviewer_policy,
            event_sink: self.event_sink,
            legacy_event_emitter: self.legacy_event_emitter,
        })
    }
}

pub struct Run {
    run_id: String,
    snapshot_handles: Vec<SnapshotHandle>,
    shards: Vec<RunShard>,
    limits: Arc<RuntimeLimits>,
    model_router: Arc<dyn RuntimeModelRouter>,
    reviewer_policy: Arc<ReviewerPolicy>,
    event_sink: Option<Arc<dyn RuntimeEventSink>>,
    legacy_event_emitter: Option<Arc<EventEmitter>>,
}

struct RunShard {
    snapshot_handle: SnapshotHandle,
    snapshot: Arc<RepoSnapshot>,
    tools: Arc<ToolEngine>,
    review_revision_id: String,
    sessions: Vec<SessionScope>,
}

impl Run {
    pub fn builder(spec: RunSpec) -> RunBuilder {
        RunBuilder::new(spec)
    }

    pub async fn execute(self) -> RunReport {
        self.execute_with_cancel(Cancellation::new()).await
    }

    pub async fn execute_with_cancel(self, cancel: Cancellation) -> RunReport {
        let first_snapshot = self.snapshot_handles[0].clone();
        let run_event_sink = self.event_sink.as_ref().map(|sink| {
            Arc::new(ContextualEventSink::new(
                Arc::clone(sink),
                self.run_id.clone(),
                None,
            )) as Arc<dyn RuntimeEventSink>
        });
        if let Some(sink) = &run_event_sink {
            sink.emit(RuntimeEvent::JobStarted {
                snapshot_id: first_snapshot.snapshot_id.clone(),
            });
        }
        let aggregate_artifacts = Arc::new(RuntimeArtifactStore::default());
        let mut snapshot_readers = Vec::new();
        let mut summaries = Vec::new();
        let mut findings = Vec::new();
        for shard in self.shards {
            snapshot_readers.push(SnapshotReader::new(Arc::clone(&shard.snapshot)));
            let shard_event_sink = self.event_sink.as_ref().map(|sink| {
                Arc::new(ContextualEventSink::new(
                    Arc::clone(sink),
                    self.run_id.clone(),
                    Some(shard.snapshot_handle.snapshot_id.clone()),
                )) as Arc<dyn RuntimeEventSink>
            });
            if let Some(sink) = &shard_event_sink {
                sink.emit(RuntimeEvent::SnapshotStarted {
                    snapshot_id: shard.snapshot_handle.snapshot_id.clone(),
                });
            }
            let runtime = JobRuntime {
                snapshot: Arc::clone(&shard.snapshot),
                model_router: Arc::clone(&self.model_router),
                tools: Arc::clone(&shard.tools),
                policy: Arc::clone(&self.reviewer_policy),
                limits: Arc::clone(&self.limits),
                review_revision_id: shard.review_revision_id.clone(),
                events: RuntimeEventDispatcher::new(
                    shard_event_sink.clone(),
                    self.legacy_event_emitter.clone(),
                ),
            };
            let session_specs = shard
                .sessions
                .into_iter()
                .map(|scope| SessionSpec { scope })
                .collect::<Vec<_>>();
            let summary = runtime
                .run_sessions_with_cancel(session_specs, cancel.clone())
                .await;
            aggregate_artifacts.merge_from(&runtime.tools.artifacts);
            findings.extend(runtime.tools.findings.all());
            if let Some(sink) = &shard_event_sink {
                sink.emit(RuntimeEvent::SnapshotFinished {
                    snapshot_id: shard.snapshot_handle.snapshot_id.clone(),
                    sessions: summary.sessions,
                    completed_sessions: summary.completed_sessions,
                });
            }
            summaries.push(summary);
        }
        let metrics = merge_run_summaries(summaries);
        if let Some(sink) = &run_event_sink {
            sink.emit(RuntimeEvent::JobFinished {
                status: if metrics.completed_sessions == metrics.sessions {
                    "completed".to_string()
                } else {
                    "partial".to_string()
                },
            });
        }
        RunReport {
            run_id: self.run_id,
            snapshot: first_snapshot,
            snapshots: self.snapshot_handles,
            summary: ReviewRunSummary::from_metrics(&metrics),
            metrics,
            artifacts: aggregate_artifacts,
            snapshot_readers,
            findings,
        }
    }
}

struct ContextualEventSink {
    inner: Arc<dyn RuntimeEventSink>,
    run_id: String,
    snapshot_id: Option<SnapshotId>,
}

impl ContextualEventSink {
    fn new(
        inner: Arc<dyn RuntimeEventSink>,
        run_id: String,
        snapshot_id: Option<SnapshotId>,
    ) -> Self {
        Self {
            inner,
            run_id,
            snapshot_id,
        }
    }

    fn context_for(&self, event: &RuntimeEvent) -> RuntimeEventContext {
        let mut context = RuntimeEventContext::from_event(event).with_run_id(self.run_id.clone());
        if let Some(snapshot_id) = &self.snapshot_id {
            context = context.with_default_snapshot_id(snapshot_id.clone());
        }
        context
    }
}

impl RuntimeEventSink for ContextualEventSink {
    fn emit(&self, event: RuntimeEvent) {
        let context = self.context_for(&event);
        self.inner.emit_with_context(context, event);
    }

    fn emit_with_context(&self, context: RuntimeEventContext, event: RuntimeEvent) {
        let mut merged = self.context_for(&event);
        if context.session_id.is_some() {
            merged.session_id = context.session_id;
        }
        if context.turn_id.is_some() {
            merged.turn_id = context.turn_id;
        }
        if context.tool_call_id.is_some() {
            merged.tool_call_id = context.tool_call_id;
        }
        if context.artifact_id.is_some() {
            merged.artifact_id = context.artifact_id;
        }
        if context.finding_id.is_some() {
            merged.finding_id = context.finding_id;
        }
        if context.snapshot_id.is_some() {
            merged.snapshot_id = context.snapshot_id;
        }
        self.inner.emit_with_context(merged, event);
    }
}

pub trait ReviewEventSink: Send + Sync {
    fn emit_review_event(&self, record: ReviewEventRecord);
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewEventRecord {
    pub seq: u64,
    pub timestamp_utc: String,
    pub run_id: Option<String>,
    pub snapshot_id: Option<SnapshotId>,
    pub session_id: Option<String>,
    pub turn: Option<u32>,
    pub tool_call_id: Option<String>,
    pub artifact_id: Option<ArtifactId>,
    pub finding_id: Option<String>,
    pub event: ReviewEvent,
}

impl ReviewEventRecord {
    fn from_runtime(seq: u64, context: RuntimeEventContext, event: &RuntimeEvent) -> Self {
        Self {
            seq,
            timestamp_utc: timestamp_utc(),
            run_id: context.run_id,
            snapshot_id: context.snapshot_id,
            session_id: context.session_id.map(|id| id.0),
            turn: context.turn_id.map(|turn| turn.0),
            tool_call_id: context.tool_call_id.map(|id| id.0),
            artifact_id: context.artifact_id,
            finding_id: context.finding_id,
            event: ReviewEvent::from_runtime(event),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ReviewEvent {
    RunStarted {
        snapshot_id: SnapshotId,
    },
    SnapshotStarted {
        snapshot_id: SnapshotId,
    },
    RepoManifestCompleted {
        files: usize,
        skipped: usize,
        bytes: u64,
        ms: u64,
    },
    SessionStarted {
        session_id: String,
    },
    ModelStarted {
        session_id: String,
        turn: u32,
    },
    ModelCompleted {
        session_id: String,
        turn: u32,
        tool_call_count: usize,
    },
    ToolBatchStarted {
        session_id: String,
        turn: u32,
        count: usize,
    },
    ToolCallCompleted {
        call_id: String,
        tool_id: String,
        ok: bool,
        error_code: Option<ToolErrorCode>,
    },
    ToolCallDenied {
        call_id: String,
        tool_id: String,
        error_code: ToolErrorCode,
        reason: String,
    },
    ArtifactCreated {
        artifact_id: ArtifactId,
        tool_call_id: String,
        tool_id: String,
        bytes: usize,
        content_hash: String,
    },
    FindingRecorded {
        finding_id: String,
        session_id: String,
        tool_call_id: String,
    },
    SearchBatchCompleted {
        searched_files: usize,
        skipped_files: usize,
        bytes_scanned: usize,
        ms: u64,
    },
    SessionFinished {
        session_id: String,
        status: String,
    },
    SnapshotFinished {
        snapshot_id: SnapshotId,
        sessions: usize,
        completed_sessions: usize,
    },
    RunFinished {
        status: String,
    },
}

impl ReviewEvent {
    fn from_runtime(event: &RuntimeEvent) -> Self {
        match event {
            RuntimeEvent::JobStarted { snapshot_id } => Self::RunStarted {
                snapshot_id: snapshot_id.clone(),
            },
            RuntimeEvent::SnapshotStarted { snapshot_id } => Self::SnapshotStarted {
                snapshot_id: snapshot_id.clone(),
            },
            RuntimeEvent::RepoManifestCompleted {
                files,
                skipped,
                bytes,
                ms,
            } => Self::RepoManifestCompleted {
                files: *files,
                skipped: *skipped,
                bytes: *bytes,
                ms: *ms,
            },
            RuntimeEvent::SessionStarted { session_id } => Self::SessionStarted {
                session_id: session_id.0.clone(),
            },
            RuntimeEvent::ModelStarted {
                session_id,
                turn_id,
            } => Self::ModelStarted {
                session_id: session_id.0.clone(),
                turn: turn_id.0,
            },
            RuntimeEvent::ModelCompleted {
                session_id,
                turn_id,
                tool_call_count,
            } => Self::ModelCompleted {
                session_id: session_id.0.clone(),
                turn: turn_id.0,
                tool_call_count: *tool_call_count,
            },
            RuntimeEvent::ToolBatchStarted {
                session_id,
                turn_id,
                count,
            } => Self::ToolBatchStarted {
                session_id: session_id.0.clone(),
                turn: turn_id.0,
                count: *count,
            },
            RuntimeEvent::ToolCallCompleted {
                call_id,
                tool_name,
                ok,
                error_code,
                ..
            } => Self::ToolCallCompleted {
                call_id: call_id.0.clone(),
                tool_id: tool_name.as_str().to_string(),
                ok: *ok,
                error_code: *error_code,
            },
            RuntimeEvent::ToolCallDenied {
                call_id,
                tool_name,
                error_code,
                reason,
                ..
            } => Self::ToolCallDenied {
                call_id: call_id.0.clone(),
                tool_id: tool_name.as_str().to_string(),
                error_code: *error_code,
                reason: reason.clone(),
            },
            RuntimeEvent::ArtifactCreated {
                artifact_id,
                tool_call_id,
                tool_name,
                bytes,
                content_hash,
                ..
            } => Self::ArtifactCreated {
                artifact_id: artifact_id.clone(),
                tool_call_id: tool_call_id.0.clone(),
                tool_id: tool_name.as_str().to_string(),
                bytes: *bytes,
                content_hash: content_hash.clone(),
            },
            RuntimeEvent::FindingRecorded {
                finding_id,
                session_id,
                tool_call_id,
            } => Self::FindingRecorded {
                finding_id: finding_id.clone(),
                session_id: session_id.0.clone(),
                tool_call_id: tool_call_id.0.clone(),
            },
            RuntimeEvent::SearchBatchCompleted {
                searched_files,
                skipped_files,
                bytes_scanned,
                ms,
            } => Self::SearchBatchCompleted {
                searched_files: *searched_files,
                skipped_files: *skipped_files,
                bytes_scanned: *bytes_scanned,
                ms: *ms,
            },
            RuntimeEvent::SessionFinished { session_id, status } => Self::SessionFinished {
                session_id: session_id.0.clone(),
                status: status.clone(),
            },
            RuntimeEvent::SnapshotFinished {
                snapshot_id,
                sessions,
                completed_sessions,
            } => Self::SnapshotFinished {
                snapshot_id: snapshot_id.clone(),
                sessions: *sessions,
                completed_sessions: *completed_sessions,
            },
            RuntimeEvent::JobFinished { status } => Self::RunFinished {
                status: status.clone(),
            },
        }
    }
}

#[derive(Default)]
pub struct InMemoryReviewEventSink {
    records: Mutex<Vec<ReviewEventRecord>>,
}

impl std::fmt::Debug for InMemoryReviewEventSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryReviewEventSink")
            .field("records", &self.records().len())
            .finish()
    }
}

impl InMemoryReviewEventSink {
    pub fn records(&self) -> Vec<ReviewEventRecord> {
        self.records
            .lock()
            .expect("review event sink poisoned")
            .clone()
    }

    pub fn events(&self) -> Vec<ReviewEvent> {
        self.records()
            .into_iter()
            .map(|record| record.event)
            .collect()
    }
}

impl ReviewEventSink for InMemoryReviewEventSink {
    fn emit_review_event(&self, record: ReviewEventRecord) {
        self.records
            .lock()
            .expect("review event sink poisoned")
            .push(record);
    }
}

pub const REVIEW_EVENT_LOG_SCHEMA_VERSION: &str = "heimdaal.review-events.v1";

#[derive(Debug, Clone)]
pub struct ReviewEventJsonlManifest {
    pub path: PathBuf,
    pub schema_version: String,
    pub record_count: usize,
    pub bytes: usize,
}

#[derive(Debug, Clone)]
pub struct ReviewEventJsonlLoad {
    pub path: PathBuf,
    pub schema_version: String,
    pub record_count: usize,
    pub records: Vec<ReviewEventRecord>,
}

pub fn export_review_event_records_jsonl(
    path: impl AsRef<Path>,
    records: &[ReviewEventRecord],
) -> RuntimeResult<ReviewEventJsonlManifest> {
    write_review_event_records_jsonl(path.as_ref(), records)
}

pub fn load_review_event_records_jsonl(
    path: impl AsRef<Path>,
) -> RuntimeResult<ReviewEventJsonlLoad> {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path).map_err(|error| {
        RuntimeError::RepoUnavailable(format!("failed to read review event log: {error}"))
    })?;
    let mut records = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let record: ReviewEventJsonlRecord = serde_json::from_str(line).map_err(|error| {
            RuntimeError::InvalidInput(format!(
                "invalid review event log record at line {}: {error}",
                index + 1
            ))
        })?;
        if record.schema_version != REVIEW_EVENT_LOG_SCHEMA_VERSION {
            return Err(RuntimeError::InvalidInput(format!(
                "unsupported review event log schemaVersion {} at line {}",
                record.schema_version,
                index + 1
            )));
        }
        records.push(record.record);
    }
    Ok(ReviewEventJsonlLoad {
        path: path.to_path_buf(),
        schema_version: REVIEW_EVENT_LOG_SCHEMA_VERSION.to_string(),
        record_count: records.len(),
        records,
    })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BorrowedReviewEventJsonlRecord<'a> {
    schema_version: &'static str,
    #[serde(flatten)]
    record: &'a ReviewEventRecord,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReviewEventJsonlRecord {
    schema_version: String,
    #[serde(flatten)]
    record: ReviewEventRecord,
}

fn write_review_event_records_jsonl(
    path: &Path,
    records: &[ReviewEventRecord],
) -> RuntimeResult<ReviewEventJsonlManifest> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|error| {
            RuntimeError::RepoUnavailable(format!(
                "failed to create review event log directory: {error}"
            ))
        })?;
    }
    let mut file = std::fs::File::create(path).map_err(|error| {
        RuntimeError::RepoUnavailable(format!("failed to create review event log: {error}"))
    })?;
    let mut bytes = 0usize;
    for record in records {
        let line = serde_json::to_vec(&BorrowedReviewEventJsonlRecord {
            schema_version: REVIEW_EVENT_LOG_SCHEMA_VERSION,
            record,
        })
        .map_err(|error| {
            RuntimeError::RepoUnavailable(format!("failed to serialize review event log: {error}"))
        })?;
        file.write_all(&line).map_err(|error| {
            RuntimeError::RepoUnavailable(format!("failed to write review event log: {error}"))
        })?;
        file.write_all(b"\n").map_err(|error| {
            RuntimeError::RepoUnavailable(format!("failed to write review event log: {error}"))
        })?;
        bytes += line.len() + 1;
    }
    file.flush().map_err(|error| {
        RuntimeError::RepoUnavailable(format!("failed to flush review event log: {error}"))
    })?;
    Ok(ReviewEventJsonlManifest {
        path: path.to_path_buf(),
        schema_version: REVIEW_EVENT_LOG_SCHEMA_VERSION.to_string(),
        record_count: records.len(),
        bytes,
    })
}

struct ReviewEventSinkAdapter {
    inner: Arc<dyn ReviewEventSink>,
    next_seq: AtomicU64,
}

impl ReviewEventSinkAdapter {
    fn new(inner: Arc<dyn ReviewEventSink>) -> Self {
        Self {
            inner,
            next_seq: AtomicU64::new(1),
        }
    }
}

impl RuntimeEventSink for ReviewEventSinkAdapter {
    fn emit(&self, event: RuntimeEvent) {
        self.emit_with_context(RuntimeEventContext::from_event(&event), event);
    }

    fn emit_with_context(&self, context: RuntimeEventContext, event: RuntimeEvent) {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        self.inner
            .emit_review_event(ReviewEventRecord::from_runtime(seq, context, &event));
    }
}

fn merge_run_summaries(mut summaries: Vec<ConcurrentRunReport>) -> ConcurrentRunReport {
    let mut merged = summaries.remove(0);
    for summary in summaries {
        merged.sessions += summary.sessions;
        merged.completed_sessions += summary.completed_sessions;
        merged.model_calls += summary.model_calls;
        merged.tool_calls += summary.tool_calls;
        merged.tool_counts.add(summary.tool_counts);
        merged.findings += summary.findings;
        merged.publishable_findings += summary.publishable_findings;
        merged.elapsed_ms += summary.elapsed_ms;
        merged.input_tokens += summary.input_tokens;
        merged.output_tokens += summary.output_tokens;
        merged.total_tokens += summary.total_tokens;
        merged.artifacts += summary.artifacts;
        merged.artifact_bytes += summary.artifact_bytes;
        merge_counters(&mut merged.counters, summary.counters);
        merge_tool_metrics(&mut merged.tool_metrics, summary.tool_metrics);
        merged.provider_health =
            merge_provider_health(merged.provider_health, summary.provider_health);
        merged.snapshot_metrics.extend(summary.snapshot_metrics);
        merge_model_metrics(&mut merged.model_metrics, summary.model_metrics);
        merged
            .terminal_diagnostics
            .extend(summary.terminal_diagnostics);
    }
    merged
        .terminal_diagnostics
        .sort_by(|left, right| left.session_id.cmp(&right.session_id));
    merged.snapshot_metrics.sort_by(|left, right| {
        left.snapshot_id
            .0
            .cmp(&right.snapshot_id.0)
            .then(left.sessions.cmp(&right.sessions))
    });
    merged.benchmark_failures = runtime_benchmark_failures(&merged);
    merged.benchmark_valid = merged.benchmark_failures.is_empty();
    merged
}

fn merge_counters(left: &mut ConcurrentCounters, right: ConcurrentCounters) {
    left.search_scans += right.search_scans;
    left.search_dedupe_waiters += right.search_dedupe_waiters;
    left.search_cache_hits += right.search_cache_hits;
    left.read_cache_hits += right.read_cache_hits;
    left.read_file_reads += right.read_file_reads;
    left.tool_errors += right.tool_errors;
    left.artifact_cache_hits += right.artifact_cache_hits;
}

fn merge_tool_metrics(
    left: &mut BTreeMap<ToolMetricKey, ToolMetricsSnapshot>,
    right: BTreeMap<ToolMetricKey, ToolMetricsSnapshot>,
) {
    for (key, metrics) in right {
        let entry = left.entry(key).or_default();
        entry.calls += metrics.calls;
        entry.successes += metrics.successes;
        entry.errors += metrics.errors;
        entry.cache_hits += metrics.cache_hits;
        entry.deduped += metrics.deduped;
        entry.timeouts += metrics.timeouts;
        entry.cancellations += metrics.cancellations;
        entry.artifacts += metrics.artifacts;
        entry.input_bytes += metrics.input_bytes;
        entry.output_bytes += metrics.output_bytes;
        entry.latency_ms += metrics.latency_ms;
        entry.max_latency_ms = entry.max_latency_ms.max(metrics.max_latency_ms);
        entry.queue_wait_ms += metrics.queue_wait_ms;
        entry.max_queue_wait_ms = entry.max_queue_wait_ms.max(metrics.max_queue_wait_ms);
    }
}

fn merge_model_metrics(left: &mut ModelMetricsSnapshot, right: ModelMetricsSnapshot) {
    left.calls += right.calls;
    left.successes += right.successes;
    left.errors += right.errors;
    left.retries += right.retries;
    left.costed_calls += right.costed_calls;
    left.unpriced_calls += right.unpriced_calls;
    left.latency_ms += right.latency_ms;
    left.max_latency_ms = left.max_latency_ms.max(right.max_latency_ms);
    left.estimated_input_cost_micro_usd += right.estimated_input_cost_micro_usd;
    left.estimated_output_cost_micro_usd += right.estimated_output_cost_micro_usd;
    left.estimated_total_cost_micro_usd += right.estimated_total_cost_micro_usd;
    left.input_tokens += right.input_tokens;
    left.output_tokens += right.output_tokens;
    left.total_tokens += right.total_tokens;
}

fn merge_provider_health(
    left: Vec<ToolProviderHealthSnapshot>,
    right: Vec<ToolProviderHealthSnapshot>,
) -> Vec<ToolProviderHealthSnapshot> {
    let mut by_provider = BTreeMap::new();
    for snapshot in left.into_iter().chain(right) {
        let entry =
            by_provider
                .entry(snapshot.provider_id.clone())
                .or_insert(ToolProviderHealthSnapshot {
                    provider_id: snapshot.provider_id.clone(),
                    state: ToolProviderHealthState::Healthy,
                    calls: 0,
                    errors: 0,
                    timeouts: 0,
                    cancellations: 0,
                    consecutive_errors: 0,
                });
        entry.calls += snapshot.calls;
        entry.errors += snapshot.errors;
        entry.timeouts += snapshot.timeouts;
        entry.cancellations += snapshot.cancellations;
        entry.consecutive_errors = entry.consecutive_errors.max(snapshot.consecutive_errors);
        entry.state = if entry.consecutive_errors >= 3 {
            ToolProviderHealthState::Unhealthy
        } else if entry.errors > 0 {
            ToolProviderHealthState::Degraded
        } else {
            ToolProviderHealthState::Healthy
        };
    }
    by_provider.into_values().collect()
}

#[derive(Debug, Clone)]
pub struct ReviewRunSummary {
    pub status: String,
    pub sessions: usize,
    pub completed_sessions: usize,
    pub model_calls: usize,
    pub tool_calls: usize,
    pub tool_counts: ToolCounts,
    pub findings: usize,
    pub publishable_findings: usize,
    pub elapsed_ms: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub artifacts: usize,
    pub artifact_bytes: usize,
    pub snapshot_count: usize,
    pub benchmark_valid: bool,
    pub benchmark_failure_count: usize,
}

impl ReviewRunSummary {
    fn from_metrics(metrics: &ConcurrentRunReport) -> Self {
        Self {
            status: if metrics.completed_sessions == metrics.sessions {
                "completed".to_string()
            } else {
                "partial".to_string()
            },
            sessions: metrics.sessions,
            completed_sessions: metrics.completed_sessions,
            model_calls: metrics.model_calls,
            tool_calls: metrics.tool_calls,
            tool_counts: metrics.tool_counts,
            findings: metrics.findings,
            publishable_findings: metrics.publishable_findings,
            elapsed_ms: metrics.elapsed_ms,
            input_tokens: metrics.input_tokens,
            output_tokens: metrics.output_tokens,
            total_tokens: metrics.total_tokens,
            artifacts: metrics.artifacts,
            artifact_bytes: metrics.artifact_bytes,
            snapshot_count: metrics.snapshot_metrics.len(),
            benchmark_valid: metrics.benchmark_valid,
            benchmark_failure_count: metrics.benchmark_failures.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunReport {
    pub run_id: String,
    pub snapshot: SnapshotHandle,
    pub snapshots: Vec<SnapshotHandle>,
    pub summary: ReviewRunSummary,
    pub metrics: metrics::ConcurrentRunReport,
    pub artifacts: Arc<tool_adapters::ArtifactStore>,
    snapshot_readers: Vec<SnapshotReader>,
    pub(crate) findings: Vec<FindingV1>,
}

impl RunReport {
    pub fn snapshot_readers(&self) -> Vec<SnapshotReader> {
        self.snapshot_readers.clone()
    }

    pub fn snapshot_reader(&self, snapshot_id: &SnapshotId) -> Option<SnapshotReader> {
        self.snapshot_readers
            .iter()
            .find(|reader| reader.snapshot_id() == snapshot_id)
            .cloned()
    }

    pub fn snapshot_manifests(&self) -> Vec<SnapshotManifest> {
        self.snapshot_readers
            .iter()
            .map(SnapshotReader::manifest)
            .collect()
    }

    pub fn findings(&self) -> Vec<FindingView> {
        self.findings
            .iter()
            .map(FindingView::from_finding)
            .collect()
    }

    pub fn finding_evidence_artifacts(
        &self,
        finding_id: &str,
        policy: ArtifactExportPolicy,
    ) -> RuntimeResult<Vec<EvidenceArtifactView>> {
        let Some(finding) = self
            .findings
            .iter()
            .find(|finding| finding.id == finding_id)
        else {
            return Err(RuntimeError::InvalidInput(format!(
                "unknown finding id {finding_id}"
            )));
        };
        let artifacts = finding
            .evidence
            .iter()
            .filter(|evidence| policy.allows_artifact(&ArtifactId(evidence.artifact_id.clone())))
            .map(|evidence| {
                let artifact_id = ArtifactId(evidence.artifact_id.clone());
                let artifact = if policy.include_raw() {
                    self.artifacts.get_raw(&artifact_id)
                } else {
                    self.artifacts.get(&artifact_id)
                }
                .ok_or_else(|| {
                    RuntimeError::InvalidInput(format!(
                        "missing evidence artifact {}",
                        evidence.artifact_id
                    ))
                })?;
                Ok(EvidenceArtifactView {
                    evidence: EvidenceView::from_evidence(evidence),
                    artifact,
                })
            })
            .collect::<RuntimeResult<Vec<_>>>()?;
        policy.validate_retention(
            artifacts.len(),
            artifacts
                .iter()
                .map(|evidence_artifact| evidence_artifact.artifact.bytes)
                .sum(),
        )?;
        Ok(artifacts)
    }

    pub fn redacted_artifacts(&self) -> ReviewArtifacts<'_> {
        ReviewArtifacts::new(self, ArtifactExportPolicy::redacted_all())
    }

    pub fn raw_artifacts(
        &self,
        capabilities: &capabilities::CapabilitySet,
    ) -> RuntimeResult<ReviewArtifacts<'_>> {
        Ok(ReviewArtifacts::new(
            self,
            ArtifactExportPolicy::raw(capabilities)?,
        ))
    }

    pub fn export_artifacts(
        &self,
        policy: ArtifactExportPolicy,
    ) -> RuntimeResult<ArtifactExportManifest> {
        self.artifacts.export_with_policy(policy)
    }

    pub fn persist_artifacts(
        &self,
        object_store: &dyn ArtifactObjectStore,
        policy: ArtifactExportPolicy,
    ) -> RuntimeResult<ArtifactPersistenceManifest> {
        self.artifacts.persist_with_policy(object_store, policy)
    }

    pub fn export_artifact_bundle(
        &self,
        root: impl AsRef<Path>,
        policy: ArtifactExportPolicy,
    ) -> RuntimeResult<ArtifactBundleManifest> {
        self.artifacts.export_bundle(root.as_ref(), policy)
    }
}

#[derive(Debug, Clone)]
pub struct ReviewArtifacts<'a> {
    report: &'a RunReport,
    policy: ArtifactExportPolicy,
}

impl<'a> ReviewArtifacts<'a> {
    fn new(report: &'a RunReport, policy: ArtifactExportPolicy) -> Self {
        Self { report, policy }
    }

    pub fn only_artifacts<I, S>(mut self, artifact_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.policy = self.policy.with_artifact_ids(artifact_ids);
        self
    }

    pub fn with_retention_policy(mut self, retention: ArtifactRetentionPolicy) -> Self {
        self.policy = self.policy.with_retention_policy(retention);
        self
    }

    pub fn export(&self) -> RuntimeResult<ArtifactExportManifest> {
        self.report.export_artifacts(self.policy.clone())
    }

    pub fn finding_evidence(&self, finding_id: &str) -> RuntimeResult<Vec<EvidenceArtifactView>> {
        self.report
            .finding_evidence_artifacts(finding_id, self.policy.clone())
    }

    pub fn persist_to(
        &self,
        object_store: &dyn ArtifactObjectStore,
    ) -> RuntimeResult<ArtifactPersistenceManifest> {
        self.report
            .persist_artifacts(object_store, self.policy.clone())
    }

    pub fn bundle_at(&self, root: impl AsRef<Path>) -> RuntimeResult<ArtifactBundleManifest> {
        self.report
            .export_artifact_bundle(root, self.policy.clone())
    }

    pub fn policy(&self) -> &ArtifactExportPolicy {
        &self.policy
    }
}

#[derive(Debug, Clone)]
pub struct FindingView {
    pub id: String,
    pub title: String,
    pub claim: String,
    pub evidence_count: usize,
    pub publishable: bool,
}

impl FindingView {
    fn from_finding(finding: &FindingV1) -> Self {
        Self {
            id: finding.id.clone(),
            title: finding.title.clone(),
            claim: finding.claim.clone(),
            evidence_count: finding.evidence.len(),
            publishable: matches!(
                finding.publishability,
                crate::contracts::FindingPublishability::Publishable
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EvidenceView {
    pub evidence_id: String,
    pub artifact_id: ArtifactId,
    pub kind: String,
    pub content_hash: String,
    pub producing_tool_call_id: ToolCallId,
}

impl EvidenceView {
    pub fn artifact_id(&self) -> &str {
        &self.artifact_id.0
    }

    pub fn producing_tool_call_id(&self) -> &str {
        &self.producing_tool_call_id.0
    }

    fn from_evidence(evidence: &crate::contracts::EvidenceRefV1) -> Self {
        Self {
            evidence_id: evidence.evidence_id.clone(),
            artifact_id: ArtifactId(evidence.artifact_id.clone()),
            kind: evidence_kind_name(evidence.kind).to_string(),
            content_hash: evidence.content_hash.clone(),
            producing_tool_call_id: ToolCallId(evidence.producing_tool_call_id.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EvidenceArtifactView {
    pub evidence: EvidenceView,
    pub artifact: ArtifactView,
}

impl EvidenceArtifactView {
    pub fn artifact_id(&self) -> &str {
        self.artifact.artifact_id()
    }
}

impl ArtifactView {
    pub fn artifact_id(&self) -> &str {
        &self.artifact_id.0
    }
}

#[derive(Debug, Clone)]
pub struct RunHandle {
    pub run_id: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotHandle {
    pub snapshot_id: SnapshotId,
}

#[derive(Debug, Clone)]
pub struct SnapshotReader {
    snapshot: Arc<RepoSnapshot>,
}

impl SnapshotReader {
    fn new(snapshot: Arc<RepoSnapshot>) -> Self {
        Self { snapshot }
    }

    pub fn snapshot_id(&self) -> &SnapshotId {
        &self.snapshot.snapshot_id
    }

    pub fn manifest(&self) -> SnapshotManifest {
        SnapshotManifest::from_snapshot(&self.snapshot)
    }

    pub fn read_text(&self, path: &RepoPath, max_bytes: usize) -> RuntimeResult<SnapshotTextFile> {
        let file = self.snapshot.lookup(path)?;
        if file.capture_status == SnapshotCaptureStatus::SkippedMemoryLimit {
            return Err(RuntimeError::LimitExceeded {
                kind: "snapshot_capture_bytes",
            });
        }
        let content_hash = file.content_hash.clone().ok_or(RuntimeError::Invariant(
            "text candidate missing snapshot content hash",
        ))?;
        let (bytes, truncated) = self.snapshot.read_bounded(file.file_id, max_bytes)?;
        let content = String::from_utf8(bytes)
            .map_err(|_| RuntimeError::InvalidInput("snapshot file is not UTF-8".to_string()))?;
        Ok(SnapshotTextFile {
            snapshot_id: self.snapshot.snapshot_id.clone(),
            path: file.rel_path.clone(),
            content_hash,
            bytes: content.len(),
            truncated,
            content,
        })
    }

    pub fn read_text_path(
        &self,
        path: impl AsRef<str>,
        max_bytes: usize,
    ) -> RuntimeResult<SnapshotTextFile> {
        let path = RepoPath::parse(path.as_ref())?;
        self.read_text(&path, max_bytes)
    }

    pub fn validate_storage(&self) -> RuntimeResult<SnapshotStorageValidationReport> {
        let mut report = SnapshotStorageValidationReport::new(
            self.snapshot.snapshot_id.clone(),
            self.snapshot.storage_policy.clone(),
        );
        for file in &self.snapshot.manifest.files {
            let Some(content) = file.snapshot_content.as_ref() else {
                continue;
            };
            let expected_hash = file.content_hash.clone().ok_or(RuntimeError::Invariant(
                "captured snapshot file missing content hash",
            ))?;
            let object = SnapshotStorageObject {
                path: file.rel_path.clone(),
                content_hash: expected_hash.clone(),
                bytes: content.len(),
                store_path: storage_object_path(content),
                store_uri: storage_object_uri(content),
            };
            report.checked_files += 1;
            report.checked_bytes += content.len();
            report.checked_objects.push(object.clone());
            let bytes = match content {
                SnapshotContentRef::Memory(bytes) => bytes.to_vec(),
                SnapshotContentRef::ContentAddressedFile { path, .. } => match fs::read(path) {
                    Ok(bytes) => bytes,
                    Err(error) if error.kind() == ErrorKind::NotFound => {
                        report.missing_files.push(object);
                        continue;
                    }
                    Err(error) => {
                        return Err(RuntimeError::RepoUnavailable(format!(
                            "failed to read snapshot storage object: {error}"
                        )))
                    }
                },
                SnapshotContentRef::RemoteObject { uri, store, .. } => {
                    let Some(bytes) = store.read_snapshot_object(uri)? else {
                        report.missing_files.push(object);
                        continue;
                    };
                    bytes
                }
            };
            if snapshot_content_hash(&bytes) != expected_hash {
                report.stale_files.push(object);
            }
        }
        report.valid = report.missing_files.is_empty() && report.stale_files.is_empty();
        Ok(report)
    }

    pub fn cleanup_storage(&self) -> RuntimeResult<SnapshotStorageCleanupReport> {
        let mut report = SnapshotStorageCleanupReport::new(
            self.snapshot.snapshot_id.clone(),
            self.snapshot.storage_policy.clone(),
        );
        let mut candidate_dirs = Vec::new();
        for file in &self.snapshot.manifest.files {
            let Some(content) = file.snapshot_content.as_ref() else {
                continue;
            };
            let expected_hash = file.content_hash.clone().ok_or(RuntimeError::Invariant(
                "captured snapshot file missing content hash",
            ))?;
            let object = SnapshotStorageObject {
                path: file.rel_path.clone(),
                content_hash: expected_hash,
                bytes: content.len(),
                store_path: storage_object_path(content),
                store_uri: storage_object_uri(content),
            };
            match content {
                SnapshotContentRef::Memory(_) => {}
                SnapshotContentRef::ContentAddressedFile { path, .. } => match fs::metadata(path) {
                    Ok(metadata) => {
                        fs::remove_file(path).map_err(|error| {
                            RuntimeError::RepoUnavailable(format!(
                                "failed to remove snapshot storage object: {error}"
                            ))
                        })?;
                        report.removed_files += 1;
                        report.removed_bytes =
                            report.removed_bytes.saturating_add(metadata.len() as usize);
                        report.removed_objects.push(object);
                        if let Some(parent) = path.parent() {
                            candidate_dirs.push(parent.to_path_buf());
                        }
                    }
                    Err(error) if error.kind() == ErrorKind::NotFound => {
                        report.missing_files.push(object);
                    }
                    Err(error) => {
                        return Err(RuntimeError::RepoUnavailable(format!(
                            "failed to inspect snapshot storage object: {error}"
                        )))
                    }
                },
                SnapshotContentRef::RemoteObject { uri, store, .. } => {
                    if store.remove_snapshot_object(uri)? {
                        report.removed_files += 1;
                        report.removed_bytes = report.removed_bytes.saturating_add(content.len());
                        report.removed_objects.push(object);
                    } else {
                        report.missing_files.push(object);
                    }
                }
            }
        }
        candidate_dirs.sort();
        candidate_dirs.dedup();
        for directory in candidate_dirs {
            if prune_empty_directory(&directory)? {
                report.pruned_empty_directories += 1;
            }
        }
        Ok(report)
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotManifest {
    pub snapshot_id: SnapshotId,
    pub manifest_hash: String,
    pub path_policy_hash: String,
    pub storage_policy_hash: String,
    pub storage_policy: SnapshotStoragePolicy,
    pub file_count: usize,
    pub changed_file_count: usize,
    pub captured_text_file_count: usize,
    pub captured_text_bytes: usize,
    pub capture_skipped_file_count: usize,
    pub capture_skipped_bytes: u64,
    pub files: Vec<SnapshotFile>,
    pub changed_files: Vec<SnapshotChangedFile>,
}

impl SnapshotManifest {
    pub fn uses_content_addressed_storage(&self) -> bool {
        matches!(
            self.storage_policy.mode,
            SnapshotStorageMode::ContentAddressedDirectory { .. }
        )
    }

    pub fn uses_remote_object_storage(&self) -> bool {
        matches!(
            self.storage_policy.mode,
            SnapshotStorageMode::RemoteObjectStore { .. }
        )
    }

    pub fn max_captured_text_bytes(&self) -> usize {
        self.storage_policy.max_captured_text_bytes
    }

    fn from_snapshot(snapshot: &RepoSnapshot) -> Self {
        let files = snapshot
            .manifest
            .files
            .iter()
            .map(|file| SnapshotFile {
                path: file.rel_path.clone(),
                size: file.size,
                content_hash: file.content_hash.clone(),
                is_changed: file.is_changed,
                is_text_candidate: file.is_text_candidate,
                captured: file.snapshot_content.is_some(),
                capture_status: file.capture_status,
            })
            .collect::<Vec<_>>();
        let captured_text_file_count = files.iter().filter(|file| file.captured).count();
        let captured_text_bytes = snapshot
            .manifest
            .files
            .iter()
            .filter_map(|file| file.snapshot_content.as_ref())
            .map(|content| content.len())
            .sum();
        let changed_files = snapshot
            .manifest
            .changed_file_entries
            .iter()
            .map(|file| SnapshotChangedFile {
                path: file.rel_path.clone(),
                summary: file.summary.clone(),
            })
            .collect::<Vec<_>>();
        Self {
            snapshot_id: snapshot.snapshot_id.clone(),
            manifest_hash: snapshot.manifest_hash.clone(),
            path_policy_hash: snapshot.path_policy_hash.clone(),
            storage_policy_hash: snapshot.storage_policy_hash.clone(),
            storage_policy: snapshot.storage_policy.clone(),
            file_count: files.len(),
            changed_file_count: snapshot.manifest.changed_files.len(),
            captured_text_file_count,
            captured_text_bytes,
            capture_skipped_file_count: snapshot.capture_skipped_files,
            capture_skipped_bytes: snapshot.capture_skipped_bytes,
            files,
            changed_files,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotFile {
    pub path: RepoPath,
    pub size: u64,
    pub content_hash: Option<String>,
    pub is_changed: bool,
    pub is_text_candidate: bool,
    pub captured: bool,
    pub capture_status: SnapshotCaptureStatus,
}

impl SnapshotFile {
    pub fn capture_skipped_memory_limit(&self) -> bool {
        self.capture_status == SnapshotCaptureStatus::SkippedMemoryLimit
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotChangedFile {
    pub path: RepoPath,
    pub summary: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotTextFile {
    pub snapshot_id: SnapshotId,
    pub path: RepoPath,
    pub content_hash: String,
    pub bytes: usize,
    pub truncated: bool,
    pub content: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotStorageObject {
    pub path: RepoPath,
    pub content_hash: String,
    pub bytes: usize,
    pub store_path: Option<PathBuf>,
    pub store_uri: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SnapshotStorageValidationReport {
    pub snapshot_id: SnapshotId,
    pub storage_policy: SnapshotStoragePolicy,
    pub checked_files: usize,
    pub checked_bytes: usize,
    pub checked_objects: Vec<SnapshotStorageObject>,
    pub valid: bool,
    pub missing_files: Vec<SnapshotStorageObject>,
    pub stale_files: Vec<SnapshotStorageObject>,
}

impl SnapshotStorageValidationReport {
    pub fn uses_content_addressed_storage(&self) -> bool {
        matches!(
            self.storage_policy.mode,
            SnapshotStorageMode::ContentAddressedDirectory { .. }
        )
    }

    pub fn uses_remote_object_storage(&self) -> bool {
        matches!(
            self.storage_policy.mode,
            SnapshotStorageMode::RemoteObjectStore { .. }
        )
    }

    fn new(snapshot_id: SnapshotId, storage_policy: SnapshotStoragePolicy) -> Self {
        Self {
            snapshot_id,
            storage_policy,
            checked_files: 0,
            checked_bytes: 0,
            checked_objects: Vec::new(),
            valid: true,
            missing_files: Vec::new(),
            stale_files: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotStorageCleanupReport {
    pub snapshot_id: SnapshotId,
    pub storage_policy: SnapshotStoragePolicy,
    pub removed_files: usize,
    pub removed_bytes: usize,
    pub removed_objects: Vec<SnapshotStorageObject>,
    pub missing_files: Vec<SnapshotStorageObject>,
    pub pruned_empty_directories: usize,
}

impl SnapshotStorageCleanupReport {
    fn new(snapshot_id: SnapshotId, storage_policy: SnapshotStoragePolicy) -> Self {
        Self {
            snapshot_id,
            storage_policy,
            removed_files: 0,
            removed_bytes: 0,
            removed_objects: Vec::new(),
            missing_files: Vec::new(),
            pruned_empty_directories: 0,
        }
    }
}

pub trait RemoteSnapshotObjectClient: Send + Sync {
    fn put_remote_snapshot_object(&self, uri: &str, bytes: Vec<u8>) -> RuntimeResult<()>;

    fn read_remote_snapshot_object(&self, uri: &str) -> RuntimeResult<Option<Vec<u8>>>;

    fn remove_remote_snapshot_object(&self, uri: &str) -> RuntimeResult<bool>;
}

pub struct RemoteSnapshotObjectStore {
    base_uri: String,
    client: Arc<dyn RemoteSnapshotObjectClient>,
}

impl RemoteSnapshotObjectStore {
    pub fn new(
        base_uri: impl Into<String>,
        client: Arc<dyn RemoteSnapshotObjectClient>,
    ) -> RuntimeResult<Self> {
        Ok(Self {
            base_uri: normalize_remote_store_base_uri(base_uri.into(), "snapshot")?,
            client,
        })
    }

    pub fn base_uri(&self) -> &str {
        &self.base_uri
    }
}

impl std::fmt::Debug for RemoteSnapshotObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteSnapshotObjectStore")
            .field("base_uri", &self.base_uri)
            .finish_non_exhaustive()
    }
}

impl SnapshotObjectStore for RemoteSnapshotObjectStore {
    fn put_snapshot_object(&self, uri: &str, bytes: Vec<u8>) -> RuntimeResult<()> {
        validate_remote_snapshot_object_uri(&self.base_uri, uri)?;
        self.client.put_remote_snapshot_object(uri, bytes)
    }

    fn read_snapshot_object(&self, uri: &str) -> RuntimeResult<Option<Vec<u8>>> {
        validate_remote_snapshot_object_uri(&self.base_uri, uri)?;
        self.client.read_remote_snapshot_object(uri)
    }

    fn remove_snapshot_object(&self, uri: &str) -> RuntimeResult<bool> {
        validate_remote_snapshot_object_uri(&self.base_uri, uri)?;
        self.client.remove_remote_snapshot_object(uri)
    }
}

#[derive(Debug, Default)]
pub struct InMemoryRemoteSnapshotObjectClient {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl InMemoryRemoteSnapshotObjectClient {
    pub fn read(&self, uri: &str) -> Option<Vec<u8>> {
        self.objects
            .lock()
            .expect("in-memory remote snapshot object client poisoned")
            .get(uri)
            .cloned()
    }

    pub fn write(&self, uri: impl Into<String>, bytes: Vec<u8>) {
        self.objects
            .lock()
            .expect("in-memory remote snapshot object client poisoned")
            .insert(uri.into(), bytes);
    }

    pub fn remove(&self, uri: &str) {
        self.objects
            .lock()
            .expect("in-memory remote snapshot object client poisoned")
            .remove(uri);
    }

    pub fn object_count(&self) -> usize {
        self.objects
            .lock()
            .expect("in-memory remote snapshot object client poisoned")
            .len()
    }
}

impl RemoteSnapshotObjectClient for InMemoryRemoteSnapshotObjectClient {
    fn put_remote_snapshot_object(&self, uri: &str, bytes: Vec<u8>) -> RuntimeResult<()> {
        self.write(uri.to_string(), bytes);
        Ok(())
    }

    fn read_remote_snapshot_object(&self, uri: &str) -> RuntimeResult<Option<Vec<u8>>> {
        Ok(self.read(uri))
    }

    fn remove_remote_snapshot_object(&self, uri: &str) -> RuntimeResult<bool> {
        let mut objects = self
            .objects
            .lock()
            .expect("in-memory remote snapshot object client poisoned");
        Ok(objects.remove(uri).is_some())
    }
}

#[derive(Debug, Clone)]
pub struct HttpRemoteObjectClient {
    http: reqwest::blocking::Client,
    authorization_header: Option<String>,
}

impl HttpRemoteObjectClient {
    pub fn new() -> RuntimeResult<Self> {
        Self::with_authorization_header(None)
    }

    pub fn bearer_token(token: impl Into<String>) -> RuntimeResult<Self> {
        let token = token.into();
        if token.trim().is_empty() {
            return Err(RuntimeError::InvalidInput(
                "remote object-store bearer token must not be empty".to_string(),
            ));
        }
        Self::with_authorization_header(Some(format!("Bearer {token}")))
    }

    pub fn with_authorization_header(authorization_header: Option<String>) -> RuntimeResult<Self> {
        let http = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|error| {
                RuntimeError::RepoUnavailable(format!(
                    "failed to build remote object-store HTTP client: {error}"
                ))
            })?;
        Ok(Self {
            http,
            authorization_header,
        })
    }

    fn put_remote_object(&self, uri: &str, bytes: Vec<u8>) -> RuntimeResult<()> {
        let response = self
            .with_auth(self.http.put(uri).body(bytes))
            .send()
            .map_err(remote_object_http_error)?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(remote_object_http_status_error(
                "put",
                uri,
                response.status(),
            ))
        }
    }

    fn read_remote_object(&self, uri: &str) -> RuntimeResult<Option<Vec<u8>>> {
        let response = self
            .with_auth(self.http.get(uri))
            .send()
            .map_err(remote_object_http_error)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(remote_object_http_status_error(
                "read",
                uri,
                response.status(),
            ));
        }
        let bytes = response.bytes().map_err(remote_object_http_error)?;
        Ok(Some(bytes.to_vec()))
    }

    fn remove_remote_object(&self, uri: &str) -> RuntimeResult<bool> {
        let response = self
            .with_auth(self.http.delete(uri))
            .send()
            .map_err(remote_object_http_error)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if response.status().is_success() {
            Ok(true)
        } else {
            Err(remote_object_http_status_error(
                "remove",
                uri,
                response.status(),
            ))
        }
    }

    fn with_auth(
        &self,
        request: reqwest::blocking::RequestBuilder,
    ) -> reqwest::blocking::RequestBuilder {
        match &self.authorization_header {
            Some(header) => request.header(reqwest::header::AUTHORIZATION, header),
            None => request,
        }
    }
}

impl RemoteSnapshotObjectClient for HttpRemoteObjectClient {
    fn put_remote_snapshot_object(&self, uri: &str, bytes: Vec<u8>) -> RuntimeResult<()> {
        self.put_remote_object(uri, bytes)
    }

    fn read_remote_snapshot_object(&self, uri: &str) -> RuntimeResult<Option<Vec<u8>>> {
        self.read_remote_object(uri)
    }

    fn remove_remote_snapshot_object(&self, uri: &str) -> RuntimeResult<bool> {
        self.remove_remote_object(uri)
    }
}

impl RemoteArtifactObjectClient for HttpRemoteObjectClient {
    fn put_remote_artifact_object(&self, uri: &str, bytes: Vec<u8>) -> RuntimeResult<()> {
        self.put_remote_object(uri, bytes)
    }

    fn read_remote_artifact_object(&self, uri: &str) -> RuntimeResult<Option<Vec<u8>>> {
        self.read_remote_object(uri)
    }

    fn remove_remote_artifact_object(&self, uri: &str) -> RuntimeResult<bool> {
        self.remove_remote_object(uri)
    }
}

fn storage_object_path(content: &SnapshotContentRef) -> Option<PathBuf> {
    match content {
        SnapshotContentRef::Memory(_) => None,
        SnapshotContentRef::ContentAddressedFile { path, .. } => Some(path.clone()),
        SnapshotContentRef::RemoteObject { .. } => None,
    }
}

fn storage_object_uri(content: &SnapshotContentRef) -> Option<String> {
    match content {
        SnapshotContentRef::Memory(_) | SnapshotContentRef::ContentAddressedFile { .. } => None,
        SnapshotContentRef::RemoteObject { uri, .. } => Some(uri.clone()),
    }
}

fn validate_remote_snapshot_object_uri(base_uri: &str, uri: &str) -> RuntimeResult<()> {
    let prefix = format!("{}/snapshots/", base_uri.trim_end_matches('/'));
    let Some(hash) = uri.strip_prefix(&prefix) else {
        return Err(RuntimeError::RepoAccessDenied);
    };
    if remote_content_addressed_uri(base_uri, hash)? != uri {
        return Err(RuntimeError::RepoAccessDenied);
    }
    Ok(())
}

fn snapshot_content_hash(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn prune_empty_directory(path: &Path) -> RuntimeResult<bool> {
    let mut entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(RuntimeError::RepoUnavailable(format!(
                "failed to inspect snapshot storage directory: {error}"
            )))
        }
    };
    if entries.next().is_some() {
        return Ok(false);
    }
    fs::remove_dir(path).map_err(|error| {
        RuntimeError::RepoUnavailable(format!(
            "failed to prune snapshot storage directory: {error}"
        ))
    })?;
    Ok(true)
}

pub trait ArtifactReader: Send + Sync {
    fn get_artifact(&self, artifact_id: &ArtifactId) -> Option<ArtifactView>;
    fn list_artifacts(&self) -> Vec<ArtifactView>;
    fn export_with_policy(
        &self,
        policy: ArtifactExportPolicy,
    ) -> RuntimeResult<ArtifactExportManifest>;
    fn export_bundle(
        &self,
        root: &Path,
        policy: ArtifactExportPolicy,
    ) -> RuntimeResult<ArtifactBundleManifest>;
    fn persist_with_policy(
        &self,
        object_store: &dyn ArtifactObjectStore,
        policy: ArtifactExportPolicy,
    ) -> RuntimeResult<ArtifactPersistenceManifest>;
}

impl ArtifactReader for RuntimeArtifactStore {
    fn get_artifact(&self, artifact_id: &ArtifactId) -> Option<ArtifactView> {
        self.get(artifact_id)
    }

    fn list_artifacts(&self) -> Vec<ArtifactView> {
        self.list()
    }

    fn export_with_policy(
        &self,
        policy: ArtifactExportPolicy,
    ) -> RuntimeResult<ArtifactExportManifest> {
        let artifacts = artifacts_for_policy(
            if policy.include_raw() {
                self.list_raw()
            } else {
                self.list()
            },
            &policy,
        )?;
        Ok(ArtifactExportManifest {
            view: artifact_view_mode(&policy),
            retention: policy.retention_policy().clone(),
            artifact_count: artifacts.len(),
            total_bytes: artifacts.iter().map(|artifact| artifact.bytes).sum(),
            artifacts,
        })
    }

    fn export_bundle(
        &self,
        root: &Path,
        policy: ArtifactExportPolicy,
    ) -> RuntimeResult<ArtifactBundleManifest> {
        let artifacts = artifacts_for_policy(
            if policy.include_raw() {
                self.list_raw()
            } else {
                self.list()
            },
            &policy,
        )?;
        let artifact_dir = root.join(ARTIFACT_BUNDLE_ARTIFACTS_DIR);
        std::fs::create_dir_all(&artifact_dir).map_err(|error| {
            RuntimeError::RepoUnavailable(format!("failed to create artifact bundle: {error}"))
        })?;

        let mut entries = Vec::with_capacity(artifacts.len());
        for artifact in artifacts {
            let relative_path = PathBuf::from(ARTIFACT_BUNDLE_ARTIFACTS_DIR)
                .join(format!("{}.txt", artifact.artifact_id.0));
            let artifact_path = root.join(&relative_path);
            std::fs::write(&artifact_path, artifact.content.as_bytes()).map_err(|error| {
                RuntimeError::RepoUnavailable(format!("failed to write artifact bundle: {error}"))
            })?;
            entries.push(ArtifactBundleEntry {
                artifact_id: artifact.artifact_id,
                bytes: artifact.bytes,
                content_hash: artifact.content_hash,
                relative_path,
            });
        }

        let total_bytes = entries.iter().map(|entry| entry.bytes).sum();
        let manifest_path = root.join(ARTIFACT_BUNDLE_MANIFEST_FILE);
        let view = artifact_view_mode(&policy);
        let manifest_json = serde_json::json!({
            "view": artifact_view_name(view),
            "artifactCount": entries.len(),
            "totalBytes": total_bytes,
            "retention": policy.retention_policy(),
            "artifacts": entries.iter().map(|entry| serde_json::json!({
                "artifactId": entry.artifact_id,
                "bytes": entry.bytes,
                "contentHash": entry.content_hash,
                "relativePath": entry.relative_path.to_string_lossy(),
            })).collect::<Vec<_>>(),
        });
        let manifest_bytes = serde_json::to_vec_pretty(&manifest_json).map_err(|error| {
            RuntimeError::RepoUnavailable(format!("failed to serialize artifact bundle: {error}"))
        })?;
        std::fs::write(&manifest_path, manifest_bytes).map_err(|error| {
            RuntimeError::RepoUnavailable(format!("failed to write artifact bundle: {error}"))
        })?;

        Ok(ArtifactBundleManifest {
            view,
            root: root.to_path_buf(),
            manifest_path,
            retention: policy.retention_policy().clone(),
            artifact_count: entries.len(),
            total_bytes,
            artifacts: entries,
        })
    }

    fn persist_with_policy(
        &self,
        object_store: &dyn ArtifactObjectStore,
        policy: ArtifactExportPolicy,
    ) -> RuntimeResult<ArtifactPersistenceManifest> {
        let artifacts = artifacts_for_policy(
            if policy.include_raw() {
                self.list_raw()
            } else {
                self.list()
            },
            &policy,
        )?;
        persist_artifacts_to_store(artifacts, object_store, policy)
    }
}

fn artifacts_for_policy(
    artifacts: Vec<ArtifactView>,
    policy: &ArtifactExportPolicy,
) -> RuntimeResult<Vec<ArtifactView>> {
    let artifacts = artifacts
        .into_iter()
        .filter(|artifact| policy.allows_artifact(&artifact.artifact_id))
        .collect::<Vec<_>>();
    policy.validate_retention(
        artifacts.len(),
        artifacts.iter().map(|artifact| artifact.bytes).sum(),
    )?;
    Ok(artifacts)
}

fn persist_artifacts_to_store(
    artifacts: Vec<ArtifactView>,
    object_store: &dyn ArtifactObjectStore,
    policy: ArtifactExportPolicy,
) -> RuntimeResult<ArtifactPersistenceManifest> {
    let view = artifact_view_mode(&policy);
    let total_bytes = artifacts.iter().map(|artifact| artifact.bytes).sum();
    let mut objects = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        let object = ArtifactStoreObject {
            artifact_id: artifact.artifact_id,
            view,
            bytes: artifact.bytes,
            content_hash: artifact.content_hash,
            content: artifact.content,
        };
        objects.push(object_store.put_artifact_object(object)?);
    }
    Ok(ArtifactPersistenceManifest {
        view,
        retention: policy.retention_policy().clone(),
        artifact_count: objects.len(),
        total_bytes,
        objects,
    })
}

const ARTIFACT_BUNDLE_ARTIFACTS_DIR: &str = "artifacts";
const ARTIFACT_BUNDLE_MANIFEST_FILE: &str = "manifest.json";

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactViewMode {
    Redacted,
    Raw,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactExportPolicy {
    include_raw: bool,
    allowed_artifact_ids: Option<Vec<ArtifactId>>,
    retention: ArtifactRetentionPolicy,
}

impl ArtifactExportPolicy {
    pub fn redacted_all() -> Self {
        Self::redacted_with_artifacts(None)
    }

    pub fn redacted_artifacts<I, S>(artifact_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::redacted_with_artifacts(Some(
            artifact_ids
                .into_iter()
                .map(|artifact_id| ArtifactId(artifact_id.as_ref().to_string()))
                .collect(),
        ))
    }

    pub fn redacted(capabilities: &capabilities::CapabilitySet) -> RuntimeResult<Self> {
        if capabilities.artifact_access.read_redacted || capabilities.artifact_access.read_raw {
            Ok(Self::redacted_with_artifacts(
                capabilities.artifact_access.allowed_artifact_ids.clone(),
            ))
        } else {
            Err(RuntimeError::InvalidInput(
                "redacted artifact export requires artifact read capability".to_string(),
            ))
        }
    }

    pub fn raw(capabilities: &capabilities::CapabilitySet) -> RuntimeResult<Self> {
        if capabilities.artifact_access.read_raw {
            Ok(Self {
                include_raw: true,
                allowed_artifact_ids: capabilities.artifact_access.allowed_artifact_ids.clone(),
                retention: ArtifactRetentionPolicy::unlimited(),
            })
        } else {
            Err(RuntimeError::InvalidInput(
                "raw artifact export requires raw artifact read capability".to_string(),
            ))
        }
    }

    pub fn include_raw(&self) -> bool {
        self.include_raw
    }

    fn redacted_with_artifacts(allowed_artifact_ids: Option<Vec<ArtifactId>>) -> Self {
        Self {
            include_raw: false,
            allowed_artifact_ids,
            retention: ArtifactRetentionPolicy::unlimited(),
        }
    }

    fn with_artifact_ids<I, S>(mut self, artifact_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.allowed_artifact_ids = Some(
            artifact_ids
                .into_iter()
                .map(|artifact_id| ArtifactId(artifact_id.as_ref().to_string()))
                .collect(),
        );
        self
    }

    pub fn with_retention_policy(mut self, retention: ArtifactRetentionPolicy) -> Self {
        self.retention = retention;
        self
    }

    pub fn retention_policy(&self) -> &ArtifactRetentionPolicy {
        &self.retention
    }

    pub fn allows_artifact(&self, artifact_id: &ArtifactId) -> bool {
        match &self.allowed_artifact_ids {
            Some(allowed) => allowed.iter().any(|allowed_id| allowed_id == artifact_id),
            None => true,
        }
    }

    fn validate_retention(&self, artifact_count: usize, total_bytes: usize) -> RuntimeResult<()> {
        self.retention.validate(artifact_count, total_bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactRetentionPolicy {
    pub max_artifacts: Option<usize>,
    pub max_bytes: Option<usize>,
}

impl ArtifactRetentionPolicy {
    pub fn unlimited() -> Self {
        Self {
            max_artifacts: None,
            max_bytes: None,
        }
    }

    pub fn max_artifacts(max_artifacts: usize) -> Self {
        Self {
            max_artifacts: Some(max_artifacts),
            max_bytes: None,
        }
    }

    pub fn max_bytes(max_bytes: usize) -> Self {
        Self {
            max_artifacts: None,
            max_bytes: Some(max_bytes),
        }
    }

    pub fn bounded(max_artifacts: usize, max_bytes: usize) -> Self {
        Self {
            max_artifacts: Some(max_artifacts),
            max_bytes: Some(max_bytes),
        }
    }

    fn validate(&self, artifact_count: usize, total_bytes: usize) -> RuntimeResult<()> {
        if self
            .max_artifacts
            .is_some_and(|max_artifacts| artifact_count > max_artifacts)
        {
            return Err(RuntimeError::LimitExceeded {
                kind: "artifact_retention_artifacts",
            });
        }
        if self
            .max_bytes
            .is_some_and(|max_bytes| total_bytes > max_bytes)
        {
            return Err(RuntimeError::LimitExceeded {
                kind: "artifact_retention_bytes",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactExportManifest {
    pub view: ArtifactViewMode,
    pub retention: ArtifactRetentionPolicy,
    pub artifact_count: usize,
    pub total_bytes: usize,
    pub artifacts: Vec<ArtifactView>,
}

impl ArtifactExportManifest {
    pub fn first_artifact_id(&self) -> Option<&str> {
        self.artifacts.first().map(ArtifactView::artifact_id)
    }

    pub fn contains_artifact_id(&self, artifact_id: impl AsRef<str>) -> bool {
        let artifact_id = artifact_id.as_ref();
        self.artifacts
            .iter()
            .any(|artifact| artifact.artifact_id() == artifact_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactPersistenceManifest {
    pub view: ArtifactViewMode,
    pub retention: ArtifactRetentionPolicy,
    pub artifact_count: usize,
    pub total_bytes: usize,
    pub objects: Vec<ArtifactObjectRef>,
}

impl ArtifactPersistenceManifest {
    pub fn object_refs(&self) -> &[ArtifactObjectRef] {
        &self.objects
    }

    pub fn first_object_ref(&self) -> Option<&ArtifactObjectRef> {
        self.objects.first()
    }

    pub fn contains_artifact_id(&self, artifact_id: impl AsRef<str>) -> bool {
        let artifact_id = artifact_id.as_ref();
        self.objects
            .iter()
            .any(|object_ref| object_ref.artifact_id() == artifact_id)
    }

    pub fn validate_storage(
        &self,
        object_reader: &dyn ArtifactObjectReader,
    ) -> RuntimeResult<ArtifactObjectStorageValidationReport> {
        let mut report = ArtifactObjectStorageValidationReport::new(
            self.view,
            self.retention.clone(),
            self.artifact_count,
            self.total_bytes,
        );
        for object_ref in &self.objects {
            report.checked_objects += 1;
            report.checked_bytes = report.checked_bytes.saturating_add(object_ref.bytes);
            let Some(bytes) = object_reader.read_artifact_object(object_ref)? else {
                report.missing_objects.push(object_ref.clone());
                continue;
            };
            if !artifact_object_bytes_match(object_ref, &bytes) {
                report.stale_objects.push(object_ref.clone());
            }
        }
        report.valid = report.checked_objects == self.artifact_count
            && report.checked_bytes == self.total_bytes
            && report.missing_objects.is_empty()
            && report.stale_objects.is_empty();
        Ok(report)
    }

    pub fn cleanup_storage(
        &self,
        object_store: &dyn ArtifactObjectStore,
    ) -> RuntimeResult<ArtifactObjectStorageCleanupReport> {
        let mut report = ArtifactObjectStorageCleanupReport::new(
            self.view,
            self.retention.clone(),
            self.artifact_count,
            self.total_bytes,
        );
        for object_ref in &self.objects {
            report.checked_objects += 1;
            let Some(bytes) = object_store.read_artifact_object(object_ref)? else {
                report.missing_objects.push(object_ref.clone());
                continue;
            };
            report.checked_bytes = report.checked_bytes.saturating_add(bytes.len());
            if !artifact_object_bytes_match(object_ref, &bytes) {
                report.stale_objects.push(object_ref.clone());
            }
            if object_store.remove_artifact_object(object_ref)? {
                report.removed_bytes = report.removed_bytes.saturating_add(bytes.len());
                report.removed_objects.push(object_ref.clone());
            } else {
                report.missing_objects.push(object_ref.clone());
            }
        }
        Ok(report)
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactStoreObject {
    pub artifact_id: ArtifactId,
    pub view: ArtifactViewMode,
    pub bytes: usize,
    pub content_hash: String,
    pub content: String,
}

impl ArtifactStoreObject {
    pub fn artifact_id(&self) -> &str {
        &self.artifact_id.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactObjectRef {
    pub artifact_id: ArtifactId,
    pub view: ArtifactViewMode,
    pub bytes: usize,
    pub content_hash: String,
    pub uri: String,
    pub path: Option<PathBuf>,
}

impl ArtifactObjectRef {
    pub fn artifact_id(&self) -> &str {
        &self.artifact_id.0
    }

    pub fn view(&self) -> ArtifactViewMode {
        self.view
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn content_hash(&self) -> &str {
        &self.content_hash
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub fn has_local_path(&self) -> bool {
        self.path.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactObjectStorageValidationReport {
    pub view: ArtifactViewMode,
    pub retention: ArtifactRetentionPolicy,
    pub expected_objects: usize,
    pub expected_bytes: usize,
    pub checked_objects: usize,
    pub checked_bytes: usize,
    pub valid: bool,
    pub missing_objects: Vec<ArtifactObjectRef>,
    pub stale_objects: Vec<ArtifactObjectRef>,
}

impl ArtifactObjectStorageValidationReport {
    fn new(
        view: ArtifactViewMode,
        retention: ArtifactRetentionPolicy,
        expected_objects: usize,
        expected_bytes: usize,
    ) -> Self {
        Self {
            view,
            retention,
            expected_objects,
            expected_bytes,
            checked_objects: 0,
            checked_bytes: 0,
            valid: true,
            missing_objects: Vec::new(),
            stale_objects: Vec::new(),
        }
    }

    pub fn has_missing_artifact(&self, artifact_id: impl AsRef<str>) -> bool {
        let artifact_id = artifact_id.as_ref();
        self.missing_objects
            .iter()
            .any(|object_ref| object_ref.artifact_id() == artifact_id)
    }

    pub fn has_stale_artifact(&self, artifact_id: impl AsRef<str>) -> bool {
        let artifact_id = artifact_id.as_ref();
        self.stale_objects
            .iter()
            .any(|object_ref| object_ref.artifact_id() == artifact_id)
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactObjectStorageCleanupReport {
    pub view: ArtifactViewMode,
    pub retention: ArtifactRetentionPolicy,
    pub expected_objects: usize,
    pub expected_bytes: usize,
    pub checked_objects: usize,
    pub checked_bytes: usize,
    pub removed_objects: Vec<ArtifactObjectRef>,
    pub removed_bytes: usize,
    pub missing_objects: Vec<ArtifactObjectRef>,
    pub stale_objects: Vec<ArtifactObjectRef>,
}

impl ArtifactObjectStorageCleanupReport {
    fn new(
        view: ArtifactViewMode,
        retention: ArtifactRetentionPolicy,
        expected_objects: usize,
        expected_bytes: usize,
    ) -> Self {
        Self {
            view,
            retention,
            expected_objects,
            expected_bytes,
            checked_objects: 0,
            checked_bytes: 0,
            removed_objects: Vec::new(),
            removed_bytes: 0,
            missing_objects: Vec::new(),
            stale_objects: Vec::new(),
        }
    }

    pub fn has_removed_artifact(&self, artifact_id: impl AsRef<str>) -> bool {
        let artifact_id = artifact_id.as_ref();
        self.removed_objects
            .iter()
            .any(|object_ref| object_ref.artifact_id() == artifact_id)
    }

    pub fn has_missing_artifact(&self, artifact_id: impl AsRef<str>) -> bool {
        let artifact_id = artifact_id.as_ref();
        self.missing_objects
            .iter()
            .any(|object_ref| object_ref.artifact_id() == artifact_id)
    }

    pub fn has_stale_artifact(&self, artifact_id: impl AsRef<str>) -> bool {
        let artifact_id = artifact_id.as_ref();
        self.stale_objects
            .iter()
            .any(|object_ref| object_ref.artifact_id() == artifact_id)
    }
}

pub trait ArtifactObjectReader: Send + Sync {
    fn read_artifact_object(
        &self,
        object_ref: &ArtifactObjectRef,
    ) -> RuntimeResult<Option<Vec<u8>>>;
}

pub trait ArtifactObjectStore: ArtifactObjectReader {
    fn put_artifact_object(&self, object: ArtifactStoreObject) -> RuntimeResult<ArtifactObjectRef>;

    fn remove_artifact_object(&self, object_ref: &ArtifactObjectRef) -> RuntimeResult<bool>;
}

pub trait RemoteArtifactObjectClient: Send + Sync {
    fn put_remote_artifact_object(&self, uri: &str, bytes: Vec<u8>) -> RuntimeResult<()>;

    fn read_remote_artifact_object(&self, uri: &str) -> RuntimeResult<Option<Vec<u8>>>;

    fn remove_remote_artifact_object(&self, uri: &str) -> RuntimeResult<bool>;
}

pub struct RemoteArtifactObjectStore {
    base_uri: String,
    client: Arc<dyn RemoteArtifactObjectClient>,
}

impl RemoteArtifactObjectStore {
    pub fn new(
        base_uri: impl Into<String>,
        client: Arc<dyn RemoteArtifactObjectClient>,
    ) -> RuntimeResult<Self> {
        Ok(Self {
            base_uri: normalize_remote_store_base_uri(base_uri.into(), "artifact")?,
            client,
        })
    }

    pub fn base_uri(&self) -> &str {
        &self.base_uri
    }
}

impl std::fmt::Debug for RemoteArtifactObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteArtifactObjectStore")
            .field("base_uri", &self.base_uri)
            .finish_non_exhaustive()
    }
}

impl ArtifactObjectReader for RemoteArtifactObjectStore {
    fn read_artifact_object(
        &self,
        object_ref: &ArtifactObjectRef,
    ) -> RuntimeResult<Option<Vec<u8>>> {
        let expected_uri =
            remote_artifact_object_uri(&self.base_uri, object_ref.view, &object_ref.content_hash)?;
        if object_ref.uri != expected_uri {
            return Err(RuntimeError::RepoAccessDenied);
        }
        self.client.read_remote_artifact_object(&object_ref.uri)
    }
}

impl ArtifactObjectStore for RemoteArtifactObjectStore {
    fn put_artifact_object(&self, object: ArtifactStoreObject) -> RuntimeResult<ArtifactObjectRef> {
        validate_artifact_store_object(&object)?;
        let uri = remote_artifact_object_uri(&self.base_uri, object.view, &object.content_hash)?;
        self.client
            .put_remote_artifact_object(&uri, object.content.into_bytes())?;
        Ok(ArtifactObjectRef {
            artifact_id: object.artifact_id,
            view: object.view,
            bytes: object.bytes,
            content_hash: object.content_hash,
            uri,
            path: None,
        })
    }

    fn remove_artifact_object(&self, object_ref: &ArtifactObjectRef) -> RuntimeResult<bool> {
        let expected_uri =
            remote_artifact_object_uri(&self.base_uri, object_ref.view, &object_ref.content_hash)?;
        if object_ref.uri != expected_uri {
            return Err(RuntimeError::RepoAccessDenied);
        }
        self.client.remove_remote_artifact_object(&object_ref.uri)
    }
}

#[derive(Debug)]
pub struct LocalArtifactObjectStore {
    root: PathBuf,
}

impl LocalArtifactObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl ArtifactObjectReader for LocalArtifactObjectStore {
    fn read_artifact_object(
        &self,
        object_ref: &ArtifactObjectRef,
    ) -> RuntimeResult<Option<Vec<u8>>> {
        let path =
            local_artifact_object_path(&self.root, object_ref.view, &object_ref.content_hash)?;
        match fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(RuntimeError::RepoUnavailable(format!(
                "failed to read artifact object: {error}"
            ))),
        }
    }
}

impl ArtifactObjectStore for LocalArtifactObjectStore {
    fn put_artifact_object(&self, object: ArtifactStoreObject) -> RuntimeResult<ArtifactObjectRef> {
        validate_artifact_store_object(&object)?;
        let path = local_artifact_object_path(&self.root, object.view, &object.content_hash)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                RuntimeError::RepoUnavailable(format!(
                    "failed to create artifact object store directory: {error}"
                ))
            })?;
        }
        fs::write(&path, object.content.as_bytes()).map_err(|error| {
            RuntimeError::RepoUnavailable(format!("failed to write artifact object: {error}"))
        })?;
        Ok(ArtifactObjectRef {
            artifact_id: object.artifact_id,
            view: object.view,
            bytes: object.bytes,
            content_hash: object.content_hash,
            uri: path.to_string_lossy().to_string(),
            path: Some(path),
        })
    }

    fn remove_artifact_object(&self, object_ref: &ArtifactObjectRef) -> RuntimeResult<bool> {
        let path =
            local_artifact_object_path(&self.root, object_ref.view, &object_ref.content_hash)?;
        match fs::remove_file(path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(RuntimeError::RepoUnavailable(format!(
                "failed to remove artifact object: {error}"
            ))),
        }
    }
}

#[derive(Debug, Default)]
pub struct InMemoryArtifactObjectStore {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl InMemoryArtifactObjectStore {
    pub fn read(&self, uri: &str) -> Option<Vec<u8>> {
        self.objects
            .lock()
            .expect("in-memory artifact object store poisoned")
            .get(uri)
            .cloned()
    }

    pub fn read_object(&self, object_ref: &ArtifactObjectRef) -> RuntimeResult<Option<Vec<u8>>> {
        self.read_artifact_object(object_ref)
    }

    pub fn object_count(&self) -> usize {
        self.objects
            .lock()
            .expect("in-memory artifact object store poisoned")
            .len()
    }
}

impl ArtifactObjectReader for InMemoryArtifactObjectStore {
    fn read_artifact_object(
        &self,
        object_ref: &ArtifactObjectRef,
    ) -> RuntimeResult<Option<Vec<u8>>> {
        Ok(self.read(&object_ref.uri))
    }
}

impl ArtifactObjectStore for InMemoryArtifactObjectStore {
    fn put_artifact_object(&self, object: ArtifactStoreObject) -> RuntimeResult<ArtifactObjectRef> {
        validate_artifact_store_object(&object)?;
        let uri = format!(
            "memory://artifacts/{}/{}",
            artifact_view_name(object.view),
            object.content_hash
        );
        self.objects
            .lock()
            .expect("in-memory artifact object store poisoned")
            .insert(uri.clone(), object.content.into_bytes());
        Ok(ArtifactObjectRef {
            artifact_id: object.artifact_id,
            view: object.view,
            bytes: object.bytes,
            content_hash: object.content_hash,
            uri,
            path: None,
        })
    }

    fn remove_artifact_object(&self, object_ref: &ArtifactObjectRef) -> RuntimeResult<bool> {
        let mut objects = self
            .objects
            .lock()
            .expect("in-memory artifact object store poisoned");
        Ok(objects.remove(&object_ref.uri).is_some())
    }
}

#[derive(Debug, Default)]
pub struct InMemoryRemoteArtifactObjectClient {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl InMemoryRemoteArtifactObjectClient {
    pub fn read(&self, uri: &str) -> Option<Vec<u8>> {
        self.objects
            .lock()
            .expect("in-memory remote artifact object client poisoned")
            .get(uri)
            .cloned()
    }

    pub fn write(&self, uri: impl Into<String>, bytes: Vec<u8>) {
        self.objects
            .lock()
            .expect("in-memory remote artifact object client poisoned")
            .insert(uri.into(), bytes);
    }

    pub fn remove(&self, uri: &str) {
        self.objects
            .lock()
            .expect("in-memory remote artifact object client poisoned")
            .remove(uri);
    }

    pub fn object_count(&self) -> usize {
        self.objects
            .lock()
            .expect("in-memory remote artifact object client poisoned")
            .len()
    }
}

impl RemoteArtifactObjectClient for InMemoryRemoteArtifactObjectClient {
    fn put_remote_artifact_object(&self, uri: &str, bytes: Vec<u8>) -> RuntimeResult<()> {
        self.write(uri.to_string(), bytes);
        Ok(())
    }

    fn read_remote_artifact_object(&self, uri: &str) -> RuntimeResult<Option<Vec<u8>>> {
        Ok(self.read(uri))
    }

    fn remove_remote_artifact_object(&self, uri: &str) -> RuntimeResult<bool> {
        let mut objects = self
            .objects
            .lock()
            .expect("in-memory remote artifact object client poisoned");
        Ok(objects.remove(uri).is_some())
    }
}

pub const REMOTE_OBJECT_STORE_CANARY_SCHEMA_VERSION: &str = "muzen.remote-object-store-canary.v1";

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteObjectStoreCanaryTarget {
    Snapshot,
    Artifact,
}

impl RemoteObjectStoreCanaryTarget {
    fn as_str(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Artifact => "artifact",
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteObjectStoreCanaryStepKind {
    Put,
    ReadAfterPut,
    Remove,
    ReadAfterRemove,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteObjectStoreCanaryStatus {
    Passed,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteObjectStoreCanaryStep {
    pub step: RemoteObjectStoreCanaryStepKind,
    pub status: RemoteObjectStoreCanaryStatus,
    pub uri: Option<String>,
    pub bytes: Option<usize>,
    pub message: Option<String>,
}

impl RemoteObjectStoreCanaryStep {
    fn passed(step: RemoteObjectStoreCanaryStepKind, uri: &str, bytes: usize) -> Self {
        Self {
            step,
            status: RemoteObjectStoreCanaryStatus::Passed,
            uri: Some(uri.to_string()),
            bytes: Some(bytes),
            message: None,
        }
    }

    fn failed(step: RemoteObjectStoreCanaryStepKind, uri: Option<&str>, message: String) -> Self {
        Self {
            step,
            status: RemoteObjectStoreCanaryStatus::Failed,
            uri: uri.map(ToString::to_string),
            bytes: None,
            message: Some(message),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteObjectStoreCanaryGate {
    pub valid: bool,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub failures: Vec<String>,
}

impl RemoteObjectStoreCanaryGate {
    fn evaluate(
        cleanup_supported: bool,
        steps: &[RemoteObjectStoreCanaryStep],
    ) -> RemoteObjectStoreCanaryGate {
        let expected_steps = [
            RemoteObjectStoreCanaryStepKind::Put,
            RemoteObjectStoreCanaryStepKind::ReadAfterPut,
            RemoteObjectStoreCanaryStepKind::Remove,
            RemoteObjectStoreCanaryStepKind::ReadAfterRemove,
        ];
        let mut passed = 0usize;
        let mut failed = 0usize;
        let mut skipped = 0usize;
        let mut failures = Vec::new();
        for expected_step in expected_steps {
            let count = steps
                .iter()
                .filter(|step| step.step == expected_step)
                .count();
            match count {
                0 => failures.push(format!("missing {expected_step:?} canary step")),
                1 => {}
                count => {
                    failures.push(format!("duplicate {expected_step:?} canary steps: {count}"))
                }
            }
        }
        for step in steps {
            match step.status {
                RemoteObjectStoreCanaryStatus::Passed => passed += 1,
                RemoteObjectStoreCanaryStatus::Failed => {
                    failed += 1;
                    failures.push(format!(
                        "{:?} failed: {}",
                        step.step,
                        step.message
                            .as_deref()
                            .unwrap_or("remote object-store canary step failed")
                    ));
                }
                RemoteObjectStoreCanaryStatus::Skipped => {
                    skipped += 1;
                    if cleanup_supported
                        || !matches!(
                            step.step,
                            RemoteObjectStoreCanaryStepKind::Remove
                                | RemoteObjectStoreCanaryStepKind::ReadAfterRemove
                        )
                    {
                        failures.push(format!(
                            "{:?} skipped: {}",
                            step.step,
                            step.message
                                .as_deref()
                                .unwrap_or("remote object-store canary step skipped")
                        ));
                    }
                }
            }
        }
        Self {
            valid: failures.is_empty() && failed == 0,
            passed,
            failed,
            skipped,
            failures,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteObjectStoreCanaryEvidence {
    pub schema_version: String,
    pub generated_at_utc: String,
    pub target: RemoteObjectStoreCanaryTarget,
    pub base_uri: String,
    pub object_uri: Option<String>,
    pub payload_bytes: usize,
    pub payload_hash: String,
    pub cleanup_supported: bool,
    pub steps: Vec<RemoteObjectStoreCanaryStep>,
    pub gate: RemoteObjectStoreCanaryGate,
}

struct RemoteObjectStoreCanaryEvidenceParts {
    generated_at_utc: String,
    target: RemoteObjectStoreCanaryTarget,
    base_uri: String,
    object_uri: Option<String>,
    payload_bytes: usize,
    payload_hash: String,
    cleanup_supported: bool,
    steps: Vec<RemoteObjectStoreCanaryStep>,
}

impl RemoteObjectStoreCanaryEvidence {
    fn from_parts(parts: RemoteObjectStoreCanaryEvidenceParts) -> Self {
        let gate = RemoteObjectStoreCanaryGate::evaluate(parts.cleanup_supported, &parts.steps);
        Self {
            schema_version: REMOTE_OBJECT_STORE_CANARY_SCHEMA_VERSION.to_string(),
            generated_at_utc: parts.generated_at_utc,
            target: parts.target,
            base_uri: parts.base_uri,
            object_uri: parts.object_uri,
            payload_bytes: parts.payload_bytes,
            payload_hash: parts.payload_hash,
            cleanup_supported: parts.cleanup_supported,
            steps: parts.steps,
            gate,
        }
    }

    pub fn require_passed(&self) -> RuntimeResult<()> {
        let failures = self.validation_failures();
        if failures.is_empty() {
            return Ok(());
        }
        Err(RuntimeError::InvalidInput(format!(
            "remote object-store canary gate failed: {}",
            failures.join("; ")
        )))
    }

    fn validation_failures(&self) -> Vec<String> {
        let mut failures = Vec::new();
        if self.schema_version != REMOTE_OBJECT_STORE_CANARY_SCHEMA_VERSION {
            failures.push(format!(
                "unsupported remote object-store canary schema {}",
                self.schema_version
            ));
        }
        let evaluated = RemoteObjectStoreCanaryGate::evaluate(self.cleanup_supported, &self.steps);
        if evaluated != self.gate {
            failures
                .push("stored remote object-store canary gate does not match steps".to_string());
        }
        failures.extend(evaluated.failures);
        failures
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteObjectStoreCanaryEvidenceExport {
    pub path: PathBuf,
    pub bytes: usize,
    pub valid: bool,
    pub failures: Vec<String>,
}

pub fn run_remote_snapshot_object_store_canary(
    base_uri: impl Into<String>,
    client: &dyn RemoteSnapshotObjectClient,
) -> RemoteObjectStoreCanaryEvidence {
    let generated_at_utc = timestamp_utc();
    let base_uri = base_uri.into();
    let payload = remote_object_store_canary_payload(
        RemoteObjectStoreCanaryTarget::Snapshot,
        &base_uri,
        &generated_at_utc,
    );
    let payload_hash = snapshot_content_hash(&payload);
    let mut steps = Vec::new();
    let object_uri = match normalize_remote_store_base_uri(base_uri.clone(), "snapshot")
        .and_then(|base_uri| remote_content_addressed_uri(&base_uri, &payload_hash))
    {
        Ok(uri) => Some(uri),
        Err(error) => {
            steps.push(RemoteObjectStoreCanaryStep::failed(
                RemoteObjectStoreCanaryStepKind::Put,
                None,
                error.to_string(),
            ));
            None
        }
    };
    if let Some(uri) = object_uri.as_deref() {
        match client.put_remote_snapshot_object(uri, payload.clone()) {
            Ok(()) => steps.push(RemoteObjectStoreCanaryStep::passed(
                RemoteObjectStoreCanaryStepKind::Put,
                uri,
                payload.len(),
            )),
            Err(error) => steps.push(RemoteObjectStoreCanaryStep::failed(
                RemoteObjectStoreCanaryStepKind::Put,
                Some(uri),
                error.to_string(),
            )),
        }
        push_remote_snapshot_read_step(client, uri, &payload, &mut steps);
        match client.remove_remote_snapshot_object(uri) {
            Ok(true) => steps.push(RemoteObjectStoreCanaryStep::passed(
                RemoteObjectStoreCanaryStepKind::Remove,
                uri,
                0,
            )),
            Ok(false) => steps.push(RemoteObjectStoreCanaryStep::failed(
                RemoteObjectStoreCanaryStepKind::Remove,
                Some(uri),
                "remove returned false".to_string(),
            )),
            Err(error) => steps.push(RemoteObjectStoreCanaryStep::failed(
                RemoteObjectStoreCanaryStepKind::Remove,
                Some(uri),
                error.to_string(),
            )),
        }
        match client.read_remote_snapshot_object(uri) {
            Ok(None) => steps.push(RemoteObjectStoreCanaryStep::passed(
                RemoteObjectStoreCanaryStepKind::ReadAfterRemove,
                uri,
                0,
            )),
            Ok(Some(bytes)) => steps.push(RemoteObjectStoreCanaryStep::failed(
                RemoteObjectStoreCanaryStepKind::ReadAfterRemove,
                Some(uri),
                format!("read returned {} bytes after remove", bytes.len()),
            )),
            Err(error) => steps.push(RemoteObjectStoreCanaryStep::failed(
                RemoteObjectStoreCanaryStepKind::ReadAfterRemove,
                Some(uri),
                error.to_string(),
            )),
        }
    }
    RemoteObjectStoreCanaryEvidence::from_parts(RemoteObjectStoreCanaryEvidenceParts {
        generated_at_utc,
        target: RemoteObjectStoreCanaryTarget::Snapshot,
        base_uri,
        object_uri,
        payload_bytes: payload.len(),
        payload_hash,
        cleanup_supported: true,
        steps,
    })
}

pub fn run_remote_artifact_object_store_canary(
    base_uri: impl Into<String>,
    client: &dyn RemoteArtifactObjectClient,
) -> RemoteObjectStoreCanaryEvidence {
    let generated_at_utc = timestamp_utc();
    let base_uri = base_uri.into();
    let payload = remote_object_store_canary_payload(
        RemoteObjectStoreCanaryTarget::Artifact,
        &base_uri,
        &generated_at_utc,
    );
    let payload_content = String::from_utf8(payload.clone())
        .expect("remote object-store canary payload is valid UTF-8");
    let payload_hash = stable_id(&[&payload_content]);
    let mut steps = Vec::new();
    let object_uri =
        match normalize_remote_store_base_uri(base_uri.clone(), "artifact").and_then(|base_uri| {
            remote_artifact_object_uri(&base_uri, ArtifactViewMode::Redacted, &payload_hash)
        }) {
            Ok(uri) => Some(uri),
            Err(error) => {
                steps.push(RemoteObjectStoreCanaryStep::failed(
                    RemoteObjectStoreCanaryStepKind::Put,
                    None,
                    error.to_string(),
                ));
                None
            }
        };
    if let Some(uri) = object_uri.as_deref() {
        match client.put_remote_artifact_object(uri, payload.clone()) {
            Ok(()) => steps.push(RemoteObjectStoreCanaryStep::passed(
                RemoteObjectStoreCanaryStepKind::Put,
                uri,
                payload.len(),
            )),
            Err(error) => steps.push(RemoteObjectStoreCanaryStep::failed(
                RemoteObjectStoreCanaryStepKind::Put,
                Some(uri),
                error.to_string(),
            )),
        }
        match client.read_remote_artifact_object(uri) {
            Ok(Some(bytes)) if bytes == payload => {
                steps.push(RemoteObjectStoreCanaryStep::passed(
                    RemoteObjectStoreCanaryStepKind::ReadAfterPut,
                    uri,
                    bytes.len(),
                ));
            }
            Ok(Some(bytes)) => steps.push(RemoteObjectStoreCanaryStep::failed(
                RemoteObjectStoreCanaryStepKind::ReadAfterPut,
                Some(uri),
                format!("read returned {} stale bytes", bytes.len()),
            )),
            Ok(None) => steps.push(RemoteObjectStoreCanaryStep::failed(
                RemoteObjectStoreCanaryStepKind::ReadAfterPut,
                Some(uri),
                "read returned missing object".to_string(),
            )),
            Err(error) => steps.push(RemoteObjectStoreCanaryStep::failed(
                RemoteObjectStoreCanaryStepKind::ReadAfterPut,
                Some(uri),
                error.to_string(),
            )),
        }
        match client.remove_remote_artifact_object(uri) {
            Ok(true) => steps.push(RemoteObjectStoreCanaryStep::passed(
                RemoteObjectStoreCanaryStepKind::Remove,
                uri,
                0,
            )),
            Ok(false) => steps.push(RemoteObjectStoreCanaryStep::failed(
                RemoteObjectStoreCanaryStepKind::Remove,
                Some(uri),
                "remove returned false".to_string(),
            )),
            Err(error) => steps.push(RemoteObjectStoreCanaryStep::failed(
                RemoteObjectStoreCanaryStepKind::Remove,
                Some(uri),
                error.to_string(),
            )),
        }
        match client.read_remote_artifact_object(uri) {
            Ok(None) => steps.push(RemoteObjectStoreCanaryStep::passed(
                RemoteObjectStoreCanaryStepKind::ReadAfterRemove,
                uri,
                0,
            )),
            Ok(Some(bytes)) => steps.push(RemoteObjectStoreCanaryStep::failed(
                RemoteObjectStoreCanaryStepKind::ReadAfterRemove,
                Some(uri),
                format!("read returned {} bytes after remove", bytes.len()),
            )),
            Err(error) => steps.push(RemoteObjectStoreCanaryStep::failed(
                RemoteObjectStoreCanaryStepKind::ReadAfterRemove,
                Some(uri),
                error.to_string(),
            )),
        }
    }
    RemoteObjectStoreCanaryEvidence::from_parts(RemoteObjectStoreCanaryEvidenceParts {
        generated_at_utc,
        target: RemoteObjectStoreCanaryTarget::Artifact,
        base_uri,
        object_uri,
        payload_bytes: payload.len(),
        payload_hash,
        cleanup_supported: true,
        steps,
    })
}

pub fn export_remote_object_store_canary_evidence(
    path: impl AsRef<Path>,
    evidence: &RemoteObjectStoreCanaryEvidence,
) -> RuntimeResult<RemoteObjectStoreCanaryEvidenceExport> {
    let path = path.as_ref();
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|error| {
            RuntimeError::RepoUnavailable(format!(
                "failed to create remote object-store canary evidence directory: {error}"
            ))
        })?;
    }
    let mut bytes = serde_json::to_vec_pretty(evidence).map_err(|_| {
        RuntimeError::Invariant("remote object-store canary evidence serialization failed")
    })?;
    bytes.push(b'\n');
    let failures = evidence.validation_failures();
    let temp_path = remote_object_store_canary_evidence_temp_path(path)?;
    fs::write(&temp_path, &bytes).map_err(|error| {
        RuntimeError::RepoUnavailable(format!(
            "failed to write remote object-store canary evidence: {error}"
        ))
    })?;
    fs::rename(&temp_path, path).map_err(|error| {
        let _ = fs::remove_file(&temp_path);
        RuntimeError::RepoUnavailable(format!(
            "failed to publish remote object-store canary evidence: {error}"
        ))
    })?;
    Ok(RemoteObjectStoreCanaryEvidenceExport {
        path: path.to_path_buf(),
        bytes: bytes.len(),
        valid: failures.is_empty(),
        failures,
    })
}

pub fn load_remote_object_store_canary_evidence(
    path: impl AsRef<Path>,
) -> RuntimeResult<RemoteObjectStoreCanaryEvidence> {
    let path = path.as_ref();
    let contents = fs::read_to_string(path).map_err(|error| {
        RuntimeError::RepoUnavailable(format!(
            "failed to read remote object-store canary evidence {}: {error}",
            path.display()
        ))
    })?;
    serde_json::from_str(&contents).map_err(|error| {
        RuntimeError::InvalidInput(format!(
            "invalid remote object-store canary evidence {}: {error}",
            path.display()
        ))
    })
}

pub const CANARY_EVIDENCE_MANIFEST_SCHEMA_VERSION: &str = "muzen.canary-evidence-manifest.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanaryEvidenceManifest {
    pub schema_version: String,
    pub generated_at_utc: String,
    pub model_provider: Option<ModelProviderCanaryEvidence>,
    pub remote_object_stores: Vec<RemoteObjectStoreCanaryEvidence>,
    pub gate: CanaryEvidenceGate,
}

impl CanaryEvidenceManifest {
    pub fn from_evidence(
        model_provider: Option<ModelProviderCanaryEvidence>,
        remote_object_stores: Vec<RemoteObjectStoreCanaryEvidence>,
    ) -> Self {
        Self::with_generated_at(timestamp_utc(), model_provider, remote_object_stores)
    }

    pub fn with_generated_at(
        generated_at_utc: impl Into<String>,
        model_provider: Option<ModelProviderCanaryEvidence>,
        remote_object_stores: Vec<RemoteObjectStoreCanaryEvidence>,
    ) -> Self {
        let gate = CanaryEvidenceGate::evaluate(&model_provider, &remote_object_stores);
        Self {
            schema_version: CANARY_EVIDENCE_MANIFEST_SCHEMA_VERSION.to_string(),
            generated_at_utc: generated_at_utc.into(),
            model_provider,
            remote_object_stores,
            gate,
        }
    }

    pub fn require_passed(&self) -> RuntimeResult<()> {
        let failures = self.validation_failures();
        if failures.is_empty() {
            return Ok(());
        }
        Err(RuntimeError::InvalidInput(format!(
            "canary evidence manifest gate failed: {}",
            failures.join("; ")
        )))
    }

    pub fn require_passed_with_freshness(
        &self,
        policy: &CanaryEvidenceFreshnessPolicy,
    ) -> RuntimeResult<()> {
        let report = self.status_report(policy);
        let failures = report.failures();
        if failures.is_empty() {
            return Ok(());
        }
        Err(RuntimeError::InvalidInput(format!(
            "canary evidence manifest gate failed: {}",
            failures.join("; ")
        )))
    }

    pub fn status_report(
        &self,
        policy: &CanaryEvidenceFreshnessPolicy,
    ) -> CanaryEvidenceStatusReport {
        let validation_failures = self.validation_failures();
        let freshness_failures = self.freshness_failures(policy);
        CanaryEvidenceStatusReport {
            ok: validation_failures.is_empty() && freshness_failures.is_empty(),
            manifest_schema_version: self.schema_version.clone(),
            generated_at_utc: self.generated_at_utc.clone(),
            freshness_checked_at_utc: policy.now_utc.clone(),
            max_evidence_age_seconds: policy.max_age_seconds,
            evidence: CanaryEvidenceStatusSummary::from_manifest(self),
            gate: self.gate.clone(),
            validation_failures,
            freshness_failures,
        }
    }

    fn validation_failures(&self) -> Vec<String> {
        let mut failures = Vec::new();
        if self.schema_version != CANARY_EVIDENCE_MANIFEST_SCHEMA_VERSION {
            failures.push(format!(
                "unsupported canary evidence manifest schema {}",
                self.schema_version
            ));
        }
        let evaluated =
            CanaryEvidenceGate::evaluate(&self.model_provider, &self.remote_object_stores);
        if evaluated != self.gate {
            failures
                .push("stored canary evidence manifest gate does not match evidence".to_string());
        }
        failures.extend(evaluated.failures);
        failures
    }

    fn freshness_failures(&self, policy: &CanaryEvidenceFreshnessPolicy) -> Vec<String> {
        let mut failures = Vec::new();
        push_timestamp_freshness_failure(
            &mut failures,
            "canary evidence manifest",
            &self.generated_at_utc,
            policy,
        );
        if let Some(evidence) = &self.model_provider {
            push_timestamp_freshness_failure(
                &mut failures,
                "model provider canary evidence",
                &evidence.generated_at_utc,
                policy,
            );
        }
        for evidence in &self.remote_object_stores {
            push_timestamp_freshness_failure(
                &mut failures,
                &format!(
                    "{} remote object-store canary evidence",
                    remote_object_store_canary_target_name(evidence.target)
                ),
                &evidence.generated_at_utc,
                policy,
            );
        }
        failures
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanaryEvidenceStatusReport {
    pub ok: bool,
    pub manifest_schema_version: String,
    pub generated_at_utc: String,
    pub freshness_checked_at_utc: String,
    pub max_evidence_age_seconds: u64,
    pub evidence: CanaryEvidenceStatusSummary,
    pub gate: CanaryEvidenceGate,
    pub validation_failures: Vec<String>,
    pub freshness_failures: Vec<String>,
}

impl CanaryEvidenceStatusReport {
    pub fn failures(&self) -> Vec<String> {
        let mut failures =
            Vec::with_capacity(self.validation_failures.len() + self.freshness_failures.len());
        failures.extend(self.validation_failures.iter().cloned());
        failures.extend(self.freshness_failures.iter().cloned());
        failures
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanaryEvidenceStatusSummary {
    pub model_provider: CanaryModelProviderEvidenceStatus,
    pub remote_object_stores: Vec<CanaryRemoteObjectStoreEvidenceStatus>,
}

impl CanaryEvidenceStatusSummary {
    fn from_manifest(manifest: &CanaryEvidenceManifest) -> Self {
        Self {
            model_provider: CanaryModelProviderEvidenceStatus::from_evidence(
                manifest.model_provider.as_ref(),
            ),
            remote_object_stores: CanaryRemoteObjectStoreEvidenceStatus::from_evidence(
                &manifest.remote_object_stores,
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanaryModelProviderEvidenceStatus {
    pub present: bool,
    pub generated_at_utc: Option<String>,
    pub required_protocols: Vec<ModelApiProtocol>,
    pub reported_protocols: Vec<ModelApiProtocol>,
    pub passed_protocols: Vec<ModelApiProtocol>,
    pub gate: Option<ModelProviderCanaryGate>,
}

impl CanaryModelProviderEvidenceStatus {
    fn from_evidence(evidence: Option<&ModelProviderCanaryEvidence>) -> Self {
        let required_protocols = openai_provider_canary_protocols().to_vec();
        match evidence {
            Some(evidence) => Self {
                present: true,
                generated_at_utc: Some(evidence.generated_at_utc.clone()),
                required_protocols,
                reported_protocols: evidence
                    .reports
                    .iter()
                    .map(|report| report.protocol)
                    .collect(),
                passed_protocols: evidence
                    .reports
                    .iter()
                    .filter(|report| matches!(report.status, ModelProviderCanaryStatus::Passed))
                    .map(|report| report.protocol)
                    .collect(),
                gate: Some(evidence.gate.clone()),
            },
            None => Self {
                present: false,
                generated_at_utc: None,
                required_protocols,
                reported_protocols: Vec::new(),
                passed_protocols: Vec::new(),
                gate: None,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanaryRemoteObjectStoreEvidenceStatus {
    pub target: RemoteObjectStoreCanaryTarget,
    pub evidence_count: usize,
    pub generated_at_utc: Option<String>,
    pub base_uri: Option<String>,
    pub object_uri: Option<String>,
    pub gate: Option<RemoteObjectStoreCanaryGate>,
}

impl CanaryRemoteObjectStoreEvidenceStatus {
    fn from_evidence(evidence: &[RemoteObjectStoreCanaryEvidence]) -> Vec<Self> {
        [
            RemoteObjectStoreCanaryTarget::Snapshot,
            RemoteObjectStoreCanaryTarget::Artifact,
        ]
        .into_iter()
        .map(|target| {
            let mut matching = evidence.iter().filter(|evidence| evidence.target == target);
            let first = matching.next();
            let evidence_count = usize::from(first.is_some()) + matching.count();
            Self {
                target,
                evidence_count,
                generated_at_utc: first.map(|evidence| evidence.generated_at_utc.clone()),
                base_uri: first.map(|evidence| evidence.base_uri.clone()),
                object_uri: first.and_then(|evidence| evidence.object_uri.clone()),
                gate: first.map(|evidence| evidence.gate.clone()),
            }
        })
        .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanaryEvidenceGate {
    pub valid: bool,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub failures: Vec<String>,
}

impl CanaryEvidenceGate {
    fn evaluate(
        model_provider: &Option<ModelProviderCanaryEvidence>,
        remote_object_stores: &[RemoteObjectStoreCanaryEvidence],
    ) -> Self {
        let mut passed = 0usize;
        let mut failed = 0usize;
        let mut skipped = 0usize;
        let mut failures = Vec::new();

        match model_provider {
            Some(evidence) => {
                passed = passed.saturating_add(evidence.gate.passed);
                failed = failed.saturating_add(evidence.gate.failed);
                skipped = skipped.saturating_add(evidence.gate.skipped);
                if let Err(error) = evidence.require_passed() {
                    failures.push(format!("model provider canary invalid: {error}"));
                }
            }
            None => {
                failed = failed.saturating_add(1);
                failures.push("missing model provider canary evidence".to_string());
            }
        }

        for target in [
            RemoteObjectStoreCanaryTarget::Snapshot,
            RemoteObjectStoreCanaryTarget::Artifact,
        ] {
            let matching = remote_object_stores
                .iter()
                .filter(|evidence| evidence.target == target)
                .collect::<Vec<_>>();
            let target_name = remote_object_store_canary_target_name(target);
            match matching.len() {
                0 => {
                    failed = failed.saturating_add(1);
                    failures.push(format!(
                        "missing {target_name} remote object-store canary evidence"
                    ));
                }
                1 => {}
                count => {
                    failed = failed.saturating_add(count.saturating_sub(1));
                    failures.push(format!(
                        "duplicate {target_name} remote object-store canary evidence: {count}"
                    ));
                }
            }
            for evidence in matching {
                passed = passed.saturating_add(evidence.gate.passed);
                failed = failed.saturating_add(evidence.gate.failed);
                skipped = skipped.saturating_add(evidence.gate.skipped);
                if let Err(error) = evidence.require_passed() {
                    failures.push(format!(
                        "{target_name} remote object-store canary invalid: {error}"
                    ));
                }
            }
        }

        Self {
            valid: failures.is_empty(),
            passed,
            failed,
            skipped,
            failures,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanaryEvidenceFreshnessPolicy {
    pub now_utc: String,
    pub max_age_seconds: u64,
}

impl CanaryEvidenceFreshnessPolicy {
    pub fn current(max_age_seconds: u64) -> Self {
        Self {
            now_utc: timestamp_utc(),
            max_age_seconds,
        }
    }

    pub fn at(now_utc: impl Into<String>, max_age_seconds: u64) -> Self {
        Self {
            now_utc: now_utc.into(),
            max_age_seconds,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanaryEvidenceManifestExport {
    pub path: PathBuf,
    pub bytes: usize,
    pub valid: bool,
    pub failures: Vec<String>,
}

pub fn export_canary_evidence_manifest(
    path: impl AsRef<Path>,
    manifest: &CanaryEvidenceManifest,
) -> RuntimeResult<CanaryEvidenceManifestExport> {
    let path = path.as_ref();
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|error| {
            RuntimeError::RepoUnavailable(format!(
                "failed to create canary evidence manifest directory: {error}"
            ))
        })?;
    }
    let mut bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|_| RuntimeError::Invariant("canary evidence manifest serialization failed"))?;
    bytes.push(b'\n');
    let failures = manifest.validation_failures();
    let temp_path = canary_evidence_manifest_temp_path(path)?;
    fs::write(&temp_path, &bytes).map_err(|error| {
        RuntimeError::RepoUnavailable(format!("failed to write canary evidence manifest: {error}"))
    })?;
    fs::rename(&temp_path, path).map_err(|error| {
        let _ = fs::remove_file(&temp_path);
        RuntimeError::RepoUnavailable(format!(
            "failed to publish canary evidence manifest: {error}"
        ))
    })?;
    Ok(CanaryEvidenceManifestExport {
        path: path.to_path_buf(),
        bytes: bytes.len(),
        valid: failures.is_empty(),
        failures,
    })
}

pub fn load_canary_evidence_manifest(
    path: impl AsRef<Path>,
) -> RuntimeResult<CanaryEvidenceManifest> {
    let path = path.as_ref();
    let contents = fs::read_to_string(path).map_err(|error| {
        RuntimeError::RepoUnavailable(format!(
            "failed to read canary evidence manifest {}: {error}",
            path.display()
        ))
    })?;
    serde_json::from_str(&contents).map_err(|error| {
        RuntimeError::InvalidInput(format!(
            "invalid canary evidence manifest {}: {error}",
            path.display()
        ))
    })
}

fn remote_object_store_canary_target_name(target: RemoteObjectStoreCanaryTarget) -> &'static str {
    match target {
        RemoteObjectStoreCanaryTarget::Snapshot => "snapshot",
        RemoteObjectStoreCanaryTarget::Artifact => "artifact",
    }
}

fn push_timestamp_freshness_failure(
    failures: &mut Vec<String>,
    label: &str,
    generated_at_utc: &str,
    policy: &CanaryEvidenceFreshnessPolicy,
) {
    let now_seconds = match parse_unix_timestamp_seconds(&policy.now_utc) {
        Ok(seconds) => seconds,
        Err(error) => {
            failures.push(format!("invalid freshness reference time: {error}"));
            return;
        }
    };
    let generated_seconds = match parse_unix_timestamp_seconds(generated_at_utc) {
        Ok(seconds) => seconds,
        Err(error) => {
            failures.push(format!("{label} generatedAtUtc is invalid: {error}"));
            return;
        }
    };
    if generated_seconds > now_seconds {
        failures.push(format!(
            "{label} generatedAtUtc is in the future by {} seconds",
            generated_seconds.saturating_sub(now_seconds)
        ));
        return;
    }
    let age = now_seconds.saturating_sub(generated_seconds);
    if age > policy.max_age_seconds {
        failures.push(format!(
            "{label} is stale: age {age} seconds exceeds max {} seconds",
            policy.max_age_seconds
        ));
    }
}

fn parse_unix_timestamp_seconds(timestamp: &str) -> Result<u64, String> {
    let without_z = timestamp
        .strip_suffix('Z')
        .ok_or_else(|| format!("{timestamp} does not end with Z"))?;
    let seconds = without_z
        .split_once('.')
        .map(|(seconds, _)| seconds)
        .unwrap_or(without_z);
    if seconds.is_empty() {
        return Err(format!("{timestamp} has no seconds"));
    }
    seconds
        .parse::<u64>()
        .map_err(|error| format!("{timestamp} has invalid seconds: {error}"))
}

fn push_remote_snapshot_read_step(
    client: &dyn RemoteSnapshotObjectClient,
    uri: &str,
    payload: &[u8],
    steps: &mut Vec<RemoteObjectStoreCanaryStep>,
) {
    match client.read_remote_snapshot_object(uri) {
        Ok(Some(bytes)) if bytes == payload => steps.push(RemoteObjectStoreCanaryStep::passed(
            RemoteObjectStoreCanaryStepKind::ReadAfterPut,
            uri,
            bytes.len(),
        )),
        Ok(Some(bytes)) => steps.push(RemoteObjectStoreCanaryStep::failed(
            RemoteObjectStoreCanaryStepKind::ReadAfterPut,
            Some(uri),
            format!("read returned {} stale bytes", bytes.len()),
        )),
        Ok(None) => steps.push(RemoteObjectStoreCanaryStep::failed(
            RemoteObjectStoreCanaryStepKind::ReadAfterPut,
            Some(uri),
            "read returned missing object".to_string(),
        )),
        Err(error) => steps.push(RemoteObjectStoreCanaryStep::failed(
            RemoteObjectStoreCanaryStepKind::ReadAfterPut,
            Some(uri),
            error.to_string(),
        )),
    }
}

fn remote_object_store_canary_payload(
    target: RemoteObjectStoreCanaryTarget,
    base_uri: &str,
    generated_at_utc: &str,
) -> Vec<u8> {
    format!(
        "muzen remote object-store canary\ntarget={}\nbaseUri={base_uri}\ngeneratedAt={generated_at_utc}\n",
        target.as_str()
    )
    .into_bytes()
}

fn remote_object_store_canary_evidence_temp_path(path: &Path) -> RuntimeResult<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        RuntimeError::InvalidInput(
            "remote object-store canary evidence path must name a file".to_string(),
        )
    })?;
    Ok(path.with_file_name(format!(
        ".{}.{}.tmp",
        file_name.to_string_lossy(),
        std::process::id()
    )))
}

fn canary_evidence_manifest_temp_path(path: &Path) -> RuntimeResult<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        RuntimeError::InvalidInput("canary evidence manifest path must name a file".to_string())
    })?;
    Ok(path.with_file_name(format!(
        ".{}.{}.tmp",
        file_name.to_string_lossy(),
        std::process::id()
    )))
}

fn validate_artifact_store_object(object: &ArtifactStoreObject) -> RuntimeResult<()> {
    if object.content.len() != object.bytes || stable_id(&[&object.content]) != object.content_hash
    {
        return Err(RuntimeError::InvalidInput(
            "artifact object content does not match metadata".to_string(),
        ));
    }
    Ok(())
}

fn artifact_object_bytes_match(object_ref: &ArtifactObjectRef, bytes: &[u8]) -> bool {
    if bytes.len() != object_ref.bytes {
        return false;
    }
    let Ok(content) = std::str::from_utf8(bytes) else {
        return false;
    };
    stable_id(&[content]) == object_ref.content_hash
}

fn remote_object_http_error(error: reqwest::Error) -> RuntimeError {
    RuntimeError::RepoUnavailable(format!("remote object-store HTTP request failed: {error}"))
}

fn remote_object_http_status_error(
    operation: &str,
    _uri: &str,
    status: reqwest::StatusCode,
) -> RuntimeError {
    RuntimeError::RepoUnavailable(format!(
        "remote object-store HTTP {operation} failed with status {status}"
    ))
}

fn normalize_remote_store_base_uri(base_uri: String, object_kind: &str) -> RuntimeResult<String> {
    let normalized = base_uri.trim_end_matches('/').to_string();
    if normalized.is_empty()
        || !normalized.contains("://")
        || normalized.starts_with("file://")
        || normalized.chars().any(char::is_whitespace)
    {
        return Err(RuntimeError::InvalidInput(format!(
            "remote {object_kind} object store requires a non-file URI base"
        )));
    }
    Ok(normalized)
}

fn remote_artifact_object_uri(
    base_uri: &str,
    view: ArtifactViewMode,
    content_hash: &str,
) -> RuntimeResult<String> {
    if content_hash.len() != 64 || !content_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(RuntimeError::RepoAccessDenied);
    }
    Ok(format!(
        "{base_uri}/artifacts/{}/{}.txt",
        artifact_view_name(view),
        content_hash
    ))
}

fn local_artifact_object_path(
    root: &Path,
    view: ArtifactViewMode,
    content_hash: &str,
) -> RuntimeResult<PathBuf> {
    if content_hash.len() != 64 || !content_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(RuntimeError::RepoAccessDenied);
    }
    Ok(root
        .join(artifact_view_name(view))
        .join(&content_hash[..2])
        .join(format!("{content_hash}.txt")))
}

fn artifact_view_mode(policy: &ArtifactExportPolicy) -> ArtifactViewMode {
    if policy.include_raw() {
        ArtifactViewMode::Raw
    } else {
        ArtifactViewMode::Redacted
    }
}

fn artifact_view_name(view: ArtifactViewMode) -> &'static str {
    match view {
        ArtifactViewMode::Redacted => "redacted",
        ArtifactViewMode::Raw => "raw",
    }
}

fn evidence_kind_name(kind: crate::contracts::ArtifactKind) -> &'static str {
    match kind {
        crate::contracts::ArtifactKind::FileSlice => "file_slice",
        crate::contracts::ArtifactKind::DiffHunk => "diff_hunk",
        crate::contracts::ArtifactKind::SearchResults => "search_results",
        crate::contracts::ArtifactKind::FileList => "file_list",
        crate::contracts::ArtifactKind::ChangedFileList => "changed_file_list",
        crate::contracts::ArtifactKind::ImportSummary => "import_summary",
        crate::contracts::ArtifactKind::ToolSummary => "tool_summary",
        crate::contracts::ArtifactKind::RedactedView => "redacted_view",
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactBundleManifest {
    pub view: ArtifactViewMode,
    pub root: PathBuf,
    pub manifest_path: PathBuf,
    pub retention: ArtifactRetentionPolicy,
    pub artifact_count: usize,
    pub total_bytes: usize,
    pub artifacts: Vec<ArtifactBundleEntry>,
}

impl ArtifactBundleManifest {
    pub fn new(
        view: ArtifactViewMode,
        root: impl Into<PathBuf>,
        retention: ArtifactRetentionPolicy,
        artifacts: Vec<ArtifactBundleEntry>,
    ) -> Self {
        let root = root.into();
        let artifact_count = artifacts.len();
        let total_bytes = artifacts.iter().map(|entry| entry.bytes).sum();
        let manifest_path = root.join(ARTIFACT_BUNDLE_MANIFEST_FILE);
        Self {
            view,
            root,
            manifest_path,
            retention,
            artifact_count,
            total_bytes,
            artifacts,
        }
    }

    pub fn with_manifest_path(mut self, manifest_path: impl Into<PathBuf>) -> Self {
        self.manifest_path = manifest_path.into();
        self
    }

    pub fn validate_storage(&self) -> RuntimeResult<ArtifactBundleValidationReport> {
        let manifest_path = safe_bundle_manifest_path(&self.root, &self.manifest_path)?;
        let mut report = ArtifactBundleValidationReport {
            root: self.root.clone(),
            manifest_path: manifest_path.clone(),
            view: self.view,
            retention: self.retention.clone(),
            checked_artifacts: 0,
            checked_bytes: 0,
            checked_objects: Vec::new(),
            manifest_present: manifest_path.exists(),
            valid: true,
            missing_artifacts: Vec::new(),
            stale_artifacts: Vec::new(),
        };
        for entry in &self.artifacts {
            let object = ArtifactBundleObject::from_entry(&self.root, entry)?;
            report.checked_artifacts += 1;
            report.checked_bytes = report.checked_bytes.saturating_add(entry.bytes);
            report.checked_objects.push(object.clone());
            let bytes = match fs::read(&object.path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    report.missing_artifacts.push(object);
                    continue;
                }
                Err(error) => {
                    return Err(RuntimeError::RepoUnavailable(format!(
                        "failed to read artifact bundle object: {error}"
                    )))
                }
            };
            let content_hash = std::str::from_utf8(&bytes)
                .ok()
                .map(|content| stable_id(&[content]));
            if bytes.len() != entry.bytes
                || content_hash.as_deref() != Some(entry.content_hash.as_str())
            {
                report.stale_artifacts.push(object);
            }
        }
        report.valid = report.manifest_present
            && report.missing_artifacts.is_empty()
            && report.stale_artifacts.is_empty();
        Ok(report)
    }

    pub fn cleanup_storage(&self) -> RuntimeResult<ArtifactBundleCleanupReport> {
        let manifest_path = safe_bundle_manifest_path(&self.root, &self.manifest_path)?;
        let mut report = ArtifactBundleCleanupReport {
            root: self.root.clone(),
            manifest_path: manifest_path.clone(),
            view: self.view,
            retention: self.retention.clone(),
            removed_artifacts: 0,
            removed_bytes: 0,
            removed_objects: Vec::new(),
            missing_artifacts: Vec::new(),
            removed_manifest: false,
            pruned_empty_directories: 0,
        };
        let mut candidate_dirs = Vec::new();
        for entry in &self.artifacts {
            let object = ArtifactBundleObject::from_entry(&self.root, entry)?;
            match fs::metadata(&object.path) {
                Ok(metadata) => {
                    fs::remove_file(&object.path).map_err(|error| {
                        RuntimeError::RepoUnavailable(format!(
                            "failed to remove artifact bundle object: {error}"
                        ))
                    })?;
                    report.removed_artifacts += 1;
                    report.removed_bytes =
                        report.removed_bytes.saturating_add(metadata.len() as usize);
                    if let Some(parent) = object.path.parent() {
                        candidate_dirs.push(parent.to_path_buf());
                    }
                    report.removed_objects.push(object);
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    report.missing_artifacts.push(object);
                }
                Err(error) => {
                    return Err(RuntimeError::RepoUnavailable(format!(
                        "failed to inspect artifact bundle object: {error}"
                    )))
                }
            }
        }
        match fs::remove_file(&manifest_path) {
            Ok(()) => report.removed_manifest = true,
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(RuntimeError::RepoUnavailable(format!(
                    "failed to remove artifact bundle manifest: {error}"
                )))
            }
        }
        candidate_dirs.sort();
        candidate_dirs.dedup();
        for directory in candidate_dirs {
            if directory.starts_with(&self.root) && prune_empty_directory(&directory)? {
                report.pruned_empty_directories += 1;
            }
        }
        Ok(report)
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactBundleEntry {
    pub artifact_id: ArtifactId,
    pub bytes: usize,
    pub content_hash: String,
    pub relative_path: PathBuf,
}

impl ArtifactBundleEntry {
    pub fn new(
        artifact_id: impl Into<String>,
        bytes: usize,
        content_hash: impl Into<String>,
        relative_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            artifact_id: ArtifactId(artifact_id.into()),
            bytes,
            content_hash: content_hash.into(),
            relative_path: relative_path.into(),
        }
    }

    pub fn artifact_id(&self) -> &str {
        &self.artifact_id.0
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactBundleObject {
    pub artifact_id: ArtifactId,
    pub bytes: usize,
    pub content_hash: String,
    pub relative_path: PathBuf,
    pub path: PathBuf,
}

impl ArtifactBundleObject {
    fn from_entry(root: &Path, entry: &ArtifactBundleEntry) -> RuntimeResult<Self> {
        Ok(Self {
            artifact_id: entry.artifact_id.clone(),
            bytes: entry.bytes,
            content_hash: entry.content_hash.clone(),
            relative_path: entry.relative_path.clone(),
            path: safe_bundle_entry_path(root, &entry.relative_path)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactBundleValidationReport {
    pub root: PathBuf,
    pub manifest_path: PathBuf,
    pub view: ArtifactViewMode,
    pub retention: ArtifactRetentionPolicy,
    pub checked_artifacts: usize,
    pub checked_bytes: usize,
    pub checked_objects: Vec<ArtifactBundleObject>,
    pub manifest_present: bool,
    pub valid: bool,
    pub missing_artifacts: Vec<ArtifactBundleObject>,
    pub stale_artifacts: Vec<ArtifactBundleObject>,
}

#[derive(Debug, Clone)]
pub struct ArtifactBundleCleanupReport {
    pub root: PathBuf,
    pub manifest_path: PathBuf,
    pub view: ArtifactViewMode,
    pub retention: ArtifactRetentionPolicy,
    pub removed_artifacts: usize,
    pub removed_bytes: usize,
    pub removed_objects: Vec<ArtifactBundleObject>,
    pub missing_artifacts: Vec<ArtifactBundleObject>,
    pub removed_manifest: bool,
    pub pruned_empty_directories: usize,
}

fn safe_bundle_entry_path(root: &Path, relative_path: &Path) -> RuntimeResult<PathBuf> {
    if relative_path.as_os_str().is_empty() || relative_path.is_absolute() {
        return Err(RuntimeError::RepoAccessDenied);
    }
    for component in relative_path.components() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(RuntimeError::RepoAccessDenied);
        }
    }
    Ok(root.join(relative_path))
}

fn safe_bundle_manifest_path(root: &Path, manifest_path: &Path) -> RuntimeResult<PathBuf> {
    let expected = root.join(ARTIFACT_BUNDLE_MANIFEST_FILE);
    if manifest_path == expected {
        Ok(manifest_path.to_path_buf())
    } else {
        Err(RuntimeError::RepoAccessDenied)
    }
}

pub mod runtime_events {
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Mutex;

    use crate::runtime::contracts::{RuntimeError, RuntimeResult};
    pub use crate::runtime::contracts::{
        RuntimeEvent, RuntimeEventContext, RuntimeEventRecord, RuntimeEventSink as EventSink,
        TurnId,
    };
    use crate::util::{timestamp_utc, SCHEMA_VERSION};

    #[derive(Debug, Default)]
    pub struct InMemoryEventSink {
        records: Mutex<Vec<RuntimeEventRecord>>,
    }

    impl InMemoryEventSink {
        pub fn events(&self) -> Vec<RuntimeEvent> {
            self.records()
                .into_iter()
                .map(|record| record.event)
                .collect()
        }

        pub fn records(&self) -> Vec<RuntimeEventRecord> {
            self.records
                .lock()
                .expect("in-memory event sink poisoned")
                .clone()
        }

        pub fn export_jsonl(
            &self,
            path: impl AsRef<Path>,
        ) -> RuntimeResult<RuntimeEventJsonlManifest> {
            write_event_records_jsonl(path.as_ref(), &self.records(), 0)
        }

        fn record(&self, context: RuntimeEventContext, event: RuntimeEvent) {
            let mut records = self.records.lock().expect("in-memory event sink poisoned");
            let seq = records.len() as u64 + 1;
            records.push(RuntimeEventRecord {
                seq,
                timestamp_utc: timestamp_utc(),
                context,
                event,
            });
        }
    }

    impl EventSink for InMemoryEventSink {
        fn emit(&self, event: RuntimeEvent) {
            let context = RuntimeEventContext::from_event(&event);
            self.record(context, event);
        }

        fn emit_with_context(&self, context: RuntimeEventContext, event: RuntimeEvent) {
            self.record(context, event);
        }
    }

    #[derive(Debug)]
    pub struct BoundedInMemoryEventSink {
        capacity: usize,
        policy: EventBackpressurePolicy,
        next_seq: AtomicU64,
        dropped: AtomicUsize,
        records: Mutex<Vec<RuntimeEventRecord>>,
    }

    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub enum EventBackpressurePolicy {
        DropNewest,
        DropOldest,
    }

    impl BoundedInMemoryEventSink {
        pub fn new(capacity: usize) -> Self {
            Self::with_policy(capacity, EventBackpressurePolicy::DropNewest)
        }

        pub fn with_policy(capacity: usize, policy: EventBackpressurePolicy) -> Self {
            Self {
                capacity: capacity.max(1),
                policy,
                next_seq: AtomicU64::new(1),
                dropped: AtomicUsize::new(0),
                records: Mutex::new(Vec::new()),
            }
        }

        pub fn records(&self) -> Vec<RuntimeEventRecord> {
            self.records
                .lock()
                .expect("bounded in-memory event sink poisoned")
                .clone()
        }

        pub fn dropped_count(&self) -> usize {
            self.dropped.load(Ordering::Relaxed)
        }

        pub fn export_jsonl(
            &self,
            path: impl AsRef<Path>,
        ) -> RuntimeResult<RuntimeEventJsonlManifest> {
            write_event_records_jsonl(path.as_ref(), &self.records(), self.dropped_count())
        }

        fn record(&self, context: RuntimeEventContext, event: RuntimeEvent) {
            let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
            let mut records = self
                .records
                .lock()
                .expect("bounded in-memory event sink poisoned");
            if records.len() >= self.capacity {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                match self.policy {
                    EventBackpressurePolicy::DropNewest => return,
                    EventBackpressurePolicy::DropOldest => {
                        records.remove(0);
                    }
                }
            }
            records.push(RuntimeEventRecord {
                seq,
                timestamp_utc: timestamp_utc(),
                context,
                event,
            });
        }
    }

    impl EventSink for BoundedInMemoryEventSink {
        fn emit(&self, event: RuntimeEvent) {
            let context = RuntimeEventContext::from_event(&event);
            self.record(context, event);
        }

        fn emit_with_context(&self, context: RuntimeEventContext, event: RuntimeEvent) {
            self.record(context, event);
        }
    }

    #[derive(Debug, Clone)]
    pub struct RuntimeEventJsonlManifest {
        pub path: PathBuf,
        pub record_count: usize,
        pub dropped_count: usize,
        pub bytes: usize,
    }

    #[derive(Debug, Clone)]
    pub struct RuntimeEventJsonlLoad {
        pub path: PathBuf,
        pub record_count: usize,
        pub migration: RuntimeEventJsonlMigrationReport,
        pub records: Vec<RuntimeEventRecord>,
    }

    #[derive(Debug, Clone)]
    pub struct RuntimeEventJsonlMigrationReport {
        pub current_schema_version: String,
        pub source_schema_versions: BTreeMap<String, usize>,
        pub migrated_records: usize,
    }

    pub const LEGACY_CONTEXTLESS_EVENT_LOG_SCHEMA_VERSION: &str =
        "heimdaal.review-run.v0.contextless";

    pub fn export_event_records_jsonl(
        path: impl AsRef<Path>,
        records: &[RuntimeEventRecord],
    ) -> RuntimeResult<RuntimeEventJsonlManifest> {
        write_event_records_jsonl(path.as_ref(), records, 0)
    }

    pub fn load_event_records_jsonl(
        path: impl AsRef<Path>,
    ) -> RuntimeResult<RuntimeEventJsonlLoad> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|error| {
            RuntimeError::RepoUnavailable(format!("failed to read event log: {error}"))
        })?;
        let mut records = Vec::new();
        let mut source_schema_versions = BTreeMap::new();
        let mut migrated_records = 0usize;
        for (index, line) in text.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let record: RuntimeEventJsonlRecord = serde_json::from_str(line).map_err(|error| {
                RuntimeError::InvalidInput(format!(
                    "invalid event log record at line {}: {error}",
                    index + 1
                ))
            })?;
            *source_schema_versions
                .entry(record.schema_version.clone())
                .or_insert(0) += 1;
            let context = match record.schema_version.as_str() {
                SCHEMA_VERSION => record.context.ok_or_else(|| {
                    RuntimeError::InvalidInput(format!(
                        "missing event log context at line {} for schemaVersion {}",
                        index + 1,
                        SCHEMA_VERSION
                    ))
                })?,
                LEGACY_CONTEXTLESS_EVENT_LOG_SCHEMA_VERSION => {
                    migrated_records += 1;
                    record
                        .context
                        .unwrap_or_else(|| RuntimeEventContext::from_event(&record.event))
                }
                _ => {
                    return Err(RuntimeError::InvalidInput(format!(
                        "unsupported event log schemaVersion {} at line {}",
                        record.schema_version,
                        index + 1
                    )))
                }
            };
            records.push(RuntimeEventRecord {
                seq: record.seq,
                timestamp_utc: record.timestamp_utc,
                context,
                event: record.event,
            });
        }
        Ok(RuntimeEventJsonlLoad {
            path: path.to_path_buf(),
            record_count: records.len(),
            migration: RuntimeEventJsonlMigrationReport {
                current_schema_version: SCHEMA_VERSION.to_string(),
                source_schema_versions,
                migrated_records,
            },
            records,
        })
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct RuntimeEventJsonlRecord {
        schema_version: String,
        seq: u64,
        timestamp_utc: String,
        context: Option<RuntimeEventContext>,
        event: RuntimeEvent,
    }

    fn write_event_records_jsonl(
        path: &Path,
        records: &[RuntimeEventRecord],
        dropped_count: usize,
    ) -> RuntimeResult<RuntimeEventJsonlManifest> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                RuntimeError::RepoUnavailable(format!(
                    "failed to create event log directory: {error}"
                ))
            })?;
        }
        let mut file = std::fs::File::create(path).map_err(|error| {
            RuntimeError::RepoUnavailable(format!("failed to create event log: {error}"))
        })?;
        let mut bytes = 0usize;
        for record in records {
            let line = serde_json::to_vec(&serde_json::json!({
                "schemaVersion": SCHEMA_VERSION,
                "seq": record.seq,
                "timestampUtc": record.timestamp_utc,
                "context": &record.context,
                "event": &record.event,
            }))
            .map_err(|error| {
                RuntimeError::RepoUnavailable(format!("failed to serialize event log: {error}"))
            })?;
            file.write_all(&line).map_err(|error| {
                RuntimeError::RepoUnavailable(format!("failed to write event log: {error}"))
            })?;
            file.write_all(b"\n").map_err(|error| {
                RuntimeError::RepoUnavailable(format!("failed to write event log: {error}"))
            })?;
            bytes += line.len() + 1;
        }
        file.flush().map_err(|error| {
            RuntimeError::RepoUnavailable(format!("failed to flush event log: {error}"))
        })?;
        Ok(RuntimeEventJsonlManifest {
            path: path.to_path_buf(),
            record_count: records.len(),
            dropped_count,
            bytes,
        })
    }
}

pub(crate) fn run_review_job_with_events(
    job: ReviewRunJobV1,
    emitter: Option<Arc<EventEmitter>>,
) -> anyhow::Result<ConcurrentRunReport> {
    validate_job(&job)?;
    let registry = Arc::new(
        RuntimeToolRegistry::review_defaults()
            .map_err(|error| anyhow::anyhow!("failed to build tool registry: {error}"))?,
    );
    let mut limits = RuntimeLimits::standard(
        job.budgets.max_active_sessions.max(1),
        job.path_policy.max_file_bytes,
        job.path_policy.max_search_results,
    );
    limits.max_tool_calls_per_turn = 4;
    limits.max_model_concurrency_global = job.budgets.max_active_sessions.max(1);
    let base_url = std::env::var("OAI_BASE_URL")
        .or_else(|_| std::env::var("OPENAI_BASE_URL"))
        .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
    let router = ProfileModelRouter::from_profiles(
        &job.model_profiles,
        job.default_model_profile_id.clone(),
        base_url,
        Arc::new(ModelLimiter::new_with_per_key(
            limits.max_model_concurrency_global,
            limits.max_model_concurrency_per_key,
        )),
        Arc::clone(&registry),
        Arc::new(ReviewerPolicy::new()),
        Arc::new(EnvCredentialResolver),
    )
    .map_err(|error| anyhow::anyhow!("{error}"))?;
    let run = Run::builder(run_spec_from_job(&job, limits))
        .model_router(Arc::new(router))
        .shared_tool_registry(registry)
        .legacy_event_emitter(emitter.clone())
        .build()
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let session_count = run
        .shards
        .iter()
        .map(|shard| shard.sessions.len())
        .sum::<usize>();
    if let Some(emitter) = &emitter {
        emitter.emit(EventRecord::new(
            EventLevel::Info,
            EventType::RunStarted,
            serde_json::json!({
            "projectId": job.project_id,
            "sessions": session_count,
            "runtime": "concurrent"
            }),
        ));
    }
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get().clamp(2, 8))
        .enable_all()
        .build()
        .map_err(|error| anyhow::anyhow!("failed to build tokio runtime: {error}"))?;
    let run_report = tokio_runtime.block_on(run.execute());
    let report = run_report.metrics.clone();
    if let Some(emitter) = &emitter {
        emitter.emit(EventRecord::new(
            EventLevel::Info,
            EventType::RunFinished,
            serde_json::json!(ReviewRunResultV1 {
                schema_version: SCHEMA_VERSION,
                run_id: job.run_id,
                attempt: job.attempt,
                runtime: ReviewRuntimeV1::Concurrent,
                outcome: concurrent_review_outcome(&report, run_report.findings.len()),
                publishability: if report.completed_sessions == report.sessions {
                    Publishability::Publishable
                } else {
                    Publishability::DiagnosticOnly
                },
                sessions: report.sessions,
                completed_sessions: report.completed_sessions,
                findings: run_report.findings,
                tool_counts: report.tool_counts,
                model_calls: report.model_calls,
                tokens: TokenUsage {
                    input_tokens: report.input_tokens,
                    output_tokens: report.output_tokens,
                    total_tokens: report.total_tokens,
                },
                artifact_stats: crate::contracts::ArtifactStats {
                    artifacts: report.artifacts,
                    artifact_bytes: report.artifact_bytes,
                    content_refs: report.artifacts,
                },
                elapsed_ms: report.elapsed_ms,
            }),
        ));
    }
    Ok(report)
}

pub(crate) fn run_spec_from_job(job: &ReviewRunJobV1, limits: RuntimeLimits) -> RunSpec {
    RunSpec::single_snapshot(
        job.run_id.clone(),
        SnapshotSpec {
            snapshot_id: None,
            repo_root: job.repo.worktree_root.clone(),
            default_cwd: Some(job.repo.default_cwd.clone()),
            change: job.change.clone().into(),
            path_policy: job.path_policy.clone().into(),
            storage_policy: SnapshotStoragePolicy::default(),
        },
        effective_personas(job)
            .into_iter()
            .map(|persona| {
                ReviewSessionSpec::review_read_only(
                    persona.id,
                    persona.role,
                    persona.objective,
                    persona.budget,
                )
                .with_model_profile_id(
                    persona
                        .model_profile_id
                        .unwrap_or_else(|| job.default_model_profile_id.clone()),
                )
                .with_capabilities(capabilities_from_mask(persona.allowed_tools))
            })
            .collect(),
        limits,
    )
}

pub(crate) fn capabilities_from_mask(mask: ToolMask) -> CapabilitySet {
    let mut capabilities = CapabilitySet::empty_review_policy(FsScope::repo_root());
    for &tool in ToolName::review_read_only_tools() {
        if tool_allowed(mask, tool) {
            capabilities.grant(ToolId::from(tool), ToolGrant::allow_review_read_only());
        }
    }
    capabilities
}

fn concurrent_review_outcome(report: &ConcurrentRunReport, findings: usize) -> ReviewOutcomeV1 {
    if report.completed_sessions < report.sessions {
        ReviewOutcomeV1::FailedPartial
    } else if findings > 0 {
        ReviewOutcomeV1::CompletedWithFindings
    } else {
        ReviewOutcomeV1::CompletedNoFindings
    }
}
