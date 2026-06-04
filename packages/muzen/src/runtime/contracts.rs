use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::contracts::{AgentBudget, Role, TokenUsage, ToolCounts, ToolName};

pub const CONCURRENT_CONTRACT_VERSION: u16 = 1;
pub const REDACTION_POLICY_VERSION: u16 = 1;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("resource limit exceeded: {kind}")]
    LimitExceeded { kind: &'static str },
    #[error("operation timed out")]
    Timeout,
    #[error("operation cancelled")]
    Cancelled,
    #[error("provider error: {status:?}")]
    Provider {
        status: Option<u16>,
        retryable: bool,
    },
    #[error("repository access denied")]
    RepoAccessDenied,
    #[error("repository unavailable: {0}")]
    RepoUnavailable(String),
    #[error("snapshot content changed after capture: {path}")]
    SnapshotStale { path: String },
    #[error("internal invariant violation: {0}")]
    Invariant(&'static str),
}

pub type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SnapshotId(pub String);

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotStoragePolicy {
    pub mode: SnapshotStorageMode,
    pub max_captured_text_bytes: usize,
    #[serde(skip, default)]
    remote_object_store: Option<Arc<dyn SnapshotObjectStore>>,
}

impl SnapshotStoragePolicy {
    pub fn memory(max_captured_text_bytes: usize) -> Self {
        Self {
            mode: SnapshotStorageMode::Memory,
            max_captured_text_bytes,
            remote_object_store: None,
        }
    }

    pub fn content_addressed_directory(
        root: impl Into<PathBuf>,
        max_captured_text_bytes: usize,
    ) -> Self {
        Self {
            mode: SnapshotStorageMode::ContentAddressedDirectory { root: root.into() },
            max_captured_text_bytes,
            remote_object_store: None,
        }
    }

    pub fn remote_object_store(
        base_uri: impl Into<String>,
        max_captured_text_bytes: usize,
        object_store: Arc<dyn SnapshotObjectStore>,
    ) -> RuntimeResult<Self> {
        Ok(Self {
            mode: SnapshotStorageMode::RemoteObjectStore {
                base_uri: normalize_remote_object_base_uri(base_uri.into())?,
            },
            max_captured_text_bytes,
            remote_object_store: Some(object_store),
        })
    }

    pub(crate) fn remote_store(&self) -> Option<Arc<dyn SnapshotObjectStore>> {
        self.remote_object_store.clone()
    }
}

impl std::fmt::Debug for SnapshotStoragePolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotStoragePolicy")
            .field("mode", &self.mode)
            .field("max_captured_text_bytes", &self.max_captured_text_bytes)
            .field(
                "has_remote_object_store",
                &self.remote_object_store.is_some(),
            )
            .finish()
    }
}

impl PartialEq for SnapshotStoragePolicy {
    fn eq(&self, other: &Self) -> bool {
        self.mode == other.mode && self.max_captured_text_bytes == other.max_captured_text_bytes
    }
}

impl Eq for SnapshotStoragePolicy {}

pub trait SnapshotObjectStore: Send + Sync {
    fn put_snapshot_object(&self, uri: &str, bytes: Vec<u8>) -> RuntimeResult<()>;

    fn read_snapshot_object(&self, uri: &str) -> RuntimeResult<Option<Vec<u8>>>;

    fn remove_snapshot_object(&self, uri: &str) -> RuntimeResult<bool>;
}

impl Default for SnapshotStoragePolicy {
    fn default() -> Self {
        Self::memory(64 * 1024 * 1024)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStorageMode {
    Memory,
    ContentAddressedDirectory { root: PathBuf },
    RemoteObjectStore { base_uri: String },
}

fn normalize_remote_object_base_uri(base_uri: String) -> RuntimeResult<String> {
    let normalized = base_uri.trim_end_matches('/').to_string();
    if normalized.is_empty()
        || !normalized.contains("://")
        || normalized.starts_with("file://")
        || normalized.chars().any(char::is_whitespace)
    {
        return Err(RuntimeError::InvalidInput(
            "remote snapshot object store requires a non-file URI base".to_string(),
        ));
    }
    Ok(normalized)
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotCaptureStatus {
    Captured,
    SkippedMemoryLimit,
    SkippedUnreadable,
    NotTextCandidate,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArtifactKey(pub String);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FileId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArtifactId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EvidenceId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolCallId(pub String);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TurnId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolId(String);

impl ToolId {
    pub fn parse(input: &str) -> RuntimeResult<Self> {
        if input.is_empty() || input.len() > 64 {
            return Err(RuntimeError::InvalidInput(
                "invalid tool id length".to_string(),
            ));
        }
        if !input
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        {
            return Err(RuntimeError::InvalidInput("invalid tool id".to_string()));
        }
        Ok(Self(input.to_string()))
    }

    pub(crate) fn from_builtin(tool: ToolName) -> Self {
        Self(tool.as_str().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn as_builtin(&self) -> Option<ToolName> {
        match self.0.as_str() {
            "list_changed_files" => Some(ToolName::ListChangedFiles),
            "read_diff" => Some(ToolName::ReadDiff),
            "list_files" => Some(ToolName::ListFiles),
            "read_file" => Some(ToolName::ReadFile),
            "read_base_file" => Some(ToolName::ReadBaseFile),
            "read_head_file" => Some(ToolName::ReadHeadFile),
            "search_text" => Some(ToolName::SearchText),
            "find_related_files" => Some(ToolName::FindRelatedFiles),
            "find_tests_for_file" => Some(ToolName::FindTestsForFile),
            "list_imports" => Some(ToolName::ListImports),
            "record_finding" => Some(ToolName::RecordFinding),
            "challenge_finding" => Some(ToolName::ChallengeFinding),
            "finish" => Some(ToolName::Finish),
            _ => None,
        }
    }
}

impl From<ToolName> for ToolId {
    fn from(value: ToolName) -> Self {
        Self::from_builtin(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolProviderId(String);

impl ToolProviderId {
    pub fn parse(input: &str) -> RuntimeResult<Self> {
        if input.is_empty() || input.len() > 64 {
            return Err(RuntimeError::InvalidInput(
                "invalid tool provider id length".to_string(),
            ));
        }
        if !input
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        {
            return Err(RuntimeError::InvalidInput(
                "invalid tool provider id".to_string(),
            ));
        }
        Ok(Self(input.to_string()))
    }

    pub fn builtin_review() -> Self {
        Self("builtin_review".to_string())
    }

    pub fn in_process() -> Self {
        Self("in_process".to_string())
    }

    pub fn runtime() -> Self {
        Self("runtime".to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderResourceId(String);

impl ProviderResourceId {
    pub fn parse(input: &str) -> RuntimeResult<Self> {
        if input.is_empty() || input.len() > 128 {
            return Err(RuntimeError::InvalidInput(
                "invalid provider resource id length".to_string(),
            ));
        }
        if !input.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/' | b':')
        }) {
            return Err(RuntimeError::InvalidInput(
                "invalid provider resource id".to_string(),
            ));
        }
        Ok(Self(input.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderResourceScope {
    pub provider_id: ToolProviderId,
    pub resource_id: ProviderResourceId,
}

impl ProviderResourceScope {
    pub fn new(provider_id: ToolProviderId, resource_id: ProviderResourceId) -> Self {
        Self {
            provider_id,
            resource_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolMetricKey(String);

impl ToolMetricKey {
    pub fn new(provider_id: &ToolProviderId, tool_id: &ToolId) -> Self {
        Self(format!("{}:{}", provider_id.as_str(), tool_id.as_str()))
    }

    pub fn builtin(tool: ToolName) -> Self {
        Self::new(&ToolProviderId::builtin_review(), &ToolId::from(tool))
    }

    pub fn in_process(tool_id: &ToolId) -> Self {
        Self::new(&ToolProviderId::in_process(), tool_id)
    }

    pub(crate) fn from_encoded(value: String) -> Self {
        Self(value)
    }

    pub fn provider_id(&self) -> Option<ToolProviderId> {
        self.0
            .split_once(':')
            .and_then(|(provider, _)| ToolProviderId::parse(provider).ok())
    }

    pub fn tool_id(&self) -> Option<ToolId> {
        self.0
            .split_once(':')
            .and_then(|(_, tool)| ToolId::parse(tool).ok())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RepoPath(PathBuf);

impl RepoPath {
    pub fn parse(input: &str) -> RuntimeResult<Self> {
        if input.is_empty() {
            return Err(RuntimeError::InvalidInput("repo path is empty".to_string()));
        }
        if input.as_bytes().contains(&0) {
            return Err(RuntimeError::RepoAccessDenied);
        }
        if input.contains(':') {
            return Err(RuntimeError::RepoAccessDenied);
        }
        let path = Path::new(input);
        let mut clean = PathBuf::new();
        for component in path.components() {
            match component {
                Component::Normal(part) => clean.push(part),
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(RuntimeError::RepoAccessDenied)
                }
            }
        }
        if clean.as_os_str().is_empty() {
            return Err(RuntimeError::RepoAccessDenied);
        }
        Ok(Self(clean))
    }

    pub fn from_path(path: PathBuf) -> RuntimeResult<Self> {
        let text = path
            .to_str()
            .ok_or_else(|| RuntimeError::InvalidInput("repo path is not UTF-8".to_string()))?;
        Self::parse(text)
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }

    pub fn display(&self) -> String {
        self.0.to_string_lossy().into_owned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ScopeKey(String);

impl ScopeKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FsScope {
    pub cwd: Option<RepoPath>,
    pub allowed_roots: Vec<RepoPath>,
    pub candidate_set_hash: String,
}

impl FsScope {
    pub fn repo_root() -> Self {
        Self {
            cwd: None,
            allowed_roots: Vec::new(),
            candidate_set_hash: "root".to_string(),
        }
    }

    pub fn subtree(path: RepoPath) -> Self {
        let candidate_set_hash = stable_id(&[&path.display()]);
        Self {
            cwd: Some(path.clone()),
            allowed_roots: vec![path],
            candidate_set_hash,
        }
    }

    pub fn allows(&self, path: &RepoPath) -> bool {
        if let Some(cwd) = &self.cwd {
            if path.as_path() != cwd.as_path() && !path.as_path().starts_with(cwd.as_path()) {
                return false;
            }
        }
        if self.allowed_roots.is_empty() {
            return true;
        }
        self.allowed_roots.iter().any(|root| {
            path.as_path() == root.as_path() || path.as_path().starts_with(root.as_path())
        })
    }

    pub fn scope_key(&self, snapshot_id: &SnapshotId) -> ScopeKey {
        let mut owned_parts = vec![snapshot_id.0.clone(), self.candidate_set_hash.clone()];
        if let Some(cwd) = &self.cwd {
            owned_parts.push(cwd.display());
        }
        for root in &self.allowed_roots {
            owned_parts.push(root.display());
        }
        let parts = owned_parts.iter().map(String::as_str).collect::<Vec<_>>();
        ScopeKey(stable_id(&parts))
    }
}

#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolEffects {
    #[serde(default)]
    pub repo_read: bool,
    #[serde(default)]
    pub artifact_read: bool,
    #[serde(default)]
    pub artifact_write: bool,
    #[serde(default)]
    pub network_read: bool,
    #[serde(default)]
    pub host_read: bool,
    #[serde(default)]
    pub scratch_read: bool,
    #[serde(default)]
    pub scratch_write: bool,
    #[serde(default)]
    pub external_side_effect: bool,
}

impl ToolEffects {
    pub fn review_read_only() -> Self {
        Self {
            repo_read: true,
            artifact_read: true,
            artifact_write: true,
            network_read: false,
            host_read: false,
            scratch_read: false,
            scratch_write: false,
            external_side_effect: false,
        }
    }

    pub fn custom_read_only() -> Self {
        Self {
            repo_read: true,
            artifact_read: true,
            artifact_write: true,
            network_read: false,
            host_read: true,
            scratch_read: false,
            scratch_write: false,
            external_side_effect: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolGrant {
    pub allow: bool,
    pub max_calls: Option<u32>,
    pub effects_allowed: ToolEffects,
}

impl ToolGrant {
    pub fn allow_review_read_only() -> Self {
        Self {
            allow: true,
            max_calls: None,
            effects_allowed: ToolEffects::review_read_only(),
        }
    }

    pub fn allow_custom_read_only() -> Self {
        Self {
            allow: true,
            max_calls: None,
            effects_allowed: ToolEffects::custom_read_only(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapabilitySet {
    pub fs_scope: FsScope,
    pub tool_grants: BTreeMap<ToolId, ToolGrant>,
    #[serde(default)]
    pub artifact_access: ArtifactAccessPolicy,
    #[serde(default)]
    pub model_output: ModelOutputPolicy,
    #[serde(default)]
    pub tool_input: ToolInputPolicy,
    #[serde(default)]
    pub runtime_authority: RuntimeAuthorityPolicy,
}

impl CapabilitySet {
    pub fn review_read_only() -> Self {
        let mut capabilities = Self {
            fs_scope: FsScope::repo_root(),
            tool_grants: BTreeMap::new(),
            artifact_access: ArtifactAccessPolicy::review_read_only(),
            model_output: ModelOutputPolicy::review_read_only(),
            tool_input: ToolInputPolicy::review_read_only(),
            runtime_authority: RuntimeAuthorityPolicy::review_read_only(),
        };
        for &tool in ToolName::review_read_only_tools() {
            capabilities.grant(ToolId::from(tool), ToolGrant::allow_review_read_only());
        }
        capabilities
    }

    pub fn empty_review_policy(fs_scope: FsScope) -> Self {
        Self {
            fs_scope,
            tool_grants: BTreeMap::new(),
            artifact_access: ArtifactAccessPolicy::review_read_only(),
            model_output: ModelOutputPolicy::review_read_only(),
            tool_input: ToolInputPolicy::review_read_only(),
            runtime_authority: RuntimeAuthorityPolicy::review_read_only(),
        }
    }

    pub fn with_fs_scope(mut self, fs_scope: FsScope) -> Self {
        self.fs_scope = fs_scope;
        self
    }

    pub fn grant(&mut self, tool_id: ToolId, grant: ToolGrant) {
        self.tool_grants.insert(tool_id, grant);
    }

    pub fn grant_tool(&mut self, tool_id: ToolId, grant: ToolGrant) {
        self.grant(tool_id, grant);
    }

    pub fn allows_tool(&self, tool_id: &ToolId) -> bool {
        self.tool_grants
            .get(tool_id)
            .map(|grant| grant.allow)
            .unwrap_or(false)
    }

    pub fn allow_tool(&self, tool_id: &ToolId) -> bool {
        self.allows_tool(tool_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactAccessPolicy {
    pub read_redacted: bool,
    pub read_raw: bool,
    pub write: bool,
    #[serde(default)]
    pub allowed_artifact_ids: Option<Vec<ArtifactId>>,
}

impl ArtifactAccessPolicy {
    pub fn review_read_only() -> Self {
        Self {
            read_redacted: true,
            read_raw: false,
            write: true,
            allowed_artifact_ids: None,
        }
    }

    pub fn deny_all() -> Self {
        Self {
            read_redacted: false,
            read_raw: false,
            write: false,
            allowed_artifact_ids: None,
        }
    }

    pub fn allow_raw() -> Self {
        Self {
            read_redacted: true,
            read_raw: true,
            write: true,
            allowed_artifact_ids: None,
        }
    }

    pub fn scoped_to_artifacts(mut self, artifact_ids: Vec<ArtifactId>) -> Self {
        self.allowed_artifact_ids = Some(artifact_ids);
        self
    }

    pub fn allows_artifact(&self, artifact_id: &ArtifactId) -> bool {
        match &self.allowed_artifact_ids {
            Some(allowed) => allowed.iter().any(|allowed_id| allowed_id == artifact_id),
            None => true,
        }
    }
}

impl Default for ArtifactAccessPolicy {
    fn default() -> Self {
        Self::review_read_only()
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ModelOutputPolicy {
    pub include_tool_data: bool,
    pub include_artifact_refs: bool,
    pub max_tool_data_bytes: usize,
}

impl ModelOutputPolicy {
    pub fn review_read_only() -> Self {
        Self {
            include_tool_data: true,
            include_artifact_refs: true,
            max_tool_data_bytes: 4 * 1024,
        }
    }

    pub fn metadata_only() -> Self {
        Self {
            include_tool_data: false,
            include_artifact_refs: false,
            max_tool_data_bytes: 0,
        }
    }
}

impl Default for ModelOutputPolicy {
    fn default() -> Self {
        Self::review_read_only()
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolInputPolicy {
    pub max_argument_bytes: usize,
}

impl ToolInputPolicy {
    pub fn review_read_only() -> Self {
        Self {
            max_argument_bytes: 16 * 1024,
        }
    }
}

impl Default for ToolInputPolicy {
    fn default() -> Self {
        Self::review_read_only()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeAuthorityPolicy {
    pub network_read: bool,
    pub host_read: bool,
    pub scratch_read: bool,
    pub scratch_write: bool,
    pub external_side_effect: bool,
    #[serde(default)]
    pub allowed_provider_ids: Option<Vec<ToolProviderId>>,
    #[serde(default)]
    pub allowed_provider_resources: Option<Vec<ProviderResourceScope>>,
}

impl RuntimeAuthorityPolicy {
    pub fn review_read_only() -> Self {
        Self {
            network_read: false,
            host_read: false,
            scratch_read: false,
            scratch_write: false,
            external_side_effect: false,
            allowed_provider_ids: None,
            allowed_provider_resources: None,
        }
    }

    pub fn trusted_host_read() -> Self {
        Self {
            host_read: true,
            ..Self::review_read_only()
        }
    }

    pub fn allow_all_trusted() -> Self {
        Self {
            network_read: true,
            host_read: true,
            scratch_read: true,
            scratch_write: true,
            external_side_effect: true,
            allowed_provider_ids: None,
            allowed_provider_resources: None,
        }
    }

    pub fn scoped_to_providers(mut self, provider_ids: Vec<ToolProviderId>) -> Self {
        self.allowed_provider_ids = Some(provider_ids);
        self
    }

    pub fn allows_provider(&self, provider_id: &ToolProviderId) -> bool {
        match &self.allowed_provider_ids {
            Some(allowed) => allowed.iter().any(|allowed_id| allowed_id == provider_id),
            None => true,
        }
    }

    pub fn scoped_to_provider_resources(mut self, resources: Vec<ProviderResourceScope>) -> Self {
        self.allowed_provider_resources = Some(resources);
        self
    }

    pub fn allows_provider_resource(
        &self,
        provider_id: &ToolProviderId,
        resource_id: &ProviderResourceId,
    ) -> bool {
        match &self.allowed_provider_resources {
            Some(allowed) => allowed.iter().any(|scope| {
                &scope.provider_id == provider_id && &scope.resource_id == resource_id
            }),
            None => true,
        }
    }

    pub fn allows_provider_resources(
        &self,
        provider_id: &ToolProviderId,
        resource_ids: &[ProviderResourceId],
    ) -> bool {
        resource_ids
            .iter()
            .all(|resource_id| self.allows_provider_resource(provider_id, resource_id))
    }
}

impl Default for RuntimeAuthorityPolicy {
    fn default() -> Self {
        Self::review_read_only()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionScope {
    pub id: SessionId,
    pub role: Role,
    pub objective: String,
    pub snapshot_id: Option<SnapshotId>,
    pub model_profile_id: Option<String>,
    pub capabilities: CapabilitySet,
    pub budget: AgentBudget,
}

impl SessionScope {
    pub fn review_read_only(
        id: SessionId,
        role: Role,
        objective: impl Into<String>,
        budget: AgentBudget,
    ) -> Self {
        Self {
            id,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConversationItem {
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
        calls: Vec<ModelToolCall>,
    },
    ToolResult {
        call_id: ToolCallId,
        name: ToolId,
        content: Box<ToolResultEnvelope>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelToolCall {
    pub call_id: ToolCallId,
    pub index: usize,
    pub name: ToolId,
    pub raw_arguments: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ModelTurn {
    Text {
        content: String,
        usage: TokenUsage,
    },
    ToolCalls {
        calls: Vec<ModelToolCall>,
        usage: TokenUsage,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ToolInvocation {
    pub(crate) session_id: SessionId,
    pub(crate) turn_id: TurnId,
    pub(crate) call_id: ToolCallId,
    pub(crate) tool_id: ToolId,
    pub(crate) builtin_name: Option<ToolName>,
    pub(crate) input_bytes: usize,
    pub(crate) args: ToolArgs,
    pub(crate) capabilities: CapabilitySet,
    pub(crate) scope_key: ScopeKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolArgs {
    Empty,
    ReadFile {
        path: RepoPath,
    },
    SearchText {
        query: String,
    },
    RecordFinding {
        title: String,
        claim: String,
    },
    ChallengeFinding {
        finding_id: String,
        rationale: String,
    },
    Finish {
        reason: String,
    },
    Raw(Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolResultEnvelope {
    pub ok: bool,
    pub tool_call_id: ToolCallId,
    pub tool_name: ToolId,
    pub provider_id: ToolProviderId,
    pub snapshot_id: SnapshotId,
    pub artifact_id: Option<ArtifactId>,
    pub cache: CacheInfo,
    pub limits: LimitInfo,
    pub data: Option<Value>,
    pub error: Option<ToolErrorInfo>,
}

impl ToolResultEnvelope {
    pub(crate) fn for_call(
        &self,
        call_id: ToolCallId,
        tool_name: ToolId,
        cache_status: CacheStatus,
    ) -> Self {
        let mut cloned = self.clone();
        cloned.tool_call_id = call_id;
        cloned.tool_name = tool_name;
        cloned.cache.status = cache_status;
        cloned
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheInfo {
    pub status: CacheStatus,
    pub key_hash: Option<String>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CacheStatus {
    Hit,
    Miss,
    Deduped,
    NotCacheable,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitInfo {
    pub truncated: bool,
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub latency_ms: u64,
    pub queue_wait_ms: u64,
    pub searched_files: usize,
    pub skipped_files: usize,
    pub bytes_scanned: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolErrorInfo {
    pub code: ToolErrorCode,
    pub message: String,
    pub retryable: bool,
    pub partial: bool,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolErrorCode {
    InvalidArgs,
    UnknownTool,
    ToolNotAllowed,
    PathDenied,
    NotFound,
    NotText,
    TooLarge,
    TooManyMatches,
    SnapshotStale,
    Timeout,
    Cancelled,
    BudgetExceeded,
    QueueFull,
    RepoUnavailable,
    RedactionFailed,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceRecord {
    pub evidence_id: EvidenceId,
    pub snapshot_id: SnapshotId,
    pub file_id: Option<FileId>,
    pub path: Option<RepoPath>,
    pub start_line: Option<u32>,
    pub end_line: Option<u32>,
    pub snippet_hash: String,
    pub artifact_id: ArtifactId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeLimits {
    pub max_active_sessions: usize,
    pub max_model_concurrency_global: usize,
    pub max_model_concurrency_per_key: usize,
    pub max_tool_calls_per_turn: usize,
    pub max_tool_parallelism_per_session: usize,
    pub max_tool_provider_concurrency_per_provider: usize,
    pub max_tool_provider_ms: u64,
    #[serde(default = "default_max_tool_output_bytes")]
    pub max_tool_output_bytes: usize,
    #[serde(default = "default_max_tool_artifact_bytes")]
    pub max_tool_artifact_bytes: usize,
    pub max_read_concurrency_global: usize,
    pub max_search_jobs_global: usize,
    pub max_search_queue_depth: usize,
    pub max_file_bytes_read: usize,
    pub max_file_bytes_search: usize,
    pub max_search_matches: usize,
    pub max_search_pattern_bytes: usize,
    pub file_content_cache_bytes: u64,
    pub search_result_cache_bytes: u64,
    pub search_threads: usize,
}

impl RuntimeLimits {
    pub fn standard(sessions: usize, max_file_bytes: usize, max_search_matches: usize) -> Self {
        Self {
            max_active_sessions: sessions.max(1),
            max_model_concurrency_global: 16,
            max_model_concurrency_per_key: 4,
            max_tool_calls_per_turn: 4,
            max_tool_parallelism_per_session: 2,
            max_tool_provider_concurrency_per_provider: 8,
            max_tool_provider_ms: 30_000,
            max_tool_output_bytes: default_max_tool_output_bytes(),
            max_tool_artifact_bytes: default_max_tool_artifact_bytes(),
            max_read_concurrency_global: 32,
            max_search_jobs_global: 1,
            max_search_queue_depth: 128,
            max_file_bytes_read: max_file_bytes,
            max_file_bytes_search: max_file_bytes,
            max_search_matches,
            max_search_pattern_bytes: 512,
            file_content_cache_bytes: 32_000_000,
            search_result_cache_bytes: 16_000_000,
            search_threads: num_cpus::get().clamp(2, 8),
        }
    }
}

fn default_max_tool_output_bytes() -> usize {
    2 * 1024 * 1024
}

fn default_max_tool_artifact_bytes() -> usize {
    8 * 1024 * 1024
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum RuntimeEvent {
    JobStarted {
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
        session_id: SessionId,
    },
    ModelStarted {
        session_id: SessionId,
        turn_id: TurnId,
    },
    ModelCompleted {
        session_id: SessionId,
        turn_id: TurnId,
        tool_call_count: usize,
    },
    ToolBatchStarted {
        session_id: SessionId,
        turn_id: TurnId,
        count: usize,
    },
    ToolCallCompleted {
        call_id: ToolCallId,
        tool_name: ToolId,
        provider_id: ToolProviderId,
        cache_status: CacheStatus,
        output_bytes: usize,
        ok: bool,
        error_code: Option<ToolErrorCode>,
    },
    ToolCallDenied {
        call_id: ToolCallId,
        tool_name: ToolId,
        provider_id: ToolProviderId,
        error_code: ToolErrorCode,
        reason: String,
    },
    ArtifactCreated {
        artifact_id: ArtifactId,
        tool_call_id: ToolCallId,
        tool_name: ToolId,
        provider_id: ToolProviderId,
        bytes: usize,
        content_hash: String,
    },
    FindingRecorded {
        finding_id: String,
        session_id: SessionId,
        tool_call_id: ToolCallId,
    },
    SearchBatchCompleted {
        searched_files: usize,
        skipped_files: usize,
        bytes_scanned: usize,
        ms: u64,
    },
    SessionFinished {
        session_id: SessionId,
        status: String,
    },
    SnapshotFinished {
        snapshot_id: SnapshotId,
        sessions: usize,
        completed_sessions: usize,
    },
    JobFinished {
        status: String,
    },
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeEventContext {
    pub run_id: Option<String>,
    pub snapshot_id: Option<SnapshotId>,
    pub session_id: Option<SessionId>,
    pub turn_id: Option<TurnId>,
    pub tool_call_id: Option<ToolCallId>,
    pub artifact_id: Option<ArtifactId>,
    pub finding_id: Option<String>,
}

impl RuntimeEventContext {
    pub fn from_event(event: &RuntimeEvent) -> Self {
        match event {
            RuntimeEvent::JobStarted { snapshot_id }
            | RuntimeEvent::SnapshotStarted { snapshot_id }
            | RuntimeEvent::SnapshotFinished { snapshot_id, .. } => Self {
                snapshot_id: Some(snapshot_id.clone()),
                ..Self::default()
            },
            RuntimeEvent::RepoManifestCompleted { .. } | RuntimeEvent::JobFinished { .. } => {
                Self::default()
            }
            RuntimeEvent::SessionStarted { session_id }
            | RuntimeEvent::SessionFinished { session_id, .. } => Self {
                session_id: Some(session_id.clone()),
                ..Self::default()
            },
            RuntimeEvent::ModelStarted {
                session_id,
                turn_id,
            }
            | RuntimeEvent::ModelCompleted {
                session_id,
                turn_id,
                ..
            }
            | RuntimeEvent::ToolBatchStarted {
                session_id,
                turn_id,
                ..
            } => Self {
                session_id: Some(session_id.clone()),
                turn_id: Some(*turn_id),
                ..Self::default()
            },
            RuntimeEvent::ToolCallCompleted { call_id, .. }
            | RuntimeEvent::ToolCallDenied { call_id, .. } => Self {
                tool_call_id: Some(call_id.clone()),
                ..Self::default()
            },
            RuntimeEvent::ArtifactCreated {
                artifact_id,
                tool_call_id,
                ..
            } => Self {
                tool_call_id: Some(tool_call_id.clone()),
                artifact_id: Some(artifact_id.clone()),
                ..Self::default()
            },
            RuntimeEvent::FindingRecorded {
                finding_id,
                session_id,
                tool_call_id,
            } => Self {
                session_id: Some(session_id.clone()),
                tool_call_id: Some(tool_call_id.clone()),
                finding_id: Some(finding_id.clone()),
                ..Self::default()
            },
            RuntimeEvent::SearchBatchCompleted { .. } => Self::default(),
        }
    }

    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    pub fn with_default_snapshot_id(mut self, snapshot_id: SnapshotId) -> Self {
        if self.snapshot_id.is_none() {
            self.snapshot_id = Some(snapshot_id);
        }
        self
    }
}

pub trait RuntimeEventSink: Send + Sync {
    fn emit(&self, event: RuntimeEvent);

    fn emit_with_context(&self, context: RuntimeEventContext, event: RuntimeEvent) {
        let _ = context;
        self.emit(event);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeEventRecord {
    pub seq: u64,
    pub timestamp_utc: String,
    pub context: RuntimeEventContext,
    pub event: RuntimeEvent,
}

#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConcurrentCounters {
    pub search_scans: usize,
    pub search_dedupe_waiters: usize,
    pub search_cache_hits: usize,
    pub read_cache_hits: usize,
    pub read_file_reads: usize,
    pub tool_errors: usize,
    pub artifact_cache_hits: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolMetricsSnapshot {
    pub calls: usize,
    pub successes: usize,
    pub errors: usize,
    pub cache_hits: usize,
    pub deduped: usize,
    pub timeouts: usize,
    pub cancellations: usize,
    pub artifacts: usize,
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub latency_ms: u64,
    pub max_latency_ms: u64,
    pub queue_wait_ms: u64,
    pub max_queue_wait_ms: u64,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolProviderHealthState {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolProviderHealthSnapshot {
    pub provider_id: ToolProviderId,
    pub state: ToolProviderHealthState,
    pub calls: usize,
    pub errors: usize,
    pub timeouts: usize,
    pub cancellations: usize,
    pub consecutive_errors: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelMetricsSnapshot {
    pub calls: usize,
    pub successes: usize,
    pub errors: usize,
    pub retries: usize,
    pub costed_calls: usize,
    pub unpriced_calls: usize,
    pub latency_ms: u64,
    pub max_latency_ms: u64,
    pub estimated_input_cost_micro_usd: u64,
    pub estimated_output_cost_micro_usd: u64,
    pub estimated_total_cost_micro_usd: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Default, Copy, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCostEstimate {
    pub input_cost_micro_usd: u64,
    pub output_cost_micro_usd: u64,
    pub total_cost_micro_usd: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactView {
    pub artifact_id: ArtifactId,
    pub bytes: usize,
    pub content_hash: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionTerminalDiagnostic {
    pub session_id: String,
    pub completed: bool,
    pub terminal_tool: Option<String>,
    pub terminal_summary: Option<String>,
    pub saw_diff: bool,
    pub saw_file: bool,
    pub saw_search: bool,
    pub model_calls: usize,
    pub tool_counts: ToolCounts,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotMetricsSnapshot {
    pub snapshot_id: SnapshotId,
    pub sessions: usize,
    pub completed_sessions: usize,
    pub model_calls: usize,
    pub tool_calls: usize,
    pub artifacts: usize,
    pub artifact_bytes: usize,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConcurrentRunReport {
    pub runtime: &'static str,
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
    pub counters: ConcurrentCounters,
    pub tool_metrics: BTreeMap<ToolMetricKey, ToolMetricsSnapshot>,
    pub provider_health: Vec<ToolProviderHealthSnapshot>,
    pub snapshot_metrics: Vec<SnapshotMetricsSnapshot>,
    pub model_metrics: ModelMetricsSnapshot,
    pub terminal_diagnostics: Vec<SessionTerminalDiagnostic>,
    pub benchmark_valid: bool,
    pub benchmark_failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ComparisonReport {
    pub sessions: usize,
    pub sync: ConcurrentRunReport,
    pub concurrent: ConcurrentRunReport,
    pub speedup: f64,
    pub search_scan_reduction: f64,
    pub optimization_valid: bool,
    pub optimization_failures: Vec<String>,
}

pub(crate) fn stable_id(parts: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"\0");
    }
    hasher.finalize().to_hex().to_string()
}
