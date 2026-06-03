use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

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
    #[error("internal invariant violation: {0}")]
    Invariant(&'static str),
}

pub type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SnapshotId(pub String);

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
    pub repo_read: bool,
    pub artifact_read: bool,
    pub artifact_write: bool,
    pub network_read: bool,
    pub host_read: bool,
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
}

impl CapabilitySet {
    pub fn review_read_only() -> Self {
        let mut capabilities = Self {
            fs_scope: FsScope::repo_root(),
            tool_grants: BTreeMap::new(),
        };
        for &tool in ToolName::review_read_only_tools() {
            capabilities.grant(ToolId::from(tool), ToolGrant::allow_review_read_only());
        }
        capabilities
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionScope {
    pub id: SessionId,
    pub role: Role,
    pub objective: String,
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
            model_profile_id: None,
            capabilities: CapabilitySet::review_read_only(),
            budget,
        }
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
    pub(crate) original_index: usize,
    pub(crate) call_id: ToolCallId,
    pub(crate) tool_id: ToolId,
    pub(crate) builtin_name: Option<ToolName>,
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
    pub output_bytes: usize,
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

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum RuntimeEvent {
    JobStarted {
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
        cache_status: CacheStatus,
        output_bytes: usize,
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
    JobFinished {
        status: String,
    },
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

pub type ToolMetricKey = ToolId;

#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolMetricsSnapshot {
    pub calls: usize,
    pub successes: usize,
    pub errors: usize,
    pub cache_hits: usize,
    pub deduped: usize,
    pub output_bytes: usize,
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
}

pub(crate) fn stable_id(parts: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"\0");
    }
    hasher.finalize().to_hex().to_string()
}
