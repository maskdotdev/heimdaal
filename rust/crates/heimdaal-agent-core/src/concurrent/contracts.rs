use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::contracts::{TokenUsage, ToolCounts, ToolMask, ToolName};

pub(crate) const CONCURRENT_CONTRACT_VERSION: u16 = 1;
pub(crate) const REDACTION_POLICY_VERSION: u16 = 1;

#[derive(Debug, Error)]
pub(crate) enum RuntimeError {
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

pub(crate) type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct SnapshotId(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ArtifactKey(pub(crate) String);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct FileId(pub(crate) u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ArtifactId(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct EvidenceId(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ToolCallId(pub(crate) String);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct TurnId(pub(crate) u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct SessionId(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct RepoPath(PathBuf);

impl RepoPath {
    pub(crate) fn parse(input: &str) -> RuntimeResult<Self> {
        if input.is_empty() {
            return Err(RuntimeError::InvalidInput("repo path is empty".to_string()));
        }
        if input.as_bytes().contains(&0) {
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

    pub(crate) fn from_path(path: PathBuf) -> RuntimeResult<Self> {
        let text = path
            .to_str()
            .ok_or_else(|| RuntimeError::InvalidInput("repo path is not UTF-8".to_string()))?;
        Self::parse(text)
    }

    pub(crate) fn as_path(&self) -> &Path {
        &self.0
    }

    pub(crate) fn display(&self) -> String {
        self.0.to_string_lossy().into_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ConversationItem {
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
        name: ToolName,
        content: ToolResultEnvelope,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ModelToolCall {
    pub(crate) call_id: ToolCallId,
    pub(crate) index: usize,
    pub(crate) name: ToolName,
    pub(crate) raw_arguments: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ModelTurn {
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
    pub(crate) name: ToolName,
    pub(crate) args: ToolArgs,
    pub(crate) allowed_tools: ToolMask,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ToolArgs {
    Empty,
    ReadFile { path: RepoPath },
    SearchText { query: String },
    RecordFinding { title: String, claim: String },
    Finish { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToolResultEnvelope {
    pub(crate) ok: bool,
    pub(crate) tool_call_id: ToolCallId,
    pub(crate) tool_name: ToolName,
    pub(crate) snapshot_id: SnapshotId,
    pub(crate) artifact_id: Option<ArtifactId>,
    pub(crate) cache: CacheInfo,
    pub(crate) limits: LimitInfo,
    pub(crate) data: Option<Value>,
    pub(crate) error: Option<ToolErrorInfo>,
}

impl ToolResultEnvelope {
    pub(crate) fn for_call(
        &self,
        call_id: ToolCallId,
        tool_name: ToolName,
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
pub(crate) struct CacheInfo {
    pub(crate) status: CacheStatus,
    pub(crate) key_hash: Option<String>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CacheStatus {
    Hit,
    Miss,
    Deduped,
    NotCacheable,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LimitInfo {
    pub(crate) truncated: bool,
    pub(crate) output_bytes: usize,
    pub(crate) searched_files: usize,
    pub(crate) skipped_files: usize,
    pub(crate) bytes_scanned: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToolErrorInfo {
    pub(crate) code: ToolErrorCode,
    pub(crate) message: String,
    pub(crate) retryable: bool,
    pub(crate) partial: bool,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolErrorCode {
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
    QueueFull,
    RepoUnavailable,
    RedactionFailed,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EvidenceRecord {
    pub(crate) evidence_id: EvidenceId,
    pub(crate) snapshot_id: SnapshotId,
    pub(crate) file_id: Option<FileId>,
    pub(crate) path: Option<RepoPath>,
    pub(crate) start_line: Option<u32>,
    pub(crate) end_line: Option<u32>,
    pub(crate) snippet_hash: String,
    pub(crate) artifact_id: ArtifactId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeLimits {
    pub(crate) max_active_sessions: usize,
    pub(crate) max_model_concurrency_global: usize,
    pub(crate) max_model_concurrency_per_key: usize,
    pub(crate) max_tool_calls_per_turn: usize,
    pub(crate) max_tool_parallelism_per_session: usize,
    pub(crate) max_read_concurrency_global: usize,
    pub(crate) max_search_jobs_global: usize,
    pub(crate) max_search_queue_depth: usize,
    pub(crate) max_file_bytes_read: usize,
    pub(crate) max_file_bytes_search: usize,
    pub(crate) max_search_matches: usize,
    pub(crate) max_search_pattern_bytes: usize,
    pub(crate) file_content_cache_bytes: u64,
    pub(crate) search_result_cache_bytes: u64,
    pub(crate) search_threads: usize,
}

impl RuntimeLimits {
    pub(crate) fn standard(
        sessions: usize,
        max_file_bytes: usize,
        max_search_matches: usize,
    ) -> Self {
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
pub(crate) enum RuntimeEvent {
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
        tool_name: ToolName,
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
pub(crate) struct ConcurrentCounters {
    pub(crate) search_scans: usize,
    pub(crate) search_dedupe_waiters: usize,
    pub(crate) search_cache_hits: usize,
    pub(crate) read_cache_hits: usize,
    pub(crate) read_file_reads: usize,
    pub(crate) tool_errors: usize,
    pub(crate) artifact_cache_hits: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConcurrentRunReport {
    pub(crate) runtime: &'static str,
    pub(crate) sessions: usize,
    pub(crate) completed_sessions: usize,
    pub(crate) model_calls: usize,
    pub(crate) tool_calls: usize,
    pub(crate) tool_counts: ToolCounts,
    pub(crate) findings: usize,
    pub(crate) elapsed_ms: u64,
    pub(crate) input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) total_tokens: u64,
    pub(crate) artifacts: usize,
    pub(crate) artifact_bytes: usize,
    pub(crate) counters: ConcurrentCounters,
    pub(crate) benchmark_valid: bool,
    pub(crate) benchmark_failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ComparisonReport {
    pub(crate) sessions: usize,
    pub(crate) sync: ConcurrentRunReport,
    pub(crate) concurrent: ConcurrentRunReport,
    pub(crate) speedup: f64,
    pub(crate) search_scan_reduction: f64,
}

pub(crate) fn stable_id(parts: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"\0");
    }
    hasher.finalize().to_hex().to_string()
}
