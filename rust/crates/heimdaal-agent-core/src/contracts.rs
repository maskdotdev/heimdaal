use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::util::stable_hash;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReviewRunJobV1 {
    pub(crate) schema_version: String,
    pub(crate) run_id: String,
    pub(crate) project_id: String,
    pub(crate) attempt: u32,
    pub(crate) idempotency_key: String,
    pub(crate) deadline_utc: Option<String>,
    pub(crate) repo: MaterializedRepoScopeV1,
    pub(crate) change: ChangeScopeV1,
    pub(crate) model_profiles: Vec<ModelProfileRefV1>,
    pub(crate) default_model_profile_id: String,
    pub(crate) personas: Vec<PersonaSpecV1>,
    pub(crate) path_policy: PathPolicyV1,
    pub(crate) scratch_policy: ScratchPolicyV1,
    pub(crate) model_visibility: ModelVisibilityPolicyV1,
    pub(crate) output_redaction: OutputRedactionPolicyV1,
    pub(crate) budgets: RunBudgetsV1,
    pub(crate) telemetry: TelemetryPolicyV1,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MaterializedRepoScopeV1 {
    pub(crate) provider: RepoProvider,
    pub(crate) repo_id: String,
    pub(crate) repo_root: PathBuf,
    pub(crate) worktree_root: PathBuf,
    pub(crate) default_cwd: PathBuf,
    pub(crate) materialization_id: String,
    pub(crate) materialized_at_utc: String,
    pub(crate) materialization_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RepoProvider {
    Github,
    Gitlab,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ChangeScopeV1 {
    pub(crate) kind: ChangeKind,
    pub(crate) change_id: String,
    pub(crate) source_ref: String,
    pub(crate) target_ref: String,
    pub(crate) base_revision_id: String,
    pub(crate) head_revision_id: String,
    pub(crate) merge_base_revision_id: Option<String>,
    pub(crate) changed_files_manifest_ref: Option<String>,
    pub(crate) diff_manifest_ref: Option<String>,
    pub(crate) snapshot_mode: SnapshotMode,
    pub(crate) rename_detection: RenameDetection,
    pub(crate) changed_files: Vec<ChangedFileEntryV1>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChangeKind {
    PullRequest,
    MergeRequest,
    LocalDiff,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SnapshotMode {
    WorktreeHead,
    BaseHeadManifests,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RenameDetection {
    None,
    AppManifest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ChangedFileEntryV1 {
    pub(crate) status: ChangedFileStatus,
    pub(crate) old_path: Option<PathBuf>,
    pub(crate) new_path: Option<PathBuf>,
    pub(crate) old_content_hash: Option<String>,
    pub(crate) new_content_hash: Option<String>,
    pub(crate) is_binary: bool,
    pub(crate) is_generated: bool,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChangedFileStatus {
    Added,
    Modified,
    Deleted,
    Renamed,
    Copied,
    TypeChanged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ModelProfileRefV1 {
    pub(crate) id: String,
    pub(crate) provider_kind: ProviderKind,
    pub(crate) provider_profile_id: String,
    pub(crate) credential_ref: String,
    pub(crate) model: String,
    pub(crate) max_input_tokens: u32,
    pub(crate) max_output_tokens: u32,
    pub(crate) tool_calling_mode: ToolCallingMode,
    pub(crate) temperature: Option<f32>,
    pub(crate) top_p: Option<f32>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProviderKind {
    OpenaiCompatible,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolCallingMode {
    Required,
    Auto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PersonaSpecV1 {
    pub(crate) id: String,
    pub(crate) role: Role,
    pub(crate) objective: String,
    pub(crate) cwd: Option<PathBuf>,
    pub(crate) model_profile_id: Option<String>,
    pub(crate) allowed_tools: ToolMask,
    pub(crate) budget: AgentBudget,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Role {
    Generalist,
    Security,
    Performance,
    Maintainability,
    Correctness,
    Architecture,
    Validator,
}

impl Role {
    pub(crate) fn for_index(index: usize) -> Self {
        match index % 6 {
            0 => Self::Correctness,
            1 => Self::Security,
            2 => Self::Performance,
            3 => Self::Maintainability,
            4 => Self::Architecture,
            _ => Self::Validator,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PathPolicyV1 {
    pub(crate) allowed_roots: Vec<PathBuf>,
    pub(crate) denied_globs: Vec<String>,
    pub(crate) allowed_globs: Option<Vec<String>>,
    pub(crate) allow_dot_git: bool,
    pub(crate) follow_symlinks: bool,
    pub(crate) max_file_bytes: usize,
    pub(crate) max_diff_bytes: usize,
    pub(crate) max_search_results: usize,
    pub(crate) max_directory_entries: usize,
}

impl PathPolicyV1 {
    pub(crate) fn bench(max_file_kb: usize, max_search_matches: usize) -> Self {
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
            max_file_bytes: max_file_kb * 1024,
            max_diff_bytes: max_file_kb * 1024,
            max_search_results: max_search_matches,
            max_directory_entries: 20_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ScratchPolicyV1 {
    pub(crate) scratch_root: Option<PathBuf>,
    pub(crate) output_root: Option<PathBuf>,
    pub(crate) max_scratch_bytes: usize,
    pub(crate) cleanup_on_finish: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ModelVisibilityPolicyV1 {
    pub(crate) max_prompt_artifact_bytes: usize,
    pub(crate) allow_full_file_content_in_prompts: bool,
    pub(crate) deny_globs: Vec<String>,
    pub(crate) redact_secret_like_content: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OutputRedactionPolicyV1 {
    pub(crate) policy_id: String,
    pub(crate) redact_repo_secrets: bool,
    pub(crate) persist_full_file_contents: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TelemetryPolicyV1 {
    pub(crate) emit_debug_events: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RunBudgetsV1 {
    pub(crate) max_active_sessions: usize,
    pub(crate) max_wall_time_ms: u64,
    pub(crate) max_model_calls: usize,
    pub(crate) max_tool_calls: usize,
    pub(crate) max_prompt_tokens: u64,
    pub(crate) max_output_tokens: u64,
    pub(crate) max_artifact_bytes: usize,
    pub(crate) max_scratch_bytes: usize,
    pub(crate) rss_target_mb: Option<u64>,
    pub(crate) rss_limit_mb: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AgentBudget {
    pub(crate) max_turns: usize,
    pub(crate) max_tool_calls: usize,
    pub(crate) max_prompt_tokens: u64,
    pub(crate) max_output_tokens: u64,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToolMask {
    pub(crate) list_changed_files: bool,
    pub(crate) read_diff: bool,
    pub(crate) list_files: bool,
    pub(crate) read_file: bool,
    pub(crate) read_base_file: bool,
    pub(crate) read_head_file: bool,
    pub(crate) search_text: bool,
    pub(crate) find_related_files: bool,
    pub(crate) find_tests_for_file: bool,
    pub(crate) list_imports: bool,
    pub(crate) record_finding: bool,
    pub(crate) challenge_finding: bool,
    pub(crate) finish: bool,
}

impl ToolMask {
    pub(crate) fn review_read_only() -> Self {
        Self {
            list_changed_files: true,
            read_diff: true,
            list_files: true,
            read_file: true,
            read_base_file: true,
            read_head_file: true,
            search_text: true,
            find_related_files: true,
            find_tests_for_file: true,
            list_imports: true,
            record_finding: true,
            challenge_finding: true,
            finish: true,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RunEventV1 {
    pub(crate) schema_version: &'static str,
    pub(crate) event_id: String,
    pub(crate) run_id: String,
    pub(crate) attempt: u32,
    pub(crate) seq: u64,
    pub(crate) timestamp_utc: String,
    pub(crate) level: EventLevel,
    pub(crate) event_type: EventType,
    pub(crate) session_id: Option<String>,
    pub(crate) tool_call_id: Option<String>,
    pub(crate) artifact_id: Option<String>,
    pub(crate) finding_id: Option<String>,
    pub(crate) payload: Value,
    pub(crate) redaction: RedactionMetadataV1,
    pub(crate) trace: EventTraceV1,
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventType {
    RunStarted,
    SessionStarted,
    ModelCallStarted,
    ModelCallCompleted,
    ToolCallRequested,
    ToolCallCompleted,
    ArtifactRecorded,
    FindingCandidate,
    FindingValidated,
    BudgetUpdate,
    SessionFinished,
    RunFinished,
    Error,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RedactionMetadataV1 {
    pub(crate) redaction_state: RedactionState,
    pub(crate) redaction_policy_id: String,
    pub(crate) contains_repo_content: bool,
    pub(crate) contains_prompt_content: bool,
    pub(crate) contains_model_output: bool,
    pub(crate) contains_secret_material: bool,
}

#[derive(Debug, Copy, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RedactionState {
    None,
    Partial,
    Full,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EventTraceV1 {
    pub(crate) parent_event_id: Option<String>,
    pub(crate) correlation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReviewRunResultV1 {
    pub(crate) schema_version: &'static str,
    pub(crate) run_id: String,
    pub(crate) attempt: u32,
    pub(crate) outcome: ReviewOutcomeV1,
    pub(crate) publishability: Publishability,
    pub(crate) sessions: usize,
    pub(crate) completed_sessions: usize,
    pub(crate) findings: Vec<FindingV1>,
    pub(crate) tool_counts: ToolCounts,
    pub(crate) model_calls: usize,
    pub(crate) tokens: TokenUsage,
    pub(crate) artifact_stats: ArtifactStats,
    pub(crate) elapsed_ms: u64,
}

#[derive(Debug, Copy, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReviewOutcomeV1 {
    CompletedNoFindings,
    CompletedWithFindings,
    BudgetExhaustedPartial,
    CancelledPartial,
    FailedPartial,
}

#[derive(Debug, Copy, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Publishability {
    Publishable,
    DiagnosticOnly,
    NotPublishable,
}

#[derive(Debug, Copy, Clone, Serialize, PartialEq, Eq, Hash)]
pub(crate) struct ArtifactId(pub(crate) usize);

impl ArtifactId {
    pub(crate) fn as_string(self) -> String {
        format!("artifact-{}", self.0)
    }
}

#[derive(Debug, Copy, Clone, Serialize, PartialEq, Eq, Hash)]
pub(crate) struct FindingId(pub(crate) usize);

impl FindingId {
    pub(crate) fn as_string(self) -> String {
        format!("finding-{}", self.0)
    }
}

#[derive(Debug, Copy, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Completeness {
    Complete,
    Partial,
    Truncated,
    MetadataOnly,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ArtifactMeta {
    pub(crate) id: ArtifactId,
    pub(crate) artifact_id: String,
    pub(crate) kind: ArtifactKind,
    pub(crate) bytes: usize,
    pub(crate) content_hash: String,
    pub(crate) completeness: Completeness,
    pub(crate) redaction: RedactionMetadataV1,
    pub(crate) summary: String,
}

#[derive(Debug, Copy, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactKind {
    FileSlice,
    DiffHunk,
    SearchResults,
    FileList,
    ChangedFileList,
    ImportSummary,
    ToolSummary,
    RedactedView,
}

#[derive(Debug, Clone)]
pub(crate) struct Artifact {
    pub(crate) meta: ArtifactMeta,
    pub(crate) content: Arc<str>,
}

#[derive(Debug, Default)]
pub(crate) struct ArtifactStore {
    pub(crate) inner: Mutex<Vec<Artifact>>,
}

impl ArtifactStore {
    pub(crate) fn insert(
        &self,
        kind: ArtifactKind,
        content: String,
        summary: String,
        completeness: Completeness,
        redaction: RedactionMetadataV1,
    ) -> ArtifactId {
        let mut artifacts = self.inner.lock().expect("artifact store poisoned");
        let id = ArtifactId(artifacts.len());
        let bytes = content.len();
        let content_hash = stable_hash(content.as_bytes());
        artifacts.push(Artifact {
            meta: ArtifactMeta {
                id,
                artifact_id: id.as_string(),
                kind,
                bytes,
                content_hash,
                completeness,
                redaction,
                summary,
            },
            content: Arc::<str>::from(content),
        });
        id
    }

    pub(crate) fn meta(&self, id: ArtifactId) -> Option<ArtifactMeta> {
        self.inner
            .lock()
            .expect("artifact store poisoned")
            .get(id.0)
            .map(|artifact| artifact.meta.clone())
    }

    pub(crate) fn stats(&self) -> ArtifactStats {
        let artifacts = self.inner.lock().expect("artifact store poisoned");
        ArtifactStats {
            artifacts: artifacts.len(),
            artifact_bytes: artifacts.iter().map(|artifact| artifact.meta.bytes).sum(),
            content_refs: artifacts
                .iter()
                .map(|artifact| Arc::strong_count(&artifact.content))
                .sum(),
        }
    }

    pub(crate) fn snippet(&self, id: ArtifactId, max_chars: usize) -> Option<String> {
        let artifacts = self.inner.lock().expect("artifact store poisoned");
        let artifact = artifacts.get(id.0)?;
        Some(artifact.content.chars().take(max_chars).collect())
    }
}

#[derive(Debug, Default, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ArtifactStats {
    pub(crate) artifacts: usize,
    pub(crate) artifact_bytes: usize,
    pub(crate) content_refs: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EvidenceRefV1 {
    pub(crate) evidence_id: String,
    pub(crate) artifact_id: String,
    pub(crate) kind: ArtifactKind,
    pub(crate) revision: EvidenceRevision,
    pub(crate) revision_id: String,
    pub(crate) location: EvidenceLocationV1,
    pub(crate) line_range: Option<LineRangeV1>,
    pub(crate) byte_range: Option<ByteRangeV1>,
    pub(crate) diff_anchor: Option<DiffAnchorV1>,
    pub(crate) content_hash: String,
    pub(crate) redaction: RedactionMetadataV1,
    pub(crate) producing_tool_call_id: String,
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EvidenceRevision {
    Base,
    Head,
    MergeBase,
    Review,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "locationKind", rename_all = "snake_case")]
pub(crate) enum EvidenceLocationV1 {
    SinglePath { path: String },
    Rename { old_path: String, new_path: String },
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LineRangeV1 {
    pub(crate) start_line: usize,
    pub(crate) end_line: usize,
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ByteRangeV1 {
    pub(crate) start_byte: usize,
    pub(crate) end_byte: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiffAnchorV1 {
    pub(crate) hunk_id: String,
    pub(crate) side: DiffSide,
    pub(crate) old_start: Option<usize>,
    pub(crate) old_lines: Option<usize>,
    pub(crate) new_start: Option<usize>,
    pub(crate) new_lines: Option<usize>,
    pub(crate) patch_hash: String,
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiffSide {
    Base,
    Head,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FindingV1 {
    pub(crate) id: String,
    pub(crate) title: String,
    pub(crate) claim: String,
    pub(crate) severity: FindingSeverity,
    pub(crate) confidence: f32,
    pub(crate) validation_status: ValidationStatus,
    pub(crate) report_status: ReportStatus,
    pub(crate) publishability: FindingPublishability,
    pub(crate) evidence: Vec<EvidenceRefV1>,
    pub(crate) file_refs: Vec<EvidenceLocationV1>,
    pub(crate) discovered_by: Vec<String>,
    pub(crate) challenged_by: Vec<String>,
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FindingSeverity {
    Blocker,
    High,
    Medium,
    Low,
    Nit,
}

#[derive(Debug, Copy, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ValidationStatus {
    Candidate,
    Challenged,
    Validated,
    Rejected,
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReportStatus {
    Included,
    Suppressed,
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FindingPublishability {
    Publishable,
    NotPublishable,
}

#[derive(Debug, Clone)]
pub(crate) struct FindingRecord {
    pub(crate) finding: FindingV1,
}

#[derive(Debug, Default)]
pub(crate) struct FindingStore {
    pub(crate) findings: Mutex<Vec<FindingRecord>>,
}

impl FindingStore {
    pub(crate) fn insert(
        &self,
        title: String,
        claim: String,
        discovered_by: String,
        evidence: Vec<EvidenceRefV1>,
    ) -> FindingId {
        let mut findings = self.findings.lock().expect("finding store poisoned");
        let id = FindingId(findings.len());
        let file_refs = evidence
            .iter()
            .map(|item| item.location.clone())
            .collect::<Vec<_>>();
        let validated = !evidence.is_empty()
            && evidence
                .iter()
                .all(|item| item.redaction.redaction_state != RedactionState::Full);
        findings.push(FindingRecord {
            finding: FindingV1 {
                id: id.as_string(),
                title,
                claim,
                severity: FindingSeverity::Low,
                confidence: if validated { 0.72 } else { 0.25 },
                validation_status: if validated {
                    ValidationStatus::Validated
                } else {
                    ValidationStatus::Rejected
                },
                report_status: if validated {
                    ReportStatus::Included
                } else {
                    ReportStatus::Suppressed
                },
                publishability: if validated {
                    FindingPublishability::Publishable
                } else {
                    FindingPublishability::NotPublishable
                },
                evidence,
                file_refs,
                discovered_by: vec![discovered_by],
                challenged_by: Vec::new(),
            },
        });
        id
    }

    pub(crate) fn all(&self) -> Vec<FindingV1> {
        self.findings
            .lock()
            .expect("finding store poisoned")
            .iter()
            .map(|record| record.finding.clone())
            .collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.findings.lock().expect("finding store poisoned").len()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum AgentEvent {
    ModelAction {
        summary: String,
    },
    ToolResult {
        tool_call_id: String,
        tool: ToolName,
        artifact_id: ArtifactId,
        summary: String,
        completeness: Completeness,
    },
    Finding {
        finding_id: FindingId,
        summary: String,
    },
    ToolDenied {
        tool_call_id: String,
        tool: ToolName,
        error_code: String,
    },
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolName {
    ListChangedFiles,
    ReadDiff,
    ListFiles,
    ReadFile,
    ReadBaseFile,
    ReadHeadFile,
    SearchText,
    FindRelatedFiles,
    FindTestsForFile,
    ListImports,
    RecordFinding,
    ChallengeFinding,
    Finish,
}

impl ToolName {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ListChangedFiles => "list_changed_files",
            Self::ReadDiff => "read_diff",
            Self::ListFiles => "list_files",
            Self::ReadFile => "read_file",
            Self::ReadBaseFile => "read_base_file",
            Self::ReadHeadFile => "read_head_file",
            Self::SearchText => "search_text",
            Self::FindRelatedFiles => "find_related_files",
            Self::FindTestsForFile => "find_tests_for_file",
            Self::ListImports => "list_imports",
            Self::RecordFinding => "record_finding",
            Self::ChallengeFinding => "challenge_finding",
            Self::Finish => "finish",
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolStatus {
    Ok,
    Denied,
    NotFound,
    InvalidArgs,
    BudgetExceeded,
    Timeout,
    InternalError,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToolResultV1 {
    pub(crate) tool_call_id: String,
    pub(crate) tool_name: ToolName,
    pub(crate) status: ToolStatus,
    pub(crate) error_code: Option<String>,
    pub(crate) completeness: Completeness,
    pub(crate) artifact_ids: Vec<String>,
    pub(crate) summary: String,
    pub(crate) bytes_read: usize,
    pub(crate) bytes_returned: usize,
    pub(crate) duration_ms: u64,
    pub(crate) redaction: RedactionMetadataV1,
}

#[derive(Debug, Clone)]
pub(crate) enum ModelAction {
    ListChangedFiles,
    ReadDiff,
    ListFiles,
    ReadFile(PathBuf),
    ReadBaseFile(PathBuf),
    ReadHeadFile(PathBuf),
    SearchText(String),
    FindRelatedFiles(PathBuf),
    FindTestsForFile(PathBuf),
    ListImports(PathBuf),
    RecordFinding {
        title: String,
        claim: String,
    },
    ChallengeFinding {
        finding_id: String,
        rationale: String,
    },
    Finish(String),
}

impl ModelAction {
    pub(crate) fn tool_name(&self) -> ToolName {
        match self {
            Self::ListChangedFiles => ToolName::ListChangedFiles,
            Self::ReadDiff => ToolName::ReadDiff,
            Self::ListFiles => ToolName::ListFiles,
            Self::ReadFile(_) => ToolName::ReadFile,
            Self::ReadBaseFile(_) => ToolName::ReadBaseFile,
            Self::ReadHeadFile(_) => ToolName::ReadHeadFile,
            Self::SearchText(_) => ToolName::SearchText,
            Self::FindRelatedFiles(_) => ToolName::FindRelatedFiles,
            Self::FindTestsForFile(_) => ToolName::FindTestsForFile,
            Self::ListImports(_) => ToolName::ListImports,
            Self::RecordFinding { .. } => ToolName::RecordFinding,
            Self::ChallengeFinding { .. } => ToolName::ChallengeFinding,
            Self::Finish(_) => ToolName::Finish,
        }
    }

    pub(crate) fn summary(&self) -> String {
        match self {
            Self::ListChangedFiles => "model chose list_changed_files".to_string(),
            Self::ReadDiff => "model chose read_diff".to_string(),
            Self::ListFiles => "model chose list_files".to_string(),
            Self::ReadFile(path) => format!("model chose read_file {}", path.display()),
            Self::ReadBaseFile(path) => format!("model chose read_base_file {}", path.display()),
            Self::ReadHeadFile(path) => format!("model chose read_head_file {}", path.display()),
            Self::SearchText(query) => format!("model chose search_text {query}"),
            Self::FindRelatedFiles(path) => {
                format!("model chose find_related_files {}", path.display())
            }
            Self::FindTestsForFile(path) => {
                format!("model chose find_tests_for_file {}", path.display())
            }
            Self::ListImports(path) => format!("model chose list_imports {}", path.display()),
            Self::RecordFinding { title, .. } => format!("model chose record_finding {title}"),
            Self::ChallengeFinding { finding_id, .. } => {
                format!("model chose challenge_finding {finding_id}")
            }
            Self::Finish(reason) => format!("model finished: {reason}"),
        }
    }
}

#[derive(Debug, Default, Copy, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TokenUsage {
    pub(crate) input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) total_tokens: u64,
}

impl TokenUsage {
    pub(crate) fn add(&mut self, other: TokenUsage) {
        self.input_tokens += other.input_tokens;
        self.output_tokens += other.output_tokens;
        self.total_tokens += other.total_tokens;
    }
}

#[derive(Debug, Default, Copy, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToolCounts {
    pub(crate) list_changed_files: usize,
    pub(crate) read_diff: usize,
    pub(crate) list_files: usize,
    pub(crate) read_file: usize,
    pub(crate) read_base_file: usize,
    pub(crate) read_head_file: usize,
    pub(crate) search_text: usize,
    pub(crate) find_related_files: usize,
    pub(crate) find_tests_for_file: usize,
    pub(crate) list_imports: usize,
    pub(crate) record_finding: usize,
    pub(crate) challenge_finding: usize,
    pub(crate) finish: usize,
}

impl ToolCounts {
    pub(crate) fn add(&mut self, other: ToolCounts) {
        self.list_changed_files += other.list_changed_files;
        self.read_diff += other.read_diff;
        self.list_files += other.list_files;
        self.read_file += other.read_file;
        self.read_base_file += other.read_base_file;
        self.read_head_file += other.read_head_file;
        self.search_text += other.search_text;
        self.find_related_files += other.find_related_files;
        self.find_tests_for_file += other.find_tests_for_file;
        self.list_imports += other.list_imports;
        self.record_finding += other.record_finding;
        self.challenge_finding += other.challenge_finding;
        self.finish += other.finish;
    }

    pub(crate) fn increment(&mut self, tool: ToolName) {
        match tool {
            ToolName::ListChangedFiles => self.list_changed_files += 1,
            ToolName::ReadDiff => self.read_diff += 1,
            ToolName::ListFiles => self.list_files += 1,
            ToolName::ReadFile => self.read_file += 1,
            ToolName::ReadBaseFile => self.read_base_file += 1,
            ToolName::ReadHeadFile => self.read_head_file += 1,
            ToolName::SearchText => self.search_text += 1,
            ToolName::FindRelatedFiles => self.find_related_files += 1,
            ToolName::FindTestsForFile => self.find_tests_for_file += 1,
            ToolName::ListImports => self.list_imports += 1,
            ToolName::RecordFinding => self.record_finding += 1,
            ToolName::ChallengeFinding => self.challenge_finding += 1,
            ToolName::Finish => self.finish += 1,
        }
    }

    pub(crate) fn total(self) -> usize {
        self.list_changed_files
            + self.read_diff
            + self.list_files
            + self.read_file
            + self.read_base_file
            + self.read_head_file
            + self.search_text
            + self.find_related_files
            + self.find_tests_for_file
            + self.list_imports
            + self.record_finding
            + self.challenge_finding
            + self.finish
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BlackboardEntry {
    pub(crate) session_id: String,
    pub(crate) entry_type: &'static str,
    pub(crate) summary: String,
    pub(crate) artifact_id: Option<String>,
    pub(crate) finding_id: Option<String>,
}

#[derive(Debug, Default)]
pub(crate) struct Blackboard {
    pub(crate) entries: Mutex<Vec<BlackboardEntry>>,
}

impl Blackboard {
    pub(crate) fn push(&self, entry: BlackboardEntry) {
        self.entries
            .lock()
            .expect("blackboard poisoned")
            .push(entry);
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.lock().expect("blackboard poisoned").len()
    }
}

#[derive(Debug, Copy, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AgentState {
    Ready,
    Running,
    Done,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AgentSession {
    pub(crate) id: String,
    pub(crate) run_id: String,
    pub(crate) role: Role,
    pub(crate) objective: String,
    pub(crate) cwd: PathBuf,
    pub(crate) model_profile_id: String,
    pub(crate) state: AgentState,
    pub(crate) budget: AgentBudget,
    pub(crate) allowed_tools: ToolMask,
    pub(crate) events: Vec<AgentEvent>,
}
