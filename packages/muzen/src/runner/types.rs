use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::contracts::Role;
use crate::reviewer::artifacts::ArtifactView;

fn default_role() -> Role {
    Role::Generalist
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerHandshakeParams {
    pub protocol_version: String,
    #[serde(default)]
    pub client_name: Option<String>,
    #[serde(default)]
    pub client_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunStartParams {
    #[serde(default)]
    pub protocol_version: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    pub repo: PathBuf,
    #[serde(default)]
    pub changed_files: Vec<String>,
    #[serde(default)]
    pub sessions: Vec<RunSessionParams>,
    #[serde(default)]
    pub limits: Option<RunLimitParams>,
    #[serde(default)]
    pub model: Option<RunModelParams>,
    #[serde(default)]
    pub tools: Vec<RunToolParams>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunModelParams {
    #[serde(default)]
    pub callback: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunToolParams {
    pub id: String,
    pub description: String,
    pub parameters: Value,
    #[serde(default)]
    pub cacheable: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSessionParams {
    pub id: String,
    #[serde(default = "default_role")]
    pub role: Role,
    pub objective: String,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub model_profile_id: Option<String>,
    #[serde(default)]
    pub budget: Option<RunAgentBudgetParams>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunAgentBudgetParams {
    pub max_turns: usize,
    pub max_tool_calls: usize,
    pub max_prompt_tokens: u64,
    pub max_output_tokens: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunLimitParams {
    #[serde(default)]
    pub max_active_sessions: Option<usize>,
    #[serde(default)]
    pub max_file_bytes: Option<usize>,
    #[serde(default)]
    pub max_search_matches: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunLookupParams {
    pub run_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactReadParams {
    pub run_id: String,
    pub artifact_id: String,
    #[serde(default)]
    pub view: RunnerArtifactView,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactExportParams {
    pub run_id: String,
    #[serde(default)]
    pub artifact_ids: Vec<String>,
    #[serde(default)]
    pub view: RunnerArtifactView,
    #[serde(default)]
    pub max_artifacts: Option<usize>,
    #[serde(default)]
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotReadTextParams {
    pub run_id: String,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    pub path: String,
    #[serde(default)]
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunStatusResult {
    pub run_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunCancelResult {
    pub run_id: String,
    pub status: String,
    pub cancelled: bool,
    pub reason: String,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunnerArtifactView {
    Redacted,
    Raw,
}

impl Default for RunnerArtifactView {
    fn default() -> Self {
        Self::Redacted
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerArtifact {
    pub artifact_id: String,
    pub bytes: usize,
    pub content_hash: String,
    pub content: String,
}

impl RunnerArtifact {
    pub(crate) fn from_artifact_view(artifact: ArtifactView) -> Self {
        Self {
            artifact_id: artifact.artifact_id.0,
            bytes: artifact.bytes,
            content_hash: artifact.content_hash,
            content: artifact.content,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerArtifactReadResult {
    pub run_id: String,
    pub view: RunnerArtifactView,
    pub artifact: RunnerArtifact,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerArtifactExportResult {
    pub run_id: String,
    pub view: RunnerArtifactView,
    pub artifact_count: usize,
    pub total_bytes: usize,
    pub artifacts: Vec<RunnerArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerSnapshotTextResult {
    pub run_id: String,
    pub snapshot_id: String,
    pub path: String,
    pub content_hash: String,
    pub bytes: usize,
    pub truncated: bool,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerRunResult {
    pub protocol_version: String,
    pub run_id: String,
    pub status: String,
    pub summary: RunnerRunSummary,
    pub findings: Vec<RunnerFinding>,
    pub snapshots: Vec<RunnerSnapshotSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerRunSummary {
    pub sessions: usize,
    pub completed_sessions: usize,
    pub model_calls: usize,
    pub tool_calls: usize,
    pub findings: usize,
    pub publishable_findings: usize,
    pub elapsed_ms: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub artifacts: usize,
    pub artifact_bytes: usize,
    pub snapshot_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerFinding {
    pub id: String,
    pub title: String,
    pub claim: String,
    pub evidence_count: usize,
    pub publishable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerSnapshotSummary {
    pub snapshot_id: String,
    pub files: usize,
    pub changed_files: usize,
    pub captured_files: usize,
    pub captured_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerHandshakeResult {
    pub protocol_version: String,
    pub runner_name: String,
    pub runner_version: String,
    pub capabilities: RunnerCapabilities,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerCapabilities {
    pub supported_methods: Vec<String>,
    pub planned_methods: Vec<String>,
    pub transports: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerCheckResult {
    pub ok: bool,
    pub protocol_version: String,
    pub runner_name: String,
    pub runner_version: String,
    pub rust_package: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerProtocolSchema {
    pub schema_version: String,
    pub transport: String,
    pub requests: Vec<RunnerMethodSchema>,
    pub callbacks: Vec<RunnerMethodSchema>,
    pub notifications: Vec<RunnerMethodSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerMethodSchema {
    pub method: String,
    pub direction: RunnerMessageDirection,
    pub status: RunnerMethodStatus,
    pub summary: String,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunnerMessageDirection {
    SdkToRunner,
    RunnerToSdk,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunnerMethodStatus {
    Implemented,
    Reserved,
}
