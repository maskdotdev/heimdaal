use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::contracts::{AgentBudget, Role};
use crate::reviewer::{
    artifacts::ArtifactView, capabilities, paths, Cancellation, ChangeSpec, ChangedFileSpec,
    ReviewEventRecord, ReviewEventSink, ReviewModel, ReviewModelRequest, ReviewModelTurn,
    ReviewRunLimits, ReviewRunSummary, ReviewSessionSpec, ReviewToolCall, Run, RunSpec,
    SnapshotPathPolicy, SnapshotReader, SnapshotSpec, TokenUsage,
};

pub const RUNNER_PROTOCOL_VERSION: &str = "muzen.runner.v1";
pub const RUNNER_NAME: &str = "muzen-runner";

#[derive(Parser, Debug)]
#[command(name = RUNNER_NAME)]
#[command(about = "Muzen SDK runner protocol host")]
pub struct RunnerCli {
    #[command(subcommand)]
    command: RunnerCommand,
}

#[derive(Subcommand, Debug)]
pub enum RunnerCommand {
    /// Serve newline-delimited JSON-RPC over stdin/stdout.
    Stdio,
    /// Print local runner diagnostics.
    Check,
    /// Print protocol schema metadata.
    Schema {
        #[command(subcommand)]
        command: RunnerSchemaCommand,
    },
}

#[derive(Subcommand, Debug)]
pub enum RunnerSchemaCommand {
    /// Export the runner protocol schema metadata as JSON.
    Export,
}

pub fn main_entry() {
    let code = match run_main() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error:#}");
            4
        }
    };
    std::process::exit(code);
}

pub fn run_main() -> Result<i32> {
    let cli = RunnerCli::parse();
    match cli.command {
        RunnerCommand::Stdio => {
            let stdin = std::io::stdin();
            let stdout = std::io::stdout();
            let mut reader = std::io::BufReader::new(stdin.lock());
            let mut writer = stdout.lock();
            run_stdio(&mut reader, &mut writer)
        }
        RunnerCommand::Check => {
            println!("{}", serde_json::to_string_pretty(&runner_check())?);
            Ok(0)
        }
        RunnerCommand::Schema {
            command: RunnerSchemaCommand::Export,
        } => {
            println!("{}", serde_json::to_string_pretty(&protocol_schema())?);
            Ok(0)
        }
    }
}

pub fn run_stdio<R, W>(reader: &mut R, writer: &mut W) -> Result<i32>
where
    R: BufRead,
    W: Write,
{
    let mut session = RunnerStdioSession::default();
    let mut line = String::new();
    loop {
        line.clear();
        let bytes = reader
            .read_line(&mut line)
            .context("failed to read runner protocol frame")?;
        if bytes == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        session.handle_line(line.trim_end(), writer)?;
    }
    Ok(0)
}

pub fn handle_jsonrpc_line(line: &str) -> JsonRpcResponse {
    match serde_json::from_str::<JsonRpcRequest>(line) {
        Ok(request) => handle_request(request),
        Err(error) => JsonRpcResponse::error(
            None,
            JsonRpcError::parse_error(format!("invalid JSON-RPC request: {error}")),
        ),
    }
}

fn handle_request(request: JsonRpcRequest) -> JsonRpcResponse {
    if request.jsonrpc != "2.0" {
        return JsonRpcResponse::error(
            request.id,
            JsonRpcError::invalid_request("jsonrpc must be 2.0"),
        );
    }
    match request.method.as_str() {
        "runner.handshake" => {
            let params = parse_params::<RunnerHandshakeParams>(request.params);
            match params {
                Ok(params) => {
                    if params.protocol_version != RUNNER_PROTOCOL_VERSION {
                        return JsonRpcResponse::error(
                            request.id,
                            JsonRpcError::protocol_error(format!(
                                "unsupported protocolVersion {}",
                                params.protocol_version
                            )),
                        );
                    }
                    JsonRpcResponse::success(request.id, json!(runner_handshake()))
                }
                Err(error) => JsonRpcResponse::error(request.id, error),
            }
        }
        "runner.check" => JsonRpcResponse::success(request.id, json!(runner_check())),
        "runner.schema.export" => JsonRpcResponse::success(request.id, json!(protocol_schema())),
        "run.start" | "run.cancel" | "run.status" | "run.result" | "artifact.read"
        | "artifact.export" | "snapshot.readText" => JsonRpcResponse::error(
            request.id,
            JsonRpcError::not_implemented(format!(
                "{} requires the stateful stdio session in {}",
                request.method, RUNNER_PROTOCOL_VERSION
            )),
        ),
        _ => JsonRpcResponse::error(
            request.id,
            JsonRpcError::method_not_found(format!("unknown method {}", request.method)),
        ),
    }
}

#[derive(Default)]
struct RunnerStdioSession {
    reports: BTreeMap<String, RunnerStoredRun>,
}

impl RunnerStdioSession {
    fn handle_line<W: Write>(&mut self, line: &str, writer: &mut W) -> Result<()> {
        let response = match serde_json::from_str::<JsonRpcRequest>(line) {
            Ok(request) => self.handle_stateful_request(request, writer)?,
            Err(error) => JsonRpcResponse::error(
                None,
                JsonRpcError::parse_error(format!("invalid JSON-RPC request: {error}")),
            ),
        };
        write_response(writer, &response)?;
        Ok(())
    }

    fn handle_stateful_request<W: Write>(
        &mut self,
        request: JsonRpcRequest,
        writer: &mut W,
    ) -> Result<JsonRpcResponse> {
        if !stateful_method(request.method.as_str()) {
            return Ok(handle_request(request));
        }
        if request.jsonrpc != "2.0" {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::invalid_request("jsonrpc must be 2.0"),
            ));
        }
        match request.method.as_str() {
            "run.start" => {
                let params = match parse_params::<RunStartParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
                };
                match execute_run_start(params) {
                    Ok(executed) => {
                        for event in &executed.events {
                            write_notification(writer, "event.review", json!(event))?;
                        }
                        let result = executed.result.clone();
                        write_notification(writer, "run.finished", json!(result.clone()))?;
                        self.reports.insert(result.run_id.clone(), executed.stored);
                        Ok(JsonRpcResponse::success(request.id, json!(result)))
                    }
                    Err(error) => {
                        let runner_error = JsonRpcError::runner_error(error.to_string());
                        write_notification(
                            writer,
                            "run.failed",
                            json!({"error": runner_error.message, "kind": "runner_error"}),
                        )?;
                        Ok(JsonRpcResponse::error(request.id, runner_error))
                    }
                }
            }
            "run.status" => {
                let params = match parse_params::<RunLookupParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
                };
                let Some(stored) = self.reports.get(&params.run_id) else {
                    return Ok(JsonRpcResponse::error(
                        request.id,
                        JsonRpcError::invalid_params(format!("unknown runId {}", params.run_id)),
                    ));
                };
                Ok(JsonRpcResponse::success(
                    request.id,
                    json!(RunStatusResult {
                        run_id: params.run_id,
                        status: stored.status.clone(),
                    }),
                ))
            }
            "run.result" => {
                let params = match parse_params::<RunLookupParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
                };
                let Some(stored) = self.reports.get(&params.run_id) else {
                    return Ok(JsonRpcResponse::error(
                        request.id,
                        JsonRpcError::invalid_params(format!("unknown runId {}", params.run_id)),
                    ));
                };
                Ok(JsonRpcResponse::success(
                    request.id,
                    json!(stored.result.clone()),
                ))
            }
            "run.cancel" => self.handle_run_cancel(request),
            "artifact.read" => self.handle_artifact_read(request),
            "artifact.export" => self.handle_artifact_export(request),
            "snapshot.readText" => self.handle_snapshot_read_text(request),
            _ => Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::method_not_found(format!("unknown method {}", request.method)),
            )),
        }
    }

    fn handle_run_cancel(&mut self, request: JsonRpcRequest) -> Result<JsonRpcResponse> {
        let params = match parse_params::<RunLookupParams>(request.params) {
            Ok(params) => params,
            Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
        };
        let Some(stored) = self.reports.get(&params.run_id) else {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::invalid_params(format!("unknown runId {}", params.run_id)),
            ));
        };
        Ok(JsonRpcResponse::success(
            request.id,
            json!(RunCancelResult {
                run_id: params.run_id,
                status: stored.status.clone(),
                cancelled: false,
                reason: "run already reached a terminal state".to_string(),
            }),
        ))
    }

    fn handle_artifact_read(&self, request: JsonRpcRequest) -> Result<JsonRpcResponse> {
        let params = match parse_params::<ArtifactReadParams>(request.params) {
            Ok(params) => params,
            Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
        };
        let Some(stored) = self.reports.get(&params.run_id) else {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::invalid_params(format!("unknown runId {}", params.run_id)),
            ));
        };
        let Some(artifact) = stored.artifact(params.view, &params.artifact_id) else {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::invalid_params(format!("unknown artifactId {}", params.artifact_id)),
            ));
        };
        Ok(JsonRpcResponse::success(
            request.id,
            json!(RunnerArtifactReadResult {
                run_id: params.run_id,
                view: params.view,
                artifact: artifact.clone(),
            }),
        ))
    }

    fn handle_artifact_export(&self, request: JsonRpcRequest) -> Result<JsonRpcResponse> {
        let params = match parse_params::<ArtifactExportParams>(request.params) {
            Ok(params) => params,
            Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
        };
        let Some(stored) = self.reports.get(&params.run_id) else {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::invalid_params(format!("unknown runId {}", params.run_id)),
            ));
        };
        let mut artifacts = stored.artifacts(params.view).to_vec();
        if !params.artifact_ids.is_empty() {
            artifacts.retain(|artifact| {
                params
                    .artifact_ids
                    .iter()
                    .any(|artifact_id| artifact_id == &artifact.artifact_id)
            });
        }
        let total_bytes = artifacts
            .iter()
            .map(|artifact| artifact.bytes)
            .sum::<usize>();
        if params
            .max_artifacts
            .is_some_and(|max_artifacts| artifacts.len() > max_artifacts)
        {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::limit_exceeded("artifact_retention_artifacts"),
            ));
        }
        if params
            .max_bytes
            .is_some_and(|max_bytes| total_bytes > max_bytes)
        {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::limit_exceeded("artifact_retention_bytes"),
            ));
        }
        Ok(JsonRpcResponse::success(
            request.id,
            json!(RunnerArtifactExportResult {
                run_id: params.run_id,
                view: params.view,
                artifact_count: artifacts.len(),
                total_bytes,
                artifacts,
            }),
        ))
    }

    fn handle_snapshot_read_text(&self, request: JsonRpcRequest) -> Result<JsonRpcResponse> {
        let params = match parse_params::<SnapshotReadTextParams>(request.params) {
            Ok(params) => params,
            Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
        };
        let Some(stored) = self.reports.get(&params.run_id) else {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::invalid_params(format!("unknown runId {}", params.run_id)),
            ));
        };
        let reader = match stored.snapshot_reader(params.snapshot_id.as_deref()) {
            Ok(reader) => reader,
            Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
        };
        let max_bytes = params.max_bytes.unwrap_or(200 * 1024);
        match reader.read_text_path(&params.path, max_bytes) {
            Ok(file) => Ok(JsonRpcResponse::success(
                request.id,
                json!(RunnerSnapshotTextResult {
                    run_id: params.run_id,
                    snapshot_id: file.snapshot_id.0,
                    path: file.path.display(),
                    content_hash: file.content_hash,
                    bytes: file.bytes,
                    truncated: file.truncated,
                    content: file.content,
                }),
            )),
            Err(error) => Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::runner_error(error.to_string()),
            )),
        }
    }
}

fn stateful_method(method: &str) -> bool {
    matches!(
        method,
        "run.start"
            | "run.cancel"
            | "run.status"
            | "run.result"
            | "artifact.read"
            | "artifact.export"
            | "snapshot.readText"
    )
}

fn write_response<W: Write>(writer: &mut W, response: &JsonRpcResponse) -> Result<()> {
    serde_json::to_writer(&mut *writer, response)
        .context("failed to write runner protocol response")?;
    writer
        .write_all(b"\n")
        .context("failed to terminate runner protocol response")?;
    writer.flush().context("failed to flush runner response")?;
    Ok(())
}

fn write_notification<W: Write>(writer: &mut W, method: &str, params: Value) -> Result<()> {
    let notification = JsonRpcNotification {
        jsonrpc: "2.0",
        method: method.to_string(),
        params,
    };
    serde_json::to_writer(&mut *writer, &notification)
        .context("failed to write runner protocol notification")?;
    writer
        .write_all(b"\n")
        .context("failed to terminate runner protocol notification")?;
    writer
        .flush()
        .context("failed to flush runner protocol notification")?;
    Ok(())
}

fn parse_params<T>(params: Option<Value>) -> Result<T, JsonRpcError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|error| JsonRpcError::invalid_params(error.to_string()))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcNotification {
    pub jsonrpc: &'static str,
    pub method: String,
    pub params: Value,
}

impl JsonRpcResponse {
    fn success(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    fn error(id: Option<Value>, error: JsonRpcError) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<RunnerErrorData>,
}

impl JsonRpcError {
    fn parse_error(message: impl Into<String>) -> Self {
        Self::new(-32700, "protocol_error", message)
    }

    fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(-32600, "invalid_request", message)
    }

    fn method_not_found(message: impl Into<String>) -> Self {
        Self::new(-32601, "method_not_found", message)
    }

    fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(-32602, "invalid_input", message)
    }

    fn protocol_error(message: impl Into<String>) -> Self {
        Self::new(-32000, "protocol_error", message)
    }

    fn not_implemented(message: impl Into<String>) -> Self {
        Self::new(-32001, "not_implemented", message)
    }

    fn runner_error(message: impl Into<String>) -> Self {
        Self::new(-32002, "runner_error", message)
    }

    fn limit_exceeded(kind: impl Into<String>) -> Self {
        let kind = kind.into();
        Self::new(-32003, "limit_exceeded", format!("limit exceeded: {kind}"))
    }

    fn new(code: i64, kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: Some(RunnerErrorData { kind: kind.into() }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerErrorData {
    pub kind: String,
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

#[derive(Debug, Clone)]
struct RunnerStoredRun {
    status: String,
    result: RunnerRunResult,
    redacted_artifacts: Vec<RunnerArtifact>,
    raw_artifacts: Vec<RunnerArtifact>,
    snapshot_readers: Vec<SnapshotReader>,
}

impl RunnerStoredRun {
    fn from_report(report: &crate::reviewer::RunReport, result: RunnerRunResult) -> Self {
        Self {
            status: result.status.clone(),
            result,
            redacted_artifacts: report
                .artifacts
                .list()
                .into_iter()
                .map(RunnerArtifact::from_artifact_view)
                .collect(),
            raw_artifacts: report
                .artifacts
                .list_raw()
                .into_iter()
                .map(RunnerArtifact::from_artifact_view)
                .collect(),
            snapshot_readers: report.snapshot_readers(),
        }
    }

    fn artifact(&self, view: RunnerArtifactView, artifact_id: &str) -> Option<&RunnerArtifact> {
        self.artifacts(view)
            .iter()
            .find(|artifact| artifact.artifact_id == artifact_id)
    }

    fn artifacts(&self, view: RunnerArtifactView) -> &[RunnerArtifact] {
        match view {
            RunnerArtifactView::Redacted => &self.redacted_artifacts,
            RunnerArtifactView::Raw => &self.raw_artifacts,
        }
    }

    fn snapshot_reader(&self, snapshot_id: Option<&str>) -> Result<&SnapshotReader, JsonRpcError> {
        match snapshot_id {
            Some(snapshot_id) => self
                .snapshot_readers
                .iter()
                .find(|reader| reader.snapshot_id().0 == snapshot_id)
                .ok_or_else(|| {
                    JsonRpcError::invalid_params(format!("unknown snapshotId {snapshot_id}"))
                }),
            None if self.snapshot_readers.len() == 1 => Ok(&self.snapshot_readers[0]),
            None => Err(JsonRpcError::invalid_params(
                "snapshotId is required for multi-snapshot runs",
            )),
        }
    }
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
    fn from_artifact_view(artifact: ArtifactView) -> Self {
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

struct ExecutedRun {
    result: RunnerRunResult,
    events: Vec<ReviewEventRecord>,
    stored: RunnerStoredRun,
}

fn default_role() -> Role {
    Role::Generalist
}

fn execute_run_start(params: RunStartParams) -> Result<ExecutedRun> {
    if let Some(protocol_version) = &params.protocol_version {
        if protocol_version != RUNNER_PROTOCOL_VERSION {
            anyhow::bail!("unsupported protocolVersion {protocol_version}");
        }
    }
    let run_id = params.run_id.unwrap_or_else(|| "muzen-run".to_string());
    let repo_root = params.repo;
    let target_path = select_target_path(&repo_root, &params.changed_files)?;
    let changed_files = changed_file_specs(&repo_root, &params.changed_files, &target_path);
    let change = ChangeSpec::local("sdk-run", "head", changed_files);
    let max_file_bytes = params
        .limits
        .as_ref()
        .and_then(|limits| limits.max_file_bytes)
        .unwrap_or(200 * 1024);
    let max_search_matches = params
        .limits
        .as_ref()
        .and_then(|limits| limits.max_search_matches)
        .unwrap_or(120);
    let max_active_sessions = params
        .limits
        .as_ref()
        .and_then(|limits| limits.max_active_sessions)
        .unwrap_or_else(|| params.sessions.len().max(1));
    let snapshot = SnapshotSpec::new(&repo_root, change).with_path_policy(
        SnapshotPathPolicy::standard(max_file_bytes, max_search_matches),
    );
    let sessions = if params.sessions.is_empty() {
        vec![RunSessionParams {
            id: "generalist".to_string(),
            role: Role::Generalist,
            objective: "Review the repository change.".to_string(),
            cwd: None,
            model_profile_id: None,
            budget: None,
        }]
    } else {
        params.sessions
    };
    let session_specs = sessions
        .into_iter()
        .map(run_session_spec)
        .collect::<Result<Vec<_>>>()?;
    let limits = ReviewRunLimits::standard(max_active_sessions, max_file_bytes, max_search_matches);
    let spec = RunSpec::single_snapshot(run_id.clone(), snapshot, session_specs, limits);
    let event_sink = Arc::new(RecordingReviewEventSink::default());
    let model = Arc::new(DeterministicRunnerModel {
        target_path,
        search_query: "TODO|fn|class|export|pub".to_string(),
    });
    let run = Run::builder(spec)
        .review_model(model)
        .review_event_sink(event_sink.clone())
        .build()
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build runner tokio runtime")?;
    let report = runtime.block_on(run.execute_with_cancel(CancellationToken::new()));
    let result = runner_result_from_report(&report);
    let stored = RunnerStoredRun::from_report(&report, result.clone());
    Ok(ExecutedRun {
        result,
        events: event_sink.records(),
        stored,
    })
}

fn run_session_spec(params: RunSessionParams) -> Result<ReviewSessionSpec> {
    let budget = params.budget.map_or(
        AgentBudget {
            max_turns: 7,
            max_tool_calls: 14,
            max_prompt_tokens: 64_000,
            max_output_tokens: 8_000,
        },
        |budget| AgentBudget {
            max_turns: budget.max_turns,
            max_tool_calls: budget.max_tool_calls,
            max_prompt_tokens: budget.max_prompt_tokens,
            max_output_tokens: budget.max_output_tokens,
        },
    );
    let mut spec =
        ReviewSessionSpec::review_read_only(params.id, params.role, params.objective, budget);
    if let Some(model_profile_id) = params.model_profile_id {
        spec = spec.with_model_profile_id(model_profile_id);
    }
    if let Some(cwd) = params.cwd {
        let repo_path = paths::RepoPath::parse(&cwd).map_err(|error| anyhow::anyhow!("{error}"))?;
        let capabilities = capabilities::CapabilitySet::review_read_only()
            .with_fs_scope(capabilities::FsScope::subtree(repo_path));
        spec = spec.with_capabilities(capabilities);
    }
    Ok(spec)
}

fn runner_result_from_report(report: &crate::reviewer::RunReport) -> RunnerRunResult {
    let summary = runner_summary_from_review(&report.summary);
    let snapshots = report
        .snapshot_manifests()
        .into_iter()
        .map(|manifest| RunnerSnapshotSummary {
            snapshot_id: manifest.snapshot_id.0,
            files: manifest.files.len(),
            changed_files: manifest.changed_files.len(),
            captured_files: manifest
                .files
                .iter()
                .filter(|file| {
                    matches!(
                        file.capture_status,
                        crate::reviewer::storage::SnapshotCaptureStatus::Captured
                    )
                })
                .count(),
            captured_bytes: manifest.captured_text_bytes as u64,
        })
        .collect();
    let findings = report
        .findings()
        .into_iter()
        .map(|finding| RunnerFinding {
            id: finding.id,
            title: finding.title,
            claim: finding.claim,
            evidence_count: finding.evidence_count,
            publishable: finding.publishable,
        })
        .collect();
    RunnerRunResult {
        protocol_version: RUNNER_PROTOCOL_VERSION.to_string(),
        run_id: report.run_id.clone(),
        status: summary_status(&summary),
        summary,
        findings,
        snapshots,
    }
}

fn runner_summary_from_review(summary: &ReviewRunSummary) -> RunnerRunSummary {
    RunnerRunSummary {
        sessions: summary.sessions,
        completed_sessions: summary.completed_sessions,
        model_calls: summary.model_calls,
        tool_calls: summary.tool_calls,
        findings: summary.findings,
        publishable_findings: summary.publishable_findings,
        elapsed_ms: summary.elapsed_ms,
        input_tokens: summary.input_tokens,
        output_tokens: summary.output_tokens,
        total_tokens: summary.total_tokens,
        artifacts: summary.artifacts,
        artifact_bytes: summary.artifact_bytes,
        snapshot_count: summary.snapshot_count,
    }
}

fn summary_status(summary: &RunnerRunSummary) -> String {
    if summary.completed_sessions == summary.sessions {
        "completed".to_string()
    } else {
        "partial".to_string()
    }
}

fn changed_file_specs(
    repo_root: &Path,
    changed_files: &[String],
    target_path: &str,
) -> Vec<ChangedFileSpec> {
    let files = if changed_files.is_empty() {
        vec![target_path.to_string()]
    } else {
        changed_files.to_vec()
    };
    files
        .into_iter()
        .filter(|path| repo_root.join(path).is_file())
        .map(ChangedFileSpec::modified)
        .collect()
}

fn select_target_path(repo_root: &Path, changed_files: &[String]) -> Result<String> {
    for path in changed_files {
        if repo_root.join(path).is_file() {
            return Ok(path.clone());
        }
    }
    for candidate in ["Cargo.toml", "package.json", "README.md", "pyproject.toml"] {
        if repo_root.join(candidate).is_file() {
            return Ok(candidate.to_string());
        }
    }
    find_first_text_candidate(repo_root)
        .ok_or_else(|| anyhow::anyhow!("repo has no obvious text file to review"))
}

fn find_first_text_candidate(repo_root: &Path) -> Option<String> {
    fn visit(root: &Path, dir: &Path, depth: usize) -> Option<String> {
        if depth > 4 {
            return None;
        }
        let entries = fs::read_dir(dir).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if matches!(
                name.as_ref(),
                ".git" | "node_modules" | "target" | "dist" | "build" | ".next"
            ) {
                continue;
            }
            if path.is_file() && looks_textual(&path) {
                return path
                    .strip_prefix(root)
                    .ok()
                    .map(|path| path.to_string_lossy().into_owned());
            }
            if path.is_dir() {
                if let Some(found) = visit(root, &path, depth + 1) {
                    return Some(found);
                }
            }
        }
        None
    }
    visit(repo_root, repo_root, 0)
}

fn looks_textual(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            matches!(
                extension,
                "rs" | "ts"
                    | "tsx"
                    | "js"
                    | "jsx"
                    | "json"
                    | "toml"
                    | "md"
                    | "py"
                    | "go"
                    | "java"
                    | "kt"
                    | "rb"
                    | "php"
                    | "c"
                    | "h"
                    | "cpp"
                    | "hpp"
                    | "cs"
                    | "swift"
            )
        })
        .unwrap_or(false)
}

#[derive(Default)]
struct RecordingReviewEventSink {
    records: std::sync::Mutex<Vec<ReviewEventRecord>>,
}

impl RecordingReviewEventSink {
    fn records(&self) -> Vec<ReviewEventRecord> {
        self.records
            .lock()
            .expect("review event sink poisoned")
            .clone()
    }
}

impl ReviewEventSink for RecordingReviewEventSink {
    fn emit_review_event(&self, record: ReviewEventRecord) {
        self.records
            .lock()
            .expect("review event sink poisoned")
            .push(record);
    }
}

struct DeterministicRunnerModel {
    target_path: String,
    search_query: String,
}

#[async_trait]
impl ReviewModel for DeterministicRunnerModel {
    async fn complete_review(
        &self,
        request: ReviewModelRequest,
        _cancel: Cancellation,
    ) -> crate::reviewer::runtime::RuntimeResult<ReviewModelTurn> {
        let usage = TokenUsage {
            input_tokens: request.transcript_item_count() as u64 * 64,
            output_tokens: 32,
            total_tokens: request.transcript_item_count() as u64 * 64 + 32,
        };
        if request.tool_result_count() == 0 {
            return Ok(ReviewModelTurn::ToolCalls {
                usage,
                calls: vec![
                    ReviewToolCall::new("read_diff", json!({}))
                        .with_call_id(request.tool_call_id("read-diff")),
                    ReviewToolCall::new("read_file", json!({ "path": self.target_path }))
                        .with_call_id(request.tool_call_id("read-file")),
                    ReviewToolCall::new("search_text", json!({ "query": self.search_query }))
                        .with_call_id(request.tool_call_id("search")),
                ],
            });
        }
        Ok(ReviewModelTurn::ToolCalls {
            usage,
            calls: vec![ReviewToolCall::new(
                "finish",
                json!({ "reason": "deterministic SDK smoke review completed" }),
            )
            .with_call_id(request.tool_call_id("finish"))],
        })
    }
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

pub fn runner_handshake() -> RunnerHandshakeResult {
    RunnerHandshakeResult {
        protocol_version: RUNNER_PROTOCOL_VERSION.to_string(),
        runner_name: RUNNER_NAME.to_string(),
        runner_version: env!("CARGO_PKG_VERSION").to_string(),
        capabilities: RunnerCapabilities {
            supported_methods: vec![
                "runner.handshake".to_string(),
                "runner.check".to_string(),
                "runner.schema.export".to_string(),
                "run.start".to_string(),
                "run.cancel".to_string(),
                "run.status".to_string(),
                "run.result".to_string(),
                "artifact.read".to_string(),
                "artifact.export".to_string(),
                "snapshot.readText".to_string(),
                "event.review".to_string(),
                "run.finished".to_string(),
                "run.failed".to_string(),
            ],
            planned_methods: vec![
                "model.complete".to_string(),
                "tool.execute".to_string(),
                "event.runtime".to_string(),
            ],
            transports: vec!["stdio-jsonl".to_string()],
        },
    }
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

pub fn runner_check() -> RunnerCheckResult {
    RunnerCheckResult {
        ok: true,
        protocol_version: RUNNER_PROTOCOL_VERSION.to_string(),
        runner_name: RUNNER_NAME.to_string(),
        runner_version: env!("CARGO_PKG_VERSION").to_string(),
        rust_package: env!("CARGO_PKG_NAME").to_string(),
    }
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

pub fn protocol_schema() -> RunnerProtocolSchema {
    RunnerProtocolSchema {
        schema_version: RUNNER_PROTOCOL_VERSION.to_string(),
        transport: "newline-delimited JSON-RPC 2.0 over stdio".to_string(),
        requests: vec![
            implemented(
                "runner.handshake",
                "Negotiate protocol version and capabilities.",
            ),
            implemented("runner.check", "Return local runner diagnostics."),
            implemented(
                "runner.schema.export",
                "Return protocol method metadata for SDK validation.",
            ),
            implemented("run.start", "Start a review run."),
            implemented("run.cancel", "Cancel an active review run."),
            implemented("run.status", "Read active run status."),
            implemented("run.result", "Read final run report."),
            implemented("artifact.read", "Read one redacted or raw artifact."),
            implemented("artifact.export", "Export artifacts using a policy."),
            implemented("snapshot.readText", "Read captured snapshot text."),
        ],
        callbacks: vec![
            reserved_runner_to_sdk(
                "model.complete",
                "Ask the SDK model adapter for one model turn.",
            ),
            reserved_runner_to_sdk("tool.execute", "Ask the SDK to execute a host custom tool."),
        ],
        notifications: vec![
            implemented_runner_to_sdk("event.review", "Emit one host-facing review event."),
            reserved_runner_to_sdk("event.runtime", "Emit one advanced runtime event."),
            implemented_runner_to_sdk(
                "run.finished",
                "Notify that a run reached a terminal state.",
            ),
            implemented_runner_to_sdk(
                "run.failed",
                "Notify that a run failed before producing a report.",
            ),
        ],
    }
}

fn implemented(method: &'static str, summary: &'static str) -> RunnerMethodSchema {
    RunnerMethodSchema {
        method: method.to_string(),
        direction: RunnerMessageDirection::SdkToRunner,
        status: RunnerMethodStatus::Implemented,
        summary: summary.to_string(),
    }
}

fn reserved_runner_to_sdk(method: &'static str, summary: &'static str) -> RunnerMethodSchema {
    RunnerMethodSchema {
        method: method.to_string(),
        direction: RunnerMessageDirection::RunnerToSdk,
        status: RunnerMethodStatus::Reserved,
        summary: summary.to_string(),
    }
}

fn implemented_runner_to_sdk(method: &'static str, summary: &'static str) -> RunnerMethodSchema {
    RunnerMethodSchema {
        method: method.to_string(),
        direction: RunnerMessageDirection::RunnerToSdk,
        status: RunnerMethodStatus::Implemented,
        summary: summary.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn handshake_returns_protocol_version() {
        let response = handle_jsonrpc_line(
            r#"{"jsonrpc":"2.0","id":1,"method":"runner.handshake","params":{"protocolVersion":"muzen.runner.v1","clientName":"test"}}"#,
        );

        assert!(response.error.is_none());
        assert_eq!(response.id, Some(json!(1)));
        assert_eq!(
            response
                .result
                .as_ref()
                .and_then(|value| value.get("protocolVersion")),
            Some(&json!(RUNNER_PROTOCOL_VERSION))
        );
    }

    #[test]
    fn handshake_rejects_protocol_mismatch() {
        let response = handle_jsonrpc_line(
            r#"{"jsonrpc":"2.0","id":"bad","method":"runner.handshake","params":{"protocolVersion":"muzen.runner.v0"}}"#,
        );

        let error = response.error.expect("protocol error");
        assert_eq!(error.data.expect("error data").kind, "protocol_error");
    }

    #[test]
    fn schema_marks_wired_run_methods_implemented_and_future_methods_reserved() {
        let schema = protocol_schema();

        assert!(schema
            .requests
            .iter()
            .any(|method| method.method == "run.start"
                && method.status == RunnerMethodStatus::Implemented));
        assert!(schema
            .requests
            .iter()
            .any(|method| method.method == "run.result"
                && method.status == RunnerMethodStatus::Implemented));
        assert!(schema
            .requests
            .iter()
            .any(|method| method.method == "artifact.read"
                && method.status == RunnerMethodStatus::Implemented));
        assert!(schema
            .requests
            .iter()
            .any(|method| method.method == "snapshot.readText"
                && method.status == RunnerMethodStatus::Implemented));
        assert!(schema
            .notifications
            .iter()
            .any(|method| method.method == "event.review"
                && method.status == RunnerMethodStatus::Implemented));
        assert!(schema
            .callbacks
            .iter()
            .any(|method| method.method == "model.complete"
                && method.status == RunnerMethodStatus::Reserved));
    }

    #[test]
    fn stdio_handles_multiple_requests() {
        let input = br#"{"jsonrpc":"2.0","id":1,"method":"runner.check"}
{"jsonrpc":"2.0","id":2,"method":"runner.schema.export"}
"#;
        let mut reader = std::io::Cursor::new(input);
        let mut writer = Vec::new();

        run_stdio(&mut reader, &mut writer).expect("stdio run");

        let output = String::from_utf8(writer).expect("utf8 output");
        let lines = output.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"ok\":true"));
        assert!(lines[1].contains("runner.schema.export"));
    }

    #[test]
    fn stdio_starts_run_emits_events_and_stores_result() {
        let repo = tempfile::tempdir().expect("temp repo");
        std::fs::write(
            repo.path().join("Cargo.toml"),
            "[package]\nname = \"fixture\"\nversion = \"0.0.0\"\n",
        )
        .expect("fixture file");
        let start = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "run.start",
            "params": {
                "protocolVersion": RUNNER_PROTOCOL_VERSION,
                "runId": "fixture-run",
                "repo": repo.path(),
                "changedFiles": ["Cargo.toml"],
                "sessions": [
                    {
                        "id": "security",
                        "role": "security",
                        "objective": "Check fixture repo"
                    }
                ]
            }
        });
        let status = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "run.status",
            "params": {"runId": "fixture-run"}
        });
        let result = json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "run.result",
            "params": {"runId": "fixture-run"}
        });
        let input = format!("{start}\n{status}\n{result}\n");
        let mut reader = std::io::Cursor::new(input.into_bytes());
        let mut writer = Vec::new();

        run_stdio(&mut reader, &mut writer).expect("stdio run");

        let output = String::from_utf8(writer).expect("utf8 output");
        let values = output
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json line"))
            .collect::<Vec<_>>();
        assert!(values
            .iter()
            .any(|value| value.get("method") == Some(&json!("event.review"))));
        assert!(values
            .iter()
            .any(|value| value.get("method") == Some(&json!("run.finished"))));
        let start_response = values
            .iter()
            .find(|value| value.get("id") == Some(&json!(1)))
            .expect("start response");
        assert_eq!(start_response["result"]["runId"], "fixture-run");
        assert_eq!(start_response["result"]["status"], "completed");
        assert_eq!(start_response["result"]["summary"]["completedSessions"], 1);
        let status_response = values
            .iter()
            .find(|value| value.get("id") == Some(&json!(2)))
            .expect("status response");
        assert_eq!(status_response["result"]["status"], "completed");
        let result_response = values
            .iter()
            .find(|value| value.get("id") == Some(&json!(3)))
            .expect("result response");
        assert_eq!(result_response["result"]["runId"], "fixture-run");
    }

    #[test]
    fn stdio_reads_artifacts_snapshots_and_cancel_status() {
        let repo = tempfile::tempdir().expect("temp repo");
        std::fs::write(
            repo.path().join("Cargo.toml"),
            "[package]\nname = \"fixture\"\nversion = \"0.0.0\"\n",
        )
        .expect("fixture file");
        let mut session = RunnerStdioSession::default();
        let mut writer = Vec::new();
        let start = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "run.start",
            "params": {
                "protocolVersion": RUNNER_PROTOCOL_VERSION,
                "runId": "resource-run",
                "repo": repo.path(),
                "changedFiles": ["Cargo.toml"],
                "sessions": [
                    {
                        "id": "security",
                        "role": "security",
                        "objective": "Check fixture repo"
                    }
                ]
            }
        });

        session
            .handle_line(&start.to_string(), &mut writer)
            .expect("start run");
        let start_values = parse_json_lines(&writer);
        let artifact_id = start_values
            .iter()
            .find_map(|value| value.get("params")?.get("artifactId")?.as_str())
            .expect("artifact id")
            .to_string();
        let snapshot_id = start_values
            .iter()
            .find(|value| value.get("id") == Some(&json!(1)))
            .and_then(|value| value["result"]["snapshots"][0]["snapshotId"].as_str())
            .expect("snapshot id")
            .to_string();

        let read = send_jsonrpc(
            &mut session,
            &mut writer,
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "artifact.read",
                "params": {"runId": "resource-run", "artifactId": artifact_id}
            }),
        );
        assert_eq!(read[0]["result"]["view"], "redacted");
        assert!(read[0]["result"]["artifact"]["content"]
            .as_str()
            .expect("artifact content")
            .contains("Cargo.toml"));

        let export = send_jsonrpc(
            &mut session,
            &mut writer,
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "artifact.export",
                "params": {
                    "runId": "resource-run",
                    "artifactIds": [artifact_id],
                    "maxArtifacts": 1,
                    "maxBytes": 1000
                }
            }),
        );
        assert_eq!(export[0]["result"]["artifactCount"], 1);

        let snapshot = send_jsonrpc(
            &mut session,
            &mut writer,
            json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "snapshot.readText",
                "params": {
                    "runId": "resource-run",
                    "snapshotId": snapshot_id,
                    "path": "Cargo.toml",
                    "maxBytes": 1000
                }
            }),
        );
        assert_eq!(snapshot[0]["result"]["path"], "Cargo.toml");
        assert!(snapshot[0]["result"]["content"]
            .as_str()
            .expect("snapshot content")
            .contains("fixture"));

        let cancel = send_jsonrpc(
            &mut session,
            &mut writer,
            json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "run.cancel",
                "params": {"runId": "resource-run"}
            }),
        );
        assert_eq!(cancel[0]["result"]["status"], "completed");
        assert_eq!(cancel[0]["result"]["cancelled"], false);
    }

    fn send_jsonrpc(
        session: &mut RunnerStdioSession,
        writer: &mut Vec<u8>,
        request: serde_json::Value,
    ) -> Vec<serde_json::Value> {
        let start = writer.len();
        session
            .handle_line(&request.to_string(), writer)
            .expect("handle request");
        parse_json_lines(&writer[start..])
    }

    fn parse_json_lines(bytes: &[u8]) -> Vec<serde_json::Value> {
        let output = std::str::from_utf8(bytes).expect("utf8 output");
        output
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json line"))
            .collect()
    }

    #[test]
    fn handshake_fixture_matches_current_response() {
        let fixture = include_str!("../fixtures/runner-handshake-v1.jsonl");
        let lines = fixture.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);

        let actual = serde_json::to_value(handle_jsonrpc_line(lines[0])).expect("actual response");
        let expected: serde_json::Value =
            serde_json::from_str(lines[1]).expect("expected response");

        assert_eq!(actual, expected);
    }

    #[test]
    fn schema_fixture_matches_current_schema() {
        let actual = serde_json::to_value(protocol_schema()).expect("actual schema");
        let expected: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/runner-schema-v1.json"))
                .expect("expected schema");

        assert_eq!(actual, expected);
    }
}
