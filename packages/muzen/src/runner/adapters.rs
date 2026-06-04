use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::contracts::Role;
use crate::reviewer::{
    runtime::RuntimeError,
    runtime_events::{
        EventSink as RuntimeEventSink, RuntimeEvent, RuntimeEventContext, RuntimeEventRecord,
    },
    Cancellation, ReviewEvent, ReviewEventRecord, ReviewModel, ReviewModelRequest, ReviewModelTurn,
    ReviewToolArtifact, ReviewToolCall, ReviewToolContext, ReviewToolHandler, ReviewToolOutput,
    TokenUsage,
};
use crate::util::timestamp_utc;

use super::transport::RunnerCallbackTransport;
use super::RUNNER_PROTOCOL_VERSION;

pub(crate) struct DeterministicRunnerModel {
    target_path: String,
    search_query: String,
}

impl DeterministicRunnerModel {
    pub(crate) fn new(target_path: String, search_query: String) -> Self {
        Self {
            target_path,
            search_query,
        }
    }
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

pub(crate) struct CallbackReviewModel {
    run_id: String,
    transport: Arc<dyn RunnerCallbackTransport>,
}

impl CallbackReviewModel {
    pub(crate) fn new(run_id: String, transport: Arc<dyn RunnerCallbackTransport>) -> Self {
        Self { run_id, transport }
    }
}

#[async_trait]
impl ReviewModel for CallbackReviewModel {
    async fn complete_review(
        &self,
        request: ReviewModelRequest,
        _cancel: Cancellation,
    ) -> crate::reviewer::runtime::RuntimeResult<ReviewModelTurn> {
        let params = RunnerModelCompleteParams::from_request(&self.run_id, request);
        let value = self
            .transport
            .request("model.complete", json!(params))
            .map_err(runtime_error)?;
        let result =
            serde_json::from_value::<RunnerModelCompleteResult>(value).map_err(|error| {
                RuntimeError::InvalidInput(format!("invalid model.complete result: {error}"))
            })?;
        let usage = result.usage.unwrap_or_default().into_token_usage();
        if !result.tool_calls.is_empty() {
            let calls = result
                .tool_calls
                .into_iter()
                .map(|call| {
                    let mut tool_call = ReviewToolCall::new(call.tool_id, call.arguments);
                    if let Some(call_id) = call.call_id {
                        tool_call = tool_call.with_call_id(call_id);
                    }
                    tool_call
                })
                .collect();
            return Ok(ReviewModelTurn::ToolCalls { calls, usage });
        }
        Ok(ReviewModelTurn::Text {
            content: result.content.unwrap_or_default(),
            usage,
        })
    }
}

pub(crate) struct CallbackReviewTool {
    run_id: String,
    transport: Arc<dyn RunnerCallbackTransport>,
}

impl CallbackReviewTool {
    pub(crate) fn new(run_id: String, transport: Arc<dyn RunnerCallbackTransport>) -> Self {
        Self { run_id, transport }
    }
}

#[async_trait]
impl ReviewToolHandler for CallbackReviewTool {
    async fn execute_review_tool(
        &self,
        context: ReviewToolContext,
        arguments: Value,
        _cancel: Cancellation,
    ) -> crate::reviewer::runtime::RuntimeResult<ReviewToolOutput> {
        let params = RunnerToolExecuteParams {
            protocol_version: RUNNER_PROTOCOL_VERSION.to_string(),
            run_id: self.run_id.clone(),
            session_id: context.session_id,
            turn: context.turn,
            call_id: context.call_id,
            tool_id: context.tool_id,
            snapshot_id: context.snapshot_id.0,
            provider_resources: context
                .provider_resources
                .iter()
                .map(|resource| resource.as_str().to_string())
                .collect(),
            arguments,
        };
        let value = self
            .transport
            .request("tool.execute", json!(params))
            .map_err(runtime_error)?;
        let result = serde_json::from_value::<RunnerToolExecuteResult>(value).map_err(|error| {
            RuntimeError::InvalidInput(format!("invalid tool.execute result: {error}"))
        })?;
        Ok(ReviewToolOutput {
            data: result.data,
            artifact: result.artifact.map(|artifact| ReviewToolArtifact {
                key: artifact.key,
                content: artifact.content,
            }),
        })
    }
}

pub(crate) struct StreamingRunnerEventSink {
    transport: Arc<dyn RunnerCallbackTransport>,
    next_seq: AtomicU64,
}

impl StreamingRunnerEventSink {
    pub(crate) fn new(transport: Arc<dyn RunnerCallbackTransport>) -> Self {
        Self {
            transport,
            next_seq: AtomicU64::new(1),
        }
    }
}

impl RuntimeEventSink for StreamingRunnerEventSink {
    fn emit(&self, event: RuntimeEvent) {
        self.emit_with_context(RuntimeEventContext::from_event(&event), event);
    }

    fn emit_with_context(&self, context: RuntimeEventContext, event: RuntimeEvent) {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let runtime_record = RuntimeEventRecord {
            seq,
            timestamp_utc: timestamp_utc(),
            context: context.clone(),
            event: event.clone(),
        };
        let _ = self
            .transport
            .notify("event.runtime", json!(runtime_record));
        let review_record = ReviewEventRecord {
            seq,
            timestamp_utc: timestamp_utc(),
            run_id: context.run_id,
            snapshot_id: context.snapshot_id,
            session_id: context.session_id.map(|id| id.0),
            turn: context.turn_id.map(|turn| turn.0),
            tool_call_id: context.tool_call_id.map(|id| id.0),
            artifact_id: context.artifact_id,
            finding_id: context.finding_id,
            event: review_event_from_runtime(&event),
        };
        let _ = self.transport.notify("event.review", json!(review_record));
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct RunnerModelCompleteParams {
    protocol_version: String,
    run_id: String,
    session_id: String,
    role: Role,
    objective: String,
    snapshot_id: Option<String>,
    model_profile_id: Option<String>,
    turn: u32,
    transcript: Vec<Value>,
}

impl RunnerModelCompleteParams {
    fn from_request(run_id: &str, request: ReviewModelRequest) -> Self {
        Self {
            protocol_version: RUNNER_PROTOCOL_VERSION.to_string(),
            run_id: run_id.to_string(),
            session_id: request.session_id,
            role: request.role,
            objective: request.objective,
            snapshot_id: request.snapshot_id.map(|snapshot_id| snapshot_id.0),
            model_profile_id: request.model_profile_id,
            turn: request.turn,
            transcript: request
                .transcript
                .into_iter()
                .map(runner_transcript_item)
                .collect(),
        }
    }
}

fn runner_transcript_item(item: crate::reviewer::ReviewTranscriptItem) -> Value {
    match item {
        crate::reviewer::ReviewTranscriptItem::System { content } => {
            json!({"kind": "system", "content": content})
        }
        crate::reviewer::ReviewTranscriptItem::User { content } => {
            json!({"kind": "user", "content": content})
        }
        crate::reviewer::ReviewTranscriptItem::AssistantText { content } => {
            json!({"kind": "assistant_text", "content": content})
        }
        crate::reviewer::ReviewTranscriptItem::AssistantToolCalls { calls } => json!({
            "kind": "assistant_tool_calls",
            "calls": calls.into_iter().map(|call| json!({
                "callId": call.call_id,
                "toolId": call.tool_id,
                "arguments": call.arguments,
            })).collect::<Vec<_>>()
        }),
        crate::reviewer::ReviewTranscriptItem::ToolResult {
            call_id,
            tool_id,
            ok,
            artifact_id,
            data,
            error_code,
        } => json!({
            "kind": "tool_result",
            "callId": call_id,
            "toolId": tool_id,
            "ok": ok,
            "artifactId": artifact_id,
            "data": data,
            "errorCode": error_code,
        }),
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunnerModelCompleteResult {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<RunnerModelToolCallResult>,
    #[serde(default)]
    usage: Option<RunnerTokenUsage>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunnerModelToolCallResult {
    #[serde(default)]
    call_id: Option<String>,
    tool_id: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Debug, Copy, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunnerTokenUsage {
    input_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
}

impl RunnerTokenUsage {
    fn into_token_usage(self) -> TokenUsage {
        TokenUsage {
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            total_tokens: self.total_tokens,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct RunnerToolExecuteParams {
    protocol_version: String,
    run_id: String,
    session_id: String,
    turn: u32,
    call_id: String,
    tool_id: String,
    snapshot_id: String,
    provider_resources: Vec<String>,
    arguments: Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunnerToolExecuteResult {
    #[serde(default)]
    data: Option<Value>,
    #[serde(default)]
    artifact: Option<RunnerToolArtifactResult>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunnerToolArtifactResult {
    key: String,
    content: String,
}

fn runtime_error(error: anyhow::Error) -> RuntimeError {
    RuntimeError::InvalidInput(error.to_string())
}

fn review_event_from_runtime(event: &RuntimeEvent) -> ReviewEvent {
    match event {
        RuntimeEvent::JobStarted { snapshot_id } => ReviewEvent::RunStarted {
            snapshot_id: snapshot_id.clone(),
        },
        RuntimeEvent::SnapshotStarted { snapshot_id } => ReviewEvent::SnapshotStarted {
            snapshot_id: snapshot_id.clone(),
        },
        RuntimeEvent::RepoManifestCompleted {
            files,
            skipped,
            bytes,
            ms,
        } => ReviewEvent::RepoManifestCompleted {
            files: *files,
            skipped: *skipped,
            bytes: *bytes,
            ms: *ms,
        },
        RuntimeEvent::SessionStarted { session_id } => ReviewEvent::SessionStarted {
            session_id: session_id.0.clone(),
        },
        RuntimeEvent::ModelStarted {
            session_id,
            turn_id,
        } => ReviewEvent::ModelStarted {
            session_id: session_id.0.clone(),
            turn: turn_id.0,
        },
        RuntimeEvent::ModelCompleted {
            session_id,
            turn_id,
            tool_call_count,
        } => ReviewEvent::ModelCompleted {
            session_id: session_id.0.clone(),
            turn: turn_id.0,
            tool_call_count: *tool_call_count,
        },
        RuntimeEvent::ToolBatchStarted {
            session_id,
            turn_id,
            count,
        } => ReviewEvent::ToolBatchStarted {
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
        } => ReviewEvent::ToolCallCompleted {
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
        } => ReviewEvent::ToolCallDenied {
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
        } => ReviewEvent::ArtifactCreated {
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
        } => ReviewEvent::FindingRecorded {
            finding_id: finding_id.clone(),
            session_id: session_id.0.clone(),
            tool_call_id: tool_call_id.0.clone(),
        },
        RuntimeEvent::SearchBatchCompleted {
            searched_files,
            skipped_files,
            bytes_scanned,
            ms,
        } => ReviewEvent::SearchBatchCompleted {
            searched_files: *searched_files,
            skipped_files: *skipped_files,
            bytes_scanned: *bytes_scanned,
            ms: *ms,
        },
        RuntimeEvent::SessionFinished { session_id, status } => ReviewEvent::SessionFinished {
            session_id: session_id.0.clone(),
            status: status.clone(),
        },
        RuntimeEvent::SnapshotFinished {
            snapshot_id,
            sessions,
            completed_sessions,
        } => ReviewEvent::SnapshotFinished {
            snapshot_id: snapshot_id.clone(),
            sessions: *sessions,
            completed_sessions: *completed_sessions,
        },
        RuntimeEvent::JobFinished { status } => ReviewEvent::RunFinished {
            status: status.clone(),
        },
    }
}
