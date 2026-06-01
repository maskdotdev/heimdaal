use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::concurrent::contracts::*;
use crate::contracts::{ModelProfileRefV1, TokenUsage, ToolCallingMode, ToolName};
use crate::model::resolve_credential_ref;

#[async_trait]
pub(crate) trait ConcurrentModelClient: Send + Sync {
    async fn complete(
        &self,
        session_id: &SessionId,
        transcript: &[ConversationItem],
        turn_id: TurnId,
        cancel: CancellationToken,
    ) -> RuntimeResult<ModelTurn>;
}

#[derive(Debug)]
pub(crate) struct MockReviewModel {
    target_path: String,
    query: String,
}

impl MockReviewModel {
    pub(crate) fn new(target_path: String, query: String) -> Self {
        Self { target_path, query }
    }
}

#[async_trait]
impl ConcurrentModelClient for MockReviewModel {
    async fn complete(
        &self,
        session_id: &SessionId,
        transcript: &[ConversationItem],
        turn_id: TurnId,
        _cancel: CancellationToken,
    ) -> RuntimeResult<ModelTurn> {
        let tool_results = transcript
            .iter()
            .filter(|item| matches!(item, ConversationItem::ToolResult { .. }))
            .count();
        let usage = TokenUsage {
            input_tokens: transcript.len() as u64 * 100,
            output_tokens: 64,
            total_tokens: transcript.len() as u64 * 100 + 64,
        };
        if tool_results == 0 {
            return Ok(ModelTurn::ToolCalls {
                usage,
                calls: vec![
                    ModelToolCall {
                        call_id: ToolCallId(format!("{}-{}-read-diff", session_id.0, turn_id.0)),
                        index: 0,
                        name: ToolName::ReadDiff,
                        raw_arguments: "{}".to_string(),
                    },
                    ModelToolCall {
                        call_id: ToolCallId(format!("{}-{}-read-file", session_id.0, turn_id.0)),
                        index: 1,
                        name: ToolName::ReadFile,
                        raw_arguments: json!({ "path": self.target_path }).to_string(),
                    },
                    ModelToolCall {
                        call_id: ToolCallId(format!("{}-{}-search", session_id.0, turn_id.0)),
                        index: 2,
                        name: ToolName::SearchText,
                        raw_arguments: json!({ "query": self.query }).to_string(),
                    },
                ],
            });
        }
        Ok(ModelTurn::ToolCalls {
            usage,
            calls: vec![ModelToolCall {
                call_id: ToolCallId(format!("{}-{}-finding", session_id.0, turn_id.0)),
                index: 0,
                name: ToolName::RecordFinding,
                raw_arguments: json!({
                    "title": format!("{} reviewed with parallel evidence", session_id.0),
                    "claim": "The benchmark session gathered diff, file, and search evidence."
                })
                .to_string(),
            }],
        })
    }
}

#[derive(Debug)]
pub(crate) struct ModelLimiter {
    global: Semaphore,
}

impl ModelLimiter {
    pub(crate) fn new(global_concurrency: usize) -> Self {
        Self {
            global: Semaphore::new(global_concurrency.max(1)),
        }
    }

    pub(crate) async fn acquire(&self) -> RuntimeResult<tokio::sync::SemaphorePermit<'_>> {
        self.global
            .acquire()
            .await
            .map_err(|_| RuntimeError::Cancelled)
    }
}

#[derive(Debug)]
pub(crate) struct OpenAiChatCompletionsClient {
    http: reqwest::Client,
    profile: ModelProfileRefV1,
    api_key: String,
    base_url: String,
    limiter: Arc<ModelLimiter>,
}

impl OpenAiChatCompletionsClient {
    pub(crate) fn from_profile(
        profile: ModelProfileRefV1,
        base_url: String,
        limiter: Arc<ModelLimiter>,
    ) -> RuntimeResult<Self> {
        let api_key = resolve_credential_ref(&profile.credential_ref).map_err(|_| {
            RuntimeError::InvalidInput("model credential is unavailable".to_string())
        })?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(90))
            .build()
            .map_err(|_| RuntimeError::Invariant("failed to build async HTTP client"))?;
        Ok(Self {
            http,
            profile,
            api_key,
            base_url: base_url.trim_end_matches('/').to_string(),
            limiter,
        })
    }
}

#[async_trait]
impl ConcurrentModelClient for OpenAiChatCompletionsClient {
    async fn complete(
        &self,
        _session_id: &SessionId,
        transcript: &[ConversationItem],
        _turn_id: TurnId,
        cancel: CancellationToken,
    ) -> RuntimeResult<ModelTurn> {
        if cancel.is_cancelled() {
            return Err(RuntimeError::Cancelled);
        }
        let _permit = self.limiter.acquire().await?;
        let token_param =
            if self.profile.model.starts_with("gpt-5") || self.profile.model.starts_with('o') {
                "max_completion_tokens"
            } else {
                "max_tokens"
            };
        let mut body = json!({
            "model": self.profile.model,
            "messages": chat_messages(transcript)?,
            "tools": tool_schemas(),
            "tool_choice": match self.profile.tool_calling_mode {
                ToolCallingMode::Required => "required",
                ToolCallingMode::Auto => "auto",
            }
        });
        body[token_param] = json!(self.profile.max_output_tokens);
        if !(self.profile.model.starts_with("gpt-5") || self.profile.model.starts_with('o')) {
            body["temperature"] = json!(self.profile.temperature.unwrap_or(0.0));
        }
        let response = self
            .http
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|_| RuntimeError::Provider {
                status: None,
                retryable: true,
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(RuntimeError::Provider {
                status: Some(status.as_u16()),
                retryable: status.as_u16() == 429 || status.is_server_error(),
            });
        }
        let decoded: ChatCompletionResponse =
            response.json().await.map_err(|_| RuntimeError::Provider {
                status: None,
                retryable: false,
            })?;
        parse_chat_response(decoded)
    }
}

fn chat_messages(transcript: &[ConversationItem]) -> RuntimeResult<Vec<Value>> {
    let mut messages = Vec::new();
    for item in transcript {
        match item {
            ConversationItem::System { content } => {
                messages.push(json!({ "role": "system", "content": content }));
            }
            ConversationItem::User { content } => {
                messages.push(json!({ "role": "user", "content": content }));
            }
            ConversationItem::AssistantText { content } => {
                messages.push(json!({ "role": "assistant", "content": content }));
            }
            ConversationItem::AssistantToolCalls { calls } => {
                messages.push(json!({
                    "role": "assistant",
                    "tool_calls": calls.iter().map(|call| {
                        json!({
                            "id": call.call_id.0,
                            "type": "function",
                            "function": {
                                "name": call.name.as_str(),
                                "arguments": call.raw_arguments,
                            }
                        })
                    }).collect::<Vec<_>>()
                }));
            }
            ConversationItem::ToolResult {
                call_id, content, ..
            } => {
                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": call_id.0,
                    "content": serde_json::to_string(content).map_err(|_| RuntimeError::Invariant("tool result serialization failed"))?,
                }));
            }
        }
    }
    Ok(messages)
}

fn parse_chat_response(response: ChatCompletionResponse) -> RuntimeResult<ModelTurn> {
    let usage = response.usage.unwrap_or_default().into_token_usage();
    let message = response
        .choices
        .into_iter()
        .next()
        .ok_or(RuntimeError::Provider {
            status: None,
            retryable: false,
        })?
        .message;
    let mut seen = HashSet::new();
    let calls = message
        .tool_calls
        .unwrap_or_default()
        .into_iter()
        .enumerate()
        .filter(|(_, call)| call.call_type == "function")
        .map(|(index, call)| {
            if !seen.insert(call.id.clone()) {
                return Err(RuntimeError::Provider {
                    status: None,
                    retryable: false,
                });
            }
            let name = tool_name_from_str(&call.function.name)?;
            Ok(ModelToolCall {
                call_id: ToolCallId(call.id),
                index,
                name,
                raw_arguments: call.function.arguments,
            })
        })
        .collect::<RuntimeResult<Vec<_>>>()?;
    if !calls.is_empty() {
        Ok(ModelTurn::ToolCalls { calls, usage })
    } else {
        Ok(ModelTurn::Text {
            content: message.content.unwrap_or_default(),
            usage,
        })
    }
}

fn tool_name_from_str(value: &str) -> RuntimeResult<ToolName> {
    match value {
        "read_diff" => Ok(ToolName::ReadDiff),
        "list_files" => Ok(ToolName::ListFiles),
        "read_file" => Ok(ToolName::ReadFile),
        "read_head_file" => Ok(ToolName::ReadHeadFile),
        "search_text" => Ok(ToolName::SearchText),
        "record_finding" => Ok(ToolName::RecordFinding),
        "finish" => Ok(ToolName::Finish),
        _ => Err(RuntimeError::InvalidInput("unknown tool name".to_string())),
    }
}

fn tool_schemas() -> Vec<Value> {
    vec![
        tool_schema("read_diff", "Read the review diff manifest.", json!({})),
        tool_schema(
            "list_files",
            "List text/code files in the materialized repo.",
            json!({}),
        ),
        tool_schema(
            "read_file",
            "Read a text file by repo-relative path.",
            json!({"path": {"type": "string"}}),
        ),
        tool_schema(
            "read_head_file",
            "Read a head/review file by repo-relative path.",
            json!({"path": {"type": "string"}}),
        ),
        tool_schema(
            "search_text",
            "Search repository text for literal terms separated by |.",
            json!({"query": {"type": "string"}}),
        ),
        tool_schema(
            "record_finding",
            "Record one evidence-backed candidate finding.",
            json!({
                "title": {"type": "string"},
                "claim": {"type": "string"}
            }),
        ),
        tool_schema(
            "finish",
            "Finish the review session.",
            json!({"reason": {"type": "string"}}),
        ),
    ]
}

fn tool_schema(name: &str, description: &str, properties: Value) -> Value {
    let required = properties
        .as_object()
        .map(|object| object.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    json!({
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required,
                "additionalProperties": false
            }
        }
    })
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
    usage: Option<ChatUsage>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    content: Option<String>,
    tool_calls: Option<Vec<ChatToolCall>>,
}

#[derive(Debug, Deserialize)]
struct ChatToolCall {
    id: String,
    #[serde(rename = "type")]
    call_type: String,
    function: ChatToolFunction,
}

#[derive(Debug, Deserialize)]
struct ChatToolFunction {
    name: String,
    arguments: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct ChatUsage {
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    total_tokens: Option<u64>,
}

impl ChatUsage {
    fn into_token_usage(self) -> TokenUsage {
        TokenUsage {
            input_tokens: self.prompt_tokens.unwrap_or(0),
            output_tokens: self.completion_tokens.unwrap_or(0),
            total_tokens: self.total_tokens.unwrap_or(0),
        }
    }
}
