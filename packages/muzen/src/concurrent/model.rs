use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::concurrent::contracts::*;
use crate::concurrent::tool_registry::ToolRegistry;
use crate::contracts::{ModelProfileRefV1, ProviderKind, TokenUsage, ToolCallingMode, ToolName};
use crate::util::resolve_credential_ref;

#[async_trait]
pub trait ConcurrentModelClient: Send + Sync {
    async fn complete(
        &self,
        scope: &SessionScope,
        transcript: &[ConversationItem],
        turn_id: TurnId,
        cancel: CancellationToken,
    ) -> RuntimeResult<ModelTurn>;
}

#[async_trait]
pub trait ConcurrentModelRouter: Send + Sync {
    async fn client_for(
        &self,
        scope: &SessionScope,
    ) -> RuntimeResult<Arc<dyn ConcurrentModelClient>>;
}

pub struct StaticModelRouter {
    client: Arc<dyn ConcurrentModelClient>,
}

impl StaticModelRouter {
    pub fn new(client: Arc<dyn ConcurrentModelClient>) -> Self {
        Self { client }
    }
}

impl std::fmt::Debug for StaticModelRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticModelRouter").finish_non_exhaustive()
    }
}

#[async_trait]
impl ConcurrentModelRouter for StaticModelRouter {
    async fn client_for(
        &self,
        _scope: &SessionScope,
    ) -> RuntimeResult<Arc<dyn ConcurrentModelClient>> {
        Ok(Arc::clone(&self.client))
    }
}

pub struct ProfileModelRouter {
    clients: HashMap<String, Arc<dyn ConcurrentModelClient>>,
    default_profile_id: String,
}

impl ProfileModelRouter {
    pub(crate) fn from_profiles(
        profiles: &[ModelProfileRefV1],
        default_profile_id: String,
        base_url: String,
        limiter: Arc<ModelLimiter>,
        tool_registry: Arc<ToolRegistry>,
    ) -> RuntimeResult<Self> {
        if profiles.is_empty() {
            return Err(RuntimeError::InvalidInput(
                "at least one model profile is required".to_string(),
            ));
        }
        let mut clients = HashMap::new();
        for profile in profiles {
            match profile.provider_kind {
                ProviderKind::OpenaiCompatible => {}
            }
            let client = OpenAiChatCompletionsClient::from_profile(
                profile.clone(),
                base_url.clone(),
                Arc::clone(&limiter),
                Arc::clone(&tool_registry),
            )?;
            clients.insert(
                profile.id.clone(),
                Arc::new(client) as Arc<dyn ConcurrentModelClient>,
            );
        }
        if !clients.contains_key(&default_profile_id) {
            return Err(RuntimeError::InvalidInput(format!(
                "missing default model profile {default_profile_id}"
            )));
        }
        Ok(Self {
            clients,
            default_profile_id,
        })
    }
}

impl std::fmt::Debug for ProfileModelRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProfileModelRouter")
            .field("clients", &self.clients.len())
            .field("default_profile_id", &self.default_profile_id)
            .finish()
    }
}

#[async_trait]
impl ConcurrentModelRouter for ProfileModelRouter {
    async fn client_for(
        &self,
        scope: &SessionScope,
    ) -> RuntimeResult<Arc<dyn ConcurrentModelClient>> {
        let profile_id = scope
            .model_profile_id
            .as_ref()
            .unwrap_or(&self.default_profile_id);
        self.clients
            .get(profile_id)
            .or_else(|| self.clients.get(&self.default_profile_id))
            .cloned()
            .ok_or_else(|| {
                RuntimeError::InvalidInput(format!("missing model profile {profile_id}"))
            })
    }
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
        scope: &SessionScope,
        transcript: &[ConversationItem],
        turn_id: TurnId,
        _cancel: CancellationToken,
    ) -> RuntimeResult<ModelTurn> {
        let session_id = &scope.id;
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
                        name: ToolId::from(ToolName::ReadDiff),
                        raw_arguments: "{}".to_string(),
                    },
                    ModelToolCall {
                        call_id: ToolCallId(format!("{}-{}-read-file", session_id.0, turn_id.0)),
                        index: 1,
                        name: ToolId::from(ToolName::ReadFile),
                        raw_arguments: json!({ "path": self.target_path }).to_string(),
                    },
                    ModelToolCall {
                        call_id: ToolCallId(format!("{}-{}-search", session_id.0, turn_id.0)),
                        index: 2,
                        name: ToolId::from(ToolName::SearchText),
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
                name: ToolId::from(ToolName::RecordFinding),
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
pub struct ModelLimiter {
    global: Semaphore,
}

impl ModelLimiter {
    pub fn new(global_concurrency: usize) -> Self {
        Self {
            global: Semaphore::new(global_concurrency.max(1)),
        }
    }

    pub async fn acquire(&self) -> RuntimeResult<tokio::sync::SemaphorePermit<'_>> {
        self.global
            .acquire()
            .await
            .map_err(|_| RuntimeError::Cancelled)
    }
}

#[derive(Debug)]
pub struct OpenAiChatCompletionsClient {
    http: reqwest::Client,
    profile: ModelProfileRefV1,
    api_key: String,
    base_url: String,
    limiter: Arc<ModelLimiter>,
    tool_registry: Arc<ToolRegistry>,
}

impl OpenAiChatCompletionsClient {
    pub(crate) fn from_profile(
        profile: ModelProfileRefV1,
        base_url: String,
        limiter: Arc<ModelLimiter>,
        tool_registry: Arc<ToolRegistry>,
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
            tool_registry,
        })
    }
}

#[async_trait]
impl ConcurrentModelClient for OpenAiChatCompletionsClient {
    async fn complete(
        &self,
        scope: &SessionScope,
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
            "tools": tool_schemas_for_transcript(
                &self.tool_registry,
                transcript,
                &scope.capabilities
            ),
            "parallel_tool_calls": false,
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
        parse_chat_response(decoded, &self.tool_registry)
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
                let compact = compact_tool_result(content);
                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": call_id.0,
                    "content": serde_json::to_string(&compact).map_err(|_| RuntimeError::Invariant("tool result serialization failed"))?,
                }));
            }
        }
    }
    Ok(messages)
}

fn compact_tool_result(result: &ToolResultEnvelope) -> Value {
    let data = result
        .data
        .as_ref()
        .map(|data| compact_tool_data(result.tool_name.as_builtin(), data));
    json!({
        "ok": result.ok,
        "toolName": result.tool_name.as_str(),
        "artifactId": result.artifact_id.as_ref().map(|id| id.0.as_str()),
        "cacheStatus": result.cache.status,
        "limits": {
            "truncated": result.limits.truncated,
            "outputBytes": result.limits.output_bytes,
            "searchedFiles": result.limits.searched_files,
            "skippedFiles": result.limits.skipped_files,
            "bytesScanned": result.limits.bytes_scanned,
        },
        "data": data,
        "error": result.error,
    })
}

fn compact_tool_data(tool: Option<ToolName>, data: &Value) -> Value {
    match tool {
        Some(ToolName::ReadDiff) => json!({
            "contentHash": data.get("contentHash").cloned(),
            "contentSnippet": data.get("content").and_then(Value::as_str).map(|value| truncate_chars(value, 1200)),
        }),
        Some(ToolName::ReadFile | ToolName::ReadHeadFile | ToolName::ReadBaseFile) => json!({
            "path": data.get("path").cloned(),
            "available": data.get("available").cloned(),
            "evidenceId": data.get("evidenceId").cloned(),
            "contentSnippet": data.get("content").and_then(Value::as_str).map(|value| truncate_chars(value, 1200)),
            "message": data.get("message").and_then(Value::as_str).map(|value| truncate_chars(value, 400)),
        }),
        Some(ToolName::SearchText) => json!({
            "query": data.get("query").cloned(),
            "returnedMatches": data.get("returnedMatches").cloned(),
            "truncated": data.get("truncated").cloned(),
            "matches": compact_string_array(data.get("matches"), 30, 300),
        }),
        Some(
            ToolName::ListChangedFiles
            | ToolName::ListFiles
            | ToolName::FindRelatedFiles
            | ToolName::FindTestsForFile,
        ) => json!({
            "changedFiles": compact_string_array(data.get("changedFiles"), 80, 240),
            "files": compact_string_array(data.get("files"), 80, 240),
            "path": data.get("path").cloned(),
        }),
        Some(ToolName::ListImports) => json!({
            "path": data.get("path").cloned(),
            "imports": compact_string_array(data.get("imports"), 80, 300),
        }),
        _ => data.clone(),
    }
}

fn compact_string_array(value: Option<&Value>, max_items: usize, max_chars: usize) -> Value {
    let Some(items) = value.and_then(Value::as_array) else {
        return Value::Null;
    };
    Value::Array(
        items
            .iter()
            .take(max_items)
            .filter_map(Value::as_str)
            .map(|item| Value::String(truncate_chars(item, max_chars)))
            .collect(),
    )
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut output = value.chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        output.push_str("\n[truncated]");
    }
    output
}

fn parse_chat_response(
    response: ChatCompletionResponse,
    tool_registry: &ToolRegistry,
) -> RuntimeResult<ModelTurn> {
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
            let name = ToolId::parse(&call.function.name)?;
            if tool_registry.definition(&name).is_none() {
                return Err(RuntimeError::InvalidInput("unknown tool name".to_string()));
            }
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

fn tool_schemas_for_transcript(
    registry: &ToolRegistry,
    transcript: &[ConversationItem],
    capabilities: &CapabilitySet,
) -> Vec<Value> {
    let has_read_diff = transcript_has_successful_tool(transcript, ToolName::ReadDiff);
    let has_read_file = transcript_has_successful_tool(transcript, ToolName::ReadFile)
        || transcript_has_successful_tool(transcript, ToolName::ReadHeadFile);
    let has_search = transcript_has_successful_tool(transcript, ToolName::SearchText);

    if !has_read_diff {
        if transcript_has_successful_tool(transcript, ToolName::ListChangedFiles) {
            return schemas_for_tools(registry, &[ToolName::ReadDiff]);
        }
        return schemas_for_tools(registry, &[ToolName::ListChangedFiles, ToolName::ReadDiff]);
    }

    if !has_read_file {
        return schemas_for_tools(registry, &[ToolName::ReadFile, ToolName::ReadHeadFile]);
    }

    if !has_search {
        return schemas_for_tools(registry, &[ToolName::SearchText]);
    }

    schemas_for_tools(registry, &allowed_terminal_tools(capabilities))
}

fn transcript_has_successful_tool(transcript: &[ConversationItem], expected: ToolName) -> bool {
    transcript.iter().any(|item| {
        matches!(
            item,
            ConversationItem::ToolResult { content, .. }
                if content.ok && content.tool_name.as_builtin() == Some(expected)
        )
    })
}

fn schemas_for_tools(registry: &ToolRegistry, tools: &[ToolName]) -> Vec<Value> {
    tools
        .iter()
        .filter_map(|tool| registry.definition(&ToolId::from(*tool)))
        .map(|definition| {
            json!({
                "type": "function",
                "function": {
                    "name": definition.id.as_str(),
                    "description": definition.description,
                    "parameters": definition.parameters
                }
            })
        })
        .collect()
}

fn allowed_terminal_tools(capabilities: &CapabilitySet) -> Vec<ToolName> {
    let mut tools = Vec::new();
    if capabilities.allow_tool(&ToolId::from(ToolName::RecordFinding)) {
        tools.push(ToolName::RecordFinding);
    }
    if capabilities.allow_tool(&ToolId::from(ToolName::Finish)) {
        tools.push(ToolName::Finish);
    }
    tools
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn post_diff_file_evidence_schema_excludes_repo_wide_listing() {
        let registry = ToolRegistry::review_defaults().expect("registry");
        let transcript = vec![successful_tool_result(ToolName::ReadDiff)];
        let capabilities = CapabilitySet::review_read_only();

        let names = schema_names(tool_schemas_for_transcript(
            &registry,
            &transcript,
            &capabilities,
        ));

        assert_eq!(names, vec!["read_file", "read_head_file"]);
        assert!(!names.contains(&"list_files"));
    }

    #[test]
    fn terminal_schema_excludes_finish_when_capability_denies_finish() {
        let registry = ToolRegistry::review_defaults().expect("registry");
        let transcript = vec![
            successful_tool_result(ToolName::ReadDiff),
            successful_tool_result(ToolName::ReadFile),
            successful_tool_result(ToolName::SearchText),
        ];
        let mut capabilities = CapabilitySet::review_read_only();
        capabilities
            .tool_grants
            .remove(&ToolId::from(ToolName::Finish));

        let names = schema_names(tool_schemas_for_transcript(
            &registry,
            &transcript,
            &capabilities,
        ));

        assert_eq!(names, vec!["record_finding"]);
    }

    fn successful_tool_result(tool: ToolName) -> ConversationItem {
        let tool_id = ToolId::from(tool);
        ConversationItem::ToolResult {
            call_id: ToolCallId(format!("call-{}", tool.as_str())),
            name: tool_id.clone(),
            content: Box::new(ToolResultEnvelope {
                ok: true,
                tool_call_id: ToolCallId(format!("call-{}", tool.as_str())),
                tool_name: tool_id,
                snapshot_id: SnapshotId("snapshot".to_string()),
                artifact_id: None,
                cache: CacheInfo {
                    status: CacheStatus::NotCacheable,
                    key_hash: None,
                },
                limits: LimitInfo::default(),
                data: None,
                error: None,
            }),
        }
    }

    fn schema_names(schemas: Vec<Value>) -> Vec<&'static str> {
        schemas
            .into_iter()
            .map(|schema| {
                let name = schema["function"]["name"].as_str().expect("schema name");
                match name {
                    "read_file" => "read_file",
                    "read_head_file" => "read_head_file",
                    "list_files" => "list_files",
                    "record_finding" => "record_finding",
                    "finish" => "finish",
                    other => panic!("unexpected schema {other}"),
                }
            })
            .collect()
    }
}
