use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::contracts::ToolName;
use crate::runtime::contracts::{
    ArtifactKey, LimitInfo, ProviderResourceId, RuntimeError, RuntimeResult, SessionId, SnapshotId,
    ToolCallId, ToolEffects, ToolId, ToolProviderId, TurnId,
};

use super::catalog::{review_builtin_specs, BuiltinToolSpec};

#[derive(Clone)]
pub struct ToolRegistry {
    definitions: HashMap<ToolId, ToolDefinition>,
    jsonrpc_transports: HashMap<ToolProviderId, Arc<dyn JsonRpcToolTransport>>,
}

impl fmt::Debug for ToolRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolRegistry")
            .field("definitions", &self.definitions.len())
            .field("jsonrpc_transports", &self.jsonrpc_transports.len())
            .finish()
    }
}

impl ToolRegistry {
    pub fn review_defaults() -> RuntimeResult<Self> {
        let mut registry = Self {
            definitions: HashMap::new(),
            jsonrpc_transports: HashMap::new(),
        };
        for spec in review_builtin_specs() {
            registry.register_builtin(spec)?;
        }
        Ok(registry)
    }

    pub fn register_custom(
        &mut self,
        id: ToolId,
        description: impl Into<String>,
        parameters: Value,
        cacheable: bool,
        handler: Arc<dyn CustomToolHandler>,
    ) -> RuntimeResult<()> {
        self.register_custom_with_effects(
            id,
            description,
            parameters,
            cacheable,
            ToolEffects::custom_read_only(),
            handler,
        )
    }

    pub fn register_custom_with_effects(
        &mut self,
        id: ToolId,
        description: impl Into<String>,
        parameters: Value,
        cacheable: bool,
        effects: ToolEffects,
        handler: Arc<dyn CustomToolHandler>,
    ) -> RuntimeResult<()> {
        self.register_custom_with_alias_and_effects(
            id.clone(),
            id,
            description,
            parameters,
            CustomToolOptions {
                cacheable,
                effects,
                provider_resources: Vec::new(),
            },
            handler,
        )
    }

    pub fn register_custom_with_alias_and_effects(
        &mut self,
        id: ToolId,
        model_alias: ToolId,
        description: impl Into<String>,
        parameters: Value,
        options: CustomToolOptions,
        handler: Arc<dyn CustomToolHandler>,
    ) -> RuntimeResult<()> {
        self.register(ToolDefinition {
            id,
            model_alias,
            description: description.into(),
            parameters: validate_parameters(parameters)?,
            builtin: None,
            cacheable: options.cacheable,
            effects: options.effects,
            provider_resources: options.provider_resources,
            provider_id: ToolProviderId::in_process(),
            handler: Some(handler),
        })
    }

    pub fn register_jsonrpc_tool(
        &mut self,
        provider_id: ToolProviderId,
        id: ToolId,
        description: impl Into<String>,
        parameters: Value,
        options: CustomToolOptions,
        transport: Arc<dyn JsonRpcToolTransport>,
    ) -> RuntimeResult<()> {
        self.register_jsonrpc_tool_with_alias(JsonRpcToolRegistration {
            provider_id,
            id: id.clone(),
            model_alias: id,
            description: description.into(),
            parameters,
            options,
            transport,
        })
    }

    pub fn register_jsonrpc_tool_with_alias(
        &mut self,
        registration: JsonRpcToolRegistration,
    ) -> RuntimeResult<()> {
        let JsonRpcToolRegistration {
            provider_id,
            id,
            model_alias,
            description,
            parameters,
            options,
            transport,
        } = registration;
        if provider_id == ToolProviderId::builtin_review()
            || provider_id == ToolProviderId::in_process()
            || provider_id == ToolProviderId::runtime()
        {
            return Err(RuntimeError::InvalidInput(
                "reserved tool provider id".to_string(),
            ));
        }
        self.jsonrpc_transports
            .entry(provider_id.clone())
            .or_insert_with(|| transport);
        self.register(ToolDefinition {
            id,
            model_alias,
            description,
            parameters: validate_parameters(parameters)?,
            builtin: None,
            cacheable: options.cacheable,
            effects: options.effects,
            provider_resources: options.provider_resources,
            provider_id,
            handler: None,
        })
    }

    pub fn definition(&self, id: &ToolId) -> Option<&ToolDefinition> {
        self.definitions.get(id)
    }

    pub fn alias_table(&self) -> RuntimeResult<ToolAliasTable> {
        ToolAliasTable::from_registry(self)
    }

    pub(crate) fn tool_id_for_model_alias(&self, alias: &ToolId) -> Option<ToolId> {
        self.definitions
            .values()
            .find(|definition| &definition.model_alias == alias)
            .map(|definition| definition.id.clone())
    }

    pub fn schemas(&self) -> Vec<ToolSchema> {
        let mut schemas = self
            .definitions
            .values()
            .map(|definition| ToolSchema {
                id: definition.id.clone(),
                model_alias: definition.model_alias.clone(),
                description: definition.description.clone(),
                parameters: definition.parameters.clone(),
            })
            .collect::<Vec<_>>();
        schemas.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        schemas
    }

    pub(crate) fn jsonrpc_transports(
        &self,
    ) -> Vec<(ToolProviderId, Arc<dyn JsonRpcToolTransport>)> {
        self.jsonrpc_transports
            .iter()
            .map(|(provider_id, transport)| (provider_id.clone(), Arc::clone(transport)))
            .collect()
    }

    fn register_builtin(&mut self, spec: BuiltinToolSpec) -> RuntimeResult<()> {
        self.register(ToolDefinition {
            id: ToolId::from(spec.name),
            model_alias: ToolId::from(spec.name),
            description: spec.description.to_string(),
            parameters: validate_parameters(spec.parameters())?,
            builtin: Some(spec.name),
            cacheable: spec.cacheable,
            effects: ToolEffects::review_read_only(),
            provider_resources: Vec::new(),
            provider_id: ToolProviderId::builtin_review(),
            handler: None,
        })
    }

    fn register(&mut self, definition: ToolDefinition) -> RuntimeResult<()> {
        if self.definitions.contains_key(&definition.id) {
            return Err(RuntimeError::InvalidInput(format!(
                "duplicate tool id {}",
                definition.id.as_str()
            )));
        }
        if self
            .definitions
            .values()
            .any(|existing| existing.model_alias == definition.model_alias)
        {
            return Err(RuntimeError::InvalidInput(format!(
                "duplicate tool alias {}",
                definition.model_alias.as_str()
            )));
        }
        self.definitions.insert(definition.id.clone(), definition);
        Ok(())
    }
}

#[derive(Clone)]
pub struct ToolDefinition {
    pub id: ToolId,
    pub model_alias: ToolId,
    pub description: String,
    pub parameters: Value,
    pub(crate) builtin: Option<ToolName>,
    pub cacheable: bool,
    pub effects: ToolEffects,
    pub provider_resources: Vec<ProviderResourceId>,
    pub provider_id: ToolProviderId,
    pub(crate) handler: Option<Arc<dyn CustomToolHandler>>,
}

impl fmt::Debug for ToolDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolDefinition")
            .field("id", &self.id)
            .field("model_alias", &self.model_alias)
            .field("description", &self.description)
            .field("builtin", &self.builtin)
            .field("cacheable", &self.cacheable)
            .field("effects", &self.effects)
            .field("provider_resources", &self.provider_resources)
            .field("provider_id", &self.provider_id)
            .field("has_handler", &self.handler.is_some())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ToolSchema {
    pub id: ToolId,
    pub model_alias: ToolId,
    pub description: String,
    pub parameters: Value,
}

#[derive(Debug, Clone)]
pub struct CustomToolOptions {
    pub cacheable: bool,
    pub effects: ToolEffects,
    pub provider_resources: Vec<ProviderResourceId>,
}

impl Default for CustomToolOptions {
    fn default() -> Self {
        Self {
            cacheable: false,
            effects: ToolEffects::custom_read_only(),
            provider_resources: Vec::new(),
        }
    }
}

#[derive(Clone)]
pub struct JsonRpcToolRegistration {
    pub provider_id: ToolProviderId,
    pub id: ToolId,
    pub model_alias: ToolId,
    pub description: String,
    pub parameters: Value,
    pub options: CustomToolOptions,
    pub transport: Arc<dyn JsonRpcToolTransport>,
}

#[derive(Debug, Clone)]
pub struct ToolAliasTable {
    by_tool: HashMap<ToolId, ToolId>,
    by_alias: HashMap<ToolId, ToolId>,
}

impl ToolAliasTable {
    pub fn from_registry(registry: &ToolRegistry) -> RuntimeResult<Self> {
        let mut by_tool = HashMap::new();
        let mut by_alias = HashMap::new();
        for definition in registry.definitions.values() {
            if by_alias
                .insert(definition.model_alias.clone(), definition.id.clone())
                .is_some()
            {
                return Err(RuntimeError::InvalidInput(format!(
                    "duplicate tool alias {}",
                    definition.model_alias.as_str()
                )));
            }
            by_tool.insert(definition.id.clone(), definition.model_alias.clone());
        }
        Ok(Self { by_tool, by_alias })
    }

    pub fn alias_for(&self, tool_id: &ToolId) -> Option<&ToolId> {
        self.by_tool.get(tool_id)
    }

    pub fn tool_for_alias(&self, alias: &ToolId) -> Option<&ToolId> {
        self.by_alias.get(alias)
    }
}

#[async_trait]
pub trait CustomToolHandler: Send + Sync {
    async fn execute(
        &self,
        context: CustomToolContext,
        args: Value,
        cancel: CancellationToken,
    ) -> RuntimeResult<CustomToolOutput>;
}

#[async_trait]
pub trait JsonRpcToolTransport: Send + Sync {
    async fn call(
        &self,
        request: JsonRpcToolRequest,
        cancel: CancellationToken,
    ) -> RuntimeResult<JsonRpcToolResponse>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonRpcToolRequest {
    pub provider_id: ToolProviderId,
    pub session_id: SessionId,
    pub turn_id: TurnId,
    pub call_id: ToolCallId,
    pub tool_id: ToolId,
    pub snapshot_id: SnapshotId,
    #[serde(default)]
    pub provider_resources: Vec<ProviderResourceId>,
    pub arguments: Value,
}

#[derive(Debug, Clone)]
pub struct HttpJsonRpcToolTransport {
    endpoint: String,
    client: reqwest::Client,
}

impl HttpJsonRpcToolTransport {
    pub fn new(endpoint: impl Into<String>) -> RuntimeResult<Self> {
        let endpoint = endpoint.into();
        reqwest::Url::parse(&endpoint)
            .map_err(|_| RuntimeError::InvalidInput("invalid JSON-RPC endpoint".to_string()))?;
        Ok(Self {
            endpoint,
            client: reqwest::Client::new(),
        })
    }
}

#[async_trait]
impl JsonRpcToolTransport for HttpJsonRpcToolTransport {
    async fn call(
        &self,
        request: JsonRpcToolRequest,
        cancel: CancellationToken,
    ) -> RuntimeResult<JsonRpcToolResponse> {
        if cancel.is_cancelled() {
            return Err(RuntimeError::Cancelled);
        }
        let body = JsonRpcRequestEnvelope {
            jsonrpc: "2.0",
            id: request.call_id.0.clone(),
            method: "tool.call",
            params: request,
        };
        let response = tokio::select! {
            _ = cancel.cancelled() => return Err(RuntimeError::Cancelled),
            response = self.client.post(&self.endpoint).json(&body).send() => response,
        }
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
        let envelope: JsonRpcResponseEnvelope =
            response.json().await.map_err(|_| RuntimeError::Provider {
                status: None,
                retryable: false,
            })?;
        if envelope.jsonrpc != "2.0" || envelope.id != body.id {
            return Err(RuntimeError::Provider {
                status: None,
                retryable: false,
            });
        }
        if envelope.error.is_some() {
            return Err(RuntimeError::Provider {
                status: None,
                retryable: false,
            });
        }
        envelope.result.ok_or(RuntimeError::Provider {
            status: None,
            retryable: false,
        })
    }
}

#[derive(Debug, Serialize)]
struct JsonRpcRequestEnvelope {
    jsonrpc: &'static str,
    id: String,
    method: &'static str,
    params: JsonRpcToolRequest,
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponseEnvelope {
    jsonrpc: String,
    id: String,
    result: Option<JsonRpcToolResponse>,
    error: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct CustomToolContext {
    pub session_id: SessionId,
    pub turn_id: TurnId,
    pub call_id: ToolCallId,
    pub tool_id: ToolId,
    pub snapshot_id: SnapshotId,
    pub provider_resources: Vec<ProviderResourceId>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonRpcToolResponse {
    pub data: Option<Value>,
    pub artifact: Option<CustomToolArtifact>,
    pub limits: LimitInfo,
}

pub type CustomToolOutput = JsonRpcToolResponse;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomToolArtifact {
    pub key: ArtifactKey,
    pub content: String,
}

fn validate_parameters(parameters: Value) -> RuntimeResult<Value> {
    let object = parameters.as_object().ok_or_else(|| {
        RuntimeError::InvalidInput("tool parameters must be an object".to_string())
    })?;
    if object.get("type").and_then(Value::as_str) != Some("object") {
        return Err(RuntimeError::InvalidInput(
            "tool parameters must use type=object".to_string(),
        ));
    }
    if !object.contains_key("properties") {
        return Err(RuntimeError::InvalidInput(
            "tool parameters must declare properties".to_string(),
        ));
    }
    Ok(parameters)
}
