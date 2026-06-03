use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::concurrent::contracts::{
    ArtifactKey, LimitInfo, RuntimeError, RuntimeResult, SessionId, SnapshotId, ToolCallId, ToolId,
    TurnId,
};
use crate::contracts::ToolName;

use super::catalog::{review_builtin_specs, BuiltinToolSpec};

#[derive(Clone)]
pub struct ToolRegistry {
    definitions: HashMap<ToolId, ToolDefinition>,
}

impl fmt::Debug for ToolRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolRegistry")
            .field("definitions", &self.definitions.len())
            .finish()
    }
}

impl ToolRegistry {
    pub fn review_defaults() -> RuntimeResult<Self> {
        let mut registry = Self {
            definitions: HashMap::new(),
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
        self.register(ToolDefinition {
            id,
            description: description.into(),
            parameters: validate_parameters(parameters)?,
            builtin: None,
            cacheable,
            handler: Some(handler),
        })
    }

    pub fn definition(&self, id: &ToolId) -> Option<&ToolDefinition> {
        self.definitions.get(id)
    }

    pub fn schemas(&self) -> Vec<ToolSchema> {
        let mut schemas = self
            .definitions
            .values()
            .map(|definition| ToolSchema {
                id: definition.id.clone(),
                description: definition.description.clone(),
                parameters: definition.parameters.clone(),
            })
            .collect::<Vec<_>>();
        schemas.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        schemas
    }

    fn register_builtin(&mut self, spec: BuiltinToolSpec) -> RuntimeResult<()> {
        self.register(ToolDefinition {
            id: ToolId::from(spec.name),
            description: spec.description.to_string(),
            parameters: validate_parameters(spec.parameters())?,
            builtin: Some(spec.name),
            cacheable: spec.cacheable,
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
        self.definitions.insert(definition.id.clone(), definition);
        Ok(())
    }
}

#[derive(Clone)]
pub struct ToolDefinition {
    pub id: ToolId,
    pub description: String,
    pub parameters: Value,
    pub(crate) builtin: Option<ToolName>,
    pub cacheable: bool,
    pub(crate) handler: Option<Arc<dyn CustomToolHandler>>,
}

impl fmt::Debug for ToolDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolDefinition")
            .field("id", &self.id)
            .field("description", &self.description)
            .field("builtin", &self.builtin)
            .field("cacheable", &self.cacheable)
            .field("has_handler", &self.handler.is_some())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ToolSchema {
    pub id: ToolId,
    pub description: String,
    pub parameters: Value,
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

#[derive(Debug, Clone)]
pub struct CustomToolContext {
    pub session_id: SessionId,
    pub turn_id: TurnId,
    pub call_id: ToolCallId,
    pub tool_id: ToolId,
    pub snapshot_id: SnapshotId,
}

#[derive(Debug, Clone, Default)]
pub struct CustomToolOutput {
    pub data: Option<Value>,
    pub artifact: Option<CustomToolArtifact>,
    pub limits: LimitInfo,
}

#[derive(Debug, Clone)]
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
