use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::concurrent::contracts::{
    ArtifactKey, LimitInfo, RuntimeError, RuntimeResult, SessionId, SnapshotId, ToolCallId, ToolId,
    TurnId,
};
use crate::concurrent::repo::RepoSnapshot;
use crate::contracts::ToolName;

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
        registry.register_builtin(
            ToolName::ListChangedFiles,
            "List files in the review change set.",
            json!({}),
            true,
        )?;
        registry.register_builtin(
            ToolName::ReadDiff,
            "Read the review diff manifest.",
            json!({}),
            true,
        )?;
        registry.register_builtin(
            ToolName::ListFiles,
            "List text/code files in the materialized repo.",
            json!({}),
            true,
        )?;
        registry.register_builtin(
            ToolName::ReadFile,
            "Read a text file by repo-relative path.",
            json!({"path": {"type": "string"}}),
            true,
        )?;
        registry.register_builtin(
            ToolName::ReadBaseFile,
            "Read a base snapshot file by repo-relative path when a base snapshot is available.",
            json!({"path": {"type": "string"}}),
            true,
        )?;
        registry.register_builtin(
            ToolName::ReadHeadFile,
            "Read a head/review file by repo-relative path.",
            json!({"path": {"type": "string"}}),
            true,
        )?;
        registry.register_builtin(
            ToolName::SearchText,
            "Search repository text for literal terms separated by |.",
            json!({"query": {"type": "string"}}),
            true,
        )?;
        registry.register_builtin(
            ToolName::FindRelatedFiles,
            "Find files likely related to a repo-relative path.",
            json!({"path": {"type": "string"}}),
            true,
        )?;
        registry.register_builtin(
            ToolName::FindTestsForFile,
            "Find likely tests for a repo-relative path.",
            json!({"path": {"type": "string"}}),
            true,
        )?;
        registry.register_builtin(
            ToolName::ListImports,
            "List import-like lines from a repo-relative text file.",
            json!({"path": {"type": "string"}}),
            true,
        )?;
        registry.register_builtin(
            ToolName::RecordFinding,
            "Record one evidence-backed candidate finding.",
            json!({
                "title": {"type": "string"},
                "claim": {"type": "string"}
            }),
            false,
        )?;
        registry.register_builtin(
            ToolName::ChallengeFinding,
            "Challenge a recorded finding with a rationale.",
            json!({
                "finding_id": {"type": "string"},
                "rationale": {"type": "string"}
            }),
            false,
        )?;
        registry.register_builtin(
            ToolName::Finish,
            "Finish the review session.",
            json!({"reason": {"type": "string"}}),
            false,
        )?;
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

    fn register_builtin(
        &mut self,
        tool: ToolName,
        description: impl Into<String>,
        properties: Value,
        cacheable: bool,
    ) -> RuntimeResult<()> {
        self.register(ToolDefinition {
            id: ToolId::from(tool),
            description: description.into(),
            parameters: object_parameters(properties)?,
            builtin: Some(tool),
            cacheable,
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
    pub(crate) snapshot: Arc<RepoSnapshot>,
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

fn object_parameters(properties: Value) -> RuntimeResult<Value> {
    let required = properties
        .as_object()
        .ok_or_else(|| RuntimeError::InvalidInput("tool properties must be an object".to_string()))?
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    validate_parameters(json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    }))
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
