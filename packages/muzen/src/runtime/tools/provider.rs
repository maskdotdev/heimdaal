use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::runtime::contracts::{
    CacheStatus, RuntimeError, RuntimeResult, ToolArgs, ToolInvocation, ToolProviderId,
    ToolResultEnvelope,
};

use super::engine::ToolEngine;
use super::registry::{JsonRpcToolRequest, JsonRpcToolTransport, ToolDefinition};

#[async_trait]
pub(crate) trait ToolProvider: Send + Sync {
    fn id(&self) -> ToolProviderId;
    fn accepts(&self, definition: &ToolDefinition) -> bool;

    async fn execute(
        &self,
        engine: &ToolEngine,
        invocation: ToolInvocation,
        cancel: CancellationToken,
        cache_status: CacheStatus,
    ) -> RuntimeResult<ToolResultEnvelope>;
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BuiltinReviewToolProvider;

#[async_trait]
impl ToolProvider for BuiltinReviewToolProvider {
    fn id(&self) -> ToolProviderId {
        ToolProviderId::builtin_review()
    }

    fn accepts(&self, definition: &ToolDefinition) -> bool {
        definition.provider_id == self.id() && definition.builtin.is_some()
    }

    async fn execute(
        &self,
        engine: &ToolEngine,
        invocation: ToolInvocation,
        cancel: CancellationToken,
        cache_status: CacheStatus,
    ) -> RuntimeResult<ToolResultEnvelope> {
        Ok(engine
            .execute_builtin_tool(invocation, cancel, cache_status)
            .await)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct InProcessToolProvider;

#[async_trait]
impl ToolProvider for InProcessToolProvider {
    fn id(&self) -> ToolProviderId {
        ToolProviderId::in_process()
    }

    fn accepts(&self, definition: &ToolDefinition) -> bool {
        definition.provider_id == self.id() && definition.builtin.is_none()
    }

    async fn execute(
        &self,
        engine: &ToolEngine,
        invocation: ToolInvocation,
        cancel: CancellationToken,
        cache_status: CacheStatus,
    ) -> RuntimeResult<ToolResultEnvelope> {
        Ok(engine
            .execute_in_process_tool(invocation, cancel, cache_status)
            .await)
    }
}

pub(crate) struct JsonRpcToolProvider {
    provider_id: ToolProviderId,
    transport: Arc<dyn JsonRpcToolTransport>,
}

impl JsonRpcToolProvider {
    pub(crate) fn new(
        provider_id: ToolProviderId,
        transport: Arc<dyn JsonRpcToolTransport>,
    ) -> Self {
        Self {
            provider_id,
            transport,
        }
    }
}

impl std::fmt::Debug for JsonRpcToolProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonRpcToolProvider")
            .field("provider_id", &self.provider_id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ToolProvider for JsonRpcToolProvider {
    fn id(&self) -> ToolProviderId {
        self.provider_id.clone()
    }

    fn accepts(&self, definition: &ToolDefinition) -> bool {
        definition.provider_id == self.provider_id
            && definition.builtin.is_none()
            && definition.handler.is_none()
    }

    async fn execute(
        &self,
        engine: &ToolEngine,
        invocation: ToolInvocation,
        cancel: CancellationToken,
        cache_status: CacheStatus,
    ) -> RuntimeResult<ToolResultEnvelope> {
        let ToolArgs::Raw(arguments) = invocation.args else {
            return Err(RuntimeError::InvalidInput(
                "JSON-RPC tool requires raw JSON arguments".to_string(),
            ));
        };
        let provider_id = self.provider_id.clone();
        let call_id = invocation.call_id;
        let tool_id = invocation.tool_id;
        let provider_resources = engine
            .registry
            .definition(&tool_id)
            .map(|definition| definition.provider_resources.clone())
            .unwrap_or_default();
        let output = self
            .transport
            .call(
                JsonRpcToolRequest {
                    provider_id: provider_id.clone(),
                    session_id: invocation.session_id,
                    turn_id: invocation.turn_id,
                    call_id: call_id.clone(),
                    tool_id: tool_id.clone(),
                    snapshot_id: engine.snapshot.snapshot_id.clone(),
                    provider_resources,
                    arguments,
                },
                cancel,
            )
            .await?;
        Ok(engine.provider_output_result(
            call_id,
            tool_id,
            provider_id,
            cache_status,
            &invocation.capabilities,
            output,
        ))
    }
}
