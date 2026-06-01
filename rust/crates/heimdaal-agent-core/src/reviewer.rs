use std::path::PathBuf;

pub use crate::concurrent::contracts::{
    ArtifactId, ArtifactKey, ArtifactView, CacheInfo, CacheStatus, CapabilitySet,
    ConcurrentCounters, ConcurrentRunReport, EvidenceId, FsScope, LimitInfo, ModelToolCall,
    ModelTurn, RuntimeError, RuntimeEvent, RuntimeLimits, ScopeKey, SessionId, SessionScope,
    SnapshotId, ToolCallId, ToolEffects, ToolErrorCode, ToolErrorInfo, ToolGrant, ToolId,
    ToolMetricKey, ToolMetricsSnapshot, ToolResultEnvelope, TurnId,
};
pub use crate::concurrent::model::{
    ConcurrentModelClient as ModelClient, ConcurrentModelRouter as ModelRouter, ModelLimiter,
    StaticModelRouter,
};
pub use crate::concurrent::tool_registry::{
    CustomToolArtifact, CustomToolContext, CustomToolHandler, CustomToolOutput, ToolDefinition,
    ToolRegistry, ToolSchema,
};
pub use crate::contracts::{AgentBudget, Role, TokenUsage, ToolCounts};

#[derive(Debug, Clone)]
pub struct RunSpec {
    pub run_id: String,
    pub snapshots: Vec<SnapshotSpec>,
    pub sessions: Vec<SessionScope>,
    pub limits: RuntimeLimits,
}

#[derive(Debug, Clone)]
pub struct SnapshotSpec {
    pub snapshot_id: Option<SnapshotId>,
    pub repo_root: PathBuf,
    pub default_cwd: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct RunHandle {
    pub run_id: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotHandle {
    pub snapshot_id: SnapshotId,
}

pub trait EventSink: Send + Sync {
    fn emit(&self, event: RuntimeEvent);
}
