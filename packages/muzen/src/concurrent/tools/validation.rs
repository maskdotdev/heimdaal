use serde::Deserialize;
use serde_json::Value;

use crate::concurrent::contracts::*;
use crate::contracts::{ToolCounts, ToolName};

use super::registry::ToolRegistry;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadFileArgs {
    path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchTextArgs {
    query: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordFindingArgs {
    title: String,
    claim: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChallengeFindingArgs {
    finding_id: String,
    rationale: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FinishArgs {
    reason: Option<String>,
}

pub(crate) fn validate_invocation(
    session_id: SessionId,
    turn_id: TurnId,
    call: crate::concurrent::contracts::ModelToolCall,
    capabilities: CapabilitySet,
    scope_key: ScopeKey,
    registry: &ToolRegistry,
) -> Result<ToolInvocation, (ToolCallId, ToolId, ToolErrorCode)> {
    let tool_id = call.name;
    let builtin_name = tool_id.as_builtin();
    let Some(definition) = registry.definition(&tool_id) else {
        return Err((call.call_id, tool_id, ToolErrorCode::UnknownTool));
    };
    if definition.builtin != builtin_name {
        return Err((call.call_id, tool_id, ToolErrorCode::UnknownTool));
    }
    if !capabilities.allow_tool(&tool_id) {
        return Err((call.call_id, tool_id, ToolErrorCode::ToolNotAllowed));
    }
    let args = match builtin_name {
        Some(ToolName::ListChangedFiles | ToolName::ReadDiff | ToolName::ListFiles) => {
            ToolArgs::Empty
        }
        Some(
            ToolName::ReadFile
            | ToolName::ReadBaseFile
            | ToolName::ReadHeadFile
            | ToolName::FindRelatedFiles
            | ToolName::FindTestsForFile
            | ToolName::ListImports,
        ) => {
            let parsed: ReadFileArgs = serde_json::from_str(&call.raw_arguments).map_err(|_| {
                (
                    call.call_id.clone(),
                    tool_id.clone(),
                    ToolErrorCode::InvalidArgs,
                )
            })?;
            let path = RepoPath::parse(&parsed.path).map_err(|_| {
                (
                    call.call_id.clone(),
                    tool_id.clone(),
                    ToolErrorCode::PathDenied,
                )
            })?;
            ToolArgs::ReadFile { path }
        }
        Some(ToolName::SearchText) => {
            let parsed: SearchTextArgs =
                serde_json::from_str(&call.raw_arguments).map_err(|_| {
                    (
                        call.call_id.clone(),
                        tool_id.clone(),
                        ToolErrorCode::InvalidArgs,
                    )
                })?;
            ToolArgs::SearchText {
                query: parsed.query,
            }
        }
        Some(ToolName::RecordFinding) => {
            let parsed: RecordFindingArgs =
                serde_json::from_str(&call.raw_arguments).map_err(|_| {
                    (
                        call.call_id.clone(),
                        tool_id.clone(),
                        ToolErrorCode::InvalidArgs,
                    )
                })?;
            ToolArgs::RecordFinding {
                title: parsed.title,
                claim: parsed.claim,
            }
        }
        Some(ToolName::ChallengeFinding) => {
            let parsed: ChallengeFindingArgs =
                serde_json::from_str(&call.raw_arguments).map_err(|_| {
                    (
                        call.call_id.clone(),
                        tool_id.clone(),
                        ToolErrorCode::InvalidArgs,
                    )
                })?;
            ToolArgs::ChallengeFinding {
                finding_id: parsed.finding_id,
                rationale: parsed.rationale,
            }
        }
        Some(ToolName::Finish) => {
            let parsed: FinishArgs = serde_json::from_str(&call.raw_arguments).map_err(|_| {
                (
                    call.call_id.clone(),
                    tool_id.clone(),
                    ToolErrorCode::InvalidArgs,
                )
            })?;
            ToolArgs::Finish {
                reason: parsed.reason.unwrap_or_else(|| "finished".to_string()),
            }
        }
        None => {
            let parsed: Value = serde_json::from_str(&call.raw_arguments).map_err(|_| {
                (
                    call.call_id.clone(),
                    tool_id.clone(),
                    ToolErrorCode::InvalidArgs,
                )
            })?;
            ToolArgs::Raw(parsed)
        }
    };
    Ok(ToolInvocation {
        session_id,
        turn_id,
        call_id: call.call_id,
        tool_id,
        builtin_name,
        args,
        capabilities,
        scope_key,
    })
}

pub(crate) fn count_tool_result(counts: &mut ToolCounts, result: &ToolResultEnvelope) {
    if result.ok {
        if let Some(tool_name) = result.tool_name.as_builtin() {
            counts.increment(tool_name);
        }
    }
}
