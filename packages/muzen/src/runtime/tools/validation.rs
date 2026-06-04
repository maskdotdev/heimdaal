use serde::Deserialize;
use serde_json::Value;

use crate::contracts::{ToolCounts, ToolName};
use crate::runtime::contracts::*;

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
    call: crate::runtime::contracts::ModelToolCall,
    capabilities: CapabilitySet,
    scope_key: ScopeKey,
    registry: &ToolRegistry,
) -> Result<ToolInvocation, (ToolCallId, ToolId, ToolErrorCode)> {
    let tool_id = call.name;
    let builtin_name = tool_id.as_builtin();
    let input_bytes = call.raw_arguments.len();
    if input_bytes > capabilities.tool_input.max_argument_bytes {
        return Err((call.call_id, tool_id, ToolErrorCode::TooLarge));
    }
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
        input_bytes,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::tools::ToolRegistry;

    #[test]
    fn validation_rejects_arguments_over_capability_input_limit() {
        let registry = ToolRegistry::review_defaults().expect("registry");
        let mut capabilities = CapabilitySet::review_read_only();
        capabilities.tool_input.max_argument_bytes = 1;
        let call = ModelToolCall {
            call_id: ToolCallId("call".to_string()),
            index: 0,
            name: ToolId::from(ToolName::ReadDiff),
            raw_arguments: "{}".to_string(),
        };

        let err = validate_invocation(
            SessionId("session".to_string()),
            TurnId(1),
            call,
            capabilities,
            FsScope::repo_root().scope_key(&SnapshotId("snapshot".to_string())),
            &registry,
        )
        .expect_err("oversized arguments should be denied");

        assert_eq!(err.2, ToolErrorCode::TooLarge);
    }
}
