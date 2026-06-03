use serde_json::Value;

use crate::concurrent::contracts::{RuntimeError, ToolErrorCode, ToolResultEnvelope};
use crate::contracts::ToolName;
use crate::util::redact_known_secrets;

#[derive(Debug, Default)]
pub(crate) struct SessionEvidence {
    saw_diff: bool,
    saw_file: bool,
    saw_search: bool,
    results: Vec<ToolResultEnvelope>,
}

impl SessionEvidence {
    pub(crate) fn ready(&self) -> bool {
        self.saw_diff && self.saw_file && self.saw_search
    }

    pub(crate) fn results(&self) -> &[ToolResultEnvelope] {
        &self.results
    }

    pub(crate) fn saw_diff(&self) -> bool {
        self.saw_diff
    }

    pub(crate) fn saw_file(&self) -> bool {
        self.saw_file
    }

    pub(crate) fn saw_search(&self) -> bool {
        self.saw_search
    }

    pub(crate) fn observe(&mut self, result: &ToolResultEnvelope) {
        if !result.ok {
            return;
        }
        match result.tool_name.as_builtin() {
            Some(ToolName::ReadDiff) => self.saw_diff = true,
            Some(ToolName::ReadFile | ToolName::ReadHeadFile) => self.saw_file = true,
            Some(ToolName::SearchText) => self.saw_search = true,
            _ => {}
        }
        if result.artifact_id.is_some()
            && !matches!(
                result.tool_name.as_builtin(),
                Some(ToolName::RecordFinding | ToolName::ChallengeFinding | ToolName::Finish)
            )
        {
            self.results.push(result.clone());
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct SessionTerminal {
    seen: bool,
    tool: Option<String>,
    summary: Option<String>,
    denied_tool_errors: usize,
}

impl SessionTerminal {
    pub(crate) fn observe_batch(&mut self, results: &[ToolResultEnvelope]) -> bool {
        let terminal = results.iter().any(is_successful_terminal);
        if let Some(result) = results.iter().find(|result| is_successful_terminal(result)) {
            self.tool = Some(result.tool_name.as_str().to_string());
            self.summary = terminal_result_summary(result);
        }
        self.seen |= terminal;
        terminal
    }

    pub(crate) fn observe_error(&mut self, result: &ToolResultEnvelope) {
        if !result.ok
            && matches!(
                result.error.as_ref().map(|error| error.code),
                Some(ToolErrorCode::ToolNotAllowed)
            )
        {
            self.denied_tool_errors += 1;
        }
    }

    pub(crate) fn too_many_denied_tools(&self) -> bool {
        self.denied_tool_errors >= 2
    }

    pub(crate) fn seen(&self) -> bool {
        self.seen
    }

    pub(crate) fn tool(&self) -> Option<String> {
        self.tool.clone()
    }

    pub(crate) fn summary(&self) -> Option<String> {
        self.summary.clone()
    }
}

pub(crate) fn tool_status(result: &ToolResultEnvelope) -> &'static str {
    if result.ok {
        "ok"
    } else {
        "error"
    }
}

pub(crate) fn session_state(
    completed: bool,
    terminal_seen: bool,
    cancelled: bool,
    failed: bool,
) -> &'static str {
    if cancelled {
        "cancelled"
    } else if completed {
        "done"
    } else if failed || terminal_seen {
        "failed"
    } else {
        "budget_exhausted"
    }
}

pub(crate) fn artifact_event_summary(result: &ToolResultEnvelope) -> String {
    let data = result.data.as_ref();
    let summary = match result.tool_name.as_builtin() {
        Some(ToolName::ReadDiff) => {
            let hash = data
                .and_then(|value| value.get("contentHash"))
                .and_then(Value::as_str)
                .unwrap_or("<unknown>");
            format!("diff artifact contentHash={hash}")
        }
        Some(ToolName::ListChangedFiles) => {
            let files = data
                .and_then(|value| value.get("changedFiles"))
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            format!("changed files: {files}")
        }
        Some(ToolName::ListFiles) => {
            let files = data
                .and_then(|value| value.get("files"))
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            format!("listed files: {files}")
        }
        Some(ToolName::ReadFile | ToolName::ReadBaseFile | ToolName::ReadHeadFile) => {
            let path = data
                .and_then(|value| value.get("path"))
                .and_then(Value::as_str)
                .unwrap_or("<unknown>");
            format!("read file artifact {path}")
        }
        Some(ToolName::SearchText) => {
            let query = data
                .and_then(|value| value.get("query"))
                .and_then(Value::as_str)
                .unwrap_or("<unknown>");
            let matches = data
                .and_then(|value| value.get("returnedMatches"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            format!("search_text query={query} matches={matches}")
        }
        Some(ToolName::FindRelatedFiles | ToolName::FindTestsForFile) => {
            let path = data
                .and_then(|value| value.get("path"))
                .and_then(Value::as_str)
                .unwrap_or("<unknown>");
            let files = data
                .and_then(|value| value.get("files"))
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            format!("file relation artifact {path} files={files}")
        }
        Some(ToolName::ListImports) => {
            let path = data
                .and_then(|value| value.get("path"))
                .and_then(Value::as_str)
                .unwrap_or("<unknown>");
            let imports = data
                .and_then(|value| value.get("imports"))
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            format!("imports artifact {path} imports={imports}")
        }
        Some(ToolName::ChallengeFinding) => {
            let finding_id = data
                .and_then(|value| value.get("findingId"))
                .and_then(Value::as_str)
                .unwrap_or("<unknown>");
            format!("challenge finding artifact {finding_id}")
        }
        _ => format!("{} artifact", result.tool_name.as_str()),
    };
    truncate_summary(&redact_known_secrets(&summary, &[]), 240)
}

pub(crate) fn should_retry_model_error(error: &RuntimeError) -> bool {
    match error {
        RuntimeError::Provider { retryable, .. } => *retryable,
        RuntimeError::Timeout => true,
        RuntimeError::Cancelled => false,
        _ => false,
    }
}

fn is_successful_terminal(result: &ToolResultEnvelope) -> bool {
    matches!(
        result.tool_name.as_builtin(),
        Some(ToolName::RecordFinding | ToolName::Finish)
    ) && result.ok
}

fn terminal_result_summary(result: &ToolResultEnvelope) -> Option<String> {
    let data = result.data.as_ref()?;
    let raw = match result.tool_name.as_builtin() {
        Some(ToolName::RecordFinding) => data
            .get("title")
            .and_then(serde_json::Value::as_str)
            .or_else(|| data.get("claim").and_then(serde_json::Value::as_str)),
        Some(ToolName::Finish) => data.get("reason").and_then(serde_json::Value::as_str),
        _ => None,
    }?;
    Some(truncate_summary(&redact_known_secrets(raw, &[]), 240))
}

fn truncate_summary(value: &str, max_chars: usize) -> String {
    let mut output = value.chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        output.push_str(" [truncated]");
    }
    output
}
