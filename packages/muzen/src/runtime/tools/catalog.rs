use serde_json::{json, Value};

use crate::contracts::ToolName;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum ToolArgShape {
    Empty,
    Path,
    SearchQuery,
    RecordFinding,
    ChallengeFinding,
    Finish,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct BuiltinToolSpec {
    pub(crate) name: ToolName,
    pub(crate) description: &'static str,
    pub(crate) arg_shape: ToolArgShape,
    pub(crate) cacheable: bool,
}

impl BuiltinToolSpec {
    pub(crate) fn parameters(self) -> Value {
        match self.arg_shape {
            ToolArgShape::Empty => object_schema(json!({}), Vec::new()),
            ToolArgShape::Path => object_schema(
                json!({"path": {"type": "string"}}),
                vec!["path".to_string()],
            ),
            ToolArgShape::SearchQuery => object_schema(
                json!({"query": {"type": "string"}}),
                vec!["query".to_string()],
            ),
            ToolArgShape::RecordFinding => object_schema(
                json!({
                    "title": {"type": "string"},
                    "claim": {"type": "string"}
                }),
                vec!["title".to_string(), "claim".to_string()],
            ),
            ToolArgShape::ChallengeFinding => object_schema(
                json!({
                    "finding_id": {"type": "string"},
                    "rationale": {"type": "string"}
                }),
                vec!["finding_id".to_string(), "rationale".to_string()],
            ),
            ToolArgShape::Finish => {
                object_schema(json!({"reason": {"type": "string"}}), Vec::new())
            }
        }
    }
}

pub(crate) fn review_builtin_specs() -> impl Iterator<Item = BuiltinToolSpec> {
    ToolName::review_read_only_tools()
        .iter()
        .copied()
        .map(builtin_tool_spec)
}

fn builtin_tool_spec(name: ToolName) -> BuiltinToolSpec {
    match name {
        ToolName::ListChangedFiles => BuiltinToolSpec {
            name,
            description: "List files in the review change set.",
            arg_shape: ToolArgShape::Empty,
            cacheable: true,
        },
        ToolName::ReadDiff => BuiltinToolSpec {
            name,
            description: "Read the review diff manifest.",
            arg_shape: ToolArgShape::Empty,
            cacheable: true,
        },
        ToolName::ListFiles => BuiltinToolSpec {
            name,
            description: "List text/code files in the materialized repo.",
            arg_shape: ToolArgShape::Empty,
            cacheable: true,
        },
        ToolName::ReadFile => BuiltinToolSpec {
            name,
            description: "Read a text file by repo-relative path.",
            arg_shape: ToolArgShape::Path,
            cacheable: true,
        },
        ToolName::ReadBaseFile => BuiltinToolSpec {
            name,
            description:
                "Read a base snapshot file by repo-relative path when a base snapshot is available.",
            arg_shape: ToolArgShape::Path,
            cacheable: true,
        },
        ToolName::ReadHeadFile => BuiltinToolSpec {
            name,
            description: "Read a head/review file by repo-relative path.",
            arg_shape: ToolArgShape::Path,
            cacheable: true,
        },
        ToolName::SearchText => BuiltinToolSpec {
            name,
            description: "Search repository text for literal terms separated by |.",
            arg_shape: ToolArgShape::SearchQuery,
            cacheable: true,
        },
        ToolName::FindRelatedFiles => BuiltinToolSpec {
            name,
            description: "Find files likely related to a repo-relative path.",
            arg_shape: ToolArgShape::Path,
            cacheable: true,
        },
        ToolName::FindTestsForFile => BuiltinToolSpec {
            name,
            description: "Find likely tests for a repo-relative path.",
            arg_shape: ToolArgShape::Path,
            cacheable: true,
        },
        ToolName::ListImports => BuiltinToolSpec {
            name,
            description: "List import-like lines from a repo-relative text file.",
            arg_shape: ToolArgShape::Path,
            cacheable: true,
        },
        ToolName::RecordFinding => BuiltinToolSpec {
            name,
            description: "Record one evidence-backed candidate finding.",
            arg_shape: ToolArgShape::RecordFinding,
            cacheable: false,
        },
        ToolName::ChallengeFinding => BuiltinToolSpec {
            name,
            description: "Challenge a recorded finding with a rationale.",
            arg_shape: ToolArgShape::ChallengeFinding,
            cacheable: false,
        },
        ToolName::Finish => BuiltinToolSpec {
            name,
            description: "Finish the review session.",
            arg_shape: ToolArgShape::Finish,
            cacheable: false,
        },
    }
}

fn object_schema(properties: Value, required: Vec<String>) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    })
}
