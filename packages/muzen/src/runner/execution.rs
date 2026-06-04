use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

use crate::contracts::{AgentBudget, Role};
use crate::reviewer::{
    capabilities, ids::ToolId, paths, runtime_events::EventSink as RuntimeEventSink, ChangeSpec,
    ChangedFileSpec, ReviewEventRecord, ReviewEventSink, ReviewRunLimits, ReviewRunSummary,
    ReviewSessionSpec, ReviewToolRegistry, Run, RunSpec, SnapshotPathPolicy, SnapshotSpec,
};

use super::adapters::{
    CallbackReviewModel, CallbackReviewTool, DeterministicRunnerModel, StreamingRunnerEventSink,
};
use super::stored::RunnerStoredRun;
use super::transport::RunnerCallbackTransport;
use super::types::{
    RunSessionParams, RunStartParams, RunnerFinding, RunnerRunResult, RunnerRunSummary,
    RunnerSnapshotSummary,
};
use super::RUNNER_PROTOCOL_VERSION;

pub(crate) struct ExecutedRun {
    pub(crate) result: RunnerRunResult,
    pub(crate) events: Vec<ReviewEventRecord>,
    pub(crate) stored: RunnerStoredRun,
}

pub(crate) fn execute_run_start(
    params: RunStartParams,
    transport: Option<Arc<dyn RunnerCallbackTransport>>,
) -> Result<ExecutedRun> {
    if let Some(protocol_version) = &params.protocol_version {
        if protocol_version != RUNNER_PROTOCOL_VERSION {
            anyhow::bail!("unsupported protocolVersion {protocol_version}");
        }
    }
    let run_id = params.run_id.unwrap_or_else(|| "muzen-run".to_string());
    let repo_root = params.repo;
    let target_path = select_target_path(&repo_root, &params.changed_files)?;
    let changed_files = changed_file_specs(&repo_root, &params.changed_files, &target_path);
    let change = ChangeSpec::local("sdk-run", "head", changed_files);
    let max_file_bytes = params
        .limits
        .as_ref()
        .and_then(|limits| limits.max_file_bytes)
        .unwrap_or(200 * 1024);
    let max_search_matches = params
        .limits
        .as_ref()
        .and_then(|limits| limits.max_search_matches)
        .unwrap_or(120);
    let max_active_sessions = params
        .limits
        .as_ref()
        .and_then(|limits| limits.max_active_sessions)
        .unwrap_or_else(|| params.sessions.len().max(1));
    let snapshot = SnapshotSpec::new(&repo_root, change).with_path_policy(
        SnapshotPathPolicy::standard(max_file_bytes, max_search_matches),
    );
    let sessions = if params.sessions.is_empty() {
        vec![RunSessionParams {
            id: "generalist".to_string(),
            role: Role::Generalist,
            objective: "Review the repository change.".to_string(),
            cwd: None,
            model_profile_id: None,
            budget: None,
        }]
    } else {
        params.sessions
    };
    let callback_tool_ids = params
        .tools
        .iter()
        .map(|tool| tool.id.clone())
        .collect::<Vec<_>>();
    let session_specs = sessions
        .into_iter()
        .map(|session| run_session_spec(session, &callback_tool_ids))
        .collect::<Result<Vec<_>>>()?;
    let limits = ReviewRunLimits::standard(max_active_sessions, max_file_bytes, max_search_matches);
    let spec = RunSpec::single_snapshot(run_id.clone(), snapshot, session_specs, limits);
    let event_sink = Arc::new(RecordingReviewEventSink::default());
    let streaming_sink = transport.as_ref().map(|transport| {
        Arc::new(StreamingRunnerEventSink::new(transport.clone())) as Arc<dyn RuntimeEventSink>
    });
    let mut builder = Run::builder(spec);
    let use_callback_model = params.model.as_ref().is_some_and(|model| model.callback);
    if use_callback_model {
        let transport = transport
            .clone()
            .ok_or_else(|| anyhow::anyhow!("callback model requires interactive stdio"))?;
        builder = builder.review_model(Arc::new(CallbackReviewModel::new(
            run_id.clone(),
            transport,
        )));
    } else {
        builder = builder.review_model(Arc::new(DeterministicRunnerModel::new(
            target_path,
            "TODO|fn|class|export|pub".to_string(),
        )));
    }
    if !params.tools.is_empty() {
        let transport = transport
            .clone()
            .ok_or_else(|| anyhow::anyhow!("callback tools require interactive stdio"))?;
        let mut registry = ReviewToolRegistry::review_defaults()
            .map_err(|error| anyhow::anyhow!("failed to create review tool registry: {error}"))?;
        for tool in params.tools {
            registry
                .register_read_only_tool(
                    &tool.id,
                    tool.description,
                    tool.parameters,
                    tool.cacheable,
                    Arc::new(CallbackReviewTool::new(run_id.clone(), transport.clone())),
                )
                .map_err(|error| {
                    anyhow::anyhow!("failed to register SDK tool {}: {error}", tool.id)
                })?;
        }
        builder = builder.review_tool_registry(registry);
    }
    let run = if let Some(streaming_sink) = streaming_sink {
        builder.event_sink(streaming_sink).build()
    } else {
        builder.review_event_sink(event_sink.clone()).build()
    }
    .map_err(|error| anyhow::anyhow!("{error}"))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build runner tokio runtime")?;
    let report = runtime.block_on(run.execute_with_cancel(CancellationToken::new()));
    let result = runner_result_from_report(&report);
    let stored = RunnerStoredRun::from_report(&report, result.clone());
    Ok(ExecutedRun {
        result,
        events: event_sink.records(),
        stored,
    })
}

fn run_session_spec(
    params: RunSessionParams,
    callback_tool_ids: &[String],
) -> Result<ReviewSessionSpec> {
    let budget = params.budget.map_or(
        AgentBudget {
            max_turns: 7,
            max_tool_calls: 14,
            max_prompt_tokens: 64_000,
            max_output_tokens: 8_000,
        },
        |budget| AgentBudget {
            max_turns: budget.max_turns,
            max_tool_calls: budget.max_tool_calls,
            max_prompt_tokens: budget.max_prompt_tokens,
            max_output_tokens: budget.max_output_tokens,
        },
    );
    let mut spec =
        ReviewSessionSpec::review_read_only(params.id, params.role, params.objective, budget);
    if let Some(model_profile_id) = params.model_profile_id {
        spec = spec.with_model_profile_id(model_profile_id);
    }
    if let Some(cwd) = params.cwd {
        let repo_path = paths::RepoPath::parse(&cwd).map_err(|error| anyhow::anyhow!("{error}"))?;
        let capabilities = capabilities::CapabilitySet::review_read_only()
            .with_fs_scope(capabilities::FsScope::subtree(repo_path));
        spec = spec.with_capabilities(capabilities);
    }
    for tool_id in callback_tool_ids {
        let tool_id = ToolId::parse(tool_id).map_err(|error| anyhow::anyhow!("{error}"))?;
        spec = spec.grant_custom_read_only_tool(tool_id);
    }
    Ok(spec)
}

fn runner_result_from_report(report: &crate::reviewer::RunReport) -> RunnerRunResult {
    let summary = runner_summary_from_review(&report.summary);
    let snapshots = report
        .snapshot_manifests()
        .into_iter()
        .map(|manifest| RunnerSnapshotSummary {
            snapshot_id: manifest.snapshot_id.0,
            files: manifest.files.len(),
            changed_files: manifest.changed_files.len(),
            captured_files: manifest
                .files
                .iter()
                .filter(|file| {
                    matches!(
                        file.capture_status,
                        crate::reviewer::storage::SnapshotCaptureStatus::Captured
                    )
                })
                .count(),
            captured_bytes: manifest.captured_text_bytes as u64,
        })
        .collect();
    let findings = report
        .findings()
        .into_iter()
        .map(|finding| RunnerFinding {
            id: finding.id,
            title: finding.title,
            claim: finding.claim,
            evidence_count: finding.evidence_count,
            publishable: finding.publishable,
        })
        .collect();
    RunnerRunResult {
        protocol_version: RUNNER_PROTOCOL_VERSION.to_string(),
        run_id: report.run_id.clone(),
        status: summary_status(&summary),
        summary,
        findings,
        snapshots,
    }
}

fn runner_summary_from_review(summary: &ReviewRunSummary) -> RunnerRunSummary {
    RunnerRunSummary {
        sessions: summary.sessions,
        completed_sessions: summary.completed_sessions,
        model_calls: summary.model_calls,
        tool_calls: summary.tool_calls,
        findings: summary.findings,
        publishable_findings: summary.publishable_findings,
        elapsed_ms: summary.elapsed_ms,
        input_tokens: summary.input_tokens,
        output_tokens: summary.output_tokens,
        total_tokens: summary.total_tokens,
        artifacts: summary.artifacts,
        artifact_bytes: summary.artifact_bytes,
        snapshot_count: summary.snapshot_count,
    }
}

fn summary_status(summary: &RunnerRunSummary) -> String {
    if summary.completed_sessions == summary.sessions {
        "completed".to_string()
    } else {
        "partial".to_string()
    }
}

fn changed_file_specs(
    repo_root: &Path,
    changed_files: &[String],
    target_path: &str,
) -> Vec<ChangedFileSpec> {
    let files = if changed_files.is_empty() {
        vec![target_path.to_string()]
    } else {
        changed_files.to_vec()
    };
    files
        .into_iter()
        .filter(|path| repo_root.join(path).is_file())
        .map(ChangedFileSpec::modified)
        .collect()
}

fn select_target_path(repo_root: &Path, changed_files: &[String]) -> Result<String> {
    for path in changed_files {
        if repo_root.join(path).is_file() {
            return Ok(path.clone());
        }
    }
    for candidate in ["Cargo.toml", "package.json", "README.md", "pyproject.toml"] {
        if repo_root.join(candidate).is_file() {
            return Ok(candidate.to_string());
        }
    }
    find_first_text_candidate(repo_root)
        .ok_or_else(|| anyhow::anyhow!("repo has no obvious text file to review"))
}

fn find_first_text_candidate(repo_root: &Path) -> Option<String> {
    fn visit(root: &Path, dir: &Path, depth: usize) -> Option<String> {
        if depth > 4 {
            return None;
        }
        let entries = fs::read_dir(dir).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if matches!(
                name.as_ref(),
                ".git" | "node_modules" | "target" | "dist" | "build" | ".next"
            ) {
                continue;
            }
            if path.is_file() && looks_textual(&path) {
                return path
                    .strip_prefix(root)
                    .ok()
                    .map(|path| path.to_string_lossy().into_owned());
            }
            if path.is_dir() {
                if let Some(found) = visit(root, &path, depth + 1) {
                    return Some(found);
                }
            }
        }
        None
    }
    visit(repo_root, repo_root, 0)
}

fn looks_textual(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            matches!(
                extension,
                "rs" | "ts"
                    | "tsx"
                    | "js"
                    | "jsx"
                    | "json"
                    | "toml"
                    | "md"
                    | "py"
                    | "go"
                    | "java"
                    | "kt"
                    | "rb"
                    | "php"
                    | "c"
                    | "h"
                    | "cpp"
                    | "hpp"
                    | "cs"
                    | "swift"
            )
        })
        .unwrap_or(false)
}

#[derive(Default)]
struct RecordingReviewEventSink {
    records: std::sync::Mutex<Vec<ReviewEventRecord>>,
}

impl RecordingReviewEventSink {
    fn records(&self) -> Vec<ReviewEventRecord> {
        self.records
            .lock()
            .expect("review event sink poisoned")
            .clone()
    }
}

impl ReviewEventSink for RecordingReviewEventSink {
    fn emit_review_event(&self, record: ReviewEventRecord) {
        self.records
            .lock()
            .expect("review event sink poisoned")
            .push(record);
    }
}
