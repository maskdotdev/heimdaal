use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::Parser;
use tokio_util::sync::CancellationToken;

use crate::bench::{preferred_bench_file_score, synthetic_changed_files};
use crate::concurrent::contracts::*;
use crate::concurrent::model::{
    MockReviewModel, ModelLimiter, OpenAiChatCompletionsClient, ProfileModelRouter,
    StaticModelRouter,
};
use crate::concurrent::repo::RepoSnapshot;
use crate::concurrent::runtime::{ConcurrentJobRuntime, ConcurrentSessionSpec};
use crate::concurrent::tools::{ToolEngine, ToolRegistry};
use crate::contracts::*;
use crate::events::{EventEmitter, EventRecord};
use crate::job::{effective_personas, tool_allowed, validate_job};
use crate::util::{timestamp_utc, SCHEMA_VERSION};

#[derive(Parser, Debug, Clone)]
pub(crate) struct ConcurrentBenchArgs {
    #[arg(long, default_value = ".")]
    pub(crate) repo: PathBuf,

    #[arg(long, default_value_t = 50)]
    pub(crate) sessions: usize,

    #[arg(long, default_value_t = 200)]
    pub(crate) max_file_kb: usize,

    #[arg(long, default_value_t = 120)]
    pub(crate) max_search_matches: usize,

    #[arg(long, default_value = "use|fn|struct")]
    pub(crate) query: String,
}

#[derive(Parser, Debug, Clone)]
pub(crate) struct ConcurrentRealBenchArgs {
    #[arg(long, default_value = ".")]
    pub(crate) repo: PathBuf,

    #[arg(long, default_value_t = 10)]
    pub(crate) sessions: usize,

    #[arg(long, default_value_t = 10)]
    pub(crate) max_active: usize,

    #[arg(long, default_value_t = 16)]
    pub(crate) max_model_concurrency: usize,

    #[arg(long, default_value_t = 5)]
    pub(crate) max_turns: usize,

    #[arg(long, default_value_t = 4)]
    pub(crate) max_tool_calls: usize,

    #[arg(long, default_value_t = 200)]
    pub(crate) max_file_kb: usize,

    #[arg(long, default_value_t = 120)]
    pub(crate) max_search_matches: usize,

    #[arg(long, default_value = "use|fn|struct")]
    pub(crate) query: String,

    #[arg(long, default_value = "gpt-4.1-nano")]
    pub(crate) model: String,

    #[arg(long, default_value_t = 128)]
    pub(crate) max_output_tokens: u32,

    #[arg(long, default_value_t = 1000)]
    pub(crate) hold_ms: u64,
}

pub(crate) fn run_compare(args: ConcurrentBenchArgs) -> Result<ComparisonReport> {
    let root = std::fs::canonicalize(&args.repo)
        .with_context(|| format!("failed to canonicalize repo {}", args.repo.display()))?;
    let policy = PathPolicyV1::bench(args.max_file_kb, args.max_search_matches);
    let change = synthetic_change(&root, &policy)?;
    let target_path = target_file(&change).context("benchmark repo has no target file")?;

    let baseline = run_serial_baseline(
        &root,
        policy.clone(),
        change.clone(),
        &target_path,
        &args.query,
        args.sessions,
    )?;
    let concurrent = run_concurrent(
        &root,
        policy,
        change,
        &target_path,
        &args.query,
        args.sessions,
    )?;
    let speedup = if concurrent.elapsed_ms == 0 {
        0.0
    } else {
        baseline.elapsed_ms as f64 / concurrent.elapsed_ms as f64
    };
    let search_scan_reduction = if concurrent.counters.search_scans == 0 {
        0.0
    } else {
        baseline.counters.search_scans as f64 / concurrent.counters.search_scans as f64
    };
    let report = ComparisonReport {
        sessions: args.sessions,
        sync: baseline,
        concurrent,
        speedup,
        search_scan_reduction,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !report.concurrent.benchmark_valid {
        bail!(
            "concurrent benchmark failed proof gates: {:?}",
            report.concurrent.benchmark_failures
        );
    }
    Ok(report)
}

pub(crate) fn run_real_bench(args: ConcurrentRealBenchArgs) -> Result<ConcurrentRunReport> {
    let root = std::fs::canonicalize(&args.repo)
        .with_context(|| format!("failed to canonicalize repo {}", args.repo.display()))?;
    let policy = PathPolicyV1::bench(args.max_file_kb, args.max_search_matches);
    let change = synthetic_change(&root, &policy)?;
    let target_path = target_file(&change).context("benchmark repo has no target file")?;
    let mut limits = RuntimeLimits::standard(
        args.max_active.max(1).min(args.sessions.max(1)),
        policy.max_file_bytes,
        policy.max_search_results,
    );
    limits.max_model_concurrency_global = args.max_model_concurrency.max(1);
    limits.max_tool_calls_per_turn = args.max_tool_calls.max(1);
    let limits = Arc::new(limits);

    let (snapshot, _snapshot_report) =
        RepoSnapshot::build(&root, &policy, &change).map_err(|error| anyhow::anyhow!("{error}"))?;
    let registry = Arc::new(
        ToolRegistry::review_defaults()
            .map_err(|error| anyhow::anyhow!("failed to build tool registry: {error}"))?,
    );
    let tools = Arc::new(
        ToolEngine::with_registry(
            Arc::clone(&snapshot),
            Arc::clone(&limits),
            Arc::clone(&registry),
        )
        .map_err(|error| anyhow::anyhow!("failed to build concurrent tool engine: {error}"))?,
    );
    let profile = ModelProfileRefV1 {
        id: "bench-oai".to_string(),
        provider_kind: ProviderKind::OpenaiCompatible,
        provider_profile_id: "env-openai-compatible".to_string(),
        credential_ref: "env:OPENAI_API_KEY".to_string(),
        model: args.model.clone(),
        max_input_tokens: 32_000,
        max_output_tokens: args.max_output_tokens,
        tool_calling_mode: ToolCallingMode::Required,
        temperature: Some(0.0),
        top_p: None,
    };
    let base_url = std::env::var("OAI_BASE_URL")
        .or_else(|_| std::env::var("OPENAI_BASE_URL"))
        .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
    let model = Arc::new(OpenAiChatCompletionsClient::from_profile(
        profile,
        base_url,
        Arc::new(ModelLimiter::new(args.max_model_concurrency)),
        registry,
    )?);
    let runtime = ConcurrentJobRuntime {
        snapshot,
        model_router: Arc::new(StaticModelRouter::new(model)),
        tools,
        limits,
        review_revision_id: change.head_revision_id.clone(),
        emitter: None,
    };
    let target_path = target_path.to_string_lossy().into_owned();
    let session_specs = (0..args.sessions)
        .map(|index| ConcurrentSessionSpec {
            scope: SessionScope {
                id: SessionId(format!("concurrent-oai-session-{index}")),
                role: Role::for_index(index),
                objective: format!(
                    "Benchmark task: call read_diff, call read_file with path `{target_path}`, call search_text with query `{}`, then record one concise evidence-backed benchmark finding.",
                    args.query
                ),
                model_profile_id: Some("bench-oai".to_string()),
                capabilities: CapabilitySet::review_read_only(),
                budget: AgentBudget {
                    max_turns: args.max_turns,
                    max_tool_calls: args.max_tool_calls,
                    max_prompt_tokens: 32_000,
                    max_output_tokens: args.max_output_tokens as u64,
                },
            },
        })
        .collect::<Vec<_>>();
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get().clamp(2, 8))
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;
    let report = tokio_runtime.block_on(runtime.run_sessions(session_specs));
    if args.hold_ms > 0 {
        thread::sleep(Duration::from_millis(args.hold_ms));
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !report.benchmark_valid {
        bail!(
            "concurrent real benchmark failed proof gates: {:?}",
            report.benchmark_failures
        );
    }
    Ok(report)
}

pub(crate) fn run_job_concurrent(job: ReviewRunJobV1) -> Result<ConcurrentRunReport> {
    Ok(run_job_concurrent_with_result(job, None)?.report)
}

#[derive(Debug)]
pub(crate) struct ConcurrentJobOutput {
    pub(crate) report: ConcurrentRunReport,
    pub(crate) result: ReviewRunResultV1,
}

pub(crate) fn run_job_concurrent_with_result(
    job: ReviewRunJobV1,
    emitter: Option<Arc<EventEmitter>>,
) -> Result<ConcurrentJobOutput> {
    validate_job(&job)?;
    let registry = Arc::new(
        ToolRegistry::review_defaults()
            .map_err(|error| anyhow::anyhow!("failed to build tool registry: {error}"))?,
    );
    let mut limits = RuntimeLimits::standard(
        job.budgets.max_active_sessions.max(1),
        job.path_policy.max_file_bytes,
        job.path_policy.max_search_results,
    );
    limits.max_tool_calls_per_turn = 4;
    limits.max_model_concurrency_global = job.budgets.max_active_sessions.max(1);
    let limits = Arc::new(limits);
    let (snapshot, _snapshot_report) =
        RepoSnapshot::build(&job.repo.worktree_root, &job.path_policy, &job.change)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
    let tools = Arc::new(
        ToolEngine::with_registry(
            Arc::clone(&snapshot),
            Arc::clone(&limits),
            Arc::clone(&registry),
        )
        .map_err(|error| anyhow::anyhow!("failed to build concurrent tool engine: {error}"))?,
    );
    let base_url = std::env::var("OAI_BASE_URL")
        .or_else(|_| std::env::var("OPENAI_BASE_URL"))
        .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
    let router = ProfileModelRouter::from_profiles(
        &job.model_profiles,
        job.default_model_profile_id.clone(),
        base_url,
        Arc::new(ModelLimiter::new(job.budgets.max_active_sessions.max(1))),
        registry,
    )
    .map_err(|error| anyhow::anyhow!("{error}"))?;
    let runtime = ConcurrentJobRuntime {
        snapshot,
        model_router: Arc::new(router),
        tools,
        limits,
        review_revision_id: job.change.head_revision_id.clone(),
        emitter: emitter.clone(),
    };
    let session_specs = effective_personas(&job)
        .into_iter()
        .map(|persona| ConcurrentSessionSpec {
            scope: SessionScope {
                id: SessionId(persona.id),
                role: persona.role,
                objective: persona.objective,
                model_profile_id: persona
                    .model_profile_id
                    .or_else(|| Some(job.default_model_profile_id.clone())),
                capabilities: capabilities_from_mask(persona.allowed_tools),
                budget: persona.budget,
            },
        })
        .collect::<Vec<_>>();
    if let Some(emitter) = &emitter {
        emitter.emit(EventRecord::new(
            EventLevel::Info,
            EventType::RunStarted,
            serde_json::json!({
            "projectId": job.project_id,
            "sessions": session_specs.len(),
            "runtime": "concurrent"
            }),
        ));
    }
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get().clamp(2, 8))
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;
    let report = tokio_runtime.block_on(runtime.run_sessions(session_specs));
    let findings = runtime.tools.findings.all();
    let outcome = concurrent_review_outcome(&report, findings.len());
    let result = ReviewRunResultV1 {
        schema_version: SCHEMA_VERSION,
        run_id: job.run_id,
        attempt: job.attempt,
        runtime: ReviewRuntimeV1::Concurrent,
        outcome,
        publishability: if report.completed_sessions == report.sessions {
            Publishability::Publishable
        } else {
            Publishability::DiagnosticOnly
        },
        sessions: report.sessions,
        completed_sessions: report.completed_sessions,
        findings,
        tool_counts: report.tool_counts,
        model_calls: report.model_calls,
        tokens: TokenUsage {
            input_tokens: report.input_tokens,
            output_tokens: report.output_tokens,
            total_tokens: report.total_tokens,
        },
        artifact_stats: ArtifactStats {
            artifacts: report.artifacts,
            artifact_bytes: report.artifact_bytes,
            content_refs: report.artifacts,
        },
        elapsed_ms: report.elapsed_ms,
    };
    if let Some(emitter) = &emitter {
        emitter.emit(EventRecord::new(
            EventLevel::Info,
            EventType::RunFinished,
            serde_json::json!(result),
        ));
    }
    Ok(ConcurrentJobOutput { report, result })
}

fn concurrent_review_outcome(report: &ConcurrentRunReport, findings: usize) -> ReviewOutcomeV1 {
    if report.completed_sessions < report.sessions {
        ReviewOutcomeV1::FailedPartial
    } else if findings > 0 {
        ReviewOutcomeV1::CompletedWithFindings
    } else {
        ReviewOutcomeV1::CompletedNoFindings
    }
}

pub(crate) fn capabilities_from_mask(mask: ToolMask) -> CapabilitySet {
    let mut capabilities = CapabilitySet {
        // Preserve sync runtime path semantics for the ReviewRunJobV1 bridge:
        // tool arguments are repo-relative, while persona cwd is prompt context.
        fs_scope: FsScope::repo_root(),
        tool_grants: Default::default(),
    };
    for &tool in ToolName::review_read_only_tools() {
        if tool_allowed(mask, tool) {
            capabilities.grant(ToolId::from(tool), ToolGrant::allow_review_read_only());
        }
    }
    capabilities
}

fn run_concurrent(
    root: &Path,
    policy: PathPolicyV1,
    change: ChangeScopeV1,
    target_path: &Path,
    query: &str,
    sessions: usize,
) -> Result<ConcurrentRunReport> {
    let limits = Arc::new(RuntimeLimits::standard(
        sessions,
        policy.max_file_bytes,
        policy.max_search_results,
    ));
    let (snapshot, _snapshot_report) =
        RepoSnapshot::build(root, &policy, &change).map_err(|error| anyhow::anyhow!("{error}"))?;
    let tools = Arc::new(
        ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits))
            .map_err(|error| anyhow::anyhow!("failed to build concurrent tool engine: {error}"))?,
    );
    let model = Arc::new(MockReviewModel::new(
        target_path.to_string_lossy().into_owned(),
        query.to_string(),
    ));
    let model_router = Arc::new(StaticModelRouter::new(model));
    let runtime = ConcurrentJobRuntime {
        snapshot,
        model_router,
        tools,
        limits,
        review_revision_id: change.head_revision_id.clone(),
        emitter: None,
    };
    let session_specs = (0..sessions)
        .map(|index| ConcurrentSessionSpec {
            scope: SessionScope {
                id: SessionId(format!("parallel-session-{index}")),
                role: Role::for_index(index),
                objective: "Gather diff, file, and search evidence with concurrent tools."
                    .to_string(),
                model_profile_id: Some("mock".to_string()),
                capabilities: CapabilitySet::review_read_only(),
                budget: AgentBudget {
                    max_turns: 4,
                    max_tool_calls: 8,
                    max_prompt_tokens: 32_000,
                    max_output_tokens: 512,
                },
            },
        })
        .collect::<Vec<_>>();
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get().clamp(2, 8))
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;
    Ok(tokio_runtime.block_on(runtime.run_sessions(session_specs)))
}

fn run_serial_baseline(
    root: &Path,
    policy: PathPolicyV1,
    change: ChangeScopeV1,
    target_path: &Path,
    query: &str,
    sessions: usize,
) -> Result<ConcurrentRunReport> {
    let started = Instant::now();
    let (snapshot, _snapshot_report) =
        RepoSnapshot::build(root, &policy, &change).map_err(|error| anyhow::anyhow!("{error}"))?;
    let limits = Arc::new(RuntimeLimits::standard(
        1,
        policy.max_file_bytes,
        policy.max_search_results,
    ));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build serial baseline runtime")?;
    let target_path = target_path.to_string_lossy().into_owned();
    let mut tool_counts = ToolCounts::default();
    let mut errors = 0usize;
    let mut counters = ConcurrentCounters::default();
    let mut artifacts = 0usize;
    let mut artifact_bytes = 0usize;
    let mut completed_sessions = 0usize;
    for index in 0..sessions {
        let tools = ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits))
            .map_err(|error| anyhow::anyhow!("failed to build serial baseline tools: {error}"))?;
        let scope = SessionScope {
            id: SessionId(format!("serial-baseline-session-{index}")),
            role: Role::for_index(index),
            objective: "Gather diff, file, and search evidence with serial tools.".to_string(),
            model_profile_id: Some("mock".to_string()),
            capabilities: CapabilitySet::review_read_only(),
            budget: AgentBudget {
                max_turns: 4,
                max_tool_calls: 8,
                max_prompt_tokens: 32_000,
                max_output_tokens: 512,
            },
        };
        let first_turn = runtime.block_on(tools.execute_batch(
            scope.clone(),
            TurnId(0),
            vec![
                ModelToolCall {
                    call_id: ToolCallId(format!("serial-baseline-{index}-read-diff")),
                    index: 0,
                    name: ToolId::from(ToolName::ReadDiff),
                    raw_arguments: "{}".to_string(),
                },
                ModelToolCall {
                    call_id: ToolCallId(format!("serial-baseline-{index}-read-file")),
                    index: 1,
                    name: ToolId::from(ToolName::ReadFile),
                    raw_arguments: serde_json::json!({ "path": target_path.clone() }).to_string(),
                },
                ModelToolCall {
                    call_id: ToolCallId(format!("serial-baseline-{index}-search")),
                    index: 2,
                    name: ToolId::from(ToolName::SearchText),
                    raw_arguments: serde_json::json!({ "query": query.to_string() }).to_string(),
                },
            ],
            CancellationToken::new(),
        ));
        let second_turn = runtime.block_on(tools.execute_batch(
            scope,
            TurnId(1),
            vec![ModelToolCall {
                call_id: ToolCallId(format!("serial-baseline-{index}-finding")),
                index: 0,
                name: ToolId::from(ToolName::RecordFinding),
                raw_arguments: serde_json::json!({
                    "title": format!("serial baseline session {index}"),
                    "claim": "The benchmark session gathered diff, file, and search evidence."
                })
                .to_string(),
            }],
            CancellationToken::new(),
        ));
        let mut session_errors = 0usize;
        for result in first_turn.iter().chain(second_turn.iter()) {
            if result.ok {
                if let Some(tool) = result.tool_name.as_builtin() {
                    tool_counts.increment(tool);
                }
            } else {
                session_errors += 1;
            }
        }
        if session_errors == 0 {
            completed_sessions += 1;
        }
        errors += session_errors;
        add_concurrent_counters(&mut counters, tools.snapshot_counters());
        let (session_artifacts, session_artifact_bytes) = tools.artifacts.stats();
        artifacts += session_artifacts;
        artifact_bytes += session_artifact_bytes;
    }
    let mut report = ConcurrentRunReport {
        runtime: "serial",
        sessions,
        completed_sessions,
        model_calls: sessions * 2,
        tool_calls: tool_counts.total(),
        tool_counts,
        findings: tool_counts.record_finding,
        publishable_findings: tool_counts.record_finding,
        elapsed_ms: started.elapsed().as_millis() as u64,
        input_tokens: 0,
        output_tokens: 0,
        total_tokens: 0,
        artifacts,
        artifact_bytes,
        counters,
        tool_metrics: Default::default(),
        terminal_diagnostics: (0..completed_sessions)
            .map(|index| SessionTerminalDiagnostic {
                session_id: format!("serial-baseline-session-{index}"),
                completed: true,
                terminal_tool: Some(ToolName::RecordFinding.as_str().to_string()),
                terminal_summary: Some("synthetic serial baseline finding".to_string()),
                saw_diff: true,
                saw_file: true,
                saw_search: true,
                model_calls: 2,
                tool_counts: ToolCounts {
                    read_diff: 1,
                    read_file: 1,
                    search_text: 1,
                    record_finding: 1,
                    ..Default::default()
                },
            })
            .collect(),
        benchmark_valid: false,
        benchmark_failures: Vec::new(),
    };
    report.counters.tool_errors = errors;
    report.benchmark_failures = serial_baseline_failures(&report);
    report.benchmark_valid = report.benchmark_failures.is_empty();
    Ok(report)
}

fn add_concurrent_counters(total: &mut ConcurrentCounters, next: ConcurrentCounters) {
    total.search_scans += next.search_scans;
    total.search_dedupe_waiters += next.search_dedupe_waiters;
    total.search_cache_hits += next.search_cache_hits;
    total.read_cache_hits += next.read_cache_hits;
    total.read_file_reads += next.read_file_reads;
    total.tool_errors += next.tool_errors;
    total.artifact_cache_hits += next.artifact_cache_hits;
}

fn serial_baseline_failures(report: &ConcurrentRunReport) -> Vec<String> {
    let mut failures = Vec::new();
    if report.completed_sessions != report.sessions {
        failures.push(format!(
            "only {}/{} serial baseline sessions completed",
            report.completed_sessions, report.sessions
        ));
    }
    if report.tool_counts.read_diff == 0
        || report.tool_counts.read_file == 0
        || report.tool_counts.search_text == 0
    {
        failures.push("serial baseline did not exercise required tools".to_string());
    }
    failures
}

fn synthetic_change(root: &Path, policy: &PathPolicyV1) -> Result<ChangeScopeV1> {
    Ok(ChangeScopeV1 {
        kind: ChangeKind::LocalDiff,
        change_id: "concurrent-bench-change".to_string(),
        source_ref: "local-review".to_string(),
        target_ref: "local-base".to_string(),
        base_revision_id: "base-unavailable".to_string(),
        head_revision_id: "review-worktree".to_string(),
        merge_base_revision_id: None,
        changed_files_manifest_ref: None,
        diff_manifest_ref: None,
        snapshot_mode: SnapshotMode::WorktreeHead,
        rename_detection: RenameDetection::None,
        changed_files: synthetic_changed_files(root, policy)?,
    })
}

fn target_file(change: &ChangeScopeV1) -> Option<PathBuf> {
    let mut paths = change
        .changed_files
        .iter()
        .filter_map(|file| file.new_path.as_ref().or(file.old_path.as_ref()))
        .cloned()
        .collect::<Vec<_>>();
    paths.sort_by_key(|path| preferred_bench_file_score(path));
    paths.into_iter().next()
}
