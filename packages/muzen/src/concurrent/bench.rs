use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use clap::Parser;

use crate::bench::{preferred_bench_file_score, synthetic_changed_files};
use crate::concurrent::contracts::*;
use crate::concurrent::model::{MockReviewModel, StaticModelRouter};
use crate::concurrent::repo::RepoSnapshot;
use crate::concurrent::runtime::{ConcurrentJobRuntime, ConcurrentSessionSpec};
use crate::concurrent::tools::ToolEngine;
use crate::contracts::*;
use crate::repo::RepoContext;
use crate::tools::{ToolOutcome, ToolRegistry};
use crate::util::timestamp_utc;

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

pub(crate) fn run_compare(args: ConcurrentBenchArgs) -> Result<ComparisonReport> {
    let root = std::fs::canonicalize(&args.repo)
        .with_context(|| format!("failed to canonicalize repo {}", args.repo.display()))?;
    let policy = PathPolicyV1::bench(args.max_file_kb, args.max_search_matches);
    let change = synthetic_change(&root, &policy)?;
    let target_path = target_file(&change).context("benchmark repo has no target file")?;

    let sync = run_sync_baseline(
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
        sync.elapsed_ms as f64 / concurrent.elapsed_ms as f64
    };
    let search_scan_reduction = if concurrent.counters.search_scans == 0 {
        0.0
    } else {
        sync.counters.search_scans as f64 / concurrent.counters.search_scans as f64
    };
    let report = ComparisonReport {
        sessions: args.sessions,
        sync,
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

fn run_sync_baseline(
    root: &Path,
    policy: PathPolicyV1,
    change: ChangeScopeV1,
    target_path: &Path,
    query: &str,
    sessions: usize,
) -> Result<ConcurrentRunReport> {
    let started = Instant::now();
    let repo = Arc::new(RepoContext::new(root.to_path_buf(), policy, change)?);
    let artifacts = Arc::new(ArtifactStore::default());
    let tools = Arc::new(ToolRegistry {
        repo: Arc::clone(&repo),
        artifacts: Arc::clone(&artifacts),
    });
    let reports = Arc::new(Mutex::new(Vec::new()));
    thread::scope(|scope| {
        for index in 0..sessions {
            let tools = Arc::clone(&tools);
            let reports = Arc::clone(&reports);
            let target_path = target_path.to_path_buf();
            let query = query.to_string();
            scope.spawn(move || {
                let session = AgentSession {
                    id: format!("sync-session-{index}"),
                    run_id: "sync-comparison".to_string(),
                    role: Role::for_index(index),
                    objective: "Synchronous comparison workload".to_string(),
                    cwd: PathBuf::from("."),
                    model_profile_id: "mock".to_string(),
                    state: AgentState::Running,
                    budget: AgentBudget {
                        max_turns: 4,
                        max_tool_calls: 8,
                        max_prompt_tokens: 32_000,
                        max_output_tokens: 512,
                    },
                    allowed_tools: ToolMask::review_read_only(),
                    events: Vec::new(),
                };
                let mut counts = ToolCounts::default();
                let mut errors = 0usize;
                for (tool_index, outcome) in [
                    tools.read_diff(&format!("{index}-read-diff"), &session),
                    tools.read_file(
                        &format!("{index}-read-file"),
                        &session,
                        ToolName::ReadFile,
                        &target_path,
                        EvidenceRevision::Review,
                    ),
                    tools.search_text(&format!("{index}-search"), &session, &query),
                ]
                .into_iter()
                .enumerate()
                {
                    match outcome {
                        Ok(outcome) => count_sync_outcome(&mut counts, outcome),
                        Err(_) => {
                            let _ = tool_index;
                            errors += 1;
                        }
                    }
                }
                counts.record_finding += 1;
                reports
                    .lock()
                    .expect("sync reports poisoned")
                    .push((counts, errors));
            });
        }
    });
    let reports = reports.lock().expect("sync reports poisoned");
    let mut tool_counts = ToolCounts::default();
    let mut errors = 0usize;
    for (counts, report_errors) in reports.iter() {
        tool_counts.add(*counts);
        errors += *report_errors;
    }
    let stats = artifacts.stats();
    let mut report = ConcurrentRunReport {
        runtime: "sync",
        sessions,
        completed_sessions: reports.len(),
        model_calls: sessions * 2,
        tool_calls: tool_counts.total(),
        tool_counts,
        findings: sessions,
        elapsed_ms: started.elapsed().as_millis() as u64,
        input_tokens: 0,
        output_tokens: 0,
        total_tokens: 0,
        artifacts: stats.artifacts,
        artifact_bytes: stats.artifact_bytes,
        counters: ConcurrentCounters {
            search_scans: sessions,
            search_dedupe_waiters: 0,
            search_cache_hits: 0,
            read_cache_hits: sessions.saturating_sub(1),
            read_file_reads: 1,
            tool_errors: errors,
            artifact_cache_hits: 0,
        },
        tool_metrics: Default::default(),
        benchmark_valid: false,
        benchmark_failures: Vec::new(),
    };
    report.benchmark_failures = sync_failures(&report);
    report.benchmark_valid = report.benchmark_failures.is_empty();
    Ok(report)
}

fn count_sync_outcome(counts: &mut ToolCounts, outcome: ToolOutcome) {
    if outcome.tool_result.status == ToolStatus::Ok {
        counts.increment(outcome.tool_result.tool_name);
    }
}

fn sync_failures(report: &ConcurrentRunReport) -> Vec<String> {
    let mut failures = Vec::new();
    if report.completed_sessions != report.sessions {
        failures.push(format!(
            "only {}/{} sync sessions completed",
            report.completed_sessions, report.sessions
        ));
    }
    if report.tool_counts.read_diff == 0
        || report.tool_counts.read_file == 0
        || report.tool_counts.search_text == 0
    {
        failures.push("sync baseline did not exercise required tools".to_string());
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
