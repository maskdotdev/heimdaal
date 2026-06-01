use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::cli::BenchArgs;
use crate::contracts::*;
use crate::repo::RepoContext;
use crate::runtime::{log_bench_event, run_review, RuntimeReport};
use crate::util::{timestamp_utc, DEFAULT_MODEL, SCHEMA_VERSION};

pub(crate) fn run_bench(args: BenchArgs) -> Result<RuntimeReport> {
    let job = bench_job(&args)?;
    log_bench_event("after_job_build", &args.model, args.sessions, None);
    let report = run_review(job, None)?;
    log_bench_event("after_work", &args.model, args.sessions, Some(&report));
    thread::sleep(Duration::from_millis(args.hold_ms));
    log_bench_event("after_hold", &args.model, args.sessions, Some(&report));
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !report.benchmark_valid {
        bail!("benchmark gates failed: {:?}", report.benchmark_failures);
    }
    Ok(report)
}

pub(crate) fn bench_job(args: &BenchArgs) -> Result<ReviewRunJobV1> {
    let root = fs::canonicalize(&args.repo)
        .with_context(|| format!("failed to canonicalize repo {}", args.repo.display()))?;
    let path_policy = PathPolicyV1::bench(args.max_file_kb, args.max_search_matches);
    let changed_files = synthetic_changed_files(&root, &path_policy)?;
    let personas = (0..args.sessions)
        .map(|index| PersonaSpecV1 {
            id: format!("bench-session-{index}"),
            role: Role::for_index(index),
            objective: format!(
                "Review the materialized repo as {:?}. Gather concrete evidence with read-only tools and finish with a concise finding or no-finding rationale.",
                Role::for_index(index)
            ),
            cwd: Some(PathBuf::from(".")),
            model_profile_id: Some("bench-oai".to_string()),
            allowed_tools: ToolMask::review_read_only(),
            budget: AgentBudget {
                max_turns: args.max_turns,
                max_tool_calls: args.max_tool_calls,
                max_prompt_tokens: 32_000,
                max_output_tokens: args.max_output_tokens as u64,
            },
        })
        .collect();

    Ok(ReviewRunJobV1 {
        schema_version: SCHEMA_VERSION.to_string(),
        run_id: "bench-run".to_string(),
        project_id: "heimdaal-bench".to_string(),
        attempt: 1,
        idempotency_key: "bench-run-1".to_string(),
        deadline_utc: None,
        repo: MaterializedRepoScopeV1 {
            provider: RepoProvider::Local,
            repo_id: "heimdaal-local".to_string(),
            repo_root: root.clone(),
            worktree_root: root,
            default_cwd: PathBuf::from("."),
            materialization_id: "bench-materialization".to_string(),
            materialized_at_utc: timestamp_utc(),
            materialization_digest: None,
        },
        change: ChangeScopeV1 {
            kind: ChangeKind::LocalDiff,
            change_id: "bench-change".to_string(),
            source_ref: "local-review".to_string(),
            target_ref: "local-base".to_string(),
            base_revision_id: "base-unavailable".to_string(),
            head_revision_id: "review-worktree".to_string(),
            merge_base_revision_id: None,
            changed_files_manifest_ref: None,
            diff_manifest_ref: None,
            snapshot_mode: SnapshotMode::WorktreeHead,
            rename_detection: RenameDetection::None,
            changed_files,
        },
        model_profiles: vec![ModelProfileRefV1 {
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
        }],
        default_model_profile_id: "bench-oai".to_string(),
        personas,
        path_policy,
        scratch_policy: ScratchPolicyV1 {
            scratch_root: None,
            output_root: None,
            max_scratch_bytes: 0,
            cleanup_on_finish: true,
        },
        model_visibility: ModelVisibilityPolicyV1 {
            max_prompt_artifact_bytes: 1200,
            allow_full_file_content_in_prompts: false,
            deny_globs: vec![".git".to_string()],
            redact_secret_like_content: true,
        },
        output_redaction: OutputRedactionPolicyV1 {
            policy_id: "bench-redaction-v1".to_string(),
            redact_repo_secrets: true,
            persist_full_file_contents: false,
        },
        budgets: RunBudgetsV1 {
            max_active_sessions: args.max_active.max(1).min(args.sessions.max(1)),
            max_wall_time_ms: 120_000,
            max_model_calls: args.sessions * args.max_turns,
            max_tool_calls: args.sessions * args.max_tool_calls,
            max_prompt_tokens: 1_000_000,
            max_output_tokens: 1_000_000,
            max_artifact_bytes: 64 * 1024 * 1024,
            max_scratch_bytes: 0,
            rss_target_mb: Some(64),
            rss_limit_mb: Some(256),
        },
        telemetry: TelemetryPolicyV1 {
            emit_debug_events: false,
        },
    })
}

pub(crate) fn synthetic_changed_files(
    root: &Path,
    policy: &PathPolicyV1,
) -> Result<Vec<ChangedFileEntryV1>> {
    let repo = RepoContext::new(
        root.to_path_buf(),
        policy.clone(),
        ChangeScopeV1 {
            kind: ChangeKind::LocalDiff,
            change_id: "synthetic".to_string(),
            source_ref: "review".to_string(),
            target_ref: "base".to_string(),
            base_revision_id: "base".to_string(),
            head_revision_id: "head".to_string(),
            merge_base_revision_id: None,
            changed_files_manifest_ref: None,
            diff_manifest_ref: None,
            snapshot_mode: SnapshotMode::WorktreeHead,
            rename_detection: RenameDetection::None,
            changed_files: Vec::new(),
        },
    )?;
    let mut files = repo.walk_files()?;
    files.sort_by_key(|path| preferred_bench_file_score(path));
    Ok(files
        .into_iter()
        .take(24)
        .map(|path| ChangedFileEntryV1 {
            status: ChangedFileStatus::Modified,
            old_path: Some(path.clone()),
            new_path: Some(path),
            old_content_hash: None,
            new_content_hash: None,
            is_binary: false,
            is_generated: false,
        })
        .collect())
}

pub(crate) fn preferred_bench_file_score(path: &Path) -> usize {
    let text = path.to_string_lossy();
    if text == "README.md" {
        0
    } else if text.contains("packages/muzen/src/main.rs") {
        1
    } else if text.contains("packages/review") {
        2
    } else if text.contains("docs/architecture") {
        3
    } else {
        10
    }
}
