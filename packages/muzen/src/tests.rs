use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::bench::bench_job;
use crate::cli::{BenchArgs, BenchTerminalPolicy, Cli, Command};
use crate::concurrent::bench::capabilities_from_mask;
use crate::concurrent::contracts::{
    ArtifactId as ConcurrentArtifactId, ArtifactKey, CacheInfo, CacheStatus, CapabilitySet,
    ConcurrentCounters, ConcurrentRunReport, ConversationItem, FsScope, LimitInfo, ModelToolCall,
    ModelTurn, RepoPath, RuntimeError, RuntimeLimits, RuntimeResult, SessionId, SessionScope,
    SnapshotId, ToolCallId, ToolErrorCode, ToolGrant, ToolId, TurnId,
};
use crate::concurrent::model::{ConcurrentModelClient, MockReviewModel, StaticModelRouter};
use crate::concurrent::repo::RepoSnapshot;
use crate::concurrent::runtime::{
    benchmark_failures as concurrent_benchmark_failures, ConcurrentJobRuntime,
    ConcurrentSessionSpec,
};
use crate::concurrent::tools::ToolEngine;
use crate::concurrent::tools::{
    CustomToolArtifact, CustomToolContext, CustomToolHandler, CustomToolOutput, ToolRegistry,
};
use crate::contracts::*;
use crate::events::{EventEmitter, EventEmitterState};
use crate::repo::RepoContext;
use crate::util::DEFAULT_MODEL;
use async_trait::async_trait;

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn concurrent_transcript_stores_artifact_refs_not_content() {
        let item = ConversationItem::ToolResult {
            call_id: ToolCallId("tool-1".to_string()),
            name: ToolId::from(ToolName::ReadFile),
            content: Box::new(crate::concurrent::contracts::ToolResultEnvelope {
                ok: true,
                tool_call_id: ToolCallId("tool-1".to_string()),
                tool_name: ToolId::from(ToolName::ReadFile),
                snapshot_id: SnapshotId("snapshot-1".to_string()),
                artifact_id: Some(ConcurrentArtifactId("artifact-1".to_string())),
                cache: CacheInfo {
                    status: CacheStatus::Miss,
                    key_hash: None,
                },
                limits: LimitInfo::default(),
                data: None,
                error: None,
            }),
        };

        let serialized = serde_json::to_string(&item).unwrap();
        assert!(serialized.contains("artifact-1"));
        assert!(!serialized.contains("full file content"));
    }

    #[test]
    fn path_policy_blocks_parent_escape() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello").unwrap();
        let repo = test_repo(temp.path());
        let escaped = repo.normalize_tool_path(Path::new("../outside"));
        assert!(escaped.is_err());
    }

    #[test]
    fn path_policy_blocks_dot_git() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir(temp.path().join(".git")).unwrap();
        fs::write(temp.path().join(".git/config"), "secret").unwrap();
        let repo = test_repo(temp.path());
        let denied = repo.read_text_file(Path::new(".git/config"), 1024);
        assert!(denied.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn path_policy_blocks_symlink_escape() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("secret.txt"), "secret").unwrap();
        symlink(
            outside.path().join("secret.txt"),
            temp.path().join("link.txt"),
        )
        .unwrap();
        let repo = test_repo(temp.path());
        let denied = repo.read_text_file(Path::new("link.txt"), 1024);
        assert!(denied.is_err());
    }

    #[test]
    fn benchmark_gate_requires_real_tools() {
        let mut report = ConcurrentRunReport {
            runtime: "concurrent",
            sessions: 10,
            completed_sessions: 10,
            model_calls: 10,
            tool_calls: 0,
            tool_counts: ToolCounts::default(),
            findings: 0,
            publishable_findings: 0,
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
            elapsed_ms: 0,
            artifacts: 0,
            artifact_bytes: 0,
            counters: ConcurrentCounters::default(),
            tool_metrics: Default::default(),
            terminal_diagnostics: Vec::new(),
            benchmark_valid: false,
            benchmark_failures: Vec::new(),
        };
        report.benchmark_failures = concurrent_benchmark_failures(&report);
        assert!(report
            .benchmark_failures
            .iter()
            .any(|failure| failure.contains("read_file/read_head_file")));
        assert!(report
            .benchmark_failures
            .iter()
            .any(|failure| failure.contains("read_diff")));
        assert!(report
            .benchmark_failures
            .iter()
            .any(|failure| failure.contains("search_text")));
    }

    #[test]
    fn concurrent_report_exports_publishable_finding_count() {
        let report = ConcurrentRunReport {
            runtime: "concurrent",
            sessions: 1,
            completed_sessions: 1,
            model_calls: 1,
            tool_calls: 1,
            tool_counts: ToolCounts::default(),
            findings: 1,
            publishable_findings: 1,
            elapsed_ms: 1,
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
            artifacts: 1,
            artifact_bytes: 1,
            counters: ConcurrentCounters::default(),
            tool_metrics: Default::default(),
            terminal_diagnostics: Vec::new(),
            benchmark_valid: true,
            benchmark_failures: Vec::new(),
        };

        let value = serde_json::to_value(report).unwrap();
        assert_eq!(value["findings"], 1);
        assert_eq!(value["publishableFindings"], 1);
    }

    #[test]
    fn bench_terminal_policy_controls_finish_tool() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "benchmark repo").unwrap();

        let normal_job = bench_job(&bench_args(temp.path(), BenchTerminalPolicy::Normal)).unwrap();
        assert!(normal_job
            .personas
            .iter()
            .all(|persona| persona.allowed_tools.finish));
        assert!(normal_job
            .personas
            .iter()
            .all(|persona| persona.allowed_tools.record_finding));

        let finding_required_job = bench_job(&bench_args(
            temp.path(),
            BenchTerminalPolicy::FindingRequired,
        ))
        .unwrap();
        assert!(finding_required_job
            .personas
            .iter()
            .all(|persona| !persona.allowed_tools.finish));
        assert!(finding_required_job
            .personas
            .iter()
            .all(|persona| persona.allowed_tools.record_finding));
        assert!(finding_required_job
            .personas
            .iter()
            .all(|persona| persona.objective.contains("record_finding exactly once")));
    }

    #[test]
    fn run_and_bench_have_no_runtime_selector() {
        let run_cli = Cli::parse_from(["muzen", "run", "--job", "job.json"]);
        match run_cli.command {
            Command::Run(args) => assert_eq!(args.job, PathBuf::from("job.json")),
            _ => panic!("expected run command"),
        }

        let bench_cli = Cli::parse_from(["muzen", "bench"]);
        match bench_cli.command {
            Command::Bench(args) => assert_eq!(args.sessions, 10),
            _ => panic!("expected bench command"),
        }
    }

    #[test]
    fn concurrent_repo_path_denies_escapes_and_windows_prefixes() {
        assert!(RepoPath::parse("../secret.txt").is_err());
        assert!(RepoPath::parse("/etc/passwd").is_err());
        assert!(RepoPath::parse("C:\\secret.txt").is_err());
        assert!(RepoPath::parse("safe/path.rs").is_ok());
    }

    #[test]
    fn concurrent_tool_batch_rejects_finish_mixed_with_other_tools() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let (snapshot, _) = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = std::sync::Arc::new(RuntimeLimits::standard(1, 64 * 1024, 10));
        let engine = ToolEngine::new(snapshot, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let results = runtime.block_on(engine.execute_batch(
            test_scope("session"),
            TurnId(0),
            vec![
                ModelToolCall {
                    call_id: ToolCallId("finish".to_string()),
                    index: 0,
                    name: ToolId::from(ToolName::Finish),
                    raw_arguments: r#"{"reason":"done"}"#.to_string(),
                },
                ModelToolCall {
                    call_id: ToolCallId("read".to_string()),
                    index: 1,
                    name: ToolId::from(ToolName::ReadDiff),
                    raw_arguments: "{}".to_string(),
                },
            ],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| !result.ok));
    }

    #[test]
    fn concurrent_duplicate_search_uses_one_underlying_scan() {
        let temp = tempfile::tempdir().unwrap();
        for index in 0..200 {
            fs::write(
                temp.path().join(format!("file-{index}.rs")),
                format!("fn f_{index}() {{ let needle = {index}; }}\n"),
            )
            .unwrap();
        }
        let change = test_change_with_file("file-0.rs");
        let policy = PathPolicyV1::bench(64, 20);
        let (snapshot, _) = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = std::sync::Arc::new(RuntimeLimits::standard(10, 64 * 1024, 20));
        let engine = std::sync::Arc::new(ToolEngine::new(snapshot, limits).unwrap());
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut joins = tokio::task::JoinSet::new();
            for index in 0..10 {
                let engine = std::sync::Arc::clone(&engine);
                joins.spawn(async move {
                    let scope = test_scope(&format!("session-{index}"));
                    engine
                        .execute_batch(
                            scope,
                            TurnId(0),
                            vec![ModelToolCall {
                                call_id: ToolCallId(format!("search-{index}")),
                                index: 0,
                                name: ToolId::from(ToolName::SearchText),
                                raw_arguments: r#"{"query":"needle"}"#.to_string(),
                            }],
                            tokio_util::sync::CancellationToken::new(),
                        )
                        .await
                });
            }
            while let Some(result) = joins.join_next().await {
                let batch = result.unwrap();
                assert_eq!(batch.len(), 1);
                assert!(batch[0].ok);
            }
        });
        let counters = engine.snapshot_counters();
        assert_eq!(counters.search_scans, 1);
    }

    #[test]
    fn concurrent_duplicate_tool_calls_in_one_turn_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let (snapshot, _) = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let engine = ToolEngine::new(snapshot, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let results = runtime.block_on(engine.execute_batch(
            test_scope("session"),
            TurnId(0),
            vec![
                ModelToolCall {
                    call_id: ToolCallId("read-diff-1".to_string()),
                    index: 0,
                    name: ToolId::from(ToolName::ReadDiff),
                    raw_arguments: "{}".to_string(),
                },
                ModelToolCall {
                    call_id: ToolCallId("read-diff-2".to_string()),
                    index: 1,
                    name: ToolId::from(ToolName::ReadDiff),
                    raw_arguments: "{}".to_string(),
                },
            ],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert_eq!(results.len(), 2);
        assert!(results[0].ok);
        assert!(!results[1].ok);
        assert_eq!(
            results[1].error.as_ref().unwrap().code,
            ToolErrorCode::InvalidArgs
        );
        let metrics = engine.snapshot_tool_metrics();
        let read_diff_metrics = &metrics[&ToolId::from(ToolName::ReadDiff)];
        assert_eq!(read_diff_metrics.successes, 1);
        assert_eq!(read_diff_metrics.errors, 1);
    }

    #[test]
    fn concurrent_tool_invalid_args_and_path_denied_are_reported() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let (snapshot, _) = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let engine = ToolEngine::new(snapshot, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let results = runtime.block_on(engine.execute_batch(
            test_scope("session"),
            TurnId(0),
            vec![
                ModelToolCall {
                    call_id: ToolCallId("invalid-args".to_string()),
                    index: 0,
                    name: ToolId::from(ToolName::ReadFile),
                    raw_arguments: "{}".to_string(),
                },
                ModelToolCall {
                    call_id: ToolCallId("path-denied".to_string()),
                    index: 1,
                    name: ToolId::from(ToolName::ReadFile),
                    raw_arguments: serde_json::json!({ "path": "missing.md" }).to_string(),
                },
            ],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert_eq!(results.len(), 2);
        assert!(!results[0].ok);
        assert_eq!(
            results[0].error.as_ref().unwrap().code,
            ToolErrorCode::InvalidArgs
        );
        assert!(!results[1].ok);
        assert_eq!(
            results[1].error.as_ref().unwrap().code,
            ToolErrorCode::PathDenied
        );
        assert_eq!(engine.snapshot_counters().tool_errors, 2);
        let metrics = engine.snapshot_tool_metrics();
        let read_file_metrics = &metrics[&ToolId::from(ToolName::ReadFile)];
        assert_eq!(read_file_metrics.calls, 2);
        assert_eq!(read_file_metrics.errors, 2);
    }

    #[test]
    fn concurrent_search_cache_is_scoped_by_filesystem_scope() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir(temp.path().join("src")).unwrap();
        fs::write(temp.path().join("README.md"), "needle in root\n").unwrap();
        fs::write(temp.path().join("src/lib.rs"), "needle in src\n").unwrap();
        let change = test_change_with_file("src/lib.rs");
        let policy = PathPolicyV1::bench(64, 20);
        let (snapshot, _) = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(2, 64 * 1024, 20));
        let engine = ToolEngine::new(snapshot, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let root = runtime.block_on(engine.execute_batch(
            test_scope("root-session"),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("root-search".to_string()),
                index: 0,
                name: ToolId::from(ToolName::SearchText),
                raw_arguments: r#"{"query":"needle"}"#.to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert!(root[0].ok);
        assert_eq!(root[0].limits.searched_files, 2);

        let mut scoped_capabilities = CapabilitySet::review_read_only();
        scoped_capabilities.fs_scope = FsScope::subtree(RepoPath::parse("src").unwrap());
        let scoped = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("src-session", scoped_capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("src-search".to_string()),
                index: 0,
                name: ToolId::from(ToolName::SearchText),
                raw_arguments: r#"{"query":"needle"}"#.to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert!(scoped[0].ok);
        assert_eq!(scoped[0].limits.searched_files, 1);
        let matches = scoped[0].data.as_ref().unwrap()["matches"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert!(matches.iter().all(|line| line.starts_with("src/lib.rs:")));

        let counters = engine.snapshot_counters();
        assert_eq!(counters.search_scans, 2);
        let metrics = engine.snapshot_tool_metrics();
        let search_metrics = &metrics[&ToolId::from(ToolName::SearchText)];
        assert_eq!(search_metrics.calls, 2);
        assert_eq!(search_metrics.successes, 2);
    }

    #[test]
    fn concurrent_registry_executes_allowed_custom_tool() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let (snapshot, _) = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 10));
        let tool_id = ToolId::parse("host_custom_check").unwrap();
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_custom(
                tool_id.clone(),
                "Host engine supplied custom reviewer check.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" }
                    },
                    "required": ["value"],
                    "additionalProperties": false
                }),
                false,
                Arc::new(EchoCustomTool),
            )
            .unwrap();
        let engine = ToolEngine::with_registry(snapshot, limits, Arc::new(registry)).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let denied = runtime.block_on(engine.execute_batch(
            test_scope("session"),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("denied-custom".to_string()),
                index: 0,
                name: tool_id.clone(),
                raw_arguments: r#"{"value":"ok"}"#.to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert_eq!(denied.len(), 1);
        assert!(!denied[0].ok);
        assert_eq!(
            denied[0].error.as_ref().unwrap().code,
            ToolErrorCode::ToolNotAllowed
        );

        let mut allowed_capabilities = CapabilitySet::review_read_only();
        allowed_capabilities.grant_tool(tool_id.clone(), ToolGrant::allow_custom_read_only());
        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", allowed_capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("custom".to_string()),
                index: 0,
                name: tool_id.clone(),
                raw_arguments: r#"{"value":"ok"}"#.to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert_eq!(results.len(), 1);
        assert!(results[0].ok);
        assert_eq!(results[0].tool_name, tool_id);
        assert!(results[0].artifact_id.is_some());
        let data = results[0].data.as_ref().unwrap().to_string();
        assert!(data.contains("[REDACTED]"));
        assert!(!data.contains("AKIA1234567890ABCDEF"));
        let metrics = engine.snapshot_tool_metrics();
        let custom_metrics = &metrics[&tool_id];
        assert_eq!(custom_metrics.calls, 2);
        assert_eq!(custom_metrics.successes, 1);
        assert_eq!(custom_metrics.errors, 1);
    }

    #[test]
    fn concurrent_review_defaults_register_all_sync_builtin_tools() {
        let registry = ToolRegistry::review_defaults().unwrap();
        let schemas = registry.schemas();
        for tool in all_builtin_tools() {
            let id = ToolId::from(tool);
            assert!(
                registry.definition(&id).is_some(),
                "missing concurrent registry definition for {}",
                tool.as_str()
            );
            assert!(
                schemas.iter().any(|schema| schema.id == id),
                "missing concurrent model schema for {}",
                tool.as_str()
            );
        }
    }

    #[test]
    fn concurrent_job_bridge_preserves_sync_repo_root_scope() {
        let capabilities = capabilities_from_mask(ToolMask::review_read_only());
        assert!(capabilities.fs_scope.cwd.is_none());
        assert!(capabilities.fs_scope.allowed_roots.is_empty());
        assert!(capabilities
            .fs_scope
            .allows(&RepoPath::parse("README.md").unwrap()));
        assert!(capabilities
            .fs_scope
            .allows(&RepoPath::parse("src/lib.rs").unwrap()));
        for tool in all_builtin_tools() {
            assert!(
                capabilities.allows_tool(&ToolId::from(tool)),
                "missing job-bridge capability for {}",
                tool.as_str()
            );
        }
    }

    #[test]
    fn concurrent_executes_every_sync_builtin_tool() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir(temp.path().join("src")).unwrap();
        fs::write(
            temp.path().join("README.md"),
            "use docs::needle;\nimport example from 'example';\nneedle\n",
        )
        .unwrap();
        fs::write(temp.path().join("src/lib.rs"), "pub fn needle() {}\n").unwrap();
        fs::write(
            temp.path().join("src/lib_test.rs"),
            "use crate::lib;\n#[test] fn needle_test() {}\n",
        )
        .unwrap();
        let change = test_change_with_file("src/lib.rs");
        let policy = PathPolicyV1::bench(64, 20);
        let (snapshot, _) = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let engine = ToolEngine::new(snapshot, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        for (index, tool) in all_builtin_tools().into_iter().enumerate() {
            let results = runtime.block_on(engine.execute_batch(
                test_scope("session"),
                TurnId(index as u32),
                vec![ModelToolCall {
                    call_id: ToolCallId(format!("call-{}", tool.as_str())),
                    index: 0,
                    name: ToolId::from(tool),
                    raw_arguments: builtin_args(tool),
                }],
                tokio_util::sync::CancellationToken::new(),
            ));
            assert_eq!(results.len(), 1, "unexpected result count for {tool:?}");
            assert_eq!(results[0].tool_name, ToolId::from(tool));
            assert!(
                results[0].ok,
                "concurrent builtin {} failed: {:?}",
                tool.as_str(),
                results[0].error
            );
        }
    }

    #[test]
    fn concurrent_runtime_emits_lifecycle_events() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let (snapshot, _) = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let output = Arc::new(std::sync::Mutex::new(Vec::new()));
        let emitter = Arc::new(EventEmitter {
            run_id: "run-1".to_string(),
            attempt: 0,
            redaction_policy_id: "test-redaction".to_string(),
            state: std::sync::Mutex::new(EventEmitterState {
                seq: 0,
                writer: Box::new(SharedWriter(Arc::clone(&output))),
            }),
        });
        let runtime = ConcurrentJobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(MockReviewModel::new(
                "README.md".to_string(),
                "needle".to_string(),
            )))),
            tools,
            limits,
            review_revision_id: change.head_revision_id.clone(),
            emitter: Some(emitter),
        };
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(runtime.run_sessions(vec![ConcurrentSessionSpec {
            scope: test_scope("session"),
        }]));
        assert_eq!(report.completed_sessions, 1);

        let bytes = output.lock().unwrap().clone();
        let text = String::from_utf8(bytes).unwrap();
        let events = text
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let event_types = events
            .iter()
            .map(|event| event["eventType"].as_str().unwrap())
            .collect::<Vec<_>>();
        for expected in [
            "session_started",
            "model_call_started",
            "model_call_completed",
            "tool_call_requested",
            "artifact_recorded",
            "finding_validated",
            "session_finished",
        ] {
            assert!(
                event_types.contains(&expected),
                "missing {expected} in {event_types:?}"
            );
        }
        assert!(events
            .iter()
            .any(|event| event["findingId"].as_str().is_some()));
        let completed_tool_call_ids = events
            .iter()
            .filter(|event| event["eventType"].as_str() == Some("tool_call_completed"))
            .filter_map(|event| event["toolCallId"].as_str())
            .collect::<std::collections::HashSet<_>>();
        for artifact_event in events
            .iter()
            .filter(|event| event["eventType"].as_str() == Some("artifact_recorded"))
        {
            let tool_call_id = artifact_event["toolCallId"].as_str().unwrap();
            assert!(
                completed_tool_call_ids.contains(tool_call_id),
                "artifact event {tool_call_id} missing matching tool_call_completed"
            );
        }
    }

    #[test]
    fn concurrent_runtime_rejects_terminal_before_evidence() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let (snapshot, _) = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let runtime = ConcurrentJobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(PrematureTerminalModel))),
            tools,
            limits,
            review_revision_id: change.head_revision_id.clone(),
            emitter: None,
        };
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(runtime.run_sessions(vec![ConcurrentSessionSpec {
            scope: test_scope("session"),
        }]));
        assert_eq!(report.findings, 0);
        assert_eq!(report.tool_counts.record_finding, 0);
        assert!(report.counters.tool_errors > 0);
        assert!(!report.benchmark_valid);
        assert!(report
            .benchmark_failures
            .iter()
            .any(|failure| failure.contains("read_diff")));
    }

    #[test]
    fn concurrent_runtime_reports_max_turn_exhaustion_without_terminal() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let (snapshot, _) = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let runtime = ConcurrentJobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(EvidenceOnlyModel))),
            tools,
            limits,
            review_revision_id: change.head_revision_id.clone(),
            emitter: None,
        };
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(runtime.run_sessions(vec![ConcurrentSessionSpec {
            scope: test_scope_with_budget("session", 1, 8),
        }]));
        assert_eq!(report.completed_sessions, 0);
        assert_eq!(report.model_calls, 1);
        assert_eq!(report.tool_counts.read_diff, 1);
        assert_eq!(report.tool_counts.read_file, 1);
        assert_eq!(report.tool_counts.search_text, 1);
        assert_eq!(report.findings, 0);
        assert!(!report.benchmark_valid);
        assert!(report
            .benchmark_failures
            .iter()
            .any(|failure| failure.contains("only 0/1 sessions completed")));
    }

    #[test]
    fn concurrent_runtime_enforces_session_tool_budget_with_batched_calls() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let (snapshot, _) = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let runtime = ConcurrentJobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(EvidenceOnlyModel))),
            tools,
            limits,
            review_revision_id: change.head_revision_id.clone(),
            emitter: None,
        };
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(runtime.run_sessions(vec![ConcurrentSessionSpec {
            scope: test_scope_with_budget("session", 4, 2),
        }]));
        assert_eq!(report.completed_sessions, 0);
        assert_eq!(report.tool_calls, 2);
        assert_eq!(report.tool_counts.read_diff, 1);
        assert_eq!(report.tool_counts.read_file, 1);
        assert_eq!(report.tool_counts.search_text, 0);
        assert_eq!(report.counters.tool_errors, 1);
        let search_metrics = &report.tool_metrics[&ToolId::from(ToolName::SearchText)];
        assert_eq!(search_metrics.calls, 1);
        assert_eq!(search_metrics.errors, 1);
        assert!(!report.benchmark_valid);
    }

    #[test]
    fn concurrent_runtime_reports_cancelled_sessions() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let (snapshot, _) = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let runtime = ConcurrentJobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(MockReviewModel::new(
                "README.md".to_string(),
                "needle".to_string(),
            )))),
            tools,
            limits,
            review_revision_id: change.head_revision_id.clone(),
            emitter: None,
        };
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(runtime.run_sessions_with_cancel(
            vec![ConcurrentSessionSpec {
                scope: test_scope("session"),
            }],
            cancel,
        ));
        assert_eq!(report.completed_sessions, 0);
        assert_eq!(report.model_calls, 0);
        assert_eq!(report.tool_calls, 0);
        assert_eq!(report.findings, 0);
        assert!(!report.benchmark_valid);
    }

    #[test]
    fn concurrent_runtime_retries_retryable_provider_error() {
        let report = run_with_model(Arc::new(FailThenMockModel::new(
            1,
            ModelFailure::RetryableProvider,
        )));
        assert_eq!(report.completed_sessions, 1);
        assert_eq!(report.findings, 1);
        assert_eq!(
            report.model_calls, 3,
            "first turn should retry once, then terminal turn should run once"
        );
        assert!(report.benchmark_valid);
    }

    #[test]
    fn concurrent_runtime_retries_timeout() {
        let report = run_with_model(Arc::new(FailThenMockModel::new(1, ModelFailure::Timeout)));
        assert_eq!(report.completed_sessions, 1);
        assert_eq!(report.findings, 1);
        assert_eq!(report.model_calls, 3);
        assert!(report.benchmark_valid);
    }

    #[test]
    fn concurrent_runtime_does_not_retry_non_retryable_provider_error() {
        let report = run_with_model(Arc::new(FailThenMockModel::new(
            1,
            ModelFailure::NonRetryableProvider,
        )));
        assert_eq!(report.completed_sessions, 0);
        assert_eq!(report.model_calls, 1);
        assert_eq!(report.tool_calls, 0);
        assert_eq!(report.findings, 0);
        assert!(!report.benchmark_valid);
    }

    #[test]
    fn concurrent_runtime_marks_provider_error_session_failed() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let (snapshot, _) = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let output = Arc::new(std::sync::Mutex::new(Vec::new()));
        let emitter = Arc::new(EventEmitter {
            run_id: "run-1".to_string(),
            attempt: 0,
            redaction_policy_id: "test-redaction".to_string(),
            state: std::sync::Mutex::new(EventEmitterState {
                seq: 0,
                writer: Box::new(SharedWriter(Arc::clone(&output))),
            }),
        });
        let runtime = ConcurrentJobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(FailThenMockModel::new(
                1,
                ModelFailure::NonRetryableProvider,
            )))),
            tools,
            limits,
            review_revision_id: change.head_revision_id.clone(),
            emitter: Some(emitter),
        };
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(runtime.run_sessions(vec![ConcurrentSessionSpec {
            scope: test_scope("session"),
        }]));
        assert_eq!(report.completed_sessions, 0);

        let bytes = output.lock().unwrap().clone();
        let text = String::from_utf8(bytes).unwrap();
        let events = text
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let session_finished = events
            .iter()
            .find(|event| event["eventType"].as_str() == Some("session_finished"))
            .expect("missing session_finished");
        assert_eq!(session_finished["payload"]["state"], "failed");
        assert!(events.iter().any(|event| {
            event["eventType"].as_str() == Some("error")
                && event["payload"]["retrying"].as_bool() == Some(false)
        }));
    }

    #[derive(Debug)]
    struct EchoCustomTool;

    #[derive(Debug)]
    struct PrematureTerminalModel;

    #[derive(Debug)]
    struct EvidenceOnlyModel;

    #[derive(Debug)]
    struct FailThenMockModel {
        failures_left: AtomicUsize,
        failure: ModelFailure,
        inner: MockReviewModel,
    }

    impl FailThenMockModel {
        fn new(failures: usize, failure: ModelFailure) -> Self {
            Self {
                failures_left: AtomicUsize::new(failures),
                failure,
                inner: MockReviewModel::new("README.md".to_string(), "needle".to_string()),
            }
        }
    }

    #[derive(Debug, Copy, Clone)]
    enum ModelFailure {
        RetryableProvider,
        NonRetryableProvider,
        Timeout,
    }

    impl ModelFailure {
        fn error(self) -> RuntimeError {
            match self {
                Self::RetryableProvider => RuntimeError::Provider {
                    status: Some(429),
                    retryable: true,
                },
                Self::NonRetryableProvider => RuntimeError::Provider {
                    status: Some(400),
                    retryable: false,
                },
                Self::Timeout => RuntimeError::Timeout,
            }
        }
    }

    #[async_trait]
    impl ConcurrentModelClient for FailThenMockModel {
        async fn complete(
            &self,
            scope: &SessionScope,
            transcript: &[ConversationItem],
            turn_id: TurnId,
            cancel: tokio_util::sync::CancellationToken,
        ) -> RuntimeResult<ModelTurn> {
            if self
                .failures_left
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    if left > 0 {
                        Some(left - 1)
                    } else {
                        None
                    }
                })
                .is_ok()
            {
                return Err(self.failure.error());
            }
            self.inner
                .complete(scope, transcript, turn_id, cancel)
                .await
        }
    }

    #[async_trait]
    impl ConcurrentModelClient for EvidenceOnlyModel {
        async fn complete(
            &self,
            scope: &SessionScope,
            _transcript: &[ConversationItem],
            turn_id: TurnId,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> RuntimeResult<ModelTurn> {
            let session_id = &scope.id;
            Ok(ModelTurn::ToolCalls {
                usage: TokenUsage {
                    input_tokens: 1,
                    output_tokens: 1,
                    total_tokens: 2,
                },
                calls: vec![
                    ModelToolCall {
                        call_id: ToolCallId(format!("{}-{}-diff", session_id.0, turn_id.0)),
                        index: 0,
                        name: ToolId::from(ToolName::ReadDiff),
                        raw_arguments: "{}".to_string(),
                    },
                    ModelToolCall {
                        call_id: ToolCallId(format!("{}-{}-file", session_id.0, turn_id.0)),
                        index: 1,
                        name: ToolId::from(ToolName::ReadFile),
                        raw_arguments: serde_json::json!({ "path": "README.md" }).to_string(),
                    },
                    ModelToolCall {
                        call_id: ToolCallId(format!("{}-{}-search", session_id.0, turn_id.0)),
                        index: 2,
                        name: ToolId::from(ToolName::SearchText),
                        raw_arguments: serde_json::json!({ "query": "needle" }).to_string(),
                    },
                ],
            })
        }
    }

    #[async_trait]
    impl ConcurrentModelClient for PrematureTerminalModel {
        async fn complete(
            &self,
            scope: &SessionScope,
            _transcript: &[ConversationItem],
            turn_id: TurnId,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> RuntimeResult<ModelTurn> {
            let session_id = &scope.id;
            Ok(ModelTurn::ToolCalls {
                usage: TokenUsage {
                    input_tokens: 1,
                    output_tokens: 1,
                    total_tokens: 2,
                },
                calls: vec![ModelToolCall {
                    call_id: ToolCallId(format!("{}-{}-finding", session_id.0, turn_id.0)),
                    index: 0,
                    name: ToolId::from(ToolName::RecordFinding),
                    raw_arguments: serde_json::json!({
                        "title": "premature finding",
                        "claim": "terminal call before evidence"
                    })
                    .to_string(),
                }],
            })
        }
    }

    #[derive(Clone)]
    struct SharedWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl CustomToolHandler for EchoCustomTool {
        async fn execute(
            &self,
            context: CustomToolContext,
            args: serde_json::Value,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> crate::concurrent::contracts::RuntimeResult<CustomToolOutput> {
            Ok(CustomToolOutput {
                data: Some(serde_json::json!({
                    "tool": context.tool_id.as_str(),
                    "session": context.session_id.0,
                    "value": args["value"],
                    "secret": "AKIA1234567890ABCDEF"
                })),
                artifact: Some(CustomToolArtifact {
                    key: ArtifactKey("host_custom_check".to_string()),
                    content: "artifact AKIA1234567890ABCDEF".to_string(),
                }),
                limits: LimitInfo::default(),
            })
        }
    }

    fn test_scope(id: &str) -> SessionScope {
        test_scope_with_capabilities(id, CapabilitySet::review_read_only())
    }

    fn run_with_model(model: Arc<dyn ConcurrentModelClient>) -> ConcurrentRunReport {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let (snapshot, _) = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let runtime = ConcurrentJobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(model)),
            tools,
            limits,
            review_revision_id: change.head_revision_id.clone(),
            emitter: None,
        };
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio.block_on(runtime.run_sessions(vec![ConcurrentSessionSpec {
            scope: test_scope("session"),
        }]))
    }

    fn test_scope_with_budget(id: &str, max_turns: usize, max_tool_calls: usize) -> SessionScope {
        let mut scope = test_scope(id);
        scope.budget.max_turns = max_turns;
        scope.budget.max_tool_calls = max_tool_calls;
        scope
    }

    fn all_builtin_tools() -> [ToolName; 13] {
        [
            ToolName::ListChangedFiles,
            ToolName::ReadDiff,
            ToolName::ListFiles,
            ToolName::ReadFile,
            ToolName::ReadBaseFile,
            ToolName::ReadHeadFile,
            ToolName::SearchText,
            ToolName::FindRelatedFiles,
            ToolName::FindTestsForFile,
            ToolName::ListImports,
            ToolName::RecordFinding,
            ToolName::ChallengeFinding,
            ToolName::Finish,
        ]
    }

    fn builtin_args(tool: ToolName) -> String {
        match tool {
            ToolName::ListChangedFiles | ToolName::ReadDiff | ToolName::ListFiles => {
                "{}".to_string()
            }
            ToolName::ReadFile | ToolName::ReadBaseFile | ToolName::ReadHeadFile => {
                serde_json::json!({ "path": "README.md" }).to_string()
            }
            ToolName::SearchText => serde_json::json!({ "query": "needle" }).to_string(),
            ToolName::FindRelatedFiles | ToolName::FindTestsForFile | ToolName::ListImports => {
                serde_json::json!({ "path": "src/lib.rs" }).to_string()
            }
            ToolName::RecordFinding => serde_json::json!({
                "title": "benchmark finding",
                "claim": "claim"
            })
            .to_string(),
            ToolName::ChallengeFinding => serde_json::json!({
                "finding_id": "finding-1",
                "rationale": "challenge"
            })
            .to_string(),
            ToolName::Finish => serde_json::json!({ "reason": "done" }).to_string(),
        }
    }

    fn test_scope_with_capabilities(id: &str, capabilities: CapabilitySet) -> SessionScope {
        SessionScope {
            id: SessionId(id.to_string()),
            role: Role::Generalist,
            objective: "test review scope".to_string(),
            model_profile_id: Some("test-model".to_string()),
            capabilities,
            budget: AgentBudget {
                max_turns: 4,
                max_tool_calls: 8,
                max_prompt_tokens: 32_000,
                max_output_tokens: 512,
            },
        }
    }

    fn bench_args(repo: &Path, terminal_policy: BenchTerminalPolicy) -> BenchArgs {
        BenchArgs {
            repo: repo.to_path_buf(),
            sessions: 3,
            max_active: 3,
            max_turns: 10,
            max_tool_calls: 14,
            hold_ms: 0,
            max_file_kb: 200,
            max_search_matches: 120,
            model: DEFAULT_MODEL.to_string(),
            max_output_tokens: 128,
            terminal_policy,
        }
    }

    fn test_repo(path: &Path) -> RepoContext {
        RepoContext::new(
            path.to_path_buf(),
            PathPolicyV1::bench(64, 10),
            ChangeScopeV1 {
                kind: ChangeKind::LocalDiff,
                change_id: "test".to_string(),
                source_ref: "head".to_string(),
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
        )
        .unwrap()
    }

    fn test_change_with_file(path: &str) -> ChangeScopeV1 {
        ChangeScopeV1 {
            kind: ChangeKind::LocalDiff,
            change_id: "test".to_string(),
            source_ref: "head".to_string(),
            target_ref: "base".to_string(),
            base_revision_id: "base".to_string(),
            head_revision_id: "head".to_string(),
            merge_base_revision_id: None,
            changed_files_manifest_ref: None,
            diff_manifest_ref: None,
            snapshot_mode: SnapshotMode::WorktreeHead,
            rename_detection: RenameDetection::None,
            changed_files: vec![ChangedFileEntryV1 {
                status: ChangedFileStatus::Modified,
                old_path: Some(PathBuf::from(path)),
                new_path: Some(PathBuf::from(path)),
                old_content_hash: None,
                new_content_hash: None,
                is_binary: false,
                is_generated: false,
            }],
        }
    }
}
