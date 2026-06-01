use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::concurrent::contracts::{
    ArtifactKey, CapabilitySet, FsScope, LimitInfo, ModelToolCall, RepoPath, RuntimeLimits,
    SessionId, SessionScope, ToolCallId, ToolErrorCode, ToolGrant, ToolId, TurnId,
};
use crate::concurrent::repo::RepoSnapshot;
use crate::concurrent::tool_registry::{
    CustomToolArtifact, CustomToolContext, CustomToolHandler, CustomToolOutput, ToolRegistry,
};
use crate::concurrent::tools::ToolEngine;
use crate::contracts::*;
use crate::repo::RepoContext;
use crate::runtime::{benchmark_failures, RuntimeReport};
use crate::util::DEFAULT_MODEL;
use async_trait::async_trait;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_stores_artifact_refs_not_content() {
        let mut session = AgentSession {
            id: "session-1".to_string(),
            run_id: "run-1".to_string(),
            role: Role::Security,
            objective: "test".to_string(),
            cwd: PathBuf::from("."),
            model_profile_id: "bench-oai".to_string(),
            state: AgentState::Ready,
            budget: AgentBudget {
                max_turns: 2,
                max_tool_calls: 2,
                max_prompt_tokens: 100,
                max_output_tokens: 100,
            },
            allowed_tools: ToolMask::review_read_only(),
            events: Vec::new(),
        };

        session.events.push(AgentEvent::ToolResult {
            tool_call_id: "tool-1".to_string(),
            tool: ToolName::ReadFile,
            artifact_id: ArtifactId(42),
            summary: "read 10 lines".to_string(),
            completeness: Completeness::Complete,
        });

        let serialized = serde_json::to_string(&session).unwrap();
        assert!(serialized.contains("artifact_id"));
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
        let mut report = RuntimeReport {
            model: DEFAULT_MODEL.to_string(),
            sessions: 10,
            completed_sessions: 10,
            model_calls: 10,
            tool_calls: 0,
            tool_counts: ToolCounts::default(),
            findings: 0,
            publishable_findings: 0,
            blackboard_entries: 0,
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
            elapsed_ms: 0,
            artifact_stats: ArtifactStats::default(),
            benchmark_valid: false,
            benchmark_failures: Vec::new(),
        };
        report.benchmark_failures = benchmark_failures(&report);
        assert!(report
            .benchmark_failures
            .iter()
            .any(|failure| failure.contains("read_file")));
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

    #[derive(Debug)]
    struct EchoCustomTool;

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
