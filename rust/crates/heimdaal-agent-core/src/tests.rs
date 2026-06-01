use std::fs;
use std::path::{Path, PathBuf};

use crate::concurrent::contracts::{
    ModelToolCall, RepoPath, RuntimeLimits, SessionId, ToolCallId, TurnId,
};
use crate::concurrent::repo::RepoSnapshot;
use crate::concurrent::tools::ToolEngine;
use crate::contracts::*;
use crate::repo::RepoContext;
use crate::runtime::{benchmark_failures, RuntimeReport};
use crate::util::DEFAULT_MODEL;

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
            SessionId("session".to_string()),
            TurnId(0),
            vec![
                ModelToolCall {
                    call_id: ToolCallId("finish".to_string()),
                    index: 0,
                    name: ToolName::Finish,
                    raw_arguments: r#"{"reason":"done"}"#.to_string(),
                },
                ModelToolCall {
                    call_id: ToolCallId("read".to_string()),
                    index: 1,
                    name: ToolName::ReadDiff,
                    raw_arguments: "{}".to_string(),
                },
            ],
            ToolMask::review_read_only(),
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
                    engine
                        .execute_batch(
                            SessionId(format!("session-{index}")),
                            TurnId(0),
                            vec![ModelToolCall {
                                call_id: ToolCallId(format!("search-{index}")),
                                index: 0,
                                name: ToolName::SearchText,
                                raw_arguments: r#"{"query":"needle"}"#.to_string(),
                            }],
                            ToolMask::review_read_only(),
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
