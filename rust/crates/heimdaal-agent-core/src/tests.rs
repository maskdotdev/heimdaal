use std::fs;
use std::path::{Path, PathBuf};

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
}
