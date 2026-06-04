use tokio_util::sync::CancellationToken;

use crate::runtime::contracts::*;
use crate::runtime::dispatch::RuntimeEventDispatcher;
use crate::runtime::policy::ReviewerPolicy;
use crate::runtime::tools::ToolEngine;

pub(crate) struct ToolBatchRunner<'a> {
    policy: &'a ReviewerPolicy,
    tools: &'a ToolEngine,
    events: &'a RuntimeEventDispatcher,
}

impl<'a> ToolBatchRunner<'a> {
    pub(crate) fn new(
        policy: &'a ReviewerPolicy,
        tools: &'a ToolEngine,
        events: &'a RuntimeEventDispatcher,
    ) -> Self {
        Self {
            policy,
            tools,
            events,
        }
    }

    pub(crate) async fn execute(
        &self,
        scope: SessionScope,
        turn_id: TurnId,
        calls: Vec<ModelToolCall>,
        evidence_ready: bool,
        remaining_tool_calls: usize,
        cancel: CancellationToken,
    ) -> Vec<ToolResultEnvelope> {
        let plan = self
            .policy
            .plan_tool_batch(calls, evidence_ready, remaining_tool_calls);
        if let Some(planned) =
            self.policy
                .plan_tool_batch_started_runtime_event(&scope, turn_id, plan.scheduled_count)
        {
            self.events.emit_planned_runtime(planned);
        }
        let mut indexed_results = Vec::new();
        for denied in plan.denied_calls {
            let result = self.tools.error_result(
                denied.call_id,
                denied.tool_id,
                denied.denial.code,
                denied.denial.message,
                denied.denial.retryable,
            );
            self.tools
                .record_tool_metrics(std::slice::from_ref(&result));
            indexed_results.push((denied.index, result));
        }

        if !plan.allowed_calls.is_empty() {
            let allowed_indices = plan
                .allowed_calls
                .iter()
                .map(|call| call.index)
                .collect::<Vec<_>>();
            let allowed_results = self
                .tools
                .execute_batch(scope, turn_id, plan.allowed_calls, cancel)
                .await;
            for (index, result) in allowed_indices.into_iter().zip(allowed_results) {
                indexed_results.push((index, result));
            }
        }
        indexed_results.sort_by_key(|(index, _)| *index);
        indexed_results
            .into_iter()
            .map(|(_, result)| result)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::contracts::{
        AgentBudget, ChangeKind, ChangeScopeV1, ChangedFileEntryV1, ChangedFileStatus,
        PathPolicyV1, RenameDetection, Role, SnapshotMode, ToolName,
    };
    use crate::runtime::repo::RepoSnapshot;

    #[derive(Default)]
    struct RecordingRuntimeSink {
        records: Mutex<Vec<(RuntimeEventContext, RuntimeEvent)>>,
    }

    impl RuntimeEventSink for RecordingRuntimeSink {
        fn emit(&self, event: RuntimeEvent) {
            self.emit_with_context(RuntimeEventContext::from_event(&event), event);
        }

        fn emit_with_context(&self, context: RuntimeEventContext, event: RuntimeEvent) {
            self.records
                .lock()
                .expect("sink lock")
                .push((context, event));
        }
    }

    #[tokio::test]
    async fn tool_batch_runner_applies_policy_denials_and_preserves_model_order() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(temp.path().join("README.md"), "needle\n").expect("write readme");
        let change = test_change_with_file("README.md");
        let snapshot = RepoSnapshot::build(temp.path(), &PathPolicyV1::bench(64, 20), &change)
            .expect("snapshot");
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = ToolEngine::new(Arc::clone(&snapshot), limits).expect("tool engine");
        let policy = ReviewerPolicy::new();
        let runtime_sink = Arc::new(RecordingRuntimeSink::default());
        let sink: Arc<dyn RuntimeEventSink> = runtime_sink.clone();
        let dispatcher = RuntimeEventDispatcher::new(Some(sink), None);
        let runner = ToolBatchRunner::new(&policy, &tools, &dispatcher);
        let scope = test_scope("tool-batch-session");

        let results = runner
            .execute(
                scope.clone(),
                TurnId(3),
                vec![
                    model_call("finding", 0, ToolName::RecordFinding, "{}"),
                    model_call("finish", 1, ToolName::Finish, "{}"),
                    model_call("diff", 2, ToolName::ReadDiff, "{}"),
                ],
                false,
                usize::MAX,
                CancellationToken::new(),
            )
            .await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].tool_call_id, ToolCallId("finding".to_string()));
        assert_eq!(
            results[0].error.as_ref().expect("denial").code,
            ToolErrorCode::ToolNotAllowed
        );
        assert_eq!(results[1].tool_call_id, ToolCallId("finish".to_string()));
        assert_eq!(
            results[1].error.as_ref().expect("denial").code,
            ToolErrorCode::ToolNotAllowed
        );
        assert_eq!(results[2].tool_call_id, ToolCallId("diff".to_string()));
        assert!(results[2].ok);
        let records = runtime_sink.records.lock().expect("sink lock");
        assert!(records.iter().any(|(context, event)| {
            context.session_id.as_ref() == Some(&scope.id)
                && context.turn_id == Some(TurnId(3))
                && matches!(event, RuntimeEvent::ToolBatchStarted { count: 3, .. })
        }));
    }

    fn model_call(id: &str, index: usize, tool: ToolName, raw_arguments: &str) -> ModelToolCall {
        ModelToolCall {
            call_id: ToolCallId(id.to_string()),
            index,
            name: ToolId::from(tool),
            raw_arguments: raw_arguments.to_string(),
        }
    }

    fn test_scope(id: &str) -> SessionScope {
        SessionScope::review_read_only(
            SessionId(id.to_string()),
            Role::Generalist,
            "tool batch runner test",
            AgentBudget {
                max_turns: 4,
                max_tool_calls: 8,
                max_prompt_tokens: 32_000,
                max_output_tokens: 512,
            },
        )
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
