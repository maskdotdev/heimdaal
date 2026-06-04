use crate::contracts::{ToolCounts, ToolName};
use crate::runtime::contracts::{ConversationItem, SessionScope, ToolResultEnvelope, TurnId};
use crate::runtime::dispatch::RuntimeEventDispatcher;
use crate::runtime::policy::{ReviewerPolicy, SessionEvidence, SessionTerminal};
use crate::runtime::tools::{count_tool_result, ToolEngine};

pub(crate) struct ToolResultEffectProcessor<'a> {
    policy: &'a ReviewerPolicy,
    tools: &'a ToolEngine,
    events: &'a RuntimeEventDispatcher,
    review_revision_id: &'a str,
}

impl<'a> ToolResultEffectProcessor<'a> {
    pub(crate) fn new(
        policy: &'a ReviewerPolicy,
        tools: &'a ToolEngine,
        events: &'a RuntimeEventDispatcher,
        review_revision_id: &'a str,
    ) -> Self {
        Self {
            policy,
            tools,
            events,
            review_revision_id,
        }
    }

    pub(crate) fn apply_batch(
        &self,
        scope: &SessionScope,
        turn_id: TurnId,
        results: Vec<ToolResultEnvelope>,
        mut state: ToolResultBatchState<'_>,
    ) -> ToolResultBatchOutcome {
        for result in &results {
            if result.ok {
                self.policy.observe_evidence_result(state.evidence, result);
            }
        }
        let terminal_seen = self.policy.observe_terminal_batch(state.terminal, &results);
        for result in results {
            self.apply_result(scope, turn_id, result, &mut state);
        }
        ToolResultBatchOutcome { terminal_seen }
    }

    fn apply_result(
        &self,
        scope: &SessionScope,
        turn_id: TurnId,
        result: ToolResultEnvelope,
        state: &mut ToolResultBatchState<'_>,
    ) {
        self.policy.observe_terminal_error(state.terminal, &result);
        let mut finding_id = None;
        let mut artifact = None;
        if result.ok && result.tool_name.as_builtin() == Some(ToolName::RecordFinding) {
            let recorded_finding_id = self.tools.record_finding_result(
                &scope.id,
                &result,
                state.evidence.results(),
                self.review_revision_id,
            );
            if let Some(recorded_finding_id) = recorded_finding_id {
                self.events
                    .emit_legacy(self.policy.plan_finding_validated_event(
                        scope,
                        &result,
                        &recorded_finding_id,
                    ));
                finding_id = Some(recorded_finding_id);
            }
        } else if let Some(artifact_id) = &result.artifact_id {
            if let Some(event) = self.policy.plan_artifact_recorded_event(scope, &result) {
                self.events.emit_legacy(event);
            }
            artifact = self.tools.artifacts.get(artifact_id);
            self.events
                .emit_legacy(self.policy.plan_tool_call_completed_event(scope, &result));
        } else {
            self.events
                .emit_legacy(self.policy.plan_tool_call_completed_event(scope, &result));
        }
        let runtime_events = self.policy.plan_tool_result_runtime_events(
            scope,
            turn_id,
            &result,
            artifact.as_ref(),
            finding_id.as_deref(),
        );
        for planned in runtime_events.events {
            self.events
                .emit_runtime_with_context(planned.context, planned.event);
        }
        count_tool_result(state.tool_counts, &result);
        state
            .transcript
            .push(self.policy.plan_tool_result_transcript_item(result));
    }
}

pub(crate) struct ToolResultBatchState<'a> {
    pub(crate) evidence: &'a mut SessionEvidence,
    pub(crate) terminal: &'a mut SessionTerminal,
    pub(crate) tool_counts: &'a mut ToolCounts,
    pub(crate) transcript: &'a mut Vec<ConversationItem>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct ToolResultBatchOutcome {
    pub(crate) terminal_seen: bool,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::contracts::{
        AgentBudget, ChangeKind, ChangeScopeV1, ChangedFileEntryV1, ChangedFileStatus,
        PathPolicyV1, RenameDetection, Role, SnapshotMode,
    };
    use crate::runtime::contracts::{
        CacheInfo, CacheStatus, CapabilitySet, LimitInfo, RuntimeEvent, RuntimeEventContext,
        RuntimeEventSink, RuntimeLimits, SessionId, SnapshotId, ToolCallId, ToolErrorCode,
        ToolErrorInfo, ToolId, ToolProviderId,
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

    #[test]
    fn processor_applies_tool_result_side_effects_in_batch_order() {
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
        let processor =
            ToolResultEffectProcessor::new(&policy, &tools, &dispatcher, &change.head_revision_id);
        let scope = test_scope("effects-session");
        let mut evidence = SessionEvidence::default();
        let mut terminal = SessionTerminal::default();
        let mut tool_counts = ToolCounts::default();
        let mut transcript = Vec::new();

        let outcome = processor.apply_batch(
            &scope,
            TurnId(2),
            vec![
                denied_result(ToolName::RecordFinding, "denied-record_finding-1"),
                denied_result(ToolName::RecordFinding, "denied-record_finding-2"),
                successful_search_result(&snapshot.snapshot_id),
            ],
            ToolResultBatchState {
                evidence: &mut evidence,
                terminal: &mut terminal,
                tool_counts: &mut tool_counts,
                transcript: &mut transcript,
            },
        );

        assert!(!outcome.terminal_seen);
        assert!(policy.should_fail_after_terminal_errors(&terminal));
        assert!(evidence.saw_search());
        assert_eq!(tool_counts.search_text, 1);
        assert_eq!(tool_counts.record_finding, 0);
        assert_eq!(transcript.len(), 3);
        assert!(matches!(
            transcript[0],
            ConversationItem::ToolResult { ref call_id, .. } if call_id.0 == "denied-record_finding-1"
        ));
        assert!(matches!(
            transcript[1],
            ConversationItem::ToolResult { ref call_id, .. } if call_id.0 == "denied-record_finding-2"
        ));
        assert!(matches!(
            transcript[2],
            ConversationItem::ToolResult { ref call_id, .. } if call_id.0 == "search"
        ));

        let records = runtime_sink.records.lock().expect("sink lock");
        assert_eq!(records.len(), 6);
        assert!(matches!(
            records[0].1,
            RuntimeEvent::ToolCallDenied { ref call_id, .. } if call_id.0 == "denied-record_finding-1"
        ));
        assert!(matches!(
            records[1].1,
            RuntimeEvent::ToolCallCompleted { ref call_id, ok: false, .. }
                if call_id.0 == "denied-record_finding-1"
        ));
        assert!(matches!(
            records[2].1,
            RuntimeEvent::ToolCallDenied { ref call_id, .. } if call_id.0 == "denied-record_finding-2"
        ));
        assert!(matches!(
            records[3].1,
            RuntimeEvent::ToolCallCompleted { ref call_id, ok: false, .. }
                if call_id.0 == "denied-record_finding-2"
        ));
        assert!(matches!(
            records[4].1,
            RuntimeEvent::ToolCallCompleted { ref call_id, ok: true, .. }
                if call_id.0 == "search"
        ));
        assert!(matches!(
            records[5].1,
            RuntimeEvent::SearchBatchCompleted {
                searched_files: 3,
                ..
            }
        ));
    }

    fn denied_result(tool: ToolName, call_id: &str) -> ToolResultEnvelope {
        ToolResultEnvelope {
            ok: false,
            tool_call_id: ToolCallId(call_id.to_string()),
            tool_name: ToolId::from(tool),
            provider_id: ToolProviderId::builtin_review(),
            snapshot_id: SnapshotId("snapshot".to_string()),
            artifact_id: None,
            cache: CacheInfo {
                status: CacheStatus::NotCacheable,
                key_hash: None,
            },
            limits: LimitInfo::default(),
            data: None,
            error: Some(ToolErrorInfo {
                code: ToolErrorCode::ToolNotAllowed,
                message: "denied".to_string(),
                retryable: false,
                partial: false,
            }),
        }
    }

    fn successful_search_result(snapshot_id: &SnapshotId) -> ToolResultEnvelope {
        ToolResultEnvelope {
            ok: true,
            tool_call_id: ToolCallId("search".to_string()),
            tool_name: ToolId::from(ToolName::SearchText),
            provider_id: ToolProviderId::builtin_review(),
            snapshot_id: snapshot_id.clone(),
            artifact_id: None,
            cache: CacheInfo {
                status: CacheStatus::NotCacheable,
                key_hash: None,
            },
            limits: LimitInfo {
                searched_files: 3,
                skipped_files: 1,
                bytes_scanned: 128,
                ..LimitInfo::default()
            },
            data: None,
            error: None,
        }
    }

    fn test_scope(id: &str) -> SessionScope {
        SessionScope {
            id: SessionId(id.to_string()),
            role: Role::Generalist,
            objective: "effects test".to_string(),
            snapshot_id: None,
            model_profile_id: Some("test-model".to_string()),
            capabilities: CapabilitySet::review_read_only(),
            budget: AgentBudget {
                max_turns: 4,
                max_tool_calls: 8,
                max_prompt_tokens: 32_000,
                max_output_tokens: 512,
            },
        }
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
