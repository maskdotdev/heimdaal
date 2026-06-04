use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::contracts::{TokenUsage, ToolCounts};
use crate::events::EventRecord;
use crate::runtime::accounting::SessionModelAccounting;
use crate::runtime::contracts::*;
use crate::runtime::dispatch::RuntimeEventDispatcher;
use crate::runtime::effects::{ToolResultBatchState, ToolResultEffectProcessor};
use crate::runtime::flow::SessionFlow;
use crate::runtime::model::ConcurrentModelRouter;
use crate::runtime::model_turn::ModelTurnRunner;
use crate::runtime::policy::{
    PlannedRuntimeEvent, ReviewerPolicy, SessionEvidence, SessionTerminal,
};
use crate::runtime::repo::RepoSnapshot;
use crate::runtime::tool_batch::ToolBatchRunner;
use crate::runtime::tools::ToolEngine;

#[derive(Clone)]
pub(crate) struct SessionRunner {
    snapshot: Arc<RepoSnapshot>,
    model_router: Arc<dyn ConcurrentModelRouter>,
    tools: Arc<ToolEngine>,
    policy: Arc<ReviewerPolicy>,
    review_revision_id: String,
    events: RuntimeEventDispatcher,
}

impl SessionRunner {
    pub(crate) fn new(
        snapshot: Arc<RepoSnapshot>,
        model_router: Arc<dyn ConcurrentModelRouter>,
        tools: Arc<ToolEngine>,
        policy: Arc<ReviewerPolicy>,
        review_revision_id: String,
        events: RuntimeEventDispatcher,
    ) -> Self {
        Self {
            snapshot,
            model_router,
            tools,
            policy,
            review_revision_id,
            events,
        }
    }

    pub(crate) fn empty_report(
        &self,
        scope: &SessionScope,
        terminal_reason: Option<String>,
    ) -> SessionReport {
        SessionReport {
            completed: false,
            model_calls: 0,
            model_metrics: ModelMetricsSnapshot::default(),
            tool_counts: ToolCounts::default(),
            tokens: TokenUsage::default(),
            terminal_diagnostic: self.policy.empty_session_terminal_diagnostic(
                scope,
                false,
                terminal_reason,
            ),
        }
    }

    pub(crate) async fn run_scope(
        &self,
        scope: SessionScope,
        cancel: CancellationToken,
    ) -> SessionReport {
        let mut flow = SessionFlow::default();
        self.emit_session_started(&scope);
        if flow.cancel_before_model(cancel.is_cancelled()) {
            self.emit_session_finished(&scope, "cancelled", ToolCounts::default(), 0);
            return self.empty_report(&scope, Some("cancelled before model call".to_string()));
        }
        let model = match self.model_router.client_for(&scope).await {
            Ok(model) => model,
            Err(error) => {
                self.emit(self.policy.plan_model_router_error_event(&scope, &error));
                self.emit_session_finished(&scope, "failed", ToolCounts::default(), 0);
                return self.empty_report(&scope, Some("model router failed".to_string()));
            }
        };
        let mut transcript = self.policy.initial_transcript(&scope, &self.snapshot);
        let mut model_accounting = SessionModelAccounting::default();
        let mut tool_counts = ToolCounts::default();
        let mut evidence = SessionEvidence::default();
        let mut terminal = SessionTerminal::default();

        for turn_index in 0..scope.budget.max_turns {
            if !flow.begin_turn(
                tool_counts.total(),
                scope.budget.max_tool_calls,
                cancel.is_cancelled(),
            ) {
                break;
            }
            let turn_id = TurnId(turn_index as u32);
            let turn = match ModelTurnRunner::new(self.policy.as_ref(), &self.events)
                .complete(
                    model.as_ref(),
                    &scope,
                    &transcript,
                    turn_id,
                    turn_index,
                    cancel.child_token(),
                )
                .await
            {
                Ok(completion) => {
                    model_accounting.record_success(completion.attempts, completion.elapsed_ms);
                    completion.turn
                }
                Err(failure) => {
                    model_accounting.record_error(failure.attempts, failure.elapsed_ms);
                    flow.record_model_error(&failure.error);
                    break;
                }
            };
            match turn {
                ModelTurn::Text { content, usage } => {
                    model_accounting.record_usage(usage, model.estimate_cost(&usage));
                    transcript.push(self.policy.plan_assistant_text_transcript_item(content));
                    flow.record_completion();
                    break;
                }
                ModelTurn::ToolCalls { calls, usage } => {
                    model_accounting.record_usage(usage, model.estimate_cost(&usage));
                    if calls.is_empty() {
                        flow.record_completion();
                        break;
                    }
                    for call in &calls {
                        self.emit(self.policy.plan_tool_call_requested_event(&scope, call));
                    }
                    transcript.push(
                        self.policy
                            .plan_assistant_tool_calls_transcript_item(&calls),
                    );
                    let results = ToolBatchRunner::new(
                        self.policy.as_ref(),
                        self.tools.as_ref(),
                        &self.events,
                    )
                    .execute(
                        scope.clone(),
                        turn_id,
                        calls,
                        evidence.ready(),
                        scope
                            .budget
                            .max_tool_calls
                            .saturating_sub(tool_counts.total()),
                        cancel.child_token(),
                    )
                    .await;
                    if flow.cancel_after_successful_tool_batch(
                        cancel.is_cancelled(),
                        results.iter().any(|result| result.ok),
                    ) {
                        break;
                    }
                    let outcome = ToolResultEffectProcessor::new(
                        self.policy.as_ref(),
                        self.tools.as_ref(),
                        &self.events,
                        &self.review_revision_id,
                    )
                    .apply_batch(
                        &scope,
                        turn_id,
                        results,
                        ToolResultBatchState {
                            evidence: &mut evidence,
                            terminal: &mut terminal,
                            tool_counts: &mut tool_counts,
                            transcript: &mut transcript,
                        },
                    );
                    if flow.record_tool_batch_outcome(
                        outcome.terminal_seen,
                        self.policy.should_fail_after_terminal_errors(&terminal),
                    ) {
                        break;
                    }
                }
            }
        }

        let status = self.policy.session_state(
            flow.completed(),
            terminal.seen(),
            flow.cancelled(),
            flow.failed(),
        );
        let model_calls = model_accounting.calls();
        self.emit_session_finished(&scope, status, tool_counts, model_calls);
        SessionReport {
            completed: flow.completed(),
            model_calls,
            model_metrics: model_accounting.snapshot(),
            tool_counts,
            tokens: model_accounting.tokens(),
            terminal_diagnostic: self.policy.session_terminal_diagnostic(
                &scope,
                flow.completed(),
                &evidence,
                &terminal,
                model_calls,
                tool_counts,
            ),
        }
    }

    fn emit(&self, event: EventRecord) {
        self.events.emit_legacy(event);
    }

    fn emit_session_started(&self, scope: &SessionScope) {
        self.emit(self.policy.plan_session_started_event(scope));
        self.emit_planned_runtime(self.policy.plan_session_started_runtime_event(scope));
    }

    fn emit_session_finished(
        &self,
        scope: &SessionScope,
        status: &str,
        tool_counts: ToolCounts,
        model_calls: usize,
    ) {
        self.emit(
            self.policy
                .plan_session_finished_event(scope, status, tool_counts, model_calls),
        );
        self.emit_planned_runtime(
            self.policy
                .plan_session_finished_runtime_event(scope, status),
        );
    }

    fn emit_planned_runtime(&self, planned: PlannedRuntimeEvent) {
        self.events.emit_planned_runtime(planned);
    }
}

#[derive(Debug)]
pub(crate) struct SessionReport {
    pub(crate) completed: bool,
    pub(crate) model_calls: usize,
    pub(crate) model_metrics: ModelMetricsSnapshot,
    pub(crate) tool_counts: ToolCounts,
    pub(crate) tokens: TokenUsage,
    pub(crate) terminal_diagnostic: SessionTerminalDiagnostic,
}
