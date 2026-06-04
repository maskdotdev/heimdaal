use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use crate::contracts::TokenUsage;
use crate::runtime::contracts::*;
use crate::runtime::dispatch::RuntimeEventDispatcher;
use crate::runtime::model::ConcurrentModelClient;
use crate::runtime::policy::ReviewerPolicy;

pub(crate) struct ModelTurnRunner<'a> {
    policy: &'a ReviewerPolicy,
    events: &'a RuntimeEventDispatcher,
}

impl<'a> ModelTurnRunner<'a> {
    pub(crate) fn new(policy: &'a ReviewerPolicy, events: &'a RuntimeEventDispatcher) -> Self {
        Self { policy, events }
    }

    pub(crate) async fn complete(
        &self,
        model: &dyn ConcurrentModelClient,
        scope: &SessionScope,
        transcript: &[ConversationItem],
        turn_id: TurnId,
        turn_index: usize,
        cancel: CancellationToken,
    ) -> Result<ModelTurnCompletion, ModelTurnFailure> {
        let started = Instant::now();
        let max_attempts = 3usize;
        let mut attempts = 0usize;
        loop {
            attempts += 1;
            self.emit_model_started(scope, turn_id, turn_index, attempts);
            match model
                .complete(scope, transcript, turn_id, cancel.child_token())
                .await
            {
                Ok(turn) => {
                    let (tool_call_count, usage) = match &turn {
                        ModelTurn::ToolCalls { calls, usage } => (calls.len(), *usage),
                        ModelTurn::Text { usage, .. } => (0, *usage),
                    };
                    self.emit_model_completed(scope, turn_id, turn_index, usage, tool_call_count);
                    return Ok(ModelTurnCompletion {
                        turn,
                        attempts,
                        elapsed_ms: elapsed_ms(started),
                    });
                }
                Err(error)
                    if self.policy.should_retry_model_error(&error)
                        && attempts < max_attempts
                        && !cancel.is_cancelled() =>
                {
                    self.events.emit_legacy(
                        self.policy.plan_model_attempt_error_event(
                            scope, turn_index, attempts, true, &error,
                        ),
                    );
                    tokio::time::sleep(retry_delay(attempts)).await;
                }
                Err(error) => {
                    self.events
                        .emit_legacy(self.policy.plan_model_attempt_error_event(
                            scope, turn_index, attempts, false, &error,
                        ));
                    return Err(ModelTurnFailure {
                        error,
                        attempts,
                        elapsed_ms: elapsed_ms(started),
                    });
                }
            }
        }
    }

    fn emit_model_started(
        &self,
        scope: &SessionScope,
        turn_id: TurnId,
        turn_index: usize,
        attempt: usize,
    ) {
        self.events.emit_legacy(
            self.policy
                .plan_model_started_event(scope, turn_index, attempt),
        );
        self.events
            .emit_planned_runtime(self.policy.plan_model_started_runtime_event(scope, turn_id));
    }

    fn emit_model_completed(
        &self,
        scope: &SessionScope,
        turn_id: TurnId,
        turn_index: usize,
        usage: TokenUsage,
        tool_call_count: usize,
    ) {
        self.events.emit_legacy(
            self.policy
                .plan_model_completed_event(scope, turn_index, usage),
        );
        self.events
            .emit_planned_runtime(self.policy.plan_model_completed_runtime_event(
                scope,
                turn_id,
                tool_call_count,
            ));
    }
}

#[derive(Debug)]
pub(crate) struct ModelTurnCompletion {
    pub(crate) turn: ModelTurn,
    pub(crate) attempts: usize,
    pub(crate) elapsed_ms: u64,
}

#[derive(Debug)]
pub(crate) struct ModelTurnFailure {
    pub(crate) error: RuntimeError,
    pub(crate) attempts: usize,
    pub(crate) elapsed_ms: u64,
}

fn retry_delay(attempt: usize) -> Duration {
    Duration::from_millis((attempt as u64).saturating_mul(25))
}

fn elapsed_ms(started: Instant) -> u64 {
    (started.elapsed().as_micros().div_ceil(1000) as u64).max(1)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::contracts::{AgentBudget, Role};

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

    struct RetryOnceModel {
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl ConcurrentModelClient for RetryOnceModel {
        async fn complete(
            &self,
            _scope: &SessionScope,
            _transcript: &[ConversationItem],
            _turn_id: TurnId,
            _cancel: CancellationToken,
        ) -> RuntimeResult<ModelTurn> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(RuntimeError::Timeout);
            }
            Ok(ModelTurn::Text {
                content: "done".to_string(),
                usage: TokenUsage {
                    input_tokens: 5,
                    output_tokens: 7,
                    total_tokens: 12,
                },
            })
        }
    }

    #[tokio::test]
    async fn model_turn_runner_retries_retryable_errors_and_emits_model_events() {
        let policy = ReviewerPolicy::new();
        let runtime_sink = Arc::new(RecordingRuntimeSink::default());
        let sink: Arc<dyn RuntimeEventSink> = runtime_sink.clone();
        let dispatcher = RuntimeEventDispatcher::new(Some(sink), None);
        let runner = ModelTurnRunner::new(&policy, &dispatcher);
        let model = RetryOnceModel {
            attempts: AtomicUsize::new(0),
        };
        let scope = test_scope("model-turn-session");

        let completion = runner
            .complete(&model, &scope, &[], TurnId(4), 4, CancellationToken::new())
            .await
            .expect("model turn completion");

        assert_eq!(completion.attempts, 2);
        assert!(completion.elapsed_ms >= 1);
        assert!(matches!(completion.turn, ModelTurn::Text { .. }));
        let records = runtime_sink.records.lock().expect("sink lock");
        assert_eq!(
            records
                .iter()
                .filter(|(_, event)| matches!(event, RuntimeEvent::ModelStarted { .. }))
                .count(),
            2
        );
        assert_eq!(
            records
                .iter()
                .filter(|(_, event)| matches!(event, RuntimeEvent::ModelCompleted { .. }))
                .count(),
            1
        );
        assert!(records.iter().any(|(context, event)| {
            context.session_id.as_ref() == Some(&scope.id)
                && context.turn_id == Some(TurnId(4))
                && matches!(event, RuntimeEvent::ModelCompleted { .. })
        }));
    }

    fn test_scope(id: &str) -> SessionScope {
        SessionScope::review_read_only(
            SessionId(id.to_string()),
            Role::Generalist,
            "model turn runner test",
            AgentBudget {
                max_turns: 4,
                max_tool_calls: 8,
                max_prompt_tokens: 32_000,
                max_output_tokens: 512,
            },
        )
    }
}
