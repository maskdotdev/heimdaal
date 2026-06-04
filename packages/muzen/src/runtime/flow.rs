use crate::runtime::contracts::RuntimeError;

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub(crate) struct SessionFlow {
    completed: bool,
    cancelled: bool,
    failed: bool,
}

impl SessionFlow {
    pub(crate) fn cancel_before_model(&mut self, cancel_requested: bool) -> bool {
        if cancel_requested {
            self.cancelled = true;
            return true;
        }
        false
    }

    pub(crate) fn begin_turn(
        &mut self,
        tool_calls_used: usize,
        max_tool_calls: usize,
        cancel_requested: bool,
    ) -> bool {
        if tool_calls_used >= max_tool_calls {
            return false;
        }
        if cancel_requested {
            self.cancelled = true;
            return false;
        }
        true
    }

    pub(crate) fn record_model_error(&mut self, error: &RuntimeError) {
        self.cancelled = matches!(error, RuntimeError::Cancelled);
        self.failed = !self.cancelled;
    }

    pub(crate) fn record_completion(&mut self) {
        self.completed = true;
    }

    pub(crate) fn cancel_after_successful_tool_batch(
        &mut self,
        cancel_requested: bool,
        batch_has_success: bool,
    ) -> bool {
        if cancel_requested && batch_has_success {
            self.cancelled = true;
            return true;
        }
        false
    }

    pub(crate) fn record_tool_batch_outcome(
        &mut self,
        terminal_seen: bool,
        should_fail_after_terminal_errors: bool,
    ) -> bool {
        if terminal_seen {
            self.completed = true;
            return true;
        }
        if should_fail_after_terminal_errors {
            self.failed = true;
            return true;
        }
        false
    }

    pub(crate) fn completed(&self) -> bool {
        self.completed
    }

    pub(crate) fn cancelled(&self) -> bool {
        self.cancelled
    }

    pub(crate) fn failed(&self) -> bool {
        self.failed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_flow_stops_when_cancelled_before_model_or_turn() {
        let mut flow = SessionFlow::default();
        assert!(flow.cancel_before_model(true));
        assert!(flow.cancelled());
        assert!(!flow.completed());
        assert!(!flow.failed());

        let mut flow = SessionFlow::default();
        assert!(!flow.begin_turn(0, 8, true));
        assert!(flow.cancelled());
    }

    #[test]
    fn session_flow_stops_when_budget_is_exhausted_without_marking_failure() {
        let mut flow = SessionFlow::default();

        assert!(!flow.begin_turn(8, 8, false));

        assert!(!flow.completed());
        assert!(!flow.cancelled());
        assert!(!flow.failed());
    }

    #[test]
    fn session_flow_records_model_errors_as_cancelled_or_failed() {
        let mut flow = SessionFlow::default();
        flow.record_model_error(&RuntimeError::Provider {
            status: Some(503),
            retryable: false,
        });
        assert!(flow.failed());
        assert!(!flow.cancelled());

        let mut flow = SessionFlow::default();
        flow.record_model_error(&RuntimeError::Cancelled);
        assert!(flow.cancelled());
        assert!(!flow.failed());
    }

    #[test]
    fn session_flow_records_completion_and_tool_batch_stop_conditions() {
        let mut flow = SessionFlow::default();
        flow.record_completion();
        assert!(flow.completed());

        let mut flow = SessionFlow::default();
        assert!(flow.cancel_after_successful_tool_batch(true, true));
        assert!(flow.cancelled());

        let mut flow = SessionFlow::default();
        assert!(flow.record_tool_batch_outcome(true, false));
        assert!(flow.completed());

        let mut flow = SessionFlow::default();
        assert!(flow.record_tool_batch_outcome(false, true));
        assert!(flow.failed());
    }
}
