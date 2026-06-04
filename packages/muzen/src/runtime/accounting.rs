use crate::contracts::TokenUsage;
use crate::runtime::contracts::{ModelCostEstimate, ModelMetricsSnapshot};

#[derive(Debug, Default, Clone)]
pub(crate) struct SessionModelAccounting {
    calls: usize,
    successes: usize,
    errors: usize,
    retries: usize,
    costed_calls: usize,
    unpriced_calls: usize,
    latency_ms: u64,
    max_latency_ms: u64,
    estimated_input_cost_micro_usd: u64,
    estimated_output_cost_micro_usd: u64,
    estimated_total_cost_micro_usd: u64,
    tokens: TokenUsage,
}

impl SessionModelAccounting {
    pub(crate) fn record_success(&mut self, attempts: usize, elapsed_ms: u64) {
        self.calls += attempts;
        self.successes += 1;
        self.retries += attempts.saturating_sub(1);
        self.record_latency(elapsed_ms);
    }

    pub(crate) fn record_error(&mut self, attempts: usize, elapsed_ms: u64) {
        self.calls += attempts;
        self.errors += 1;
        self.retries += attempts.saturating_sub(1);
        self.record_latency(elapsed_ms);
    }

    pub(crate) fn record_usage(&mut self, usage: TokenUsage, cost: Option<ModelCostEstimate>) {
        self.tokens.add(usage);
        if let Some(cost) = cost {
            self.costed_calls += 1;
            self.estimated_input_cost_micro_usd += cost.input_cost_micro_usd;
            self.estimated_output_cost_micro_usd += cost.output_cost_micro_usd;
            self.estimated_total_cost_micro_usd += cost.total_cost_micro_usd;
        } else {
            self.unpriced_calls += 1;
        }
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls
    }

    pub(crate) fn tokens(&self) -> TokenUsage {
        self.tokens
    }

    pub(crate) fn snapshot(&self) -> ModelMetricsSnapshot {
        ModelMetricsSnapshot {
            calls: self.calls,
            successes: self.successes,
            errors: self.errors,
            retries: self.retries,
            costed_calls: self.costed_calls,
            unpriced_calls: self.unpriced_calls,
            latency_ms: self.latency_ms,
            max_latency_ms: self.max_latency_ms,
            estimated_input_cost_micro_usd: self.estimated_input_cost_micro_usd,
            estimated_output_cost_micro_usd: self.estimated_output_cost_micro_usd,
            estimated_total_cost_micro_usd: self.estimated_total_cost_micro_usd,
            input_tokens: self.tokens.input_tokens,
            output_tokens: self.tokens.output_tokens,
            total_tokens: self.tokens.total_tokens,
        }
    }

    fn record_latency(&mut self, elapsed_ms: u64) {
        self.latency_ms += elapsed_ms;
        self.max_latency_ms = self.max_latency_ms.max(elapsed_ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_model_accounting_records_success_usage_cost_and_retries() {
        let mut accounting = SessionModelAccounting::default();

        accounting.record_success(3, 42);
        accounting.record_usage(
            TokenUsage {
                input_tokens: 11,
                output_tokens: 13,
                total_tokens: 24,
            },
            Some(ModelCostEstimate {
                input_cost_micro_usd: 100,
                output_cost_micro_usd: 200,
                total_cost_micro_usd: 300,
            }),
        );

        let snapshot = accounting.snapshot();
        assert_eq!(accounting.calls(), 3);
        assert_eq!(accounting.tokens().total_tokens, 24);
        assert_eq!(snapshot.calls, 3);
        assert_eq!(snapshot.successes, 1);
        assert_eq!(snapshot.errors, 0);
        assert_eq!(snapshot.retries, 2);
        assert_eq!(snapshot.costed_calls, 1);
        assert_eq!(snapshot.unpriced_calls, 0);
        assert_eq!(snapshot.latency_ms, 42);
        assert_eq!(snapshot.max_latency_ms, 42);
        assert_eq!(snapshot.estimated_total_cost_micro_usd, 300);
        assert_eq!(snapshot.input_tokens, 11);
        assert_eq!(snapshot.output_tokens, 13);
        assert_eq!(snapshot.total_tokens, 24);
    }

    #[test]
    fn session_model_accounting_records_errors_unpriced_usage_and_max_latency() {
        let mut accounting = SessionModelAccounting::default();

        accounting.record_error(2, 5);
        accounting.record_success(1, 9);
        accounting.record_usage(
            TokenUsage {
                input_tokens: 7,
                output_tokens: 3,
                total_tokens: 10,
            },
            None,
        );

        let snapshot = accounting.snapshot();
        assert_eq!(snapshot.calls, 3);
        assert_eq!(snapshot.successes, 1);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(snapshot.retries, 1);
        assert_eq!(snapshot.costed_calls, 0);
        assert_eq!(snapshot.unpriced_calls, 1);
        assert_eq!(snapshot.latency_ms, 14);
        assert_eq!(snapshot.max_latency_ms, 9);
        assert_eq!(snapshot.total_tokens, 10);
    }
}
