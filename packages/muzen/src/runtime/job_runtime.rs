use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::contracts::{TokenUsage, ToolCounts};
use crate::runtime::contracts::*;
use crate::runtime::dispatch::RuntimeEventDispatcher;
use crate::runtime::model::ConcurrentModelRouter;
use crate::runtime::policy::ReviewerPolicy;
use crate::runtime::repo::RepoSnapshot;
use crate::runtime::session_loop::SessionRunner;
use crate::runtime::tools::ToolEngine;

#[derive(Debug, Clone)]
pub(crate) struct SessionSpec {
    pub(crate) scope: SessionScope,
}

pub(crate) struct JobRuntime {
    pub(crate) snapshot: Arc<RepoSnapshot>,
    pub(crate) model_router: Arc<dyn ConcurrentModelRouter>,
    pub(crate) tools: Arc<ToolEngine>,
    pub(crate) policy: Arc<ReviewerPolicy>,
    pub(crate) limits: Arc<RuntimeLimits>,
    pub(crate) review_revision_id: String,
    pub(crate) events: RuntimeEventDispatcher,
}

impl JobRuntime {
    pub(crate) async fn run_sessions(&self, sessions: Vec<SessionSpec>) -> ConcurrentRunReport {
        self.run_sessions_with_cancel(sessions, CancellationToken::new())
            .await
    }

    pub(crate) async fn run_sessions_with_cancel(
        &self,
        sessions: Vec<SessionSpec>,
        cancel: CancellationToken,
    ) -> ConcurrentRunReport {
        let started = Instant::now();
        let active = Arc::new(Semaphore::new(self.limits.max_active_sessions.max(1)));
        let mut joins = JoinSet::new();

        for session in sessions.clone() {
            let active = Arc::clone(&active);
            let runner = self.session_runner();
            let child_cancel = cancel.child_token();
            joins.spawn(async move {
                let permit = active.acquire_owned().await;
                if permit.is_err() {
                    return runner.empty_report(
                        &session.scope,
                        Some("failed to acquire active session permit".to_string()),
                    );
                }
                let _permit = permit.ok();
                runner.run_scope(session.scope, child_cancel).await
            });
        }

        let mut completed_sessions = 0usize;
        let mut model_calls = 0usize;
        let mut model_metrics = ModelMetricsSnapshot::default();
        let mut tool_counts = ToolCounts::default();
        let mut tokens = TokenUsage::default();
        let mut terminal_diagnostics = Vec::new();
        while let Some(result) = joins.join_next().await {
            let Ok(report) = result else {
                continue;
            };
            if report.completed {
                completed_sessions += 1;
            }
            model_calls += report.model_calls;
            model_metrics.calls += report.model_metrics.calls;
            model_metrics.successes += report.model_metrics.successes;
            model_metrics.errors += report.model_metrics.errors;
            model_metrics.retries += report.model_metrics.retries;
            model_metrics.costed_calls += report.model_metrics.costed_calls;
            model_metrics.unpriced_calls += report.model_metrics.unpriced_calls;
            model_metrics.latency_ms += report.model_metrics.latency_ms;
            model_metrics.max_latency_ms = model_metrics
                .max_latency_ms
                .max(report.model_metrics.max_latency_ms);
            model_metrics.estimated_input_cost_micro_usd +=
                report.model_metrics.estimated_input_cost_micro_usd;
            model_metrics.estimated_output_cost_micro_usd +=
                report.model_metrics.estimated_output_cost_micro_usd;
            model_metrics.estimated_total_cost_micro_usd +=
                report.model_metrics.estimated_total_cost_micro_usd;
            model_metrics.input_tokens += report.model_metrics.input_tokens;
            model_metrics.output_tokens += report.model_metrics.output_tokens;
            model_metrics.total_tokens += report.model_metrics.total_tokens;
            tool_counts.add(report.tool_counts);
            tokens.add(report.tokens);
            terminal_diagnostics.push(report.terminal_diagnostic);
        }
        terminal_diagnostics.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        let (artifacts, artifact_bytes) = self.tools.artifacts.stats();
        let counters = self.tools.snapshot_counters();
        let tool_metrics = self.tools.snapshot_tool_metrics();
        let provider_health = self.tools.snapshot_provider_health();
        let mut report = ConcurrentRunReport {
            runtime: "concurrent",
            sessions: sessions.len(),
            completed_sessions,
            model_calls,
            tool_calls: tool_counts.total(),
            tool_counts,
            findings: self.tools.findings.len(),
            publishable_findings: self.tools.findings.publishable_len(),
            elapsed_ms: (started.elapsed().as_micros().div_ceil(1000) as u64).max(1),
            input_tokens: tokens.input_tokens,
            output_tokens: tokens.output_tokens,
            total_tokens: tokens.total_tokens,
            artifacts,
            artifact_bytes,
            counters,
            tool_metrics,
            provider_health,
            snapshot_metrics: vec![SnapshotMetricsSnapshot {
                snapshot_id: self.snapshot.snapshot_id.clone(),
                sessions: sessions.len(),
                completed_sessions,
                model_calls,
                tool_calls: tool_counts.total(),
                artifacts,
                artifact_bytes,
                elapsed_ms: (started.elapsed().as_micros().div_ceil(1000) as u64).max(1),
            }],
            model_metrics,
            terminal_diagnostics,
            benchmark_valid: false,
            benchmark_failures: Vec::new(),
        };
        report.benchmark_failures = benchmark_failures(&report);
        report.benchmark_valid = report.benchmark_failures.is_empty();
        report
    }

    fn session_runner(&self) -> SessionRunner {
        SessionRunner::new(
            Arc::clone(&self.snapshot),
            Arc::clone(&self.model_router),
            Arc::clone(&self.tools),
            Arc::clone(&self.policy),
            self.review_revision_id.clone(),
            self.events.clone(),
        )
    }
}

pub(crate) fn benchmark_failures(report: &ConcurrentRunReport) -> Vec<String> {
    let mut failures = Vec::new();
    if report.completed_sessions != report.sessions {
        failures.push(format!(
            "only {}/{} sessions completed",
            report.completed_sessions, report.sessions
        ));
    }
    if report.model_calls == 0 {
        failures.push("no model calls recorded".to_string());
    }
    if report.tool_counts.read_diff == 0 {
        failures.push("read_diff was not exercised".to_string());
    }
    if report.tool_counts.read_file == 0 && report.tool_counts.read_head_file == 0 {
        failures.push("read_file/read_head_file was not exercised".to_string());
    }
    if report.tool_counts.search_text == 0 {
        failures.push("search_text was not exercised".to_string());
    }
    if report.findings == 0 && report.tool_counts.finish == 0 {
        failures.push("no finding or finish rationale was recorded".to_string());
    }
    failures
}
