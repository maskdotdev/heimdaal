use std::collections::VecDeque;
use std::fs;
use std::io::Write;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Instant;

use anyhow::{anyhow, bail, Result};
use serde::Serialize;
use serde_json::{json, Value};

use crate::contracts::*;
use crate::model::ModelClientV1;
use crate::repo::{normalize_policy_path, RepoContext};
use crate::tools::{ToolOutcome, ToolRegistry};
use crate::util::{redact_known_secrets, redaction_none, timestamp_utc, SCHEMA_VERSION};

pub(crate) struct AgentRuntime {
    pub(crate) tools: Arc<ToolRegistry>,
    pub(crate) blackboard: Arc<Blackboard>,
    pub(crate) findings: Arc<FindingStore>,
    pub(crate) model: Arc<ModelClientV1>,
    pub(crate) repo: Arc<RepoContext>,
    pub(crate) emitter: Option<Arc<EventEmitter>>,
}

impl AgentRuntime {
    pub(crate) fn new(
        repo: Arc<RepoContext>,
        artifacts: Arc<ArtifactStore>,
        job: &ReviewRunJobV1,
        emitter: Option<Arc<EventEmitter>>,
    ) -> Result<Self> {
        Ok(Self {
            tools: Arc::new(ToolRegistry {
                repo: Arc::clone(&repo),
                artifacts: Arc::clone(&artifacts),
            }),
            blackboard: Arc::new(Blackboard::default()),
            findings: Arc::new(FindingStore::default()),
            model: Arc::new(ModelClientV1::from_job(job, artifacts)?),
            repo,
            emitter,
        })
    }

    pub(crate) fn run_session(&self, mut session: AgentSession) -> Result<SessionReport> {
        session.state = AgentState::Running;
        let started = Instant::now();
        let mut tool_counts = ToolCounts::default();
        let mut tokens = TokenUsage::default();
        let mut model_calls = 0usize;
        let mut denied = 0usize;

        self.emit(
            EventLevel::Info,
            EventType::SessionStarted,
            Some(session.id.clone()),
            None,
            None,
            None,
            json!({"role": session.role, "objective": session.objective}),
        );

        for turn in 0..session.budget.max_turns {
            self.emit(
                EventLevel::Debug,
                EventType::ModelCallStarted,
                Some(session.id.clone()),
                None,
                None,
                None,
                json!({"turn": turn}),
            );
            let decision = self.model.next_action(&session, &self.repo)?;
            model_calls += 1;
            tokens.add(decision.usage);
            self.emit(
                EventLevel::Debug,
                EventType::ModelCallCompleted,
                Some(session.id.clone()),
                None,
                None,
                None,
                json!({"turn": turn, "tokens": decision.usage}),
            );

            let action = decision.action;
            let tool = action.tool_name();
            let tool_call_id = format!("{}-tool-{}", session.id, turn);
            session.events.push(AgentEvent::ModelAction {
                summary: action.summary(),
            });
            self.emit(
                EventLevel::Info,
                EventType::ToolCallRequested,
                Some(session.id.clone()),
                Some(tool_call_id.clone()),
                None,
                None,
                json!({"toolName": tool.as_str()}),
            );

            if !tool_allowed(session.allowed_tools, tool) {
                denied += 1;
                session.events.push(AgentEvent::ToolDenied {
                    tool_call_id: tool_call_id.clone(),
                    tool,
                    error_code: "TOOL_NOT_ALLOWED".to_string(),
                });
                self.emit(
                    EventLevel::Warn,
                    EventType::ToolCallCompleted,
                    Some(session.id.clone()),
                    Some(tool_call_id.clone()),
                    None,
                    None,
                    json!({"status": "denied", "toolName": tool.as_str()}),
                );
                if denied >= 2 {
                    session.state = AgentState::Failed;
                    break;
                }
                continue;
            }

            let outcome = match self.tools.execute(&session, tool_call_id.clone(), action) {
                Ok(outcome) => outcome,
                Err(error) => {
                    denied += 1;
                    session.events.push(AgentEvent::ToolDenied {
                        tool_call_id: tool_call_id.clone(),
                        tool,
                        error_code: "TOOL_EXECUTION_ERROR".to_string(),
                    });
                    self.emit(
                        EventLevel::Warn,
                        EventType::ToolCallCompleted,
                        Some(session.id.clone()),
                        Some(tool_call_id.clone()),
                        None,
                        None,
                        json!({
                            "status": "internal_error",
                            "toolName": tool.as_str(),
                            "error": redact_known_secrets(&format!("{error:#}"), &[])
                        }),
                    );
                    if denied >= 2 {
                        session.state = AgentState::Failed;
                        break;
                    }
                    continue;
                }
            };

            tool_counts.increment(tool);
            self.record_tool(&mut session, outcome);

            if tool == ToolName::Finish
                || tool == ToolName::RecordFinding
                || tool_counts.total() >= session.budget.max_tool_calls
                || turn + 1 >= session.budget.max_turns
            {
                session.state = AgentState::Done;
                break;
            }
        }

        let event_count = session.events.len();
        self.emit(
            EventLevel::Info,
            EventType::SessionFinished,
            Some(session.id.clone()),
            None,
            None,
            None,
            json!({"state": session.state, "toolCounts": tool_counts, "modelCalls": model_calls}),
        );
        Ok(SessionReport {
            session_id: session.id,
            role: session.role,
            events: event_count,
            tool_counts,
            model_calls,
            tokens,
            elapsed_ms: started.elapsed().as_millis() as u64,
            state: session.state,
        })
    }

    pub(crate) fn record_tool(&self, session: &mut AgentSession, outcome: ToolOutcome) {
        let tool_result = outcome.tool_result;
        if let Some(artifact_id) = outcome.artifact_id {
            session.events.push(AgentEvent::ToolResult {
                tool_call_id: tool_result.tool_call_id.clone(),
                tool: tool_result.tool_name,
                artifact_id,
                summary: tool_result.summary.clone(),
                completeness: tool_result.completeness,
            });
            self.blackboard.push(BlackboardEntry {
                session_id: session.id.clone(),
                entry_type: "tool_result",
                summary: tool_result.summary.clone(),
                artifact_id: Some(artifact_id.as_string()),
                finding_id: None,
            });
            self.emit(
                EventLevel::Info,
                EventType::ArtifactRecorded,
                Some(session.id.clone()),
                Some(tool_result.tool_call_id.clone()),
                Some(artifact_id.as_string()),
                None,
                json!({"toolName": tool_result.tool_name.as_str(), "summary": tool_result.summary}),
            );
        }

        if let Some((title, claim)) = outcome.finding {
            let evidence = evidence_from_session(
                session,
                &self.tools.artifacts,
                &self.repo.change.head_revision_id,
                outcome.evidence_revision,
            );
            let finding_id =
                self.findings
                    .insert(title.clone(), claim, session.id.clone(), evidence);
            session.events.push(AgentEvent::Finding {
                finding_id,
                summary: title.clone(),
            });
            self.blackboard.push(BlackboardEntry {
                session_id: session.id.clone(),
                entry_type: "candidate_finding",
                summary: title,
                artifact_id: None,
                finding_id: Some(finding_id.as_string()),
            });
            self.emit(
                EventLevel::Info,
                EventType::FindingValidated,
                Some(session.id.clone()),
                Some(tool_result.tool_call_id),
                None,
                Some(finding_id.as_string()),
                json!({"validationStatus": "validated"}),
            );
        } else {
            self.emit(
                EventLevel::Info,
                EventType::ToolCallCompleted,
                Some(session.id.clone()),
                Some(tool_result.tool_call_id),
                None,
                None,
                json!({"status": tool_result.status, "toolName": tool_result.tool_name.as_str()}),
            );
        }
    }

    pub(crate) fn emit(
        &self,
        level: EventLevel,
        event_type: EventType,
        session_id: Option<String>,
        tool_call_id: Option<String>,
        artifact_id: Option<String>,
        finding_id: Option<String>,
        payload: Value,
    ) {
        if let Some(emitter) = &self.emitter {
            emitter.emit(
                level,
                event_type,
                session_id,
                tool_call_id,
                artifact_id,
                finding_id,
                payload,
            );
        }
    }
}

pub(crate) fn evidence_from_session(
    session: &AgentSession,
    artifacts: &ArtifactStore,
    revision_id: &str,
    revision: EvidenceRevision,
) -> Vec<EvidenceRefV1> {
    session
        .events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ToolResult {
                tool_call_id,
                artifact_id,
                ..
            } => {
                let meta = artifacts.meta(*artifact_id)?;
                Some(EvidenceRefV1 {
                    evidence_id: format!("evidence-{}", artifact_id.0),
                    artifact_id: artifact_id.as_string(),
                    kind: meta.kind,
                    revision,
                    revision_id: revision_id.to_string(),
                    location: EvidenceLocationV1::SinglePath {
                        path: meta.summary.clone(),
                    },
                    line_range: None,
                    byte_range: Some(ByteRangeV1 {
                        start_byte: 0,
                        end_byte: meta.bytes,
                    }),
                    diff_anchor: None,
                    content_hash: meta.content_hash,
                    redaction: meta.redaction,
                    producing_tool_call_id: tool_call_id.clone(),
                })
            }
            _ => None,
        })
        .collect()
}

pub(crate) fn tool_allowed(mask: ToolMask, tool: ToolName) -> bool {
    match tool {
        ToolName::ListChangedFiles => mask.list_changed_files,
        ToolName::ReadDiff => mask.read_diff,
        ToolName::ListFiles => mask.list_files,
        ToolName::ReadFile => mask.read_file,
        ToolName::ReadBaseFile => mask.read_base_file,
        ToolName::ReadHeadFile => mask.read_head_file,
        ToolName::SearchText => mask.search_text,
        ToolName::FindRelatedFiles => mask.find_related_files,
        ToolName::FindTestsForFile => mask.find_tests_for_file,
        ToolName::ListImports => mask.list_imports,
        ToolName::RecordFinding => mask.record_finding,
        ToolName::ChallengeFinding => mask.challenge_finding,
        ToolName::Finish => mask.finish,
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SessionReport {
    pub(crate) session_id: String,
    pub(crate) role: Role,
    pub(crate) events: usize,
    pub(crate) tool_counts: ToolCounts,
    pub(crate) model_calls: usize,
    pub(crate) tokens: TokenUsage,
    pub(crate) elapsed_ms: u64,
    pub(crate) state: AgentState,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeReport {
    pub(crate) model: String,
    pub(crate) sessions: usize,
    pub(crate) completed_sessions: usize,
    pub(crate) model_calls: usize,
    pub(crate) tool_calls: usize,
    pub(crate) tool_counts: ToolCounts,
    pub(crate) findings: usize,
    pub(crate) publishable_findings: usize,
    pub(crate) blackboard_entries: usize,
    pub(crate) input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) total_tokens: u64,
    pub(crate) elapsed_ms: u64,
    pub(crate) artifact_stats: ArtifactStats,
    pub(crate) benchmark_valid: bool,
    pub(crate) benchmark_failures: Vec<String>,
}

pub(crate) struct EventEmitter {
    pub(crate) run_id: String,
    pub(crate) attempt: u32,
    pub(crate) redaction_policy_id: String,
    pub(crate) state: Mutex<EventEmitterState>,
}

pub(crate) struct EventEmitterState {
    pub(crate) seq: u64,
    pub(crate) writer: Box<dyn Write + Send>,
}

impl EventEmitter {
    pub(crate) fn stdout(run_id: String, attempt: u32, redaction_policy_id: String) -> Self {
        Self {
            run_id,
            attempt,
            redaction_policy_id,
            state: Mutex::new(EventEmitterState {
                seq: 0,
                writer: Box::new(std::io::stdout()),
            }),
        }
    }

    pub(crate) fn emit(
        &self,
        level: EventLevel,
        event_type: EventType,
        session_id: Option<String>,
        tool_call_id: Option<String>,
        artifact_id: Option<String>,
        finding_id: Option<String>,
        payload: Value,
    ) {
        let mut state = self.state.lock().expect("event emitter poisoned");
        state.seq += 1;
        let event = RunEventV1 {
            schema_version: SCHEMA_VERSION,
            event_id: format!("{}-event-{}", self.run_id, state.seq),
            run_id: self.run_id.clone(),
            attempt: self.attempt,
            seq: state.seq,
            timestamp_utc: timestamp_utc(),
            level,
            event_type,
            session_id,
            tool_call_id,
            artifact_id,
            finding_id,
            payload,
            redaction: RedactionMetadataV1 {
                redaction_policy_id: self.redaction_policy_id.clone(),
                ..redaction_none()
            },
            trace: EventTraceV1 {
                parent_event_id: None,
                correlation_id: None,
            },
        };
        let _ = serde_json::to_writer(&mut state.writer, &event);
        let _ = state.writer.write_all(b"\n");
        let _ = state.writer.flush();
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct BenchEvent<'a> {
    pub(crate) event: &'static str,
    pub(crate) label: &'a str,
    pub(crate) model: &'a str,
    pub(crate) sessions: usize,
    pub(crate) completed_sessions: usize,
    pub(crate) model_calls: usize,
    pub(crate) tool_calls: usize,
    pub(crate) list_changed_files_calls: usize,
    pub(crate) read_diff_calls: usize,
    pub(crate) list_files_calls: usize,
    pub(crate) read_file_calls: usize,
    pub(crate) search_text_calls: usize,
    pub(crate) finish_calls: usize,
    pub(crate) findings: usize,
    pub(crate) publishable_findings: usize,
    pub(crate) blackboard_entries: usize,
    pub(crate) tokens_in: u64,
    pub(crate) tokens_out: u64,
    pub(crate) tokens_total: u64,
    pub(crate) artifacts: usize,
    pub(crate) artifact_bytes: usize,
    pub(crate) benchmark_valid: bool,
    pub(crate) benchmark_failures: Vec<String>,
    pub(crate) elapsed_ms: u64,
}

pub(crate) fn log_bench_event(
    label: &str,
    model: &str,
    sessions: usize,
    report: Option<&RuntimeReport>,
) {
    let event = BenchEvent {
        event: "rust_memory",
        label,
        model,
        sessions,
        completed_sessions: report.map_or(0, |value| value.completed_sessions),
        model_calls: report.map_or(0, |value| value.model_calls),
        tool_calls: report.map_or(0, |value| value.tool_calls),
        list_changed_files_calls: report.map_or(0, |value| value.tool_counts.list_changed_files),
        read_diff_calls: report.map_or(0, |value| value.tool_counts.read_diff),
        list_files_calls: report.map_or(0, |value| value.tool_counts.list_files),
        read_file_calls: report.map_or(0, |value| value.tool_counts.read_file),
        search_text_calls: report.map_or(0, |value| value.tool_counts.search_text),
        finish_calls: report.map_or(0, |value| value.tool_counts.finish),
        findings: report.map_or(0, |value| value.findings),
        publishable_findings: report.map_or(0, |value| value.publishable_findings),
        blackboard_entries: report.map_or(0, |value| value.blackboard_entries),
        tokens_in: report.map_or(0, |value| value.input_tokens),
        tokens_out: report.map_or(0, |value| value.output_tokens),
        tokens_total: report.map_or(0, |value| value.total_tokens),
        artifacts: report.map_or(0, |value| value.artifact_stats.artifacts),
        artifact_bytes: report.map_or(0, |value| value.artifact_stats.artifact_bytes),
        benchmark_valid: report.is_some_and(|value| value.benchmark_valid),
        benchmark_failures: report
            .map(|value| value.benchmark_failures.clone())
            .unwrap_or_default(),
        elapsed_ms: report.map_or(0, |value| value.elapsed_ms),
    };
    println!(
        "{}",
        serde_json::to_string(&event).expect("serialize bench event")
    );
}

pub(crate) fn run_review(
    job: ReviewRunJobV1,
    emitter: Option<Arc<EventEmitter>>,
) -> Result<RuntimeReport> {
    validate_job(&job)?;
    let started = Instant::now();
    let artifacts = Arc::new(ArtifactStore::default());
    let repo = Arc::new(RepoContext::new(
        job.repo.worktree_root.clone(),
        job.path_policy.clone(),
        job.change.clone(),
    )?);
    let runtime = Arc::new(AgentRuntime::new(
        Arc::clone(&repo),
        Arc::clone(&artifacts),
        &job,
        emitter.clone(),
    )?);
    let sessions = build_sessions(&job);

    if let Some(emitter) = &emitter {
        emitter.emit(
            EventLevel::Info,
            EventType::RunStarted,
            None,
            None,
            None,
            None,
            json!({"projectId": job.project_id, "sessions": sessions.len()}),
        );
    }

    let queue = Arc::new(WorkQueue::new(VecDeque::from(sessions)));
    let reports = Arc::new(Mutex::new(Vec::new()));
    let workers = job
        .budgets
        .max_active_sessions
        .max(1)
        .min(job.personas.len().max(1));

    thread::scope(|scope| {
        for _ in 0..workers {
            let queue = Arc::clone(&queue);
            let runtime = Arc::clone(&runtime);
            let reports = Arc::clone(&reports);
            scope.spawn(move || loop {
                let Some(session) = queue.pop() else {
                    break;
                };
                match runtime.run_session(session) {
                    Ok(report) => reports.lock().expect("reports poisoned").push(report),
                    Err(error) => {
                        runtime.emit(
                            EventLevel::Error,
                            EventType::Error,
                            None,
                            None,
                            None,
                            None,
                            json!({"error": redact_known_secrets(&format!("{error:#}"), &[])}),
                        );
                    }
                }
            });
        }
    });

    let reports = reports.lock().expect("reports poisoned");
    let mut tool_counts = ToolCounts::default();
    let mut tokens = TokenUsage::default();
    for report in reports.iter() {
        tool_counts.add(report.tool_counts);
        tokens.add(report.tokens);
    }
    let findings = runtime.findings.all();
    let publishable_findings = findings
        .iter()
        .filter(|finding| {
            finding.validation_status == ValidationStatus::Validated
                && matches!(finding.publishability, FindingPublishability::Publishable)
        })
        .count();

    let mut report = RuntimeReport {
        model: runtime.model.default_model(),
        sessions: job.personas.len(),
        completed_sessions: reports
            .iter()
            .filter(|report| report.state == AgentState::Done)
            .count(),
        model_calls: reports.iter().map(|report| report.model_calls).sum(),
        tool_calls: tool_counts.total(),
        tool_counts,
        findings: runtime.findings.len(),
        publishable_findings,
        blackboard_entries: runtime.blackboard.len(),
        input_tokens: tokens.input_tokens,
        output_tokens: tokens.output_tokens,
        total_tokens: tokens.total_tokens,
        elapsed_ms: started.elapsed().as_millis() as u64,
        artifact_stats: artifacts.stats(),
        benchmark_valid: false,
        benchmark_failures: Vec::new(),
    };
    report.benchmark_failures = benchmark_failures(&report);
    report.benchmark_valid = report.benchmark_failures.is_empty();

    if let Some(emitter) = &emitter {
        let outcome = review_outcome(&report);
        let result = ReviewRunResultV1 {
            schema_version: SCHEMA_VERSION,
            run_id: job.run_id.clone(),
            attempt: job.attempt,
            outcome,
            publishability: if report.completed_sessions == report.sessions {
                Publishability::Publishable
            } else {
                Publishability::DiagnosticOnly
            },
            sessions: report.sessions,
            completed_sessions: report.completed_sessions,
            findings,
            tool_counts: report.tool_counts,
            model_calls: report.model_calls,
            tokens,
            artifact_stats: report.artifact_stats.clone(),
            elapsed_ms: report.elapsed_ms,
        };
        emitter.emit(
            EventLevel::Info,
            EventType::RunFinished,
            None,
            None,
            None,
            None,
            json!(result),
        );
    }

    Ok(report)
}

pub(crate) fn validate_job(job: &ReviewRunJobV1) -> Result<()> {
    if job.schema_version != SCHEMA_VERSION {
        bail!("unsupported schemaVersion {}", job.schema_version);
    }
    if job.model_profiles.is_empty() {
        bail!("at least one model profile is required");
    }
    if job.repo.default_cwd.is_absolute() {
        bail!("repo.defaultCwd must be repo-relative");
    }
    for root in &job.path_policy.allowed_roots {
        normalize_policy_path(root)?;
    }
    if job
        .scratch_policy
        .scratch_root
        .as_ref()
        .is_some_and(|root| {
            fs::canonicalize(root)
                .ok()
                .zip(fs::canonicalize(&job.repo.worktree_root).ok())
                .is_some_and(|(scratch, repo)| scratch.starts_with(repo))
        })
    {
        bail!("scratchRoot must be outside worktreeRoot");
    }
    Ok(())
}

pub(crate) fn build_sessions(job: &ReviewRunJobV1) -> Vec<AgentSession> {
    let personas = if job.personas.is_empty() {
        default_personas(job)
    } else {
        job.personas.clone()
    };
    personas
        .into_iter()
        .map(|persona| AgentSession {
            id: persona.id,
            run_id: job.run_id.clone(),
            role: persona.role,
            objective: persona.objective,
            cwd: persona.cwd.unwrap_or_else(|| job.repo.default_cwd.clone()),
            model_profile_id: persona
                .model_profile_id
                .unwrap_or_else(|| job.default_model_profile_id.clone()),
            state: AgentState::Ready,
            budget: persona.budget,
            allowed_tools: persona.allowed_tools,
            events: Vec::new(),
        })
        .collect()
}

pub(crate) fn default_personas(job: &ReviewRunJobV1) -> Vec<PersonaSpecV1> {
    (0..job.budgets.max_active_sessions.max(1))
        .map(|index| PersonaSpecV1 {
            id: format!("persona-{index}"),
            role: Role::for_index(index),
            objective: "Review the change for evidence-backed risks. Use read-only tools; finish when evidence is sufficient.".to_string(),
            cwd: Some(job.repo.default_cwd.clone()),
            model_profile_id: Some(job.default_model_profile_id.clone()),
            allowed_tools: ToolMask::review_read_only(),
            budget: AgentBudget {
                max_turns: 7,
                max_tool_calls: 14,
                max_prompt_tokens: 32_000,
                max_output_tokens: 1_024,
            },
        })
        .collect()
}

pub(crate) fn review_outcome(report: &RuntimeReport) -> ReviewOutcomeV1 {
    if report.completed_sessions < report.sessions {
        ReviewOutcomeV1::FailedPartial
    } else if report.findings > 0 {
        ReviewOutcomeV1::CompletedWithFindings
    } else {
        ReviewOutcomeV1::CompletedNoFindings
    }
}

pub(crate) fn benchmark_failures(report: &RuntimeReport) -> Vec<String> {
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
    if report.tool_counts.read_file == 0 {
        failures.push("read_file was not exercised".to_string());
    }
    if report.tool_counts.read_diff == 0 {
        failures.push("read_diff was not exercised".to_string());
    }
    if report.tool_counts.search_text == 0 {
        failures.push("search_text was not exercised".to_string());
    }
    if report.tool_calls == 0 {
        failures.push("no model-driven tool calls recorded".to_string());
    }
    if report.findings == 0 && report.tool_counts.finish == 0 {
        failures.push("no finding and no explicit finish rationale".to_string());
    }
    failures
}

#[derive(Debug)]
pub(crate) struct WorkQueue {
    pub(crate) inner: Mutex<VecDeque<AgentSession>>,
    pub(crate) done: Condvar,
}

impl WorkQueue {
    pub(crate) fn new(queue: VecDeque<AgentSession>) -> Self {
        Self {
            inner: Mutex::new(queue),
            done: Condvar::new(),
        }
    }

    pub(crate) fn pop(&self) -> Option<AgentSession> {
        let mut queue = self.inner.lock().expect("queue poisoned");
        let value = queue.pop_front();
        if value.is_none() {
            self.done.notify_all();
        }
        value
    }
}
