use super::types::{
    RunnerCapabilities, RunnerCheckResult, RunnerHandshakeResult, RunnerMessageDirection,
    RunnerMethodSchema, RunnerMethodStatus, RunnerProtocolSchema,
};
use super::{RUNNER_NAME, RUNNER_PROTOCOL_VERSION};

pub fn runner_handshake() -> RunnerHandshakeResult {
    RunnerHandshakeResult {
        protocol_version: RUNNER_PROTOCOL_VERSION.to_string(),
        runner_name: RUNNER_NAME.to_string(),
        runner_version: env!("CARGO_PKG_VERSION").to_string(),
        capabilities: RunnerCapabilities {
            supported_methods: vec![
                "runner.handshake".to_string(),
                "runner.check".to_string(),
                "runner.schema.export".to_string(),
                "run.start".to_string(),
                "run.cancel".to_string(),
                "run.status".to_string(),
                "run.result".to_string(),
                "artifact.read".to_string(),
                "artifact.export".to_string(),
                "snapshot.readText".to_string(),
                "model.complete".to_string(),
                "tool.execute".to_string(),
                "event.review".to_string(),
                "event.runtime".to_string(),
                "run.finished".to_string(),
                "run.failed".to_string(),
            ],
            planned_methods: Vec::new(),
            transports: vec!["stdio-jsonl".to_string()],
        },
    }
}

pub fn runner_check() -> RunnerCheckResult {
    RunnerCheckResult {
        ok: true,
        protocol_version: RUNNER_PROTOCOL_VERSION.to_string(),
        runner_name: RUNNER_NAME.to_string(),
        runner_version: env!("CARGO_PKG_VERSION").to_string(),
        rust_package: env!("CARGO_PKG_NAME").to_string(),
    }
}

pub fn protocol_schema() -> RunnerProtocolSchema {
    RunnerProtocolSchema {
        schema_version: RUNNER_PROTOCOL_VERSION.to_string(),
        transport: "newline-delimited JSON-RPC 2.0 over stdio".to_string(),
        requests: vec![
            implemented(
                "runner.handshake",
                "Negotiate protocol version and capabilities.",
            ),
            implemented("runner.check", "Return local runner diagnostics."),
            implemented(
                "runner.schema.export",
                "Return protocol method metadata for SDK validation.",
            ),
            implemented("run.start", "Start a review run."),
            implemented("run.cancel", "Cancel an active review run."),
            implemented("run.status", "Read active run status."),
            implemented("run.result", "Read final run report."),
            implemented("artifact.read", "Read one redacted or raw artifact."),
            implemented("artifact.export", "Export artifacts using a policy."),
            implemented("snapshot.readText", "Read captured snapshot text."),
        ],
        callbacks: vec![
            implemented_runner_to_sdk(
                "model.complete",
                "Ask the SDK model adapter for one model turn.",
            ),
            implemented_runner_to_sdk("tool.execute", "Ask the SDK to execute a host custom tool."),
        ],
        notifications: vec![
            implemented_runner_to_sdk("event.review", "Emit one host-facing review event."),
            implemented_runner_to_sdk("event.runtime", "Emit one advanced runtime event."),
            implemented_runner_to_sdk(
                "run.finished",
                "Notify that a run reached a terminal state.",
            ),
            implemented_runner_to_sdk(
                "run.failed",
                "Notify that a run failed before producing a report.",
            ),
        ],
    }
}

fn implemented(method: &'static str, summary: &'static str) -> RunnerMethodSchema {
    RunnerMethodSchema {
        method: method.to_string(),
        direction: RunnerMessageDirection::SdkToRunner,
        status: RunnerMethodStatus::Implemented,
        summary: summary.to_string(),
    }
}

fn implemented_runner_to_sdk(method: &'static str, summary: &'static str) -> RunnerMethodSchema {
    RunnerMethodSchema {
        method: method.to_string(),
        direction: RunnerMessageDirection::RunnerToSdk,
        status: RunnerMethodStatus::Implemented,
        summary: summary.to_string(),
    }
}
