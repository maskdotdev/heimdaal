pub const RUNNER_PROTOCOL_VERSION: &str = "muzen.runner.v1";
pub const RUNNER_NAME: &str = "muzen-runner";

mod adapters;
mod cli;
mod execution;
mod protocol;
mod schema;
mod session;
mod stored;
mod transport;
mod types;

pub use cli::{main_entry, run_main, RunnerCli, RunnerCommand, RunnerSchemaCommand};
pub use protocol::{
    JsonRpcError, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, RunnerErrorData,
};
pub use schema::{protocol_schema, runner_check, runner_handshake};
pub use session::{handle_jsonrpc_line, run_stdio, run_stdio_interactive};
pub use types::*;

#[cfg(test)]
pub(crate) use session::RunnerStdioSession;

#[cfg(test)]
mod tests {
    use std::io::{Result as IoResult, Write};
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use super::*;

    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, bytes: &[u8]) -> IoResult<usize> {
            self.0.lock().expect("writer lock").extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> IoResult<()> {
            Ok(())
        }
    }

    #[test]
    fn handshake_returns_protocol_version() {
        let response = handle_jsonrpc_line(
            r#"{"jsonrpc":"2.0","id":1,"method":"runner.handshake","params":{"protocolVersion":"muzen.runner.v1","clientName":"test"}}"#,
        );

        assert!(response.error.is_none());
        assert_eq!(response.id, Some(json!(1)));
        assert_eq!(
            response
                .result
                .as_ref()
                .and_then(|value| value.get("protocolVersion")),
            Some(&json!(RUNNER_PROTOCOL_VERSION))
        );
    }

    #[test]
    fn handshake_rejects_protocol_mismatch() {
        let response = handle_jsonrpc_line(
            r#"{"jsonrpc":"2.0","id":"bad","method":"runner.handshake","params":{"protocolVersion":"muzen.runner.v0"}}"#,
        );

        let error = response.error.expect("protocol error");
        assert_eq!(error.data.expect("error data").kind, "protocol_error");
    }

    #[test]
    fn schema_marks_wired_run_methods_and_callbacks_implemented() {
        let schema = protocol_schema();

        assert!(schema
            .requests
            .iter()
            .any(|method| method.method == "run.start"
                && method.status == RunnerMethodStatus::Implemented));
        assert!(schema
            .requests
            .iter()
            .any(|method| method.method == "run.result"
                && method.status == RunnerMethodStatus::Implemented));
        assert!(schema
            .requests
            .iter()
            .any(|method| method.method == "artifact.read"
                && method.status == RunnerMethodStatus::Implemented));
        assert!(schema
            .requests
            .iter()
            .any(|method| method.method == "snapshot.readText"
                && method.status == RunnerMethodStatus::Implemented));
        assert!(schema
            .notifications
            .iter()
            .any(|method| method.method == "event.review"
                && method.status == RunnerMethodStatus::Implemented));
        assert!(schema
            .callbacks
            .iter()
            .any(|method| method.method == "model.complete"
                && method.status == RunnerMethodStatus::Implemented));
        assert!(schema
            .callbacks
            .iter()
            .any(|method| method.method == "tool.execute"
                && method.status == RunnerMethodStatus::Implemented));
        assert!(schema
            .notifications
            .iter()
            .any(|method| method.method == "event.runtime"
                && method.status == RunnerMethodStatus::Implemented));
    }

    #[test]
    fn stdio_handles_multiple_requests() {
        let input = br#"{"jsonrpc":"2.0","id":1,"method":"runner.check"}
{"jsonrpc":"2.0","id":2,"method":"runner.schema.export"}
"#;
        let mut reader = std::io::Cursor::new(input);
        let mut writer = Vec::new();

        run_stdio(&mut reader, &mut writer).expect("stdio run");

        let output = String::from_utf8(writer).expect("utf8 output");
        let lines = output.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"ok\":true"));
        assert!(lines[1].contains("runner.schema.export"));
    }

    #[test]
    fn stdio_starts_run_emits_events_and_stores_result() {
        let repo = tempfile::tempdir().expect("temp repo");
        std::fs::write(
            repo.path().join("Cargo.toml"),
            "[package]\nname = \"fixture\"\nversion = \"0.0.0\"\n",
        )
        .expect("fixture file");
        let start = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "run.start",
            "params": {
                "protocolVersion": RUNNER_PROTOCOL_VERSION,
                "runId": "fixture-run",
                "repo": repo.path(),
                "changedFiles": ["Cargo.toml"],
                "sessions": [
                    {
                        "id": "security",
                        "role": "security",
                        "objective": "Check fixture repo"
                    }
                ]
            }
        });
        let status = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "run.status",
            "params": {"runId": "fixture-run"}
        });
        let result = json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "run.result",
            "params": {"runId": "fixture-run"}
        });
        let input = format!("{start}\n{status}\n{result}\n");
        let mut reader = std::io::Cursor::new(input.into_bytes());
        let mut writer = Vec::new();

        run_stdio(&mut reader, &mut writer).expect("stdio run");

        let output = String::from_utf8(writer).expect("utf8 output");
        let values = output
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json line"))
            .collect::<Vec<_>>();
        assert!(values
            .iter()
            .any(|value| value.get("method") == Some(&json!("event.review"))));
        assert!(values
            .iter()
            .any(|value| value.get("method") == Some(&json!("run.finished"))));
        let start_response = values
            .iter()
            .find(|value| value.get("id") == Some(&json!(1)))
            .expect("start response");
        assert_eq!(start_response["result"]["runId"], "fixture-run");
        assert_eq!(start_response["result"]["status"], "completed");
        assert_eq!(start_response["result"]["summary"]["completedSessions"], 1);
        let status_response = values
            .iter()
            .find(|value| value.get("id") == Some(&json!(2)))
            .expect("status response");
        assert_eq!(status_response["result"]["status"], "completed");
        let result_response = values
            .iter()
            .find(|value| value.get("id") == Some(&json!(3)))
            .expect("result response");
        assert_eq!(result_response["result"]["runId"], "fixture-run");
    }

    #[test]
    fn stdio_reads_artifacts_snapshots_and_cancel_status() {
        let repo = tempfile::tempdir().expect("temp repo");
        std::fs::write(
            repo.path().join("Cargo.toml"),
            "[package]\nname = \"fixture\"\nversion = \"0.0.0\"\n",
        )
        .expect("fixture file");
        let mut session = RunnerStdioSession::default();
        let mut writer = Vec::new();
        let start = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "run.start",
            "params": {
                "protocolVersion": RUNNER_PROTOCOL_VERSION,
                "runId": "resource-run",
                "repo": repo.path(),
                "changedFiles": ["Cargo.toml"],
                "sessions": [
                    {
                        "id": "security",
                        "role": "security",
                        "objective": "Check fixture repo"
                    }
                ]
            }
        });

        session
            .handle_line(&start.to_string(), &mut writer)
            .expect("start run");
        let start_values = parse_json_lines(&writer);
        let artifact_id = start_values
            .iter()
            .find_map(|value| value.get("params")?.get("artifactId")?.as_str())
            .expect("artifact id")
            .to_string();
        let snapshot_id = start_values
            .iter()
            .find(|value| value.get("id") == Some(&json!(1)))
            .and_then(|value| value["result"]["snapshots"][0]["snapshotId"].as_str())
            .expect("snapshot id")
            .to_string();

        let read = send_jsonrpc(
            &mut session,
            &mut writer,
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "artifact.read",
                "params": {"runId": "resource-run", "artifactId": artifact_id}
            }),
        );
        assert_eq!(read[0]["result"]["view"], "redacted");
        assert!(read[0]["result"]["artifact"]["content"]
            .as_str()
            .expect("artifact content")
            .contains("Cargo.toml"));

        let export = send_jsonrpc(
            &mut session,
            &mut writer,
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "artifact.export",
                "params": {
                    "runId": "resource-run",
                    "artifactIds": [artifact_id],
                    "maxArtifacts": 1,
                    "maxBytes": 1000
                }
            }),
        );
        assert_eq!(export[0]["result"]["artifactCount"], 1);

        let snapshot = send_jsonrpc(
            &mut session,
            &mut writer,
            json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "snapshot.readText",
                "params": {
                    "runId": "resource-run",
                    "snapshotId": snapshot_id,
                    "path": "Cargo.toml",
                    "maxBytes": 1000
                }
            }),
        );
        assert_eq!(snapshot[0]["result"]["path"], "Cargo.toml");
        assert!(snapshot[0]["result"]["content"]
            .as_str()
            .expect("snapshot content")
            .contains("fixture"));

        let cancel = send_jsonrpc(
            &mut session,
            &mut writer,
            json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "run.cancel",
                "params": {"runId": "resource-run"}
            }),
        );
        assert_eq!(cancel[0]["result"]["status"], "completed");
        assert_eq!(cancel[0]["result"]["cancelled"], false);
    }

    #[test]
    fn interactive_stdio_runs_model_and_tool_callbacks() {
        let repo = tempfile::tempdir().expect("temp repo");
        std::fs::write(
            repo.path().join("Cargo.toml"),
            "[package]\nname = \"fixture\"\nversion = \"0.0.0\"\n",
        )
        .expect("fixture file");
        let start = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "run.start",
            "params": {
                "protocolVersion": RUNNER_PROTOCOL_VERSION,
                "runId": "interactive-run",
                "repo": repo.path(),
                "changedFiles": ["Cargo.toml"],
                "model": {"callback": true},
                "tools": [
                    {
                        "id": "host_context",
                        "description": "Return host-supplied context.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "topic": {"type": "string"}
                            },
                            "required": ["topic"],
                            "additionalProperties": false
                        }
                    }
                ],
                "sessions": [
                    {
                        "id": "callback-session",
                        "role": "correctness",
                        "objective": "Exercise SDK callbacks"
                    }
                ],
                "limits": {"maxActiveSessions": 1}
            }
        });
        let first_model = json!({
            "jsonrpc": "2.0",
            "id": "runner-callback-1",
            "result": {
                "toolCalls": [
                    {"toolId": "read_diff", "arguments": {}},
                    {"toolId": "read_file", "arguments": {"path": "Cargo.toml"}},
                    {"toolId": "host_context", "arguments": {"topic": "sdk"}},
                    {"toolId": "search_text", "arguments": {"query": "fixture"}}
                ],
                "usage": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15}
            }
        });
        let tool_result = json!({
            "jsonrpc": "2.0",
            "id": "runner-callback-2",
            "result": {
                "data": {"topic": "sdk", "message": "host context received"},
                "artifact": {"key": "host-context", "content": "context artifact"}
            }
        });
        let second_model = json!({
            "jsonrpc": "2.0",
            "id": "runner-callback-3",
            "result": {
                "toolCalls": [
                    {"toolId": "finish", "arguments": {"reason": "callback test complete"}}
                ],
                "usage": {"inputTokens": 20, "outputTokens": 5, "totalTokens": 25}
            }
        });
        let input = format!("{start}\n{first_model}\n{tool_result}\n{second_model}\n");
        let reader = std::io::Cursor::new(input.into_bytes());
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = SharedWriter(output.clone());

        run_stdio_interactive(reader, writer).expect("interactive stdio");

        let bytes = output.lock().expect("output lock").clone();
        let values = parse_json_lines(&bytes);
        assert!(values
            .iter()
            .any(|value| value.get("method") == Some(&json!("model.complete"))));
        assert!(values
            .iter()
            .any(|value| value.get("method") == Some(&json!("tool.execute"))));
        assert!(values
            .iter()
            .any(|value| value.get("method") == Some(&json!("event.runtime"))));
        assert!(values
            .iter()
            .any(|value| value.get("method") == Some(&json!("event.review"))));
        assert!(values
            .iter()
            .any(|value| value.get("method") == Some(&json!("run.finished"))));
        let start_response = values
            .iter()
            .find(|value| value.get("id") == Some(&json!(1)))
            .expect("start response");
        assert_eq!(start_response["result"]["status"], "completed");
        assert_eq!(start_response["result"]["summary"]["completedSessions"], 1);
    }

    fn send_jsonrpc(
        session: &mut RunnerStdioSession,
        writer: &mut Vec<u8>,
        request: serde_json::Value,
    ) -> Vec<serde_json::Value> {
        let start = writer.len();
        session
            .handle_line(&request.to_string(), writer)
            .expect("handle request");
        parse_json_lines(&writer[start..])
    }

    fn parse_json_lines(bytes: &[u8]) -> Vec<serde_json::Value> {
        let output = std::str::from_utf8(bytes).expect("utf8 output");
        output
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json line"))
            .collect()
    }

    #[test]
    fn handshake_fixture_matches_current_response() {
        let fixture = include_str!("../fixtures/runner-handshake-v1.jsonl");
        let lines = fixture.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);

        let actual = serde_json::to_value(handle_jsonrpc_line(lines[0])).expect("actual response");
        let expected: serde_json::Value =
            serde_json::from_str(lines[1]).expect("expected response");

        assert_eq!(actual, expected);
    }

    #[test]
    fn schema_fixture_matches_current_schema() {
        let actual = serde_json::to_value(protocol_schema()).expect("actual schema");
        let expected: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/runner-schema-v1.json"))
                .expect("expected schema");

        assert_eq!(actual, expected);
    }
}
