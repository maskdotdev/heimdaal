use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::json;

use super::execution::execute_run_start;
use super::protocol::{
    parse_params, stateful_method, write_notification, write_response, JsonRpcError, JsonRpcFrame,
    JsonRpcRequest, JsonRpcResponse,
};
use super::schema::{protocol_schema, runner_check, runner_handshake};
use super::stored::RunnerStoredRun;
use super::transport::{InteractiveTransport, RunnerCallbackTransport};
use super::types::{
    ArtifactExportParams, ArtifactReadParams, RunCancelResult, RunLookupParams, RunStartParams,
    RunStatusResult, RunnerArtifactExportResult, RunnerArtifactReadResult, RunnerHandshakeParams,
    RunnerSnapshotTextResult, SnapshotReadTextParams,
};
use super::RUNNER_PROTOCOL_VERSION;

pub fn run_stdio<R, W>(reader: &mut R, writer: &mut W) -> Result<i32>
where
    R: BufRead,
    W: Write,
{
    let mut session = RunnerStdioSession::default();
    let mut line = String::new();
    loop {
        line.clear();
        let bytes = reader
            .read_line(&mut line)
            .context("failed to read runner protocol frame")?;
        if bytes == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        session.handle_line(line.trim_end(), writer)?;
    }
    Ok(0)
}

pub fn run_stdio_interactive<R, W>(reader: R, writer: W) -> Result<i32>
where
    R: BufRead + Send + 'static,
    W: Write + Send + 'static,
{
    let transport = Arc::new(InteractiveTransport::new(reader, writer));
    let mut session = RunnerStdioSession::default();
    loop {
        let frame = transport.read_frame()?;
        let Some(frame) = frame else {
            break;
        };
        match frame {
            JsonRpcFrame::Request(request) => {
                let response = session.handle_interactive_request(request, transport.clone())?;
                transport.write_response(&response)?;
            }
            JsonRpcFrame::Response(response) => {
                let error = JsonRpcResponse::error(
                    response.id,
                    JsonRpcError::protocol_error("runner received an unexpected JSON-RPC response"),
                );
                transport.write_response(&error)?;
            }
            JsonRpcFrame::Notification => {}
        }
    }
    Ok(0)
}

pub fn handle_jsonrpc_line(line: &str) -> JsonRpcResponse {
    match serde_json::from_str::<JsonRpcRequest>(line) {
        Ok(request) => handle_request(request),
        Err(error) => JsonRpcResponse::error(
            None,
            JsonRpcError::parse_error(format!("invalid JSON-RPC request: {error}")),
        ),
    }
}

fn handle_request(request: JsonRpcRequest) -> JsonRpcResponse {
    if request.jsonrpc != "2.0" {
        return JsonRpcResponse::error(
            request.id,
            JsonRpcError::invalid_request("jsonrpc must be 2.0"),
        );
    }
    match request.method.as_str() {
        "runner.handshake" => {
            let params = parse_params::<RunnerHandshakeParams>(request.params);
            match params {
                Ok(params) => {
                    if params.protocol_version != RUNNER_PROTOCOL_VERSION {
                        return JsonRpcResponse::error(
                            request.id,
                            JsonRpcError::protocol_error(format!(
                                "unsupported protocolVersion {}",
                                params.protocol_version
                            )),
                        );
                    }
                    JsonRpcResponse::success(request.id, json!(runner_handshake()))
                }
                Err(error) => JsonRpcResponse::error(request.id, error),
            }
        }
        "runner.check" => JsonRpcResponse::success(request.id, json!(runner_check())),
        "runner.schema.export" => JsonRpcResponse::success(request.id, json!(protocol_schema())),
        "run.start" | "run.cancel" | "run.status" | "run.result" | "artifact.read"
        | "artifact.export" | "snapshot.readText" => JsonRpcResponse::error(
            request.id,
            JsonRpcError::not_implemented(format!(
                "{} requires the stateful stdio session in {}",
                request.method, RUNNER_PROTOCOL_VERSION
            )),
        ),
        _ => JsonRpcResponse::error(
            request.id,
            JsonRpcError::method_not_found(format!("unknown method {}", request.method)),
        ),
    }
}

#[derive(Default)]
pub(crate) struct RunnerStdioSession {
    reports: BTreeMap<String, RunnerStoredRun>,
}

impl RunnerStdioSession {
    pub(crate) fn handle_line<W: Write>(&mut self, line: &str, writer: &mut W) -> Result<()> {
        let response = match serde_json::from_str::<JsonRpcRequest>(line) {
            Ok(request) => self.handle_stateful_request(request, writer)?,
            Err(error) => JsonRpcResponse::error(
                None,
                JsonRpcError::parse_error(format!("invalid JSON-RPC request: {error}")),
            ),
        };
        write_response(writer, &response)?;
        Ok(())
    }

    fn handle_stateful_request<W: Write>(
        &mut self,
        request: JsonRpcRequest,
        writer: &mut W,
    ) -> Result<JsonRpcResponse> {
        if !stateful_method(request.method.as_str()) {
            return Ok(handle_request(request));
        }
        if request.jsonrpc != "2.0" {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::invalid_request("jsonrpc must be 2.0"),
            ));
        }
        match request.method.as_str() {
            "run.start" => {
                let params = match parse_params::<RunStartParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
                };
                match execute_run_start(params, None) {
                    Ok(executed) => {
                        for event in &executed.events {
                            write_notification(writer, "event.review", json!(event))?;
                        }
                        let result = executed.result.clone();
                        write_notification(writer, "run.finished", json!(result.clone()))?;
                        self.reports.insert(result.run_id.clone(), executed.stored);
                        Ok(JsonRpcResponse::success(request.id, json!(result)))
                    }
                    Err(error) => {
                        let runner_error = JsonRpcError::runner_error(error.to_string());
                        write_notification(
                            writer,
                            "run.failed",
                            json!({"error": runner_error.message, "kind": "runner_error"}),
                        )?;
                        Ok(JsonRpcResponse::error(request.id, runner_error))
                    }
                }
            }
            "run.status" => {
                let params = match parse_params::<RunLookupParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
                };
                let Some(stored) = self.reports.get(&params.run_id) else {
                    return Ok(JsonRpcResponse::error(
                        request.id,
                        JsonRpcError::invalid_params(format!("unknown runId {}", params.run_id)),
                    ));
                };
                Ok(JsonRpcResponse::success(
                    request.id,
                    json!(RunStatusResult {
                        run_id: params.run_id,
                        status: stored.status().to_string(),
                    }),
                ))
            }
            "run.result" => {
                let params = match parse_params::<RunLookupParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
                };
                let Some(stored) = self.reports.get(&params.run_id) else {
                    return Ok(JsonRpcResponse::error(
                        request.id,
                        JsonRpcError::invalid_params(format!("unknown runId {}", params.run_id)),
                    ));
                };
                Ok(JsonRpcResponse::success(
                    request.id,
                    json!(stored.result().clone()),
                ))
            }
            "run.cancel" => self.handle_run_cancel(request),
            "artifact.read" => self.handle_artifact_read(request),
            "artifact.export" => self.handle_artifact_export(request),
            "snapshot.readText" => self.handle_snapshot_read_text(request),
            _ => Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::method_not_found(format!("unknown method {}", request.method)),
            )),
        }
    }

    fn handle_interactive_request<T>(
        &mut self,
        request: JsonRpcRequest,
        transport: Arc<T>,
    ) -> Result<JsonRpcResponse>
    where
        T: RunnerCallbackTransport + 'static,
    {
        if !stateful_method(request.method.as_str()) {
            return Ok(handle_request(request));
        }
        if request.jsonrpc != "2.0" {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::invalid_request("jsonrpc must be 2.0"),
            ));
        }
        if request.method.as_str() != "run.start" {
            return self.handle_stateful_request_without_notifications(request);
        }
        let params = match parse_params::<RunStartParams>(request.params) {
            Ok(params) => params,
            Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
        };
        let transport: Arc<dyn RunnerCallbackTransport> = transport;
        match execute_run_start(params, Some(transport.clone())) {
            Ok(executed) => {
                let result = executed.result.clone();
                transport.notify("run.finished", json!(result.clone()))?;
                self.reports.insert(result.run_id.clone(), executed.stored);
                Ok(JsonRpcResponse::success(request.id, json!(result)))
            }
            Err(error) => {
                let runner_error = JsonRpcError::runner_error(error.to_string());
                transport.notify(
                    "run.failed",
                    json!({"error": runner_error.message, "kind": "runner_error"}),
                )?;
                Ok(JsonRpcResponse::error(request.id, runner_error))
            }
        }
    }

    fn handle_stateful_request_without_notifications(
        &mut self,
        request: JsonRpcRequest,
    ) -> Result<JsonRpcResponse> {
        match request.method.as_str() {
            "run.status" => {
                let params = match parse_params::<RunLookupParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
                };
                let Some(stored) = self.reports.get(&params.run_id) else {
                    return Ok(JsonRpcResponse::error(
                        request.id,
                        JsonRpcError::invalid_params(format!("unknown runId {}", params.run_id)),
                    ));
                };
                Ok(JsonRpcResponse::success(
                    request.id,
                    json!(RunStatusResult {
                        run_id: params.run_id,
                        status: stored.status().to_string(),
                    }),
                ))
            }
            "run.result" => {
                let params = match parse_params::<RunLookupParams>(request.params) {
                    Ok(params) => params,
                    Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
                };
                let Some(stored) = self.reports.get(&params.run_id) else {
                    return Ok(JsonRpcResponse::error(
                        request.id,
                        JsonRpcError::invalid_params(format!("unknown runId {}", params.run_id)),
                    ));
                };
                Ok(JsonRpcResponse::success(
                    request.id,
                    json!(stored.result().clone()),
                ))
            }
            "run.cancel" => self.handle_run_cancel(request),
            "artifact.read" => self.handle_artifact_read(request),
            "artifact.export" => self.handle_artifact_export(request),
            "snapshot.readText" => self.handle_snapshot_read_text(request),
            _ => Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::method_not_found(format!("unknown method {}", request.method)),
            )),
        }
    }

    fn handle_run_cancel(&mut self, request: JsonRpcRequest) -> Result<JsonRpcResponse> {
        let params = match parse_params::<RunLookupParams>(request.params) {
            Ok(params) => params,
            Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
        };
        let Some(stored) = self.reports.get(&params.run_id) else {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::invalid_params(format!("unknown runId {}", params.run_id)),
            ));
        };
        Ok(JsonRpcResponse::success(
            request.id,
            json!(RunCancelResult {
                run_id: params.run_id,
                status: stored.status().to_string(),
                cancelled: false,
                reason: "run already reached a terminal state".to_string(),
            }),
        ))
    }

    fn handle_artifact_read(&self, request: JsonRpcRequest) -> Result<JsonRpcResponse> {
        let params = match parse_params::<ArtifactReadParams>(request.params) {
            Ok(params) => params,
            Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
        };
        let Some(stored) = self.reports.get(&params.run_id) else {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::invalid_params(format!("unknown runId {}", params.run_id)),
            ));
        };
        let Some(artifact) = stored.artifact(params.view, &params.artifact_id) else {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::invalid_params(format!("unknown artifactId {}", params.artifact_id)),
            ));
        };
        Ok(JsonRpcResponse::success(
            request.id,
            json!(RunnerArtifactReadResult {
                run_id: params.run_id,
                view: params.view,
                artifact: artifact.clone(),
            }),
        ))
    }

    fn handle_artifact_export(&self, request: JsonRpcRequest) -> Result<JsonRpcResponse> {
        let params = match parse_params::<ArtifactExportParams>(request.params) {
            Ok(params) => params,
            Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
        };
        let Some(stored) = self.reports.get(&params.run_id) else {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::invalid_params(format!("unknown runId {}", params.run_id)),
            ));
        };
        let mut artifacts = stored.artifacts(params.view).to_vec();
        if !params.artifact_ids.is_empty() {
            artifacts.retain(|artifact| {
                params
                    .artifact_ids
                    .iter()
                    .any(|artifact_id| artifact_id == &artifact.artifact_id)
            });
        }
        let total_bytes = artifacts
            .iter()
            .map(|artifact| artifact.bytes)
            .sum::<usize>();
        if params
            .max_artifacts
            .is_some_and(|max_artifacts| artifacts.len() > max_artifacts)
        {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::limit_exceeded("artifact_retention_artifacts"),
            ));
        }
        if params
            .max_bytes
            .is_some_and(|max_bytes| total_bytes > max_bytes)
        {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::limit_exceeded("artifact_retention_bytes"),
            ));
        }
        Ok(JsonRpcResponse::success(
            request.id,
            json!(RunnerArtifactExportResult {
                run_id: params.run_id,
                view: params.view,
                artifact_count: artifacts.len(),
                total_bytes,
                artifacts,
            }),
        ))
    }

    fn handle_snapshot_read_text(&self, request: JsonRpcRequest) -> Result<JsonRpcResponse> {
        let params = match parse_params::<SnapshotReadTextParams>(request.params) {
            Ok(params) => params,
            Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
        };
        let Some(stored) = self.reports.get(&params.run_id) else {
            return Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::invalid_params(format!("unknown runId {}", params.run_id)),
            ));
        };
        let reader = match stored.snapshot_reader(params.snapshot_id.as_deref()) {
            Ok(reader) => reader,
            Err(error) => return Ok(JsonRpcResponse::error(request.id, error)),
        };
        let max_bytes = params.max_bytes.unwrap_or(200 * 1024);
        match reader.read_text_path(&params.path, max_bytes) {
            Ok(file) => Ok(JsonRpcResponse::success(
                request.id,
                json!(RunnerSnapshotTextResult {
                    run_id: params.run_id,
                    snapshot_id: file.snapshot_id.0,
                    path: file.path.display(),
                    content_hash: file.content_hash,
                    bytes: file.bytes,
                    truncated: file.truncated,
                    content: file.content,
                }),
            )),
            Err(error) => Ok(JsonRpcResponse::error(
                request.id,
                JsonRpcError::runner_error(error.to_string()),
            )),
        }
    }
}
