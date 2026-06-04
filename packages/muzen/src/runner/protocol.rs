use std::io::Write;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) fn stateful_method(method: &str) -> bool {
    matches!(
        method,
        "run.start"
            | "run.cancel"
            | "run.status"
            | "run.result"
            | "artifact.read"
            | "artifact.export"
            | "snapshot.readText"
    )
}

pub(crate) fn write_response<W: Write>(writer: &mut W, response: &JsonRpcResponse) -> Result<()> {
    serde_json::to_writer(&mut *writer, response)
        .context("failed to write runner protocol response")?;
    writer
        .write_all(b"\n")
        .context("failed to terminate runner protocol response")?;
    writer.flush().context("failed to flush runner response")?;
    Ok(())
}

pub(crate) fn write_notification<W: Write>(
    writer: &mut W,
    method: &str,
    params: Value,
) -> Result<()> {
    let notification = JsonRpcNotification {
        jsonrpc: "2.0".to_string(),
        method: method.to_string(),
        params,
    };
    serde_json::to_writer(&mut *writer, &notification)
        .context("failed to write runner protocol notification")?;
    writer
        .write_all(b"\n")
        .context("failed to terminate runner protocol notification")?;
    writer
        .flush()
        .context("failed to flush runner protocol notification")?;
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) enum JsonRpcFrame {
    Request(JsonRpcRequest),
    Response(JsonRpcResponse),
    Notification,
}

pub(crate) fn parse_jsonrpc_frame(line: &str) -> Result<JsonRpcFrame> {
    let value = serde_json::from_str::<Value>(line)
        .with_context(|| format!("invalid JSON-RPC frame: {line}"))?;
    if value.get("method").is_some() {
        if value.get("id").is_some() {
            Ok(JsonRpcFrame::Request(serde_json::from_value(value)?))
        } else {
            Ok(JsonRpcFrame::Notification)
        }
    } else {
        Ok(JsonRpcFrame::Response(serde_json::from_value(value)?))
    }
}

pub(crate) fn parse_params<T>(params: Option<Value>) -> Result<T, JsonRpcError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|error| JsonRpcError::invalid_params(error.to_string()))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcNotification {
    pub jsonrpc: String,
    pub method: String,
    pub params: Value,
}

impl JsonRpcResponse {
    pub(crate) fn success(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(result),
            error: None,
        }
    }

    pub(crate) fn error(id: Option<Value>, error: JsonRpcError) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<RunnerErrorData>,
}

impl JsonRpcError {
    pub(crate) fn parse_error(message: impl Into<String>) -> Self {
        Self::new(-32700, "protocol_error", message)
    }

    pub(crate) fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(-32600, "invalid_request", message)
    }

    pub(crate) fn method_not_found(message: impl Into<String>) -> Self {
        Self::new(-32601, "method_not_found", message)
    }

    pub(crate) fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(-32602, "invalid_input", message)
    }

    pub(crate) fn protocol_error(message: impl Into<String>) -> Self {
        Self::new(-32000, "protocol_error", message)
    }

    pub(crate) fn not_implemented(message: impl Into<String>) -> Self {
        Self::new(-32001, "not_implemented", message)
    }

    pub(crate) fn runner_error(message: impl Into<String>) -> Self {
        Self::new(-32002, "runner_error", message)
    }

    pub(crate) fn limit_exceeded(kind: impl Into<String>) -> Self {
        let kind = kind.into();
        Self::new(-32003, "limit_exceeded", format!("limit exceeded: {kind}"))
    }

    fn new(code: i64, kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: Some(RunnerErrorData { kind: kind.into() }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunnerErrorData {
    pub kind: String,
}
