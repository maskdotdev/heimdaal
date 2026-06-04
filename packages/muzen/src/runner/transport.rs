use std::io::{BufRead, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use anyhow::{Context, Result};
use serde_json::{json, Value};

use super::protocol::{
    parse_jsonrpc_frame, write_notification, write_response, JsonRpcError, JsonRpcFrame,
    JsonRpcResponse,
};

#[derive(Debug, Clone, serde::Serialize)]
struct JsonRpcOutboundRequest {
    jsonrpc: &'static str,
    id: Value,
    method: String,
    params: Value,
}

pub(crate) trait RunnerCallbackTransport: Send + Sync {
    fn request(&self, method: &str, params: Value) -> Result<Value>;
    fn notify(&self, method: &str, params: Value) -> Result<()>;
}

pub(crate) struct InteractiveTransport<R, W> {
    state: Mutex<InteractiveTransportState<R, W>>,
    next_request_id: AtomicU64,
}

pub(crate) struct InteractiveTransportState<R, W> {
    reader: R,
    writer: W,
    line: String,
}

impl<R, W> InteractiveTransport<R, W>
where
    R: BufRead,
    W: Write,
{
    pub(crate) fn new(reader: R, writer: W) -> Self {
        Self {
            state: Mutex::new(InteractiveTransportState {
                reader,
                writer,
                line: String::new(),
            }),
            next_request_id: AtomicU64::new(1),
        }
    }

    pub(crate) fn read_frame(&self) -> Result<Option<JsonRpcFrame>> {
        let mut state = self.state.lock().expect("runner stdio lock poisoned");
        state.read_frame()
    }

    pub(crate) fn write_response(&self, response: &JsonRpcResponse) -> Result<()> {
        let mut state = self.state.lock().expect("runner stdio lock poisoned");
        write_response(&mut state.writer, response)
    }
}

impl<R, W> RunnerCallbackTransport for InteractiveTransport<R, W>
where
    R: BufRead + Send,
    W: Write + Send,
{
    fn request(&self, method: &str, params: Value) -> Result<Value> {
        let request_id = format!(
            "runner-callback-{}",
            self.next_request_id.fetch_add(1, Ordering::SeqCst)
        );
        let request_id_value = json!(request_id);
        let mut state = self.state.lock().expect("runner stdio lock poisoned");
        state.write_request(&request_id_value, method, params)?;
        loop {
            let Some(frame) = state.read_frame()? else {
                anyhow::bail!("SDK closed stdio while waiting for {method} response");
            };
            match frame {
                JsonRpcFrame::Response(response)
                    if response.id == Some(request_id_value.clone()) =>
                {
                    if let Some(error) = response.error {
                        anyhow::bail!(
                            "SDK callback {method} failed: {} ({})",
                            error.message,
                            error
                                .data
                                .as_ref()
                                .map(|data| data.kind.as_str())
                                .unwrap_or("unknown")
                        );
                    }
                    return Ok(response.result.unwrap_or(Value::Null));
                }
                JsonRpcFrame::Response(_) | JsonRpcFrame::Notification => {}
                JsonRpcFrame::Request(request) => {
                    let response = JsonRpcResponse::error(
                        request.id,
                        JsonRpcError::protocol_error(
                            "runner cannot service nested SDK-to-runner requests during callback wait",
                        ),
                    );
                    write_response(&mut state.writer, &response)?;
                }
            }
        }
    }

    fn notify(&self, method: &str, params: Value) -> Result<()> {
        let mut state = self.state.lock().expect("runner stdio lock poisoned");
        write_notification(&mut state.writer, method, params)
    }
}

impl<R, W> InteractiveTransportState<R, W>
where
    R: BufRead,
    W: Write,
{
    fn read_frame(&mut self) -> Result<Option<JsonRpcFrame>> {
        loop {
            self.line.clear();
            let bytes = self
                .reader
                .read_line(&mut self.line)
                .context("failed to read runner protocol frame")?;
            if bytes == 0 {
                return Ok(None);
            }
            if self.line.trim().is_empty() {
                continue;
            }
            return parse_jsonrpc_frame(self.line.trim_end()).map(Some);
        }
    }

    fn write_request(&mut self, id: &Value, method: &str, params: Value) -> Result<()> {
        let request = JsonRpcOutboundRequest {
            jsonrpc: "2.0",
            id: id.clone(),
            method: method.to_string(),
            params,
        };
        serde_json::to_writer(&mut self.writer, &request)
            .context("failed to write runner callback request")?;
        self.writer
            .write_all(b"\n")
            .context("failed to terminate runner callback request")?;
        self.writer
            .flush()
            .context("failed to flush runner callback request")?;
        Ok(())
    }
}
