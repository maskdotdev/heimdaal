use std::io::Write;
use std::sync::Mutex;

use serde_json::Value;

use crate::contracts::{EventLevel, EventTraceV1, EventType, RedactionMetadataV1, RunEventV1};
use crate::util::{redaction_none, timestamp_utc, SCHEMA_VERSION};

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
