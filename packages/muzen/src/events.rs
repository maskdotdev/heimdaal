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

pub(crate) struct EventRecord {
    pub(crate) level: EventLevel,
    pub(crate) event_type: EventType,
    pub(crate) session_id: Option<String>,
    pub(crate) tool_call_id: Option<String>,
    pub(crate) artifact_id: Option<String>,
    pub(crate) finding_id: Option<String>,
    pub(crate) payload: Value,
}

impl EventRecord {
    pub(crate) fn new(level: EventLevel, event_type: EventType, payload: Value) -> Self {
        Self {
            level,
            event_type,
            session_id: None,
            tool_call_id: None,
            artifact_id: None,
            finding_id: None,
            payload,
        }
    }

    pub(crate) fn session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    pub(crate) fn tool_call_id(mut self, tool_call_id: impl Into<String>) -> Self {
        self.tool_call_id = Some(tool_call_id.into());
        self
    }

    pub(crate) fn artifact_id(mut self, artifact_id: impl Into<String>) -> Self {
        self.artifact_id = Some(artifact_id.into());
        self
    }

    pub(crate) fn finding_id(mut self, finding_id: impl Into<String>) -> Self {
        self.finding_id = Some(finding_id.into());
        self
    }
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

    pub(crate) fn emit(&self, event: EventRecord) {
        let mut state = self.state.lock().expect("event emitter poisoned");
        state.seq += 1;
        let event = RunEventV1 {
            schema_version: SCHEMA_VERSION,
            event_id: format!("{}-event-{}", self.run_id, state.seq),
            run_id: self.run_id.clone(),
            attempt: self.attempt,
            seq: state.seq,
            timestamp_utc: timestamp_utc(),
            level: event.level,
            event_type: event.event_type,
            session_id: event.session_id,
            tool_call_id: event.tool_call_id,
            artifact_id: event.artifact_id,
            finding_id: event.finding_id,
            payload: event.payload,
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
