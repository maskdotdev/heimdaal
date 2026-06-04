use std::sync::Arc;

use crate::events::{EventEmitter, EventRecord};
use crate::runtime::contracts::{RuntimeEvent, RuntimeEventContext, RuntimeEventSink};
use crate::runtime::policy::PlannedRuntimeEvent;

#[derive(Clone)]
pub(crate) struct RuntimeEventDispatcher {
    runtime_sink: Option<Arc<dyn RuntimeEventSink>>,
    legacy_emitter: Option<Arc<EventEmitter>>,
}

impl RuntimeEventDispatcher {
    pub(crate) fn new(
        runtime_sink: Option<Arc<dyn RuntimeEventSink>>,
        legacy_emitter: Option<Arc<EventEmitter>>,
    ) -> Self {
        Self {
            runtime_sink,
            legacy_emitter,
        }
    }

    pub(crate) fn none() -> Self {
        Self::new(None, None)
    }

    pub(crate) fn emit_legacy(&self, event: EventRecord) {
        if let Some(emitter) = &self.legacy_emitter {
            emitter.emit(event);
        }
    }

    pub(crate) fn emit_planned_runtime(&self, planned: PlannedRuntimeEvent) {
        self.emit_runtime_with_context(planned.context, planned.event);
    }

    pub(crate) fn emit_runtime_with_context(
        &self,
        context: RuntimeEventContext,
        event: RuntimeEvent,
    ) {
        if let Some(runtime_sink) = &self.runtime_sink {
            runtime_sink.emit_with_context(context, event);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Result as IoResult, Write};
    use std::sync::Mutex;

    use serde_json::{json, Value};

    use super::*;
    use crate::contracts::{EventLevel, EventType};
    use crate::events::{EventEmitterState, EventRecord};
    use crate::runtime::contracts::{RuntimeEvent, RuntimeEventContext, SessionId};

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

    #[derive(Default)]
    struct RecordingRuntimeSink {
        records: Mutex<Vec<(RuntimeEventContext, RuntimeEvent)>>,
    }

    impl RuntimeEventSink for RecordingRuntimeSink {
        fn emit(&self, event: RuntimeEvent) {
            self.emit_with_context(RuntimeEventContext::from_event(&event), event);
        }

        fn emit_with_context(&self, context: RuntimeEventContext, event: RuntimeEvent) {
            self.records
                .lock()
                .expect("sink lock")
                .push((context, event));
        }
    }

    #[test]
    fn dispatcher_emits_legacy_records_when_emitter_is_configured() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let emitter = Arc::new(EventEmitter {
            run_id: "run-1".to_string(),
            attempt: 2,
            redaction_policy_id: "test-redaction".to_string(),
            state: Mutex::new(EventEmitterState {
                seq: 0,
                writer: Box::new(SharedWriter(Arc::clone(&output))),
            }),
        });
        let dispatcher = RuntimeEventDispatcher::new(None, Some(emitter));

        dispatcher.emit_legacy(
            EventRecord::new(
                EventLevel::Info,
                EventType::SessionStarted,
                json!({"role": "generalist"}),
            )
            .session_id("session-1"),
        );

        let bytes = output.lock().expect("output lock").clone();
        let line = String::from_utf8(bytes).expect("utf8 output");
        let event: Value = serde_json::from_str(line.trim()).expect("legacy event json");
        assert_eq!(event["runId"], "run-1");
        assert_eq!(event["attempt"], 2);
        assert_eq!(event["seq"], 1);
        assert_eq!(event["sessionId"], "session-1");
        assert_eq!(event["eventType"], "session_started");
    }

    #[test]
    fn dispatcher_emits_runtime_records_with_context_when_sink_is_configured() {
        let sink = Arc::new(RecordingRuntimeSink::default());
        let runtime_sink: Arc<dyn RuntimeEventSink> = sink.clone();
        let dispatcher = RuntimeEventDispatcher::new(Some(runtime_sink), None);
        let context = RuntimeEventContext {
            session_id: Some(SessionId("session-1".to_string())),
            ..RuntimeEventContext::default()
        };

        dispatcher.emit_runtime_with_context(
            context,
            RuntimeEvent::SessionFinished {
                session_id: SessionId("session-1".to_string()),
                status: "done".to_string(),
            },
        );

        let records = sink.records.lock().expect("sink lock");
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].0.session_id.as_ref(),
            Some(&SessionId("session-1".to_string()))
        );
        assert!(matches!(
            records[0].1,
            RuntimeEvent::SessionFinished { ref status, .. } if status == "done"
        ));
    }

    #[test]
    fn dispatcher_without_sinks_drops_events() {
        let dispatcher = RuntimeEventDispatcher::none();

        dispatcher.emit_legacy(EventRecord::new(
            EventLevel::Info,
            EventType::SessionStarted,
            json!({}),
        ));
        dispatcher.emit_runtime_with_context(
            RuntimeEventContext::default(),
            RuntimeEvent::JobFinished {
                status: "done".to_string(),
            },
        );
    }
}
