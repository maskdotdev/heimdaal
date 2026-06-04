use regex::Regex;
use serde_json::Value;

use crate::runtime::contracts::{RuntimeError, RuntimeResult};

#[derive(Debug)]
pub(crate) struct Redactor {
    patterns: Vec<Regex>,
}

impl Redactor {
    pub(super) fn new() -> RuntimeResult<Self> {
        let patterns = [
            r"AKIA[0-9A-Z]{16}",
            r"github_pat_[A-Za-z0-9_]{20,}",
            r"ghp_[A-Za-z0-9_]{20,}",
            r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}",
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----",
        ];
        let mut compiled = Vec::new();
        for pattern in patterns {
            compiled.push(Regex::new(pattern).map_err(|_| {
                RuntimeError::Invariant("failed to compile built-in redaction regex")
            })?);
        }
        Ok(Self { patterns: compiled })
    }

    pub(super) fn redact(&self, input: &str) -> String {
        let mut output = input.to_string();
        for pattern in &self.patterns {
            output = pattern.replace_all(&output, "[REDACTED]").into_owned();
        }
        output
    }

    pub(super) fn redact_value(&self, mut value: Value) -> Value {
        match &mut value {
            Value::String(text) => {
                *text = self.redact(text);
            }
            Value::Array(items) => {
                for item in items {
                    *item = self.redact_value(std::mem::take(item));
                }
            }
            Value::Object(object) => {
                for item in object.values_mut() {
                    *item = self.redact_value(std::mem::take(item));
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
        value
    }
}
