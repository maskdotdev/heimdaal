use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::contracts::{RedactionMetadataV1, RedactionState};

pub(crate) const SCHEMA_VERSION: &str = "heimdaal.review-run.v1";
pub(crate) const DEFAULT_MODEL: &str = "gpt-4.1-nano";

pub(crate) fn redaction_none() -> RedactionMetadataV1 {
    RedactionMetadataV1 {
        redaction_state: RedactionState::None,
        redaction_policy_id: "runtime-default".to_string(),
        contains_repo_content: false,
        contains_prompt_content: false,
        contains_model_output: false,
        contains_secret_material: false,
    }
}

pub(crate) fn stable_hash(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("fnv1a64:{hash:016x}")
}

pub(crate) fn timestamp_utc() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    format!("{}.{:09}Z", now.as_secs(), now.subsec_nanos())
}

pub(crate) fn redact_known_secrets(text: &str, secrets: &[&str]) -> String {
    let mut redacted = text.to_string();
    for secret in secrets {
        if !secret.is_empty() {
            redacted = redacted.replace(secret, "[REDACTED]");
        }
    }
    redacted
}
