use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

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

pub(crate) fn resolve_credential_ref(ref_name: &str) -> Result<String> {
    if ref_name == "env:OPENAI_API_KEY" || ref_name == "env:OAI_API_KEY" {
        return env::var("OAI_API_KEY")
            .or_else(|_| env::var("OPENAI_API_KEY"))
            .context("OAI_API_KEY or OPENAI_API_KEY is required");
    }
    if let Some(name) = ref_name.strip_prefix("env:") {
        return env::var(name).with_context(|| format!("{name} is required"));
    }
    bail!("unsupported credentialRef; MVP supports env:NAME refs only")
}
