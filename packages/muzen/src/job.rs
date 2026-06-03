use std::fs;

use anyhow::{bail, Result};

use crate::contracts::*;
use crate::repo::normalize_policy_path;
use crate::util::SCHEMA_VERSION;

pub(crate) fn validate_job(job: &ReviewRunJobV1) -> Result<()> {
    if job.schema_version != SCHEMA_VERSION {
        bail!("unsupported schemaVersion {}", job.schema_version);
    }
    if job.model_profiles.is_empty() {
        bail!("at least one model profile is required");
    }
    if job.repo.default_cwd.is_absolute() {
        bail!("repo.defaultCwd must be repo-relative");
    }
    for root in &job.path_policy.allowed_roots {
        normalize_policy_path(root)?;
    }
    if job
        .scratch_policy
        .scratch_root
        .as_ref()
        .is_some_and(|root| {
            fs::canonicalize(root)
                .ok()
                .zip(fs::canonicalize(&job.repo.worktree_root).ok())
                .is_some_and(|(scratch, repo)| scratch.starts_with(repo))
        })
    {
        bail!("scratchRoot must be outside worktreeRoot");
    }
    Ok(())
}

pub(crate) fn effective_personas(job: &ReviewRunJobV1) -> Vec<PersonaSpecV1> {
    if job.personas.is_empty() {
        default_personas(job)
    } else {
        job.personas.clone()
    }
}

pub(crate) fn default_personas(job: &ReviewRunJobV1) -> Vec<PersonaSpecV1> {
    (0..job.budgets.max_active_sessions.max(1))
        .map(|index| PersonaSpecV1 {
            id: format!("persona-{index}"),
            role: Role::for_index(index),
            objective: "Review the change for evidence-backed risks. Use read-only tools; finish when evidence is sufficient.".to_string(),
            cwd: Some(job.repo.default_cwd.clone()),
            model_profile_id: Some(job.default_model_profile_id.clone()),
            allowed_tools: ToolMask::review_read_only(),
            budget: AgentBudget {
                max_turns: 7,
                max_tool_calls: 14,
                max_prompt_tokens: 32_000,
                max_output_tokens: 1_024,
            },
        })
        .collect()
}

pub(crate) fn tool_allowed(mask: ToolMask, tool: ToolName) -> bool {
    match tool {
        ToolName::ListChangedFiles => mask.list_changed_files,
        ToolName::ReadDiff => mask.read_diff,
        ToolName::ListFiles => mask.list_files,
        ToolName::ReadFile => mask.read_file,
        ToolName::ReadBaseFile => mask.read_base_file,
        ToolName::ReadHeadFile => mask.read_head_file,
        ToolName::SearchText => mask.search_text,
        ToolName::FindRelatedFiles => mask.find_related_files,
        ToolName::FindTestsForFile => mask.find_tests_for_file,
        ToolName::ListImports => mask.list_imports,
        ToolName::RecordFinding => mask.record_finding,
        ToolName::ChallengeFinding => mask.challenge_finding,
        ToolName::Finish => mask.finish,
    }
}
