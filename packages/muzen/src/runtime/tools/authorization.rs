use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;

use crate::runtime::contracts::{
    stable_id, SessionId, ToolEffects, ToolErrorCode, ToolGrant, ToolId, ToolInvocation,
    ToolProviderId,
};

use super::registry::ToolDefinition;

#[derive(Debug, Default)]
pub(crate) struct ToolAuthorizer {
    calls_by_session_tool: DashMap<String, AtomicUsize>,
}

impl ToolAuthorizer {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn authorize(
        &self,
        invocation: &ToolInvocation,
        definition: &ToolDefinition,
    ) -> Result<(), ToolAuthorizationError> {
        let Some(grant) = invocation.capabilities.tool_grants.get(&invocation.tool_id) else {
            return Err(denied(
                ToolErrorCode::ToolNotAllowed,
                "tool is not allowed for this session",
            ));
        };
        if !grant.allow {
            return Err(denied(
                ToolErrorCode::ToolNotAllowed,
                "tool is not allowed for this session",
            ));
        }
        if !invocation
            .capabilities
            .runtime_authority
            .allows_provider(&definition.provider_id)
        {
            return Err(denied(
                ToolErrorCode::ToolNotAllowed,
                "tool provider is not allowed for this session",
            ));
        }
        if !invocation
            .capabilities
            .runtime_authority
            .allows_provider_resources(&definition.provider_id, &definition.provider_resources)
        {
            return Err(denied(
                ToolErrorCode::ToolNotAllowed,
                "tool provider resource is not allowed for this session",
            ));
        }
        if !effects_allow(definition.effects, grant.effects_allowed) {
            return Err(denied(
                ToolErrorCode::ToolNotAllowed,
                "tool effects exceed this session capability grant",
            ));
        }
        if definition.effects.network_read
            && !invocation.capabilities.runtime_authority.network_read
        {
            return Err(denied(
                ToolErrorCode::ToolNotAllowed,
                "tool network read is not allowed for this session",
            ));
        }
        if definition.effects.host_read && !invocation.capabilities.runtime_authority.host_read {
            return Err(denied(
                ToolErrorCode::ToolNotAllowed,
                "tool host read is not allowed for this session",
            ));
        }
        if definition.effects.scratch_read
            && !invocation.capabilities.runtime_authority.scratch_read
        {
            return Err(denied(
                ToolErrorCode::ToolNotAllowed,
                "tool scratch read is not allowed for this session",
            ));
        }
        if definition.effects.scratch_write
            && !invocation.capabilities.runtime_authority.scratch_write
        {
            return Err(denied(
                ToolErrorCode::ToolNotAllowed,
                "tool scratch write is not allowed for this session",
            ));
        }
        if definition.effects.external_side_effect
            && !invocation
                .capabilities
                .runtime_authority
                .external_side_effect
        {
            return Err(denied(
                ToolErrorCode::ToolNotAllowed,
                "tool external side effect is not allowed for this session",
            ));
        }
        if definition.effects.artifact_read
            && !invocation.capabilities.artifact_access.read_redacted
        {
            return Err(denied(
                ToolErrorCode::ToolNotAllowed,
                "tool artifact read is not allowed for this session",
            ));
        }
        if definition.effects.artifact_write && !invocation.capabilities.artifact_access.write {
            return Err(denied(
                ToolErrorCode::ToolNotAllowed,
                "tool artifact write is not allowed for this session",
            ));
        }
        self.enforce_call_limit(invocation, grant)?;
        if invocation.builtin_name.is_none()
            && definition.provider_id == ToolProviderId::in_process()
            && definition.handler.as_ref().is_none()
        {
            return Err(denied(
                ToolErrorCode::UnknownTool,
                "custom tool has no registered handler",
            ));
        }
        Ok(())
    }

    fn enforce_call_limit(
        &self,
        invocation: &ToolInvocation,
        grant: &ToolGrant,
    ) -> Result<(), ToolAuthorizationError> {
        let Some(max_calls) = grant.max_calls else {
            return Ok(());
        };
        let key = call_limit_key(&invocation.session_id, &invocation.tool_id);
        let counter = self
            .calls_by_session_tool
            .entry(key)
            .or_insert_with(|| AtomicUsize::new(0));
        if counter.fetch_add(1, Ordering::Relaxed) >= max_calls as usize {
            return Err(denied(
                ToolErrorCode::BudgetExceeded,
                "tool capability call limit exceeded",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct ToolAuthorizationError {
    pub(crate) code: ToolErrorCode,
    pub(crate) message: &'static str,
}

fn denied(code: ToolErrorCode, message: &'static str) -> ToolAuthorizationError {
    ToolAuthorizationError { code, message }
}

fn call_limit_key(session_id: &SessionId, tool_id: &ToolId) -> String {
    stable_id(&[&session_id.0, tool_id.as_str()])
}

fn effects_allow(required: ToolEffects, allowed: ToolEffects) -> bool {
    (!required.repo_read || allowed.repo_read)
        && (!required.artifact_read || allowed.artifact_read)
        && (!required.artifact_write || allowed.artifact_write)
        && (!required.network_read || allowed.network_read)
        && (!required.host_read || allowed.host_read)
        && (!required.scratch_read || allowed.scratch_read)
        && (!required.scratch_write || allowed.scratch_write)
        && (!required.external_side_effect || allowed.external_side_effect)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::ToolName;
    use crate::runtime::contracts::{
        CapabilitySet, FsScope, ProviderResourceId, ProviderResourceScope, SnapshotId, ToolArgs,
        ToolCallId, TurnId,
    };
    use crate::runtime::tools::registry::{ToolDefinition, ToolRegistry};

    #[test]
    fn authorizer_blocks_effects_outside_grant() {
        let registry = ToolRegistry::review_defaults().expect("registry");
        let tool_id = ToolId::from(ToolName::ReadFile);
        let definition = registry.definition(&tool_id).expect("read_file");
        let mut capabilities = CapabilitySet::empty_review_policy(FsScope::repo_root());
        capabilities.grant(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::default(),
            },
        );

        let err = ToolAuthorizer::new()
            .authorize(&invocation(tool_id, capabilities), definition)
            .expect_err("effects should be denied");

        assert_eq!(err.code, ToolErrorCode::ToolNotAllowed);
    }

    #[test]
    fn authorizer_enforces_per_session_tool_call_limit() {
        let registry = ToolRegistry::review_defaults().expect("registry");
        let tool_id = ToolId::from(ToolName::ReadDiff);
        let definition = registry.definition(&tool_id).expect("read_diff");
        let mut capabilities = CapabilitySet::review_read_only();
        capabilities.grant(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: Some(1),
                effects_allowed: ToolEffects::review_read_only(),
            },
        );
        let authorizer = ToolAuthorizer::new();

        authorizer
            .authorize(
                &invocation(tool_id.clone(), capabilities.clone()),
                definition,
            )
            .expect("first call allowed");
        let err = authorizer
            .authorize(&invocation(tool_id, capabilities), definition)
            .expect_err("second call denied");

        assert_eq!(err.code, ToolErrorCode::BudgetExceeded);
    }

    #[test]
    fn authorizer_blocks_artifact_writes_outside_global_policy() {
        let registry = ToolRegistry::review_defaults().expect("registry");
        let tool_id = ToolId::from(ToolName::ReadDiff);
        let definition = registry.definition(&tool_id).expect("read_diff");
        let mut capabilities = CapabilitySet::review_read_only();
        capabilities.artifact_access.write = false;

        let err = ToolAuthorizer::new()
            .authorize(&invocation(tool_id, capabilities), definition)
            .expect_err("artifact write should be denied");

        assert_eq!(err.code, ToolErrorCode::ToolNotAllowed);
    }

    #[test]
    fn authorizer_blocks_provider_outside_runtime_authority_policy() {
        let tool_id = ToolId::parse("provider_scoped_tool").expect("tool id");
        let definition = custom_definition(tool_id.clone(), ToolEffects::default());
        let mut capabilities = CapabilitySet::empty_review_policy(FsScope::repo_root());
        capabilities.runtime_authority = capabilities
            .runtime_authority
            .scoped_to_providers(vec![ToolProviderId::parse("other_provider").unwrap()]);
        capabilities.grant(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::default(),
            },
        );

        let err = ToolAuthorizer::new()
            .authorize(&invocation(tool_id, capabilities), &definition)
            .expect_err("provider should be denied by runtime authority");

        assert_eq!(err.code, ToolErrorCode::ToolNotAllowed);
        assert!(err.message.contains("provider"));
    }

    #[test]
    fn authorizer_allows_provider_inside_runtime_authority_policy() {
        let tool_id = ToolId::parse("provider_allowed_tool").expect("tool id");
        let definition = custom_definition(tool_id.clone(), ToolEffects::default());
        let provider_id = definition.provider_id.clone();
        let mut capabilities = CapabilitySet::empty_review_policy(FsScope::repo_root());
        capabilities.runtime_authority = capabilities
            .runtime_authority
            .scoped_to_providers(vec![provider_id]);
        capabilities.grant(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::default(),
            },
        );

        ToolAuthorizer::new()
            .authorize(&invocation(tool_id, capabilities), &definition)
            .expect("provider should be allowed by runtime authority");
    }

    #[test]
    fn authorizer_blocks_provider_resource_outside_runtime_authority_policy() {
        let tool_id = ToolId::parse("provider_resource_scoped_tool").expect("tool id");
        let resource_id = ProviderResourceId::parse("github/org-a/repo-a").unwrap();
        let mut definition = custom_definition(tool_id.clone(), ToolEffects::default());
        definition.provider_resources = vec![resource_id];
        let mut capabilities = CapabilitySet::empty_review_policy(FsScope::repo_root());
        capabilities.runtime_authority = capabilities
            .runtime_authority
            .scoped_to_provider_resources(vec![ProviderResourceScope::new(
                definition.provider_id.clone(),
                ProviderResourceId::parse("github/org-b/repo-b").unwrap(),
            )]);
        capabilities.grant(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::default(),
            },
        );

        let err = ToolAuthorizer::new()
            .authorize(&invocation(tool_id, capabilities), &definition)
            .expect_err("provider resource should be denied by runtime authority");

        assert_eq!(err.code, ToolErrorCode::ToolNotAllowed);
        assert!(err.message.contains("provider resource"));
    }

    #[test]
    fn authorizer_allows_provider_resource_inside_runtime_authority_policy() {
        let tool_id = ToolId::parse("provider_resource_allowed_tool").expect("tool id");
        let resource_id = ProviderResourceId::parse("github/org-a/repo-a").unwrap();
        let mut definition = custom_definition(tool_id.clone(), ToolEffects::default());
        definition.provider_resources = vec![resource_id.clone()];
        let mut capabilities = CapabilitySet::empty_review_policy(FsScope::repo_root());
        capabilities.runtime_authority = capabilities
            .runtime_authority
            .scoped_to_provider_resources(vec![ProviderResourceScope::new(
                definition.provider_id.clone(),
                resource_id,
            )]);
        capabilities.grant(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::default(),
            },
        );

        ToolAuthorizer::new()
            .authorize(&invocation(tool_id, capabilities), &definition)
            .expect("provider resource should be allowed by runtime authority");
    }

    #[test]
    fn authorizer_blocks_runtime_authority_outside_global_policy() {
        for (effect, message) in [
            (
                ToolEffects {
                    network_read: true,
                    ..ToolEffects::default()
                },
                "network read",
            ),
            (
                ToolEffects {
                    host_read: true,
                    ..ToolEffects::default()
                },
                "host read",
            ),
            (
                ToolEffects {
                    scratch_write: true,
                    ..ToolEffects::default()
                },
                "scratch write",
            ),
            (
                ToolEffects {
                    external_side_effect: true,
                    ..ToolEffects::default()
                },
                "external side effect",
            ),
        ] {
            let tool_id = ToolId::parse(&format!("authority_{}", message.replace(' ', "_")))
                .expect("tool id");
            let definition = custom_definition(tool_id.clone(), effect);
            let mut capabilities = CapabilitySet::empty_review_policy(FsScope::repo_root());
            capabilities.grant(
                tool_id.clone(),
                ToolGrant {
                    allow: true,
                    max_calls: None,
                    effects_allowed: effect,
                },
            );

            let err = ToolAuthorizer::new()
                .authorize(&invocation(tool_id, capabilities), &definition)
                .expect_err("global runtime authority should deny effect");

            assert_eq!(err.code, ToolErrorCode::ToolNotAllowed, "{message}");
            assert!(err.message.contains(message), "{message}");
        }
    }

    #[test]
    fn authorizer_allows_runtime_authority_when_grant_and_global_policy_allow() {
        let tool_id = ToolId::parse("trusted_scratch_tool").expect("tool id");
        let effects = ToolEffects {
            scratch_read: true,
            scratch_write: true,
            ..ToolEffects::default()
        };
        let definition = custom_definition(tool_id.clone(), effects);
        let mut capabilities = CapabilitySet::empty_review_policy(FsScope::repo_root());
        capabilities.runtime_authority.scratch_read = true;
        capabilities.runtime_authority.scratch_write = true;
        capabilities.grant(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: effects,
            },
        );

        ToolAuthorizer::new()
            .authorize(&invocation(tool_id, capabilities), &definition)
            .expect("grant and global runtime authority should allow effect");
    }

    fn invocation(tool_id: ToolId, capabilities: CapabilitySet) -> ToolInvocation {
        let scope_key = capabilities
            .fs_scope
            .scope_key(&SnapshotId("snapshot".to_string()));
        ToolInvocation {
            session_id: SessionId("session".to_string()),
            turn_id: TurnId(1),
            call_id: ToolCallId("call".to_string()),
            builtin_name: tool_id.as_builtin(),
            input_bytes: 0,
            args: ToolArgs::Empty,
            scope_key,
            tool_id,
            capabilities,
        }
    }

    fn custom_definition(tool_id: ToolId, effects: ToolEffects) -> ToolDefinition {
        ToolDefinition {
            id: tool_id.clone(),
            model_alias: tool_id,
            description: "Authority test tool".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {},
            }),
            builtin: None,
            cacheable: false,
            effects,
            provider_resources: Vec::new(),
            provider_id: ToolProviderId::parse("authority_test_provider").expect("provider id"),
            handler: None,
        }
    }
}
