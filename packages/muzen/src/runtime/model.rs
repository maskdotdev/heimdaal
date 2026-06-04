use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::contracts::{
    AgentBudget, ModelApiProtocol, ModelProfileRefV1, ProviderKind, Role, TokenUsage,
    ToolCallingMode, ToolName,
};
use crate::runtime::contracts::*;
use crate::runtime::policy::ReviewerPolicy;
use crate::runtime::tools::ToolRegistry;
use crate::util::{resolve_credential_ref, timestamp_utc};

#[async_trait]
pub trait ConcurrentModelClient: Send + Sync {
    async fn complete(
        &self,
        scope: &SessionScope,
        transcript: &[ConversationItem],
        turn_id: TurnId,
        cancel: CancellationToken,
    ) -> RuntimeResult<ModelTurn>;

    fn estimate_cost(&self, _usage: &TokenUsage) -> Option<ModelCostEstimate> {
        None
    }
}

#[async_trait]
pub trait ConcurrentModelRouter: Send + Sync {
    async fn client_for(
        &self,
        scope: &SessionScope,
    ) -> RuntimeResult<Arc<dyn ConcurrentModelClient>>;
}

pub struct StaticModelRouter {
    client: Arc<dyn ConcurrentModelClient>,
}

impl StaticModelRouter {
    pub fn new(client: Arc<dyn ConcurrentModelClient>) -> Self {
        Self { client }
    }
}

impl std::fmt::Debug for StaticModelRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticModelRouter").finish_non_exhaustive()
    }
}

#[async_trait]
impl ConcurrentModelRouter for StaticModelRouter {
    async fn client_for(
        &self,
        _scope: &SessionScope,
    ) -> RuntimeResult<Arc<dyn ConcurrentModelClient>> {
        Ok(Arc::clone(&self.client))
    }
}

pub struct ProfileModelRouter {
    clients: HashMap<String, Arc<dyn ConcurrentModelClient>>,
    default_profile_id: String,
}

pub trait CredentialResolver: Send + Sync {
    fn resolve_credential(&self, credential_ref: &str) -> RuntimeResult<String>;
}

#[derive(Debug, Clone, Default)]
pub struct EnvCredentialResolver;

impl CredentialResolver for EnvCredentialResolver {
    fn resolve_credential(&self, credential_ref: &str) -> RuntimeResult<String> {
        resolve_credential_ref(credential_ref)
            .map_err(|_| RuntimeError::InvalidInput("model credential is unavailable".to_string()))
    }
}

impl ProfileModelRouter {
    pub(crate) fn from_profiles(
        profiles: &[ModelProfileRefV1],
        default_profile_id: String,
        base_url: String,
        limiter: Arc<ModelLimiter>,
        tool_registry: Arc<ToolRegistry>,
        reviewer_policy: Arc<ReviewerPolicy>,
        credential_resolver: Arc<dyn CredentialResolver>,
    ) -> RuntimeResult<Self> {
        if profiles.is_empty() {
            return Err(RuntimeError::InvalidInput(
                "at least one model profile is required".to_string(),
            ));
        }
        let mut clients = HashMap::new();
        for profile in profiles {
            match profile.provider_kind {
                ProviderKind::OpenaiCompatible => {}
            }
            let client = openai_client_from_profile(
                profile.clone(),
                base_url.clone(),
                Arc::clone(&limiter),
                Arc::clone(&tool_registry),
                Arc::clone(&reviewer_policy),
                Arc::clone(&credential_resolver),
            )?;
            clients.insert(profile.id.clone(), client);
        }
        if !clients.contains_key(&default_profile_id) {
            return Err(RuntimeError::InvalidInput(format!(
                "missing default model profile {default_profile_id}"
            )));
        }
        Ok(Self {
            clients,
            default_profile_id,
        })
    }
}

impl std::fmt::Debug for ProfileModelRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProfileModelRouter")
            .field("clients", &self.clients.len())
            .field("default_profile_id", &self.default_profile_id)
            .finish()
    }
}

#[async_trait]
impl ConcurrentModelRouter for ProfileModelRouter {
    async fn client_for(
        &self,
        scope: &SessionScope,
    ) -> RuntimeResult<Arc<dyn ConcurrentModelClient>> {
        let profile_id = scope
            .model_profile_id
            .as_ref()
            .unwrap_or(&self.default_profile_id);
        self.clients
            .get(profile_id)
            .or_else(|| self.clients.get(&self.default_profile_id))
            .cloned()
            .ok_or_else(|| {
                RuntimeError::InvalidInput(format!("missing model profile {profile_id}"))
            })
    }
}

#[derive(Debug)]
pub(crate) struct MockReviewModel {
    target_path: String,
    query: String,
}

impl MockReviewModel {
    pub(crate) fn new(target_path: String, query: String) -> Self {
        Self { target_path, query }
    }
}

#[async_trait]
impl ConcurrentModelClient for MockReviewModel {
    async fn complete(
        &self,
        scope: &SessionScope,
        transcript: &[ConversationItem],
        turn_id: TurnId,
        _cancel: CancellationToken,
    ) -> RuntimeResult<ModelTurn> {
        let session_id = &scope.id;
        let tool_results = transcript
            .iter()
            .filter(|item| matches!(item, ConversationItem::ToolResult { .. }))
            .count();
        let usage = TokenUsage {
            input_tokens: transcript.len() as u64 * 100,
            output_tokens: 64,
            total_tokens: transcript.len() as u64 * 100 + 64,
        };
        if tool_results == 0 {
            return Ok(ModelTurn::ToolCalls {
                usage,
                calls: vec![
                    ModelToolCall {
                        call_id: ToolCallId(format!("{}-{}-read-diff", session_id.0, turn_id.0)),
                        index: 0,
                        name: ToolId::from(ToolName::ReadDiff),
                        raw_arguments: "{}".to_string(),
                    },
                    ModelToolCall {
                        call_id: ToolCallId(format!("{}-{}-read-file", session_id.0, turn_id.0)),
                        index: 1,
                        name: ToolId::from(ToolName::ReadFile),
                        raw_arguments: json!({ "path": self.target_path }).to_string(),
                    },
                    ModelToolCall {
                        call_id: ToolCallId(format!("{}-{}-search", session_id.0, turn_id.0)),
                        index: 2,
                        name: ToolId::from(ToolName::SearchText),
                        raw_arguments: json!({ "query": self.query }).to_string(),
                    },
                ],
            });
        }
        Ok(ModelTurn::ToolCalls {
            usage,
            calls: vec![ModelToolCall {
                call_id: ToolCallId(format!("{}-{}-finding", session_id.0, turn_id.0)),
                index: 0,
                name: ToolId::from(ToolName::RecordFinding),
                raw_arguments: json!({
                    "title": format!("{} reviewed with parallel evidence", session_id.0),
                    "claim": "The benchmark session gathered diff, file, and search evidence."
                })
                .to_string(),
            }],
        })
    }
}

#[derive(Debug)]
pub struct ModelLimiter {
    global: Arc<Semaphore>,
    per_provider: DashMap<String, Arc<Semaphore>>,
    per_profile: DashMap<String, Arc<Semaphore>>,
    per_key: DashMap<String, Arc<Semaphore>>,
    per_session: DashMap<String, Arc<Semaphore>>,
    max_per_provider: usize,
    max_per_profile: usize,
    max_per_key: usize,
    max_per_session: usize,
}

#[derive(Debug)]
pub struct ModelPermit {
    _global: OwnedSemaphorePermit,
    _buckets: Vec<OwnedSemaphorePermit>,
}

impl ModelLimiter {
    pub fn new(global_concurrency: usize) -> Self {
        Self::new_with_per_key(global_concurrency, global_concurrency)
    }

    pub fn new_with_per_key(global_concurrency: usize, max_per_key: usize) -> Self {
        Self::new_with_buckets(
            global_concurrency,
            global_concurrency,
            global_concurrency,
            max_per_key,
            1,
        )
    }

    pub fn new_with_buckets(
        global_concurrency: usize,
        max_per_provider: usize,
        max_per_profile: usize,
        max_per_key: usize,
        max_per_session: usize,
    ) -> Self {
        Self {
            global: Arc::new(Semaphore::new(global_concurrency.max(1))),
            per_provider: DashMap::new(),
            per_profile: DashMap::new(),
            per_key: DashMap::new(),
            per_session: DashMap::new(),
            max_per_provider: max_per_provider.max(1),
            max_per_profile: max_per_profile.max(1),
            max_per_key: max_per_key.max(1),
            max_per_session: max_per_session.max(1),
        }
    }

    pub async fn acquire(&self) -> RuntimeResult<ModelPermit> {
        let global = self
            .global
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RuntimeError::Cancelled)?;
        Ok(ModelPermit {
            _global: global,
            _buckets: Vec::new(),
        })
    }

    pub async fn acquire_for_key(&self, key: &str) -> RuntimeResult<ModelPermit> {
        let global = self
            .global
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RuntimeError::Cancelled)?;
        let key_limiter = self
            .per_key
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(self.max_per_key)))
            .clone();
        let key = key_limiter
            .acquire_owned()
            .await
            .map_err(|_| RuntimeError::Cancelled)?;
        Ok(ModelPermit {
            _global: global,
            _buckets: vec![key],
        })
    }

    pub async fn acquire_for_model(
        &self,
        provider: &str,
        profile_id: &str,
        credential_ref: &str,
        session_id: &SessionId,
    ) -> RuntimeResult<ModelPermit> {
        let global = self
            .global
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RuntimeError::Cancelled)?;
        let buckets = vec![
            self.acquire_bucket(&self.per_provider, provider, self.max_per_provider)
                .await?,
            self.acquire_bucket(&self.per_profile, profile_id, self.max_per_profile)
                .await?,
            self.acquire_bucket(&self.per_key, credential_ref, self.max_per_key)
                .await?,
            self.acquire_bucket(&self.per_session, &session_id.0, self.max_per_session)
                .await?,
        ];
        Ok(ModelPermit {
            _global: global,
            _buckets: buckets,
        })
    }

    async fn acquire_bucket(
        &self,
        buckets: &DashMap<String, Arc<Semaphore>>,
        key: &str,
        max_per_bucket: usize,
    ) -> RuntimeResult<OwnedSemaphorePermit> {
        buckets
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(max_per_bucket)))
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RuntimeError::Cancelled)
    }
}

#[derive(Debug)]
pub struct OpenAiChatCompletionsClient {
    http: reqwest::Client,
    profile: ModelProfileRefV1,
    api_key: String,
    base_url: String,
    limiter: Arc<ModelLimiter>,
    tool_registry: Arc<ToolRegistry>,
    reviewer_policy: Arc<ReviewerPolicy>,
}

impl OpenAiChatCompletionsClient {
    pub(crate) fn from_profile(
        profile: ModelProfileRefV1,
        base_url: String,
        limiter: Arc<ModelLimiter>,
        tool_registry: Arc<ToolRegistry>,
        reviewer_policy: Arc<ReviewerPolicy>,
        credential_resolver: Arc<dyn CredentialResolver>,
    ) -> RuntimeResult<Self> {
        let api_key = credential_resolver.resolve_credential(&profile.credential_ref)?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(90))
            .build()
            .map_err(|_| RuntimeError::Invariant("failed to build async HTTP client"))?;
        Ok(Self {
            http,
            profile,
            api_key,
            base_url: base_url.trim_end_matches('/').to_string(),
            limiter,
            tool_registry,
            reviewer_policy,
        })
    }
}

fn openai_client_from_profile(
    profile: ModelProfileRefV1,
    base_url: String,
    limiter: Arc<ModelLimiter>,
    tool_registry: Arc<ToolRegistry>,
    reviewer_policy: Arc<ReviewerPolicy>,
    credential_resolver: Arc<dyn CredentialResolver>,
) -> RuntimeResult<Arc<dyn ConcurrentModelClient>> {
    match profile.api_protocol {
        ModelApiProtocol::ChatCompletions => {
            Ok(Arc::new(OpenAiChatCompletionsClient::from_profile(
                profile,
                base_url,
                limiter,
                tool_registry,
                reviewer_policy,
                credential_resolver,
            )?))
        }
        ModelApiProtocol::Responses => Ok(Arc::new(OpenAiResponsesClient::from_profile(
            profile,
            base_url,
            limiter,
            tool_registry,
            reviewer_policy,
            credential_resolver,
        )?)),
    }
}

const OPENAI_PROVIDER_CANARY_PROMPT: &str = "Return the word ready.";
const OPENAI_PROVIDER_CANARY_MAX_INPUT_TOKENS: u32 = 4_096;
pub const MODEL_PROVIDER_CANARY_EVIDENCE_SCHEMA_VERSION: &str = "muzen.model-provider-canary.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenAiProviderCanaryConfig {
    pub enabled: bool,
    pub base_url: String,
    pub credential_ref: String,
    pub model: String,
    pub max_output_tokens: u32,
    pub prompt: String,
}

impl OpenAiProviderCanaryConfig {
    pub fn from_env(default_model: impl Into<String>) -> Self {
        let default_model = default_model.into();
        Self {
            enabled: std::env::var("MUZEN_RUN_REAL_PROVIDER_CANARY")
                .or_else(|_| std::env::var("MUZEN_RUN_REAL_PROVIDER_SMOKE"))
                .ok()
                .as_deref()
                == Some("1"),
            base_url: std::env::var("OAI_BASE_URL")
                .or_else(|_| std::env::var("OPENAI_BASE_URL"))
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
            credential_ref: "env:OPENAI_API_KEY".to_string(),
            model: std::env::var("MUZEN_REAL_PROVIDER_MODEL").unwrap_or(default_model),
            max_output_tokens: 32,
            prompt: OPENAI_PROVIDER_CANARY_PROMPT.to_string(),
        }
    }

    fn profile_for_protocol(&self, protocol: ModelApiProtocol) -> ModelProfileRefV1 {
        let protocol_slug = openai_provider_canary_protocol_slug(protocol);
        ModelProfileRefV1 {
            id: format!("real-provider-canary-{protocol_slug}"),
            provider_kind: ProviderKind::OpenaiCompatible,
            api_protocol: protocol,
            provider_profile_id: format!("real-provider-canary-openai-{protocol_slug}"),
            credential_ref: self.credential_ref.clone(),
            model: self.model.clone(),
            max_input_tokens: OPENAI_PROVIDER_CANARY_MAX_INPUT_TOKENS,
            max_output_tokens: self.max_output_tokens,
            tool_calling_mode: ToolCallingMode::Auto,
            temperature: Some(0.0),
            top_p: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelProviderCanaryReport {
    pub protocol: ModelApiProtocol,
    pub base_url: String,
    pub model: String,
    pub credential_ref: String,
    pub status: ModelProviderCanaryStatus,
}

impl ModelProviderCanaryReport {
    fn skipped(
        protocol: ModelApiProtocol,
        config: &OpenAiProviderCanaryConfig,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            protocol,
            base_url: config.base_url.clone(),
            model: config.model.clone(),
            credential_ref: config.credential_ref.clone(),
            status: ModelProviderCanaryStatus::Skipped {
                reason: reason.into(),
            },
        }
    }

    fn passed(protocol: ModelApiProtocol, config: &OpenAiProviderCanaryConfig) -> Self {
        Self {
            protocol,
            base_url: config.base_url.clone(),
            model: config.model.clone(),
            credential_ref: config.credential_ref.clone(),
            status: ModelProviderCanaryStatus::Passed,
        }
    }

    fn failed(
        protocol: ModelApiProtocol,
        config: &OpenAiProviderCanaryConfig,
        error: impl Into<String>,
    ) -> Self {
        Self {
            protocol,
            base_url: config.base_url.clone(),
            model: config.model.clone(),
            credential_ref: config.credential_ref.clone(),
            status: ModelProviderCanaryStatus::Failed {
                error: error.into(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelProviderCanaryStatus {
    Skipped { reason: String },
    Passed,
    Failed { error: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelProviderCanaryEvidence {
    pub schema_version: String,
    pub generated_at_utc: String,
    pub required_protocols: Vec<ModelApiProtocol>,
    pub reports: Vec<ModelProviderCanaryReport>,
    pub gate: ModelProviderCanaryGate,
}

impl ModelProviderCanaryEvidence {
    pub fn from_reports(reports: Vec<ModelProviderCanaryReport>) -> Self {
        Self::with_generated_at(timestamp_utc(), reports)
    }

    pub fn with_generated_at(
        generated_at_utc: impl Into<String>,
        reports: Vec<ModelProviderCanaryReport>,
    ) -> Self {
        let required_protocols = openai_provider_canary_protocols().to_vec();
        let gate = ModelProviderCanaryGate::evaluate(&required_protocols, &reports);
        Self {
            schema_version: MODEL_PROVIDER_CANARY_EVIDENCE_SCHEMA_VERSION.to_string(),
            generated_at_utc: generated_at_utc.into(),
            required_protocols,
            reports,
            gate,
        }
    }

    pub fn require_passed(&self) -> RuntimeResult<()> {
        let failures = self.validation_failures();
        if failures.is_empty() {
            return Ok(());
        }
        Err(RuntimeError::InvalidInput(format!(
            "real-provider canary gate failed: {}",
            failures.join("; ")
        )))
    }

    fn validation_failures(&self) -> Vec<String> {
        let mut failures = Vec::new();
        if self.schema_version != MODEL_PROVIDER_CANARY_EVIDENCE_SCHEMA_VERSION {
            failures.push(format!(
                "unsupported provider canary evidence schema {}",
                self.schema_version
            ));
        }
        if self.required_protocols.as_slice() != openai_provider_canary_protocols() {
            failures.push(
                "required protocol matrix does not match current provider canary contract"
                    .to_string(),
            );
        }
        let evaluated = ModelProviderCanaryGate::evaluate(&self.required_protocols, &self.reports);
        if evaluated != self.gate {
            failures.push("stored provider canary gate does not match reports".to_string());
        }
        failures.extend(evaluated.failures);
        failures
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelProviderCanaryGate {
    pub valid: bool,
    pub passed: usize,
    pub skipped: usize,
    pub failed: usize,
    pub failures: Vec<String>,
}

impl ModelProviderCanaryGate {
    fn evaluate(
        required_protocols: &[ModelApiProtocol],
        reports: &[ModelProviderCanaryReport],
    ) -> Self {
        let mut passed = 0usize;
        let mut skipped = 0usize;
        let mut failed = 0usize;
        let mut failures = Vec::new();

        for protocol in required_protocols {
            let protocol_reports = reports
                .iter()
                .filter(|report| report.protocol == *protocol)
                .collect::<Vec<_>>();
            let protocol_slug = openai_provider_canary_protocol_slug(*protocol);
            match protocol_reports.len() {
                0 => failures.push(format!("missing {protocol_slug} canary report")),
                1 => {}
                count => {
                    failures.push(format!("duplicate {protocol_slug} canary reports: {count}"))
                }
            }
            for report in protocol_reports {
                match &report.status {
                    ModelProviderCanaryStatus::Passed => passed += 1,
                    ModelProviderCanaryStatus::Skipped { reason } => {
                        skipped += 1;
                        failures.push(format!("{protocol_slug} skipped: {reason}"));
                    }
                    ModelProviderCanaryStatus::Failed { error } => {
                        failed += 1;
                        failures.push(format!("{protocol_slug} failed: {error}"));
                    }
                }
            }
        }

        Self {
            valid: failures.is_empty() && passed == required_protocols.len(),
            passed,
            skipped,
            failed,
            failures,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelProviderCanaryEvidenceExport {
    pub path: PathBuf,
    pub bytes: usize,
    pub valid: bool,
    pub failures: Vec<String>,
}

pub fn export_model_provider_canary_evidence(
    path: impl AsRef<Path>,
    evidence: &ModelProviderCanaryEvidence,
) -> RuntimeResult<ModelProviderCanaryEvidenceExport> {
    let path = path.as_ref();
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|error| {
            RuntimeError::RepoUnavailable(format!(
                "failed to create provider canary evidence directory: {error}"
            ))
        })?;
    }
    let mut bytes = serde_json::to_vec_pretty(evidence)
        .map_err(|_| RuntimeError::Invariant("provider canary evidence serialization failed"))?;
    bytes.push(b'\n');
    let temp_path = provider_canary_evidence_temp_path(path)?;
    let failures = evidence.validation_failures();
    fs::write(&temp_path, &bytes).map_err(|error| {
        RuntimeError::RepoUnavailable(format!("failed to write provider canary evidence: {error}"))
    })?;
    fs::rename(&temp_path, path).map_err(|error| {
        let _ = fs::remove_file(&temp_path);
        RuntimeError::RepoUnavailable(format!(
            "failed to publish provider canary evidence: {error}"
        ))
    })?;
    Ok(ModelProviderCanaryEvidenceExport {
        path: path.to_path_buf(),
        bytes: bytes.len(),
        valid: failures.is_empty(),
        failures,
    })
}

pub fn load_model_provider_canary_evidence(
    path: impl AsRef<Path>,
) -> RuntimeResult<ModelProviderCanaryEvidence> {
    let path = path.as_ref();
    let contents = fs::read_to_string(path).map_err(|error| {
        RuntimeError::RepoUnavailable(format!(
            "failed to read provider canary evidence {}: {error}",
            path.display()
        ))
    })?;
    serde_json::from_str(&contents).map_err(|error| {
        RuntimeError::InvalidInput(format!(
            "invalid provider canary evidence {}: {error}",
            path.display()
        ))
    })
}

fn provider_canary_evidence_temp_path(path: &Path) -> RuntimeResult<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        RuntimeError::InvalidInput("provider canary evidence path must name a file".to_string())
    })?;
    Ok(path.with_file_name(format!(
        ".{}.{}.tmp",
        file_name.to_string_lossy(),
        std::process::id()
    )))
}

pub fn openai_provider_canary_protocols() -> &'static [ModelApiProtocol] {
    const PROTOCOLS: &[ModelApiProtocol] = &[
        ModelApiProtocol::ChatCompletions,
        ModelApiProtocol::Responses,
    ];
    PROTOCOLS
}

pub(crate) fn openai_provider_canary_profiles(
    config: &OpenAiProviderCanaryConfig,
) -> Vec<ModelProfileRefV1> {
    openai_provider_canary_protocols()
        .iter()
        .copied()
        .map(|protocol| config.profile_for_protocol(protocol))
        .collect()
}

pub async fn run_openai_provider_canaries(
    config: OpenAiProviderCanaryConfig,
    credential_resolver: Arc<dyn CredentialResolver>,
) -> Vec<ModelProviderCanaryReport> {
    let profiles = openai_provider_canary_profiles(&config);
    if !config.enabled {
        return profiles
            .iter()
            .map(|profile| {
                ModelProviderCanaryReport::skipped(
                    profile.api_protocol,
                    &config,
                    "disabled: set MUZEN_RUN_REAL_PROVIDER_CANARY=1 to run live provider canaries",
                )
            })
            .collect();
    }
    if credential_resolver
        .resolve_credential(&config.credential_ref)
        .is_err()
    {
        return profiles
            .iter()
            .map(|profile| {
                ModelProviderCanaryReport::skipped(
                    profile.api_protocol,
                    &config,
                    "credential unavailable",
                )
            })
            .collect();
    }
    let registry = match ToolRegistry::review_defaults() {
        Ok(registry) => Arc::new(registry),
        Err(error) => {
            return profiles
                .iter()
                .map(|profile| {
                    ModelProviderCanaryReport::failed(
                        profile.api_protocol,
                        &config,
                        format!("failed to build tool registry: {error}"),
                    )
                })
                .collect();
        }
    };
    let limiter = Arc::new(ModelLimiter::new_with_buckets(
        profiles.len().max(1),
        profiles.len().max(1),
        profiles.len().max(1),
        profiles.len().max(1),
        1,
    ));
    let reviewer_policy = Arc::new(ReviewerPolicy::new());
    let mut reports = Vec::with_capacity(profiles.len());
    for profile in profiles {
        reports.push(
            run_openai_provider_canary(
                profile,
                &config,
                Arc::clone(&credential_resolver),
                Arc::clone(&registry),
                Arc::clone(&reviewer_policy),
                Arc::clone(&limiter),
            )
            .await,
        );
    }
    reports
}

async fn run_openai_provider_canary(
    profile: ModelProfileRefV1,
    config: &OpenAiProviderCanaryConfig,
    credential_resolver: Arc<dyn CredentialResolver>,
    registry: Arc<ToolRegistry>,
    reviewer_policy: Arc<ReviewerPolicy>,
    limiter: Arc<ModelLimiter>,
) -> ModelProviderCanaryReport {
    let protocol = profile.api_protocol;
    let scope = openai_provider_canary_scope(profile.id.clone(), config.max_output_tokens);
    let client = match openai_client_from_profile(
        profile,
        config.base_url.clone(),
        limiter,
        registry,
        reviewer_policy,
        credential_resolver,
    ) {
        Ok(client) => client,
        Err(error) => {
            return ModelProviderCanaryReport::failed(protocol, config, error.to_string());
        }
    };
    let transcript = [ConversationItem::User {
        content: config.prompt.clone(),
    }];
    match client
        .complete(&scope, &transcript, TurnId(0), CancellationToken::new())
        .await
    {
        Ok(turn) if model_turn_has_payload(&turn) => {
            ModelProviderCanaryReport::passed(protocol, config)
        }
        Ok(_) => ModelProviderCanaryReport::failed(protocol, config, "empty model response"),
        Err(error) => ModelProviderCanaryReport::failed(protocol, config, error.to_string()),
    }
}

fn openai_provider_canary_scope(profile_id: String, max_output_tokens: u32) -> SessionScope {
    SessionScope {
        id: SessionId(format!("real-provider-canary-{profile_id}")),
        role: Role::Generalist,
        objective: "real-provider protocol canary".to_string(),
        snapshot_id: None,
        model_profile_id: Some(profile_id),
        capabilities: CapabilitySet::review_read_only(),
        budget: AgentBudget {
            max_turns: 1,
            max_tool_calls: 1,
            max_prompt_tokens: OPENAI_PROVIDER_CANARY_MAX_INPUT_TOKENS as u64,
            max_output_tokens: max_output_tokens as u64,
        },
    }
}

fn model_turn_has_payload(turn: &ModelTurn) -> bool {
    match turn {
        ModelTurn::Text { content, .. } => !content.trim().is_empty(),
        ModelTurn::ToolCalls { calls, .. } => !calls.is_empty(),
    }
}

fn openai_provider_canary_protocol_slug(protocol: ModelApiProtocol) -> &'static str {
    match protocol {
        ModelApiProtocol::ChatCompletions => "chat_completions",
        ModelApiProtocol::Responses => "responses",
    }
}

#[async_trait]
impl ConcurrentModelClient for OpenAiChatCompletionsClient {
    async fn complete(
        &self,
        scope: &SessionScope,
        transcript: &[ConversationItem],
        _turn_id: TurnId,
        cancel: CancellationToken,
    ) -> RuntimeResult<ModelTurn> {
        if cancel.is_cancelled() {
            return Err(RuntimeError::Cancelled);
        }
        let _permit = self
            .limiter
            .acquire_for_model(
                "openai_compatible",
                &self.profile.id,
                &self.profile.credential_ref,
                &scope.id,
            )
            .await?;
        let token_param =
            if self.profile.model.starts_with("gpt-5") || self.profile.model.starts_with('o') {
                "max_completion_tokens"
            } else {
                "max_tokens"
            };
        let mut body = json!({
            "model": self.profile.model,
            "messages": chat_messages(
                &self.reviewer_policy,
                &self.tool_registry,
                scope,
                transcript,
            )?,
            "tools": openai_tools_for_protocol(
                ModelApiProtocol::ChatCompletions,
                &self.reviewer_policy,
                &self.tool_registry,
                transcript,
                &scope.capabilities
            )?,
            "parallel_tool_calls": false,
            "tool_choice": match self.profile.tool_calling_mode {
                ToolCallingMode::Required => "required",
                ToolCallingMode::Auto => "auto",
            }
        });
        body[token_param] = json!(self.profile.max_output_tokens);
        if !(self.profile.model.starts_with("gpt-5") || self.profile.model.starts_with('o')) {
            body["temperature"] = json!(self.profile.temperature.unwrap_or(0.0));
        }
        let response = self
            .http
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|_| RuntimeError::Provider {
                status: None,
                retryable: true,
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(RuntimeError::Provider {
                status: Some(status.as_u16()),
                retryable: status.as_u16() == 429 || status.is_server_error(),
            });
        }
        let decoded: ChatCompletionResponse =
            response.json().await.map_err(|_| RuntimeError::Provider {
                status: None,
                retryable: false,
            })?;
        parse_chat_response(decoded, &self.tool_registry)
    }
}

#[derive(Debug)]
pub struct OpenAiResponsesClient {
    http: reqwest::Client,
    profile: ModelProfileRefV1,
    api_key: String,
    base_url: String,
    limiter: Arc<ModelLimiter>,
    tool_registry: Arc<ToolRegistry>,
    reviewer_policy: Arc<ReviewerPolicy>,
}

impl OpenAiResponsesClient {
    pub(crate) fn from_profile(
        profile: ModelProfileRefV1,
        base_url: String,
        limiter: Arc<ModelLimiter>,
        tool_registry: Arc<ToolRegistry>,
        reviewer_policy: Arc<ReviewerPolicy>,
        credential_resolver: Arc<dyn CredentialResolver>,
    ) -> RuntimeResult<Self> {
        let api_key = credential_resolver.resolve_credential(&profile.credential_ref)?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(90))
            .build()
            .map_err(|_| RuntimeError::Invariant("failed to build async HTTP client"))?;
        Ok(Self {
            http,
            profile,
            api_key,
            base_url: base_url.trim_end_matches('/').to_string(),
            limiter,
            tool_registry,
            reviewer_policy,
        })
    }
}

#[async_trait]
impl ConcurrentModelClient for OpenAiResponsesClient {
    async fn complete(
        &self,
        scope: &SessionScope,
        transcript: &[ConversationItem],
        _turn_id: TurnId,
        cancel: CancellationToken,
    ) -> RuntimeResult<ModelTurn> {
        if cancel.is_cancelled() {
            return Err(RuntimeError::Cancelled);
        }
        let _permit = self
            .limiter
            .acquire_for_model(
                "openai_compatible",
                &self.profile.id,
                &self.profile.credential_ref,
                &scope.id,
            )
            .await?;
        let body = responses_request_body(
            &self.profile,
            &self.reviewer_policy,
            &self.tool_registry,
            scope,
            transcript,
        )?;
        let response = self
            .http
            .post(format!("{}/responses", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|_| RuntimeError::Provider {
                status: None,
                retryable: true,
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(RuntimeError::Provider {
                status: Some(status.as_u16()),
                retryable: status.as_u16() == 429 || status.is_server_error(),
            });
        }
        let decoded: ResponsesResponse =
            response.json().await.map_err(|_| RuntimeError::Provider {
                status: None,
                retryable: false,
            })?;
        parse_responses_response(decoded, &self.tool_registry)
    }
}

fn chat_messages(
    reviewer_policy: &ReviewerPolicy,
    tool_registry: &ToolRegistry,
    scope: &SessionScope,
    transcript: &[ConversationItem],
) -> RuntimeResult<Vec<Value>> {
    let mut messages = Vec::new();
    for item in transcript {
        match item {
            ConversationItem::System { content } => {
                messages.push(json!({ "role": "system", "content": content }));
            }
            ConversationItem::User { content } => {
                messages.push(json!({ "role": "user", "content": content }));
            }
            ConversationItem::AssistantText { content } => {
                messages.push(json!({ "role": "assistant", "content": content }));
            }
            ConversationItem::AssistantToolCalls { calls } => {
                messages.push(json!({
                    "role": "assistant",
                    "tool_calls": calls.iter().map(|call| {
                        let name = model_alias_for_tool(tool_registry, &call.name)?;
                        Ok(json!({
                            "id": call.call_id.0,
                            "type": "function",
                            "function": {
                                "name": name.as_str(),
                                "arguments": call.raw_arguments,
                            }
                        }))
                    }).collect::<RuntimeResult<Vec<_>>>()?
                }));
            }
            ConversationItem::ToolResult {
                call_id, content, ..
            } => {
                let compact = reviewer_policy.compact_tool_result(content, &scope.capabilities);
                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": call_id.0,
                    "content": serde_json::to_string(&compact).map_err(|_| RuntimeError::Invariant("tool result serialization failed"))?,
                }));
            }
        }
    }
    Ok(messages)
}

fn responses_request_body(
    profile: &ModelProfileRefV1,
    reviewer_policy: &ReviewerPolicy,
    tool_registry: &ToolRegistry,
    scope: &SessionScope,
    transcript: &[ConversationItem],
) -> RuntimeResult<Value> {
    let mut instructions = Vec::new();
    let mut input = Vec::new();
    for item in transcript {
        match item {
            ConversationItem::System { content } => instructions.push(content.as_str()),
            ConversationItem::User { content } => {
                input.push(response_message("user", content));
            }
            ConversationItem::AssistantText { content } => {
                input.push(response_message("assistant", content));
            }
            ConversationItem::AssistantToolCalls { calls } => {
                for call in calls {
                    input.push(json!({
                        "type": "function_call",
                        "call_id": call.call_id.0,
                        "name": model_alias_for_tool(tool_registry, &call.name)?.as_str(),
                        "arguments": call.raw_arguments,
                    }));
                }
            }
            ConversationItem::ToolResult {
                call_id, content, ..
            } => {
                let compact = reviewer_policy.compact_tool_result(content, &scope.capabilities);
                input.push(json!({
                    "type": "function_call_output",
                    "call_id": call_id.0,
                    "output": serde_json::to_string(&compact)
                        .map_err(|_| RuntimeError::Invariant("tool result serialization failed"))?,
                }));
            }
        }
    }

    let mut body = json!({
        "model": profile.model.as_str(),
        "input": input,
        "tools": openai_tools_for_protocol(
            ModelApiProtocol::Responses,
            reviewer_policy,
            tool_registry,
            transcript,
            &scope.capabilities,
        )?,
        "parallel_tool_calls": false,
        "tool_choice": match profile.tool_calling_mode {
            ToolCallingMode::Required => "required",
            ToolCallingMode::Auto => "auto",
        },
        "max_output_tokens": profile.max_output_tokens,
    });
    if !instructions.is_empty() {
        body["instructions"] = json!(instructions.join("\n\n"));
    }
    if !(profile.model.starts_with("gpt-5") || profile.model.starts_with('o')) {
        body["temperature"] = json!(profile.temperature.unwrap_or(0.0));
    }
    if let Some(top_p) = profile.top_p {
        body["top_p"] = json!(top_p);
    }
    Ok(body)
}

fn response_message(role: &str, content: &str) -> Value {
    json!({
        "type": "message",
        "role": role,
        "content": [{
            "type": "input_text",
            "text": content,
        }],
    })
}

fn openai_tools_for_protocol(
    protocol: ModelApiProtocol,
    reviewer_policy: &ReviewerPolicy,
    tool_registry: &ToolRegistry,
    transcript: &[ConversationItem],
    capabilities: &CapabilitySet,
) -> RuntimeResult<Vec<Value>> {
    let tools =
        reviewer_policy.tool_schemas_for_transcript(tool_registry, transcript, capabilities);
    match protocol {
        ModelApiProtocol::ChatCompletions => Ok(tools),
        ModelApiProtocol::Responses => tools
            .into_iter()
            .map(response_tool_from_chat_tool)
            .collect::<RuntimeResult<Vec<_>>>(),
    }
}

fn response_tool_from_chat_tool(tool: Value) -> RuntimeResult<Value> {
    let function = tool
        .get("function")
        .and_then(Value::as_object)
        .ok_or(RuntimeError::Invariant("chat tool schema missing function"))?;
    let name = function
        .get("name")
        .and_then(Value::as_str)
        .ok_or(RuntimeError::Invariant("chat tool schema missing name"))?;
    let description =
        function
            .get("description")
            .and_then(Value::as_str)
            .ok_or(RuntimeError::Invariant(
                "chat tool schema missing description",
            ))?;
    let parameters = function
        .get("parameters")
        .cloned()
        .ok_or(RuntimeError::Invariant(
            "chat tool schema missing parameters",
        ))?;
    Ok(json!({
        "type": "function",
        "name": name,
        "description": description,
        "parameters": parameters,
        "strict": true,
    }))
}

fn model_alias_for_tool(tool_registry: &ToolRegistry, tool_id: &ToolId) -> RuntimeResult<ToolId> {
    tool_registry
        .alias_table()?
        .alias_for(tool_id)
        .cloned()
        .ok_or(RuntimeError::Invariant("missing model alias for tool"))
}

fn parse_chat_response(
    response: ChatCompletionResponse,
    tool_registry: &ToolRegistry,
) -> RuntimeResult<ModelTurn> {
    let usage = response.usage.unwrap_or_default().into_token_usage();
    let message = response
        .choices
        .into_iter()
        .next()
        .ok_or(RuntimeError::Provider {
            status: None,
            retryable: false,
        })?
        .message;
    let mut seen = HashSet::new();
    let calls = message
        .tool_calls
        .unwrap_or_default()
        .into_iter()
        .enumerate()
        .filter(|(_, call)| call.call_type == "function")
        .map(|(index, call)| {
            if !seen.insert(call.id.clone()) {
                return Err(RuntimeError::Provider {
                    status: None,
                    retryable: false,
                });
            }
            let alias = ToolId::parse(&call.function.name)?;
            let Some(name) = tool_registry.tool_id_for_model_alias(&alias) else {
                return Err(RuntimeError::InvalidInput("unknown tool name".to_string()));
            };
            Ok(ModelToolCall {
                call_id: ToolCallId(call.id),
                index,
                name,
                raw_arguments: call.function.arguments,
            })
        })
        .collect::<RuntimeResult<Vec<_>>>()?;
    if !calls.is_empty() {
        Ok(ModelTurn::ToolCalls { calls, usage })
    } else {
        Ok(ModelTurn::Text {
            content: message.content.unwrap_or_default(),
            usage,
        })
    }
}

fn parse_responses_response(
    response: ResponsesResponse,
    tool_registry: &ToolRegistry,
) -> RuntimeResult<ModelTurn> {
    let usage = response.usage.unwrap_or_default().into_token_usage();
    let mut seen = HashSet::new();
    let mut calls = Vec::new();
    let mut text = String::new();
    for (index, item) in response.output.into_iter().enumerate() {
        let item_type = item.get("type").and_then(Value::as_str);
        match item_type {
            Some("function_call") => {
                let call_id =
                    item.get("call_id")
                        .and_then(Value::as_str)
                        .ok_or(RuntimeError::Provider {
                            status: None,
                            retryable: false,
                        })?;
                if !seen.insert(call_id.to_string()) {
                    return Err(RuntimeError::Provider {
                        status: None,
                        retryable: false,
                    });
                }
                let alias = item
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or(RuntimeError::Provider {
                        status: None,
                        retryable: false,
                    })
                    .and_then(ToolId::parse)?;
                let Some(name) = tool_registry.tool_id_for_model_alias(&alias) else {
                    return Err(RuntimeError::InvalidInput("unknown tool name".to_string()));
                };
                let raw_arguments = item.get("arguments").and_then(Value::as_str).ok_or(
                    RuntimeError::Provider {
                        status: None,
                        retryable: false,
                    },
                )?;
                calls.push(ModelToolCall {
                    call_id: ToolCallId(call_id.to_string()),
                    index,
                    name,
                    raw_arguments: raw_arguments.to_string(),
                });
            }
            Some("message") => {
                if let Some(content) = item.get("content").and_then(Value::as_array) {
                    for part in content {
                        if matches!(
                            part.get("type").and_then(Value::as_str),
                            Some("output_text" | "text")
                        ) {
                            if let Some(part_text) = part.get("text").and_then(Value::as_str) {
                                text.push_str(part_text);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    if !calls.is_empty() {
        Ok(ModelTurn::ToolCalls { calls, usage })
    } else {
        Ok(ModelTurn::Text {
            content: response.output_text.unwrap_or(text),
            usage,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
    usage: Option<ChatUsage>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    content: Option<String>,
    tool_calls: Option<Vec<ChatToolCall>>,
}

#[derive(Debug, Deserialize)]
struct ChatToolCall {
    id: String,
    #[serde(rename = "type")]
    call_type: String,
    function: ChatToolFunction,
}

#[derive(Debug, Deserialize)]
struct ChatToolFunction {
    name: String,
    arguments: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct ChatUsage {
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    total_tokens: Option<u64>,
}

impl ChatUsage {
    fn into_token_usage(self) -> TokenUsage {
        TokenUsage {
            input_tokens: self.prompt_tokens.unwrap_or(0),
            output_tokens: self.completion_tokens.unwrap_or(0),
            total_tokens: self.total_tokens.unwrap_or(0),
        }
    }
}

#[derive(Debug, Deserialize)]
struct ResponsesResponse {
    output: Vec<Value>,
    output_text: Option<String>,
    usage: Option<ResponsesUsage>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct ResponsesUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    total_tokens: Option<u64>,
}

impl ResponsesUsage {
    fn into_token_usage(self) -> TokenUsage {
        TokenUsage {
            input_tokens: self.input_tokens.unwrap_or(0),
            output_tokens: self.output_tokens.unwrap_or(0),
            total_tokens: self.total_tokens.unwrap_or(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{AgentBudget, Role};
    use crate::runtime::tools::{CustomToolHandler, CustomToolOptions, CustomToolOutput};

    #[test]
    fn openai_client_uses_supplied_credential_resolver() {
        let registry = Arc::new(ToolRegistry::review_defaults().expect("registry"));
        let client = OpenAiChatCompletionsClient::from_profile(
            test_profile(),
            "http://localhost:9999/v1".to_string(),
            Arc::new(ModelLimiter::new(1)),
            registry,
            Arc::new(ReviewerPolicy::new()),
            Arc::new(StaticCredentialResolver),
        )
        .expect("client");

        assert_eq!(client.api_key, "resolved-key");
    }

    #[test]
    fn openai_responses_client_uses_supplied_credential_resolver() {
        let registry = Arc::new(ToolRegistry::review_defaults().expect("registry"));
        let client = OpenAiResponsesClient::from_profile(
            test_profile_for_protocol(ModelApiProtocol::Responses),
            "http://localhost:9999/v1".to_string(),
            Arc::new(ModelLimiter::new(1)),
            registry,
            Arc::new(ReviewerPolicy::new()),
            Arc::new(StaticCredentialResolver),
        )
        .expect("client");

        assert_eq!(client.api_key, "resolved-key");
    }

    #[test]
    fn openai_protocols_parse_tool_calls_through_same_alias_table() {
        let (registry, internal_tool, model_alias) = aliased_registry();
        let chat_turn = parse_chat_response(
            ChatCompletionResponse {
                choices: vec![ChatChoice {
                    message: ChatMessage {
                        content: None,
                        tool_calls: Some(vec![ChatToolCall {
                            id: "call_chat".to_string(),
                            call_type: "function".to_string(),
                            function: ChatToolFunction {
                                name: model_alias.as_str().to_string(),
                                arguments: r#"{"value":"ok"}"#.to_string(),
                            },
                        }]),
                    },
                }],
                usage: Some(ChatUsage {
                    prompt_tokens: Some(3),
                    completion_tokens: Some(2),
                    total_tokens: Some(5),
                }),
            },
            &registry,
        )
        .expect("chat turn");
        assert_tool_call_turn(chat_turn, &internal_tool, "call_chat", 3, 2, 5);

        let responses_turn = parse_responses_response(
            ResponsesResponse {
                output: vec![json!({
                    "type": "function_call",
                    "call_id": "call_responses",
                    "name": model_alias.as_str(),
                    "arguments": "{\"value\":\"ok\"}",
                    "status": "completed"
                })],
                output_text: None,
                usage: Some(ResponsesUsage {
                    input_tokens: Some(7),
                    output_tokens: Some(4),
                    total_tokens: Some(11),
                }),
            },
            &registry,
        )
        .expect("responses turn");
        assert_tool_call_turn(responses_turn, &internal_tool, "call_responses", 7, 4, 11);
    }

    #[test]
    fn openai_protocol_requests_replay_tool_calls_with_model_aliases() {
        let (registry, internal_tool, model_alias) = aliased_registry();
        let scope = test_scope();
        let transcript = vec![
            ConversationItem::System {
                content: "system".to_string(),
            },
            ConversationItem::User {
                content: "user".to_string(),
            },
            ConversationItem::AssistantToolCalls {
                calls: vec![ModelToolCall {
                    call_id: ToolCallId("call_aliased".to_string()),
                    index: 0,
                    name: internal_tool,
                    raw_arguments: r#"{"value":"ok"}"#.to_string(),
                }],
            },
        ];

        let chat = chat_messages(&ReviewerPolicy::new(), &registry, &scope, &transcript)
            .expect("chat messages");
        assert_eq!(
            chat[2]["tool_calls"][0]["function"]["name"].as_str(),
            Some(model_alias.as_str())
        );

        let responses = responses_request_body(
            &test_profile_for_protocol(ModelApiProtocol::Responses),
            &ReviewerPolicy::new(),
            &registry,
            &scope,
            &transcript,
        )
        .expect("responses body");
        assert_eq!(
            responses["input"][1]["name"].as_str(),
            Some(model_alias.as_str())
        );
        assert_eq!(
            responses["input"][1]["type"].as_str(),
            Some("function_call")
        );
        assert_eq!(responses["instructions"].as_str(), Some("system"));
    }

    #[test]
    fn openai_protocol_tool_schemas_share_exposure_policy() {
        let registry = ToolRegistry::review_defaults().expect("registry");
        let scope = test_scope();
        let transcript = vec![ConversationItem::User {
            content: "review".to_string(),
        }];
        let policy = ReviewerPolicy::new();
        let chat_tools = openai_tools_for_protocol(
            ModelApiProtocol::ChatCompletions,
            &policy,
            &registry,
            &transcript,
            &scope.capabilities,
        )
        .expect("chat tools");
        let responses_tools = openai_tools_for_protocol(
            ModelApiProtocol::Responses,
            &policy,
            &registry,
            &transcript,
            &scope.capabilities,
        )
        .expect("responses tools");

        assert_eq!(chat_tools.len(), responses_tools.len());
        assert_eq!(
            chat_tools[0]["function"]["name"],
            responses_tools[0]["name"]
        );
        assert_eq!(
            chat_tools[0]["function"]["parameters"],
            responses_tools[0]["parameters"]
        );
        assert_eq!(responses_tools[0]["type"].as_str(), Some("function"));
        assert_eq!(responses_tools[0]["strict"].as_bool(), Some(true));
        assert!(responses_tools[0].get("function").is_none());
    }

    #[test]
    fn openai_provider_canary_contract_covers_chat_and_responses() {
        let config = test_canary_config(false);
        let profiles = openai_provider_canary_profiles(&config);
        let protocols = profiles
            .iter()
            .map(|profile| profile.api_protocol)
            .collect::<Vec<_>>();
        assert_eq!(protocols.as_slice(), openai_provider_canary_protocols());
        assert_eq!(profiles[0].credential_ref, config.credential_ref);
        assert_eq!(profiles[1].model, config.model);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let reports = runtime.block_on(run_openai_provider_canaries(
            config,
            Arc::new(StaticCredentialResolver),
        ));
        assert_eq!(reports.len(), openai_provider_canary_protocols().len());
        let evidence = ModelProviderCanaryEvidence::with_generated_at("123.000000000Z", reports);
        assert_eq!(
            evidence.gate.skipped,
            openai_provider_canary_protocols().len()
        );
        assert!(!evidence.gate.valid);
    }

    #[test]
    fn openai_provider_canary_skips_all_protocols_without_credential() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let reports = runtime.block_on(run_openai_provider_canaries(
            test_canary_config(true),
            Arc::new(MissingCredentialResolver),
        ));
        assert_eq!(reports.len(), openai_provider_canary_protocols().len());
        assert!(reports.iter().all(|report| {
            matches!(
                &report.status,
                ModelProviderCanaryStatus::Skipped { reason }
                    if reason == "credential unavailable"
            )
        }));
    }

    #[test]
    fn model_provider_canary_evidence_gates_full_protocol_matrix() {
        let config = test_canary_config(true);
        let evidence = ModelProviderCanaryEvidence::with_generated_at(
            "123.000000000Z",
            vec![
                ModelProviderCanaryReport::passed(ModelApiProtocol::ChatCompletions, &config),
                ModelProviderCanaryReport::skipped(
                    ModelApiProtocol::Responses,
                    &config,
                    "credential unavailable",
                ),
            ],
        );

        assert!(!evidence.gate.valid);
        assert_eq!(evidence.gate.passed, 1);
        assert_eq!(evidence.gate.skipped, 1);
        assert!(evidence
            .gate
            .failures
            .contains(&"responses skipped: credential unavailable".to_string()));
        assert!(evidence
            .require_passed()
            .unwrap_err()
            .to_string()
            .contains("responses skipped"));

        let missing = ModelProviderCanaryEvidence::with_generated_at(
            "123.000000000Z",
            vec![ModelProviderCanaryReport::passed(
                ModelApiProtocol::ChatCompletions,
                &config,
            )],
        );
        assert!(missing
            .gate
            .failures
            .contains(&"missing responses canary report".to_string()));

        let duplicate = ModelProviderCanaryEvidence::with_generated_at(
            "123.000000000Z",
            vec![
                ModelProviderCanaryReport::passed(ModelApiProtocol::ChatCompletions, &config),
                ModelProviderCanaryReport::passed(ModelApiProtocol::ChatCompletions, &config),
                ModelProviderCanaryReport::passed(ModelApiProtocol::Responses, &config),
            ],
        );
        assert!(duplicate
            .gate
            .failures
            .contains(&"duplicate chat_completions canary reports: 2".to_string()));
    }

    #[test]
    fn model_provider_canary_evidence_export_roundtrips() {
        let config = test_canary_config(true);
        let reports = openai_provider_canary_protocols()
            .iter()
            .copied()
            .map(|protocol| ModelProviderCanaryReport::passed(protocol, &config))
            .collect::<Vec<_>>();
        let evidence = ModelProviderCanaryEvidence::with_generated_at("123.000000000Z", reports);
        evidence.require_passed().expect("valid evidence");

        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp
            .path()
            .join("canaries")
            .join("real-provider-openai.json");
        let export = export_model_provider_canary_evidence(&path, &evidence).expect("export");

        assert_eq!(export.path, path);
        assert!(export.valid);
        assert!(export.bytes > 0);
        let serialized = std::fs::read_to_string(&export.path).expect("read evidence");
        assert!(serialized.ends_with('\n'));
        let loaded: ModelProviderCanaryEvidence =
            serde_json::from_str(&serialized).expect("load evidence");
        assert_eq!(
            loaded.schema_version,
            MODEL_PROVIDER_CANARY_EVIDENCE_SCHEMA_VERSION
        );
        assert_eq!(
            loaded.required_protocols,
            openai_provider_canary_protocols()
        );
        assert_eq!(loaded.gate.passed, openai_provider_canary_protocols().len());
        loaded.require_passed().expect("roundtrip evidence valid");

        let mut forged = loaded.clone();
        forged.reports.pop();
        forged.gate.valid = true;
        forged.gate.failures.clear();
        let error = forged.require_passed().unwrap_err().to_string();
        assert!(error.contains("stored provider canary gate does not match reports"));
        assert!(error.contains("missing responses canary report"));
    }

    #[test]
    fn model_limiter_isolates_distinct_credential_keys() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let limiter = ModelLimiter::new_with_per_key(2, 1);
            let first_key_a = limiter.acquire_for_key("credential-a").await.unwrap();

            let second_key_a = tokio::time::timeout(
                Duration::from_millis(5),
                limiter.acquire_for_key("credential-a"),
            )
            .await;
            assert!(second_key_a.is_err());

            let key_b = tokio::time::timeout(
                Duration::from_millis(50),
                limiter.acquire_for_key("credential-b"),
            )
            .await
            .expect("credential-b should not share credential-a limiter")
            .unwrap();

            drop(key_b);
            drop(first_key_a);
        });
    }

    #[test]
    fn model_limiter_isolates_provider_profile_key_and_session_buckets() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let limiter = ModelLimiter::new_with_buckets(4, 4, 1, 4, 1);
            let session_a = SessionId("session-a".to_string());
            let session_b = SessionId("session-b".to_string());
            let profile_a = limiter
                .acquire_for_model("provider", "profile-a", "credential-a", &session_a)
                .await
                .unwrap();

            let same_session = tokio::time::timeout(
                Duration::from_millis(5),
                limiter.acquire_for_model("provider", "profile-b", "credential-b", &session_a),
            )
            .await;
            assert!(same_session.is_err());

            let same_profile = tokio::time::timeout(
                Duration::from_millis(5),
                limiter.acquire_for_model("provider", "profile-a", "credential-b", &session_b),
            )
            .await;
            assert!(same_profile.is_err());

            let unrelated_profile = tokio::time::timeout(
                Duration::from_millis(50),
                limiter.acquire_for_model("provider", "profile-b", "credential-b", &session_b),
            )
            .await
            .expect("unrelated profile/session should not share saturated buckets")
            .unwrap();

            drop(unrelated_profile);
            drop(profile_a);
        });
    }

    struct StaticCredentialResolver;

    impl CredentialResolver for StaticCredentialResolver {
        fn resolve_credential(&self, credential_ref: &str) -> RuntimeResult<String> {
            assert_eq!(credential_ref, "test:credential");
            Ok("resolved-key".to_string())
        }
    }

    struct MissingCredentialResolver;

    impl CredentialResolver for MissingCredentialResolver {
        fn resolve_credential(&self, _credential_ref: &str) -> RuntimeResult<String> {
            Err(RuntimeError::InvalidInput("missing credential".to_string()))
        }
    }

    fn test_profile() -> ModelProfileRefV1 {
        test_profile_for_protocol(ModelApiProtocol::ChatCompletions)
    }

    fn test_canary_config(enabled: bool) -> OpenAiProviderCanaryConfig {
        OpenAiProviderCanaryConfig {
            enabled,
            base_url: "http://localhost:9999/v1".to_string(),
            credential_ref: "test:credential".to_string(),
            model: "gpt-4o-mini".to_string(),
            max_output_tokens: 16,
            prompt: "Return ready.".to_string(),
        }
    }

    fn test_profile_for_protocol(api_protocol: ModelApiProtocol) -> ModelProfileRefV1 {
        ModelProfileRefV1 {
            id: "test-profile".to_string(),
            provider_kind: ProviderKind::OpenaiCompatible,
            api_protocol,
            provider_profile_id: "test-provider-profile".to_string(),
            credential_ref: "test:credential".to_string(),
            model: "gpt-4o-mini".to_string(),
            max_input_tokens: 32_000,
            max_output_tokens: 128,
            tool_calling_mode: ToolCallingMode::Auto,
            temperature: Some(0.0),
            top_p: None,
        }
    }

    fn test_scope() -> SessionScope {
        SessionScope::review_read_only(
            SessionId("test-session".to_string()),
            Role::Generalist,
            "test objective",
            AgentBudget {
                max_turns: 2,
                max_tool_calls: 4,
                max_prompt_tokens: 1024,
                max_output_tokens: 128,
            },
        )
    }

    fn aliased_registry() -> (ToolRegistry, ToolId, ToolId) {
        let mut registry = ToolRegistry::review_defaults().expect("registry");
        let internal_tool = ToolId::parse("internal_review_tool").unwrap();
        let model_alias = ToolId::parse("provider_review_tool").unwrap();
        registry
            .register_custom_with_alias_and_effects(
                internal_tool.clone(),
                model_alias.clone(),
                "provider-visible alias test tool",
                json!({
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" }
                    },
                    "required": ["value"],
                    "additionalProperties": false
                }),
                CustomToolOptions::default(),
                Arc::new(NoopCustomTool),
            )
            .unwrap();
        (registry, internal_tool, model_alias)
    }

    fn assert_tool_call_turn(
        turn: ModelTurn,
        expected_tool: &ToolId,
        expected_call_id: &str,
        input_tokens: u64,
        output_tokens: u64,
        total_tokens: u64,
    ) {
        let ModelTurn::ToolCalls { calls, usage } = turn else {
            panic!("expected tool-call turn");
        };
        assert_eq!(calls.len(), 1);
        assert_eq!(&calls[0].name, expected_tool);
        assert_eq!(calls[0].call_id.0, expected_call_id);
        assert_eq!(calls[0].raw_arguments, r#"{"value":"ok"}"#);
        assert_eq!(usage.input_tokens, input_tokens);
        assert_eq!(usage.output_tokens, output_tokens);
        assert_eq!(usage.total_tokens, total_tokens);
    }

    struct NoopCustomTool;

    #[async_trait::async_trait]
    impl CustomToolHandler for NoopCustomTool {
        async fn execute(
            &self,
            _context: crate::runtime::tools::CustomToolContext,
            _args: Value,
            _cancel: CancellationToken,
        ) -> RuntimeResult<CustomToolOutput> {
            Ok(CustomToolOutput::default())
        }
    }
}
