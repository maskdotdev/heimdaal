# Top-Tier Reviewer Primitive Plan

Generated: 2026-06-01 after external Pro architecture review.

## Core Direction

Heimdaal should become a capability-secured review execution kernel, not just an agent runtime with extension hooks.

The durable primitive shape is:

- `Run`: owns orchestration, budgets, cancellation, final report, and event stream.
- `Snapshot`: owns immutable repo evidence, manifest, read/search services, and per-snapshot caches.
- `SessionScope`: owns persona, cwd/subtree visibility, model profile, tool grants, and session budgets.
- `ModelRouter`: owns per-session model/profile/API-key selection, rate limits, retries, and token/cost budgets.
- `ToolRegistry`: owns stable tool specs, schemas, provider-visible aliases, versions, and effects.
- `ToolProvider`: owns execution behind the capability wall, for built-in, in-process, and external tools.
- `ArtifactStore`: owns evidence, redacted content, provenance, retrieval, and export.
- `EventSink` plus metrics: owns host observability without forcing hosts to inspect internals.

## Strongest Design Corrections

### 1. Make `ToolId` The Public Tool Identity

Do not expose `ToolName` in public result/event/metrics contracts. Built-ins should just be registered tools with stable `ToolId`s.

Keep an internal built-in classification if useful, but public APIs should use:

```rust
pub struct ToolId(String);
pub struct ProviderId(String);
pub struct ModelToolName(String);
```

Provider-visible function names should be aliases, not raw internal `ToolId`s. Add a registry compilation step that maps:

```text
ToolId -> provider-safe ModelToolName
```

and rejects alias collisions.

### 2. Replace `ToolMask + allowed_custom_tools` With One Capability Contract

The split policy model will become a bypass risk. Use a single `CapabilitySet`.

```rust
pub struct CapabilitySet {
    pub fs_scope: FsScope,
    pub tool_grants: BTreeMap<ToolId, ToolGrant>,
    pub artifact_access: ArtifactAccessPolicy,
    pub model_visible_output: OutputPolicy,
    pub budgets: CapabilityBudgets,
}

pub struct ToolGrant {
    pub allow: bool,
    pub max_calls: Option<u32>,
    pub input_constraints: Option<JsonSchema>,
    pub output_policy: OutputPolicy,
    pub effects_allowed: ToolEffects,
}
```

Separate hard authorization from prompt shaping:

```rust
pub trait ToolExposurePolicy: Send + Sync {
    fn tools_for_turn(&self, ctx: &TurnPolicyContext<'_>) -> Vec<ToolId>;
}
```

Capabilities decide what is possible. Exposure policy decides what the model sees this turn.

### 3. Make Cache Keys Scope-Aware

Current duplicate-search collapse is valuable, but it becomes dangerous once cwd/subtree scopes exist.

Every read/search/tool cache key must include effective scope:

```rust
pub struct ToolCacheKey {
    pub snapshot_id: SnapshotId,
    pub scope_key: ScopeKey,
    pub tool_id: ToolId,
    pub tool_version: ToolVersion,
    pub schema_digest: SchemaDigest,
    pub canonical_args_hash: Hash,
    pub redaction_policy_version: u16,
    pub contract_version: u16,
}
```

For search, `ScopeKey` must represent the candidate file set, not only a cwd string.

Minimum scope ingredients:

- snapshot id
- cwd
- allowed subtree roots
- denied globs/rules
- language/glob filters
- changed-only flag
- manifest version/candidate set hash

Otherwise a `src/payments/` session could receive a cached root-wide search result.

### 4. Add Per-Session Model Routing And BYOK Isolation

The runtime should not hold one global model client.

```rust
#[async_trait::async_trait]
pub trait ModelRouter: Send + Sync {
    async fn client_for(
        &self,
        session: &SessionScope,
    ) -> RuntimeResult<Arc<dyn ModelClient>>;
}
```

Limits must be isolated by:

- provider
- model profile
- API key reference
- session

Do not pool unrelated API keys behind one limiter.

### 5. Make Custom Tools First-Class In Metrics

Metrics keyed by built-in enum will permanently make custom tools second-class.

Use dynamic metrics:

```rust
pub struct ToolMetricKey {
    pub tool_id: ToolId,
    pub provider_id: ProviderId,
}
```

Track:

- calls
- successes/errors
- latency
- queue wait
- cache hits/dedupe waiters
- input/output bytes
- artifacts created
- cancellation/timeout counts

### 6. Add Public Artifact And Event APIs

Other reviewer engines need evidence and audit trails, not only final counts.

Add:

- artifact retrieval by `ArtifactId`
- finding -> evidence -> artifact traversal
- redacted/default export
- optional raw export gated by policy
- JSONL event stream
- bounded event sink with explicit backpressure policy

Events should be public and JSONL-stable:

```rust
#[non_exhaustive]
pub enum RuntimeEvent {
    RunStarted(RunStarted),
    SnapshotManifestCompleted(SnapshotManifestCompleted),
    SessionStarted(SessionStarted),
    ModelCallStarted(ModelCallStarted),
    ModelCallCompleted(ModelCallCompleted),
    ToolBatchStarted(ToolBatchStarted),
    ToolCallCompleted(ToolCallCompleted),
    SearchBatchCompleted(SearchBatchCompleted),
    ArtifactCreated(ArtifactCreated),
    FindingRecorded(FindingRecorded),
    BudgetExceeded(BudgetExceeded),
    Cancelled(Cancelled),
    RunFinished(RunFinished),
}
```

Each event should include run id, sequence number, timestamp, and optional snapshot/session/turn ids.

## Custom Tool Model

### Tool Effects

Every tool declares effects:

```rust
pub struct ToolEffects {
    pub repo_read: bool,
    pub artifact_read: bool,
    pub artifact_write: bool,
    pub network_read: bool,
    pub host_read: bool,
    pub external_side_effect: bool,
}
```

Default reviewer primitive policy:

- Allow repo read, artifact read/write, and side-effect-free compute.
- Deny network read, arbitrary host read, filesystem writes, subprocesses, and external side effects unless explicitly granted.

### In-Process Providers

Treat these as trusted plugins only. They are fast, but not sandboxed.

Required guardrails:

- timeout
- bounded concurrency
- input schema validation
- output schema validation
- output byte limits
- redaction before model visibility
- panic containment where possible
- provider/tool metrics

### Out-Of-Process Providers

Support JSON-RPC and MCP-style adapters later.

Rules:

- spawn directly, never through shell
- minimal environment
- no API keys unless explicitly granted
- no repo root mount by default
- persistent process/connection, not spawn-per-call
- bounded request/response/stderr payloads
- per-call timeout
- provider health state
- kill on shutdown or protocol violation
- every external tool still needs a `ToolGrant`

External tools should receive bounded content, artifact refs, or mediated APIs, not raw repo-root access.

## Concurrency Shape

Keep the current foundation and make it policy-driven:

- One model call at a time per session.
- Many sessions can call models concurrently.
- Model concurrency/rate/token budgets enforced per provider/profile/API key.
- Tool batches validate all calls before execution.
- Tool results append in original model-call order.
- `finish` must be alone.
- Full-repo search concurrency should stay low; one deduped, parallel, microbatched scan usually beats many concurrent scans.

Recommended default limits:

```toml
[runtime]
max_active_sessions = 100

[model]
global_concurrency = 16
per_profile_concurrency = 8
per_api_key_concurrency = 4

[tools]
max_tool_calls_per_turn = 4
per_session_tool_parallelism = 2
global_read_concurrency = 32
global_custom_tool_concurrency = 16
search_queue_depth_per_snapshot = 128
full_repo_searches_per_snapshot = 1
search_threads_per_snapshot = 4
```

## Proof Gates

Keep the current deterministic mock proof and add these gates.

### Existing Gates To Preserve

- 50 sessions: RSS <= 15 MB, speedup >= 8x, search scans 50 -> 1.
- 100 sessions: RSS <= 25 MB, speedup >= 15x, search scans 100 -> 1.

### Scope Isolation Gates

- Root session search sees root-visible matches.
- `src/` session search sees only `src/`.
- Same query across different scopes does not share unsafe cache results.
- `read_file("../secret")` is denied.
- File outside cwd but inside repo is denied unless scope grants it.

### Dynamic Tool Gates

- Built-in and custom metrics both keyed by `ToolId`.
- Unknown custom tool is denied.
- Registered but ungranted custom tool is denied.
- Provider alias collision is rejected.
- Same provider-visible alias maps to exactly one `ToolId`.

### External Provider Gates

- Malformed JSON-RPC -> typed provider protocol error.
- Timeout -> typed timeout, no leaked process.
- Huge payload -> too_large/truncated.
- Provider crash -> provider_unavailable, run continues if policy allows.
- Provider stderr containing fake secret is redacted in events.

### Real Provider Cheap-Model Gates

Run with one OpenAI-compatible cheap model.

10 sessions:

- zero protocol-shape errors
- all tool_call_ids matched
- no dropped tool calls
- p95 model wait recorded
- p95 tool wait recorded
- final report valid

50 sessions:

- rate limiter prevents provider stampede
- no request after cancellation
- per-profile metrics correct
- per-api-key metrics correct

### Cancellation Gates

Cancel while:

- model call is in flight
- queued for search
- deduped behind another search
- external provider is running
- tool completed before transcript commit

Required:

- no panic
- no leaked tasks
- no orphan transcript append
- resources cleaned by shutdown deadline
- partial report emitted when policy allows

## Implementation Order

1. Public API facade and identity cleanup.
   - Add `RunSpec`, `RunHandle`, `RunReport`.
   - Add `SnapshotSpec`, `SnapshotHandle`.
   - Add `SessionSpec`, `SessionScope`.
   - Make `ToolId` public and remove public `ToolName`.
   - Add provider-safe model tool aliases.

2. Unified capability contract and per-session cwd.
   - Add `CapabilitySet`, `ToolGrant`, `FsScope`, `ScopeKey`.
   - Replace `ToolMask + allowed_custom_tools`.
   - Include scope in cache/dedupe keys.

3. Dynamic metrics and host-facing events.
   - Add `ToolMetrics` keyed by `ToolId`.
   - Add model/profile/API-key metrics.
   - Add bounded public event stream.

4. ModelRouter and BYOK.
   - Route model client per session.
   - Add per-profile and per-API-key limiters.
   - Add budget accounting per profile/key/session.

5. Artifact retrieval/export API.
   - Add artifact/evidence lookup.
   - Add redacted/default export.
   - Gate raw export by policy.

6. ToolProvider v2.
   - Built-ins use the same provider path as custom tools.
   - Add trusted in-process provider helper.
   - Add tool context with scoped repo/artifact APIs.

7. External provider adapters.
   - JSON-RPC provider.
   - MCP-style provider.
   - Admission, health, timeout, payload, and shutdown policies.

8. Responses adapter parity.
   - Provider-neutral transcript remains unchanged.
   - Both Chat Completions and Responses consume the same registry/alias table.

9. Multi-snapshot run hardening.
   - Multiple snapshots per run.
   - Per-session snapshot selection.
   - Snapshot-level services and metrics.

10. Optimization after measurement.
   - Microbatch search.
   - Optional trigram index.
   - Import graph/related-file helpers.
   - Persistent artifact backend.

## Immediate Next PR

The next PR should not add more tools. It should clean the foundation:

1. Introduce public facade types under a `lib.rs`.
2. Rename public protocol around `ToolId` and add provider aliasing.
3. Add `CapabilitySet`, `ToolGrant`, `FsScope`, and `SessionScope`.
4. Make read/search cache keys include `ScopeKey`.
5. Add dynamic `ToolMetrics`.
6. Extend tests for scoped cache isolation.

That is the shortest path from the current branch to a primitive other reviewing engines can safely embed.
