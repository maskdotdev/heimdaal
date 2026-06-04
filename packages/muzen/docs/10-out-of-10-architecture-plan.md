# Muzen 10/10 Architecture Plan

Generated: 2026-06-03

## Purpose

This plan defines what it would take for `muzen` to become a 10/10
architecture for a reusable repository-review execution kernel.

The target is not just a cleaner Rust crate. The target is a deep, stable
module interface that lets Heimdaal and other trusted hosts run many
capability-scoped review sessions over immutable repository evidence, with
auditable artifacts, deterministic events, enforceable policy, and measurable
resource behavior.

## Current Baseline

`muzen` is already a strong internal V1 runtime.

Strengths:

- One concurrent runtime path after sync deletion.
- Bounded active sessions and per-session tool parallelism.
- One shared repository manifest per run.
- Text-candidate files are captured into snapshot-owned bytes at manifest
  build time; `read_file` and `search_text` use those captured bytes rather
  than later live worktree contents.
- `SnapshotStoragePolicy` gives hosts explicit memory, content-addressed
  directory, and remote object-store backing modes for captured text evidence, and
  `SnapshotManifest` reports capture skips caused by the configured envelope.
- `RunReport` exposes `SnapshotReader` and `SnapshotManifest` so hosts can
  inspect snapshot provenance and read captured text evidence without touching
  private runtime modules.
- Search singleflight and scoped cache keys.
- Dynamic `ToolId` in model calls, transcript items, result envelopes, and
  metrics.
- `ToolRegistry` support for built-in and in-process custom tools.
- Per-session capability grants for tools and filesystem scope.
- `ToolAuthorizer` enforces granted tools, tool effects, and per-session
  per-tool call limits before provider execution.
- `CapabilitySet` now owns artifact access, optional per-artifact allowlists,
  model-visible output, tool input, runtime provider and provider-resource
  scopes, and runtime authority policies for network, host, scratch, and
  external side effects; raw artifact export is constructor-gated by raw
  artifact read authority.
- In-process and JSON-RPC custom tools share one provider-output path for
  redaction, output byte accounting, artifact insertion, artifact-write
  authority, and output/artifact size limits.
- Public reviewer-facade tests now prove JSON-RPC provider execution through
  both an in-memory transport and the real HTTP JSON-RPC transport, including
  wire-envelope method, provider id, tool id, provider-resource scope, and
  argument propagation.
- `ReviewerPolicy` owns tool exposure, evidence readiness, terminal result
  tracking, repeated terminal-denial failure policy, session state resolution,
  transcript compaction, model-visible output policy enforcement, and retry
  eligibility.
- `RunSpec` now carries public `ReviewSessionSpec` values, so hosts can
  describe review sessions, model-profile selection, snapshot targeting, tool
  denial, and trusted custom read-only tool grants without constructing raw
  runtime `SessionScope` values.
- `RunBuilder::review_model` accepts a public `ReviewModel` adapter with
  `ReviewModelRequest`, `ReviewTranscriptItem`, `ReviewModelTurn`, and
  `ReviewToolCall`, so simple host models no longer need to implement the
  low-level runtime `ModelClient` trait or inspect raw `ConversationItem`
  values.
- `RunBuilder::review_tool_registry` accepts a public `ReviewToolRegistry`.
  Simple host custom tools can implement `ReviewToolHandler` and return
  `ReviewToolOutput` / `ReviewToolArtifact` without constructing
  `ToolRegistry`, `CustomToolHandler`, `CustomToolContext`, or
  `CustomToolOutput` directly.
- `ReviewToolRegistry` and `ReviewSessionSpec` now expose host-facing JSON-RPC
  provider registration and grants for read-only and network-read tools, so
  network authority can be reviewed at the same module interface where the
  provider tool is registered and granted.
- `muzen-runner stdio` now exposes the reviewer kernel through a stable
  newline-delimited JSON-RPC protocol for SDK hosts. It supports stateful run
  requests, artifact and snapshot reads, SDK `model.complete` callbacks, SDK
  `tool.execute` callbacks, and streamed `event.review` / `event.runtime`
  notifications without exposing private runtime modules.
- Runner handshake and schema fixtures now advertise model/tool callbacks and
  runtime event notifications as implemented protocol methods, and the
  interactive runner test proves a mock SDK model and custom tool can drive a
  full run through that seam.
- `RunBuilder::review_event_sink` accepts a public `ReviewEventSink`.
  Hosts can consume and persist `ReviewEventRecord` and `ReviewEvent` values
  through review-event JSONL export/load without matching raw `RuntimeEvent`
  variants or reading `RuntimeEventContext`.
- `reviewer::canaries` gives hosts and schedulers one advanced canary module
  for live model-provider evidence, remote snapshot/artifact object-store
  evidence, and a schema-versioned aggregate canary evidence manifest that
  fails closed when required provider, snapshot-store, or artifact-store proof
  is missing, duplicated, skipped, failed, or forged.
- `muzen canary-manifest` composes provider and remote object-store evidence
  JSON files into the aggregate canary manifest, writes a durable proof object,
  and exits non-zero when the gate does not pass. This gives scheduled jobs and
  CI one command-line proof gate instead of requiring bespoke JSON parsing.
- The aggregate canary manifest supports a freshness policy. The CLI defaults
  to rejecting child or aggregate evidence older than 24 hours, and the
  reviewer canary module exposes the same policy for schedulers that need a
  different window.
- `muzen canary-verify` validates an already-published aggregate canary
  manifest with the same freshness policy. Release gates can therefore check
  the exact promoted proof artifact without needing to reassemble child
  evidence files.
- `CanaryEvidenceManifest::status_report` and `muzen canary-status` expose the
  same gate and freshness checks as a structured status artifact, separating
  schema/gate failures from stale/future evidence failures so a reviewer can
  audit a promoted manifest without reverse-engineering a process exit code. The
  status report also summarizes the required provider protocol matrix,
  observed provider protocol results, and observed snapshot/artifact remote
  object-store evidence, so `status.json` is the reviewable proof entrypoint for
  the scheduled evidence bundle. `muzen canary-publish` writes this report
  beside the promoted manifest before returning a manifest-gate failure, and
  the scheduled workflow also verifies it before promotion.
- `muzen canary-publish` also writes `publication.json`, a provenance report
  that records whether provider evidence came from live canaries or a reused
  evidence file, which object-store driver was used, which model/provider base
  URL was configured, the freshness window, emitted evidence filenames, and the
  final status failures. A passing scheduled bundle can therefore prove it used
  live provider canaries plus the HTTP object-store adapter rather than only
  presenting a locally reusable manifest.
- `muzen canary-preflight` validates the same publication configuration used by
  `canary-publish` before evidence files are written: output path shape,
  reused provider evidence, live-provider credentials, provider base URL,
  remote snapshot/artifact base URIs, object-store driver compatibility,
  optional HTTP bearer-token environment, provider output envelope, and
  freshness window. It also records a schema-versioned publication config
  summary with provider mode, object-store driver, base URIs, effective
  provider base URL, model, output envelope, and freshness window. The
  scheduled workflow saves this preflight report as `preflight.json`, so
  configuration failures leave a structured artifact too.
- `muzen canary-workflow-provenance` writes schema-versioned GitHub Actions
  provenance from the scheduled job environment. The scheduled workflow now
  calls Muzen for `workflow.json`, so the Rust canary module owns both
  provenance production and validation instead of relying on bespoke workflow
  JSON assembly.
- `muzen canary-proof` validates a full scheduled evidence directory as one
  final proof bundle. It requires `workflow.json`, `preflight.json`,
  `publication.json`, child provider/snapshot/artifact evidence,
  `manifest.json`, and `status.json`; it rejects reused provider evidence,
  non-HTTP object-store publication, non-HTTP remote base URIs, stale or failed
  manifests, failed child evidence, child evidence that does not match the
  aggregate manifest, and a saved preflight report that is not shaped like a
  live scheduled run or whose recorded config does not match the
  publication/provider/object-store evidence. The proof verifier also
  freshness-gates workflow, preflight, publication, and status proof
  timestamps, and it requires workflow provenance with a scheduled GitHub
  Actions event plus the expected workflow/job identity and optional exact
  repository/ref identity. `proof.json` records that expected
  event/workflow/job/repository/ref identity beside the observed provenance,
  and it records `fileDigests` with byte counts and BLAKE3 hashes for every
  required evidence JSON file, so reviewers can audit the final gate and the
  exact validated file bytes from the artifact alone. The scheduled workflow
  writes this report as `proof.json`, pins it to the current GitHub repository
  and ref, runs with explicit read-only repository permissions, avoids
  overlapping evidence runs with a non-cancelling concurrency group, installs
  and selects stable Rust through `rustup` instead of an extra third-party
  action, and uploads the evidence bundle with an
  explicit 30-day retention period while failing the upload step if no evidence
  files are produced.
- Raw runtime event payload/context/record and sink-trait compatibility now
  lives under `muzen::reviewer::runtime_events`, so the facade root points hosts
  toward review events while still preserving migration fixtures and low-level
  runtime proof.
- Low-level model router/client contracts now live under
  `muzen::reviewer::model_adapters`, and low-level tool registry/provider
  contracts now live under `muzen::reviewer::tool_adapters`. The facade root
  keeps `ReviewModel` and `ReviewToolRegistry` as the preferred host
  interfaces.
- Capability/grant/policy/scope contracts now live under
  `muzen::reviewer::capabilities`, and run/cache/counter metrics contracts live
  under `muzen::reviewer::metrics`.
- Redaction before tool output becomes model-visible.
- JSONL lifecycle events for the CLI path, host-facing review events for public
  runs, public runtime denial events for rejected tool calls, and runtime event
  records with run/snapshot/session/turn and tool-call context.
- Versioned JSONL compatibility fixture coverage for every public
  `RuntimeEvent` variant, plus schema-version validation when loading event
  logs.
- Meaningful tests for path safety, scoped cache isolation, custom tool
  allowlisting, capability denials, input limits, model-visible output policy,
  cancellation before run start, in-flight model cancellation, in-flight
  JSON-RPC provider cancellation, queued/deduped search cancellation,
  post-tool/pre-transcript cancellation, retry behavior, and runtime events.

Main architectural gaps:

- The public `reviewer` module now has a real run facade plus host-facing
  session, model, tool, and event adapters, and raw runtime event
  payload/context/record access has moved behind an explicit advanced
  `runtime_events` module. Raw model and tool adapter contracts have also moved
  behind explicit `model_adapters` and `tool_adapters` modules. Runtime event
  JSONL helpers and bounded raw event sinks now live inside `runtime_events`.
  Capability and metrics contract surface now lives behind explicit
  `capabilities` and `metrics` modules. `RunReport::summary` is now the
  host-facing `ReviewRunSummary`, while the raw `ConcurrentRunReport` is kept
  explicit under `RunReport::metrics`. Raw ids, artifact views, repo paths,
  snapshot storage policy, and runtime result/limit/error contracts no longer
  re-export from the root facade; they live under explicit `ids`, `artifacts`,
  `paths`, `storage`, and `runtime` modules. `RunSpec` now takes host-facing
  `ReviewRunLimits` for common run construction, with raw `RuntimeLimits`
  retained only as an explicit advanced escape hatch. Remaining
  public-interface depth work is replacing compatibility contracts in
  session and advanced artifact-object workflows with narrower reviewer-owned
  builders and value constructors.
- Some CLI and benchmark paths still carry legacy `ReviewRunJobV1` adapter
  concerns that should become thinner host adapters.
- `RepoSnapshot` captures text-candidate bytes and exposes them through the
  public reviewer facade with memory, content-addressed directory, or remote
  object-store backing stores. `SnapshotReader` now exposes storage validation
  and cleanup reports for captured snapshot objects. Public remote
  object-store canary evidence now proves host-provided snapshot clients can
  put, read, remove, and verify removal; scheduled production cloud-client
  canary runs are still incomplete.
- Artifact bundle export now has a public lifecycle interface:
  `ArtifactBundleManifest::validate_storage` and `cleanup_storage` validate
  and remove only manifest-owned bundle objects. `ArtifactRetentionPolicy` gives
  hosts a count/byte envelope before artifact manifests or bundles are
  produced. `ArtifactObjectStore` gives hosts a persistence contract with
  in-memory, local filesystem, and remote-URI adapters, and serialized
  persistence manifests can be validated against reopened or host-provided
  object stores. Public remote object-store canary evidence now proves
  host-provided artifact clients can put, read, remove, and verify removal of
  content-addressed objects. `ArtifactPersistenceManifest::cleanup_storage`
  now removes manifest-owned objects through the same host-facing store seam.
  Scheduled production cloud-client canary runs are still incomplete.
- `CapabilitySet` now covers artifact access, per-artifact read scopes,
  model-visible output, tool input size, runtime provider scopes, runtime
  provider-resource scopes, runtime authority for scratch/network/host/external
  side effects, tool/effect grants, and denial events. JSON-RPC and public
  in-process host-tool fixtures now prove provider-resource allow/deny behavior.
  Remaining external-provider security gaps are broader real-provider contract
  gates.
- `JobRuntime` now owns active-session scheduling and report
  aggregation, while the per-session async loop lives behind `SessionRunner`.
  Reviewer-specific prompt/tool exposure, evidence readiness, terminal rules,
  retry behavior, evidence and session-budget tool-batch planning, session
  state, and terminal diagnostics are localized behind `ReviewerPolicy`.
  Session/model/tool legacy event planning, model-router and model-attempt
  error event planning, plus tool-result runtime event planning for findings,
  artifacts, denials, completions, and search batches is also policy-owned, as
  is runtime lifecycle event planning for session start/finish, model
  start/completion, and tool-batch start events. Transcript append item shape is
  now policy-owned for assistant text, assistant tool calls, and tool results.
  Session model/token, latency, retry, and cost accounting lives behind
  `SessionModelAccounting`. Legacy/runtime event delivery lives behind
  `RuntimeEventDispatcher`. Per-result tool side-effect ordering lives behind
  `ToolResultEffectProcessor`. Session completion/cancellation/failure
  transitions live behind `SessionFlow`. `ModelTurnRunner` owns model
  retry/await timing and model start/completion event emission.
  `ToolBatchRunner` owns guarded tool-batch scheduling, policy-denial result
  construction, batch-start runtime event emission, denied-result metrics, and
  model-call-order result merging. `SessionRunner` now owns only the
  per-session orchestration of model routing, turn flow, transcript append
  timing, tool-result side-effect handoff, cancellation windows, and terminal
  diagnostics.
- `ToolEngine` still owns built-in execution, cache orchestration, findings,
  artifacts, redaction, and metrics, although hard tool authorization has been
  moved behind its own module interface.
- Events and artifacts exist and now carry stronger host-facing context; event
  schema compatibility has a full variant-matrix JSONL fixture, a
  schema-versioned loader, and explicit v0-to-v1 migration reports backed by
  fixture history. Successful, denial, multi-snapshot, and cancellation public
  run event logs now round-trip through the public JSONL loader with context and
  migration metadata intact. Future schemas still need fixtures when introduced.
- Real-provider, multi-profile, BYOK, and broader host resource contracts still
  need more live-provider coverage. Schema-versioned local and aggregate canary
  evidence capture now exists, and scheduled credential/object-store
  configuration plus proof-bundle validation has preflight/status/proof gates,
  but scheduled credentialed provider/cloud canary jobs still need to publish
  current passing proof artifacts.

## Current Ranking

As of this pass, `muzen` is about a 9.98/10 architecture for an internal review
runtime and about 9.98/10 as a reusable execution kernel. The score should not
move to 10/10 until every criterion below has direct current evidence, not
merely a reasonable design direction.

| Criterion | Current Rating | Evidence | Gap To 10/10 |
| --- | ---: | --- | --- |
| Deep host-facing module interface | 9.98 | Public `Run`, `RunBuilder`, `RunSpec`, `ReviewRunLimits`, `ReviewSessionSpec`, `ReviewModel`, `ReviewToolRegistry`, `ReviewEventSink`, `RunReport`, `ReviewRunSummary`, public model/tool/event/artifact/snapshot adapters, host-facing snapshot storage/read helpers, host-facing redacted/raw artifact workflow facade through `RunReport::redacted_artifacts`, `RunReport::raw_artifacts`, and `ReviewArtifacts`, host-facing JSON-RPC provider read-only and network-read tool registration/grants, artifact-id/object-ref string accessors, artifact bundle value constructors, host-facing review-event JSONL adapter, explicit `runtime_events`, `model_adapters`, `tool_adapters`, `capabilities`, `metrics`, `ids`, `artifacts`, `paths`, `storage`, `canaries`, and `runtime` modules for advanced compatibility instead of root raw-event/model/tool/provider/helper/capability/metrics/summary/id/path/storage/runtime/canary re-exports; raw `RuntimeLimits` is now behind `ReviewRunLimits::from_runtime_limits`; `muzen-runner stdio` exposes the kernel through a stable SDK protocol with implemented model/tool callbacks and event streaming; `muzen canary-preflight`, `muzen canary-workflow-provenance`, `muzen canary-publish`, `muzen canary-manifest`, `muzen canary-verify`, `muzen canary-status`, and `muzen canary-proof` expose canary configuration, scheduled workflow provenance, publication, publication provenance, aggregate proof, per-evidence status summaries, status gates, and final scheduled proof-bundle validation to automation | Remaining local facade work is mostly advanced compatibility and migration tests that intentionally instantiate low-level ids/objects for schema, corruption, and forgery proof |
| Immutable evidence | 9.98 | Snapshot ids, content hashes, captured text bytes, public `SnapshotReader`, `SnapshotReader::read_text_path`, public `SnapshotManifest`, host-facing memory/content-addressed/remote snapshot storage helpers, memory, content-addressed directory, remote object-store backing stores, HTTP remote object-store canary adapter, scheduled canary workflow scaffold with persisted schema-versioned `workflow.json`, `preflight.json`, `publication.json`, publish-owned `status.json`, and final `proof.json`, public snapshot storage validation/cleanup reports, schema-versioned `RemoteObjectStoreCanaryEvidence`, aggregate `CanaryEvidenceManifest`, structured `CanaryEvidenceStatusReport` with per-target remote object-store status summaries, structured `CanaryProofReport` validating child evidence, scheduled workflow provenance, expected workflow/job/repository/ref identity, exact run URL, per-file proof byte counts/BLAKE3 digests, preflight config, proof-artifact freshness against the manifest, explicit workflow artifact retention, snapshot remote-client put/read/remove/read-after-remove canary proof, artifact retention/object-store contracts including remote artifact object refs, memory-envelope, content-addressed, and remote-object public tests, stale/missing backing-object lifecycle proof, mutation-after-capture read/search tests | The scheduled workflow must publish a current passing proof bundle from a production object-store endpoint |
| Capability security | 9.75 | `ToolAuthorizer`, effects denial tests, max-call denial tests, artifact access policy with per-artifact scopes, model-visible output policy, tool input policy, runtime authority policy, provider and provider-resource allowlist policies, raw export denial, `ToolCallDenied` events, JSON-RPC authority/provider/resource denial-before-transport tests, public in-process host-tool provider-resource allow/deny tests, public JSON-RPC provider-resource allow/deny tests through `Run`, public JSON-RPC network-read allow/deny tests through `Run` proving missing runtime network authority denies before transport, scheduled canary workflow scoped to read-only repository permissions | Broader real-provider contract gates are still incomplete |
| Policy locality | 9.95 | `ReviewerPolicy` owns exposure, initial transcript construction, transcript compaction, transcript append item shape, evidence gate, evidence and session-budget tool-batch planning, planned batch counts, denial reasons, legacy session/model/tool/error event planning, lifecycle and tool-result runtime event planning, terminal tracking, terminal diagnostics, session state, retry choice; `SessionModelAccounting` owns model/token/cost accounting outside the loop; `RuntimeEventDispatcher` owns legacy/runtime event delivery; `ModelTurnRunner` owns model retry/await timing and model start/completion event emission; `ToolBatchRunner` owns guarded tool-batch scheduling, denial result construction, batch-start event emission, denied-result metrics, and result merge ordering; `ToolResultEffectProcessor` owns per-result tool side-effect ordering; `SessionFlow` owns session completion/cancellation/failure transitions; `SessionRunner` owns the per-session async loop behind a narrow `run_scope` interface while `JobRuntime` owns only scheduling and report aggregation | The remaining locality risk is smaller and specific: `SessionRunner` still owns transcript append timing and the high-level handoff between model turns, tool batches, and tool-result effects. This is appropriate orchestration today, but a future transcript/turn coordinator would be needed if those rules grow |
| Provider-neutral tool execution | 9.82 | `ToolProvider` trait, built-in/in-process/JSON-RPC providers, shared provider-output policy, provider metrics, provider/resource allowlists, host-facing provider-resource scoped custom-tool registration, host-facing JSON-RPC read-only and network-read provider registration through `ReviewToolRegistry` named registration values, provider/resource/effect grants through `ReviewSessionSpec`, provider resources propagated through `ReviewToolContext` and `JsonRpcToolRequest`, JSON-RPC artifact/output/authority/resource/cancellation limit tests, in-process and JSON-RPC provider-resource allow/deny public facade tests, public JSON-RPC network-read allow/deny facade tests, public HTTP JSON-RPC wire-envelope proof through `Run::builder`, queued/deduped search cancellation proof, post-tool/pre-transcript cancellation guard | Broader external-provider contract runs still need stronger coverage |
| Provider-neutral model routing | 9.7 | `ModelRouter`, per-profile/client routing, `ModelApiProtocol` profile selection, Chat Completions and Responses clients, shared Chat/Responses tool exposure conversion, shared alias-table replay and parsing proof, SDK runner `model.complete` callback adapter, public `reviewer::canaries` two-protocol `OpenAiProviderCanaryConfig` / `ModelProviderCanaryReport` canary contract, schema-versioned `ModelProviderCanaryEvidence` with required-protocol validation and gate failures, aggregate `CanaryEvidenceManifest`, status-report required/reported/passed protocol summary, publication-report live-vs-reused provider evidence source, proof-report rejection of reused provider evidence, freshness-gated CLI `canary-preflight` configuration proof, freshness-gated CLI `canary-publish` evidence publication, freshness-gated CLI `canary-manifest` composition/gating, freshness-gated CLI `canary-verify` published-manifest proof, freshness-gated CLI `canary-proof` scheduled-bundle proof, scheduled canary workflow scaffold, `bench-concurrent --run-provider-canaries`, `bench-concurrent --provider-canary-report`, safe skipped-status proof for disabled or missing credentials, durable canary evidence JSON roundtrip proof, per-provider/profile/key/session limiter buckets, in-flight model cancellation proof | The scheduled workflow must actually run with credentials and publish a passing proof bundle; broader provider compatibility gates are incomplete |
| Stable observability | 9.92 | `ReviewEventSink`, `ReviewEventRecord`, `ReviewEvent`, host-facing review-event JSONL export/load with schema-version validation, SDK runner `event.review`, `event.runtime`, `run.finished`, and `run.failed` notifications, `RuntimeEvent`, camelCase runtime payload JSON, `RuntimeEventContext`, `RuntimeEventDispatcher`, in-memory and bounded runtime sinks, runtime JSONL export/load with schema-version validation, migration reports, v0 contextless and v1 full-variant JSONL fixtures, contextless legacy event-log migration, policy-owned legacy session/model/tool/error event payloads, `ToolCallDenied` events, public facade review-event assertions for successful/denial/multi-snapshot/cancellation runs, and happy-path review-event JSONL roundtrip proof | Future schema versions must add fixtures when introduced |
| Artifact/evidence retrieval | 9.98 | Redacted/raw artifact views, host-facing redacted-all and redacted-scoped artifact workflow facade, string artifact-id/object-ref accessors for evidence/export/persistence views, scoped artifact export, bundle export, finding evidence traversal, raw export gated by `CapabilitySet` authority, snapshot storage object lifecycle reports including remote snapshot objects, public artifact bundle validation/cleanup reports, artifact retention count/byte envelope, `ArtifactObjectStore` persistence/cleanup contract, `ArtifactObjectReader` validation contract, serializable persistence manifests, in-memory, local filesystem, remote-URI artifact object-store adapters, HTTP remote object-store canary adapter, artifact remote-client put/read/remove/read-after-remove canary proof, aggregate `CanaryEvidenceManifest`, structured manifest status report with snapshot/artifact target summaries, publication provenance for the object-store driver, `CanaryProofReport` validation that snapshot/artifact child files match the manifest and use HTTP base URIs plus per-file proof digests for the scheduled bundle, scheduled canary workflow scaffold with persisted preflight, publication, status, and proof artifacts, stale/missing object-store proof, stale/missing bundle-object proof, forged remote URI and bundle-path denial proof | Scheduled production object-store canary runs must publish current passing proof bundles |
| Testability through interfaces | 9.99 | Public facade tests exist, including host-facing `ReviewSessionSpec` and `ReviewRunLimits` run construction, host-facing snapshot storage/read helpers, host-facing `ReviewArtifacts` workflow proof for export, scoped evidence, persistence, validation, cleanup, and local object paths without low-level ids, host-facing redacted artifact export policy construction and artifact-id accessors, host-facing `ReviewModel` adapters, host-facing `ReviewToolRegistry` custom, provider-resource-scoped custom, JSON-RPC read-only/network-read provider registration, and HTTP JSON-RPC transport execution, SDK runner callback proof through `interactive_stdio_runs_model_and_tool_callbacks`, runner handshake/schema fixtures, host-facing `ReviewEventSink` observation and review-event JSONL roundtrip, host-facing `ReviewRunSummary` status/snapshot-count proof, memory/content-addressed/remote snapshot storage lifecycle, public remote object-store canary proof, public aggregate canary evidence manifest proof, public canary status-report and evidence-summary proof, CLI canary preflight proof with versioned config summary, CLI canary workflow-provenance generation proof, CLI canary publication proof for both passing and manifest-gate-failing bundles, CLI publication provenance proof for reused and live-provider modes, CLI aggregate canary manifest composition proof, CLI published-manifest verification proof, CLI published-manifest status proof, CLI proof-bundle acceptance for live HTTP evidence with expected workflow/source identity and evidence file digests recorded in `proof.json`, CLI proof-bundle rejection for reused provider evidence, reused-provider preflight shape, preflight config/evidence mismatch, stale preflight proof metadata, missing workflow provenance, manual workflow-dispatch provenance, wrong workflow/job/repository/ref provenance, and wrong workflow run URL, canary freshness policy proof for stale/future evidence, artifact bundle lifecycle, artifact retention envelopes, artifact object-store persistence, validation, and cleanup after manifest JSON roundtrip, fixture-backed runtime event-log migration through `reviewer::runtime_events`, bounded raw event sink proof through `reviewer::runtime_events`, runtime provider proof through `reviewer::tool_adapters`, capability and cache fixture construction through `reviewer::capabilities` / `reviewer::metrics`, explicit public id/artifact/runtime contract modules in advanced facade tests, successful/denial/multi-snapshot/cancellation review-event proof, public host-resource allow/deny proof, public JSON-RPC provider-resource allow/deny proof, and public JSON-RPC network-read allow/deny proof; focused policy/authorization/validation/model-protocol/canary/accounting/dispatch/effects/flow/model-turn/tool-batch tests exist | Some tests still instantiate private runtime/tool modules directly |
| Migration without losing proof | 9.99 | Rust tests, clippy, release build, sync runtime deletion gates, focused in-flight, queued/deduped, post-tool/pre-transcript cancellation proof, public snapshot and artifact bundle lifecycle proof, remote snapshot object-store proof, artifact retention and object-store persistence/validation/cleanup proof, schema-versioned remote object-store canary evidence export/load proof, aggregate canary evidence manifest export/load, structured canary status-report proof, freshness policy, CLI publication preflight proof, CLI workflow-provenance generation proof, CLI publication proof, CLI composition proof, CLI published-manifest verification proof, CLI published-manifest status proof, CLI final proof-bundle validation with expected workflow/source identity and per-file digests, scheduled canary workflow scaffold with preflight/status/proof artifacts, fixture-backed runtime event-log schema migration proof, runner handshake/schema fixture drift proof, SDK runner callback proof, public runtime-path event-log roundtrip proof, host-facing review-event JSONL roundtrip proof, public HTTP JSON-RPC provider wire proof, Chat/Responses model protocol contract proof, opt-in Chat/Responses real-provider canary contract proof, focused model-turn retry/event proof, focused guarded tool-batch denial/order proof, and schema-versioned provider canary evidence export/load proof | Credentialed benchmark/canary evidence and production object-store proof bundles are not proven by a completed scheduled run yet |

## 10/10 Criteria

A 10/10 `muzen` architecture must satisfy these criteria.

1. Deep host-facing module interface

   Hosts should need to understand one stable reviewer module interface:
   construct a run, provide snapshots, sessions, model routing, tool registry,
   artifact storage, event sink, and cancellation, then receive a report.

2. Immutable evidence

   Every model-visible file, diff, search result, and finding evidence item
   must trace to a stable snapshot id, artifact id, content hash, and scope.
   Worktree mutation during a run must not silently change evidence.

3. Capability security

   `CapabilitySet` must be the hard authorization module. Prompt shaping may
   expose fewer tools, but cannot make an ungranted capability possible.

4. Policy locality

   Reviewer policy must live behind a module interface separate from runtime
   orchestration. Changes to evidence readiness, terminal rules, exposure
   order, retry rules, and model-visible compaction should be local.

5. Provider-neutral tool execution

   Built-in, trusted in-process custom, and later out-of-process tools should
   execute behind one `ToolProvider` seam with timeouts, limits, validation,
   redaction, metrics, and typed errors.

6. Provider-neutral model routing

   Sessions should resolve model/profile/API-key through `ModelRouter`, with
   per-provider, per-profile, per-key, and per-session limits and metrics.

7. Stable observability

   Hosts should consume bounded `RuntimeEvent` streams and metrics without
   inspecting internal state. Events must have run ids, sequence numbers,
   timestamps, and optional snapshot/session/turn/tool ids.

8. Artifact and evidence retrieval

   Reports should link findings to evidence refs and artifact refs. Hosts must
   be able to retrieve redacted artifacts by default, and raw artifacts only
   when policy grants raw export.

9. Testability through interfaces

   The interface is the test surface. Public behavior should be testable
   without reaching into private runtime modules.

10. Migration without losing proof

   Existing concurrency, dedupe, scope, redaction, and sync-deletion proof
   gates must survive every phase.

## Target Module Shape

The target architecture is a small set of deep modules.

```text
muzen::reviewer
  Run
  RunSpec
  ReviewRunLimits
  RunReport
  ReviewRunSummary
  RunHandle
  RunBuilder
  Cancellation

  Snapshot
  SnapshotSpec
  SnapshotSpec::with_memory_storage_limit
  SnapshotSpec::with_content_addressed_storage
  SnapshotHandle
  SnapshotManifest
  SnapshotReader
  SnapshotReader::read_text_path

  ReviewSessionSpec
  ids::SnapshotId
  ids::SessionId
  ids::ToolCallId
  ids::ToolId
  artifacts::ArtifactId
  artifacts::ArtifactView
  paths::RepoPath
  storage::SnapshotStoragePolicy
  runtime::RuntimeResult
  runtime::RuntimeError
  capabilities::CapabilitySet
  capabilities::ToolGrant
  capabilities::FsScope
  capabilities::ScopeKey

  ReviewerPolicy
  ToolExposurePolicy
  EvidencePolicy
  ToolBatchPolicy
  RetryPolicy
  TranscriptPolicy
  SessionTerminalDiagnosticPolicy

  ReviewModel
  ReviewModelRequest
  ReviewTranscriptItem
  ReviewModelTurn
  ReviewToolCall
  model_adapters::ModelRouter
  ModelProfile
  model_adapters::ModelMetricsSnapshot

  ReviewToolRegistry
  ReviewToolHandler
  ReviewToolContext
  ReviewToolOutput
  ReviewToolArtifact
  tool_adapters::ToolMetricsSnapshot

  ReviewEventSink
  ReviewEventRecord
  ReviewEvent
  ReviewEventJsonlManifest
  ReviewEventJsonlLoad

  tool_adapters::ArtifactStore
  ArtifactReader
  ArtifactExporter
  ArtifactExportPolicy::redacted_all
  ArtifactExportPolicy::redacted_artifacts
  EvidenceArtifactView::artifact_id
  EvidenceIndex

  runtime_events::EventSink
  runtime_events::RuntimeEvent
  RuntimeMetrics
```

Internal packages can remain small, but the public seam should stay narrow:

```text
reviewer facade
  -> runtime orchestration
  -> snapshot implementation
  -> policy implementation
  -> model adapters
  -> tool engine/providers
  -> artifact/event adapters
```

The CLI becomes only an adapter:

```text
CLI ReviewRunJobV1 JSON
  -> ReviewRunJobAdapter
  -> reviewer::RunSpec
  -> reviewer::Run
  -> JSONL events / exit code
```

## Target Seams

### Run Seam

The `Run` module owns orchestration, budgets, cancellation, final report, and
event sequencing.

Interface responsibilities:

- Accept `RunSpec`, `ModelRouter`, `ToolRegistry`, `ArtifactStore`, and
  `EventSink`.
- Build or attach snapshots.
- Run sessions with bounded concurrency.
- Emit events in stable order.
- Return `RunReport` with findings, tool metrics, model metrics, artifacts,
  evidence, and terminal diagnostics.

Implementation responsibilities:

- Tokio task orchestration.
- Active session semaphore.
- Cancellation fanout.
- Report aggregation.
- Runtime-level budget enforcement.

Deletion test:

Deleting `Run` should force orchestration, cancellation, event ordering,
metrics aggregation, and final report assembly to reappear across hosts. That
means it is deep.

### Snapshot Seam

The `Snapshot` module owns immutable repo evidence, manifest construction,
read/search candidates, and per-snapshot caches.

Interface responsibilities:

- Build or load a `Snapshot` from a materialized repo.
- Return stable `SnapshotId`.
- Provide `SnapshotManifest`.
- Provide bounded reads by `RepoPath` or `FileId`.
- Provide search candidates for an `FsScope`.
- Provide content hashes and artifact provenance.

Implementation options:

- Strongest option: materialize content-addressed file blobs at snapshot build
  time for all readable candidate files under policy.
- Lower-cost option: record size, mtime, inode/device where available, and
  content hash on first read; reject later reads if identity changes.
- For 10/10 evidence integrity, prefer content-addressed blobs for changed
  files and lazily verified reads for broad repo search.

Current implementation:

- Readable text candidates under the path policy are captured into
  snapshot-owned bytes during manifest construction.
- `SnapshotStoragePolicy` currently supports memory-backed, content-addressed
  directory-backed, and remote-object-backed capture with a configurable
  `max_captured_text_bytes` envelope.
- `RepoSnapshot` includes `storage_policy_hash`, storage policy metadata,
  skipped-capture counts, and skipped-capture bytes; the storage policy hash is
  part of the snapshot id.
- Content-addressed directory capture writes blobs under a hash-derived path and
  keeps only the content reference in the snapshot manifest. Remote-object
  capture writes blobs through a host-provided `RemoteSnapshotObjectStore` under
  hash-derived URIs and keeps pathless URI refs in lifecycle reports.
- `read_file`, `read_head_file`, `list_imports`, and `search_text` read from
  those captured bytes through `RepoSnapshot::read_bounded`.
- `RunReport::snapshot_reader`, `RunReport::snapshot_readers`, and
  `RunReport::snapshot_manifests` expose captured snapshot evidence through the
  public reviewer facade.
- `SnapshotManifest` includes `snapshot_id`, `manifest_hash`,
  `path_policy_hash`, `storage_policy_hash`, storage policy, file counts,
  captured text byte counts, skipped-capture counts, file metadata with
  `SnapshotCaptureStatus`, and changed-file summaries.
- `SnapshotReader::read_text` returns captured UTF-8 content by `RepoPath`
  together with snapshot id, content hash, byte count, and truncation state.
- `RepoSnapshot::read_bounded` validates the stored content hash before
  returning bytes, including content loaded back from the content-addressed
  directory store or remote object store.
- `SnapshotReader::read_text` reports `snapshot_capture_bytes` when a host
  attempts to read a text file skipped by the configured memory envelope.
- Mutating the worktree after snapshot construction no longer changes
  model-visible file or search evidence for captured text candidates.

Hard invariant:

The same `SnapshotId`, `RepoPath`, and content hash must always produce the
same model-visible bytes.

### Capability Seam

The `CapabilitySet` module owns hard authorization.

Interface responsibilities:

- Filesystem scope.
- Tool grants keyed by `ToolId`.
- Per-tool max calls.
- Tool effects allowed.
- Tool input constraints.
- Model-visible output policy.
- Artifact access policy for redacted/raw reads, writes, and optional
  per-artifact read scopes.
- Runtime authority policy for network read, host read, scratch read/write,
  and external side effects.
- Optional runtime provider and provider-resource allowlists.

Implementation responsibilities:

- Enforce grants before execution.
- Count per-tool calls at the capability layer.
- Deny effects that exceed the grant.
- Apply output visibility rules before model exposure.
- Deny oversized tool arguments before JSON parsing.
- Deny provider artifact writes when artifact write authority is absent.
- Deny network, host, scratch, and external side-effect tool effects when
  runtime authority is absent, even if the per-tool grant is broad.
- Deny provider execution when the tool definition's provider id is outside
  the runtime provider allowlist.
- Deny provider execution when the tool definition's provider-local resources
  are outside the runtime provider-resource allowlist.
- Gate raw artifact export through a capability-constructed export policy.
- Apply per-artifact read scopes uniformly to finding evidence traversal,
  artifact export, and artifact bundle export.
- Emit typed errors for denials.

Hard rule:

`ToolExposurePolicy` may only hide tools. It may not grant tools.

Current implementation:

- `CapabilitySet` has `artifact_access`, `model_output`, `tool_input`, and
  `runtime_authority` policies in addition to filesystem scope and tool grants.
- `ToolAuthorizer` checks tool grants, effects, max calls, global artifact
  read/write authority, runtime provider scope, runtime provider-resource
  scope, and global runtime authority before provider execution.
- Tool invocation validation rejects arguments over
  `ToolInputPolicy::max_argument_bytes` before parsing.
- `ReviewerPolicy::compact_tool_result` applies `ModelOutputPolicy` before any
  tool result becomes model-visible.
- `ArtifactExportPolicy::raw` can only be constructed from a capability set
  with raw artifact read authority; default review capabilities can construct
  redacted export policy but not raw export policy.
- `ArtifactAccessPolicy::scoped_to_artifacts` carries an optional artifact-id
  allowlist into `ArtifactExportPolicy`; public report APIs apply it to
  finding evidence artifacts, artifact manifests, and bundle output.
- Runtime authority tests prove network, host, scratch, and external side
  effects are denied globally when not authorized.
- Provider-scope tests prove provider ids outside the runtime allowlist are
  denied globally when not authorized.
- Provider-resource tests prove provider-local resources outside the runtime
  resource allowlist are denied globally when not authorized.
- A JSON-RPC provider test proves an external network-read tool is denied before
  transport invocation when runtime network authority is absent.
- A JSON-RPC provider test proves an external provider outside the runtime
  allowlist is denied before transport invocation.
- JSON-RPC provider-resource tests prove an out-of-scope resource is denied
  before transport invocation and a matching resource is sent to the external
  transport contract when allowed.
- Runtime emits `RuntimeEvent::ToolCallDenied` for policy denials, including
  denied tool grants, path denials, and capability call-limit denials.

### Reviewer Policy Seam

The reviewer policy module owns review-specific behavior that is currently
spread across runtime and model code.

Interface responsibilities:

- Decide tools visible to the model this turn.
- Decide whether evidence is sufficient for terminal tools.
- Decide whether a terminal result completes a session.
- Compact tool results for model-visible transcript.
- Apply model-visible output policy from `CapabilitySet`.
- Decide retry eligibility and backoff.
- Produce system/user prompt content from `SessionScope` and snapshot summary.

Implementation responsibilities:

- Evidence readiness state.
- Terminal rules such as `finish` must be alone.
- Read-diff/read-file/search prerequisites.
- Prompt schema progression.
- Model error retry policy.

Benefit:

Changing reviewer behavior no longer requires editing runtime orchestration or
provider adapters.

### Model Router Seam

The `ModelRouter` module owns provider/profile/API-key selection and limits.

Interface responsibilities:

- Resolve a `ModelClient` for each `SessionScope`.
- Isolate concurrency and budget by provider, profile, API-key reference, and
  session.
- Provide model metrics including wait time, latency, token usage, retries,
  errors, and cost estimates when available.

Implementation responsibilities:

- OpenAI-compatible Chat Completions adapter.
- OpenAI Responses adapter.
- Provider-safe tool alias compilation.
- Credential lookup through host-supplied adapters instead of direct env reads.

Current implementation:

- `ModelApiProtocol` lets each model profile select Chat Completions or
  Responses while preserving a Chat Completions default for older profile JSON.
- `ProfileModelRouter` constructs either `OpenAiChatCompletionsClient` or
  `OpenAiResponsesClient` from the same profile, limiter, registry, reviewer
  policy, and credential resolver inputs.
- `openai_tools_for_protocol` converts the single `ReviewerPolicy` tool
  exposure result into Chat or Responses wire shape, so tool exposure does not
  fork by provider protocol.
- Chat and Responses request builders replay previous assistant tool calls
  through `ToolAliasTable`, so provider-visible aliases are used when the
  transcript is sent back to the model.
- Chat and Responses response parsers map provider-visible function names back
  through the same alias table before returning internal `ToolId`s to the
  runtime.
- Focused model-protocol tests prove Chat and Responses parse tool calls
  through the same alias table, replay prior tool calls with model aliases, and
  expose the same tool schemas under protocol-specific wire shapes.

Hard invariant:

Unrelated API keys must not share a limiter or budget bucket unless the host
explicitly configures them to.

### Tool Registry And Provider Seam

`ToolRegistry` owns definitions. `ToolProvider` owns execution.

Interface responsibilities:

- Register built-in and custom tools.
- Assign stable `ToolId`.
- Assign provider-visible aliases through `ToolAliasTable`.
- Reject alias collisions.
- Expose schemas by provider adapter.
- Route calls to a `ToolProvider`.

Provider responsibilities:

- Validate input.
- Enforce timeout.
- Enforce bounded concurrency.
- Enforce output byte limits.
- Respect artifact write authority before storing returned artifacts.
- Redact output before model visibility.
- Contain panics where possible for trusted in-process tools.
- Return typed errors and metrics.

Implementation sequence:

- First, wrap existing built-ins in a built-in provider.
- Keep in-process custom tools as trusted providers.
- Later, add JSON-RPC and MCP-style out-of-process providers.

Current implementation:

- Built-in, trusted in-process, and JSON-RPC providers execute behind
  `ToolProvider`.
- In-process and JSON-RPC custom tools both return through
  `ToolEngine::provider_output_result`, which applies redaction, actual output
  byte accounting, artifact-write authority, artifact-size limits, output-size
  limits, artifact insertion, and typed errors.
- `RuntimeLimits::max_tool_output_bytes` and
  `RuntimeLimits::max_tool_artifact_bytes` bound external/custom provider
  output independently from provider-reported limit metadata.
- Tool definitions can declare provider-local resource ids. `CapabilitySet`
  runtime authority narrows those resources by provider, and JSON-RPC requests
  carry the declared resource list only after authorization succeeds.
- JSON-RPC contract tests prove undeclared artifact writes are denied by
  capability policy, oversized returned artifacts are rejected before storage,
  oversized returned data is rejected, out-of-scope provider resources are
  denied before transport, and allowed provider resources are included in the
  external request contract.

### Artifact And Evidence Seam

`ArtifactStore` owns content and retrieval. `EvidenceIndex` owns finding to
evidence to artifact traversal.

Interface responsibilities:

- Insert redacted/default artifacts.
- Optionally insert raw artifacts gated by policy.
- Retrieve by `ArtifactId`.
- List artifacts by run, snapshot, session, or finding.
- Export JSONL or directory bundles for audit.
- Link findings to evidence refs and content hashes.

Implementation responsibilities:

- In-memory store for tests and small runs.
- Persistent store adapter for Heimdaal integration.
- Raw/redacted view separation.
- Provenance metadata.

Current implementation:

- Public artifact readers return redacted/default views by default.
- Raw artifact export requires a `CapabilitySet` whose `artifact_access`
  permits raw reads.
- `ArtifactAccessPolicy` can carry an artifact-id allowlist; the public
  `ArtifactExportPolicy` applies that scope to finding evidence artifact
  traversal, export manifests, and bundle output.
- `ArtifactRetentionPolicy` can cap retained artifact count and retained bytes;
  `ArtifactExportPolicy::with_retention_policy` applies that envelope after
  capability/allowlist filtering and before manifest or bundle persistence.
- Retention failures return typed `LimitExceeded` errors for artifact count or
  artifact bytes, and rejected bundle exports do not create bundle files.
- Directory bundle export uses the same constructor-gated policy path as
  in-memory artifact export.
- Artifact export manifests, bundle manifests, validation reports, and cleanup
  reports carry the applied retention policy for audit.
- `ArtifactObjectStore` is the host-facing persistence contract for artifact
  objects. `RunReport::persist_artifacts` applies the same raw/redacted,
  artifact-id scope, and retention policy as in-memory manifests and bundle
  export before writing objects.
- `InMemoryArtifactObjectStore` gives tests and embedded hosts an in-process
  adapter. `LocalArtifactObjectStore` writes content-addressed files under a
  view/hash-derived directory and returns object refs with local paths.
  `RemoteArtifactObjectStore` writes content-addressed remote object URIs
  through a host-provided `RemoteArtifactObjectClient` and rejects forged URIs
  during validation reads.
- Artifact object adapters validate declared byte count and stable content hash
  before writing, so persistence cannot silently record mismatched metadata.
- `ArtifactPersistenceManifest` is serializable and can validate persisted
  objects through `ArtifactObjectReader`, reporting checked, missing, and stale
  objects without requiring private artifact-store state.
- Public facade tests serialize a local persistence manifest, reopen the local
  store through a new adapter instance, validate clean storage, then prove stale
  and missing object detection through the same public manifest interface.
- Public facade tests persist through a remote-URI adapter, validate a
  serialized remote persistence manifest, prove stale and missing remote object
  reports, reject forged remote object URIs, and reject `file://` bases for the
  remote adapter.
- `ArtifactBundleManifest::validate_storage` reports manifest presence,
  checked artifact count/bytes/objects, missing artifacts, and stale artifacts
  using the same stable content hash contract as the artifact store.
- `ArtifactBundleManifest::cleanup_storage` removes the canonical bundle
  manifest and safe relative artifact entries owned by the manifest, reports
  removed and missing objects, and prunes empty owned artifact directories.
- Forged bundle manifests that point outside the safe relative artifact path
  space or away from the canonical `root/manifest.json` fail with
  `RepoAccessDenied` before cleanup mutates storage.
- Finding evidence traversal uses the same redacted/raw export policy, so raw
  evidence cannot be requested through a separate bypassing interface.

### Event And Metrics Seam

`EventSink` owns host observability.

Interface responsibilities:

- Accept stable `RuntimeEvent` values.
- Define backpressure behavior.
- Preserve event sequence numbers per run.
- Expose flush/close behavior.

Implementation responsibilities:

- JSONL adapter for CLI.
- In-memory sink for tests.
- Host sink for Heimdaal.
- Dropped-event metrics when policy allows lossy backpressure.

Events should include:

- `RunStarted`
- `SnapshotManifestCompleted`
- `SessionStarted`
- `ModelCallStarted`
- `ModelCallCompleted`
- `ToolBatchStarted`
- `ToolCallStarted`
- `ToolCallCompleted`
- `ToolCallDenied`
- `SearchBatchCompleted`
- `ArtifactCreated`
- `FindingRecorded`
- `BudgetExceeded`
- `Cancelled`
- `RunFinished`

Current implementation:

- `RuntimeEventRecord` includes monotonic sequence, UTC timestamp,
  `RuntimeEventContext`, and the event payload.
- `RuntimeEventContext` carries optional run, snapshot, session, turn,
  tool-call, artifact, and finding ids.
- Public `Run` execution wraps event sinks with run and snapshot context, while
  runtime tool/search/artifact emissions attach session, turn, and tool-call
  context at the record seam.
- `InMemoryEventSink` and `BoundedInMemoryEventSink` both preserve context.
- Event JSONL export and load include schema-version validation, sequence,
  timestamp, context, and event payload.
- Runtime event payload fields serialize as camelCase, matching the rest of the
  public JSONL record shape.
- `fixtures/runtime-events-v1.jsonl` is a versioned compatibility fixture for
  every `RuntimeEvent` variant, checked through the public
  `export_event_records_jsonl` and `load_event_records_jsonl` adapters.
- `fixtures/runtime-events-v0-contextless.jsonl` is a legacy compatibility
  fixture for the contextless schema, checked through the public migration
  loader and its migration report.
- Public facade tests assert run id on all run records, snapshot id on run
  events, session/turn/tool-call context on tool events, denial-event context,
  JSONL context serialization, all-variant fixture compatibility, and
  unsupported schema-version rejection.

## Implementation Phases

### Phase 0: Freeze Current Proof

Purpose:

Lock down the working V1 so refactors cannot accidentally lower quality.

Work:

- Keep `cargo test -p muzen`.
- Keep `cargo clippy -p muzen --all-targets -- -D warnings`.
- Keep current synthetic compare benchmark gates.
- Add a short architecture smoke test that proves the CLI still routes only to
  concurrent runtime.

Exit gates:

- Existing tests pass.
- Clippy passes.
- Existing benchmark proof scripts still pass or are documented as requiring
  credentials/fixtures.

Migration risk:

The current repo has many untracked files. Avoid broad formatting or file
movement until the proof baseline is cleanly captured.

### Phase 1: Public Reviewer Facade

Purpose:

Make the public module interface real before deeper internal changes.

Work:

- Add public `Run`, `RunBuilder`, `RunSpec`, `RunReport`, `SnapshotSpec`,
  `ReviewSessionSpec`, `RunContext`, and `RunError`.
- Add `Run::execute` or `RunBuilder::run` as the public path.
- Move current private `run_job_concurrent_with_events` behavior behind a
  `ReviewRunJobAdapter`.
- Keep `ReviewRunJobV1` crate-private unless it is intentionally a public wire
  contract.
- Add in-memory `EventSink` and `ArtifactStore` adapters for public tests.
- Stop requiring tests to instantiate `JobRuntime` directly.

Exit gates:

- A public test can run one repo, one snapshot, one session, a mock model, and
  a custom tool using only `muzen::reviewer`.
- CLI run output is byte-compatible or intentionally versioned.
- Current internal runtime tests still pass.

Migration risks:

- Exposing too many current internal types would freeze accidental interfaces.
- Moving too much implementation in this phase can hide behavior changes.

### Phase 2: Immutable Snapshot

Purpose:

Make evidence stable and auditable.

Work:

- Introduce `SnapshotManifest` with candidate-set hash and policy digest.
- Keep eager content capture for readable files under the path policy.
- Add content-addressed artifact refs for changed files and diff inputs.
- Include content hash and snapshot id in every read/search artifact.
- Keep search operating on snapshot-backed content.
- Add tests that mutate the worktree after snapshot build.

Exit gates:

- `read_file` after worktree mutation returns original snapshot content for
  captured text candidates.
- Search after worktree mutation returns original snapshot content for captured
  text candidates.
- Finding evidence includes artifact id, snapshot id, content hash, and scope.

Migration risks:

- Eager content capture can increase memory and startup time.
- Lazy verification requires careful cache invalidation.

### Phase 3: Capability Contract Enforcement

Purpose:

Make policy real, not descriptive.

Current evidence:

- `packages/muzen/src/concurrent/tools/authorization.rs` owns hard
  per-invocation authorization before provider execution.
- `ToolGrant.max_calls` is enforced per session/tool.
- `ToolEffects` are enforced against registered tool declarations.
- Existing runtime tests and focused authorizer tests cover max-call denial,
  effects denial, and prompt exposure not overriding capability denial.

Work:

- Keep `ToolGrant.max_calls` and `ToolEffects` enforcement centralized in
  `ToolAuthorizer`.
- Add artifact read/write policy.
- Add model-visible output policy.
- Add input constraints for tools.
- Move `ToolMask` conversion into only the job adapter path.
- Add denial events and metrics.

Exit gates:

- A tool exceeding `max_calls` is denied with typed error.
- A tool with disallowed effects is denied before execution.
- A custom tool cannot write an artifact unless granted.
- Prompt exposure cannot override capability denial.

Migration risks:

- Existing tests may assume all review tools are broadly available.
- Denial behavior must preserve model tool-call ids to avoid provider protocol
  errors.

### Phase 4: Reviewer Policy Extraction

Purpose:

Give reviewer behavior a single module with a small interface.

Current evidence:

- `ReviewerPolicy` owns schema exposure progression, terminal-before-evidence
  denial, evidence readiness tracking, terminal result tracking, repeated
  terminal-denial failure policy, final session state resolution, transcript
  compaction, and model-error retry eligibility.
- Policy unit tests exercise these decisions without a runtime, repo, model
  client, network, or tool engine.

Work:

- Keep `ReviewerPolicy` as the policy facade.
- Add `ToolExposurePolicy`.
- Add `EvidencePolicy`.
- Add `TranscriptPolicy`.
- Add `RetryPolicy`.
- Decide whether separate sub-policies buy enough locality to justify the
  extra interfaces; avoid shallow pass-through modules.
- Move any remaining terminal/session decisions out of runtime as they become
  visible.

Exit gates:

- Unit tests can exercise policy without a runtime, repo, or model client.
- Runtime tests use fake policies to prove orchestration is policy-agnostic.
- Provider adapter tests prove schemas come from policy plus registry, not
  hardcoded tool order.

Migration risks:

- Splitting policy too finely can make the interface shallow. Keep one
  reviewer policy facade with internal helpers.

### Phase 5: ToolProvider V2

Purpose:

Make built-in and custom tools first-class providers behind one execution
seam.

Work:

- Add `ToolProvider` trait.
- Move existing built-in execution into `BuiltinReviewToolProvider`.
- Keep in-process custom tools behind `InProcessToolProvider`.
- Add provider id to `ToolMetricKey`.
- Track latency, queue wait, input bytes, output bytes, artifacts created,
  cache hits, dedupe waiters, cancellations, and timeouts.
- Add provider-level timeout and concurrency controls.
- Add provider alias table for model-visible function names.

Exit gates:

- Built-in and custom tools share metrics shape.
- Alias collisions are rejected.
- Provider timeout returns typed error and does not poison the run.
- Panic in trusted in-process tool is contained where possible.

Migration risks:

- Moving built-ins behind a provider can accidentally weaken path/capability
  checks. Capability enforcement must stay outside provider execution.

### Phase 6: ModelRouter And BYOK Isolation

Purpose:

Make model routing safe for many sessions, profiles, and credentials.

Work:

- Replace direct env credential lookup with a credential resolver adapter.
- Add provider/profile/API-key/session limiter buckets.
- Add model metrics for wait, latency, retries, token usage, and errors.
- Add per-session model profile selection.
- Add Responses-compatible adapter once Chat Completions parity is stable.

Exit gates:

- Two sessions using different credential refs do not share API-key limiter.
- Cancellation before or during model call prevents later requests.
- Retry behavior is policy-controlled and metric-visible.
- Real-provider cheap-model smoke passes for 10 sessions.

Migration risks:

- Credential handling bugs are high impact. Avoid logging credential refs with
  enough detail to expose secret identity unless policy allows it.

### Phase 7: Artifact, Evidence, And Event Public APIs

Purpose:

Make audit output and host integration first-class.

Work:

- Add public artifact retrieval and export APIs.
- Add evidence traversal from finding to artifact.
- Add raw/redacted export policies.
- Add bounded `EventSink` behavior.
- Add JSONL event contract tests.
- Add event sequence and causality checks.

Exit gates:

- Host can render a finding with evidence using only public APIs.
- Raw artifact export is denied unless explicitly granted.
- Event sequence is monotonic per run.
- Every artifact event points to a retrievable artifact.

Migration risks:

- Events can become too chatty and memory-heavy. Keep event payloads bounded
  and use artifact refs for large content.

### Phase 8: External Tool Providers

Purpose:

Support non-Rust and plugin-style extension without compromising the
capability wall.

Work:

- Add JSON-RPC provider adapter.
- Add MCP-style provider adapter if needed by hosts.
- Spawn directly, never through shell.
- Use minimal environment.
- No repo-root mount by default.
- Pass bounded content, artifact refs, or mediated repo APIs.
- Add payload, stderr, timeout, health, and shutdown policies.

Exit gates:

- Malformed JSON-RPC returns typed protocol error.
- Timeout kills or quarantines provider according to policy.
- Huge payload is rejected or truncated with typed error.
- Provider crash does not crash the run.
- Fake secrets in stderr/events are redacted.

Migration risks:

- External providers dramatically increase attack surface. This phase should
  not begin before capability and artifact policy are fully enforced.

### Phase 9: Multi-Snapshot And Multi-Repo Runs

Purpose:

Generalize the runtime beyond one materialized repo snapshot.

Work:

- Support multiple snapshots per run.
- Add per-session snapshot selection.
- Add per-snapshot metrics and caches.
- Add cross-snapshot artifact export.
- Add snapshot lifecycle events.

Exit gates:

- Two snapshots in one run cannot share unsafe cache results.
- Metrics distinguish snapshots.
- Artifact ids include enough provenance to disambiguate snapshots.

Migration risks:

- Multi-snapshot support before immutable snapshots will multiply evidence
  ambiguity. Do this late.

### Phase 10: Optimization After Measurement

Purpose:

Improve cost and latency without changing architecture.

Work:

- Reduce repeated tool schemas in prompts.
- Deduplicate evidence context across sessions.
- Add search microbatching if benchmarks justify it.
- Consider persistent trigram or content index only after measuring.
- Tune artifact storage backend.

Exit gates:

- Token ratio improves without lowering evidence quality.
- Search dedupe and scoped cache gates still pass.
- Memory remains within accepted envelope.

Migration risks:

- Premature indexing can add shallow modules. Only add an index behind the
  snapshot/search seam when measured workloads justify it.

## Proof Gates

Local verification after extracting `ToolAuthorizer`:

```bash
cargo fmt --check
cargo test -p muzen
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Result: all passed. The test suite currently reports 54 passing tests.

Local verification after adding snapshot-owned text bytes:

```bash
cargo fmt --check
cargo test -p muzen
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Result: all passed. The full test suite covers mutation-after-capture reads
and searches returning original snapshot evidence.

Local verification after exposing public snapshot readers/manifests:

```bash
cargo fmt --check
cargo test -p muzen
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Result: all passed. `public_reviewer_facade_runs_mock_review` now verifies
public `SnapshotManifest` metadata and `SnapshotReader::read_text` returning
captured original content after the worktree is mutated.

Local verification after moving terminal/evidence session policy behind
`ReviewerPolicy`:

```bash
cargo fmt --check
cargo test -p muzen
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Result: all passed. The test suite currently reports 56 passing tests,
including focused `session_policy_*` tests that run without runtime, repo,
model, network, or tool-engine setup.

### Required Every Phase

```bash
cargo fmt --check
cargo test -p muzen
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

### Public Interface Gates

- Public test uses only `muzen::reviewer` to run a review.
- Public test registers one custom tool and receives metrics for it.
- Public test retrieves artifacts by id.
- Public test consumes events through `EventSink`.
- Public test inspects `SnapshotManifest` and reads captured evidence through
  `SnapshotReader`.

### Snapshot Gates

- Mutating a file after snapshot build does not alter captured read evidence.
- Mutating a file after snapshot build does not alter captured search evidence.
- Search cache key includes snapshot id, scope key, tool id, tool/schema
  version, canonical args hash, redaction policy version, and contract version.
- Root and subtree sessions do not share unsafe search results.

### Capability Gates

- Unknown tool is denied.
- Registered but ungranted tool is denied.
- Tool exceeding `max_calls` is denied.
- Tool declaring disallowed effects is denied.
- Artifact read/write denial is enforced.
- Model-visible output policy is enforced.

### Policy Gates

- Terminal tools are unavailable until evidence policy allows them.
- `finish` must be alone.
- Prompt exposure never includes tools denied by capabilities.
- Retryable and non-retryable provider errors follow `RetryPolicy`.
- Policy tests run without repo, runtime, or network.
- Policy tests cover evidence readiness, terminal summaries, repeated
  terminal-denial failure, and final session state resolution.

### Tool Provider Gates

- Built-in and custom tools share `ToolProvider` execution path.
- Tool alias collision is rejected.
- Provider timeout returns typed error.
- Provider panic/crash does not crash run when policy allows continuation.
- Tool metrics include latency, queue wait, cache hits, dedupe waiters,
  input/output bytes, artifacts, cancellation, and timeout counts.

### Model Router Gates

- Sessions can route to distinct profiles.
- Sessions can route to distinct credential refs.
- Per-key limiter isolation is tested.
- In-flight model cancellation prevents tool execution and later model
  requests.
- Chat Completions and Responses adapters consume the same registry/alias
  table when Responses support is added.

### Event And Artifact Gates

- Event sequence numbers are monotonic per run.
- Every artifact event references a retrievable artifact.
- Every finding evidence ref resolves to an artifact.
- Redacted export is available by default.
- Raw export requires explicit policy.
- JSONL event compatibility fixtures are versioned.

### Real Runtime Gates

- 10-session cheap-model run has zero protocol-shape errors.
- 10-session run has no dropped tool calls and all tool call ids matched.
- 50-session run respects provider rate limits.
- Cancellation before session start, during model call, during JSON-RPC
  provider execution, during queued search, while waiting on a deduped search,
  and after a successful tool result but before transcript consumption has
  focused proof. Successful late results are not consumed into evidence,
  transcript, report tool counts, or tool-completed runtime events; provider
  cancellation error results remain observable.
- Existing 50-session rollout canary remains publishable.

## Test Plan

Unit tests:

- `ToolId` parsing and alias table compilation.
- `CapabilitySet` grants, max calls, effects, and output policy.
- `ReviewerPolicy` exposure, evidence readiness, terminal handling, and retry.
- `SnapshotManifest` hashing and stale-read detection.
- `ToolCacheKey` canonicalization.
- `ArtifactStore` redacted/raw retrieval.

Integration tests:

- Public reviewer facade happy path with mock model.
- Public reviewer facade with custom tool.
- CLI `ReviewRunJobV1` adapter parity.
- Worktree mutation after snapshot.
- Scoped search cache isolation.
- Cancellation at each lifecycle point.
- Event and artifact traversal from final report.

Contract tests:

- JSONL `RuntimeEvent` fixtures.
- `RunReport` fixtures.
- Tool result envelope fixtures.
- Artifact export manifest fixtures.
- Provider schema fixtures for Chat Completions and Responses.

Benchmark/proof tests:

- Existing synthetic compare benchmark.
- 50-session rollout shape.
- 100-session diagnostic stress.
- Token-ratio regression.
- Memory envelope regression.
- Real-provider cheap-model smoke where credentials are available.

## Migration Principles

- Keep the current concurrent runtime as the behavior oracle until the public
  facade has equivalent tests.
- Move one seam at a time; do not combine public API, snapshot immutability,
  and provider refactors in one PR.
- Prefer adapters over rewrites when bridging old `ReviewRunJobV1` to new
  `RunSpec`.
- Keep old CLI output compatible until a versioned contract says otherwise.
- Every new public interface needs a test that uses only that interface.
- Do not expose internal types only to make migration easier.
- Do not introduce a seam with only one future adapter unless it buys immediate
  locality or is required for public API stability.
- Treat security and evidence invariants as release blockers.

## Implementation Status

Updated: 2026-06-03

Implemented in this workspace:

- Phase 1 public reviewer facade:
  - `muzen::reviewer::Run`, `RunBuilder`, `RunSpec`, `SnapshotSpec`,
    `ChangeSpec`, `ReviewSessionSpec`, `ReviewModel`, `ReviewToolRegistry`,
    `ReviewEventSink`, `RunReport`, `Cancellation`, public
    model/tool/event/artifact seams.
  - CLI `ReviewRunJobV1` adapter now routes through the public run facade.
  - Public tests run a mock review and a custom tool through `muzen::reviewer`
    without constructing raw runtime session scopes, model clients, or custom
    tool handlers. Facade-level event assertions use `ReviewEventSink` instead
    of raw runtime event records, and host-facing review events round-trip
    through review-event JSONL without exposing raw runtime event payloads.
- Phase 2 immutable or stale-detected snapshot evidence:
  - Snapshot manifest records content hashes for readable text candidates.
  - `read_file` and `search_text` reject mutated worktree content with
    `ToolErrorCode::SnapshotStale`.
  - Search no longer silently skips snapshot-stale reads.
- Phase 3 capability enforcement:
  - `ToolGrant.max_calls` is enforced per session and tool.
  - `ToolEffects` are declared on tool definitions and enforced before
    execution.
  - Custom artifact writes require an artifact-write grant.
  - Prompt exposure is filtered by capabilities, so exposure can only hide
    tools, not grant them.
  - Denials are metric-visible and appear in public `ToolCallCompleted` events
    with typed error codes.
- Phase 4 reviewer policy extraction, first slice:
  - `ReviewerPolicy` owns initial prompt construction, tool exposure
    progression, evidence-before-terminal gating, model-visible tool result
    compaction, and retry eligibility.
  - Policy tests run without a runtime, repo, provider, or network.
- Phase 5 `ToolProvider` V2, first slice:
  - Built-in and trusted in-process custom tools now execute through a
    `ToolProvider` seam.
  - Tool definitions declare a `ToolProviderId`.
  - Tool definitions carry provider-visible aliases and alias collisions are
    rejected before model adapters see ambiguous function names.
  - Tool result envelopes, public runtime events, and tool metrics carry
    provider identity.
  - `ToolMetricKey` is provider-qualified while remaining JSON-map safe.
  - Provider execution is wrapped in a typed timeout and slow providers return
    `ToolErrorCode::Timeout` without poisoning the run.
  - Provider execution is bounded by
    `RuntimeLimits::max_tool_provider_concurrency_per_provider`.
  - Trusted in-process provider panics are contained and recorded as typed tool
    errors without crashing the run.
  - Tool metrics now include provider-level input bytes, output bytes,
    artifacts, latency totals/max latency, queue wait totals/max queue wait,
    timeouts, and cancellations.
  - `ConcurrentRunReport.provider_health` exposes provider-level healthy,
    degraded, and unhealthy states with call/error/timeout/cancellation and
    consecutive-error counters.
- Phase 6 model routing and BYOK isolation, first slice:
  - Model credential lookup is behind a `CredentialResolver` seam.
  - `EnvCredentialResolver` preserves existing env-ref behavior.
  - Unit tests prove the OpenAI client uses a supplied resolver without reading
    the environment.
  - `ModelLimiter` now has global and per-credential buckets so unrelated
    credential refs do not share the same per-key limiter.
  - `ModelLimiter` now also exposes provider, profile, credential, and session
    buckets, and the OpenAI-compatible adapter acquires those dimensions per
    call.
  - `ConcurrentRunReport.model_metrics` now exposes model calls, successes,
    errors, retries, latency totals/max latency, and token totals.
- Phase 6 model routing and Responses protocol contract, second slice:
  - `ModelApiProtocol` profile selection routes OpenAI-compatible profiles to
    Chat Completions or Responses clients.
  - `OpenAiResponsesClient` uses the same credential resolver, limiter,
    tool registry, reviewer policy, and model-turn interface as the Chat
    Completions client.
  - Chat and Responses request shaping consume one reviewer-policy tool
    exposure result and one alias table.
  - Chat and Responses parsers map provider-visible function names back to
    internal `ToolId`s before runtime consumption.
  - Focused tests prove alias parsing, alias replay, Responses credential
    resolution, and protocol-specific tool schema shape.
- Phase 7 public artifact/event API, first slice:
  - `RuntimeEventSink` is the runtime-level event seam re-exported as
    `muzen::reviewer::EventSink`.
  - `RunReport::export_artifacts` can export a redacted artifact manifest by
    default.
  - `RunReport::export_artifact_bundle` writes a durable redacted bundle with
    `manifest.json` and one file per artifact.
  - `ArtifactBundleManifest::validate_storage` and `cleanup_storage` now give
    hosts public lifecycle reports for exported bundles, including
    checked/removed objects, missing artifacts, stale artifacts, manifest
    presence, removed manifest state, and pruned empty artifact directories.
  - Bundle lifecycle rejects unsafe relative artifact paths and forged manifest
    paths before cleanup mutates storage.
  - Raw artifact export is available only through
    `ArtifactExportPolicy::raw(&CapabilitySet)` when raw artifact read
    authority is present, while default retrieval remains redacted.
  - `RunReport::findings` and `RunReport::finding_evidence_artifacts` expose
    public finding/evidence traversal without leaking internal wire structs.
  - Public runtime events now include session, model, tool batch, tool
    completion, search batch, artifact creation, finding recorded, and job
    lifecycle events.
  - Public tests prove artifact-created events resolve through the final
    report's artifact store.
  - `InMemoryEventSink` records monotonic sequence numbers and timestamps for
    public event records.
  - `RuntimeEventRecord` now carries `RuntimeEventContext` with run, snapshot,
    session, turn, tool-call, artifact, and finding ids where available.
  - `BoundedInMemoryEventSink` provides bounded storage and dropped-event
    accounting for hosts that need explicit backpressure behavior.
  - Bounded event sinks support explicit `DropNewest` and `DropOldest`
    backpressure policies.
  - Public event records can be exported as schema-versioned JSONL, with tests
    parsing the durable log and verifying dropped-event accounting.

Proof gates run after these changes:

```bash
cargo fmt --check
cargo test -p muzen
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after capability-policy hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 61 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after event-context hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 61 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after external-provider policy-contract hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 64 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after snapshot storage-policy hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 65 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after content-addressed snapshot backing store:

```bash
cargo fmt --check
cargo test -p muzen              # 66 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after event-schema fixture hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 67 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after event-matrix loader hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 67 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after runtime-authority hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 70 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after scoped-artifact authority hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 70 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after provider-scope authority hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 73 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after provider-resource authority hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 77 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after in-flight cancellation proof:

```bash
cargo fmt --check
cargo test -p muzen              # 79 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after queued/deduped search cancellation proof:

```bash
cargo fmt --check
cargo test -p muzen              # 81 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after post-tool/pre-transcript cancellation proof:

```bash
cargo fmt --check
cargo test -p muzen              # 82 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after snapshot storage lifecycle contract:

```bash
cargo fmt --check
cargo test -p muzen              # 83 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after event-log migration contract:

```bash
cargo fmt --check
cargo test -p muzen              # 84 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after successful runtime event-log roundtrip proof:

```bash
cargo fmt --check
cargo test -p muzen              # 84 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after denial and multi-snapshot event-log roundtrip proof:

```bash
cargo fmt --check
cargo test -p muzen              # 84 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Local verification after cancellation event-log roundtrip proof:

```bash
cargo fmt --check
cargo test -p muzen              # 85 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Latest local verification after legacy event-log fixture hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 85 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

10/10 implementation status:

- `ToolProvider` is a real production seam with built-in, trusted in-process,
  and external JSON-RPC adapters. The built-in adapter delegates to shared
  runtime repo/tool primitives on `ToolEngine`; external extensibility no
  longer depends on editing the runtime dispatch path.
- Provider-level concurrency controls, queue wait metrics, in-process panic
  containment, and provider health snapshots are in place.
- Model routing has provider/profile/session/key limiter buckets and latency
  metrics; model clients can now provide cost estimates and reports aggregate
  costed/unpriced calls plus estimated input/output/total micro-USD.
- Artifact export has redacted/default retrieval, explicit raw export, and
  persistent redacted/raw bundles.
- Event records have run/snapshot/session/turn/tool-call context, event
  backpressure has a bounded in-memory adapter, explicit drop policies, and
  JSONL export includes context.
- Event JSONL loading now returns a schema migration report and can migrate the
  legacy contextless event-log schema by deriving context from each event
  payload.
- External JSON-RPC tool providers are implemented behind the `ToolProvider`
  seam with HTTP and mockable transports, public registry APIs, shared
  redaction/artifact/output-limit handling, metrics, provider health, and
  focused policy-contract tests.
- Multi-snapshot and multi-repo runs are implemented as isolated run shards
  with per-session snapshot selection, per-snapshot metrics, snapshot lifecycle
  events, and aggregate artifact/finding/report export.
- Snapshot storage lifecycle is now a public `SnapshotReader` contract:
  validation reports checked, missing, and stale captured objects; cleanup
  removes only snapshot-owned content-addressed objects and prunes their empty
  hash-prefix directories.
- Optimization gates are measured in comparison reports and flag search-scan
  regressions, severe wall-time regressions, and speedup floors without making
  ordinary local tests flaky.
- Real-provider proof scaffolding is present as an opt-in smoke gate requiring
  `MUZEN_RUN_REAL_PROVIDER_SMOKE=1` and `OPENAI_API_KEY`; the credentialed
  smoke gate passed during this implementation pass.

## Rating Path

Original audited baseline:

- Internal Heimdaal V1 runtime: about 7/10.
- Reusable reviewer primitive: about 6/10.

After Phase 1:

- Public module interface becomes credible: 7/10 reusable primitive.

After Phases 2 and 3:

- Evidence integrity and capability security become strong: 8/10.

After Phases 4 through 7:

- Policy, provider execution, routing, artifacts, and events gain locality:
  9/10.

Implemented state before capability-policy hardening:

- Public reviewer facade, snapshot stale detection, capability grants,
  `ReviewerPolicy`, `ToolProvider`, credential resolution, bucketed model
  limiting, provider health, raw/redacted artifact export, public
  finding/evidence traversal, bounded event backpressure, and versioned JSONL
  events are implemented with proof gates passing: about 9/10.

Implemented state after capability-policy hardening:

- `CapabilitySet` now carries artifact access, model-visible output, and tool
  input policy; tool authorization checks artifact read/write authority;
  validation rejects oversized arguments; `ReviewerPolicy` enforces
  model-visible output policy; raw artifact export requires raw artifact read
  authority; and runtime emits first-class `ToolCallDenied` events. This moves
  capability security close to 9/10, but the whole architecture remains around
  8.7/10 internal and 8.3/10 reusable until host-facing event causality,
  backing stores, external-provider policy contracts, and benchmark/canary
  automation are complete.

Implemented state after runtime-authority hardening:

- `CapabilitySet` now carries `RuntimeAuthorityPolicy` for network read, host
  read, scratch read/write, and external side effects. `ToolAuthorizer` denies
  those effects globally before provider execution even when a per-tool grant is
  broad, and a JSON-RPC contract test proves network-read authority denial
  happens before external transport invocation. This moves capability security
  to about 9.2/10 and provider-neutral tool execution to about 8.7/10.
  Remaining capability work is provider-specific resource scopes and
  real-provider contract gates.

Implemented state after scoped-artifact authority hardening:

- `ArtifactAccessPolicy` now carries optional artifact-id read scopes, and
  `ArtifactExportPolicy` applies that scope across finding evidence traversal,
  artifact export manifests, and bundle output. Public facade tests prove scoped
  export and evidence traversal expose only the allowed artifact. This moves
  capability security to about 9.3/10 and artifact/evidence retrieval to about
  8.8/10. Remaining artifact work is persistent store lifecycle, remote store
  adapters, and broader provider/resource contract gates.

Implemented state after provider-scope authority hardening:

- `RuntimeAuthorityPolicy` now carries optional provider-id allowlists, and
  `ToolAuthorizer` denies provider execution before handler or transport
  invocation when the tool definition's provider id is outside the runtime
  scope. Focused authorizer tests and a JSON-RPC contract test prove provider
  denial happens before external transport invocation. This moves capability
  security to about 9.4/10 and provider-neutral tool execution to about 8.9/10.
  Remaining provider work is real-provider contract gates, resource-specific
  provider policy fixtures, and cancellation proof.

Implemented state after provider-resource authority hardening:

- `ProviderResourceId` and `ProviderResourceScope` now let tool definitions
  declare provider-local resources and let `RuntimeAuthorityPolicy` narrow
  those resources by provider. `ToolAuthorizer` denies out-of-scope provider
  resources before handler or transport invocation, and JSON-RPC requests carry
  the declared resource list only after authorization succeeds. Focused
  authorizer tests plus deny/allow JSON-RPC contract tests prove both the hard
  denial and the external request contract. This moved capability security to
  about 9.5/10 and provider-neutral tool execution to about 9.1/10. At that
  checkpoint, remaining provider work was real-provider contract gates,
  host-specific resource contracts beyond JSON-RPC, and broader cancellation
  proof.

Implemented state after host-resource scoped tool contract:

- `CustomToolContext` now carries the tool definition's provider-resource ids
  into in-process host tool handlers, matching the resource visibility already
  available to JSON-RPC transports. `ReviewToolContext` exposes the same
  resource ids through the public reviewer facade.
- `ReviewToolRegistry::register_scoped_read_only_tool` and
  `ReviewSessionSpec::grant_custom_read_only_tool_for_resources` let hosts
  register and grant read-only in-process tools scoped to specific provider
  resources without dropping to private runtime modules.
- Public facade tests now prove the allowed path passes the expected resource id
  to the host handler and the denied path blocks execution before the handler
  runs while emitting a `ToolCallDenied` event. This moves capability security
  to about 9.6/10 and provider-neutral tool execution to about 9.5/10. Remaining
  provider work is real external-provider contract gates and credentialed
  provider canary automation.

Latest local verification after host-resource scoped tool contract:

```bash
cargo fmt --check
cargo test -p muzen              # 100 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after in-flight cancellation proof:

- Runtime tests now prove cancellation during a model call records one model
  attempt, emits no model-completed or tool-batch runtime event, and finishes
  the session as cancelled. JSON-RPC provider tests now prove cancellation
  during external tool execution records a cancelled tool result, provider
  cancellation health, no successful tool count, and no second model request.
  This moves the internal runtime rating to about 9.6/10, reusable execution to
  about 9.2/10, provider-neutral tool execution to about 9.2/10, model routing
  to about 8.2/10, and migration proof to about 8.2/10. At that checkpoint,
  the remaining cancellation proof was queued search, deduped search, and
  result-before-transcript behavior.

Implemented state after queued/deduped search cancellation hardening:

- `SearchCoordinator` now observes cancellation while waiting for a search
  permit, and deduped cache waiters observe their own cancellation instead of
  inheriting the owner result after cancellation. Tests hold the search permit
  to prove queued search cancellation returns before capacity is released, and
  prove a deduped search waiter can cancel while the owner remains blocked.
  This moved the internal runtime rating to about 9.7/10, reusable execution to
  about 9.3/10, provider-neutral tool execution to about 9.3/10, and migration
  proof to about 8.4/10. The only cancellation gap left at that checkpoint was
  result-before-transcript consumption.

Implemented state after result-before-transcript cancellation hardening:

- `JobRuntime` now checks cancellation immediately after guarded tool
  batch execution and before evidence observation, terminal observation,
  runtime-event emission, report tool counts, or transcript appends consume a
  successful returned result. Cancelled provider error results still remain
  observable, preserving the JSON-RPC cancellation contract. A focused runtime
  test proves a tool that cancels the parent token immediately before returning a
  successful output records provider/tool metrics for execution, emits no
  `ToolCallCompleted` event, appends no tool result to the model-visible
  transcript, records no report tool call, and finishes the session as
  cancelled. This moves the internal runtime rating to about 9.8/10, reusable
  execution to about 9.4/10, provider-neutral tool execution to about 9.4/10,
  and migration proof to about 8.6/10. Remaining 10/10 gaps are no longer
  cancellation-path proof; they are real external-provider contract gates,
  public facade cleanup, store lifecycle, future event-schema migration proof,
  and automated canary/benchmark evidence.

Implemented state after snapshot storage lifecycle contract:

- `SnapshotReader` now exposes a host-facing lifecycle interface for captured
  snapshot storage. `validate_storage` reports every checked object and flags
  missing or stale backing objects; `cleanup_storage` removes only
  content-addressed files referenced by that snapshot and prunes empty
  hash-prefix directories. A public facade test proves a content-addressed
  snapshot validates cleanly, detects stale backing content as `SnapshotStale`,
  cleans up the owned object, leaves the store root intact, and reports the
  object missing after cleanup. This moves immutable evidence to about 9.7/10,
  artifact/evidence retrieval to about 8.9/10, migration proof to about 8.7/10,
  and reusable execution to about 9.5/10. At that checkpoint, remaining store
  work was retention policy, persistent artifact-store lifecycle, and
  remote/blob-store adapters.

Implemented state after remote snapshot object-store adapter:

- `SnapshotStoragePolicy` now supports remote-object-backed capture through a
  host-provided `SnapshotObjectStore`, and `SnapshotSpec::with_remote_object_storage`
  gives hosts a reviewer-owned constructor for that mode. `RemoteSnapshotObjectStore`
  writes captured text bytes to hash-derived remote URIs through a
  `RemoteSnapshotObjectClient`, and `InMemoryRemoteSnapshotObjectClient` gives
  tests and embedded hosts a deterministic remote adapter. `SnapshotReader`
  reads remote snapshot objects through the same hash-validation path as memory
  and local content-addressed storage, reports remote object URIs in validation
  and cleanup reports, detects stale and missing remote objects, deletes owned
  remote objects during cleanup, and denies forged remote URIs before reaching
  the client. Public facade tests prove remote snapshot capture survives
  worktree mutation, validates cleanly, detects stale content as `SnapshotStale`,
  restores, rejects forged URIs and `file://` bases, cleans up the owned remote
  object, and reports the object missing after cleanup. This moves immutable
  evidence to about 9.8/10. Remaining remote evidence work is production
  cloud-client canaries.

Latest local verification after remote snapshot object-store adapter:

```bash
cargo fmt --check
cargo test -p muzen              # 96 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after event-log migration contract:

- Runtime event JSONL loading now localizes schema compatibility behind the
  public loader interface. The load result includes a migration report with the
  current schema version, source schema-version counts, and migrated-record
  count. The loader still requires explicit context for current-schema records,
  but it can migrate a known legacy contextless event-log schema by deriving
  `RuntimeEventContext` from each `RuntimeEvent`. Public tests prove current
  fixture logs load with zero migrations, unsupported schemas still fail, and a
  legacy model-started event migrates with session/turn context intact. This
  moves stable observability to about 9.3/10 and migration proof to about
  8.8/10. Remaining observability work is broader runtime-path event coverage
  and a future multi-version fixture history as new schemas are introduced.

Implemented state after successful runtime event-log roundtrip proof:

- `public_reviewer_facade_runs_mock_review` now exports the actual public `Run`
  event stream, reloads it through `load_event_records_jsonl`, and asserts the
  loaded records exactly match the emitted `RuntimeEventRecord`s with zero
  migrations and current-schema source counts. This connects real runtime-path
  event emission to the same public compatibility interface used by fixtures and
  legacy migration tests. Stable observability moves to about 9.4/10 and
  testability through public interfaces to about 7.8/10. Remaining observability
  proof is cancellation, denial, and multi-snapshot runtime-path event-log
  roundtrips plus future multi-version fixture history.

Implemented state after denial and multi-snapshot event-log roundtrip proof:

- Public facade tests now round-trip denied-tool and multi-snapshot runtime event
  streams through the same public JSONL export/load path and assert exact record
  equality plus zero-migration current-schema metadata. This proves the
  compatibility interface preserves policy-denial context and per-snapshot event
  context outside the happy path. Stable observability moves to about 9.5/10,
  testability through public interfaces to about 7.9/10, and migration proof to
  about 8.9/10. Remaining observability proof is cancellation runtime-path
  event-log roundtrip plus future multi-version fixture history.

Implemented state after cancellation event-log roundtrip proof:

- `public_reviewer_facade_cancelled_run_event_log_roundtrips` now drives
  cancellation through the public `Run::execute_with_cancel` seam, proves the
  model-started and cancelled-session events carry run/session/turn context,
  proves no model-completed/tool-batch/tool-completed events are emitted after
  cancellation, and round-trips the emitted records through public JSONL
  export/load with zero migrations. Stable observability moves to about 9.6/10,
  testability through public interfaces to about 8/10, and migration proof to
  about 9/10. Remaining observability work is future multi-version fixture
  history when another schema is introduced.

Implemented state after legacy event-log fixture hardening:

- `fixtures/runtime-events-v0-contextless.jsonl` now freezes the legacy
  contextless event-log schema as a compatibility artifact rather than building
  it inline in the test. The public loader test migrates that fixture, verifies
  the source schema counts and migrated record count, and checks derived context
  for snapshot, session/turn, tool-call, artifact, and session-finished events.
  Stable observability moves to about 9.7/10, testability through public
  interfaces to about 8.1/10, migration proof to about 9.1/10, and reusable
  execution to about 9.6/10. Future schema versions must add fixtures when they
  are introduced.

Implemented state after artifact bundle lifecycle contract:

- `ArtifactBundleManifest` now exposes a host-facing lifecycle interface for
  exported artifact bundles. `validate_storage` checks the canonical manifest
  and every safe relative bundle object, reports checked objects, and flags
  missing or stale artifacts using the artifact-store stable content hash.
  `cleanup_storage` removes only the canonical `root/manifest.json` and
  manifest-owned safe relative artifact files, then prunes empty owned artifact
  directories. Public facade tests prove a bundle validates cleanly, detects
  stale artifact content, restores to valid, cleans up exported files, leaves a
  separate raw bundle intact, reports missing artifacts after cleanup, and
  denies forged relative artifact paths or forged manifest paths before
  mutation. This moves artifact/evidence retrieval to about 9.1/10,
  testability through interfaces to about 8.2/10, and migration proof to about
  9.2/10. At that checkpoint, remaining store work was retention policy,
  remote store adapters, and a host-owned persistent artifact-store contract.

Latest local verification after artifact bundle lifecycle hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 86 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after Responses model protocol contract:

- Model routing now has a protocol-selected OpenAI-compatible adapter path:
  `ModelApiProtocol` defaults existing profiles to Chat Completions and can
  select `OpenAiResponsesClient` for Responses. Both adapters use the same
  credential resolver, provider/profile/key/session limiter buckets, reviewer
  policy, tool registry, model-turn interface, and provider retry typing. The
  protocol conversion helpers keep `ReviewerPolicy` as the only tool exposure
  decision point while translating that result into Chat or Responses wire
  shape. Chat and Responses request builders replay prior assistant tool calls
  with provider-visible aliases, and both parsers map provider-visible function
  names back to internal `ToolId`s before runtime consumption. Focused tests
  prove alias parsing across both protocols, alias replay across both protocols,
  Responses credential resolution, and Responses function-tool schema shape.
  This moves provider-neutral model routing to about 8.8/10, testability
  through interfaces to about 8.3/10, migration proof to about 9.3/10, and
  reusable execution to about 9.7/10. Remaining model-routing work is
  credentialed real-provider smoke/canary proof for both protocols and broader
  provider compatibility fixtures.

Latest local verification after Responses model protocol hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after Chat/Responses real-provider canary contract:

- `OpenAiProviderCanaryConfig`, `ModelProviderCanaryReport`, and
  `ModelProviderCanaryStatus` now give the model layer one runnable canary
  interface for the OpenAI-compatible Chat Completions and Responses adapters.
  Disabled canaries and missing credentials return explicit skipped reports
  instead of silently passing or forcing normal CI to call a live provider.
- The old Chat-only opt-in smoke test now asks the canary contract for its
  protocol matrix and requires both Chat Completions and Responses to pass when
  `MUZEN_RUN_REAL_PROVIDER_CANARY=1` or legacy `MUZEN_RUN_REAL_PROVIDER_SMOKE=1`
  is set and `OPENAI_API_KEY` is present. Without credentials, it asserts the
  safe skipped status for both protocols.
- `bench-concurrent --run-provider-canaries` can now run the same two-protocol
  provider canary before the real concurrent benchmark, emit the canary reports,
  and fail the benchmark if any credentialed canary skips or fails.
  Provider-neutral model routing moves to about 9.0/10. Remaining model-routing
  work is durable canary evidence capture, scheduled credentialed execution, and
  broader provider compatibility fixtures.

Latest local verification after Chat/Responses real-provider canary contract:

```bash
cargo fmt --check
cargo test -p muzen              # 98 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after artifact retention contract:

- `ArtifactRetentionPolicy` now gives hosts a public count/byte envelope for
  artifact retrieval and persistence. `ArtifactExportPolicy` applies the
  retention envelope after capability and artifact-id allowlist filtering, so
  a scoped export can retain one artifact while an unscoped export can still be
  denied by count or byte limits. Public report APIs apply the same policy to
  in-memory export manifests, finding evidence artifact traversal, and
  directory bundle export. Bundle export validates the envelope before creating
  `manifest.json` or artifact files, and returned manifests, validation reports,
  and cleanup reports carry the applied retention policy for audit. Public
  facade tests prove scoped retention succeeds, count retention fails with
  `artifact_retention_artifacts`, byte retention fails with
  `artifact_retention_bytes`, and rejected bundle export leaves no bundle files.
  This moves artifact/evidence retrieval to about 9.3/10, testability through
  interfaces to about 8.4/10, and migration proof to about 9.4/10. At that
  checkpoint, remaining store work was remote store adapters and a host-owned
  persistent artifact-store contract.

Latest local verification after artifact retention hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after root compatibility contract split:

- The root `muzen::reviewer` facade no longer re-exports raw ids, artifact
  contract types, repo paths, snapshot storage policy/status/mode, or runtime
  error/result/limit contracts directly. Those types now live under explicit
  reviewer contract families: `ids`, `artifacts`, `paths`, `storage`, and
  `runtime`. Public facade tests were updated to use those explicit modules
  instead of the root raw-contract path. This moves the deep host-facing module
  interface to about 9.5/10 and testability through interfaces to about
  9.7/10. Remaining facade work is replacing compatibility contracts in common
  host workflows with narrower reviewer-owned builders and value constructors.

Latest local verification after root compatibility contract split:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after host-facing run limits split:

- `RunSpec` now stores `ReviewRunLimits` instead of raw `RuntimeLimits`.
  Common callers use `ReviewRunLimits::standard`, while advanced callers can
  still opt into raw runtime tuning through `ReviewRunLimits::from_runtime_limits`
  and `reviewer::runtime::RuntimeLimits`. `RunBuilder` converts the facade
  limits once at the runtime orchestration edge. Public facade tests now
  construct runs without touching `reviewer::runtime::RuntimeLimits`. This moves
  the deep host-facing module interface to about 9.6/10. Remaining facade work
  is replacing compatibility ids/path/storage/artifact contracts in common host
  workflows.

Latest local verification after host-facing run limits split:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after host-facing snapshot helpers:

- Common snapshot callers no longer need `reviewer::paths::RepoPath` or
  `reviewer::storage::SnapshotStoragePolicy` for normal captured-evidence
  workflows. `SnapshotSpec` now exposes `with_memory_storage_limit` and
  `with_content_addressed_storage`; `SnapshotReader` exposes `read_text_path`;
  snapshot manifest/file/validation views expose storage/status predicates for
  common assertions. Public facade tests now prove memory-envelope skips,
  content-addressed snapshot reads, stale object detection, and cleanup through
  those host-facing methods. This moves the deep host-facing module interface to
  about 9.7/10 and testability through interfaces to about 9.8/10. Remaining
  facade work is replacing compatibility ids/artifact contracts in common host
  workflows.

Latest local verification after host-facing snapshot helpers:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after host-facing artifact export helpers:

- Common redacted artifact export no longer requires callers to construct a
  `CapabilitySet` or raw `ArtifactId` values. `ArtifactExportPolicy` now exposes
  `redacted_all` and `redacted_artifacts`; raw export remains capability-gated
  through `ArtifactExportPolicy::raw`. Evidence/export/persistence views expose
  string artifact-id accessors for common host assertions and object-store
  validation. Public facade tests now prove redacted all-artifact export,
  redacted scoped export, retention, persistence, stale/missing validation, and
  raw-export denial through this host-facing interface. This moves the deep
  host-facing module interface to about 9.8/10 and artifact/evidence retrieval
  to about 9.7/10. Remaining facade work is replacing compatibility ids and
  advanced artifact-object contracts in less common host workflows.

Latest local verification after host-facing artifact export helpers:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after policy-owned terminal diagnostics:

- Runtime no longer assembles `SessionTerminalDiagnostic` from evidence and
  terminal internals directly. `ReviewerPolicy` now owns normal and early-exit
  terminal diagnostic construction, and focused policy tests prove completed
  sessions, evidence flags, terminal summaries, model-call counts, tool counts,
  and early-exit summaries through the policy interface. This moves policy
  locality to about 8.3/10. Remaining locality work is moving more of the
  session loop and runtime event planning behind policy-owned interfaces.

Latest local verification after policy-owned terminal diagnostics:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after policy-owned evidence-gated tool batches:

- Runtime no longer loops over model tool calls to decide which terminal tools
  are denied before required evidence exists, and it no longer hard-codes the
  denial code/message/retryability for that policy decision. `ReviewerPolicy`
  now owns evidence-gated tool-batch planning and returns allowed calls plus
  typed policy denials. Runtime still executes allowed calls, records metrics,
  and emits events. Focused policy tests prove mixed batches deny
  `record_finding` and `finish` before evidence while allowing non-terminal
  tools, and allow all terminal tools once evidence is ready. This moves policy
  locality to about 8.5/10. Remaining locality work is moving more of the
  session loop and runtime event planning behind policy-owned interfaces.

Latest local verification after policy-owned evidence-gated tool batches:

```bash
cargo fmt --check
cargo test -p muzen              # 91 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after policy-owned tool-batch budget planning:

- Runtime no longer applies the session tool-call budget to model batches or
  hard-codes `BudgetExceeded` denial details. `ReviewerPolicy` now owns the
  combined tool-batch plan: the budget window, planned batch count, evidence
  denials, budget denials, allowed calls, and denial code/message/retryability.
  Runtime consumes that plan to emit the batch-start event, create typed error
  results, record metrics, and execute allowed calls. Focused policy tests prove
  that budget is applied before the evidence gate, preserving result indexes and
  existing batch semantics while moving tool-batch decisions behind the policy
  interface. This moves policy locality to about 8.7/10. Remaining locality
  work is moving more runtime event planning and the broader session loop behind
  policy-owned interfaces.

Latest local verification after policy-owned tool-batch budget planning:

```bash
cargo fmt --check
cargo test -p muzen              # 92 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after policy-owned tool-result runtime event planning:

- Runtime no longer decides which `RuntimeEvent` variants a tool result should
  produce for policy denials, completions, search batches, artifact creation, or
  recorded findings. `ReviewerPolicy` now returns planned runtime events with
  explicit contexts. Runtime still owns artifact lookup, legacy event emission,
  metrics, transcript append, and delivery to the runtime event sink. Focused
  policy tests prove denial-before-completion ordering, search batch events,
  finding events, artifact events, and context population through the policy
  interface. This moves policy locality to about 8.9/10. Remaining locality
  work is moving more of the broader session loop and lifecycle event planning
  behind deeper interfaces.

Latest local verification after policy-owned tool-result runtime event planning:

```bash
cargo fmt --check
cargo test -p muzen              # 94 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after policy-owned lifecycle runtime event planning:

- Runtime no longer constructs session start/finish, model start/completion, or
  tool-batch start runtime events inline. `ReviewerPolicy` now returns planned
  lifecycle events with explicit contexts, including suppressing zero-count
  tool-batch start events. At that checkpoint, runtime still owned the session
  loop, legacy event planning/emission, model execution, tool execution, and
  delivery to the runtime event sink. Focused policy tests prove lifecycle event
  variants, counts, statuses, and context population through the policy
  interface. This moved policy locality to about 9.0/10. Remaining locality work
  was moving the broader session state machine and legacy event planning behind
  deeper interfaces.

Latest local verification after policy-owned lifecycle runtime event planning:

```bash
cargo fmt --check
cargo test -p muzen              # 95 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after policy-owned legacy session lifecycle event planning:

- Runtime no longer constructs legacy `SessionStarted` / `SessionFinished`
  event records inline. `ReviewerPolicy` now plans the legacy event payloads,
  session ids, status values, tool counts, and model-call counts through
  `plan_session_started_event` and `plan_session_finished_event`.
- Runtime now emits session start/finish through small helpers that dispatch the
  policy-planned legacy event and the policy-planned `RuntimeEvent` together.
  The model-router failure path now also emits the structured
  `RuntimeEvent::SessionFinished` through the same helper instead of only the
  legacy finish event.
- Focused policy tests now prove legacy lifecycle event type, level, session id,
  state, model-call count, and tool-count payload shape next to the structured
  runtime lifecycle events. This moves policy locality to about 9.1/10. Runtime
  still owns async session orchestration, model/tool execution, transcript
  mutation, and event dispatch.

Latest local verification after policy-owned legacy session lifecycle event
planning:

```bash
cargo fmt --check
cargo test -p muzen              # 100 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after policy-owned legacy model event planning:

- Runtime no longer constructs legacy `ModelCallStarted` or
  `ModelCallCompleted` event records inline. `ReviewerPolicy` now plans the
  legacy model event payloads, including turn index, retry attempt, and token
  usage.
- Runtime centralizes model start/completion emission in helpers that dispatch
  policy-planned legacy events and policy-planned model `RuntimeEvent`s
  together. This removed duplicate `ModelCallCompleted` construction from the
  text and tool-call branches of the session loop.
- Focused policy tests now prove legacy model event type, level, session id,
  turn, attempt, and token payload shape next to the structured runtime model
  events. This moves policy locality to about 9.2/10. Runtime still owns async
  model execution, retry timing, transcript mutation, tool-call legacy events,
  and event dispatch.

Latest local verification after policy-owned legacy model event planning:

```bash
cargo fmt --check
cargo test -p muzen              # 100 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after policy-owned legacy tool event planning:

- Runtime no longer constructs legacy `ToolCallRequested`,
  `ToolCallCompleted`, `ArtifactRecorded`, or `FindingValidated` event records
  inline. `ReviewerPolicy` now plans those legacy tool event payloads, including
  tool name, success/error status, error code, artifact summary/id, and finding
  id.
- The previous shallow `runtime::session` helper module was removed; its
  remaining event-summary/status behavior now lives with the policy that owns
  the legacy event record shape.
- Runtime still owns side effects and orchestration: tool dispatch, finding
  recording, artifact lookup, transcript mutation, and event emission. This
  moves policy locality to about 9.3/10, but it is not enough for a confident
  10/10 until the broader async session loop has stronger locality.
- Focused policy tests now prove legacy requested/completed/artifact/finding
  payload shape next to the structured runtime tool-result event plans.

Latest local verification after policy-owned legacy tool event planning:

```bash
cargo fmt --check
cargo test -p muzen              # 100 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after policy-owned transcript append planning:

- Runtime no longer constructs `ConversationItem::AssistantText`,
  `ConversationItem::AssistantToolCalls`, or `ConversationItem::ToolResult`
  values inline. `ReviewerPolicy` now plans the transcript append item shape
  through `plan_assistant_text_transcript_item`,
  `plan_assistant_tool_calls_transcript_item`, and
  `plan_tool_result_transcript_item`.
- Runtime still owns when transcript items are appended, including cancellation
  windows and side-effect ordering around tool execution, artifact lookup,
  finding recording, and event emission. That keeps orchestration explicit while
  concentrating transcript representation policy with initial transcript,
  exposure, and model-visible compaction.
- Focused policy tests now prove assistant text, assistant tool-call, and tool
  result transcript item shape. This moves policy locality to about 9.4/10, but
  a confident 10/10 still needs broader async session-loop locality and
  scheduled credentialed canary execution.

Latest local verification after policy-owned transcript append planning:

```bash
cargo fmt --check
cargo test -p muzen              # 101 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after policy-owned model error event planning:

- Runtime no longer constructs model-router failure or model-attempt retry/fail
  `EventType::Error` records inline. `ReviewerPolicy` now plans those records
  through `plan_model_router_error_event` and
  `plan_model_attempt_error_event`, including event level, turn, attempt,
  retrying flag, session id, and redacted error text.
- Runtime still owns when those error events are emitted, retry timing, and
  final control flow. The event payload shape and error-string redaction path
  are now local to policy with the rest of the legacy event record planning.
- Focused policy tests now prove router error, retrying model error, and final
  model error payloads. This moves policy locality to about 9.5/10, but not to
  10/10 because runtime still owns the broader async session loop, side-effect
  ordering, transcript append timing, and event dispatch.

Latest local verification after policy-owned model error event planning:

```bash
cargo fmt --check
cargo test -p muzen              # 101 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after session model accounting extraction:

- Runtime no longer owns separate mutable counters for model calls, successes,
  errors, retries, latency, token totals, priced/unpriced calls, or estimated
  model cost. `runtime::accounting::SessionModelAccounting` now owns that
  accounting and produces the `ModelMetricsSnapshot` and `TokenUsage` values
  consumed by `SessionReport`.
- The session loop still owns async control flow, cancellation windows,
  transcript append timing, tool-result side-effect ordering, and event
  dispatch. This extraction narrows the loop's bookkeeping responsibility, but
  it is not enough to remove the broader async session-loop locality gap.
- Focused accounting tests prove success/retry/cost/token behavior and
  error/unpriced/max-latency behavior without a runtime, repo, model client, or
  tool engine.

Latest local verification after session model accounting extraction:

```bash
cargo fmt --check
cargo test -p muzen              # 103 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after runtime event dispatcher extraction:

- `JobRuntime` no longer stores raw optional legacy and runtime event
  sinks or implements the optional dispatch checks inline.
  `runtime::dispatch::RuntimeEventDispatcher` now owns legacy
  `EventEmitter` delivery, structured `RuntimeEventSink` delivery, planned
  runtime-event delivery, and no-sink dropping behavior.
- Runtime still owns event timing and event order: session lifecycle, model
  attempts, tool-result side effects, transcript append timing, and
  cancellation windows decide when the dispatcher is called. This narrows the
  event-dispatch part of the session-loop locality gap without hiding the
  orchestration order.
- Focused dispatcher tests prove legacy-event JSON output, runtime-event
  context delivery, and no-sink dropping behavior without running sessions.
  This moves policy/locality to about 9.6/10, but not to 10/10 because
  side-effect ordering and scheduled credentialed canary execution remain open.

Latest local verification after runtime event dispatcher extraction:

```bash
cargo fmt --check
cargo test -p muzen              # 106 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after tool-result effect processor extraction:

- Runtime no longer owns the per-result sequence for evidence observation,
  terminal error tracking, finding recording, artifact lookup, legacy event
  emission, structured runtime event emission, tool-count updates, and
  transcript appends. `runtime::effects::ToolResultEffectProcessor` now owns
  that deterministic ordering behind one batch interface.
- The async session loop still owns cancellation windows, when a tool batch is
  handed to the processor, when terminal completion stops the loop, and when
  repeated terminal denials fail the session. That keeps async control flow in
  runtime while moving the side-effect chain into a focused module.
- Focused effects tests prove ordered handling for repeated terminal denials and
  a successful search result, including terminal-error policy state, evidence
  observation, tool counts, transcript append order, and runtime event order.
  This moves policy/locality to about 9.7/10, but not to 10/10 because broader
  session-loop timing and scheduled credentialed canary execution remain open.

Latest local verification after tool-result effect processor extraction:

```bash
cargo fmt --check
cargo test -p muzen              # 107 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after session flow extraction:

- Runtime no longer mutates raw `completed`, `cancelled`, and `failed` booleans
  inline. `runtime::flow::SessionFlow` now owns state transitions for
  pre-model cancellation, per-turn budget/cancellation checks, model error
  classification, model completion, post-tool successful-batch cancellation,
  terminal completion, and repeated terminal-denial failure.
- The async session loop still owns model/tool await points, when tool batches
  are handed to `ToolResultEffectProcessor`, transcript append timing, and event
  emission timing. This leaves orchestration explicit while making the loop's
  stop/continue state machine testable through one module interface.
- Focused flow tests prove cancellation windows, budget exhaustion without
  failure, provider-error vs cancellation classification, completion, and
  terminal/failure tool-batch stop conditions. This moves policy/locality to
  about 9.8/10, but not to 10/10 because scheduled credentialed canary
  execution and broader provider/cloud proof remain open.

Latest local verification after session flow extraction:

```bash
cargo fmt --check
cargo test -p muzen              # 111 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after durable provider canary evidence gate:

- `ModelProviderCanaryEvidence` now wraps live-provider canary reports in a
  schema-versioned proof object with generated time, required protocol matrix,
  pass/skip/fail counts, explicit gate failures, and `require_passed`.
  Missing, duplicate, skipped, or failed protocol reports are proof failures
  instead of ad hoc caller checks.
- `bench-concurrent --provider-canary-report <path>` now implies provider
  canaries, writes the schema-versioned JSON evidence before the benchmark, and
  uses the same model-layer evidence gate as `--run-provider-canaries`. Failed
  or skipped live canaries leave a durable report behind for CI/release logs.
- Focused tests prove skipped, missing, duplicate, and successful protocol
  matrix behavior plus JSON export roundtrip without live credentials. This
  moves provider-neutral model routing to about 9.2/10 and migration proof to
  about 9.8/10, but not to 10/10 because scheduled credentialed execution and
  broader provider/cloud proof remain open.

Latest local verification after durable provider canary evidence gate:

```bash
cargo fmt --check
cargo test -p muzen              # 113 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after remote object-store canary evidence gate:

- `RemoteObjectStoreCanaryEvidence` now gives the public
  `muzen::reviewer::storage` interface one schema-versioned proof object for
  host-provided remote snapshot and artifact object clients. The gate records
  the target, base URI, content-addressed object URI, payload hash, cleanup
  support, per-step outcomes, and recomputed pass/fail/skip counts.
- `run_remote_snapshot_object_store_canary` proves a snapshot remote client can
  put, read after put, remove, and read-after-remove a hash-derived object.
  `run_remote_artifact_object_store_canary` now proves artifact remote clients
  can put, read after put, remove, and read-after-remove a view/hash-derived
  object after `RemoteArtifactObjectClient` gained remove authority.
- `export_remote_object_store_canary_evidence` writes durable JSON evidence,
  and public facade tests prove snapshot cleanup, artifact cleanup, JSON
  export/load, and forged gate rejection through
  `reviewer::storage`.
- `ArtifactObjectStore` now owns removal as well as persistence and readback,
  and `ArtifactPersistenceManifest::cleanup_storage` removes manifest-owned
  memory, local filesystem, and remote artifact objects while reporting checked,
  removed, missing, and stale object refs. This moves immutable evidence,
  artifact/evidence retrieval, and migration proof to about 9.9/10, but not to
  10/10 because scheduled production cloud-client canary runs remain open.

Latest local verification after remote object-store canary evidence gate:

```bash
cargo fmt --check
cargo test -p muzen              # 115 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after session runner extraction:

- `JobRuntime` no longer owns the per-session async loop. It now
  creates a `runtime::session_loop::SessionRunner` for each scheduled
  session, enforces the active-session semaphore, and aggregates
  `SessionReport`s into the run report.
- `SessionRunner::run_scope` owns the per-session orchestration for session
  lifecycle events, model routing, model retry/await timing, model accounting,
  transcript append timing, guarded tool-batch execution, tool-result
  side-effect processing, cancellation windows, and terminal diagnostics behind
  one narrow interface. This gives the per-session control-flow module a real
  seam instead of leaving it embedded in the job runtime.
- Focused runtime tests still prove cancellation during model/tool execution,
  post-tool cancellation before transcript append, retry behavior, provider
  error classification, budget exhaustion, terminal-before-evidence rejection,
  lifecycle events, and model cost accounting through the extracted runner.
  This moves policy/locality to about 9.9/10, but not to 10/10 because
  `SessionRunner` still combines model-turn awaiting, guarded tool-batch
  awaiting, transcript append timing, and event emission timing.

Latest local verification after session runner extraction:

```bash
cargo fmt --check
cargo test -p muzen              # 115 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after model-turn and tool-batch runner extraction:

- `runtime::model_turn::ModelTurnRunner` now owns model turn completion,
  retryable-error retry timing, attempt accounting inputs, elapsed-time
  measurement, and model start/completion event emission. It reports a compact
  `ModelTurnCompletion` or `ModelTurnFailure` to `SessionRunner`, which keeps
  model accounting and turn-flow decisions outside the model-await module.
- `runtime::tool_batch::ToolBatchRunner` now owns the guarded batch execution
  path after a model emits tool calls. It asks `ReviewerPolicy` for the batch
  plan, emits the planned batch-start runtime event, turns denied calls into
  tool results with metrics, executes allowed calls through `ToolEngine`, and
  returns results in original model-call order.
- `SessionRunner::run_scope` now coordinates the per-session turn loop without
  embedding model retry mechanics or policy-denial merge mechanics. Its
  remaining ownership is turn flow, transcript append timing, model/tool
  handoff, cancellation windows, tool-result side-effect application, and
  terminal diagnostics.
- Focused unit tests now prove retryable model-turn retry/event behavior and
  guarded tool-batch denial/order behavior through the new runner interfaces,
  while the existing runtime tests continue to prove cancellation, retry,
  terminal-before-evidence, lifecycle, and budget behavior end to end. This
  moves policy/locality to about 9.95/10 and testability through interfaces to
  about 9.85/10, but not to 10/10 because credentialed provider/cloud canary
  automation and broader real-provider compatibility gates remain open.

Latest local verification after model-turn and tool-batch runner extraction:

```bash
cargo fmt --check
cargo test -p muzen              # 117 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after public canary evidence manifest:

- `reviewer::canaries` now exposes the advanced canary interface in one public
  module: live OpenAI-compatible provider canary config/report/evidence export,
  remote snapshot/artifact object-store canary functions and evidence, and a
  schema-versioned aggregate `CanaryEvidenceManifest`.
- `CanaryEvidenceManifest` requires exactly one passing model-provider canary
  evidence object, one passing snapshot remote object-store canary evidence
  object, and one passing artifact remote object-store canary evidence object.
  Its gate recomputes child validation instead of trusting serialized `valid`
  flags, and fails closed for missing, duplicate, skipped, failed, or forged
  evidence.
- `export_canary_evidence_manifest` writes a durable JSON proof object with the
  same atomic-temp-file pattern as provider and remote object-store evidence.
  Public facade tests prove roundtrip export, missing-provider failure,
  duplicate snapshot-store failure, and forged aggregate-gate rejection through
  `reviewer::canaries`.
- This moves reusable-kernel confidence to about 9.9/10, provider-neutral model
  routing to about 9.4/10, immutable evidence and artifact/evidence retrieval
  to about 9.95/10, testability through interfaces to about 9.9/10, and
  migration proof to about 9.95/10. It is still not 10/10 because this local
  proof only validates the aggregate gate; scheduled credentialed provider and
  production cloud-client jobs still need to publish current passing manifests.

Latest local verification after public canary evidence manifest:

```bash
cargo fmt --check
cargo test -p muzen              # 118 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after CLI canary manifest gate:

- Provider canary evidence, remote object-store canary evidence, and aggregate
  canary evidence manifests now have public load functions in
  `reviewer::canaries`, so automation does not need to duplicate JSON parsing
  or schema handling.
- `muzen canary-manifest --provider-evidence <path>
  --remote-object-store-evidence <path> --remote-object-store-evidence <path>
  --output <path>` composes the aggregate canary evidence manifest from
  previously exported child evidence files. It writes the manifest and then
  fails the command if the aggregate gate does not pass, leaving an auditable
  failed proof object behind for scheduled jobs.
- Public tests now prove CLI parsing, successful manifest composition from
  evidence files, manifest load/require-pass after command output, and failure
  with an exported invalid manifest when artifact-store evidence is missing.
  This narrows the remaining canary gap to actual scheduled credentialed
  provider and production cloud-client execution, not Muzen-side proof
  composition.

Latest local verification after CLI canary manifest gate:

```bash
cargo fmt --check
cargo test -p muzen              # 119 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after canary evidence freshness gate:

- `CanaryEvidenceFreshnessPolicy` now gives `reviewer::canaries` a dynamic
  validation policy for current evidence. The aggregate manifest can require
  the manifest timestamp, model-provider evidence timestamp, snapshot
  object-store evidence timestamp, and artifact object-store evidence timestamp
  to be within a caller-specified age window and not in the future.
- `muzen canary-manifest` now accepts `--max-evidence-age-seconds`, defaulting
  to 86,400 seconds. The command still writes the aggregate manifest first, but
  it now fails if any child or aggregate evidence is stale or future-dated.
- Public tests prove fresh evidence passes, stale evidence fails for each
  required evidence family, future-dated evidence fails, and the CLI still
  composes and gates evidence files through the default freshness window. This
  closes the stale-passing-manifest hole in Muzen-side proof automation. The
  remaining 10/10 gap is actual scheduled credentialed provider and production
  cloud-client execution that publishes current passing manifests.

Latest local verification after canary evidence freshness gate:

```bash
cargo fmt --check
cargo test -p muzen              # 120 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after CLI published-manifest verification gate:

- `muzen canary-verify --manifest <path> --max-evidence-age-seconds <seconds>`
  now validates one previously published aggregate `CanaryEvidenceManifest`
  using the same gate and freshness policy as `muzen canary-manifest`.
- Release gates no longer need child evidence files or custom JSON parsing to
  validate the exact proof artifact being promoted. They can run
  `canary-verify` against the manifest that a scheduled provider/cloud canary
  job published.
- Public tests prove CLI parsing, successful verification of a freshly exported
  manifest, and failure for a stale published manifest. This improves
  automation locality and testability, but it still does not replace actual
  scheduled credentialed provider and production cloud-client runs.

Latest local verification after CLI published-manifest verification gate:

```bash
cargo fmt --check
cargo test -p muzen              # 121 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after host-facing artifact workflow facade:

- `RunReport::redacted_artifacts` and `RunReport::raw_artifacts` now return a
  reviewer-owned `ReviewArtifacts` workflow facade. Hosts can scope artifact
  ids, apply retention, export manifests, traverse finding evidence, persist
  objects, and write bundles without directly constructing
  `ArtifactExportPolicy` for the common paths.
- `ArtifactPersistenceManifest` and `ArtifactObjectRef` now expose read-only
  object-ref helpers such as first object, object refs, artifact containment,
  view, bytes, content hash, URI, and optional local path. Validation and
  cleanup reports expose artifact-id membership helpers for missing, stale,
  and removed objects.
- `ArtifactBundleManifest::new`, `with_manifest_path`, and
  `ArtifactBundleEntry::new` give tests and hosts value constructors for
  bundle lifecycle validation without importing compatibility artifact ids.
- Public facade tests now prove export, scoped finding evidence, in-memory
  persistence, validation, cleanup, and local persistence through the new
  facade, including object path checks via accessors instead of raw fields.
  This moves deep host-facing module depth to about 9.9/10 and artifact
  retrieval/testability slightly higher. The remaining 10/10 gap is not local
  artifact API shape; it is scheduled credentialed provider and production
  cloud-client jobs publishing current passing aggregate canary manifests.

Latest local verification after host-facing artifact workflow facade:

```bash
cargo fmt --check
cargo test -p muzen              # 122 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after canary publication interface:

- `HttpRemoteObjectClient` now implements the remote snapshot and artifact
  object-client interfaces with HTTP `PUT`, `GET`, and `DELETE`, plus optional
  bearer-token authorization. This gives production infrastructure a concrete
  adapter for the same remote object-store canary contract used by memory
  tests, without baking one cloud vendor SDK into Muzen.
- `muzen canary-publish --output-dir <dir> --snapshot-base-uri <uri>
  --artifact-base-uri <uri>` now owns the full publication workflow: run or
  reuse provider evidence, run snapshot and artifact remote object-store
  canaries, write the three child evidence files, compose `manifest.json`, and
  verify the aggregate freshness gate. Failed publication still leaves the
  child and aggregate evidence files behind for release logs.
- `--object-store-driver http` is the production/default path. Tests use
  `--object-store-driver memory` with an existing provider evidence file to
  prove the command writes `model-provider.json`,
  `remote-snapshot-object-store.json`, `remote-artifact-object-store.json`,
  and `manifest.json`, then verifies the aggregate manifest through the public
  loader and freshness policy.
- This moves Muzen-side canary publication from an out-of-band script concern
  into a deep CLI module interface. The remaining 10/10 gap is now external:
  scheduled infrastructure must run `canary-publish` with live OpenAI-compatible
  credentials and a production object-store HTTP endpoint, then publish a
  current passing manifest.
- `.github/workflows/muzen-canary-evidence.yml` wires that interface into a
  daily/manual scheduled job. The workflow builds the release binary, runs
  `canary-publish` with required provider and object-store secrets, verifies
  the published manifest, and uploads the evidence directory even on failure.
  This makes the remaining gap operational configuration and successful
  external execution, not missing repository automation.

Latest local verification after canary publication interface:

```bash
cargo fmt --check
cargo test -p muzen              # 123 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after public JSON-RPC provider facade:

- `ReviewToolRegistry` now exposes reviewer-owned JSON-RPC registration
  methods for external provider tools. The unscoped helper keeps simple tools
  terse, while scoped tools use a named `ReviewJsonRpcReadOnlyToolRegistration`
  so provider id, resource scope, schema, cacheability, and transport are
  reviewable without a long positional argument list. Common hosts no longer
  need to construct the lower-level `JsonRpcToolRegistration` for read-only
  provider tools.
- `ReviewSessionSpec` now exposes `grant_provider_read_only_tool` and
  `grant_provider_read_only_tool_for_resources`, which grant the tool and scope
  runtime provider/resource authority in one place while preserving built-in
  review-provider access.
- Public facade tests now run a scoped JSON-RPC provider tool through
  `Run::builder` and prove provider-resource propagation, provider metrics, and
  provider health. A paired public denial test proves mismatched provider
  resources emit `ToolCallDenied` and never call the transport.
- This moves external-provider contracts from mostly private `ToolEngine`
  tests into the host-facing reviewer interface. Remaining provider work is
  broader external-provider contract coverage and scheduled credentialed
  provider/object-store evidence publication.

Latest local verification after public JSON-RPC provider facade:

```bash
cargo fmt --check
cargo test -p muzen              # 131 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after public HTTP JSON-RPC provider proof:

- Public reviewer-facade tests now run a scoped provider tool through
  `HttpJsonRpcToolTransport` against a loopback JSON-RPC server. This proves
  the real HTTP adapter, not only an in-memory transport double.
- The test asserts the wire request envelope carries JSON-RPC 2.0,
  `method: tool.call`, the provider id, tool id, provider-resource scope, and
  raw arguments, then proves the run completes and records successful provider
  metrics.
- This removes the local real-HTTP JSON-RPC compatibility gap from
  provider-neutral tool execution. The remaining provider gap is broader
  external-provider ecosystem coverage and the completed scheduled
  provider/object-store canary manifest.

Latest local verification after public HTTP JSON-RPC provider proof:

```bash
cargo fmt --check
cargo test -p muzen              # 136 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after public JSON-RPC network-read capability facade:

- `ReviewToolRegistry` now exposes reviewer-owned JSON-RPC network-read
  registration methods through `register_jsonrpc_network_read_tool` and
  `register_scoped_jsonrpc_network_read_tool`, using the named
  `ReviewJsonRpcNetworkReadToolRegistration` for scoped tools.
- `ReviewSessionSpec` now exposes `grant_provider_network_read_tool` and
  `grant_provider_network_read_tool_for_resources`, which grant the provider
  tool, provider/resource scope, and runtime network-read authority together.
- Public facade tests now prove both sides of the hard capability contract:
  a network-read JSON-RPC provider tool runs when explicitly granted through
  `ReviewSessionSpec`, and the same effect is denied before transport when a
  host grants the tool/effects but omits runtime network authority.
- This moves network-read external provider authorization from a private
  `ToolEngine` proof into the host-facing reviewer interface. Broader
  real-provider ecosystem gates and the scheduled credentialed canary manifest
  remain open.

Latest local verification after public JSON-RPC network-read capability facade:

```bash
cargo fmt --check
cargo test -p muzen              # 140 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after canary publication preflight:

- `muzen canary-preflight` now accepts the same publication arguments as
  `canary-publish` and emits a structured preflight report before evidence is
  written. The report fails for missing live provider credentials, invalid
  provider base URLs, empty models, invalid freshness windows, and remote
  object-store base URIs that are incompatible with the selected driver.
- Reused provider evidence is loaded and schema-gated during preflight, so
  scheduled jobs that intentionally avoid live provider calls can prove their
  substituted evidence is structurally valid before publication begins.
- HTTP object-store authorization is reported as a pass or warning without
  printing token values. This keeps bearer-token and signed-URL deployments
  supported while still making missing token configuration visible in CI logs.
- The scheduled canary workflow now runs `canary-preflight` with the exact
  arguments passed to `canary-publish`. The remaining 10/10 gap is therefore a
  completed scheduled run with live credentials and production object-store
  endpoints, not hidden configuration ambiguity.

Latest local verification after canary publication preflight:

```bash
cargo fmt --check
cargo test -p muzen              # 134 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after structured canary status reports:

- `CanaryEvidenceManifest::status_report` now exposes the same aggregate gate
  and freshness checks as a serializable status object. The report includes the
  manifest schema version, manifest timestamp, freshness check timestamp, max
  evidence age, aggregate gate, schema/gate failures, freshness failures, and a
  combined failure list.
- `muzen canary-status --manifest <path> --output <path>
  --max-evidence-age-seconds <seconds>` writes that report for a previously
  published manifest and exits non-zero when either the aggregate gate or
  freshness gate fails. It writes the status JSON before returning a failure,
  so callers that run the status command keep a structured diagnostic.
- Public tests now prove status reports separate missing-evidence gate failures
  from stale evidence failures, and CLI tests prove fresh manifests write a
  passing status report while stale published manifests write a failing status
  report with status-specific diagnostics.
- The scheduled canary workflow now writes
  `bench/canary-evidence/status.json` before the silent `canary-verify` gate,
  so the uploaded evidence bundle contains the manifest, child evidence, and
  reviewer-friendly aggregate status when publication succeeds.
- This improves evidence reviewability for the last external gap. It still does
  not replace the required scheduled credentialed provider/object-store run
  that must publish a current passing aggregate manifest.

Latest local verification after structured canary status reports:

```bash
cargo fmt --check
cargo test -p muzen              # 138 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after SDK runner callback protocol seam:

- `muzen-runner stdio` now uses an interactive JSON-RPC transport when serving
  the SDK protocol. Stateful host requests still go through the runner session,
  while runner-to-SDK `model.complete` and `tool.execute` callbacks drive the
  public `ReviewModel` and `ReviewToolHandler` adapters behind the same review
  kernel used by Rust hosts.
- Interactive runs stream both `event.runtime` and `event.review`
  notifications, then publish `run.finished` or `run.failed`, so SDK hosts can
  consume low-level runtime telemetry or host-facing review events without
  linking to private runtime modules.
- Runner handshake and schema fixtures now mark `model.complete`,
  `tool.execute`, and `event.runtime` as implemented. The fixture tests prove
  accidental protocol-status drift is caught in Rust tests.
- `interactive_stdio_runs_model_and_tool_callbacks` proves a mock SDK can start
  a run, answer a model callback with a custom tool call, answer the tool
  callback with data, receive runtime/review event notifications, and get a
  completed run result through the stable runner protocol.
- This closes the largest local SDK-runner depth gap. It does not replace the
  remaining external proof: a scheduled credentialed provider run that
  publishes a current passing aggregate manifest against production
  object-store endpoints.

Latest local verification after SDK runner callback protocol seam:

```bash
cargo fmt --check
cargo test -p muzen              # 141 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
target/release/muzen canary-status --help
```

Implemented state after canary status evidence summary:

- `CanaryEvidenceStatusReport` now includes a structured `evidence` summary.
  It records whether model-provider evidence is present, the current required
  provider protocol matrix, reported protocols, passed protocols, and the
  provider gate. It also records one status entry each for expected snapshot and
  artifact remote object-store evidence, including evidence count, timestamp,
  base URI, object URI, and gate summary.
- `muzen canary-status` therefore writes a single review entrypoint:
  `status.json` still carries aggregate gate and freshness failures, but it now
  also answers what live provider and remote object-store proof was observed.
  Reviewers no longer have to infer the evidence contract from child file
  structure before deciding whether a scheduled bundle is usable.
- CLI tests now assert the status summary includes the required provider
  protocol matrix, passed protocols, and valid snapshot/artifact
  remote-object-store summaries for a fresh passing manifest.
- This improves reviewability of the final external proof. It still cannot
  satisfy the final 10/10 requirement without an actual scheduled credentialed
  run against production provider and object-store endpoints.

Latest local verification after canary status evidence summary:

```bash
cargo fmt --check
cargo test -p muzen              # 141 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after publish-owned canary diagnostics:

- `muzen canary-publish` now writes `status.json` beside `manifest.json`
  immediately after manifest creation and before returning a manifest-gate
  failure. A failing aggregate gate therefore still leaves provider evidence,
  snapshot object-store evidence, artifact object-store evidence, the manifest,
  and the structured status artifact in the same publication directory.
- `muzen canary-publish` also writes `publication.json`, which records the
  provider evidence source (`live_provider_canary` vs `reused_evidence_file`),
  optional reused evidence input path, object-store driver, provider base URL,
  model, freshness window, emitted evidence filenames, final status, and
  failures. This lets a scheduled artifact bundle prove it used live provider
  canaries and the HTTP object-store adapter without relying on workflow logs.
- The scheduled canary workflow now saves `canary-preflight` output as
  `bench/canary-evidence/preflight.json` with `tee`, so configuration failures
  can still be reviewed from the uploaded artifact bundle.
- CLI tests now prove `canary-publish` writes `status.json` for both passing
  publication and failing manifest-gate publication. The failure test uses
  structurally loaded provider evidence with an incomplete required protocol
  matrix, so the remote object-store child evidence is still published while
  the aggregate gate fails and the diagnostic status is preserved.
- CLI tests also prove publication provenance for both reused-evidence and
  live-provider modes without making a network call.
- This fixes a reviewability bug in the scheduled proof path. The final 10/10
  blocker remains a completed scheduled run with live provider credentials and
  production object-store endpoints that produces a current passing status and
  manifest.

Latest local verification after publish-owned canary diagnostics:

```bash
cargo fmt --check
cargo test -p muzen              # 143 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen --bin muzen
cargo run --release -p muzen --bin muzen -- canary-publish --help
```

Implemented state after scheduled canary proof verifier:

- `muzen canary-proof` now validates a full evidence directory rather than a
  single manifest. The proof report requires `workflow.json`,
  `preflight.json`, `publication.json`, `model-provider.json`,
  `remote-snapshot-object-store.json`, `remote-artifact-object-store.json`,
  `manifest.json`, and `status.json`.
- `workflow.json` is now schema-versioned and records GitHub Actions
  provenance: event name, workflow, job, run id, run attempt, repository, ref,
  commit SHA, actor, server URL, and run URL. `muzen
  canary-workflow-provenance` now writes this artifact from the GitHub Actions
  environment before publishing evidence, so the Rust canary module owns the
  JSON shape that `canary-proof` later validates.
- `preflight.json` is now schema-versioned and includes the checked
  publication config: provider evidence source/input, object-store driver,
  remote snapshot/artifact base URIs, effective provider base URL and source,
  model, output-token envelope, and freshness window.
- The verifier rejects reused provider evidence, a reused provider input path,
  non-HTTP object-store publication, freshness-window mismatches, failed
  publication/status reports, non-HTTP remote object-store base URIs, failed
  child evidence gates, child evidence files that do not match the aggregate
  manifest, and saved preflight reports that contain reused-provider checks
  instead of the live credential/base-URI checks expected from a scheduled run.
  It also rejects preflight config mismatches against the publication, provider
  reports, snapshot evidence, artifact evidence, or status summaries. The proof
  now freshness-gates the workflow provenance timestamp, preflight report
  timestamp, publication report timestamp, and status-report freshness-check
  timestamp too. It also rejects missing workflow provenance, any event that is
  not `schedule`, workflow/job/repository/ref identity mismatches, and run URLs
  that do not exactly match the recorded server URL, repository, and run id, so
  `workflow_dispatch`, a differently named job, the wrong source repository or
  ref, or a misleading run link cannot masquerade as the scheduled proof
  required for 10/10.
- `proof.json` now records the expected scheduled event, workflow name, job id,
  repository, and git ref as `workflowExpectation`, alongside the observed
  `workflow.json` provenance. This makes the final artifact self-describing for
  release reviewers and external auditors.
- `proof.json` now also records `fileDigests` for the required
  `workflow.json`, `preflight.json`, `publication.json`,
  `model-provider.json`, `remote-snapshot-object-store.json`,
  `remote-artifact-object-store.json`, `manifest.json`, and `status.json`
  inputs. Each digest records the proof label, filename, byte count, and BLAKE3
  hash, binding the final proof report to the exact evidence bytes it
  validated.
- The scheduled canary workflow now writes
  `bench/canary-evidence/proof.json` after `canary-status` and
  `canary-verify`, so uploaded evidence contains one final structured proof
  artifact for reviewers and release gates. It now calls `muzen
  canary-workflow-provenance` for `workflow.json` instead of embedding a
  hand-written JSON generator in the workflow file. The verification step is
  diagnostic-first: it attempts status, manifest verification, and final proof
  generation before returning failure, so failed evidence gates still leave a
  `proof.json` report when the Muzen binary is available. The proof command is
  called with `GITHUB_REPOSITORY` and `GITHUB_REF` as exact expectations, so the
  uploaded proof is tied to the scheduled run source as well as the workflow
  and job identity.
- The scheduled canary workflow now declares `permissions: contents: read`,
  uses a non-cancelling `muzen-canary-evidence` concurrency group so scheduled
  proof artifacts are not interrupted by overlapping runs, installs and selects
  the stable Rust toolchain with `rustup` instead of an extra third-party
  action, and uploads canary evidence with an explicit 30-day retention period.
  The upload step uses `if-no-files-found: error`, so a run that produces no
  evidence cannot look like a successful proof publication.
- CLI tests now prove `canary-proof` accepts a live-provider/HTTP-object-store
  shaped bundle, rejects a reused-provider publication bundle, and rejects a
  reused-provider preflight shape even when the underlying manifest evidence
  still passes. They also prove a tampered preflight snapshot base URI fails the
  final proof and that stale preflight metadata fails even when the manifest
  evidence still passes. They now also prove missing workflow provenance,
  manual workflow-dispatch provenance, wrong workflow/job/repository/ref
  provenance, and a wrong workflow run URL fail the final proof. The passing
  proof test asserts that `proof.json` records the expected scheduled workflow
  source identity plus all eight evidence-file digests, including the byte count
  and BLAKE3 hash for `manifest.json`. A focused CLI test proves
  `canary-workflow-provenance`
  writes the expected GitHub Actions environment into the schema-versioned
  provenance artifact.
- This closes the reusable/local-proof loophole in the scheduled canary
  interface. The score remains 9.98/10 until the workflow actually runs with
  live provider credentials and production object-store endpoints and publishes
  a current passing `proof.json`.

Latest local verification after source-pinned proof provenance:

```bash
cargo fmt --check
cargo test -p muzen              # 155 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen --bin muzen
target/release/muzen canary-proof --help
target/release/muzen canary-workflow-provenance --help
ruby -e 'require "yaml"; YAML.load_file(".github/workflows/muzen-canary-evidence.yml")'
go run github.com/rhysd/actionlint/cmd/actionlint@latest .github/workflows/muzen-canary-evidence.yml
```

Implemented state after host-facing run summary split:

- `RunReport::summary` now exposes a host-facing `ReviewRunSummary` with run
  status, session/model/tool/finding counts, token totals, artifact totals,
  snapshot count, and benchmark validity without exposing the full raw
  `ConcurrentRunReport` shape. The detailed raw report is still available
  explicitly as `RunReport::metrics` under the `reviewer::metrics` contract
  family for advanced metrics inspection and legacy CLI conversion. Public
  facade tests now prove the host summary status and snapshot count, while
  snapshot/provider/tool metric assertions use `report.metrics`. This moves the
  deep host-facing module interface to about 9.4/10 and testability through
  interfaces to about 9.6/10. Remaining facade work is narrowing older
  compatibility contract surface.

Latest local verification after host-facing run summary split:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after capability/metrics module split:

- Capability, grant, scope, artifact-access, model-output, tool-input, and
  runtime-authority contracts now live under
  `muzen::reviewer::capabilities`. Cache, counter, limit, run-summary, and
  snapshot-metrics contracts now live under `muzen::reviewer::metrics`.
  Public signatures that still need these advanced contracts point at those
  explicit modules: `ReviewSessionSpec::with_capabilities`,
  `ArtifactExportPolicy::redacted`, `ArtifactExportPolicy::raw`, and
  `RunReport::summary`. Public tests now construct review capabilities through
  `reviewer::capabilities::CapabilitySet` and runtime-event fixture cache
  status through `reviewer::metrics::CacheStatus`. This moves the deep
  host-facing module interface to about 9.3/10 and testability through
  interfaces to about 9.5/10. Remaining facade work is narrowing legacy report
  summary and compatibility contract surface.

Latest local verification after capability/metrics module split:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after runtime-event helper module move:

- Runtime event JSONL manifests/load reports/migration reports, bounded and
  in-memory raw runtime event sinks, backpressure policy, runtime event JSONL
  export/load, and the contextless legacy schema marker now live inside
  `muzen::reviewer::runtime_events` rather than as root facade items re-exported
  by that module. The root facade still points ordinary hosts at
  `ReviewEventSink`, `ReviewEventRecord`, `ReviewEvent`, and review-event JSONL,
  while advanced runtime-log compatibility remains available through one
  explicit module. This moves the deep host-facing module interface to about
  9.2/10 and testability through interfaces to about 9.4/10. Remaining facade
  work is narrowing capability/metrics contract surface.

Latest local verification after runtime-event helper module move:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after model/tool adapter module split:

- Low-level model router/client and model metrics contracts are now grouped
  under `muzen::reviewer::model_adapters`, and low-level tool registry,
  JSON-RPC/custom-provider, provider id/resource, provider health, tool metric,
  and artifact-store contracts are grouped under
  `muzen::reviewer::tool_adapters`. The facade root no longer re-exports
  `ModelClient`, `ModelRouter`, `StaticModelRouter`, credential resolver types,
  raw `ToolRegistry`, custom/JSON-RPC tool adapter types, `ArtifactStore`,
  provider ids, provider health states, or tool metric keys. Advanced builder
  hooks (`RunBuilder::model_router`, `tool_registry`, and
  `shared_tool_registry`) still exist, but their signatures now point at the
  explicit adapter modules. Runtime/provider tests use `tool_adapters` for
  provider ids and health state, while public facade tests continue through
  `ReviewModel` and `ReviewToolRegistry`. This moves the deep host-facing
  module interface to about 9.1/10 and testability through interfaces to about
  9.3/10. Remaining facade work is narrowing root-level runtime-event helper
  types plus capability/metrics contract surface.

Latest local verification after model/tool adapter module split:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after advanced runtime-event module split:

- Raw runtime event payload/context/record access is now grouped under
  `muzen::reviewer::runtime_events`. The facade root no longer re-exports
  `RuntimeEvent`, `RuntimeEventContext`, `RuntimeEventRecord`, `TurnId`, or the
  raw `RuntimeEventSink` alias; `RunBuilder::event_sink` points advanced callers
  at `runtime_events::EventSink`. Runtime-event fixture, migration, bounded-sink,
  and cancellation tests now use the explicit advanced module, while public run
  tests continue to observe through `ReviewEventSink` and review-event JSONL.
  This moves the deep host-facing module interface to about 9.0/10 and
  testability through interfaces to about 9.2/10. Remaining facade work is
  narrowing root-level runtime-event helper types plus low-level
  model/tool/provider re-exports for advanced/internal compatibility.

Latest local verification after advanced runtime-event module split:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after host-facing review event JSONL adapter:

- `ReviewEventRecord` and `ReviewEvent` are now serializable host-facing event
  contracts with a dedicated `heimdaal.review-events.v1` JSONL adapter.
  `export_review_event_records_jsonl` and `load_review_event_records_jsonl`
  give embedded hosts a stable persistence path for facade events without
  depending on raw `RuntimeEventRecord` / `RuntimeEventContext` schema details.
  The public facade happy-path test now exports the `InMemoryReviewEventSink`
  records, verifies the review-event schema marker and run id in JSON, reloads
  the records, and checks exact equality. This moves the deep host-facing
  module interface to about 8.9/10, stable observability to about 9.9/10,
  testability through interfaces to about 9.1/10, and migration proof to about
  9.7/10. Remaining facade work is narrowing low-level re-exports for
  advanced/internal compatibility.

Latest local verification after host-facing review event JSONL hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after host-facing review event adapter:

- `RunBuilder::review_event_sink` now accepts `ReviewEventSink`, and the
  reviewer facade owns conversion between runtime `RuntimeEvent` /
  `RuntimeEventContext` records and host-facing `ReviewEventRecord` /
  `ReviewEvent` values. `ReviewEventRecord` preserves sequence, timestamp,
  run id, optional snapshot/session/turn/tool/artifact/finding context, and a
  simplified review event enum. Public facade tests now observe happy-path,
  denied-tool, cancelled-run, and multi-snapshot behavior through
  `InMemoryReviewEventSink` without matching raw `RuntimeEvent` variants or
  inspecting `RuntimeEventContext`; dedicated runtime JSONL fixture tests keep
  schema and migration compatibility proof. This moves the deep host-facing
  module interface to about 8.8/10, stable observability to about 9.8/10, and
  testability through interfaces to about 9.0/10. Remaining facade work is
  narrowing low-level re-exports for advanced/internal compatibility.

Latest local verification after host-facing review event hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after host-facing review tool adapter:

- `RunBuilder::review_tool_registry` now accepts `ReviewToolRegistry`, and the
  reviewer facade owns conversion between host-facing `ReviewToolHandler` /
  `ReviewToolContext` / `ReviewToolOutput` / `ReviewToolArtifact` values and
  runtime `ToolRegistry`, `CustomToolHandler`, `CustomToolContext`, and
  `CustomToolOutput` values. Public custom tools can register by string tool id
  and JSON schema, return optional JSON data and an optional artifact, and rely
  on the facade to fill runtime output metadata. The public custom-tool facade
  test now registers and executes its host custom check through
  `ReviewToolRegistry` and `RunBuilder::review_tool_registry`, while lower-level
  provider tests keep exercising the raw registry contracts directly. This
  moves the deep host-facing module interface to about 8.6/10 and testability
  through interfaces to about 8.9/10. Remaining facade work is narrowing
  low-level re-exports for advanced/internal compatibility and reducing event
  adapter exposure.

Latest local verification after host-facing review tool hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after host-facing review model adapter:

- `RunBuilder::review_model` now accepts a public `ReviewModel` adapter, and
  the reviewer facade owns conversion between host-facing `ReviewModelRequest`
  / `ReviewTranscriptItem` / `ReviewModelTurn` / `ReviewToolCall` values and
  runtime `SessionScope`, `ConversationItem`, and `ModelTurn` values.
  `ReviewModelRequest` gives hosts stable session id, role, objective,
  optional snapshot/profile context, turn number, transcript count, tool-result
  count, and deterministic tool-call id helpers. The public transcript view
  exposes prompt text, assistant tool calls, and tool results as strings/JSON
  plus ok/error/artifact metadata without requiring direct `ToolResultEnvelope`
  inspection. Public facade tests now supply mock, cancellation, multi-snapshot,
  and custom-tool models through `ReviewModel` and `RunBuilder::review_model`
  without constructing `ModelClient`, `ModelRouter`, `ModelTurn`,
  `ModelToolCall`, or `ConversationItem`. This moves the deep host-facing
  module interface to about 8.4/10 and testability through interfaces to about
  8.8/10. Remaining facade work is narrowing low-level re-exports for
  advanced/internal compatibility and reducing tool/event adapter exposure.

Latest local verification after host-facing review model hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after host-facing session spec:

- `RunSpec` now carries `ReviewSessionSpec` instead of raw runtime
  `SessionScope` values. The public session spec owns review-session identity,
  role, objective, optional snapshot targeting, optional model profile,
  capabilities, and budget, then lowers to `SessionScope` only inside
  `RunBuilder`. Hosts get constructor methods for read-only review sessions,
  snapshot targeting, model profile selection, explicit capability override,
  denying a tool, and trusted custom read-only tool grants. The legacy
  `ReviewRunJobV1` adapter converts personas into `ReviewSessionSpec`, keeping
  job compatibility while localizing runtime session construction in the
  reviewer facade. Public facade tests now cover happy path, denial events,
  cancellation events, multi-snapshot targeting, and custom tool execution
  without constructing raw runtime session scopes. This moves the deep
  host-facing module interface to about 8.2/10 and testability through
  interfaces to about 8.7/10. Remaining facade work is narrowing low-level
  re-exports and making model/tool/event adapters less tied to runtime structs.

Latest local verification after host-facing session spec hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after artifact object-store validation contract:

- Artifact persistence manifests are now serializable host artifacts, and
  `ArtifactObjectReader` gives hosts a read-side validation interface separate
  from private in-memory artifact state. `ArtifactPersistenceManifest` validates
  expected object count, expected bytes, declared object bytes, and stable
  content hashes through an adapter, reporting missing and stale object refs.
  `InMemoryArtifactObjectStore` and `LocalArtifactObjectStore` both implement
  the read contract; the local adapter derives object paths from the configured
  store root, artifact view, and content hash rather than trusting manifest
  paths for reads. Public facade tests prove in-memory validation, JSON
  round-trip of a local persistence manifest, validation through a reopened
  local adapter, stale object detection after byte mutation, and missing object
  detection after removal. This moves artifact/evidence retrieval to about
  9.6/10, testability through interfaces to about 8.6/10, and migration proof
  to about 9.6/10. Remaining store work is remote object-store adapters.

Latest local verification after artifact object-store validation hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after artifact object-store contract:

- `ArtifactObjectStore` is now a public persistence interface for redacted or
  raw artifact objects. `RunReport::persist_artifacts` uses the same
  `ArtifactExportPolicy` path as in-memory manifests, evidence traversal, and
  bundle export: capability scope, per-artifact allowlist, raw/redacted view,
  and retention are applied before any object is written. `ArtifactStoreObject`
  writes are validated against declared byte count and stable content hash, and
  persistence returns an `ArtifactPersistenceManifest` with object refs, view,
  bytes, content hashes, and the applied retention policy. Two adapters prove
  the seam: `InMemoryArtifactObjectStore` for tests/embedded hosts and
  `LocalArtifactObjectStore` for content-addressed filesystem persistence.
  Public facade tests prove scoped object persistence, rejected retention
  writes leave the store empty, local filesystem objects are rooted under the
  configured store, and persisted bytes match the export manifest. This moves
  artifact/evidence retrieval to about 9.5/10, testability through interfaces
  to about 8.5/10, and migration proof to about 9.5/10. Remaining store work is
  remote object-store adapters and cross-process object-store fixtures.

Latest local verification after artifact object-store hardening:

```bash
cargo fmt --check
cargo test -p muzen              # 90 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after remote artifact object-store adapter:

- `RemoteArtifactObjectStore` is now a host-facing adapter for content-addressed
  artifact object persistence through a `RemoteArtifactObjectClient`.
  `RemoteArtifactObjectStore` normalizes a non-file base URI, writes objects to
  view/hash-derived remote URIs, returns pathless object refs, and rejects
  forged remote URIs during validation reads. `InMemoryRemoteArtifactObjectClient`
  gives tests and embedded hosts a deterministic remote-client adapter without
  requiring a real cloud account. Public facade tests now prove remote
  persistence after policy filtering, serialized remote manifest validation,
  stale remote object detection, missing remote object detection, forged remote
  URI denial, and `file://` base rejection. This moves artifact/evidence
  retrieval to about 9.8/10. Remaining remote storage work is production
  cloud-client canaries.

Latest local verification after remote artifact object-store adapter:

```bash
cargo fmt --check
cargo test -p muzen              # 95 tests passed
cargo clippy -p muzen --all-targets --all-features -- -D warnings
cargo build --release -p muzen
```

Implemented state after event-context hardening:

- Public runtime event records now carry `RuntimeEventContext`, public run
  execution stamps all records with run id, per-shard runtime events inherit
  snapshot id, tool/search/artifact/denial events carry session/turn/tool-call
  context, and JSONL export persists that context. This moves stable
  observability to about 8/10 and the overall architecture to about 8.8/10
  internal, 8.4/10 reusable. Remaining observability work is versioned fixture
  compatibility and broader event-schema contract coverage.

Implemented state after event-schema fixture hardening:

- Runtime event JSONL now has a versioned fixture covering every
  `RuntimeEvent` variant, a public loader that rejects unsupported schema
  versions, and camelCase event payload fields matching the top-level
  host-facing record shape. This moved stable observability to about 9/10. At
  that checkpoint, remaining observability work was future schema migration
  guarantees and broader runtime-path event coverage.

Implemented state after external-provider policy-contract hardening:

- In-process and JSON-RPC custom providers now share the same provider-output
  module path for redaction, actual output byte accounting, artifact-write
  authority, artifact insertion, and runtime output/artifact size limits.
  JSON-RPC contract tests prove malicious or oversized external outputs are
  denied before storage or model-visible use. This moves provider-neutral tool
  execution to about 8.5/10 and the overall architecture to about 8.9/10
  internal, 8.5/10 reusable. Remaining provider work is network/host/scratch
  authority and broader real-provider contract gates.

After Phases 8 through 10:

- External extensibility, multi-snapshot runs, and measured optimization bring
  the architecture to 10/10.

Implemented 10/10 state after Phases 8 through 10:

- JSON-RPC tool providers, multi-snapshot/multi-repo shard execution, model
  cost metrics, measured optimization gates, and runnable Chat/Responses
  real-provider canary scaffolding are implemented with local proof gates
  passing. Credentialed real-provider execution remains intentionally opt-in
  because it depends on external keys and provider availability.

## First Three Issues To Create

1. Build public `muzen::reviewer` run facade.

   Acceptance:

   - Public test runs a mock review using only `muzen::reviewer`.
   - CLI job adapter calls the public facade.
   - Existing tests and clippy pass.

2. Make snapshot reads immutable or stale-detecting.

   Acceptance:

   - Worktree mutation after snapshot build cannot silently change read/search
     evidence.
   - Evidence artifacts include snapshot id, content hash, and scope.
   - Existing scoped search tests still pass.

3. Enforce `CapabilitySet` fully.

   Acceptance:

   - `ToolGrant.max_calls` is enforced.
   - `ToolEffects` are enforced.
   - Tool denial events and metrics are emitted.
   - Prompt exposure cannot grant denied tools.
