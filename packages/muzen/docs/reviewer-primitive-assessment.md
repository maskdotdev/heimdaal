# Reviewer Primitive Assessment

Generated: 2026-06-01

## Verdict

The concurrent runtime is a strong V1 primitive for building repo-review agents, especially for host engines that need many read-only review sessions over the same materialized repository. It is not yet a complete external SDK boundary: model routing, per-session credentials, per-session cwd, telemetry export, and custom-tool accounting still need to be promoted from internal implementation details into stable host-facing contracts.

Current fit: good for embedding inside Heimdaal or another Rust host that can compile in custom tool handlers.

Not yet sufficient as-is for: third-party engines that need dynamic plugin loading, non-Rust tool providers, per-session BYOK routing, or stable public crate APIs.

## What Is Strong

- Immutable repo snapshot: one manifest and bounded file access shared by all sessions.
- Concurrent execution: bounded session fanout, bounded tool fanout per session, bounded read/search pools.
- Search efficiency: duplicate `search_text` calls singleflight into one underlying repo scan.
- Canonical transcript: model tool calls and tool results preserve deterministic ordering even when tools execute concurrently.
- Defensive result envelopes: every tool result carries status, tool id, snapshot id, cache state, limits, artifact id, data, or typed error.
- Extension direction: model/runtime protocol now uses dynamic `ToolId`, and `ToolRegistry` can register host-supplied custom tools with schemas and handlers.
- Per-session custom capability control: custom tools are only callable when explicitly allowlisted on the session.
- Redaction boundary: built-in and custom tool outputs pass through redaction before they become model-visible data.
- Benchmark proof: 50/100-session compare runs prove tool activity, memory capture, dedupe, and speedup versus the synchronous path.

## Current Proof

Source: `bench/results-concurrent-compare/summary.md`

| Sessions | Peak RSS | Sync ms | Concurrent ms | Speedup | Sync scans | Concurrent scans |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 50 | 9.77 MB | 77 | 7 | 11.00x | 50 | 1 |
| 100 | 14.02 MB | 146 | 7 | 20.86x | 100 | 1 |

The proof workload uses a deterministic in-process mock model, so it measures runtime/tool behavior without API cost. It does not prove provider latency behavior or real model tool-call quality.

## Extension Model

The registry splits tool identity from built-in tool classification:

- `ToolId`: protocol-level id used in model calls, transcript items, schemas, and result envelopes.
- `ToolName`: built-in classification used for existing review counters and fixed permissions.
- `ToolRegistry`: schema and handler registry for built-in and custom tools.
- `CustomToolHandler`: host-supplied async handler for custom tools.
- `allowed_custom_tools`: session-level allowlist for non-built-in tools.

This is the right direction for other engines because a host can expose different tools per repo, branch, persona, or policy without changing the model loop.

## Gaps Before This Is A General Primitive

1. Stable host API

   Most runtime modules are still `pub(crate)`. A real primitive needs a library API with stable types, constructors, and error contracts.

2. Per-session model routing and BYOK

   `JobRuntime` currently holds one `Arc<dyn ConcurrentModelClient>`. Other review engines will need a model router keyed by session/persona so each session can use a different provider profile, base URL, API key, and budget.

3. Per-session cwd

   The current concurrent session spec does not carry a cwd. Repo access is snapshot-relative. That is good for safety, but host engines need a cwd/policy field for tools whose behavior is intentionally scoped to a subdirectory.

4. Custom tool accounting

   Built-in tools increment `ToolCounts`; custom tools currently appear in result envelopes but not in a dedicated per-tool metrics map. Host engines will need per-tool latency, success/error counts, cache hits, and byte totals.

5. External tool providers

   The registry supports in-process Rust handlers. That is enough for Heimdaal internals, but not enough for third-party extension ecosystems. Add an out-of-process adapter later, likely JSON-RPC/MCP-style with strict timeouts and bounded payloads.

6. Provider schemas

   Chat Completions schemas are generated from the registry. A full primitive should expose the same registry through every supported provider adapter, including Responses-compatible adapters.

7. Cancellation and timeout reporting

   Cancellation exists, but host-facing event streams are not fully wired. Engines need reliable per-session and per-tool timeout/cancel events for debugging and scheduling.

8. Artifact retrieval API

   Artifacts are stored and counted, but there is no stable retrieval/export interface for downstream reviewer engines to publish evidence, render comments, or store audit trails.

9. Policy composition

   Built-in `ToolMask` plus custom allowlist works, but policy should eventually become a single typed capability contract that supports built-ins, custom tools, cwd scope, repo scope, and network/scratch permissions.

## Recommended Next Slice

Build the host-facing primitive boundary:

1. Convert `muzen` into a reusable library plus CLI binary.
2. Add `ModelRouter` so each session resolves its own provider profile and credential.
3. Add `SessionScope` with cwd, allowed built-ins, allowed custom tools, and repo-root policy.
4. Add `ToolMetrics` as a dynamic map keyed by `ToolId`.
5. Add artifact retrieval/export APIs.
6. Keep the existing compare benchmark as the regression gate.

## Quality Bar

This should not be considered a general reviewer primitive until these are true:

- A host can run one repo with many personas and custom tools without changing runtime code.
- A host can run many repos/branches by constructing independent snapshots and runtimes.
- A host can BYOK per session or per run.
- A host can scope each session to a cwd.
- Every tool call, including custom tools, has metrics and typed errors.
- Benchmark proof still passes at 50 and 100 sessions after host-facing APIs are added.
