# Muzen SDK And Runner Implementation Plan

Generated: 2026-06-04

## Goal

Make Muzen feel like a language-native review-agent toolkit while keeping Rust
as the single execution kernel.

The core principle is that SDKs talk to a stable runner protocol. They do not
bind directly to private Rust internals.

```txt
@muzen/sdk / muzen-py
  -> user-facing API
  -> process management
  -> typed protocol client
  -> model callbacks
  -> tool callbacks
  -> event streams
  -> artifact helpers

muzen-runner
  -> Rust binary
  -> owns snapshots, sessions, capabilities, scheduling, cache, metrics
  -> speaks JSON-RPC over stdio first
  -> optional HTTP mode later
```

## Phase 1: Protocol Contract

Define `muzen.runner.v1` as the durable SDK boundary.

Core SDK-to-runner requests:

- `runner.handshake`
- `run.start`
- `run.cancel`
- `run.status`
- `run.result`
- `artifact.read`
- `artifact.export`
- `snapshot.readText`

Runner-to-SDK callbacks:

- `model.complete`
- `tool.execute`

Runner notifications:

- `event.review`
- `event.runtime`
- `run.finished`
- `run.failed`

Contract requirements:

- Every message has `protocolVersion`, `requestId`, and `runId` where
  applicable.
- Errors are typed: `invalid_input`, `tool_denied`, `model_failed`,
  `cancelled`, `timeout`, and `protocol_error`.
- Protocol fixtures live in the repo and round-trip through Rust, TypeScript,
  and Python tests.
- JSON Schema is the source of truth for wire contracts.

## Phase 2: `muzen-runner` Binary

Add a Rust binary that wraps the current `muzen::reviewer` facade.

Responsibilities:

- Parse JSON-RPC over stdio.
- Build `RunSpec`, `SnapshotSpec`, `ReviewSessionSpec`, capabilities, and
  limits.
- Stream `ReviewEventRecord`.
- Call back into the SDK process for model and tool execution.
- Return `RunReport` summary, findings, metrics, and artifact refs.
- Enforce cancellation and process cleanup.

CLI modes:

```bash
muzen-runner stdio
muzen-runner check
muzen-runner schema export
```

The protocol must not expose private `runtime::*` modules.

## Phase 3: TypeScript SDK First

Recommended package layout:

```txt
sdk/typescript/
  packages/muzen-sdk/
  packages/runner-darwin-arm64/
  packages/runner-darwin-x64/
  packages/runner-linux-x64/
  packages/runner-win-x64/
```

Target public API:

```ts
const client = await Muzen.create();

const run = await client.review({
  repo: ".",
  sessions: [
    session("security", "Find security regressions"),
    session("tests", "Find missing test coverage"),
  ],
  model: openai({ model: "gpt-5" }),
  tools: [
    tool("jira_context", schema, async (ctx) => ({ data: await loadJira(ctx.args) })),
  ],
});

for await (const event of run.events()) {
  console.log(event);
}

const report = await run.result();
const artifacts = await report.exportArtifacts();
```

TypeScript SDK responsibilities:

- Spawn and supervise `muzen-runner`.
- Validate inputs with Zod or TypeBox.
- Provide an async iterable event stream.
- Provide ergonomic builders for sessions, capabilities, tools, and artifact
  policies.
- Support custom model providers and tool handlers.
- Bundle platform-specific runner binaries.

## Phase 4: Python SDK

Recommended package layout:

```txt
sdk/python/muzen/
  client.py
  runner.py
  models.py
  tools.py
  events.py
  artifacts.py
```

Target public API:

```py
client = await muzen.Client.create()

run = await client.review(
    repo=".",
    sessions=[
        muzen.session("security", "Find security regressions"),
        muzen.session("tests", "Find missing test coverage"),
    ],
    model=openai_model("gpt-5"),
    tools=[jira_context_tool],
)

async for event in run.events():
    print(event)

report = await run.result()
```

Python SDK responsibilities:

- `asyncio` process supervision.
- Pydantic protocol models.
- Decorator-based tools.
- Async model callback interface.
- Artifact export and read helpers.
- Notebook-friendly defaults.

## Phase 5: Examples And Developer Experience

Ship examples before advanced features.

Minimum examples:

```txt
examples/typescript/basic-review
examples/typescript/custom-tool
examples/typescript/custom-model
examples/typescript/github-app
examples/python/basic-review
examples/python/custom-tool
examples/python/notebook-review
```

Required docs:

- Review a repo in 10 lines.
- Register a custom tool.
- Use your own model provider.
- Stream events.
- Export findings and artifacts.
- Run multiple personas.
- Understand the capability and permission model.

## Phase 6: Hardening

Acceptance tests:

- TypeScript and Python can run the same golden review fixture.
- SDK custom tools receive bounded arguments and return bounded outputs.
- Model callback cancellation works.
- Runner crash produces a typed SDK error.
- Event order is stable.
- Artifact export works cross-language.
- Protocol fixture compatibility tests fail on accidental schema drift.

Operational requirements:

- Prebuilt binaries for macOS arm64/x64, Linux x64, and Windows x64.
- Checksums for downloaded binaries.
- `muzen-runner check` for diagnostics.
- Clear semver policy: SDK minor versions can support multiple runner patch
  versions.

## Non-Goals For V1

Avoid these initially:

- Rust FFI bindings.
- Hosted SaaS API.
- Full MCP server surface.
- Arbitrary untrusted plugin sandboxing.
- Multi-language codegen perfection.
- Deep GitHub/GitLab app features inside SDKs.

## Milestones

1. Protocol spec and fixtures.
2. `muzen-runner stdio` with mock model and tool callbacks.
3. TypeScript SDK basic review with event stream.
4. TypeScript custom tools and artifact export.
5. Python SDK parity.
6. Cross-language golden tests.
7. Binary packaging and docs.

The most important first deliverable is a tiny TypeScript example that feels
obvious and works end to end. That will prove whether the primitive boundary is
actually developer-friendly.
