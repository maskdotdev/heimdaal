# @muzen/sdk

TypeScript SDK scaffold for the `muzen.runner.v1` protocol.

This package intentionally talks to `muzen-runner` over newline-delimited
JSON-RPC instead of binding to Rust internals.

Current implemented SDK surface:

- Spawn or connect to `muzen-runner stdio`.
- Perform `runner.handshake`.
- Call `runner.check`.
- Call `runner.schema.export`.
- Build future review session descriptors.
- Start deterministic review runs with `run.start`.
- Read `run.status`, `run.result`, and terminal `run.cancel` status.
- Read/export stored artifacts with `artifact.read` and `artifact.export`.
- Read captured snapshot text with `snapshot.readText`.
- Register host-supplied model callbacks with `model.complete`.
- Register host-supplied read-only tools with `tool.execute`.
- Capture `event.review` notifications and expose them through an async
  iterable.
- Capture `event.runtime` notifications and expose them through an async
  iterable.

See `examples/typescript/custom-model-tool` for a callback model plus custom
tool run.
