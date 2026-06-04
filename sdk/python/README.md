# muzen

Python SDK scaffold for the `muzen.runner.v1` protocol.

This package talks to `muzen-runner` over newline-delimited JSON-RPC using
`asyncio`. It does not bind to Rust internals.

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
- Capture `event.review` notifications and expose them through an async
  iterator.

Full host-supplied model and tool callbacks are reserved for the next runner
slice.
