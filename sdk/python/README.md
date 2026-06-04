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
- Register host-supplied model callbacks with `model.complete`.
- Register host-supplied read-only tools with `tool.execute`.
- Capture `event.review` notifications and expose them through an async
  iterator.
- Capture `event.runtime` notifications and expose them through an async
  iterator.

See `examples/python/custom_model_tool.py` for a callback model plus custom
tool run.
