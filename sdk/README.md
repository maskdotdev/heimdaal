# Muzen SDKs

This directory contains language SDKs for the `muzen.runner.v1` protocol.

The SDKs are intentionally sidecar-based:

```txt
TypeScript / Python SDK
  -> starts muzen-runner stdio
  -> sends JSON-RPC requests
  -> receives runner events and callbacks

muzen-runner
  -> owns Rust review runtime
  -> owns snapshots, capabilities, scheduling, cache, metrics, artifacts
```

Implemented in this slice:

- Rust `muzen-runner stdio`, `check`, and `schema export`.
- Rust `run.start`, `run.status`, `run.result`, and terminal `run.cancel`
  status with deterministic review execution over the reviewer facade.
- Rust `artifact.read`, `artifact.export`, and `snapshot.readText` backed by
  stored reviewer artifacts and snapshot readers.
- `event.review`, `run.finished`, and `run.failed` notifications.
- Protocol fixtures for handshake and schema metadata.
- TypeScript SDK handshake/check/schema/review/resource client.
- Python SDK handshake/check/schema/review/resource client.

Reserved for the next slice:

- model callbacks
- tool callbacks
- advanced runtime event streaming
