# Sync Runtime Drop Readiness

Generated: 2026-06-03

## Decision

Sync deletion is complete against the accepted bar: 50 total sessions with
`maxActiveSessions=10`.

The concurrent runtime is now the only runtime path. `muzen run` and
`muzen bench` dispatch only to concurrent, the explicit runtime selector is gone,
and the sync runtime/model/tool modules have been removed from the local code
path.

The 100-session stress case is diagnostic only. It is not a deletion blocker.

## Current Code State

- Removed sync modules:
  - `packages/muzen/src/model.rs`
  - `packages/muzen/src/runtime.rs`
  - `packages/muzen/src/tools.rs`
- Removed sync-only contract surface from `packages/muzen/src/contracts.rs`.
- Moved shared job/event/credential helpers into concurrent-owned or shared
  modules.
- `muzen run --runtime sync` is rejected by the CLI as an unexpected argument.
- Synthetic concurrent comparison now uses a serial concurrent-owned baseline,
  not the old sync tool registry.

## Release Evidence

Release-window canary artifact:

```text
bench/results-real-release-window-canary-50-default-runtime/canary_run_001_job_50.json
```

Audit output:

```text
bench/results-real-release-window-canary-50-default-runtime/release_window_audit.json
```

Audit result:

| Field | Value |
| --- | --- |
| Source | `canary` |
| Runs | 1 |
| Sessions | 50 |
| Completed sessions | 50 |
| Default concurrent runs | 1 |
| Sync runs | 0 |
| Runtime flag present | false |
| Runtime | `concurrent` |
| Outcome | `completed_with_findings` |
| Publishability | `publishable` |
| Findings | 40 |
| Publishable findings | 40 |
| Duration | 32.622s |
| Peak RSS | 27.19 MB |
| Total tokens | 275,804 |
| Deletion ready | true |

## Benchmark Read

At the accepted 50-session, max-active-10 rollout shape, concurrent is faster
wall-clock and roughly flat on RSS, but still uses more tokens than sync.

| Artifact | Duration ratio | RSS ratio | Token ratio | Result |
| --- | ---: | ---: | ---: | --- |
| normal default-runtime | 0.518 | 1.043 | 1.828 | pass |
| normal token-trim | 0.737 | 0.977 | 1.733 | pass |
| finding-required token-trim | 0.901 | 1.045 | 1.790 | pass |

Token usage is the main follow-up optimization target. The accepted envelope is
currently less than 2x sync tokens at 50 sessions, and the refreshed artifacts
pass that bar. Broader workload/model coverage and 100-session runs are useful
diagnostics, not blockers for sync deletion.

## Proof Gates

Required final gates:

```bash
cargo test -p muzen
cargo build --release -p muzen
node --check bench/run-real-release-window-canary.mjs
node --check bench/check-real-release-window-audit.mjs
node --check bench/check-sync-removal-preflight.mjs
node bench/check-real-release-window-audit.mjs bench/results-real-release-window-canary-50-default-runtime/canary_run_001_job_50.json --source=canary --min-runs=1 --min-sessions=50
node bench/check-sync-removal-preflight.mjs --release-audit=bench/results-real-release-window-canary-50-default-runtime/release_window_audit.json
```

Latest local result: all gates passed. The preflight reported
`deletionReady: true`, `sync-surface-removed` blocker count 0, and no blockers.

## Follow-Up Work

- Token efficiency: reduce repeated prompt/tool-schema context and redundant
  evidence context.
- Broader confidence: rerun canaries on more repo shapes and model profiles.
- Diagnostics only: repeat 100-session stress after token work if useful.
