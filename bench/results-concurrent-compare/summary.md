# Concurrent Runtime Comparison Summary

Generated: 2026-06-01T22:30:24.043Z

Workload: each session runs two mock model turns and four review actions: read_diff, read_file, search_text, and record_finding. The model is in-process and deterministic, so these results measure runtime/tool scheduling, repo IO, search dedupe, artifact reuse, and process memory without API spend.

Memory metric: process-tree peak RSS from memwatch.py. PSS/USS are included when the OS exposes them.

## Decision

At 100 sessions, the concurrent runtime completed the same 400 tool actions as the synchronous baseline, reduced full-repo search scans from 100 to 1, and ran 20.86x faster in the release benchmark. The captured process-tree peak was 14.02 MB RSS for the compare process, including both sync and concurrent phases in one executable run.

## Results

| Sessions | Exit | Valid sync/concurrent | Peak RSS MB | Peak PSS MB | Peak USS MB | Sync ms | Concurrent ms | Speedup | Sync scans | Concurrent scans | Scan reduction | Dedupe waiters | Artifacts sync/concurrent | Artifact MB sync/concurrent | Peak processes | Duration s |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: |
| 50 | 0 | true/true | 9.77 | n/a | n/a | 77 | 7 | 11.00 | 50 | 1 | 50.00 | 49 | 104/3 | 1.09/0.03 | 1 | 0.75 |
| 100 | 0 | true/true | 14.02 | n/a | n/a | 146 | 7 | 20.86 | 100 | 1 | 100.00 | 99 | 203/3 | 2.18/0.03 | 1 | 0.31 |
