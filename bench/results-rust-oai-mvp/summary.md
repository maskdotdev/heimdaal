# Agent Memory Benchmark Summary

Generated: 2026-06-01T18:14:42.323Z

Memory metric: PSS when the OS exposes it, else USS, else RSS. This run used each result's available metric; macOS usually lacks PSS.

## Autonomous Agent Work Cases

Winner for the valid 10-unit autonomous work run is rust/work/10 at 14.94 MB peak RSS. 

These rows are eligible for the agent comparison only when the model-driven session produced tool calls. Prompted sessions with zero tool calls are shown, but they are not valid filesystem-tool benchmarks.

| Case | Model | Exit | Valid | Peak tree MB | Root RSS baseline | Root RSS after work | Model calls | Tool calls/results | Review tools changed/diff/list/read/search | Findings | Tokens in/out/total | Cost |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | --- | --- | ---: | --- | ---: |
| rust/work/1 | gpt-4.1-nano | 0 | true | 12.92 | n/a | n/a | 4 | 4/4 | 0/1/0/1/1 | 1 | 2627/296/2923 | n/a |
| rust/work/10 | gpt-4.1-nano | 0 | true | 14.94 | n/a | n/a | 40 | 40/40 | 0/10/0/10/10 | 10 | 26521/2322/28843 | n/a |

## All Cases

| Case | Exit | Valid | Metric | Peak tree MB | Extra tree MB/additional agent | Root RSS baseline | Root RSS created | Root RSS settled | Model calls | Tool calls/results | Review tools changed/diff/list/read/search | Findings | Root create delta MB | Peak processes | Duration s | Tokens in/out/total |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | --- |
| rust/work/1 | 0 | true | rss_mb | 12.92 | n/a | n/a | n/a | n/a | 4 | 4/4 | 0/1/0/1/1 | 1 | n/a | 1 | 5.20 | 2627/296/2923 |
| rust/work/10 | 0 | true | rss_mb | 14.94 | 0.2244 | n/a | n/a | n/a | 40 | 40/40 | 0/10/0/10/10 | 10 | n/a | 1 | 6.47 | 26521/2322/28843 |
