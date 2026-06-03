# Agent Memory Benchmark Summary

Generated: 2026-06-02T17:29:37.332Z

Memory metric: PSS when the OS exposes it, else USS, else RSS. This run used each result's available metric; macOS usually lacks PSS.

## Autonomous Agent Work Cases

Winner for the valid 10-unit autonomous work run is rust/work/10 at 15.64 MB peak RSS.

These rows are eligible for the agent comparison only when the model-driven session produced tool calls. Prompted sessions with zero tool calls are shown, but they are not valid filesystem-tool benchmarks.

| Case | Model | Exit | Valid | Peak tree MB | Root RSS baseline | Root RSS after work | Model calls | Tool calls/results | Review tools changed/diff/list/read/search | Findings | Tokens in/out/total | Cost |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | --- | --- | ---: | --- | ---: |
| rust/work/1 | gpt-4.1-nano | 0 | true | 13.59 | n/a | n/a | 4 | 4/4 | 0/1/0/1/1 | 1 | 2657/219/2876 | n/a |
| rust/work/10 | gpt-4.1-nano | 0 | true | 15.64 | n/a | n/a | 40 | 40/40 | 0/10/0/10/10 | 10 | 26736/2081/28817 | n/a |
| rust/work/30 | gpt-4.1-nano | 0 | true | 18.02 | n/a | n/a | 120 | 120/120 | 0/30/0/30/30 | 30 | 79588/5899/85487 | n/a |
| rust/work/50 | gpt-4.1-nano | 0 | true | 21.06 | n/a | n/a | 200 | 200/200 | 0/50/0/50/50 | 50 | 133250/10688/143938 | n/a |

## All Cases

| Case | Exit | Valid | Metric | Peak tree MB | Extra tree MB/additional agent | Root RSS baseline | Root RSS created | Root RSS settled | Model calls | Tool calls/results | Review tools changed/diff/list/read/search | Findings | Root create delta MB | Peak processes | Duration s | Tokens in/out/total |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | --- |
| rust/work/1 | 0 | true | rss_mb | 13.59 | n/a | n/a | n/a | n/a | 4 | 4/4 | 0/1/0/1/1 | 1 | n/a | 1 | 8.17 | 2657/219/2876 |
| rust/work/10 | 0 | true | rss_mb | 15.64 | 0.2278 | n/a | n/a | n/a | 40 | 40/40 | 0/10/0/10/10 | 10 | n/a | 1 | 7.65 | 26736/2081/28817 |
| rust/work/30 | 0 | true | rss_mb | 18.02 | 0.1528 | n/a | n/a | n/a | 120 | 120/120 | 0/30/0/30/30 | 30 | n/a | 1 | 6.55 | 79588/5899/85487 |
| rust/work/50 | 0 | true | rss_mb | 21.06 | 0.1524 | n/a | n/a | n/a | 200 | 200/200 | 0/50/0/50/50 | 50 | n/a | 1 | 7.95 | 133250/10688/143938 |
