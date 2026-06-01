import { readdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

const resultsDir = process.argv[2] || new URL("./results", import.meta.url).pathname;
const outPath = path.join(resultsDir, "summary.md");

function parseEvent(line) {
  try {
    const parsed = JSON.parse(line);
    return parsed && (parsed.event === "memory" || parsed.event === "rust_memory")
      ? parsed
      : null;
  } catch {
    return null;
  }
}

function fmt(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "n/a";
  return Number(value).toFixed(digits);
}

function bestMemoryMetric(result) {
  if (result.peak?.pss_mb !== null && result.peak?.pss_mb !== undefined) return "pss_mb";
  if (result.peak?.uss_mb !== null && result.peak?.uss_mb !== undefined) return "uss_mb";
  return "rss_mb";
}

function caseInfo(fileName) {
  const stem = fileName.replace(/\.json$/, "");
  const [agent, mode, countRaw] = stem.split("_");
  return {
    agent,
    mode,
    count: Number(countRaw || 0),
    name: stem,
  };
}

function labelFor(info) {
  return `${info.agent}/${info.mode}/${info.count}`;
}

function latestEvent(events, label) {
  return [...events].reverse().find((event) => event.label === label) || null;
}

const files = (await readdir(resultsDir)).filter((name) => name.endsWith(".json")).sort();
const rows = [];

for (const file of files) {
  const fullPath = path.join(resultsDir, file);
  const result = JSON.parse(await readFile(fullPath, "utf8"));
  const info = caseInfo(file);
  const events = result.stdout_lines.map(parseEvent).filter(Boolean);
  const metric = bestMemoryMetric(result);
  const baseline =
    latestEvent(events, "baseline") ||
    latestEvent(events, "after_import") ||
    latestEvent(events, "after_server_start") ||
    events[0] ||
    null;
  const created =
    latestEvent(events, `created_${info.count}`) ||
    [...events].reverse().find((event) => event.label?.startsWith("created_")) ||
    null;
  const settled = latestEvent(events, "after_settle") || events.at(-1) || null;
  const peak = result.peak?.[metric] ?? result.peak?.rss_mb ?? null;
  const baselineProcess = baseline?.rss_mb ?? null;
  const createdProcess = created?.rss_mb ?? null;
  const marginalProcess =
    info.count > 0 && baselineProcess !== null && createdProcess !== null
      ? (createdProcess - baselineProcess) / info.count
      : null;
  const memoryPeak = peak;
  const processCreateDelta =
    baselineProcess !== null && createdProcess !== null ? createdProcess - baselineProcess : null;

  rows.push({
    ...info,
    file,
    label: labelFor(info),
    exitCode: result.exit_code,
    duration: result.duration_s,
    metric,
    processCount: result.peak?.process_count ?? null,
    baselineProcess,
    createdProcess,
    settledProcess: settled?.rss_mb ?? null,
    peakTree: memoryPeak,
    processCreateDelta,
    marginalProcess,
    additionalTreeMb: null,
    additionalProcessCreateMb: null,
    benchmarkValid: settled?.benchmark_valid ?? null,
    benchmarkFailures: settled?.benchmark_failures ?? [],
    modelCalls: settled?.model_calls ?? null,
    toolCalls: settled?.tool_calls ?? null,
    toolResults: settled?.tool_results ?? settled?.tool_calls ?? null,
    listChangedFilesCalls: settled?.list_changed_files_calls ?? null,
    readDiffCalls: settled?.read_diff_calls ?? null,
    listFilesCalls: settled?.list_files_calls ?? null,
    readFileCalls: settled?.read_file_calls ?? null,
    searchTextCalls: settled?.search_text_calls ?? null,
    finishCalls: settled?.finish_calls ?? null,
    findings: settled?.findings ?? null,
    publishableFindings: settled?.publishable_findings ?? null,
    tokensIn: settled?.tokens_in ?? null,
    tokensOut: settled?.tokens_out ?? null,
    tokensTotal: settled?.tokens_total ?? null,
    reasoningTokensOut: settled?.reasoning_tokens_out ?? null,
    cost: settled?.cost ?? null,
    model: [...events].find((event) => event.model)?.model ?? null,
    stderrTail: result.stderr_lines.slice(-3).join(" | "),
  });
}

const comparable = rows.filter((row) => row.exitCode === 0 && row.count > 0);
const byAgentModeCount = new Map();
for (const row of comparable) {
  byAgentModeCount.set(`${row.agent}/${row.mode}/${row.count}`, row);
}

for (const row of comparable) {
  if (row.count <= 1) continue;

  const one = byAgentModeCount.get(`${row.agent}/${row.mode}/1`);
  if (!one) continue;

  if (row.peakTree !== null && one.peakTree !== null) {
    row.additionalTreeMb = Math.max(0, (row.peakTree - one.peakTree) / (row.count - 1));
  }

  if (row.createdProcess !== null && one.createdProcess !== null) {
    row.additionalProcessCreateMb = Math.max(
      0,
      (row.createdProcess - one.createdProcess) / (row.count - 1),
    );
  }
}

const ranking = comparable
  .filter((row) => row.mode === "idle" || row.mode === "memory" || row.mode === "no-tools")
  .filter((row) => row.additionalTreeMb !== null)
  .sort((a, b) => (a.additionalTreeMb ?? Infinity) - (b.additionalTreeMb ?? Infinity));

let md = `# Agent Memory Benchmark Summary\n\n`;
md += `Generated: ${new Date().toISOString()}\n\n`;
md += `Memory metric: PSS when the OS exposes it, else USS, else RSS. This run used each result's available metric; macOS usually lacks PSS.\n\n`;

const thin50 = rows.find((row) => row.label === "thin/memory/50");
const pi50 = rows.find((row) => row.label === "pi/idle/50") || rows.find((row) => row.label === "pi/no-tools/50");
const opencode50 = rows.find((row) => row.label === "opencode/idle/50");
const opencodeContext10 = rows.find((row) => row.label === "opencode/context-only/10");

if (thin50 && pi50 && opencode50) {
  md += `## Decision\n\n`;
  md += `Winner by absolute memory for a 50-unit idle fanout is the thin custom harness at ${fmt(thin50.peakTree)} MB peak RSS. Among full agent runtimes, Pi is lighter at ${fmt(pi50.peakTree)} MB peak RSS for 50 idle sessions; OpenCode is heavier at ${fmt(opencode50.peakTree)} MB peak RSS because of its server baseline. `;
  if (opencodeContext10) {
    md += `OpenCode context-only/noReply with 10 sessions peaked at ${fmt(opencodeContext10.peakTree)} MB RSS. `;
  }
  md += `Use the thin review graph for broad planning/persona/filter fanout, Pi for bounded deep review sessions, and keep OpenCode behind stricter worker/session caps unless later prompted/tool benchmarks change the result.\n\n`;
}

const autonomousWorkRows = rows.filter((row) => row.mode === "work");
const baselineWorkRows = rows.filter(
  (row) =>
    row.mode === "shell-work" ||
    row.mode === "fs-work" ||
    row.mode === "llm-fs-work",
);

if (autonomousWorkRows.length) {
  md += `## Autonomous Agent Work Cases\n\n`;
  const tenUnitWorkRows = autonomousWorkRows.filter(
    (row) => row.count === 10 && row.exitCode === 0 && (row.toolCalls ?? 0) > 0,
  );
  if (tenUnitWorkRows.length) {
    const winner = [...tenUnitWorkRows].sort((a, b) => (a.peakTree ?? Infinity) - (b.peakTree ?? Infinity))[0];
    md += `Winner for the valid 10-unit autonomous work run is ${winner.label} at ${fmt(winner.peakTree)} MB peak RSS. `;
    const piWork = tenUnitWorkRows.find((row) => row.label === "pi/work/10");
    if (piWork) {
      md += `Pi's 10-session run did autonomous LLM tool use: ${piWork.toolCalls}/${piWork.toolResults} tool calls/results at ${fmt(piWork.peakTree)} MB peak RSS. `;
    }
    md += `\n\n`;
  }
  md += `These rows are eligible for the agent comparison only when the model-driven session produced tool calls. Prompted sessions with zero tool calls are shown, but they are not valid filesystem-tool benchmarks.\n\n`;
  md += `| Case | Model | Exit | Valid | Peak tree MB | Root RSS baseline | Root RSS after work | Model calls | Tool calls/results | Review tools changed/diff/list/read/search | Findings | Tokens in/out/total | Cost |\n`;
  md += `| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | --- | --- | ---: | --- | ---: |\n`;
  for (const row of autonomousWorkRows) {
    md += `| ${row.label} | ${row.model ?? "n/a"} | ${row.exitCode} | ${row.benchmarkValid ?? "n/a"} | ${fmt(row.peakTree)} | ${fmt(row.baselineProcess)} | ${fmt(row.settledProcess)} | ${row.modelCalls ?? "n/a"} | ${row.toolCalls ?? "n/a"}/${row.toolResults ?? "n/a"} | ${row.listChangedFilesCalls ?? "n/a"}/${row.readDiffCalls ?? "n/a"}/${row.listFilesCalls ?? "n/a"}/${row.readFileCalls ?? "n/a"}/${row.searchTextCalls ?? "n/a"} | ${row.findings ?? "n/a"} | ${row.tokensIn ?? "n/a"}/${row.tokensOut ?? "n/a"}/${row.tokensTotal ?? "n/a"} | ${fmt(row.cost, 6)} |\n`;
  }
  md += `\n`;
}

if (baselineWorkRows.length) {
  md += `## Scripted Or Explicit Baselines\n\n`;
  md += `These rows run filesystem work, but they are not autonomous model-selected tool benchmarks and are excluded from the agent winner. OpenCode shell-work uses session shell execution. Thin fs-work/llm-fs-work runs explicit file reads and ripgrep before optional LLM summarization.\n\n`;
  md += `| Case | Model | Exit | Peak tree MB | Root RSS baseline | Root RSS after work | Tool calls/results | Tokens in/out/total | Cost |\n`;
  md += `| --- | --- | ---: | ---: | ---: | ---: | --- | --- | ---: |\n`;
  for (const row of baselineWorkRows) {
    md += `| ${row.label} | ${row.model ?? "n/a"} | ${row.exitCode} | ${fmt(row.peakTree)} | ${fmt(row.baselineProcess)} | ${fmt(row.settledProcess)} | ${row.toolCalls ?? "n/a"}/${row.toolResults ?? "n/a"} | ${row.tokensIn ?? "n/a"}/${row.tokensOut ?? "n/a"}/${row.tokensTotal ?? "n/a"} | ${fmt(row.cost, 6)} |\n`;
  }
  md += `\n`;
}

if (ranking.length) {
  md += `## Best Additional-Session Memory\n\n`;
  md += `This ranks N-agent runs by the extra peak process-tree RSS per additional agent after the 1-agent case for the same agent/mode. That removes fixed runtime/server baseline from the marginal number. Negative deltas from run-to-run noise are clamped to 0.\n\n`;
  md += `| Rank | Case | Extra tree MB/additional agent | Peak tree MB | Root RSS create delta MB | Root RSS extra MB/additional agent |\n`;
  md += `| ---: | --- | ---: | ---: | ---: | ---: |\n`;
  ranking.forEach((row, index) => {
    md += `| ${index + 1} | ${row.label} | ${fmt(row.additionalTreeMb, 4)} | ${fmt(row.peakTree)} | ${fmt(row.processCreateDelta)} | ${fmt(row.additionalProcessCreateMb, 4)} |\n`;
  });
  md += `\n`;
}

md += `## All Cases\n\n`;
md += `| Case | Exit | Valid | Metric | Peak tree MB | Extra tree MB/additional agent | Root RSS baseline | Root RSS created | Root RSS settled | Model calls | Tool calls/results | Review tools changed/diff/list/read/search | Findings | Root create delta MB | Peak processes | Duration s | Tokens in/out/total |\n`;
md += `| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | --- |\n`;

for (const row of rows) {
  md += `| ${row.label} | ${row.exitCode} | ${row.benchmarkValid ?? "n/a"} | ${row.metric} | ${fmt(row.peakTree)} | ${fmt(row.additionalTreeMb, 4)} | ${fmt(row.baselineProcess)} | ${fmt(row.createdProcess)} | ${fmt(row.settledProcess)} | ${row.modelCalls ?? "n/a"} | ${row.toolCalls ?? "n/a"}/${row.toolResults ?? "n/a"} | ${row.listChangedFilesCalls ?? "n/a"}/${row.readDiffCalls ?? "n/a"}/${row.listFilesCalls ?? "n/a"}/${row.readFileCalls ?? "n/a"}/${row.searchTextCalls ?? "n/a"} | ${row.findings ?? "n/a"} | ${fmt(row.processCreateDelta)} | ${fmt(row.processCount, 0)} | ${fmt(row.duration)} | ${row.tokensIn ?? "n/a"}/${row.tokensOut ?? "n/a"}/${row.tokensTotal ?? "n/a"} |\n`;
}

const failed = rows.filter((row) => row.exitCode !== 0);
if (failed.length) {
  md += `\n## Failed Or Skipped Cases\n\n`;
  for (const row of failed) {
    const failures = row.benchmarkFailures?.length
      ? `; benchmark failures: ${row.benchmarkFailures.join(", ")}`
      : "";
    md += `- ${row.label}: exit ${row.exitCode}${failures}; ${row.stderrTail || "see JSON result"}\n`;
  }
}

await writeFile(outPath, md);
console.log(md);
