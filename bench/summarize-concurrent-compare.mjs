import { readdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

const resultsDir =
  process.argv[2] || new URL("./results-concurrent-compare", import.meta.url).pathname;
const outPath = path.join(resultsDir, "summary.md");

const files = (await readdir(resultsDir))
  .filter((name) => /^compare_\d+\.json$/.test(name))
  .sort((a, b) => sessionCount(a) - sessionCount(b));
const rows = [];

for (const file of files) {
  const fullPath = path.join(resultsDir, file);
  const result = JSON.parse(await readFile(fullPath, "utf8"));
  const report = parseComparisonReport(result.stdout_lines);
  rows.push({
    file,
    sessions: sessionCount(file),
    exitCode: result.exit_code,
    durationS: result.duration_s,
    peakRssMb: result.peak?.rss_mb ?? null,
    peakPssMb: result.peak?.pss_mb ?? null,
    peakUssMb: result.peak?.uss_mb ?? null,
    peakProcesses: result.peak?.process_count ?? null,
    report,
    stderrTail: result.stderr_lines?.slice(-3).join(" | ") || "",
  });
}

let md = `# Concurrent Runtime Comparison Summary\n\n`;
md += `Generated: ${new Date().toISOString()}\n\n`;
md += `Workload: each session runs two mock model turns and four review actions: read_diff, read_file, search_text, and record_finding. The model is in-process and deterministic, so these results measure runtime/tool scheduling, repo IO, search dedupe, artifact reuse, and process memory without API spend.\n\n`;
md += `Memory metric: process-tree peak RSS from memwatch.py. PSS/USS are included when the OS exposes them.\n\n`;

const validRows = rows.filter((row) => row.exitCode === 0 && row.report);
if (validRows.length) {
  const largest = validRows.at(-1);
  const report = largest.report;
  md += `## Decision\n\n`;
  md += `At ${largest.sessions} sessions, the concurrent runtime completed the same ${report.concurrent.toolCalls} tool actions as the serial baseline, reduced full-repo search scans from ${report.sync.counters.searchScans} to ${report.concurrent.counters.searchScans}, and ran ${fmt(report.speedup, 2)}x faster in the release benchmark. `;
  md += `The captured process-tree peak was ${fmt(largest.peakRssMb, 2)} MB RSS for the compare process, including both sync and concurrent phases in one executable run.\n\n`;
}

md += `## Results\n\n`;
md += `| Sessions | Exit | Valid sync/concurrent | Peak RSS MB | Peak PSS MB | Peak USS MB | Sync ms | Concurrent ms | Speedup | Sync scans | Concurrent scans | Scan reduction | Dedupe waiters | Artifacts sync/concurrent | Artifact MB sync/concurrent | Peak processes | Duration s |\n`;
md += `| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: |\n`;

for (const row of rows) {
  const report = row.report;
  md += `| ${row.sessions} | ${row.exitCode} | ${report ? `${report.sync.benchmarkValid}/${report.concurrent.benchmarkValid}` : "n/a"} | ${fmt(row.peakRssMb)} | ${fmt(row.peakPssMb)} | ${fmt(row.peakUssMb)} | ${report?.sync.elapsedMs ?? "n/a"} | ${report?.concurrent.elapsedMs ?? "n/a"} | ${fmt(report?.speedup)} | ${report?.sync.counters.searchScans ?? "n/a"} | ${report?.concurrent.counters.searchScans ?? "n/a"} | ${fmt(report?.searchScanReduction)} | ${report?.concurrent.counters.searchDedupeWaiters ?? "n/a"} | ${report ? `${report.sync.artifacts}/${report.concurrent.artifacts}` : "n/a"} | ${report ? `${fmt(bytesToMiB(report.sync.artifactBytes))}/${fmt(bytesToMiB(report.concurrent.artifactBytes))}` : "n/a"} | ${fmt(row.peakProcesses, 0)} | ${fmt(row.durationS)} |\n`;
}

const failed = rows.filter((row) => row.exitCode !== 0 || !row.report);
if (failed.length) {
  md += `\n## Failed Cases\n\n`;
  for (const row of failed) {
    md += `- compare_${row.sessions}: exit ${row.exitCode}; ${row.stderrTail || "see JSON result"}\n`;
  }
}

await writeFile(outPath, md);
console.log(md);

function parseComparisonReport(lines) {
  const text = (lines || []).join("\n").trim();
  if (!text) return null;

  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");
  if (start === -1 || end === -1 || end <= start) return null;

  return JSON.parse(text.slice(start, end + 1), camelizeReviver);
}

function camelizeReviver(key, value) {
  const aliases = {
    completed_sessions: "completedSessions",
    model_calls: "modelCalls",
    tool_calls: "toolCalls",
    tool_counts: "toolCounts",
    read_diff: "readDiff",
    read_file: "readFile",
    search_text: "searchText",
    record_finding: "recordFinding",
    elapsed_ms: "elapsedMs",
    input_tokens: "inputTokens",
    output_tokens: "outputTokens",
    total_tokens: "totalTokens",
    artifact_bytes: "artifactBytes",
    search_scans: "searchScans",
    search_dedupe_waiters: "searchDedupeWaiters",
    search_cache_hits: "searchCacheHits",
    read_cache_hits: "readCacheHits",
    read_file_reads: "readFileReads",
    tool_errors: "toolErrors",
    artifact_cache_hits: "artifactCacheHits",
    benchmark_valid: "benchmarkValid",
    benchmark_failures: "benchmarkFailures",
    search_scan_reduction: "searchScanReduction",
  };
  if (!value || typeof value !== "object" || Array.isArray(value)) return value;

  for (const [snake, camel] of Object.entries(aliases)) {
    if (Object.hasOwn(value, snake)) {
      value[camel] = value[snake];
    }
  }
  return value;
}

function sessionCount(fileName) {
  return Number(fileName.match(/^compare_(\d+)\.json$/)?.[1] || 0);
}

function bytesToMiB(bytes) {
  if (bytes === null || bytes === undefined) return null;
  return bytes / 1024 / 1024;
}

function fmt(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "n/a";
  return Number(value).toFixed(digits);
}
