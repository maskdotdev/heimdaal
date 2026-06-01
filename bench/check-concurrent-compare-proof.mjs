import { readFile } from "node:fs/promises";
import path from "node:path";

const resultsDir =
  process.argv[2] || new URL("./results-concurrent-compare", import.meta.url).pathname;
const sessionsArg = process.argv.find((arg) => arg.startsWith("--sessions="));
const expectedSessions = (sessionsArg?.slice("--sessions=".length) || "50,100")
  .split(",")
  .map((value) => Number(value.trim()))
  .filter((value) => Number.isInteger(value) && value > 0);

const rawKey = process.env.OPENAI_API_KEY || process.env.OAI_API_KEY || "";
const apiKeyPattern = /sk-[A-Za-z0-9_-]{20,}/;
const failures = [];
const proofRows = [];

for (const count of expectedSessions) {
  const label = `compare_${count}`;
  const file = path.join(resultsDir, `${label}.json`);
  const text = await readFile(file, "utf8");
  const result = JSON.parse(text);
  const report = parseComparisonReport(result.stdout_lines);

  if (result.exit_code !== 0) failures.push(`${label}: exit ${result.exit_code}`);
  if (!report) failures.push(`${label}: missing comparison report`);
  if (!result.peak?.rss_mb || result.peak.rss_mb <= 0) failures.push(`${label}: missing peak RSS`);
  if (rawKey && text.includes(rawKey)) failures.push(`${label}: raw API key present`);
  if (apiKeyPattern.test(text)) failures.push(`${label}: API-key-shaped token present`);
  if (/Authorization:\s*Bearer/i.test(text)) failures.push(`${label}: authorization header present`);

  if (report) {
    assertEqual(failures, label, "sessions", report.sessions, count);
    checkRun(failures, label, "sync", report.sync, count);
    checkRun(failures, label, "concurrent", report.concurrent, count);

    assertEqual(failures, label, "sync search scans", field(report.sync.counters, "searchScans"), count);
    if ((field(report.concurrent.counters, "searchScans") ?? Infinity) > 1) {
      failures.push(`${label}: concurrent performed more than one duplicate full-repo scan`);
    }
    if ((field(report.concurrent.counters, "searchDedupeWaiters") ?? 0) < count - 1) {
      failures.push(`${label}: concurrent search did not prove duplicate waiter collapse`);
    }
    if ((field(report, "searchScanReduction") ?? 0) < count) {
      failures.push(`${label}: search scan reduction below ${count}x`);
    }
    if ((report.speedup ?? 0) <= 1) failures.push(`${label}: concurrent path not faster`);
    if ((report.concurrent.artifacts ?? Infinity) >= (report.sync.artifacts ?? 0)) {
      failures.push(`${label}: concurrent artifact reuse did not reduce artifact count`);
    }
  }

  proofRows.push({
    case: label,
    peakRssMb: result.peak?.rss_mb ?? null,
    peakPssMb: result.peak?.pss_mb ?? null,
    peakUssMb: result.peak?.uss_mb ?? null,
    syncElapsedMs: field(report?.sync, "elapsedMs") ?? null,
    concurrentElapsedMs: field(report?.concurrent, "elapsedMs") ?? null,
    speedup: report?.speedup ?? null,
    syncSearchScans: field(report?.sync.counters, "searchScans") ?? null,
    concurrentSearchScans: field(report?.concurrent.counters, "searchScans") ?? null,
    searchScanReduction: field(report, "searchScanReduction") ?? null,
    concurrentSearchWaiters: field(report?.concurrent.counters, "searchDedupeWaiters") ?? null,
    concurrentToolCalls: field(report?.concurrent, "toolCalls") ?? null,
    valid: {
      sync: field(report?.sync, "benchmarkValid") ?? null,
      concurrent: field(report?.concurrent, "benchmarkValid") ?? null,
    },
  });
}

console.log(JSON.stringify({ proofRows, failures }, null, 2));
if (failures.length) process.exit(1);

function checkRun(failures, label, runtime, run, sessions) {
  if (!run) {
    failures.push(`${label}: missing ${runtime} run`);
    return;
  }
  if (!field(run, "benchmarkValid")) {
    failures.push(
      `${label}: ${runtime} benchmark_valid=false (${field(run, "benchmarkFailures")?.join(", ")})`,
    );
  }
  assertEqual(failures, label, `${runtime} completed sessions`, field(run, "completedSessions"), sessions);
  assertEqual(failures, label, `${runtime} model calls`, field(run, "modelCalls"), sessions * 2);
  assertEqual(failures, label, `${runtime} tool calls`, field(run, "toolCalls"), sessions * 4);
  assertEqual(failures, label, `${runtime} read_diff`, field(field(run, "toolCounts"), "readDiff"), sessions);
  assertEqual(failures, label, `${runtime} read_file`, field(field(run, "toolCounts"), "readFile"), sessions);
  assertEqual(failures, label, `${runtime} search_text`, field(field(run, "toolCounts"), "searchText"), sessions);
  assertEqual(
    failures,
    label,
    `${runtime} record_finding`,
    field(field(run, "toolCounts"), "recordFinding"),
    sessions,
  );
}

function assertEqual(failures, label, name, actual, expected) {
  if (actual !== expected) failures.push(`${label}: ${name} expected ${expected}, got ${actual}`);
}

function field(object, camel) {
  if (!object) return undefined;
  if (Object.hasOwn(object, camel)) return object[camel];
  const snake = camel.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);
  return object[snake];
}

function parseComparisonReport(lines) {
  const text = (lines || []).join("\n").trim();
  if (!text) return null;

  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");
  if (start === -1 || end === -1 || end <= start) return null;

  return JSON.parse(text.slice(start, end + 1));
}
