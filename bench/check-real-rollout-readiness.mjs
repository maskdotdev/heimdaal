import { readdir, stat } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import path from "node:path";

const benchDir = path.resolve(new URL(".", import.meta.url).pathname);
const root = path.resolve(benchDir, "..");
const node = process.execPath;
const rawInputs = process.argv.slice(2).filter((arg) => !arg.startsWith("--"));
const inputs =
  rawInputs.length > 0
    ? rawInputs.map((input) => path.resolve(process.cwd(), input))
    : [new URL("./results-real-run-event-parity-normal-50-default-runtime", import.meta.url).pathname];
const sourceKind = argValue("--source") || "benchmark";
const minRuns = optionalPositiveInteger("--min-runs");
const minSessions =
  optionalPositiveInteger("--min-success-sessions") ??
  optionalPositiveInteger("--min-sessions") ??
  sessionsMinimum(argValue("--sessions")) ??
  1;
const auditInputs = await auditCandidateInputs(inputs);
const result = spawnSync(
  node,
  [
    path.join(benchDir, "check-real-release-window-audit.mjs"),
    ...auditInputs,
    `--source=${sourceKind}`,
    `--min-runs=${minRuns ?? Math.max(1, auditInputs.length)}`,
    `--min-sessions=${minSessions}`,
  ],
  {
    cwd: root,
    env: process.env,
    encoding: "utf8",
    maxBuffer: 20 * 1024 * 1024,
  },
);

const audit = parseJson(result.stdout);
const failures = [...(audit?.failures || [])];
if (!audit) {
  failures.push(`release audit output was not JSON: ${truncate(result.stderr || result.stdout)}`);
}
if (result.status !== 0 && failures.length === 0) {
  failures.push(`release audit exited ${result.status}: ${truncate(result.stderr || result.stdout)}`);
}

const rolloutReady = result.status === 0 && audit?.releaseWindowAuditReady === true;
const defaultRuntimeReady =
  rolloutReady && audit?.totals?.defaultConcurrentRuns === audit?.totals?.runs;

console.log(
  JSON.stringify(
    {
      sourceKind: audit?.sourceKind ?? sourceKind,
      minRuns: audit?.minRuns ?? minRuns ?? Math.max(1, auditInputs.length),
      minSessions: audit?.minSessions ?? minSessions,
      auditInputs,
      totals: audit?.totals ?? null,
      rows: audit?.rows ?? [],
      rolloutReady,
      defaultRuntimeReady,
      deletionReady: audit?.deletionReady === true,
      deletionBlocker: audit?.deletionBlocker ?? null,
      failures,
    },
    null,
    2,
  ),
);
if (!rolloutReady) process.exit(1);

async function auditCandidateInputs(paths) {
  const output = [];
  for (const input of paths) {
    const info = await stat(input).catch(() => null);
    if (!info) {
      output.push(input);
      continue;
    }
    if (info.isFile()) {
      output.push(input);
      continue;
    }
    if (!info.isDirectory()) continue;
    const files = (await readdir(input, { withFileTypes: true }))
      .filter((entry) => entry.isFile())
      .filter((entry) => entry.name.endsWith(".json") || entry.name.endsWith(".jsonl"))
      .filter((entry) => !entry.name.startsWith("job_"))
      .filter((entry) => entry.name !== "release_window_audit.json")
      .map((entry) => path.join(input, entry.name))
      .sort();
    const concurrent = files.filter((file) => path.basename(file).startsWith("concurrent_"));
    output.push(...(concurrent.length > 0 ? concurrent : files));
  }
  return [...new Set(output)].sort();
}

function parseJson(text) {
  const trimmed = String(text || "").trim();
  if (!trimmed) return null;
  try {
    return JSON.parse(trimmed);
  } catch {
    return null;
  }
}

function sessionsMinimum(raw) {
  if (!raw) return null;
  const values = raw
    .split(",")
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isInteger(value) && value > 0);
  if (values.length === 0) return null;
  return values.reduce((sum, value) => sum + value, 0);
}

function optionalPositiveInteger(name) {
  const raw = argValue(name);
  if (raw == null) return null;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

function argValue(name) {
  const prefix = `${name}=`;
  return process.argv.find((arg) => arg.startsWith(prefix))?.slice(prefix.length);
}

function truncate(value) {
  const text = String(value || "").trim();
  return text.length > 500 ? `${text.slice(0, 500)}...` : text;
}
