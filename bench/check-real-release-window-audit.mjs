import { readFile, readdir, stat } from "node:fs/promises";
import path from "node:path";

const inputPaths = process.argv.slice(2).filter((arg) => !arg.startsWith("--"));
if (inputPaths.length === 0) {
  throw new Error("usage: node check-real-release-window-audit.mjs <artifact-file-or-dir>...");
}

const minRuns = optionalPositiveInteger("--min-runs") ?? 1;
const minSessions = optionalPositiveInteger("--min-sessions") ?? 1;
const sourceKind = parseSourceKind(argValue("--source") || "unknown");
const requirePublishable = !flag("--allow-diagnostic");
const requireNoErrors = !flag("--allow-errors");
const rawKey = process.env.OPENAI_API_KEY || process.env.OAI_API_KEY || "";
const apiKeyPattern = /sk-[A-Za-z0-9_-]{20,}/;
const failures = [];
const rows = [];

const files = await collectFiles(inputPaths);
if (files.length === 0) {
  failures.push("audit: no artifact files found");
}

for (const file of files) {
  const text = await readFile(file, "utf8");
  checkNoSecrets(file, text);
  const artifact = parseArtifact(file, text);
  if (!artifact) continue;
  const run = summarizeArtifact(artifact);
  checkRun(run);
  rows.push(run);
}

const totalRuns = rows.length;
const totalSessions = rows.reduce((sum, row) => sum + (row.sessions ?? 0), 0);
const totalCompletedSessions = rows.reduce(
  (sum, row) => sum + (row.completedSessions ?? 0),
  0,
);
const fallbackRuns = rows.filter((row) => row.syncFallbackUsed === true || row.runtime === "sync");
const defaultConcurrentRuns = rows.filter(
  (row) =>
    row.runtime === "concurrent" &&
    row.runtimeRole === "candidate" &&
    row.syncFallbackUsed === false &&
    row.commandRuntimeFlagPresent === false,
);

if (totalRuns < minRuns) {
  failures.push(`audit: runs ${totalRuns} below minimum ${minRuns}`);
}
if (totalCompletedSessions < minSessions) {
  failures.push(
    `audit: completed sessions ${totalCompletedSessions} below minimum ${minSessions}`,
  );
}
if (defaultConcurrentRuns.length !== totalRuns) {
  failures.push(
    `audit: default concurrent runs ${defaultConcurrentRuns.length}/${totalRuns}`,
  );
}
if (fallbackRuns.length > 0) {
  failures.push(
    `audit: sync fallback used in ${fallbackRuns.length} run(s): ${fallbackRuns
      .map((row) => row.label)
      .join(", ")}`,
  );
}

const releaseWindowAuditReady = failures.length === 0;
const deletionEvidenceSource = sourceKind === "release" || sourceKind === "canary";
const deletionReady = releaseWindowAuditReady && deletionEvidenceSource;
const deletionBlocker = deletionReady
  ? null
  : releaseWindowAuditReady
    ? "audit source must be release or canary evidence"
    : "release-window audit failed";

console.log(
  JSON.stringify(
    {
      sourceKind,
      minRuns,
      minSessions,
      requirePublishable,
      requireNoErrors,
      totals: {
        runs: totalRuns,
        sessions: totalSessions,
        completedSessions: totalCompletedSessions,
        defaultConcurrentRuns: defaultConcurrentRuns.length,
        fallbackRuns: fallbackRuns.length,
      },
      releaseWindowAuditReady,
      deletionReady,
      deletionBlocker,
      rows,
      failures,
    },
    null,
    2,
  ),
);
if (failures.length) process.exit(1);

async function collectFiles(paths) {
  const result = [];
  for (const input of paths) {
    const resolved = path.resolve(process.cwd(), input);
    const info = await stat(resolved).catch(() => null);
    if (!info) {
      failures.push(`audit: missing path ${input}`);
      continue;
    }
    if (info.isFile()) {
      result.push(resolved);
      continue;
    }
    if (!info.isDirectory()) continue;
    for (const entry of await readdir(resolved, { withFileTypes: true })) {
      if (!entry.isFile()) continue;
      if (!entry.name.endsWith(".json") && !entry.name.endsWith(".jsonl")) continue;
      if (entry.name.startsWith("job_")) continue;
      if (entry.name === "release_window_audit.json") continue;
      result.push(path.join(resolved, entry.name));
    }
  }
  return [...new Set(result)].sort();
}

function parseArtifact(file, text) {
  const trimmed = text.trim();
  if (!trimmed) {
    failures.push(`${file}: empty artifact`);
    return null;
  }
  if (file.endsWith(".jsonl")) {
    return {
      file,
      cmd: [],
      exitCode: null,
      durationS: null,
      peakRssMb: null,
      events: parseEvents(file, trimmed.split(/\r?\n/)),
    };
  }

  let parsed;
  try {
    parsed = JSON.parse(trimmed);
  } catch (error) {
    const lines = trimmed.split(/\r?\n/).filter((line) => line.trim());
    if (lines.length > 1 && lines.every((line) => line.trim().startsWith("{"))) {
      return {
        file,
        cmd: [],
        exitCode: null,
        durationS: null,
        peakRssMb: null,
        events: parseEvents(file, lines),
      };
    }
    failures.push(`${file}: invalid JSON: ${error.message}`);
    return null;
  }

  if (Array.isArray(parsed)) {
    return { file, cmd: [], exitCode: null, durationS: null, peakRssMb: null, events: parsed };
  }
  if (parsed.eventType) {
    return { file, cmd: [], exitCode: null, durationS: null, peakRssMb: null, events: [parsed] };
  }
  if (Array.isArray(parsed.events)) {
    return {
      file,
      cmd: parsed.cmd || [],
      exitCode: parsed.exit_code ?? parsed.exitCode ?? null,
      durationS: parsed.duration_s ?? parsed.durationS ?? null,
      peakRssMb: parsed.peak?.rss_mb ?? parsed.peakRssMb ?? null,
      events: parsed.events,
    };
  }
  if (Array.isArray(parsed.stdout_lines)) {
    return {
      file,
      cmd: parsed.cmd || [],
      exitCode: parsed.exit_code ?? null,
      durationS: parsed.duration_s ?? null,
      peakRssMb: parsed.peak?.rss_mb ?? null,
      events: parseEvents(file, parsed.stdout_lines),
    };
  }

  failures.push(`${file}: unsupported artifact shape`);
  return null;
}

function parseEvents(label, lines) {
  const events = [];
  for (const [index, line] of lines.entries()) {
    if (typeof line === "object" && line !== null) {
      events.push(line);
      continue;
    }
    if (!String(line).trim()) continue;
    try {
      events.push(JSON.parse(line));
    } catch (error) {
      failures.push(`${label}: event line ${index + 1} is not JSON: ${error.message}`);
    }
  }
  return events;
}

function summarizeArtifact(artifact) {
  const started = artifact.events.find((event) => event.eventType === "run_started");
  const finished = artifact.events.findLast((event) => event.eventType === "run_finished");
  const payload = finished?.payload || {};
  const startPayload = started?.payload || {};
  const label = path.relative(process.cwd(), artifact.file);
  const commandRuntimeIndex = artifact.cmd.indexOf("--runtime");
  return {
    label,
    exit: artifact.exitCode,
    durationS: artifact.durationS,
    peakRssMb: artifact.peakRssMb,
    commandRuntimeFlagPresent: commandRuntimeIndex !== -1,
    commandRuntime:
      commandRuntimeIndex === -1 ? null : artifact.cmd[commandRuntimeIndex + 1] ?? null,
    runStarted: Boolean(started),
    runFinished: Boolean(finished),
    runtime: payload.runtime ?? startPayload.runtime ?? null,
    runtimeRole: payload.runtimeRole ?? startPayload.runtimeRole ?? null,
    syncFallbackUsed:
      payload.syncFallbackUsed ?? startPayload.syncFallbackUsed ?? null,
    sessions: payload.sessions ?? startPayload.sessions ?? null,
    completedSessions: payload.completedSessions ?? null,
    outcome: payload.outcome ?? null,
    publishability: payload.publishability ?? null,
    findings: payload.findings?.length ?? 0,
    publishableFindings: publishableFindings(payload.findings || []),
    modelCalls: payload.modelCalls ?? 0,
    toolCalls: totalToolCalls(payload.toolCounts),
    totalTokens: payload.tokens?.totalTokens ?? 0,
    eventCounts: eventCounts(artifact.events),
  };
}

function checkRun(run) {
  if (!run.runStarted) failures.push(`${run.label}: missing run_started`);
  if (!run.runFinished) failures.push(`${run.label}: missing run_finished`);
  if (run.exit != null && run.exit !== 0) failures.push(`${run.label}: exit ${run.exit}`);
  if (run.commandRuntimeFlagPresent) {
    failures.push(`${run.label}: command should use default runtime, got --runtime`);
  }
  if (run.runtime !== "concurrent") {
    failures.push(`${run.label}: runtime expected concurrent, got ${run.runtime}`);
  }
  if (run.runtimeRole !== "candidate") {
    failures.push(`${run.label}: runtimeRole expected candidate, got ${run.runtimeRole}`);
  }
  if (run.syncFallbackUsed !== false) {
    failures.push(`${run.label}: syncFallbackUsed expected false, got ${run.syncFallbackUsed}`);
  }
  if (!Number.isInteger(run.sessions) || run.sessions <= 0) {
    failures.push(`${run.label}: missing positive sessions`);
  }
  if (run.completedSessions !== run.sessions) {
    failures.push(
      `${run.label}: completed sessions ${run.completedSessions}/${run.sessions}`,
    );
  }
  if (requirePublishable && run.publishability !== "publishable") {
    failures.push(`${run.label}: publishability expected publishable, got ${run.publishability}`);
  }
  if (requireNoErrors && (run.eventCounts.error ?? 0) > 0) {
    failures.push(`${run.label}: emitted ${run.eventCounts.error} error events`);
  }
}

function publishableFindings(findings) {
  return findings.filter(
    (finding) =>
      finding.validationStatus === "validated" && finding.publishability === "publishable",
  ).length;
}

function totalToolCalls(toolCounts) {
  return Object.values(toolCounts || {}).reduce(
    (sum, value) => sum + (Number.isFinite(value) ? value : 0),
    0,
  );
}

function eventCounts(events) {
  const counts = {};
  for (const event of events) {
    counts[event.eventType] = (counts[event.eventType] || 0) + 1;
  }
  return counts;
}

function checkNoSecrets(label, text) {
  if (rawKey && text.includes(rawKey)) failures.push(`${label}: raw API key present`);
  if (apiKeyPattern.test(text)) failures.push(`${label}: API-key-shaped token present`);
  if (/Authorization:\s*Bearer/i.test(text)) failures.push(`${label}: authorization header present`);
}

function parseSourceKind(raw) {
  const value = raw.trim().toLowerCase();
  if (["release", "canary", "benchmark", "unknown"].includes(value)) return value;
  throw new Error("--source must be release, canary, benchmark, or unknown");
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

function flag(name) {
  return process.argv.includes(name);
}
