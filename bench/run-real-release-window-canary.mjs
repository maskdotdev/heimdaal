import { access, mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { constants } from "node:fs";
import { spawn } from "node:child_process";
import path from "node:path";

const root = path.resolve(new URL("..", import.meta.url).pathname);
const benchDir = path.join(root, "bench");
const binary = path.join(root, "target", "release", "muzen");
const node = process.execPath;
const resultsDir = process.env.RELEASE_WINDOW_RESULTS_DIR
  ? path.resolve(benchDir, process.env.RELEASE_WINDOW_RESULTS_DIR)
  : path.join(benchDir, "results-real-release-window-canary");
const auditOutPath = process.env.RELEASE_WINDOW_AUDIT_OUT
  ? path.resolve(process.cwd(), process.env.RELEASE_WINDOW_AUDIT_OUT)
  : path.join(resultsDir, "release_window_audit.json");
const sourceKind = parseSourceKind(process.env.RELEASE_WINDOW_SOURCE || "benchmark");
const interval = process.env.RELEASE_WINDOW_INTERVAL || "0.05";
const dryRun = parseBoolean(process.env.RELEASE_WINDOW_DRY_RUN || "");
const auditOnly = parseBoolean(process.env.RELEASE_WINDOW_AUDIT_ONLY || "");
const minRuns = optionalPositiveInteger(process.env.RELEASE_WINDOW_MIN_RUNS, "RELEASE_WINDOW_MIN_RUNS");
const explicitMinSessions = optionalPositiveInteger(
  process.env.RELEASE_WINDOW_MIN_SESSIONS,
  "RELEASE_WINDOW_MIN_SESSIONS",
);
const python = await resolvePython();
const jobPaths = await resolveJobPaths(process.env.RELEASE_WINDOW_JOBS || "");

await mkdir(resultsDir, { recursive: true });

if (!auditOnly) {
  if (jobPaths.length === 0) {
    throw new Error(
      "RELEASE_WINDOW_JOBS must point to one or more ReviewRunJobV1 JSON files or directories",
    );
  }
  if (dryRun) {
    console.log(
      JSON.stringify(
        {
          dryRun,
          sourceKind,
          resultsDir,
          jobs: jobPaths,
          commands: jobPaths.map((jobPath, index) => ({
            out: artifactPath(index, jobPath),
            cmd: [binary, "run", "--job", jobPath],
          })),
        },
        null,
        2,
      ),
    );
    process.exit(0);
  }

  await run(["cargo", "test", "--manifest-path", path.join(root, "Cargo.toml")], root);
  await run(["cargo", "build", "--release", "--manifest-path", path.join(root, "Cargo.toml")], root);

  for (const [index, jobPath] of jobPaths.entries()) {
    const outPath = artifactPath(index, jobPath);
    console.error(`\n==> canary_${index + 1}_${path.basename(jobPath, ".json")}`);
    await rm(outPath, { force: true });
    await run(
      [
        python,
        path.join(benchDir, "memwatch.py"),
        `--interval=${interval}`,
        "--out",
        outPath,
        "--",
        binary,
        "run",
        "--job",
        jobPath,
      ],
      root,
    );
  }
}

const artifactPaths = await collectArtifactPaths(resultsDir);
const minSessions =
  explicitMinSessions ?? (await sumJobSessions(jobPaths).catch(() => null)) ?? 1;
const auditInputs = artifactPaths.length > 0 ? artifactPaths : [resultsDir];
const audit = await capture(
  [
    node,
    path.join(benchDir, "check-real-release-window-audit.mjs"),
    ...auditInputs,
    `--source=${sourceKind}`,
    `--min-runs=${minRuns ?? Math.max(1, artifactPaths.length)}`,
    `--min-sessions=${minSessions}`,
  ],
  root,
);
await writeFile(auditOutPath, audit.stdout);
process.stdout.write(audit.stdout);
if (audit.stderr) process.stderr.write(audit.stderr);

function artifactPath(index, jobPath) {
  const stem = path.basename(jobPath, ".json").replace(/[^A-Za-z0-9_.-]/g, "_");
  return path.join(resultsDir, `canary_run_${String(index + 1).padStart(3, "0")}_${stem}.json`);
}

async function resolveJobPaths(raw) {
  const entries = raw
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
  const paths = [];
  for (const entry of entries) {
    const resolved = path.resolve(process.cwd(), entry);
    const stats = await stat(resolved).catch(() => null);
    if (!stats) throw new Error(`missing RELEASE_WINDOW_JOBS path: ${entry}`);
    if (stats.isFile()) {
      paths.push(resolved);
      continue;
    }
    if (stats.isDirectory()) {
      for (const child of await readdir(resolved, { withFileTypes: true })) {
        if (child.isFile() && child.name.endsWith(".json") && child.name.startsWith("job_")) {
          paths.push(path.join(resolved, child.name));
        }
      }
    }
  }
  return [...new Set(paths)].sort();
}

async function collectArtifactPaths(dir) {
  const files = [];
  for (const child of await readdir(dir, { withFileTypes: true })) {
    if (!child.isFile()) continue;
    if (!child.name.endsWith(".json") && !child.name.endsWith(".jsonl")) continue;
    if (child.name.startsWith("job_")) continue;
    if (child.name === "release_window_audit.json") continue;
    files.push(path.join(dir, child.name));
  }
  return files.sort();
}

async function sumJobSessions(paths) {
  let total = 0;
  for (const jobPath of paths) {
    const text = await readFile(jobPath, "utf8");
    const job = JSON.parse(text);
    total += job.personas?.length ?? 0;
  }
  return total || null;
}

async function resolvePython() {
  if (process.env.PYTHON) return process.env.PYTHON;

  const venvPython = path.join(benchDir, ".venv", "bin", "python");
  try {
    await access(venvPython, constants.X_OK);
    return venvPython;
  } catch {
    return "python3";
  }
}

function optionalPositiveInteger(raw, name) {
  if (raw == null || String(raw).trim() === "") return null;
  const value = Number(String(raw).trim());
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

function parseBoolean(raw) {
  const value = String(raw || "").trim().toLowerCase();
  if (value === "") return false;
  if (["1", "true", "yes", "on"].includes(value)) return true;
  if (["0", "false", "no", "off"].includes(value)) return false;
  throw new Error(`invalid boolean value: ${raw}`);
}

function parseSourceKind(raw) {
  const value = raw.trim().toLowerCase();
  if (["release", "canary", "benchmark"].includes(value)) return value;
  throw new Error("RELEASE_WINDOW_SOURCE must be release, canary, or benchmark");
}

function run(cmd, cwd) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd[0], cmd.slice(1), {
      cwd,
      env: process.env,
      stdio: "inherit",
    });
    child.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${cmd[0]} exited ${code}`));
    });
  });
}

function capture(cmd, cwd) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd[0], cmd.slice(1), {
      cwd,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("close", (code) => {
      if (code === 0) {
        resolve({ stdout, stderr });
      } else {
        reject(new Error(`${cmd[0]} exited ${code}: ${stderr || stdout}`));
      }
    });
  });
}
