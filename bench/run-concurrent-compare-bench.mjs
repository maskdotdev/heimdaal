import { access, mkdir } from "node:fs/promises";
import { constants } from "node:fs";
import { spawn } from "node:child_process";
import path from "node:path";

const root = path.resolve(new URL("..", import.meta.url).pathname);
const benchDir = path.join(root, "bench");
const resultsDir = path.join(benchDir, "results-concurrent-compare");
const node = process.execPath;
const binary = path.join(root, "target", "release", "muzen");
const repo = path.resolve(process.env.CONCURRENT_COMPARE_REPO || root);
const query = process.env.CONCURRENT_COMPARE_QUERY || "use|fn|struct";
const interval = process.env.CONCURRENT_COMPARE_INTERVAL || "0.02";
const python = await resolvePython();
const sessions = parseSessions(process.env.CONCURRENT_COMPARE_SESSIONS || "50,100");

await mkdir(resultsDir, { recursive: true });

await run(["cargo", "test", "--manifest-path", path.join(root, "Cargo.toml")], root);
await run(["cargo", "build", "--release", "--manifest-path", path.join(root, "Cargo.toml")], root);

for (const count of sessions) {
  const name = `compare_${count}`;
  const outPath = path.join(resultsDir, `${name}.json`);
  console.error(`\n==> ${name}`);
  await run(
    [
      python,
      path.join(benchDir, "memwatch.py"),
      `--interval=${interval}`,
      "--out",
      outPath,
      "--",
      binary,
      "compare-concurrent",
      "--repo",
      repo,
      "--sessions",
      String(count),
      "--query",
      query,
    ],
    root,
  );
}

await run([node, path.join(benchDir, "summarize-concurrent-compare.mjs"), resultsDir], root);
await run(
  [
    node,
    path.join(benchDir, "check-concurrent-compare-proof.mjs"),
    resultsDir,
    `--sessions=${sessions.join(",")}`,
  ],
  root,
);

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

function parseSessions(raw) {
  const parsed = raw
    .split(",")
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isInteger(value) && value > 0);
  if (parsed.length === 0) {
    throw new Error("CONCURRENT_COMPARE_SESSIONS must contain at least one positive integer");
  }
  return [...new Set(parsed)].sort((a, b) => a - b);
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
