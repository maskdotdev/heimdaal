import { mkdir } from "node:fs/promises";
import { spawn } from "node:child_process";
import path from "node:path";

const root = path.resolve(new URL("..", import.meta.url).pathname);
const benchDir = path.join(root, "bench");
const resultsDir = path.join(benchDir, "results-rust-oai-mvp");
const python = process.env.PYTHON || path.join(benchDir, ".venv", "bin", "python");
const node = process.execPath;
const binary = path.join(root, "rust", "target", "release", "heimdaal-agent-core");
const model = process.env.RUST_MVP_MODEL || "gpt-5-nano";
const sessions = (process.env.RUST_MVP_SESSIONS || "1,10")
  .split(",")
  .map((value) => Number(value.trim()))
  .filter((value) => Number.isInteger(value) && value > 0);
const maxTurns = process.env.RUST_MVP_MAX_TURNS || "5";
const maxToolCalls = process.env.RUST_MVP_MAX_TOOL_CALLS || "4";
const holdMs = process.env.RUST_MVP_HOLD_MS || "1000";
const maxOutputTokens = process.env.RUST_MVP_MAX_OUTPUT_TOKENS || "128";

await mkdir(resultsDir, { recursive: true });

await run(["cargo", "test", "--manifest-path", path.join(root, "rust", "Cargo.toml")], root);
await run(["cargo", "build", "--release", "--manifest-path", path.join(root, "rust", "Cargo.toml")], root);

for (const count of sessions) {
  const name = `rust_work_${count}`;
  const outPath = path.join(resultsDir, `${name}.json`);
  console.error(`\n==> ${name}`);
  await run(
    [
      python,
      path.join(benchDir, "memwatch.py"),
      "--interval=0.05",
      "--out",
      outPath,
      "--",
      binary,
      "bench",
      "--repo",
      root,
      "--sessions",
      String(count),
      "--max-active",
      String(count),
      "--max-turns",
      maxTurns,
      "--max-tool-calls",
      maxToolCalls,
      "--hold-ms",
      holdMs,
      "--model",
      model,
      "--max-output-tokens",
      maxOutputTokens,
    ],
    root,
  );
}

await run([node, path.join(benchDir, "summarize-results.mjs"), resultsDir], root);

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
