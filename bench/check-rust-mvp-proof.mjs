import { readFile } from "node:fs/promises";
import path from "node:path";

const resultsDir = process.argv[2] || new URL("./results-rust-oai-mvp", import.meta.url).pathname;
const sessionsArg = process.argv.find((arg) => arg.startsWith("--sessions="));
const expectedSessions = (sessionsArg?.slice("--sessions=".length) || "1,10")
  .split(",")
  .map((value) => Number(value.trim()))
  .filter((value) => Number.isInteger(value) && value > 0);

const rawKey = process.env.OPENAI_API_KEY || process.env.OAI_API_KEY || "";
const apiKeyPattern = /sk-[A-Za-z0-9_-]{20,}/;
const failures = [];
const proofRows = [];

for (const count of expectedSessions) {
  const file = path.join(resultsDir, `rust_work_${count}.json`);
  const text = await readFile(file, "utf8");
  const result = JSON.parse(text);
  const event = result.stdout_lines
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter((value) => value?.event === "rust_memory")
    .at(-1);

  const label = `rust_work_${count}`;
  if (result.exit_code !== 0) failures.push(`${label}: exit ${result.exit_code}`);
  if (!event) failures.push(`${label}: missing rust_memory event`);
  if (event) {
    if (event.sessions !== count) failures.push(`${label}: expected ${count} sessions`);
    if (event.completed_sessions !== count) failures.push(`${label}: incomplete sessions`);
    if (!event.benchmark_valid) {
      failures.push(`${label}: benchmark_valid=false (${event.benchmark_failures?.join(", ")})`);
    }
    if ((event.model_calls ?? 0) < count) failures.push(`${label}: too few model calls`);
    if ((event.tool_calls ?? 0) < count * 3) failures.push(`${label}: too few tool calls`);
    if ((event.read_diff_calls ?? 0) < count) failures.push(`${label}: read_diff not exercised`);
    if ((event.read_file_calls ?? 0) < count) failures.push(`${label}: read_file not exercised`);
    if ((event.search_text_calls ?? 0) < count) failures.push(`${label}: search_text not exercised`);
    if ((event.findings ?? 0) === 0 && (event.finish_calls ?? 0) === 0) {
      failures.push(`${label}: no finding or finish rationale`);
    }
  }

  if (!result.peak?.rss_mb || result.peak.rss_mb <= 0) failures.push(`${label}: missing peak RSS`);
  if (rawKey && text.includes(rawKey)) failures.push(`${label}: raw API key present`);
  if (apiKeyPattern.test(text)) failures.push(`${label}: API-key-shaped token present`);
  if (/Authorization:\s*Bearer/i.test(text)) failures.push(`${label}: authorization header present`);

  proofRows.push({
    case: label,
    model: event?.model ?? "n/a",
    peakRssMb: result.peak?.rss_mb ?? null,
    sessions: event?.sessions ?? null,
    modelCalls: event?.model_calls ?? null,
    toolCalls: event?.tool_calls ?? null,
    readDiff: event?.read_diff_calls ?? null,
    readFile: event?.read_file_calls ?? null,
    searchText: event?.search_text_calls ?? null,
    findings: event?.findings ?? null,
    valid: event?.benchmark_valid ?? null,
  });
}

console.log(JSON.stringify({ proofRows, failures }, null, 2));
if (failures.length) process.exit(1);
