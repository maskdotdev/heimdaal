import {
  Muzen,
  session,
  tool,
  type ModelCompleteRequest,
  type ModelCompleteResult,
} from "../../../sdk/typescript/packages/muzen-sdk/src/index.js";

const runnerPath = process.env.MUZEN_RUNNER ?? "target/debug/muzen-runner";
const client = await Muzen.create({ runnerPath });

let modelCalls = 0;
let toolCalls = 0;

function model(request: ModelCompleteRequest): ModelCompleteResult {
  modelCalls += 1;
  const hasToolResult = request.transcript.some(
    (item) => item.kind === "tool_result",
  );
  if (hasToolResult) {
    return {
      toolCalls: [
        {
          toolId: "finish",
          arguments: { reason: "custom callback review completed" },
        },
      ],
      usage: { inputTokens: 80, outputTokens: 20, totalTokens: 100 },
    };
  }
  return {
    toolCalls: [
      { toolId: "read_diff", arguments: {} },
      { toolId: "read_file", arguments: { path: "Cargo.toml" } },
      { toolId: "host_context", arguments: { topic: "sdk-callbacks" } },
      { toolId: "search_text", arguments: { query: "muzen" } },
    ],
    usage: { inputTokens: 64, outputTokens: 24, totalTokens: 88 },
  };
}

const hostContext = tool(
  "host_context",
  "Return context from the TypeScript host process.",
  {
    type: "object",
    properties: { topic: { type: "string" } },
    required: ["topic"],
    additionalProperties: false,
  },
  async (request) => {
    toolCalls += 1;
    return {
      data: {
        topic: (request.arguments as { topic?: string }).topic,
        message: "TypeScript SDK tool callback executed",
      },
      artifact: {
        key: "typescript-host-context",
        content: "host context from TypeScript SDK callback",
      },
    };
  },
);

const run = await client.review({
  runId: "typescript-callback-review",
  repo: ".",
  changedFiles: ["Cargo.toml"],
  sessions: [
    session("callback", "Exercise model.complete and tool.execute", {
      role: "correctness",
    }),
  ],
  model,
  tools: [hostContext],
  limits: { maxActiveSessions: 1 },
});

let reviewEvents = 0;
for await (const _event of run.events()) {
  reviewEvents += 1;
}

let runtimeEvents = 0;
for await (const _event of run.runtimeEvents()) {
  runtimeEvents += 1;
}

const report = await run.result();
console.log({
  runId: report.runId,
  status: report.status,
  modelCalls,
  toolCalls,
  reviewEvents,
  runtimeEvents,
  summary: report.summary,
});

await client.close();
