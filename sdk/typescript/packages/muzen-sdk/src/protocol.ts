export const RUNNER_PROTOCOL_VERSION = "muzen.runner.v1" as const;

export type JsonRpcId = string | number | null;

export interface JsonRpcRequest<TParams = unknown> {
  jsonrpc: "2.0";
  id: JsonRpcId;
  method: string;
  params?: TParams;
}

export interface JsonRpcResponse<TResult = unknown> {
  jsonrpc: "2.0";
  id?: JsonRpcId;
  result?: TResult;
  error?: JsonRpcError;
}

export interface JsonRpcNotification<TParams = unknown> {
  jsonrpc: "2.0";
  method: string;
  params: TParams;
}

export interface JsonRpcError {
  code: number;
  message: string;
  data?: {
    kind: string;
  };
}

export interface RunnerHandshakeParams {
  protocolVersion: typeof RUNNER_PROTOCOL_VERSION;
  clientName?: string;
  clientVersion?: string;
}

export interface RunnerHandshakeResult {
  protocolVersion: typeof RUNNER_PROTOCOL_VERSION;
  runnerName: "muzen-runner";
  runnerVersion: string;
  capabilities: RunnerCapabilities;
}

export interface RunnerCapabilities {
  supportedMethods: string[];
  plannedMethods: string[];
  transports: string[];
}

export interface RunnerCheckResult {
  ok: boolean;
  protocolVersion: typeof RUNNER_PROTOCOL_VERSION;
  runnerName: "muzen-runner";
  runnerVersion: string;
  rustPackage: string;
}

export interface RunnerProtocolSchema {
  schemaVersion: typeof RUNNER_PROTOCOL_VERSION;
  transport: string;
  requests: RunnerMethodSchema[];
  callbacks: RunnerMethodSchema[];
  notifications: RunnerMethodSchema[];
}

export interface RunnerMethodSchema {
  method: string;
  direction: "sdk_to_runner" | "runner_to_sdk";
  status: "implemented" | "reserved";
  summary: string;
}

export type Role =
  | "generalist"
  | "security"
  | "performance"
  | "maintainability"
  | "correctness"
  | "architecture"
  | "validator";

export interface AgentBudget {
  maxTurns: number;
  maxToolCalls: number;
  maxPromptTokens: number;
  maxOutputTokens: number;
}

export interface ReviewSessionDraft {
  id: string;
  role: Role;
  objective: string;
  cwd?: string;
  modelProfileId?: string;
  budget: AgentBudget;
}

export interface ReviewRequest {
  runId?: string;
  repo: string;
  changedFiles?: string[];
  sessions: ReviewSessionDraft[];
  model?: ModelCompleteHandler;
  tools?: ToolDefinition[];
  limits?: {
    maxActiveSessions?: number;
    maxFileBytes?: number;
    maxSearchMatches?: number;
  };
}

export interface ModelCompleteRequest {
  protocolVersion: typeof RUNNER_PROTOCOL_VERSION;
  runId: string;
  sessionId: string;
  role: Role;
  objective: string;
  snapshotId?: string;
  modelProfileId?: string;
  turn: number;
  transcript: TranscriptItem[];
}

export type TranscriptItem =
  | { kind: "system"; content: string }
  | { kind: "user"; content: string }
  | { kind: "assistant_text"; content: string }
  | { kind: "assistant_tool_calls"; calls: ModelToolCall[] }
  | {
      kind: "tool_result";
      callId: string;
      toolId: string;
      ok: boolean;
      artifactId?: string;
      data?: unknown;
      errorCode?: string;
    };

export interface ModelCompleteResult {
  content?: string;
  toolCalls?: ModelToolCall[];
  usage?: TokenUsage;
}

export interface ModelToolCall {
  callId?: string;
  toolId: string;
  arguments?: unknown;
}

export interface TokenUsage {
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
}

export type ModelCompleteHandler = (
  request: ModelCompleteRequest,
) => Promise<ModelCompleteResult> | ModelCompleteResult;

export interface ToolDefinition {
  id: string;
  description: string;
  parameters: unknown;
  cacheable?: boolean;
  execute: ToolExecuteHandler;
}

export interface ToolExecuteRequest {
  protocolVersion: typeof RUNNER_PROTOCOL_VERSION;
  runId: string;
  sessionId: string;
  turn: number;
  callId: string;
  toolId: string;
  snapshotId: string;
  providerResources: string[];
  arguments: unknown;
}

export interface ToolExecuteResult {
  data?: unknown;
  artifact?: {
    key: string;
    content: string;
  };
}

export type ToolExecuteHandler = (
  request: ToolExecuteRequest,
) => Promise<ToolExecuteResult> | ToolExecuteResult;

export interface ReviewEventRecord {
  seq: number;
  timestampUtc: string;
  runId?: string;
  snapshotId?: string;
  sessionId?: string;
  turn?: number;
  toolCallId?: string;
  artifactId?: string;
  findingId?: string;
  event: Record<string, unknown>;
}

export interface RuntimeEventRecord {
  seq: number;
  timestampUtc: string;
  context: Record<string, unknown>;
  event: Record<string, unknown>;
}

export interface RunnerRunResult {
  protocolVersion: typeof RUNNER_PROTOCOL_VERSION;
  runId: string;
  status: string;
  summary: RunnerRunSummary;
  findings: RunnerFinding[];
  snapshots: RunnerSnapshotSummary[];
}

export interface RunnerRunSummary {
  sessions: number;
  completedSessions: number;
  modelCalls: number;
  toolCalls: number;
  findings: number;
  publishableFindings: number;
  elapsedMs: number;
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
  artifacts: number;
  artifactBytes: number;
  snapshotCount: number;
}

export interface RunnerFinding {
  id: string;
  title: string;
  claim: string;
  evidenceCount: number;
  publishable: boolean;
}

export interface RunnerSnapshotSummary {
  snapshotId: string;
  files: number;
  changedFiles: number;
  capturedFiles: number;
  capturedBytes: number;
}

export interface RunStatusResult {
  runId: string;
  status: string;
}

export interface RunCancelResult {
  runId: string;
  status: string;
  cancelled: boolean;
  reason: string;
}

export type RunnerArtifactView = "redacted" | "raw";

export interface RunnerArtifact {
  artifactId: string;
  bytes: number;
  contentHash: string;
  content: string;
}

export interface RunnerArtifactReadResult {
  runId: string;
  view: RunnerArtifactView;
  artifact: RunnerArtifact;
}

export interface RunnerArtifactExportResult {
  runId: string;
  view: RunnerArtifactView;
  artifactCount: number;
  totalBytes: number;
  artifacts: RunnerArtifact[];
}

export interface RunnerSnapshotTextResult {
  runId: string;
  snapshotId: string;
  path: string;
  contentHash: string;
  bytes: number;
  truncated: boolean;
  content: string;
}
