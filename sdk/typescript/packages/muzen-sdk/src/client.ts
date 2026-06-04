import {
  type AgentBudget,
  RUNNER_PROTOCOL_VERSION,
  type ReviewRequest,
  type ReviewSessionDraft,
  type ReviewEventRecord,
  type RuntimeEventRecord,
  type Role,
  type RunCancelResult,
  type RunStatusResult,
  type ToolDefinition,
  type ToolExecuteHandler,
  type RunnerArtifactExportResult,
  type RunnerArtifactReadResult,
  type RunnerArtifactView,
  type RunnerSnapshotTextResult,
  type RunnerRunResult,
  type RunnerCheckResult,
  type RunnerHandshakeResult,
  type RunnerProtocolSchema,
} from "./protocol.js";
import { RunnerProcess, type RunnerProcessOptions } from "./runner.js";

export interface MuzenClientOptions extends RunnerProcessOptions {}

export class Muzen {
  private constructor(
    private readonly runner: RunnerProcess,
    public readonly handshake: RunnerHandshakeResult,
  ) {}

  static async create(options: MuzenClientOptions = {}): Promise<Muzen> {
    const runner = await RunnerProcess.spawn(options);
    const handshake = await runner.handshake(options);
    return new Muzen(runner, handshake);
  }

  async check(): Promise<RunnerCheckResult> {
    return this.runner.check();
  }

  async schema(): Promise<RunnerProtocolSchema> {
    return this.runner.schema();
  }

  async review(request: ReviewRequest): Promise<ReviewRun> {
    const started = await this.runner.startRun({
      protocolVersion: RUNNER_PROTOCOL_VERSION,
      runId: request.runId,
      repo: request.repo,
      changedFiles: request.changedFiles ?? [],
      sessions: request.sessions,
      model: request.model ? { callback: true } : undefined,
      tools: (request.tools ?? []).map((tool) => ({
        id: tool.id,
        description: tool.description,
        parameters: tool.parameters,
        cacheable: tool.cacheable ?? false,
      })),
      limits: request.limits,
    }, {
      model: request.model,
      tools: request.tools,
    });
    return new ReviewRun(this.runner, started.result, started.notifications);
  }

  async status(runId: string): Promise<RunStatusResult> {
    return this.runner.runStatus(runId);
  }

  async result(runId: string): Promise<RunnerRunResult> {
    return this.runner.runResult(runId);
  }

  async cancel(runId: string): Promise<RunCancelResult> {
    return this.runner.cancelRun(runId);
  }

  async readArtifact(
    runId: string,
    artifactId: string,
    options: { view?: RunnerArtifactView } = {},
  ): Promise<RunnerArtifactReadResult> {
    return this.runner.readArtifact({
      runId,
      artifactId,
      view: options.view,
    });
  }

  async exportArtifacts(
    runId: string,
    options: {
      artifactIds?: string[];
      view?: RunnerArtifactView;
      maxArtifacts?: number;
      maxBytes?: number;
    } = {},
  ): Promise<RunnerArtifactExportResult> {
    return this.runner.exportArtifacts({ runId, ...options });
  }

  async readSnapshotText(
    runId: string,
    path: string,
    options: { snapshotId?: string; maxBytes?: number } = {},
  ): Promise<RunnerSnapshotTextResult> {
    return this.runner.readSnapshotText({ runId, path, ...options });
  }

  async close(): Promise<void> {
    await this.runner.close();
  }
}

export class ReviewRun {
  constructor(
    private readonly runner: RunnerProcess,
    private readonly report: RunnerRunResult,
    private readonly notifications: Array<{ method: string; params: unknown }>,
  ) {}

  async result(): Promise<RunnerRunResult> {
    return this.report;
  }

  async status(): Promise<RunStatusResult> {
    return this.runner.runStatus(this.report.runId);
  }

  async cancel(): Promise<RunCancelResult> {
    return this.runner.cancelRun(this.report.runId);
  }

  async readArtifact(
    artifactId: string,
    options: { view?: RunnerArtifactView } = {},
  ): Promise<RunnerArtifactReadResult> {
    return this.runner.readArtifact({
      runId: this.report.runId,
      artifactId,
      view: options.view,
    });
  }

  async exportArtifacts(
    options: {
      artifactIds?: string[];
      view?: RunnerArtifactView;
      maxArtifacts?: number;
      maxBytes?: number;
    } = {},
  ): Promise<RunnerArtifactExportResult> {
    return this.runner.exportArtifacts({ runId: this.report.runId, ...options });
  }

  async readSnapshotText(
    path: string,
    options: { snapshotId?: string; maxBytes?: number } = {},
  ): Promise<RunnerSnapshotTextResult> {
    return this.runner.readSnapshotText({
      runId: this.report.runId,
      path,
      ...options,
    });
  }

  async *events(): AsyncIterable<ReviewEventRecord> {
    for (const notification of this.notifications) {
      if (notification.method === "event.review") {
        yield notification.params as ReviewEventRecord;
      }
    }
  }

  async *runtimeEvents(): AsyncIterable<RuntimeEventRecord> {
    for (const notification of this.notifications) {
      if (notification.method === "event.runtime") {
        yield notification.params as RuntimeEventRecord;
      }
    }
  }
}

export function session(
  id: string,
  objective: string,
  options: {
    role?: Role;
    cwd?: string;
    modelProfileId?: string;
    budget?: Partial<AgentBudget>;
  } = {},
): ReviewSessionDraft {
  return {
    id,
    objective,
    role: options.role ?? "generalist",
    cwd: options.cwd,
    modelProfileId: options.modelProfileId,
    budget: {
      maxTurns: options.budget?.maxTurns ?? 7,
      maxToolCalls: options.budget?.maxToolCalls ?? 14,
      maxPromptTokens: options.budget?.maxPromptTokens ?? 64_000,
      maxOutputTokens: options.budget?.maxOutputTokens ?? 8_000,
    },
  };
}

export function tool(
  id: string,
  description: string,
  parameters: unknown,
  execute: ToolExecuteHandler,
  options: { cacheable?: boolean } = {},
): ToolDefinition {
  return {
    id,
    description,
    parameters,
    execute,
    cacheable: options.cacheable,
  };
}
