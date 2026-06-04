import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createInterface, type Interface } from "node:readline";

import {
  RUNNER_PROTOCOL_VERSION,
  type JsonRpcNotification,
  type JsonRpcResponse,
  type RunCancelResult,
  type RunStatusResult,
  type RunnerArtifactExportResult,
  type RunnerArtifactReadResult,
  type RunnerArtifactView,
  type RunnerSnapshotTextResult,
  type RunnerRunResult,
  type RunnerCheckResult,
  type RunnerHandshakeResult,
  type RunnerProtocolSchema,
} from "./protocol.js";

export interface RunnerProcessOptions {
  runnerPath?: string;
  cwd?: string;
  clientName?: string;
  clientVersion?: string;
}

export class RunnerProtocolError extends Error {
  constructor(
    message: string,
    public readonly code?: number,
    public readonly kind?: string,
  ) {
    super(message);
    this.name = "RunnerProtocolError";
  }
}

export class RunnerProcess {
  private nextId = 1;
  private readonly pending = new Map<
    string,
    {
      resolve: (value: unknown) => void;
      reject: (error: Error) => void;
    }
  >();
  private readonly stderrChunks: string[] = [];
  private readonly notifications: JsonRpcNotification[] = [];

  private constructor(
    private readonly child: ChildProcessWithoutNullStreams,
    private readonly lines: Interface,
  ) {
    this.lines.on("line", (line) => this.handleLine(line));
    this.child.stderr.on("data", (chunk) => {
      this.stderrChunks.push(String(chunk));
      if (this.stderrChunks.length > 20) {
        this.stderrChunks.shift();
      }
    });
    this.child.on("exit", (code, signal) => {
      const message = `muzen-runner exited with code ${code ?? "null"} signal ${signal ?? "null"}`;
      for (const pending of this.pending.values()) {
        pending.reject(new RunnerProtocolError(message));
      }
      this.pending.clear();
    });
  }

  static async spawn(options: RunnerProcessOptions = {}): Promise<RunnerProcess> {
    const runnerPath =
      options.runnerPath ?? process.env.MUZEN_RUNNER ?? "muzen-runner";
    const child = spawn(runnerPath, ["stdio"], {
      cwd: options.cwd,
      stdio: ["pipe", "pipe", "pipe"],
    });
    const lines = createInterface({ input: child.stdout });
    return new RunnerProcess(child, lines);
  }

  async handshake(
    options: Pick<RunnerProcessOptions, "clientName" | "clientVersion"> = {},
  ): Promise<RunnerHandshakeResult> {
    return this.request<RunnerHandshakeResult>("runner.handshake", {
      protocolVersion: RUNNER_PROTOCOL_VERSION,
      clientName: options.clientName ?? "@muzen/sdk",
      clientVersion: options.clientVersion ?? "0.0.0",
    });
  }

  async check(): Promise<RunnerCheckResult> {
    return this.request<RunnerCheckResult>("runner.check");
  }

  async schema(): Promise<RunnerProtocolSchema> {
    return this.request<RunnerProtocolSchema>("runner.schema.export");
  }

  async startRun(params: unknown): Promise<{
    result: RunnerRunResult;
    notifications: JsonRpcNotification[];
  }> {
    return this.requestWithNotifications<RunnerRunResult>("run.start", params);
  }

  async runStatus(runId: string): Promise<RunStatusResult> {
    return this.request<RunStatusResult>("run.status", { runId });
  }

  async runResult(runId: string): Promise<RunnerRunResult> {
    return this.request<RunnerRunResult>("run.result", { runId });
  }

  async cancelRun(runId: string): Promise<RunCancelResult> {
    return this.request<RunCancelResult>("run.cancel", { runId });
  }

  async readArtifact(params: {
    runId: string;
    artifactId: string;
    view?: RunnerArtifactView;
  }): Promise<RunnerArtifactReadResult> {
    return this.request<RunnerArtifactReadResult>("artifact.read", params);
  }

  async exportArtifacts(params: {
    runId: string;
    artifactIds?: string[];
    view?: RunnerArtifactView;
    maxArtifacts?: number;
    maxBytes?: number;
  }): Promise<RunnerArtifactExportResult> {
    return this.request<RunnerArtifactExportResult>("artifact.export", params);
  }

  async readSnapshotText(params: {
    runId: string;
    snapshotId?: string;
    path: string;
    maxBytes?: number;
  }): Promise<RunnerSnapshotTextResult> {
    return this.request<RunnerSnapshotTextResult>("snapshot.readText", params);
  }

  async request<TResult>(method: string, params?: unknown): Promise<TResult> {
    const id = this.nextId++;
    const frame = JSON.stringify({
      jsonrpc: "2.0",
      id,
      method,
      ...(params === undefined ? {} : { params }),
    });

    const result = new Promise<TResult>((resolve, reject) => {
      this.pending.set(String(id), {
        resolve: (value) => resolve(value as TResult),
        reject,
      });
    });

    this.child.stdin.write(`${frame}\n`);
    return result;
  }

  async requestWithNotifications<TResult>(
    method: string,
    params?: unknown,
  ): Promise<{ result: TResult; notifications: JsonRpcNotification[] }> {
    const start = this.notifications.length;
    const result = await this.request<TResult>(method, params);
    return {
      result,
      notifications: this.notifications.slice(start),
    };
  }

  async close(): Promise<void> {
    this.lines.close();
    this.child.stdin.end();
    if (!this.child.killed) {
      this.child.kill();
    }
  }

  stderrTail(): string {
    return this.stderrChunks.join("");
  }

  private handleLine(line: string): void {
    let response: JsonRpcResponse;
    try {
      response = JSON.parse(line) as JsonRpcResponse;
    } catch (error) {
      throw new RunnerProtocolError(`Invalid runner JSON: ${String(error)}`);
    }

    if (response.id === undefined || response.id === null) {
      const notification = response as JsonRpcNotification;
      if (notification.method) {
        this.notifications.push(notification);
      }
      return;
    }
    const pending = this.pending.get(String(response.id));
    if (!pending) {
      return;
    }
    this.pending.delete(String(response.id));

    if (response.error) {
      pending.reject(
        new RunnerProtocolError(
          response.error.message,
          response.error.code,
          response.error.data?.kind,
        ),
      );
      return;
    }
    pending.resolve(response.result);
  }
}
