import { readFile, stat } from "node:fs/promises";
import path from "node:path";

const root = path.resolve(new URL("..", import.meta.url).pathname);
const releaseAuditPath = argValue("--release-audit");
const blockers = [];
const checks = [];

await checkConcurrentDefault();
await checkReleaseAudit();
await checkSyncSurface();

const deletionReady = blockers.length === 0;
console.log(
  JSON.stringify(
    {
      deletionReady,
      checks,
      blockers,
    },
    null,
    2,
  ),
);
if (!deletionReady) process.exit(1);

async function checkConcurrentDefault() {
  const cli = await readRepoFile("packages/muzen/src/cli.rs");
  const passed =
    !/RuntimeSelection/.test(cli) &&
    /run_job_concurrent_with_result\(job, Some\(emitter\)\)/.test(cli) &&
    /run_job_concurrent\(job\)/.test(cli);
  recordCheck("concurrent-default-runtime", passed, {
    detail: "muzen run and muzen bench have no runtime selector and dispatch to concurrent",
  });
}

async function checkReleaseAudit() {
  if (!releaseAuditPath) {
    recordCheck("release-window-audit", false, {
      detail: "missing --release-audit=<check-real-release-window-audit output JSON>",
    });
    return;
  }
  const resolved = path.resolve(process.cwd(), releaseAuditPath);
  let audit;
  try {
    audit = JSON.parse(await readFile(resolved, "utf8"));
  } catch (error) {
    recordCheck("release-window-audit", false, {
      detail: `failed to read release audit ${releaseAuditPath}: ${error.message}`,
    });
    return;
  }
  const passed =
    audit.deletionReady === true &&
    (audit.sourceKind === "release" || audit.sourceKind === "canary") &&
    audit.totals?.fallbackRuns === 0 &&
    audit.totals?.defaultConcurrentRuns === audit.totals?.runs &&
    (audit.totals?.completedSessions ?? 0) >= (audit.minSessions ?? 1);
  recordCheck("release-window-audit", passed, {
    detail: "release/canary audit must prove default concurrent runs and zero sync fallback use",
    sourceKind: audit.sourceKind ?? null,
    deletionReady: audit.deletionReady ?? null,
    totals: audit.totals ?? null,
  });
}

async function checkSyncSurface() {
  const surfaceBlockers = [];
  const blockerSpecs = [
    {
      id: "sync-runtime-module",
      file: "packages/muzen/src/runtime.rs",
      pattern: /pub\(crate\) fn run_review|pub\(crate\) struct AgentRuntime/,
      action: "delete the sync runtime module after moving shared helpers",
    },
    {
      id: "sync-model-module",
      file: "packages/muzen/src/model.rs",
      pattern: /pub\(crate\) struct ModelClientV1/,
      action: "delete the sync model client after moving credential resolution",
    },
    {
      id: "sync-tools-module",
      file: "packages/muzen/src/tools.rs",
      pattern: /pub\(crate\) struct ToolRegistry/,
      action: "delete the sync tool registry after concurrent tool parity is the only path",
    },
    {
      id: "sync-mod-declarations",
      file: "packages/muzen/src/lib.rs",
      pattern: /pub\(crate\) mod (model|runtime|tools);/,
      action: "remove sync module declarations",
    },
    {
      id: "cli-sync-branch",
      file: "packages/muzen/src/cli.rs",
      pattern: /RuntimeSelection::Sync|run_review\(job, Some\(emitter\)\)/,
      action: "remove --runtime sync and the sync execution branch",
    },
    {
      id: "bench-sync-path",
      file: "packages/muzen/src/bench.rs",
      pattern: /run_review\(job, None\)|RuntimeReport/,
      action: "make bench use the concurrent job path only",
    },
    {
      id: "concurrent-bridge-runtime-dependency",
      file: "packages/muzen/src/concurrent/bench.rs",
      pattern: /use crate::runtime::\{[^}]*build_sessions|use crate::runtime::\{[^}]*validate_job|use crate::runtime::\{[^}]*tool_allowed|use crate::runtime::\{[^}]*EventEmitter/,
      action: "move shared job validation, session building, tool masks, and event emitter out of runtime.rs",
    },
    {
      id: "concurrent-bridge-sync-tools-dependency",
      file: "packages/muzen/src/concurrent/bench.rs",
      pattern: /use crate::tools::\{[^}]*ToolRegistry|ToolOutcome/,
      action: "remove sync tool registry/types from concurrent comparison code",
    },
    {
      id: "concurrent-model-sync-credential-helper",
      file: "packages/muzen/src/concurrent/model.rs",
      pattern: /use crate::model::resolve_credential_ref/,
      action: "move credential resolution to util or a shared provider module",
    },
    {
      id: "concurrent-runtime-sync-event-emitter",
      file: "packages/muzen/src/concurrent/runtime.rs",
      pattern: /use crate::runtime::EventEmitter/,
      action: "move EventEmitter out of runtime.rs before deleting sync",
    },
    {
      id: "sync-session-contract",
      file: "packages/muzen/src/contracts.rs",
      pattern: /pub\(crate\) struct AgentSession/,
      action: "remove sync-only AgentSession contract once all callers use concurrent SessionScope",
    },
    {
      id: "sync-tests",
      file: "packages/muzen/src/tests.rs",
      pattern: /RuntimeSelection::Sync|AgentSession|RuntimeReport/,
      action: "delete or rewrite sync runtime tests after shared helper coverage moves",
    },
    {
      id: "rollout-checker-sync-fallback-loader",
      file: "bench/check-real-rollout-readiness.mjs",
      pattern: /loadRun\("sync"|fallback/,
      action: "replace shadow fallback loading with release-window audit once sync is deleted",
    },
  ];

  for (const spec of blockerSpecs) {
    const text = await readRepoFile(spec.file, { missingOk: true });
    if (text == null) continue;
    const match = text.match(spec.pattern);
    if (!match) continue;
    surfaceBlockers.push({
      id: spec.id,
      file: spec.file,
      match: match[0].slice(0, 120),
      action: spec.action,
    });
  }
  blockers.push(...surfaceBlockers);
  recordCheck("sync-surface-removed", surfaceBlockers.length === 0, {
    detail: "sync implementation, explicit sync selectors, and sync benchmark callers must be gone",
    blockerCount: surfaceBlockers.length,
    suppressBlocker: true,
  });
}

function recordCheck(id, passed, details = {}) {
  const { suppressBlocker, ...publicDetails } = details;
  checks.push({ id, passed, ...publicDetails });
  if (!passed && !suppressBlocker) {
    blockers.push({
      id,
      action: details.detail || "required preflight check failed",
      ...(details.sourceKind == null ? {} : { sourceKind: details.sourceKind }),
      ...(details.deletionReady == null ? {} : { deletionReady: details.deletionReady }),
    });
  }
}

async function readRepoFile(relativePath, options = {}) {
  const file = path.join(root, relativePath);
  const exists = await stat(file).then(
    () => true,
    () => false,
  );
  if (!exists) {
    if (options.missingOk) return null;
    throw new Error(`missing required file ${relativePath}`);
  }
  return readFile(file, "utf8");
}

function argValue(name) {
  const prefix = `${name}=`;
  return process.argv.find((arg) => arg.startsWith(prefix))?.slice(prefix.length);
}
