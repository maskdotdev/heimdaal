import { Muzen, session } from "../../../sdk/typescript/packages/muzen-sdk/src/index.js";

const runnerPath = process.env.MUZEN_RUNNER ?? "target/debug/muzen-runner";
const client = await Muzen.create({ runnerPath });

console.log(client.handshake);
console.log(await client.check());

const sessions = [
  session("security", "Find security regressions", { role: "security" }),
  session("tests", "Find missing test coverage", { role: "correctness" }),
];

const run = await client.review({
  runId: "typescript-basic-review",
  repo: ".",
  changedFiles: ["Cargo.toml"],
  sessions,
});

const events = [];
for await (const event of run.events()) {
  events.push(event);
  console.log({ event: event.event, sessionId: event.sessionId });
}

const report = await run.result();
const artifactId = events.find((event) => event.artifactId)?.artifactId;
const artifact = artifactId ? await run.readArtifact(artifactId) : undefined;
const artifactExport = artifactId
  ? await run.exportArtifacts({
      artifactIds: [artifactId],
      maxArtifacts: 1,
      maxBytes: 1_000_000,
    })
  : undefined;
const firstSnapshot = report.snapshots[0];
const snapshotText = firstSnapshot
  ? await run.readSnapshotText("Cargo.toml", {
      snapshotId: firstSnapshot.snapshotId,
      maxBytes: 2_000,
    })
  : undefined;
const storedStatus = await run.status();
const storedReport = await client.result(report.runId);
const cancel = await run.cancel();
console.log({
  runId: report.runId,
  status: report.status,
  storedStatus,
  summary: report.summary,
  storedReportStatus: storedReport.status,
  findings: storedReport.findings,
  artifact: artifact
    ? {
        id: artifact.artifact.artifactId,
        bytes: artifact.artifact.bytes,
        view: artifact.view,
      }
    : null,
  artifactExport: artifactExport
    ? {
        artifactCount: artifactExport.artifactCount,
        totalBytes: artifactExport.totalBytes,
      }
    : null,
  snapshotText: snapshotText
    ? {
        path: snapshotText.path,
        bytes: snapshotText.bytes,
        contentHash: snapshotText.contentHash,
      }
    : null,
  cancel,
});

await client.close();
