import asyncio
import os

import muzen


async def main() -> None:
    runner_path = os.environ.get("MUZEN_RUNNER", "target/debug/muzen-runner")
    client = await muzen.Client.create(runner_path=runner_path)

    print(client.handshake)
    print(await client.check())

    sessions = [
        muzen.session("security", "Find security regressions", role="security"),
        muzen.session("tests", "Find missing test coverage", role="correctness"),
    ]

    run = await client.review(
        run_id="python-basic-review",
        repo=".",
        changed_files=["Cargo.toml"],
        sessions=sessions,
    )

    events = []
    async for event in run.events():
        events.append(event)
        print({"event": event.get("event"), "sessionId": event.get("sessionId")})

    report = await run.result()
    artifact_id = next(
        (event.get("artifactId") for event in events if event.get("artifactId")),
        None,
    )
    artifact = await run.read_artifact(artifact_id) if artifact_id else None
    artifact_export = (
        await run.export_artifacts(
            artifact_ids=[artifact_id],
            max_artifacts=1,
            max_bytes=1_000_000,
        )
        if artifact_id
        else None
    )
    snapshot = report["snapshots"][0] if report["snapshots"] else None
    snapshot_text = (
        await run.read_snapshot_text(
            "Cargo.toml",
            snapshot_id=snapshot["snapshotId"],
            max_bytes=2_000,
        )
        if snapshot
        else None
    )
    stored_status = await run.status()
    stored_report = await client.result(report["runId"])
    cancel = await run.cancel()
    print(
        {
            "runId": report["runId"],
            "status": report["status"],
            "storedStatus": stored_status,
            "summary": report["summary"],
            "storedReportStatus": stored_report["status"],
            "findings": stored_report["findings"],
            "artifact": {
                "id": artifact["artifact"]["artifactId"],
                "bytes": artifact["artifact"]["bytes"],
                "view": artifact["view"],
            }
            if artifact
            else None,
            "artifactExport": {
                "artifactCount": artifact_export["artifactCount"],
                "totalBytes": artifact_export["totalBytes"],
            }
            if artifact_export
            else None,
            "snapshotText": {
                "path": snapshot_text["path"],
                "bytes": snapshot_text["bytes"],
                "contentHash": snapshot_text["contentHash"],
            }
            if snapshot_text
            else None,
            "cancel": cancel,
        }
    )

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
