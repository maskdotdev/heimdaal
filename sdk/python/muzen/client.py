from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from .protocol import (
    RUNNER_PROTOCOL_VERSION,
    ModelCompleteHandler,
    ReviewSession,
    ToolDefinition,
)
from .runner import RunnerProcess


class Client:
    def __init__(self, runner: RunnerProcess, handshake: dict[str, Any]):
        self._runner = runner
        self.handshake = handshake

    @classmethod
    async def create(
        cls,
        *,
        runner_path: str | None = None,
        cwd: str | None = None,
        client_name: str = "muzen-py",
        client_version: str = "0.0.0",
    ) -> "Client":
        runner = await RunnerProcess.spawn(runner_path=runner_path, cwd=cwd)
        handshake = await runner.handshake(
            client_name=client_name,
            client_version=client_version,
        )
        return cls(runner, handshake)

    async def check(self) -> dict[str, Any]:
        return await self._runner.check()

    async def schema(self) -> dict[str, Any]:
        return await self._runner.schema()

    async def review(
        self,
        *,
        repo: str,
        sessions: list[ReviewSession],
        run_id: str | None = None,
        changed_files: list[str] | None = None,
        model: ModelCompleteHandler | None = None,
        tools: list[ToolDefinition] | None = None,
        limits: dict[str, Any] | None = None,
    ) -> "ReviewRun":
        result, notifications = await self._runner.start_run(
            {
                "protocolVersion": RUNNER_PROTOCOL_VERSION,
                "runId": run_id,
                "repo": repo,
                "changedFiles": changed_files or [],
                "sessions": [session.to_json() for session in sessions],
                "model": {"callback": True} if model is not None else None,
                "tools": [tool.to_json() for tool in tools or []],
                "limits": limits,
            },
            model=model,
            tools=tools,
        )
        return ReviewRun(self._runner, result, notifications)

    async def status(self, run_id: str) -> dict[str, Any]:
        return await self._runner.run_status(run_id)

    async def result(self, run_id: str) -> dict[str, Any]:
        return await self._runner.run_result(run_id)

    async def cancel(self, run_id: str) -> dict[str, Any]:
        return await self._runner.cancel_run(run_id)

    async def read_artifact(
        self,
        run_id: str,
        artifact_id: str,
        *,
        view: str | None = None,
    ) -> dict[str, Any]:
        return await self._runner.read_artifact(run_id, artifact_id, view=view)

    async def export_artifacts(
        self,
        run_id: str,
        *,
        artifact_ids: list[str] | None = None,
        view: str | None = None,
        max_artifacts: int | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        return await self._runner.export_artifacts(
            run_id,
            artifact_ids=artifact_ids,
            view=view,
            max_artifacts=max_artifacts,
            max_bytes=max_bytes,
        )

    async def read_snapshot_text(
        self,
        run_id: str,
        path: str,
        *,
        snapshot_id: str | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        return await self._runner.read_snapshot_text(
            run_id,
            path,
            snapshot_id=snapshot_id,
            max_bytes=max_bytes,
        )

    async def close(self) -> None:
        await self._runner.close()


class ReviewRun:
    def __init__(
        self,
        runner: RunnerProcess,
        report: dict[str, Any],
        notifications: list[dict[str, Any]],
    ):
        self._runner = runner
        self._report = report
        self._notifications = notifications

    async def result(self) -> dict[str, Any]:
        return self._report

    async def status(self) -> dict[str, Any]:
        return await self._runner.run_status(self._report["runId"])

    async def cancel(self) -> dict[str, Any]:
        return await self._runner.cancel_run(self._report["runId"])

    async def read_artifact(
        self,
        artifact_id: str,
        *,
        view: str | None = None,
    ) -> dict[str, Any]:
        return await self._runner.read_artifact(
            self._report["runId"],
            artifact_id,
            view=view,
        )

    async def export_artifacts(
        self,
        *,
        artifact_ids: list[str] | None = None,
        view: str | None = None,
        max_artifacts: int | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        return await self._runner.export_artifacts(
            self._report["runId"],
            artifact_ids=artifact_ids,
            view=view,
            max_artifacts=max_artifacts,
            max_bytes=max_bytes,
        )

    async def read_snapshot_text(
        self,
        path: str,
        *,
        snapshot_id: str | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        return await self._runner.read_snapshot_text(
            self._report["runId"],
            path,
            snapshot_id=snapshot_id,
            max_bytes=max_bytes,
        )

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        for notification in self._notifications:
            if notification.get("method") == "event.review":
                params = notification.get("params")
                if isinstance(params, dict):
                    yield params

    async def runtime_events(self) -> AsyncIterator[dict[str, Any]]:
        for notification in self._notifications:
            if notification.get("method") == "event.runtime":
                params = notification.get("params")
                if isinstance(params, dict):
                    yield params
