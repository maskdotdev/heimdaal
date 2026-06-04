from __future__ import annotations

import asyncio
import json
import os
from asyncio.subprocess import PIPE, Process
from typing import Any

from .protocol import RUNNER_PROTOCOL_VERSION, JsonRpcResponse


class RunnerProtocolError(RuntimeError):
    def __init__(self, message: str, *, code: int | None = None, kind: str | None = None):
        super().__init__(message)
        self.code = code
        self.kind = kind


class RunnerProcess:
    def __init__(self, process: Process):
        self._process = process
        self._next_id = 1
        self._pending: dict[str, asyncio.Future[Any]] = {}
        self._notifications: list[dict[str, Any]] = []
        self._reader_task = asyncio.create_task(self._read_stdout())

    @classmethod
    async def spawn(
        cls,
        *,
        runner_path: str | None = None,
        cwd: str | None = None,
    ) -> "RunnerProcess":
        executable = runner_path or os.environ.get("MUZEN_RUNNER") or "muzen-runner"
        process = await asyncio.create_subprocess_exec(
            executable,
            "stdio",
            cwd=cwd,
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
        )
        return cls(process)

    async def handshake(
        self,
        *,
        client_name: str = "muzen-py",
        client_version: str = "0.0.0",
    ) -> dict[str, Any]:
        return await self.request(
            "runner.handshake",
            {
                "protocolVersion": RUNNER_PROTOCOL_VERSION,
                "clientName": client_name,
                "clientVersion": client_version,
            },
        )

    async def check(self) -> dict[str, Any]:
        return await self.request("runner.check")

    async def schema(self) -> dict[str, Any]:
        return await self.request("runner.schema.export")

    async def start_run(self, params: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        return await self.request_with_notifications("run.start", params)

    async def run_status(self, run_id: str) -> dict[str, Any]:
        return await self.request("run.status", {"runId": run_id})

    async def run_result(self, run_id: str) -> dict[str, Any]:
        return await self.request("run.result", {"runId": run_id})

    async def cancel_run(self, run_id: str) -> dict[str, Any]:
        return await self.request("run.cancel", {"runId": run_id})

    async def read_artifact(
        self,
        run_id: str,
        artifact_id: str,
        *,
        view: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "runId": run_id,
            "artifactId": artifact_id,
        }
        if view is not None:
            params["view"] = view
        return await self.request("artifact.read", params)

    async def export_artifacts(
        self,
        run_id: str,
        *,
        artifact_ids: list[str] | None = None,
        view: str | None = None,
        max_artifacts: int | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"runId": run_id}
        if artifact_ids is not None:
            params["artifactIds"] = artifact_ids
        if view is not None:
            params["view"] = view
        if max_artifacts is not None:
            params["maxArtifacts"] = max_artifacts
        if max_bytes is not None:
            params["maxBytes"] = max_bytes
        return await self.request("artifact.export", params)

    async def read_snapshot_text(
        self,
        run_id: str,
        path: str,
        *,
        snapshot_id: str | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "runId": run_id,
            "path": path,
        }
        if snapshot_id is not None:
            params["snapshotId"] = snapshot_id
        if max_bytes is not None:
            params["maxBytes"] = max_bytes
        return await self.request("snapshot.readText", params)

    async def request(self, method: str, params: Any | None = None) -> Any:
        if self._process.stdin is None:
            raise RunnerProtocolError("muzen-runner stdin is closed")
        request_id = self._next_id
        self._next_id += 1
        frame: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
        }
        if params is not None:
            frame["params"] = params
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[str(request_id)] = future
        self._process.stdin.write(json.dumps(frame).encode("utf-8") + b"\n")
        await self._process.stdin.drain()
        return await future

    async def request_with_notifications(
        self, method: str, params: Any | None = None
    ) -> tuple[Any, list[dict[str, Any]]]:
        start = len(self._notifications)
        result = await self.request(method, params)
        return result, self._notifications[start:]

    async def close(self) -> None:
        if self._process.stdin is not None:
            self._process.stdin.close()
            await self._process.stdin.wait_closed()
        if self._process.returncode is None:
            self._process.terminate()
            try:
                await asyncio.wait_for(self._process.wait(), timeout=2)
            except asyncio.TimeoutError:
                self._process.kill()
        self._reader_task.cancel()

    async def _read_stdout(self) -> None:
        assert self._process.stdout is not None
        while True:
            line = await self._process.stdout.readline()
            if not line:
                break
            self._handle_response(json.loads(line.decode("utf-8")))
        for future in self._pending.values():
            if not future.done():
                future.set_exception(RunnerProtocolError("muzen-runner exited"))
        self._pending.clear()

    def _handle_response(self, response: JsonRpcResponse) -> None:
        response_id = response.get("id")
        if response_id is None:
            method = response.get("method")  # type: ignore[typeddict-item]
            if isinstance(method, str):
                self._notifications.append(dict(response))
            return
        future = self._pending.pop(str(response_id), None)
        if future is None:
            return
        error = response.get("error")
        if error is not None:
            data = error.get("data", {})
            future.set_exception(
                RunnerProtocolError(
                    error.get("message", "runner protocol error"),
                    code=error.get("code"),
                    kind=data.get("kind"),
                )
            )
            return
        future.set_result(response.get("result"))
