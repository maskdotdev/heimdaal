from __future__ import annotations

import asyncio
import inspect
import json
import os
from asyncio.subprocess import PIPE, Process
from typing import Any

from .protocol import (
    RUNNER_PROTOCOL_VERSION,
    JsonRpcResponse,
    ModelCompleteHandler,
    ToolDefinition,
)


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
        self._model_callback: ModelCompleteHandler | None = None
        self._tool_callbacks: dict[str, ToolDefinition] = {}
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

    async def start_run(
        self,
        params: dict[str, Any],
        *,
        model: ModelCompleteHandler | None = None,
        tools: list[ToolDefinition] | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        return await self.request_with_notifications(
            "run.start",
            params,
            model=model,
            tools=tools,
        )

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
        self,
        method: str,
        params: Any | None = None,
        *,
        model: ModelCompleteHandler | None = None,
        tools: list[ToolDefinition] | None = None,
    ) -> tuple[Any, list[dict[str, Any]]]:
        start = len(self._notifications)
        previous_model = self._model_callback
        previous_tools = self._tool_callbacks
        self._model_callback = model
        self._tool_callbacks = {tool.id: tool for tool in tools or []}
        try:
            result = await self.request(method, params)
            return result, self._notifications[start:]
        finally:
            self._model_callback = previous_model
            self._tool_callbacks = previous_tools

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
            await self._handle_response(json.loads(line.decode("utf-8")))
        for future in self._pending.values():
            if not future.done():
                future.set_exception(RunnerProtocolError("muzen-runner exited"))
        self._pending.clear()

    async def _handle_response(self, response: JsonRpcResponse) -> None:
        response_id = response.get("id")
        method = response.get("method")
        if response_id is not None and isinstance(method, str):
            await self._handle_runner_request(response)
            return
        if response_id is None:
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

    async def _handle_runner_request(self, request: JsonRpcResponse) -> None:
        request_id = request.get("id")
        method = request.get("method")
        try:
            if method == "model.complete":
                if self._model_callback is None:
                    raise RunnerProtocolError(
                        "No model callback registered",
                        code=-32601,
                        kind="method_not_found",
                    )
                result = self._model_callback(request.get("params", {}))
                if inspect.isawaitable(result):
                    result = await result
                await self._write_frame({"jsonrpc": "2.0", "id": request_id, "result": result})
                return
            if method == "tool.execute":
                params = request.get("params", {})
                tool_id = params.get("toolId") if isinstance(params, dict) else None
                tool = self._tool_callbacks.get(tool_id) if isinstance(tool_id, str) else None
                if tool is None:
                    raise RunnerProtocolError(
                        f"No tool callback registered for {tool_id}",
                        code=-32601,
                        kind="method_not_found",
                    )
                result = tool.execute(params)
                if inspect.isawaitable(result):
                    result = await result
                await self._write_frame({"jsonrpc": "2.0", "id": request_id, "result": result})
                return
            raise RunnerProtocolError(
                f"Unsupported runner callback {method}",
                code=-32601,
                kind="method_not_found",
            )
        except Exception as error:
            if isinstance(error, RunnerProtocolError):
                code = error.code or -32002
                kind = error.kind or "runner_error"
                message = str(error)
            else:
                code = -32002
                kind = "runner_error"
                message = str(error)
            await self._write_frame(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": code,
                        "message": message,
                        "data": {"kind": kind},
                    },
                }
            )

    async def _write_frame(self, frame: dict[str, Any]) -> None:
        if self._process.stdin is None:
            raise RunnerProtocolError("muzen-runner stdin is closed")
        self._process.stdin.write(json.dumps(frame).encode("utf-8") + b"\n")
        await self._process.stdin.drain()
