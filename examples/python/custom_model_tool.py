import asyncio
import os
from typing import Any

import muzen


async def main() -> None:
    runner_path = os.environ.get("MUZEN_RUNNER", "target/debug/muzen-runner")
    client = await muzen.Client.create(runner_path=runner_path)
    counters = {"model": 0, "tool": 0}

    def model(request: dict[str, Any]) -> dict[str, Any]:
        counters["model"] += 1
        has_tool_result = any(
            item.get("kind") == "tool_result"
            for item in request.get("transcript", [])
            if isinstance(item, dict)
        )
        if has_tool_result:
            return {
                "toolCalls": [
                    {
                        "toolId": "finish",
                        "arguments": {"reason": "custom callback review completed"},
                    }
                ],
                "usage": {"inputTokens": 80, "outputTokens": 20, "totalTokens": 100},
            }
        return {
            "toolCalls": [
                {"toolId": "read_diff", "arguments": {}},
                {"toolId": "read_file", "arguments": {"path": "Cargo.toml"}},
                {"toolId": "host_context", "arguments": {"topic": "sdk-callbacks"}},
                {"toolId": "search_text", "arguments": {"query": "muzen"}},
            ],
            "usage": {"inputTokens": 64, "outputTokens": 24, "totalTokens": 88},
        }

    async def host_context(request: dict[str, Any]) -> dict[str, Any]:
        counters["tool"] += 1
        arguments = request.get("arguments", {})
        return {
            "data": {
                "topic": arguments.get("topic") if isinstance(arguments, dict) else None,
                "message": "Python SDK tool callback executed",
            },
            "artifact": {
                "key": "python-host-context",
                "content": "host context from Python SDK callback",
            },
        }

    run = await client.review(
        run_id="python-callback-review",
        repo=".",
        changed_files=["Cargo.toml"],
        sessions=[
            muzen.session(
                "callback",
                "Exercise model.complete and tool.execute",
                role="correctness",
            )
        ],
        model=model,
        tools=[
            muzen.tool(
                "host_context",
                "Return context from the Python host process.",
                {
                    "type": "object",
                    "properties": {"topic": {"type": "string"}},
                    "required": ["topic"],
                    "additionalProperties": False,
                },
                host_context,
            )
        ],
        limits={"maxActiveSessions": 1},
    )

    review_events = 0
    async for _event in run.events():
        review_events += 1

    runtime_events = 0
    async for _event in run.runtime_events():
        runtime_events += 1

    report = await run.result()
    print(
        {
            "runId": report["runId"],
            "status": report["status"],
            "modelCalls": counters["model"],
            "toolCalls": counters["tool"],
            "reviewEvents": review_events,
            "runtimeEvents": runtime_events,
            "summary": report["summary"],
        }
    )

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
