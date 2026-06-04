from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, TypedDict

RUNNER_PROTOCOL_VERSION: Literal["muzen.runner.v1"] = "muzen.runner.v1"


class JsonRpcErrorData(TypedDict, total=False):
    kind: str


class JsonRpcErrorPayload(TypedDict, total=False):
    code: int
    message: str
    data: JsonRpcErrorData


class JsonRpcResponse(TypedDict, total=False):
    jsonrpc: Literal["2.0"]
    id: str | int | None
    result: Any
    error: JsonRpcErrorPayload


@dataclass(frozen=True)
class AgentBudget:
    max_turns: int = 7
    max_tool_calls: int = 14
    max_prompt_tokens: int = 64_000
    max_output_tokens: int = 8_000

    def to_json(self) -> dict[str, int]:
        return {
            "maxTurns": self.max_turns,
            "maxToolCalls": self.max_tool_calls,
            "maxPromptTokens": self.max_prompt_tokens,
            "maxOutputTokens": self.max_output_tokens,
        }


@dataclass(frozen=True)
class ReviewSession:
    id: str
    objective: str
    role: str = "generalist"
    cwd: str | None = None
    model_profile_id: str | None = None
    budget: AgentBudget = field(default_factory=AgentBudget)

    def to_json(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "id": self.id,
            "objective": self.objective,
            "role": self.role,
            "budget": self.budget.to_json(),
        }
        if self.cwd is not None:
            data["cwd"] = self.cwd
        if self.model_profile_id is not None:
            data["modelProfileId"] = self.model_profile_id
        return data


def session(
    id: str,
    objective: str,
    *,
    role: str = "generalist",
    cwd: str | None = None,
    model_profile_id: str | None = None,
    budget: AgentBudget | None = None,
) -> ReviewSession:
    return ReviewSession(
        id=id,
        objective=objective,
        role=role,
        cwd=cwd,
        model_profile_id=model_profile_id,
        budget=budget or AgentBudget(),
    )
