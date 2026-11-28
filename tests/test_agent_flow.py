import asyncio
import json

import httpx
import pytest
from agent.actions import Actions
from agent.agents import Deps, LogReport, build_ops_agent
from agent.cli import build_prompt, run_turn
from agent.config import Settings
from agent.sources import Sources
from pydantic_ai import models
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

models.ALLOW_MODEL_REQUESTS = False

S = Settings(base_url="http://gw/v1", api_key="sk-test-key-123456")
scaled: list[int] = []


async def _runner(cmd, env, cwd):
    scaled.append(int(next(a for a in cmd if a.startswith("worker=")).split("=")[1]))
    return 0, "ok"


def make_deps():
    def handler(request: httpx.Request) -> httpx.Response:
        if "/api/queues/" in request.url.path:
            return httpx.Response(
                200, json={"messages_ready": 19, "arguments": {"x-max-length": 20}, "consumers": 1}
            )
        return httpx.Response(200, json={"data": {"result": []}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return Deps(Sources(S, client), Actions(S, client, runner=_runner))


def _last_tool_returns(messages):
    return [p for m in messages for p in m.parts if isinstance(p, ToolReturnPart)]


def test_agent_reads_snapshot_then_answers():
    def script(messages, info: AgentInfo) -> ModelResponse:
        if not _last_tool_returns(messages):
            return ModelResponse(parts=[ToolCallPart("health_snapshot", {})])
        ret = _last_tool_returns(messages)[-1].content
        return ModelResponse(parts=[TextPart(f"Очередь заполнена на {ret['queue']['fill_pct']}%")])

    ops = build_ops_agent(FunctionModel(script))
    text, _ = asyncio.run(run_turn(ops, make_deps(), S, "Что с очередью?", None))
    assert "95.0%" in text


def test_action_waits_for_approval_and_runs_when_approved():
    scaled.clear()

    def script(messages, info: AgentInfo) -> ModelResponse:
        if not _last_tool_returns(messages):
            return ModelResponse(parts=[ToolCallPart("scale_workers", {"replicas": 2})])
        return ModelResponse(parts=[TextPart("Воркеров 2")])

    ops = build_ops_agent(FunctionModel(script))
    asked: list[str] = []

    def approve(question: str) -> bool:
        asked.append(question)
        return True

    text, _ = asyncio.run(run_turn(ops, make_deps(), S, "Подними 2 воркера", None, approve=approve))
    assert asked and "scale_workers" in asked[0]
    assert scaled == [2]
    assert text == "Воркеров 2"


def test_action_not_executed_when_denied():
    scaled.clear()

    def script(messages, info: AgentInfo) -> ModelResponse:
        if not _last_tool_returns(messages):
            return ModelResponse(parts=[ToolCallPart("scale_workers", {"replicas": 3})])
        return ModelResponse(parts=[TextPart("Хорошо, не буду")])

    ops = build_ops_agent(FunctionModel(script))
    text, _ = asyncio.run(
        run_turn(ops, make_deps(), S, "Подними 3 воркера", None, approve=lambda q: False)
    )
    assert scaled == []
    assert text == "Хорошо, не буду"


def test_subagent_returns_structured_report():
    def script(messages, info: AgentInfo) -> ModelResponse:
        names = {t.name for t in info.function_tools}
        if "investigate_logs" in names:
            if not _last_tool_returns(messages):
                return ModelResponse(
                    parts=[ToolCallPart("investigate_logs", {"question": "почему DLQ"})]
                )
            return ModelResponse(parts=[TextPart("Причина найдена")])
        out = info.output_tools[0].name
        report = LogReport(
            summary="падает inference", error_groups=[], suspected_cause="poison", queries_used=[]
        )
        return ModelResponse(parts=[ToolCallPart(out, json.loads(report.model_dump_json()))])

    ops = build_ops_agent(FunctionModel(script))
    text, messages = asyncio.run(run_turn(ops, make_deps(), S, "Почему сообщения в DLQ?", None))
    assert text == "Причина найдена"
    assert _last_tool_returns(messages)[-1].tool_name == "investigate_logs"


def test_image_makes_multimodal_prompt(tmp_path):
    img = tmp_path / "dash.png"
    img.write_bytes(b"\x89PNG fake")
    prompt = build_prompt("что на графике", img)
    assert isinstance(prompt, list) and prompt[0] == "что на графике"
    assert prompt[1].media_type == "image/png"
    assert build_prompt("текст", None) == "текст"


@pytest.mark.parametrize("n", [0, 99])
def test_scale_out_of_range_is_rejected(n):
    with pytest.raises(ValueError):
        asyncio.run(make_deps().actions.scale_workers(n))
