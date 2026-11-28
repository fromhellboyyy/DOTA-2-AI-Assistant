from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.exceptions import ToolRetryError

log = logging.getLogger("agent")


@dataclass
class AgentLogging(AbstractCapability[Any]):
    _started: dict[str, float] = field(default_factory=dict)

    @staticmethod
    def _name(ctx) -> str:
        return getattr(ctx.agent, "name", None) or "agent"

    async def before_run(self, ctx) -> None:
        self._started[ctx.run_id] = time.perf_counter()
        log.info("%s: старт", self._name(ctx))

    async def after_model_request(self, ctx, *, request_context, response):
        calls = [p.tool_name for p in response.parts if p.part_kind == "tool-call"]
        log.info(
            "%s: ответ модели, токены %d->%d%s",
            self._name(ctx),
            response.usage.input_tokens,
            response.usage.output_tokens,
            f", вызывает {', '.join(calls)}" if calls else "",
        )
        return response

    async def wrap_tool_execute(self, ctx, *, call, tool_def, args, handler):
        log.info("%s: инструмент %s(%s)", self._name(ctx), call.tool_name, call.args_as_json_str())
        try:
            return await handler(args)
        except ToolRetryError as e:
            log.warning("%s: %s просит модель исправиться: %s", self._name(ctx), call.tool_name, e)
            raise
        except Exception:
            log.exception("%s: инструмент %s упал", self._name(ctx), call.tool_name)
            raise

    async def after_run(self, ctx, *, result):
        elapsed = time.perf_counter() - self._started.pop(ctx.run_id, time.perf_counter())
        u = result.usage
        log.info(
            "%s: готово за %.1f с, запросов %d, инструментов %d, токены %d->%d",
            self._name(ctx),
            elapsed,
            u.requests,
            u.tool_calls,
            u.input_tokens,
            u.output_tokens,
        )
        return result

    async def on_run_error(self, ctx, *, error):
        self._started.pop(ctx.run_id, None)
        log.error("%s: run прерван: %s", self._name(ctx), error)
        raise error
