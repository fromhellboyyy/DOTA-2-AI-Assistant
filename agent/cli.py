from __future__ import annotations

import argparse
import asyncio
import logging
import mimetypes
import os
import sys
from pathlib import Path

import httpx
from pydantic_ai import BinaryContent, DeferredToolRequests, DeferredToolResults, UsageLimits
from pydantic_ai.messages import ModelMessage

from agent.actions import Actions
from agent.agents import Deps, build_ops_agent
from agent.config import Settings, SettingsError, load_settings
from agent.model import build_model
from agent.sources import Sources

os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")


def _ask_yes_no(prompt: str) -> bool:
    return input(f"{prompt} [y/N] ").strip().lower() in {"y", "yes", "д", "да"}


def build_prompt(text: str, image: Path | None):
    if image is None:
        return text
    media_type = mimetypes.guess_type(image.name)[0] or "image/png"
    return [text, BinaryContent(image.read_bytes(), media_type=media_type)]


async def run_turn(
    ops,
    deps: Deps,
    settings: Settings,
    prompt,
    history: list[ModelMessage] | None,
    approve=_ask_yes_no,
):
    limits = UsageLimits(
        request_limit=settings.request_limit, tool_calls_limit=settings.tool_calls_limit
    )
    result = await ops.run(prompt, deps=deps, usage_limits=limits, message_history=history)
    while isinstance(result.output, DeferredToolRequests):
        approvals: dict[str, bool] = {}
        for call in result.output.approvals:
            approvals[call.tool_call_id] = approve(
                f"Агент хочет выполнить {call.tool_name}({call.args}). Разрешить?"
            )
        result = await ops.run(
            None,
            deps=deps,
            usage_limits=limits,
            message_history=result.all_messages(),
            deferred_tool_results=DeferredToolResults(approvals=approvals),
        )
    return str(result.output), result.all_messages()


async def main_async(args: argparse.Namespace) -> int:
    try:
        settings = load_settings()
    except SettingsError as e:
        print(e, file=sys.stderr)
        return 2

    client = httpx.AsyncClient(timeout=10.0)
    deps = Deps(sources=Sources(settings, client), actions=Actions(settings, client))
    ops = build_ops_agent(build_model(settings), thinking=args.think)
    history: list[ModelMessage] | None = None
    try:
        text = args.question
        image = Path(args.image) if args.image else None
        while True:
            if not text:
                text = input("вы> ").strip()
                if not text:
                    continue
            answer, history = await run_turn(
                ops, deps, settings, build_prompt(text, image), history
            )
            print(f"\nагент> {answer}\n")
            image = None
            text = ""
            if not args.interactive:
                return 0
    except (EOFError, KeyboardInterrupt):
        return 0
    finally:
        await client.aclose()


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(
        prog="agent", description="SRE-помощник пайплайна Dota-2-AI-Assistant"
    )
    parser.add_argument("question", nargs="?", default="", help="вопрос или поручение")
    parser.add_argument("-i", "--interactive", action="store_true", help="диалог")
    parser.add_argument("--image", help="картинка к вопросу (скриншот дашборда)")
    parser.add_argument(
        "--think", action="store_true", help="включить рассуждения модели (медленнее, точнее)"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="логировать шаги агента")
    args = parser.parse_args()
    if not args.question:
        args.interactive = True

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    for noisy in ("httpx", "httpx2"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
