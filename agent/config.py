from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv


class SettingsError(RuntimeError):
    pass


@dataclass(frozen=True)
class Settings:
    base_url: str
    api_key: str = field(repr=False)
    model: str = "bonsai2-27b"

    loki_url: str = "http://localhost:3100"
    victoria_url: str = "http://localhost:8428"
    rabbit_api_url: str = "http://localhost:15672"
    rabbit_user: str = "rabbit"
    rabbit_password: str = field(default="rabbit", repr=False)
    queue: str = "dota_inference_queue"
    dlq: str = "dota_inference_dlq"

    compose_dir: str = "."
    worker_service: str = "worker"
    max_workers: int = 4

    request_limit: int = 12
    tool_calls_limit: int = 20


def _env(name: str, default: str) -> str:
    raw = os.getenv(name)
    return raw.strip() if raw and raw.strip() else default


def load_settings() -> Settings:
    load_dotenv()
    base_url = _env("BASE_URL", "")
    api_key = _env("API_KEY", "")
    if not (base_url and api_key):
        raise SettingsError("Заполните BASE_URL и API_KEY в .env (пример в .env.example)")
    return Settings(
        base_url=base_url,
        api_key=api_key,
        model=_env("AGENT_MODEL", "bonsai2-27b"),
        loki_url=_env("LOKI_URL", "http://localhost:3100"),
        victoria_url=_env("VICTORIA_URL", "http://localhost:8428"),
        rabbit_api_url=_env("RABBIT_API_URL", "http://localhost:15672"),
        rabbit_user=_env("RABBIT_USER", "rabbit"),
        rabbit_password=_env("RABBIT_PASSWORD", "rabbit"),
        queue=_env("QUEUE_NAME", "dota_inference_queue"),
        dlq=_env("DLQ_NAME", "dota_inference_dlq"),
        compose_dir=_env("COMPOSE_DIR", "."),
        worker_service=_env("WORKER_SERVICE", "worker"),
        max_workers=int(_env("AGENT_MAX_WORKERS", "4")),
        request_limit=int(_env("AGENT_REQUEST_LIMIT", "12")),
        tool_calls_limit=int(_env("AGENT_TOOL_CALLS_LIMIT", "20")),
    )
