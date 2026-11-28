from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from agent.config import Settings
from agent.sources import SourceUnavailable

Runner = Callable[[list[str], dict[str, str], str], Awaitable[tuple[int, str]]]


async def run_command(cmd: list[str], env: dict[str, str], cwd: str) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=cwd,
        env={**os.environ, **env},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    return proc.returncode or 0, out.decode("utf-8", "replace")


class Actions:
    def __init__(
        self, settings: Settings, client: httpx.AsyncClient, runner: Runner = run_command
    ) -> None:
        self.s = settings
        self.client = client
        self.runner = runner

    async def scale_workers(self, replicas: int) -> str:
        if not 1 <= replicas <= self.s.max_workers:
            raise ValueError(f"replicas должно быть от 1 до {self.s.max_workers}")
        cmd = [
            "docker",
            "compose",
            "up",
            "-d",
            "--no-deps",
            "--scale",
            f"{self.s.worker_service}={replicas}",
            self.s.worker_service,
        ]
        code, out = await self.runner(cmd, {}, self.s.compose_dir)
        tail = "\n".join(out.strip().splitlines()[-6:])
        if code != 0:
            return f"docker compose завершился с кодом {code}:\n{tail}"
        return (
            f"Воркеров теперь {replicas}.\n{tail}\n"
            "Важно: лимит очереди x-max-length задан при её создании и сам не меняется. "
            "Чтобы он соответствовал новому числу воркеров, очередь надо пересоздать."
        )

    async def requeue_dlq(self, count: int) -> dict[str, Any]:
        count = max(1, min(count, 50))
        auth = httpx.BasicAuth(self.s.rabbit_user, self.s.rabbit_password)
        base = self.s.rabbit_api_url
        moved = dropped = 0
        try:
            for _ in range(count):
                got = await self.client.post(
                    f"{base}/api/queues/%2F/{self.s.dlq}/get",
                    auth=auth,
                    json={"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto"},
                )
                got.raise_for_status()
                messages = got.json()
                if not messages:
                    break
                payload = messages[0].get("payload", "")
                if _is_poison(payload):
                    dropped += 1
                    continue
                pub = await self.client.post(
                    f"{base}/api/exchanges/%2F/amq.default/publish",
                    auth=auth,
                    json={
                        "properties": {"delivery_mode": 2},
                        "routing_key": self.s.queue,
                        "payload": payload,
                        "payload_encoding": "string",
                    },
                )
                pub.raise_for_status()
                if pub.json().get("routed"):
                    moved += 1
        except httpx.HTTPError as e:
            raise SourceUnavailable(f"RabbitMQ недоступен ({type(e).__name__})") from e
        return {"requeued": moved, "dropped_poison": dropped}


def _is_poison(payload: str) -> bool:
    try:
        task_id = str(json.loads(payload).get("task_id", ""))
    except (ValueError, AttributeError):
        return False
    return task_id.startswith("poison-")
