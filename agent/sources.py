from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from agent.config import Settings
from agent.redact import redact

MAX_LOG_LINES = 100
MAX_SERIES = 20


class BadQuery(Exception):
    pass


class SourceUnavailable(Exception):
    pass


def _check(resp: httpx.Response, name: str) -> None:
    if resp.status_code == 400:
        raise BadQuery(f"{name}: {redact(resp.text, max_len=500)}")
    if resp.status_code >= 400:
        raise SourceUnavailable(f"{name}: HTTP {resp.status_code}")


class Sources:
    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None) -> None:
        self.s = settings
        self.client = client or httpx.AsyncClient(timeout=10.0)

    async def aclose(self) -> None:
        await self.client.aclose()

    async def _get(self, name: str, url: str, **kwargs: Any) -> Any:
        try:
            resp = await self.client.get(url, **kwargs)
        except httpx.HTTPError as e:
            raise SourceUnavailable(f"{name} недоступен ({type(e).__name__})") from e
        _check(resp, name)
        return resp.json()

    async def loki_query(self, logql: str, minutes: int = 15, limit: int = 100) -> dict[str, Any]:
        limit = max(1, min(limit, MAX_LOG_LINES))
        end = time.time_ns()
        start = end - max(1, minutes) * 60 * 1_000_000_000
        data = await self._get(
            "Loki",
            f"{self.s.loki_url}/loki/api/v1/query_range",
            params={
                "query": logql,
                "start": start,
                "end": end,
                "limit": limit,
                "direction": "backward",
            },
        )
        result = data.get("data", {}).get("result", [])
        if data.get("data", {}).get("resultType") == "matrix":
            series = [
                {
                    "labels": r.get("metric", {}),
                    "last_values": [v[1] for v in r.get("values", [])[-5:]],
                }
                for r in result[:MAX_SERIES]
            ]
            return {"kind": "metric", "series": series}
        rows: list[tuple[int, str, str]] = []
        for stream in result:
            svc = stream.get("stream", {}).get("service", "?")
            for ts, line in stream.get("values", []):
                rows.append((int(ts), svc, line))
        rows.sort(reverse=True)
        lines: list[str] = []
        total = 0
        for _, svc, line in rows[:limit]:
            text = f"[{svc}] {redact(line, max_len=250)}"
            total += len(text)
            if total > 8000:
                break
            lines.append(text)
        return {"kind": "logs", "count": len(lines), "lines": lines}

    async def prom_query(self, promql: str) -> list[dict[str, Any]]:
        data = await self._get(
            "VictoriaMetrics", f"{self.s.victoria_url}/api/v1/query", params={"query": promql}
        )
        out = []
        for r in data.get("data", {}).get("result", [])[:MAX_SERIES]:
            value = r.get("value", [None, None])[1]
            out.append({"labels": r.get("metric", {}), "value": value})
        return out

    async def metric_names(self, prefix: str = "") -> list[str]:
        data = await self._get(
            "VictoriaMetrics", f"{self.s.victoria_url}/api/v1/label/__name__/values"
        )
        names = (
            [n for n in data.get("data", []) if n.startswith(prefix)]
            if prefix
            else data.get("data", [])
        )
        return sorted(names)[:100]

    def _auth(self) -> httpx.BasicAuth:
        return httpx.BasicAuth(self.s.rabbit_user, self.s.rabbit_password)

    async def queue_info(self, name: str) -> dict[str, Any]:
        d = await self._get(
            "RabbitMQ", f"{self.s.rabbit_api_url}/api/queues/%2F/{name}", auth=self._auth()
        )
        stats = d.get("message_stats", {})
        max_length = d.get("arguments", {}).get("x-max-length")
        ready = d.get("messages_ready", 0)
        return {
            "queue": name,
            "state": d.get("state"),
            "ready": ready,
            "unacked": d.get("messages_unacknowledged", 0),
            "consumers": d.get("consumers", 0),
            "max_length": max_length,
            "fill_pct": round(100 * ready / max_length, 1) if max_length else None,
            "publish_rate_per_s": stats.get("publish_details", {}).get("rate"),
            "deliver_rate_per_s": stats.get("deliver_get_details", {}).get("rate"),
        }

    async def dlq_peek(self, count: int = 5) -> list[dict[str, Any]]:
        try:
            resp = await self.client.post(
                f"{self.s.rabbit_api_url}/api/queues/%2F/{self.s.dlq}/get",
                auth=self._auth(),
                json={
                    "count": max(1, min(count, 20)),
                    "ackmode": "ack_requeue_true",
                    "encoding": "auto",
                },
            )
        except httpx.HTTPError as e:
            raise SourceUnavailable(f"RabbitMQ недоступен ({type(e).__name__})") from e
        _check(resp, "RabbitMQ")
        return [
            {
                "payload": redact(m.get("payload", ""), max_len=300),
                "reason": (m.get("properties", {}).get("headers", {}).get("x-first-death-reason")),
            }
            for m in resp.json()
        ]

    async def snapshot(self) -> dict[str, Any]:
        queries = {
            "source_rate_per_s": "sum(rate(source_published_total[30s]))",
            "processed_ok_per_s": 'sum(rate(worker_tasks_processed_total{outcome="ok"}[30s]))',
            "inference_p95_s": (
                "histogram_quantile(0.95, sum by (le) (rate(worker_inference_duration_seconds_bucket[1m])))"
            ),
        }
        tasks = {
            "queue": self.queue_info(self.s.queue),
            "dlq": self.queue_info(self.s.dlq),
            **{k: self.prom_query(q) for k, q in queries.items()},
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        out: dict[str, Any] = {}
        for key, res in zip(tasks, results, strict=True):
            if isinstance(res, BaseException):
                out[key] = f"недоступно: {res}"
            elif key in queries:
                out[key] = res[0]["value"] if res else "нет данных (ряд метрики пуст)"
            else:
                out[key] = res
        return out
