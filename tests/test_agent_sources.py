import asyncio
import json

import httpx
import pytest
from agent.actions import Actions
from agent.config import Settings
from agent.redact import redact
from agent.sources import BadQuery, Sources, SourceUnavailable

S = Settings(base_url="http://gw/v1", api_key="sk-test-key-123456")


def make(handler):
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return Sources(S, client), Actions(S, client, runner=_fake_runner), client


calls: list[tuple[list[str], dict[str, str]]] = []


async def _fake_runner(cmd, env, cwd):
    calls.append((cmd, env))
    return 0, "Container worker-1 Started\nContainer worker-2 Started\n"


def test_redact_masks_secrets():
    assert "hunter2" not in redact('login password="hunter2" ok')
    assert "sk-abcdefghijk" not in redact("key sk-abcdefghijklmnop")
    assert "rabbit:***@" in redact("amqp://rabbit:secretpass@rabbitmq:5672/")
    assert redact("x" * 1000).endswith("...")


def test_settings_repr_hides_key():
    assert "sk-test" not in repr(S)


def test_loki_query_flattens_and_redacts():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/loki/api/v1/query_range"
        return httpx.Response(
            200,
            json={
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {"service": "worker"},
                            "values": [
                                ["2", 'level=error msg="boom" token=abc123'],
                                ["1", "older"],
                            ],
                        },
                    ],
                }
            },
        )

    src, _, client = make(handler)
    res = asyncio.run(src.loki_query('{service="worker"}', 5, 10))
    assert res["count"] == 2
    assert res["lines"][0].startswith("[worker] level=error")
    assert "abc123" not in res["lines"][0]
    asyncio.run(client.aclose())


def test_loki_bad_query_is_bad_query_error():
    src, _, _ = make(lambda r: httpx.Response(400, text="parse error"))
    with pytest.raises(BadQuery):
        asyncio.run(src.loki_query("oops"))


def test_unreachable_source_is_unavailable():
    def handler(request):
        raise httpx.ConnectError("down")

    src, _, _ = make(handler)
    with pytest.raises(SourceUnavailable):
        asyncio.run(src.prom_query("up"))


def test_queue_info_fill_pct():
    payload = {
        "state": "running",
        "messages_ready": 18,
        "messages_unacknowledged": 4,
        "consumers": 1,
        "arguments": {"x-max-length": 20},
        "message_stats": {"publish_details": {"rate": 9.5}, "deliver_get_details": {"rate": 4.0}},
    }
    src, _, _ = make(lambda r: httpx.Response(200, json=payload))
    info = asyncio.run(src.queue_info("q"))
    assert info["fill_pct"] == 90.0 and info["consumers"] == 1


def test_snapshot_survives_partial_failure():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.port == 8428:
            raise httpx.ConnectError("vm down")
        return httpx.Response(200, json={"messages_ready": 1, "arguments": {"x-max-length": 20}})

    src, _, _ = make(handler)
    snap = asyncio.run(src.snapshot())
    assert snap["queue"]["ready"] == 1
    assert str(snap["source_rate_per_s"]).startswith("недоступно")


def test_scale_workers_validates_and_runs():
    _, act, _ = make(lambda r: httpx.Response(200))
    calls.clear()
    out = asyncio.run(act.scale_workers(2))
    assert "--scale" in calls[0][0] and "worker=2" in calls[0][0]
    assert calls[0][1] == {}
    assert "пересоздать" in out
    with pytest.raises(ValueError):
        asyncio.run(act.scale_workers(99))


def test_requeue_dlq_skips_poison():
    queue = [
        {"payload": json.dumps({"task_id": "poison-1"})},
        {"payload": json.dumps({"task_id": "t-1"})},
    ]
    published: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/get"):
            return httpx.Response(200, json=[queue.pop(0)] if queue else [])
        published.append(json.loads(request.content))
        return httpx.Response(200, json={"routed": True})

    _, act, _ = make(handler)
    res = asyncio.run(act.requeue_dlq(10))
    assert res == {"requeued": 1, "dropped_poison": 1}
    assert published[0]["routing_key"] == S.queue
