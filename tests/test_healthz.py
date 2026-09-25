from httpx import AsyncClient


async def test_healthz_returns_ok(client: AsyncClient) -> None:
    resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_healthz_method_not_allowed(client: AsyncClient) -> None:
    resp = await client.post("/healthz")
    assert resp.status_code == 405
