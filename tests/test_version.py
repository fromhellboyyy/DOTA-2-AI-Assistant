from httpx import AsyncClient

import app


async def test_version_returns_current(client: AsyncClient) -> None:
    resp = await client.get("/api/v1/version")
    assert resp.status_code == 200
    data = resp.json()
    assert data["version"] == app.__version__


async def test_version_format(client: AsyncClient) -> None:
    resp = await client.get("/api/v1/version")
    parts = resp.json()["version"].split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)
