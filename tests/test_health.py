from unittest.mock import AsyncMock, patch

from httpx import AsyncClient


async def test_health_all_ok(client: AsyncClient) -> None:
    with patch("app.api.v1.health.check_db_connection", new_callable=AsyncMock) as mock_db:
        mock_db.return_value = (True, "PostgreSQL 16.0")
        resp = await client.get("/api/v1/health")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["components"]["postgresql"]["status"] == "ok"
    assert "response_time_s" in data["components"]["postgresql"]


async def test_health_db_down(client: AsyncClient) -> None:
    with patch("app.api.v1.health.check_db_connection", new_callable=AsyncMock) as mock_db:
        mock_db.return_value = (False, "connection refused")
        resp = await client.get("/api/v1/health")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "degraded"
    assert data["components"]["postgresql"]["status"] == "error"
