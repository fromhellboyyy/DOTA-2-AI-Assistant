from unittest.mock import patch

from httpx import AsyncClient


async def test_process_returns_radiant_win(client: AsyncClient) -> None:
    with (
        patch("app.api.v1.process.is_model_loaded", return_value=True),
        patch("app.api.v1.process.predict", return_value=1),
    ):
        resp = await client.post(
            "/api/v1/process",
            json={
                "duration": 2400,
                "radiant_score": 30,
                "dire_score": 25,
                "first_blood_time": 120,
            },
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["prediction"] == 1
    assert data["meaning"] == "Radiant Win"


async def test_process_returns_dire_win(client: AsyncClient) -> None:
    with (
        patch("app.api.v1.process.is_model_loaded", return_value=True),
        patch("app.api.v1.process.predict", return_value=0),
    ):
        resp = await client.post(
            "/api/v1/process",
            json={
                "duration": 1800,
                "radiant_score": 10,
                "dire_score": 25,
                "first_blood_time": 60,
            },
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["prediction"] == 0
    assert data["meaning"] == "Dire Win"


async def test_process_model_not_loaded_returns_503(client: AsyncClient) -> None:
    with patch("app.api.v1.process.is_model_loaded", return_value=False):
        resp = await client.post(
            "/api/v1/process",
            json={
                "duration": 2400,
                "radiant_score": 30,
                "dire_score": 25,
                "first_blood_time": 120,
            },
        )

    assert resp.status_code == 503
    assert resp.json()["detail"] == "Model is not loaded"


async def test_process_invalid_payload_returns_422(client: AsyncClient) -> None:
    with patch("app.api.v1.process.is_model_loaded", return_value=True):
        resp = await client.post(
            "/api/v1/process",
            json={"duration": -100},  
        )

    assert resp.status_code == 422
