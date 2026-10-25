from unittest.mock import AsyncMock, MagicMock, patch

from aio_pika.exceptions import DeliveryError
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_healthz():
    response = client.get("/healthz")
    assert response.status_code == 200


def test_health():
    response = client.get("/api/v1/health")
    assert response.status_code == 200


def test_version():
    response = client.get("/api/v1/version")
    assert response.status_code == 200


@patch("app.api.v1.process.predict")
@patch("app.api.v1.process.is_model_loaded", return_value=True)
def test_process_success(mock_loaded, mock_predict):
    mock_predict.return_value = 1
    response = client.post(
        "/api/v1/process",
        json={
            "duration": 2500.0,
            "radiant_score": 35.0,
            "dire_score": 20.0,
            "first_blood_time": 120.0,
        },
    )
    assert response.status_code == 200
    assert "prediction" in response.json()


@patch("app.api.v1.process.is_model_loaded", return_value=False)
def test_process_model_not_loaded(mock_loaded):
    response = client.post(
        "/api/v1/process",
        json={
            "duration": 2500.0,
            "radiant_score": 35.0,
            "dire_score": 20.0,
            "first_blood_time": 120.0,
        },
    )
    assert response.status_code == 503


@patch("app.api.v1.tasks.broker")
def test_create_task_success(mock_broker):
    mock_broker.publish = AsyncMock()
    response = client.post(
        "/api/v1/tasks",
        json={
            "duration": 2500.0,
            "radiant_score": 35.0,
            "dire_score": 20.0,
            "first_blood_time": 120.0,
        },
    )
    assert response.status_code == 202
    assert "task_id" in response.json()


@patch("app.api.v1.tasks.broker")
def test_create_task_overload(mock_broker):
    mock_broker.publish = AsyncMock(side_effect=DeliveryError(MagicMock(), MagicMock()))
    response = client.post(
        "/api/v1/tasks",
        json={
            "duration": 2500.0,
            "radiant_score": 35.0,
            "dire_score": 20.0,
            "first_blood_time": 120.0,
        },
    )
    assert response.status_code == 503
    headers_lower = {k.lower(): v for k, v in response.headers.items()}
    assert "retry-after" in headers_lower


@patch("app.api.v1.tasks.get_result", new_callable=AsyncMock)
def test_task_result_found(mock_get):
    mock_get.return_value = {"task_id": "123", "prediction": 1}
    response = client.get("/api/v1/tasks/123")
    assert response.status_code == 200
    assert response.json()["prediction"] == 1


@patch("app.api.v1.tasks.get_result", new_callable=AsyncMock)
def test_task_result_not_found(mock_get):
    mock_get.return_value = None
    response = client.get("/api/v1/tasks/999")
    assert response.status_code == 404
