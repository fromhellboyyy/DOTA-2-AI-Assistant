import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


# Тесты для /healthz
def test_healthz():
    response = client.get("/healthz")
    assert response.status_code == 200


# Тесты для /api/v1/health
def test_health():
    response = client.get("/api/v1/health")
    assert response.status_code == 200


# Тесты для /api/v1/version
def test_version():
    response = client.get("/api/v1/version")
    assert response.status_code == 200


# Тесты для /api/v1/process
@patch("app.api.v1.process.predict")
@patch("app.api.v1.process.is_model_loaded", return_value=True)
def test_process_success(mock_loaded, mock_predict):
    mock_predict.return_value = {"prediction": 1, "meaning": "Radiant Win"}

    response = client.post("/api/v1/process", json={
        "duration": 2500.0,
        "radiant_score": 35.0,
        "dire_score": 20.0,
        "first_blood_time": 120.0
    })

    assert response.status_code == 200
    assert "prediction" in response.json()


@patch("app.api.v1.process.is_model_loaded", return_value=False)
def test_process_model_not_loaded(mock_loaded):
    response = client.post("/api/v1/process", json={
        "duration": 2500.0,
        "radiant_score": 35.0,
        "dire_score": 20.0,
        "first_blood_time": 120.0
    })

    assert response.status_code == 503


# Тесты для /api/v1/tasks (Load Shedding)
@patch("app.api.v1.tasks.get_queue_depth", return_value=0)
@patch("app.api.v1.tasks.broker")
def test_create_task_success(mock_broker, mock_depth):
    mock_broker.publish = AsyncMock()

    response = client.post("/api/v1/tasks", json={
        "duration": 2500.0,
        "radiant_score": 35.0,
        "dire_score": 20.0,
        "first_blood_time": 120.0
    })

    assert response.status_code == 202
    assert "task_id" in response.json()


@patch("app.api.v1.tasks.get_queue_depth", return_value=25)
def test_create_task_overload(mock_depth):
    response = client.post("/api/v1/tasks", json={
        "duration": 2500.0,
        "radiant_score": 35.0,
        "dire_score": 20.0,
        "first_blood_time": 120.0
    })

    assert response.status_code == 503
    assert "retry-after" in response.headers