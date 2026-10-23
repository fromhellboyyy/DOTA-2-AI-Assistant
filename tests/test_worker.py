import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from faststream.rabbit import TestRabbitBroker

from app.worker import TaskMessage, broker


@pytest.fixture(autouse=True)
def mock_environment(monkeypatch):
    import app.worker

    class MockModel:
        def predict(self, df):
            return [1]

    monkeypatch.setattr(app.worker, "model", MockModel())
    monkeypatch.setattr(app.worker, "INFERENCE_DELAY_SEC", 0.0)


@pytest.mark.asyncio
async def test_process_task_success():
    async with TestRabbitBroker(broker) as br:
        task = TaskMessage(
            task_id="test-123",
            duration=2500.0,
            radiant_score=35.0,
            dire_score=20.0,
            first_blood_time=120.0,
        )
        await br.publish(task, queue="dota_inference_queue")
        await asyncio.sleep(0.1)
        assert True


@pytest.mark.asyncio
async def test_process_task_invalid_data():
    """Test processing task with invalid data"""
    async with TestRabbitBroker(broker) as br:
        task = TaskMessage(
            task_id="test-invalid",
            duration=0.0,
            radiant_score=0.0,
            dire_score=0.0,
            first_blood_time=0.0,
        )
        await br.publish(task, queue="dota_inference_queue")
        await asyncio.sleep(0.1)
        assert True


@pytest.mark.asyncio
async def test_load_model_success():
    """Test successful model loading from MLflow"""
    with (
        patch("app.worker.mlflow.pyfunc.load_model") as mock_load,
        patch("app.worker.mlflow.set_tracking_uri"),
    ):
        mock_model = AsyncMock()
        mock_load.return_value = mock_model

        import app.worker
        from app.worker import load_model

        await load_model()
        assert app.worker.model is not None
        mock_load.assert_called_once()


@pytest.mark.asyncio
async def test_worker_main_flow():
    """Test worker main lifecycle"""
    with (
        patch("app.worker.load_model", new_callable=AsyncMock) as mock_load,
        patch("app.worker.broker") as mock_broker,
        patch("asyncio.Event") as mock_event_class,
    ):
        mock_broker.start = AsyncMock()
        mock_broker.close = AsyncMock()

        async def wait_side_effect():
            raise asyncio.CancelledError()

        mock_event = AsyncMock()
        mock_event.wait = AsyncMock(side_effect=wait_side_effect)
        mock_event_class.return_value = mock_event

        from app.worker import main

        await main()

        mock_load.assert_called_once()
        mock_broker.start.assert_called_once()
        mock_broker.close.assert_called_once()
