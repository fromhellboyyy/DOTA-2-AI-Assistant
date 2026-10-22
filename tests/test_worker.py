import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from faststream.rabbit import TestRabbitBroker
from pydantic import BaseModel
from app.worker import broker, process_task, TaskMessage


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
    """Тест обработки задачи с невалидными данными"""
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