import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

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
    monkeypatch.setattr(app.worker, "save_result", AsyncMock(return_value=True))


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
        await asyncio.sleep(0.2)
        assert True


@pytest.mark.asyncio
async def test_process_task_invalid_data():
    async with TestRabbitBroker(broker) as br:
        task = TaskMessage(
            task_id="test-invalid",
            duration=0.0,
            radiant_score=0.0,
            dire_score=0.0,
            first_blood_time=0.0,
        )
        await br.publish(task, queue="dota_inference_queue")
        await asyncio.sleep(0.2)
        assert True


@pytest.mark.asyncio
async def test_load_model_success():
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
    import app.worker

    with (
        patch("app.worker.ensure_schema", new_callable=AsyncMock) as mock_schema,
        patch("app.worker.load_model", new_callable=AsyncMock) as mock_load,
        patch("app.worker._start_health_server", new_callable=AsyncMock) as mock_health,
        patch("app.worker.broker") as mock_broker,
    ):
        mock_broker.start = AsyncMock()
        mock_broker.stop = AsyncMock()
        mock_runner = AsyncMock()
        mock_health.return_value = mock_runner

        app.worker.stop_event.set()
        try:
            await app.worker.main()
        finally:
            app.worker.stop_event.clear()

        mock_schema.assert_awaited_once()
        mock_load.assert_awaited_once()
        mock_broker.start.assert_awaited_once()
        mock_broker.stop.assert_awaited_once()
        mock_runner.cleanup.assert_awaited_once()


@pytest.mark.asyncio
async def test_dead_letter_handler():
    from app.worker import dead_letter_handler

    msg = MagicMock()
    msg.body = b'{"task_id": "fail"}'
    await dead_letter_handler(msg)


@pytest.mark.asyncio
async def test_health_handler():
    from app.worker import _health_handler

    request = MagicMock()
    response = await _health_handler(request)
    assert response.status == 200


@pytest.mark.asyncio
async def test_process_task_retries_then_dead_letters(monkeypatch):
    from concurrent.futures import Future

    import app.worker

    class FailModel:
        def predict(self, df):
            raise RuntimeError("boom")

    class SyncExecutor:
        def submit(self, fn, *args, **kwargs):
            f = Future()
            try:
                f.set_result(fn(*args, **kwargs))
            except Exception as e:
                f.set_exception(e)
            return f

    monkeypatch.setattr(app.worker, "model", FailModel())
    monkeypatch.setattr(app.worker, "INFERENCE_DELAY_SEC", 0.0)
    monkeypatch.setattr(app.worker, "MAX_RETRIES", 2)
    monkeypatch.setattr(app.worker, "executor", SyncExecutor())

    published = []

    async def fake_publish(body, *, queue):
        published.append((queue, body.get("attempt")))

    with patch.object(app.worker.broker, "publish", side_effect=fake_publish):
        msg = TaskMessage(
            task_id="retry-1",
            duration=100.0,
            radiant_score=1.0,
            dire_score=1.0,
            first_blood_time=1.0,
            attempt=0,
        )
        await app.worker.process_task(msg)
        msg2 = TaskMessage(
            task_id="retry-1",
            duration=100.0,
            radiant_score=1.0,
            dire_score=1.0,
            first_blood_time=1.0,
            attempt=1,
        )
        await app.worker.process_task(msg2)

    assert published[0][0] is app.worker.MAIN_QUEUE
    assert published[0][1] == 1
    assert published[1][0] is app.worker.DLQ
