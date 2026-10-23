from unittest.mock import AsyncMock, patch

import pytest

from app.producer import main


@pytest.mark.asyncio
async def test_producer_main_backpressure():
    """Test producer backpressure logic"""
    with (
        patch("app.producer.RabbitBroker") as mock_broker_class,
        patch(
            "app.producer.get_queue_depth",
            new_callable=AsyncMock,
            return_value=25,
        ),
        patch("app.producer.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        mock_broker = AsyncMock()
        mock_broker_class.return_value = mock_broker
        mock_broker.connect = AsyncMock()
        mock_broker.publish = AsyncMock()

        call_count = {"n": 0}

        async def sleep_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] > 2:
                raise KeyboardInterrupt()

        mock_sleep.side_effect = sleep_side_effect

        await main()


@pytest.mark.asyncio
async def test_producer_main_normal_publish():
    """Test producer normal publish flow"""
    with (
        patch("app.producer.RabbitBroker") as mock_broker_class,
        patch(
            "app.producer.get_queue_depth",
            new_callable=AsyncMock,
            return_value=5,
        ),
        patch("app.producer.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        mock_broker = AsyncMock()
        mock_broker_class.return_value = mock_broker
        mock_broker.connect = AsyncMock()
        mock_broker.publish = AsyncMock()

        call_count = {"n": 0}

        async def sleep_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] > 3:
                raise KeyboardInterrupt()

        mock_sleep.side_effect = sleep_side_effect

        await main()
        assert mock_broker.publish.called
