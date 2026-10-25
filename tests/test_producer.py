from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from aio_pika.exceptions import DeliveryError

from app.producer import publish_all


@pytest.mark.asyncio
async def test_publish_all_success():
    df = pd.DataFrame(
        [
            {
                "match_id": 1,
                "duration": 100,
                "radiant_score": 10,
                "dire_score": 5,
                "first_blood_time": 60,
            }
        ]
    )
    with patch("app.producer.RabbitBroker") as mock_broker_class:
        mock_broker = AsyncMock()
        mock_broker_class.return_value = mock_broker
        mock_broker.connect = AsyncMock()
        mock_broker.close = AsyncMock()
        mock_broker.publish = AsyncMock()

        published, nacks = await publish_all(df)
        assert published == 1
        assert nacks == 0
        mock_broker.publish.assert_called_once()


@pytest.mark.asyncio
async def test_publish_all_with_nack():
    df = pd.DataFrame(
        [
            {
                "match_id": 1,
                "duration": 100,
                "radiant_score": 10,
                "dire_score": 5,
                "first_blood_time": 60,
            }
        ]
    )
    with (
        patch("app.producer.RabbitBroker") as mock_broker_class,
        patch("app.producer.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        mock_broker = AsyncMock()
        mock_broker_class.return_value = mock_broker
        mock_broker.connect = AsyncMock()
        mock_broker.close = AsyncMock()

        call_count = {"n": 0}

        async def publish_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise DeliveryError(MagicMock(), MagicMock())

        mock_broker.publish = AsyncMock(side_effect=publish_side_effect)

        published, nacks = await publish_all(df)
        assert published == 1
        assert nacks == 1
        mock_sleep.assert_called_once()
