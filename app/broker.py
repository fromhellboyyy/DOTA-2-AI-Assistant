import contextlib
import os

from faststream.rabbit import RabbitBroker

from app.capacity import prefetch_count

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://rabbit:rabbit@localhost:5672/")
PREFETCH_COUNT = prefetch_count()

broker = RabbitBroker(RABBITMQ_URL, logger=None)

with contextlib.suppress(AttributeError, TypeError):
    broker.prefetch_count = PREFETCH_COUNT

consumer_kwargs = getattr(broker, "consumer_kwargs", None)

if isinstance(consumer_kwargs, dict):
    consumer_kwargs["prefetch_count"] = PREFETCH_COUNT
