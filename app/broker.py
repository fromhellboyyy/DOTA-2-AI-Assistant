import contextlib
import os

from faststream.rabbit import RabbitBroker

from app.telemetry import telemetry_middlewares

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://rabbit:rabbit@localhost:5672/")
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "1"))

broker = RabbitBroker(RABBITMQ_URL, middlewares=telemetry_middlewares(), logger=None)

with contextlib.suppress(AttributeError, TypeError):
    broker.prefetch_count = PREFETCH_COUNT

consumer_kwargs = getattr(broker, "consumer_kwargs", None)

if isinstance(consumer_kwargs, dict):
    consumer_kwargs["prefetch_count"] = PREFETCH_COUNT
