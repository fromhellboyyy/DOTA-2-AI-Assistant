import os

from faststream.rabbit import RabbitBroker

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://rabbit:rabbit@localhost:5672/")
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "2"))

broker = RabbitBroker(RABBITMQ_URL)
broker.prefetch_count = PREFETCH_COUNT
