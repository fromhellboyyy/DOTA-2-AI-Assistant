import os
from faststream.rabbit import RabbitBroker

rabbit_url = os.getenv("RABBITMQ_URL", "amqp://rabbit:rabbit@localhost:5672/")
broker = RabbitBroker(rabbit_url)