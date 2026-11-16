import os

from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue

QUEUE_NAME = os.getenv("QUEUE_NAME", "dota_inference_queue")
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "20"))
DLX_NAME = "dota_dlx"
DLQ_NAME = "dota_inference_dlq"

DLX = RabbitExchange(DLX_NAME, type=ExchangeType.FANOUT, durable=True)

MAIN_QUEUE = RabbitQueue(
    QUEUE_NAME,
    durable=True,
    arguments={
        "x-max-length": MAX_QUEUE_SIZE,
        "x-overflow": "reject-publish",
        "x-dead-letter-exchange": DLX_NAME,
    },
)

DLQ = RabbitQueue(DLQ_NAME, durable=True)
