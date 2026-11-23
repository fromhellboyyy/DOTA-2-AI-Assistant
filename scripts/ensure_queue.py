import asyncio
import os
import sys
from pathlib import Path

import aio_pika

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.capacity import queue_limit

QUEUE = os.getenv("QUEUE_NAME", "dota_inference_queue")
URL = os.getenv("RABBITMQ_URL_LOCAL", "amqp://rabbit:rabbit@localhost:5672/")
DLX_NAME = "dota_dlx"


async def main() -> None:
    max_size = queue_limit()
    conn = await aio_pika.connect(URL)
    channel = await conn.channel()

    try:
        await channel.queue_delete(QUEUE)
    except Exception:
        pass

    await channel.declare_queue(
        QUEUE,
        durable=True,
        auto_delete=False,
        arguments={
            "x-max-length": max_size,
            "x-overflow": "reject-publish",
            "x-dead-letter-exchange": DLX_NAME,
        },
    )

    await conn.close()
    print(f"queue {QUEUE} ensured x-max-length={max_size}")


if __name__ == "__main__":
    asyncio.run(main())
