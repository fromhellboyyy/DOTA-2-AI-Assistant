import asyncio
import os

import aio_pika

QUEUE = os.getenv("QUEUE_NAME", "dota_inference_queue")
URL = os.getenv("RABBITMQ_URL_LOCAL", "amqp://rabbit:rabbit@localhost:5672/")
MAX_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "20"))


async def main() -> None:
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
            "x-max-length": MAX_SIZE,
            "x-overflow": "reject-publish",
        },
    )

    await conn.close()
    print(f"queue {QUEUE} ensured x-max-length={MAX_SIZE}")


if __name__ == "__main__":
    asyncio.run(main())
