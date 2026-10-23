import asyncio
import os
import uuid

import aiohttp
from faststream.rabbit import RabbitBroker
from pydantic import BaseModel

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://rabbit:rabbit@localhost:5672/")
RABBITMQ_MANAGEMENT_URL = "http://localhost:15672"
QUEUE_NAME = "dota_inference_queue"
MAX_QUEUE_SIZE = 20


class TaskMessage(BaseModel):
    task_id: str
    duration: float
    radiant_score: float
    dire_score: float
    first_blood_time: float


async def get_queue_depth() -> int:
    """Проверяет глубину очереди через RabbitMQ Management API"""
    url = f"{RABBITMQ_MANAGEMENT_URL}/api/queues/%2f/{QUEUE_NAME}"
    auth = aiohttp.BasicAuth("rabbit", "rabbit")
    async with (
        aiohttp.ClientSession() as session,
        session.get(url, auth=auth) as response,
    ):
        if response.status == 200:
            data = await response.json()
            return data.get("messages", 0)
    return 0


async def main():
    broker = RabbitBroker(RABBITMQ_URL)
    await broker.connect()

    tasks_published = 0
    backpressure_hits = 0

    try:
        while True:
            queue_depth = await get_queue_depth()

            if queue_depth >= MAX_QUEUE_SIZE:
                backpressure_hits += 1
                await asyncio.sleep(1.0)
                continue

            task = TaskMessage(
                task_id=str(uuid.uuid4()),
                duration=2500.0,
                radiant_score=35.0,
                dire_score=20.0,
                first_blood_time=120.0,
            )

            await broker.publish(task, queue=QUEUE_NAME)
            tasks_published += 1

            if tasks_published % 10 == 0:
                pass

            await asyncio.sleep(0.1)

    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    asyncio.run(main())
