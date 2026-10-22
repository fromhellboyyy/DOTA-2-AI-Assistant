import os
import asyncio
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
    async with aiohttp.ClientSession() as session:
        async with session.get(url, auth=aiohttp.BasicAuth('rabbit', 'rabbit')) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('messages', 0)
    return 0


async def main():
    broker = RabbitBroker(RABBITMQ_URL)
    await broker.connect()

    print(f" Producer started. Target queue: {QUEUE_NAME}")
    print(f"Max queue size: {MAX_QUEUE_SIZE}")
    print(" Checking queue depth before publishing (backpressure)")

    tasks_published = 0
    backpressure_hits = 0

    try:
        while True:
            queue_depth = await get_queue_depth()

            if queue_depth >= MAX_QUEUE_SIZE:
                backpressure_hits += 1
                print(
                    f"BACKPRESSURE! Queue depth: {queue_depth}/{MAX_QUEUE_SIZE}. Waiting 1 sec... (Hit #{backpressure_hits})")
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
                print(
                    f"Published: {tasks_published} | Queue depth: {queue_depth} | Backpressure hits: {backpressure_hits}")

            await asyncio.sleep(0.1)

    except KeyboardInterrupt:
        print(f"\nProducer stopped. Total published: {tasks_published}, Backpressure hits: {backpressure_hits}")


if __name__ == "__main__":
    asyncio.run(main())