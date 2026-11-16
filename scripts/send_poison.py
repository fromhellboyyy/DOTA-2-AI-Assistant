import argparse
import asyncio
import os
import uuid

from faststream.rabbit import RabbitBroker
from opentelemetry import trace

from app.logger import setup_logging
from app.queues import MAIN_QUEUE
from app.telemetry import setup_telemetry, shutdown_telemetry, telemetry_middlewares

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://rabbit:rabbit@localhost:5672/")


async def main(count: int) -> None:
    setup_logging()
    setup_telemetry()
    tracer = trace.get_tracer("scripts.send_poison")
    broker = RabbitBroker(RABBITMQ_URL, middlewares=telemetry_middlewares(), logger=None)
    await broker.connect()
    try:
        for _ in range(count):
            task_id = f"poison-{uuid.uuid4()}"
            payload = {
                "task_id": task_id,
                "duration": 1800.0,
                "radiant_score": 20.0,
                "dire_score": 10.0,
                "first_blood_time": 60.0,
            }
            with tracer.start_as_current_span("poison.publish", attributes={"task.id": task_id}):
                await broker.publish(payload, queue=MAIN_QUEUE, correlation_id=task_id)
            print(task_id)
    finally:
        await broker.stop()
        shutdown_telemetry()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1)
    args = parser.parse_args()
    asyncio.run(main(args.count))
