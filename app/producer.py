import asyncio
import os

import pandas as pd
import structlog
from aio_pika.exceptions import DeliveryError
from faststream.rabbit import RabbitBroker

from app.broker import RABBITMQ_URL
from app.queues import MAIN_QUEUE

logger = structlog.get_logger(__name__)

CSV_PATH = os.getenv("SCORING_CSV", "ml/data/matches.csv")
RETRY_DELAY_SEC = float(os.getenv("PUBLISH_RETRY_DELAY_SEC", "0.5"))
MAX_BACKOFF_SEC = float(os.getenv("PUBLISH_MAX_BACKOFF_SEC", "5.0"))


async def publish_all(df: pd.DataFrame) -> tuple[int, int]:
    broker = RabbitBroker(RABBITMQ_URL)
    await broker.connect()
    published = 0
    nacks = 0
    try:
        for row in df.itertuples(index=False):
            task = {
                "task_id": f"score-{row.match_id}",
                "duration": float(row.duration),
                "radiant_score": float(row.radiant_score),
                "dire_score": float(row.dire_score),
                "first_blood_time": float(row.first_blood_time),
            }
            delay = RETRY_DELAY_SEC
            while True:
                try:
                    await broker.publish(task, queue=MAIN_QUEUE)
                    published += 1
                    break
                except DeliveryError:
                    nacks += 1
                    logger.warning(
                        "backpressure_nack",
                        task_id=task["task_id"],
                        retry_in_sec=delay,
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, MAX_BACKOFF_SEC)
            if published % 20 == 0:
                logger.info("progress", published=published, nacks=nacks)
    finally:
        await broker.stop()
    return published, nacks


async def main() -> None:
    df = pd.read_csv(CSV_PATH)
    df = df[df["duration"] > 0]
    logger.info("scoring_started", rows=len(df))
    published, nacks = await publish_all(df)
    logger.info("scoring_finished", published=published, nacks=nacks)


if __name__ == "__main__":
    asyncio.run(main())
