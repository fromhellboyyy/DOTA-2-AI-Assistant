import asyncio
import csv
import os
import types
from collections.abc import Iterable
from typing import Any

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


def _row_value(row: Any, name: str) -> Any:
    if isinstance(row, dict):
        return row[name]
    return getattr(row, name)


def iter_csv_records(path: str) -> Iterable[types.SimpleNamespace]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                duration = float(row["duration"])
                match_id = row["match_id"]
                radiant_score = float(row["radiant_score"])
                dire_score = float(row["dire_score"])
                first_blood_time = float(row["first_blood_time"])
            except (KeyError, ValueError):
                continue

            if duration <= 0:
                continue

            yield types.SimpleNamespace(
                match_id=match_id,
                duration=duration,
                radiant_score=radiant_score,
                dire_score=dire_score,
                first_blood_time=first_blood_time,
            )


async def publish_all(records: Iterable[Any] | pd.DataFrame) -> tuple[int, int]:
    broker = RabbitBroker(RABBITMQ_URL)
    await broker.connect()

    published = 0
    nacks = 0

    try:
        iterator = records.itertuples(index=False) if hasattr(records, "itertuples") else records

        for row in iterator:
            duration = float(_row_value(row, "duration"))
            if duration <= 0:
                continue

            task = {
                "task_id": f"score-{_row_value(row, 'match_id')}",
                "duration": duration,
                "radiant_score": float(_row_value(row, "radiant_score")),
                "dire_score": float(_row_value(row, "dire_score")),
                "first_blood_time": float(_row_value(row, "first_blood_time")),
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
    logger.info("scoring_started", csv_path=CSV_PATH)
    published, nacks = await publish_all(iter_csv_records(CSV_PATH))
    logger.info("scoring_finished", published=published, nacks=nacks)


if __name__ == "__main__":
    asyncio.run(main())
