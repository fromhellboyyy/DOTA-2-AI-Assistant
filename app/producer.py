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
from opentelemetry import trace
from opentelemetry.context import Context

from app.broker import RABBITMQ_URL
from app.logger import setup_logging
from app.metrics import SOURCE_NACKS, SOURCE_PUBLISHED, SOURCE_WAIT
from app.queues import MAIN_QUEUE
from app.telemetry import setup_telemetry, shutdown_telemetry, telemetry_middlewares

logger = structlog.get_logger(__name__)
tracer = trace.get_tracer("app.source")

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


async def _publish_with_backoff(broker: RabbitBroker, task: dict[str, Any]) -> tuple[int, int]:
    attempt = 0
    nacks = 0
    delay = RETRY_DELAY_SEC
    span = trace.get_current_span()

    while True:
        attempt += 1
        try:
            await broker.publish(task, queue=MAIN_QUEUE, correlation_id=task["task_id"])
        except DeliveryError:
            nacks += 1
            SOURCE_NACKS.add(1)
            span.add_event("broker_nack", {"attempt": attempt, "retry_in_sec": delay})
            logger.warning("backpressure_nack", attempt=attempt, retry_in_sec=delay)
            with tracer.start_as_current_span(
                "source.backoff",
                attributes={"backoff.seconds": delay, "publish.attempt": attempt},
            ):
                await asyncio.sleep(delay)
            SOURCE_WAIT.add(delay)
            delay = min(delay * 2, MAX_BACKOFF_SEC)
        else:
            SOURCE_PUBLISHED.add(1)
            span.set_attribute("publish.attempts", attempt)
            return 1, nacks


async def publish_all(records: Iterable[Any] | pd.DataFrame) -> tuple[int, int]:
    broker = RabbitBroker(RABBITMQ_URL, middlewares=telemetry_middlewares(), logger=None)
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

            with (
                structlog.contextvars.bound_contextvars(task_id=task["task_id"]),
                tracer.start_as_current_span(
                    "source.publish_task",
                    context=Context(),
                    attributes={"task.id": task["task_id"]},
                ),
            ):
                sent, rejected = await _publish_with_backoff(broker, task)

            published += sent
            nacks += rejected

            if published % 20 == 0:
                logger.info("progress", published=published, nacks=nacks)

    finally:
        await broker.stop()

    return published, nacks


async def main() -> None:
    setup_logging()
    setup_telemetry()
    logger.info("scoring_started", csv_path=CSV_PATH)
    published, nacks = await publish_all(iter_csv_records(CSV_PATH))
    logger.info("scoring_finished", published=published, nacks=nacks)
    shutdown_telemetry()


if __name__ == "__main__":
    asyncio.run(main())
