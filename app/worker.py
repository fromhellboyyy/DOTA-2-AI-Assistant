import asyncio
import contextlib
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor

import mlflow
import mlflow.pyfunc
import pandas as pd
import structlog
from aiohttp import web
from faststream.rabbit import (
    ExchangeType,
    RabbitExchange,
    RabbitMessage,
    RabbitQueue,
)
from pydantic import BaseModel, Field

from app.broker import PREFETCH_COUNT, broker
from app.db.results import ensure_schema, save_result

os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "minio")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minio123")

logger = structlog.get_logger(__name__)

INFERENCE_DELAY_SEC = float(os.getenv("INFERENCE_DELAY_SEC", "0.5"))
MLFLOW_MODEL_URI = os.getenv("MLFLOW_MODEL_URI", "models:/Dota2WinPredictor@champion")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", "2"))
QUEUE_NAME = os.getenv("QUEUE_NAME", "dota_inference_queue")
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "20"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
HEALTH_PORT = int(os.getenv("WORKER_HEALTH_PORT", "8081"))

DLX_NAME = "dota_dlx"
DLQ_NAME = "dota_inference_dlq"

executor = ThreadPoolExecutor(max_workers=CONCURRENCY_LIMIT)
stop_event = asyncio.Event()
in_flight = 0
model = None


class TaskMessage(BaseModel):
    task_id: str = Field(..., description="Idempotency key")
    duration: float
    radiant_score: float
    dire_score: float
    first_blood_time: float
    attempt: int = 0


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


async def load_model() -> None:
    global model
    logger.info("loading_model_from_registry", uri=MLFLOW_MODEL_URI)
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model = mlflow.pyfunc.load_model(MLFLOW_MODEL_URI)
    logger.info("model_loaded_successfully")


def _predict_sync(features: pd.DataFrame) -> int:
    time.sleep(INFERENCE_DELAY_SEC)
    return int(model.predict(features)[0])


@broker.subscriber(queue=MAIN_QUEUE)
async def process_task(msg: TaskMessage) -> None:
    global in_flight
    in_flight += 1
    try:
        logger.info("task_received", task_id=msg.task_id, attempt=msg.attempt)
        features = pd.DataFrame(
            [
                {
                    "duration": msg.duration,
                    "radiant_score": msg.radiant_score,
                    "dire_score": msg.dire_score,
                    "first_blood_time": msg.first_blood_time,
                }
            ]
        )
        loop = asyncio.get_running_loop()
        prediction = await loop.run_in_executor(executor, _predict_sync, features)
        inserted = await save_result(msg.task_id, prediction)
        logger.info(
            "task_completed",
            task_id=msg.task_id,
            prediction=prediction,
            inserted=inserted,
        )
    except Exception as exc:
        logger.warning(
            "task_failed",
            task_id=msg.task_id,
            attempt=msg.attempt,
            error=str(exc),
        )
        if msg.attempt + 1 < MAX_RETRIES:
            retry_msg = msg.model_copy(update={"attempt": msg.attempt + 1})
            await broker.publish(retry_msg.model_dump(), queue=QUEUE_NAME, persistent=True)
        else:
            await broker.publish(msg.model_dump(), queue=DLQ_NAME, persistent=True)
            logger.error("task_dead_lettered", task_id=msg.task_id)
    finally:
        in_flight -= 1


@broker.subscriber(queue=DLQ, exchange=DLX)
async def dead_letter_handler(msg: RabbitMessage) -> None:
    logger.error("dead_letter_received", body=msg.body.decode(errors="replace"))


async def _health_handler(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "in_flight": in_flight})


async def _start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/health", _health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
    await site.start()
    logger.info("health_server_started", port=HEALTH_PORT)
    return runner


def _request_stop() -> None:
    logger.info("shutdown_signal_received")
    stop_event.set()


async def main() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_stop)
    await ensure_schema()
    await load_model()
    runner = await _start_health_server()
    await broker.start()
    logger.info(
        "worker_listening",
        queue=QUEUE_NAME,
        prefetch=PREFETCH_COUNT,
        workers=CONCURRENCY_LIMIT,
    )
    await stop_event.wait()
    logger.info("draining_in_flight", in_flight=in_flight)
    await broker.stop()
    await runner.cleanup()
    executor.shutdown(wait=True)
    logger.info("worker_stopped_gracefully")


if __name__ == "__main__":
    asyncio.run(main())
