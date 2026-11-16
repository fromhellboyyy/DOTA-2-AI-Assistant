import asyncio
import contextlib
import contextvars
import functools
import json
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor

import aio_pika
import mlflow
import mlflow.pyfunc
import pandas as pd
import structlog
from aio_pika.exceptions import DeliveryError
from aiohttp import web
from faststream.rabbit import RabbitMessage
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from pydantic import BaseModel, Field

from app.broker import PREFETCH_COUNT, broker
from app.db.results import ensure_schema, save_result
from app.logger import setup_logging
from app.metrics import (
    WORKER_DEAD_LETTERS,
    WORKER_IN_FLIGHT,
    WORKER_INFERENCE,
    WORKER_PROCESSED,
    WORKER_REDELIVERIES,
)
from app.ml.registry import resolve_model_version
from app.queues import DLQ, DLX, MAIN_QUEUE, MAX_QUEUE_SIZE, QUEUE_NAME
from app.telemetry import setup_telemetry, shutdown_telemetry

os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "minio")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minio123")

logger = structlog.get_logger(__name__)
tracer = trace.get_tracer("app.worker")

INFERENCE_DELAY_SEC = float(os.getenv("INFERENCE_DELAY_SEC", "0.5"))
MLFLOW_MODEL_URI = os.getenv("MLFLOW_MODEL_URI", "models:/Dota2WinPredictor@champion")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", "1"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
HEALTH_PORT = int(os.getenv("WORKER_HEALTH_PORT", "8081"))
POISON_PREFIX = os.getenv("POISON_PREFIX", "poison-")

executor = ThreadPoolExecutor(max_workers=CONCURRENCY_LIMIT)
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
stop_event = asyncio.Event()
in_flight = 0
model = None
model_alias = "unknown"
model_version = "unknown"

_QOS_PATCHED = False


def _patch_aio_pika_qos() -> None:
    global _QOS_PATCHED

    if _QOS_PATCHED:
        return

    original_channel = aio_pika.Connection.channel

    async def patched_channel(self, *args, **kwargs):
        channel = await original_channel(self, *args, **kwargs)

        try:
            await channel.set_qos(prefetch_count=PREFETCH_COUNT)
            logger.debug(
                "aio_pika_channel_qos_applied",
                prefetch_count=PREFETCH_COUNT,
            )
        except Exception as exc:
            logger.warning(
                "aio_pika_channel_qos_failed",
                error=str(exc),
            )

        return channel

    aio_pika.Connection.channel = patched_channel

    with contextlib.suppress(ImportError, AttributeError):
        import aio_pika.robust_connection as robust_module

        robust_module.RobustConnection.channel = patched_channel

    _QOS_PATCHED = True


_patch_aio_pika_qos()


class TaskMessage(BaseModel):
    task_id: str = Field(..., description="Idempotency key")
    duration: float
    radiant_score: float
    dire_score: float
    first_blood_time: float
    attempt: int = 0


async def load_model() -> None:
    global model, model_alias, model_version

    logger.info("loading_model_from_registry", uri=MLFLOW_MODEL_URI)
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model = mlflow.pyfunc.load_model(MLFLOW_MODEL_URI)
    model_alias, model_version = resolve_model_version(MLFLOW_MODEL_URI, MLFLOW_TRACKING_URI)
    logger.info(
        "model_loaded_successfully",
        uri=MLFLOW_MODEL_URI,
        alias=model_alias,
        version=model_version,
    )


def _predict_sync(features: pd.DataFrame, poison: bool = False) -> int:
    started = time.perf_counter()
    try:
        with tracer.start_as_current_span(
            "inference",
            attributes={
                "model.alias": model_alias,
                "model.version": model_version,
                "inference.delay_sec": INFERENCE_DELAY_SEC,
            },
        ) as span:
            time.sleep(INFERENCE_DELAY_SEC)
            if poison:
                raise ValueError("poison message: inference failed on purpose")
            prediction = int(model.predict(features)[0])
            span.set_attribute("inference.prediction", prediction)
            return prediction
    finally:
        WORKER_INFERENCE.record(time.perf_counter() - started)


def _extract_task_id(body: bytes) -> str | None:
    try:
        payload = json.loads(body)
    except (ValueError, TypeError):
        return None
    if isinstance(payload, dict):
        value = payload.get("task_id")
        return str(value) if value is not None else None
    return None


async def _dead_letter(msg: TaskMessage, span: trace.Span, reason: str) -> None:
    try:
        await broker.publish(msg.model_dump(), queue=DLQ)
    except DeliveryError:
        logger.critical("task_lost_dlq_reject", reason=reason)
        return
    WORKER_PROCESSED.add(1, {"outcome": "dlq"})
    span.add_event("dead_lettered", {"reason": reason, "attempt": msg.attempt})
    logger.error("task_dead_lettered", reason=reason, max_retries=MAX_RETRIES)


async def _handle_failure(msg: TaskMessage, exc: Exception, span: trace.Span) -> None:
    will_retry = msg.attempt + 1 < MAX_RETRIES

    WORKER_PROCESSED.add(1, {"outcome": "error"})
    span.record_exception(exc)
    span.set_status(Status(StatusCode.ERROR, str(exc)))
    logger.error(
        "task_failed",
        error=str(exc),
        will_retry=will_retry,
        max_retries=MAX_RETRIES,
        exc_info=True,
    )

    if not will_retry:
        await _dead_letter(msg, span, "retries_exhausted")
        return

    retry_msg = msg.model_copy(update={"attempt": msg.attempt + 1})
    try:
        await broker.publish(retry_msg.model_dump(), queue=MAIN_QUEUE)
    except DeliveryError:
        await _dead_letter(msg, span, "retry_rejected_by_broker")
        return
    span.add_event("retry_scheduled", {"next_attempt": retry_msg.attempt})
    logger.warning("task_retry_scheduled", next_attempt=retry_msg.attempt)


@broker.subscriber(queue=MAIN_QUEUE)
async def process_task(msg: TaskMessage) -> None:
    global in_flight

    with structlog.contextvars.bound_contextvars(task_id=msg.task_id, attempt=msg.attempt):
        span = trace.get_current_span()
        span.set_attribute("task.id", msg.task_id)
        span.set_attribute("task.attempt", msg.attempt)

        if msg.attempt > 0:
            WORKER_REDELIVERIES.add(1)

        async with semaphore:
            in_flight += 1
            WORKER_IN_FLIGHT.add(1)

            try:
                logger.info("task_received")

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

                poisoned = msg.task_id.startswith(POISON_PREFIX)
                predict_call = (
                    functools.partial(_predict_sync, poison=True) if poisoned else _predict_sync
                )

                loop = asyncio.get_running_loop()
                context = contextvars.copy_context()
                prediction = await loop.run_in_executor(
                    executor, context.run, predict_call, features
                )
                inserted = await save_result(msg.task_id, prediction)

                WORKER_PROCESSED.add(1, {"outcome": "ok"})
                logger.info("task_completed", prediction=prediction, inserted=inserted)

            except Exception as exc:
                await _handle_failure(msg, exc, span)

            finally:
                in_flight -= 1
                WORKER_IN_FLIGHT.add(-1)


@broker.subscriber(queue=DLQ, exchange=DLX)
async def dead_letter_handler(msg: RabbitMessage) -> None:
    task_id = _extract_task_id(msg.body)
    WORKER_DEAD_LETTERS.add(1)
    with structlog.contextvars.bound_contextvars(task_id=task_id):
        logger.error("dead_letter_received", size_bytes=len(msg.body))


async def _health_handler(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "in_flight": in_flight})


async def _start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/health", _health_handler)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
    await site.start()

    logger.info("health_server_started", port=HEALTH_PORT)
    return runner


def _request_stop() -> None:
    logger.info("shutdown_signal_received", phase="graceful_shutdown_start", in_flight=in_flight)
    stop_event.set()


async def main() -> None:
    setup_logging()
    setup_telemetry(instrument_db=True)

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
        max_size=MAX_QUEUE_SIZE,
        prefetch=PREFETCH_COUNT,
        workers=CONCURRENCY_LIMIT,
    )

    await stop_event.wait()

    logger.info("draining_in_flight", in_flight=in_flight)

    await broker.stop()

    deadline = time.monotonic() + 30.0
    while in_flight > 0 and time.monotonic() < deadline:
        await asyncio.sleep(0.1)

    if in_flight > 0:
        logger.warning("shutdown_timeout_waiting_in_flight", in_flight=in_flight)

    await runner.cleanup()
    executor.shutdown(wait=True)

    logger.info("worker_stopped_gracefully", phase="graceful_shutdown_end")
    shutdown_telemetry()


if __name__ == "__main__":
    asyncio.run(main())
