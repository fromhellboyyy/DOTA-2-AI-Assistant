import os
import uuid

import structlog
from aio_pika.exceptions import DeliveryError
from fastapi import APIRouter, HTTPException
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from pydantic import BaseModel

from app.broker import broker
from app.db.results import get_result
from app.queues import MAIN_QUEUE, MAX_QUEUE_SIZE

logger = structlog.get_logger(__name__)

INFERENCE_DELAY_SEC = float(os.getenv("INFERENCE_DELAY_SEC", "0.5"))
CONCURRENCY_PER_WORKER = int(os.getenv("CONCURRENCY_LIMIT", "1"))
WORKER_REPLICAS = int(os.getenv("WORKER_REPLICAS", "1"))
RETRY_AFTER_SECONDS_ENV = int(os.getenv("RETRY_AFTER_SECONDS", "0"))

THROUGHPUT = CONCURRENCY_PER_WORKER * WORKER_REPLICAS / INFERENCE_DELAY_SEC

if RETRY_AFTER_SECONDS_ENV > 0:
    RETRY_AFTER_SEC = RETRY_AFTER_SECONDS_ENV
else:
    RETRY_AFTER_SEC = max(1, round(MAX_QUEUE_SIZE / THROUGHPUT))

router = APIRouter(tags=["tasks"])


class TaskRequest(BaseModel):
    duration: float
    radiant_score: float
    dire_score: float
    first_blood_time: float


@router.post("/tasks", status_code=202)
async def create_task(request: TaskRequest):
    task_id = str(uuid.uuid4())

    payload = {
        "task_id": task_id,
        "duration": request.duration,
        "radiant_score": request.radiant_score,
        "dire_score": request.dire_score,
        "first_blood_time": request.first_blood_time,
    }

    with structlog.contextvars.bound_contextvars(task_id=task_id):
        span = trace.get_current_span()
        span.set_attribute("task.id", task_id)

        try:
            await broker.publish(payload, queue=MAIN_QUEUE, correlation_id=task_id)
        except DeliveryError as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, "queue is full"))
            logger.warning(
                "queue_full_503",
                queue=MAIN_QUEUE.name,
                max_queue_size=MAX_QUEUE_SIZE,
                retry_after_sec=RETRY_AFTER_SEC,
            )
            raise HTTPException(
                status_code=503,
                detail="Service overloaded. Queue is full. Retry later.",
                headers={"Retry-After": str(RETRY_AFTER_SEC)},
            ) from exc

        logger.info("task_accepted", queue=MAIN_QUEUE.name)

    return {"task_id": task_id, "status": "queued"}


@router.get("/tasks/{task_id}")
async def task_result(task_id: str):
    row = await get_result(task_id)

    if row is None:
        raise HTTPException(404, detail="Result not ready or unknown task")

    return row
