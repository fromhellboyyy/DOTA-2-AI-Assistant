import os
import uuid

from aio_pika.exceptions import DeliveryError
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.broker import broker
from app.db.results import get_result
from app.queues import MAIN_QUEUE, MAX_QUEUE_SIZE

INFERENCE_DELAY_SEC = float(os.getenv("INFERENCE_DELAY_SEC", "0.5"))
CONCURRENCY_PER_WORKER = int(os.getenv("CONCURRENCY_LIMIT", "2"))
WORKER_REPLICAS = int(os.getenv("WORKER_REPLICAS", "1"))

THROUGHPUT = CONCURRENCY_PER_WORKER * WORKER_REPLICAS / INFERENCE_DELAY_SEC
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
    try:
        await broker.publish(payload, queue=MAIN_QUEUE)
    except DeliveryError as exc:
        raise HTTPException(
            status_code=503,
            detail="Service overloaded. Queue is full. Retry later.",
            headers={"Retry-After": str(RETRY_AFTER_SEC)},
        ) from exc
    return {"task_id": task_id, "status": "queued"}


@router.get("/tasks/{task_id}")
async def task_result(task_id: str):
    row = await get_result(task_id)
    if row is None:
        raise HTTPException(404, detail="Result not ready or unknown task")
    return row
