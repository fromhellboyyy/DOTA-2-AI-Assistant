import uuid

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.broker import broker

router = APIRouter()

RABBITMQ_URL = "amqp://rabbit:rabbit@localhost:5672/"
RABBITMQ_MANAGEMENT_URL = "http://rabbitmq:15672"
QUEUE_NAME = "dota_inference_queue"
MAX_QUEUE_SIZE = 20


class TaskRequest(BaseModel):
    duration: float
    radiant_score: float
    dire_score: float
    first_blood_time: float


async def get_queue_depth() -> int:
    """Проверяет глубину очереди через RabbitMQ Management API"""
    url = f"{RABBITMQ_MANAGEMENT_URL}/api/queues/%2f/{QUEUE_NAME}"
    auth = httpx.BasicAuth("rabbit", "rabbit")
    async with (
        httpx.AsyncClient() as client,
    ):
        try:
            response = await client.get(url, auth=auth)
            if response.status_code == 200:
                return response.json().get("messages", 0)
        except httpx.RequestError:
            pass
    return 0


@router.post("/tasks", status_code=202)
async def create_task(request: TaskRequest):
    queue_depth = await get_queue_depth()
    if queue_depth >= MAX_QUEUE_SIZE:
        raise HTTPException(
            status_code=503,
            detail="Service overloaded. Queue is full. Please retry later.",
            headers={"Retry-After": "5"},
        )
    task_id = str(uuid.uuid4())
    task_message = {
        "task_id": task_id,
        "duration": request.duration,
        "radiant_score": request.radiant_score,
        "dire_score": request.dire_score,
        "first_blood_time": request.first_blood_time,
    }
    await broker.publish(task_message, queue=QUEUE_NAME)
    return {"task_id": task_id, "status": "queued"}
