import asyncio
import os

import mlflow
import mlflow.pyfunc
import pandas as pd
import structlog
from faststream.rabbit import RabbitQueue
from pydantic import BaseModel, Field

from app.broker import broker

os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "minio")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minio123")

logger = structlog.get_logger(__name__)

INFERENCE_DELAY_SEC = float(os.getenv("INFERENCE_DELAY_SEC", "0.5"))
MLFLOW_MODEL_URI = os.getenv("MLFLOW_MODEL_URI", "models:/Dota2WinPredictor@champion")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")

CONCURRENCY_LIMIT = 2
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

model = None


class TaskMessage(BaseModel):
    task_id: str = Field(..., description="Уникальный ID задачи для идемпотентности")
    duration: float
    radiant_score: float
    dire_score: float
    first_blood_time: float


async def load_model():
    global model
    logger.info("mlflow_startup", tracking_uri=MLFLOW_TRACKING_URI)
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    logger.info("loading_model_from_registry", uri=MLFLOW_MODEL_URI)
    model = mlflow.pyfunc.load_model(MLFLOW_MODEL_URI)
    logger.info("model_loaded_successfully")


@broker.subscriber(
    queue=RabbitQueue(
        "dota_inference_queue",
        durable=True,
        arguments={"x-max-length": 20, "x-overflow": "reject-publish"},
    )
)
async def process_task(msg: TaskMessage):
    logger.info("task_received", task_id=msg.task_id)

    async with semaphore:
        await asyncio.sleep(INFERENCE_DELAY_SEC)

        features_df = pd.DataFrame(
            [
                {
                    "duration": msg.duration,
                    "radiant_score": msg.radiant_score,
                    "dire_score": msg.dire_score,
                    "first_blood_time": msg.first_blood_time,
                }
            ]
        )

        prediction = await asyncio.to_thread(model.predict, features_df)

        result = {"task_id": msg.task_id, "prediction": int(prediction[0]), "probability": 0.95}

        logger.info("task_completed", task_id=msg.task_id, prediction=result["prediction"])
        return result


async def main():
    logger.info("starting_worker")

    await load_model()

    await broker.start()
    logger.info("worker_is_listening_for_messages")

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        logger.info("shutting_down_worker")
        await broker.close()


if __name__ == "__main__":
    asyncio.run(main())
