from __future__ import annotations

import os
import time
from typing import Any

import mlflow.pyfunc
import pandas as pd
import structlog

logger = structlog.get_logger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_URI = "models:/Dota2WinPredictor@champion"
MAX_RETRIES = 10
RETRY_DELAY_SEC = 5

_model: Any | None = None


def load_model() -> None:
    global _model

    logger.info("model_loading_started", tracking_uri=MLFLOW_TRACKING_URI, model_uri=MODEL_URI)
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            _model = mlflow.pyfunc.load_model(MODEL_URI)
            logger.info("model_loaded_successfully", model_uri=MODEL_URI, attempt=attempt)
            return
        except Exception as exc:
            logger.warning(
                "model_load_attempt_failed",
                attempt=attempt,
                max_retries=MAX_RETRIES,
                error=str(exc),
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SEC)

    logger.error("model_load_failed_all_retries", model_uri=MODEL_URI)
    _model = None


def is_model_loaded() -> bool:
    return _model is not None


def predict(features: dict[str, float]) -> int:
    if _model is None:
        raise RuntimeError("Model is not loaded")

    input_df = pd.DataFrame([features])
    prediction = _model.predict(input_df)
    return int(prediction[0])