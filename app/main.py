import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request

from app.api.healthz import router as healthz_router
from app.api.v1.router import v1_router
from app.api.v1.tasks import router as tasks_router
from app.broker import broker
from app.config import settings
from app.db.results import ensure_schema
from app.logger import setup_logging
from app.metrics import API_DURATION, API_REQUESTS
from app.ml.model import load_model
from app.telemetry import instrument_fastapi, setup_telemetry, shutdown_telemetry

setup_logging()
setup_telemetry(instrument_db=True)

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    logger.info("app_startup", app_name=settings.app_name)
    await ensure_schema()
    await broker.start()
    load_model()
    yield
    logger.info("app_shutdown_started")
    await broker.stop()
    logger.info("app_shutdown")
    shutdown_telemetry()


app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def record_http_metrics(request: Request, call_next):
    started = time.perf_counter()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        path = getattr(route, "path", "unmatched")
        API_REQUESTS.add(1, {"route": path, "method": request.method, "status": str(status)})
        API_DURATION.record(
            time.perf_counter() - started,
            {"route": path, "method": request.method},
        )


app.include_router(tasks_router, prefix="/api/v1")
app.include_router(healthz_router)
app.include_router(v1_router)

instrument_fastapi(app)
