from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from app.api.healthz import router as healthz_router
from app.api.v1.router import v1_router
from app.api.v1.tasks import router as tasks_router
from app.config import settings
from app.logger import setup_logging
from app.ml.model import load_model

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    setup_logging()
    logger.info("app_startup", app_name=settings.app_name)
    load_model()
    yield
    logger.info("app_shutdown")


app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(tasks_router, prefix="/api/v1")
app.include_router(healthz_router)
app.include_router(v1_router)
