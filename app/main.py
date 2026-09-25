from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from app.api.healthz import router as healthz_router
from app.api.v1.router import v1_router
from app.config import settings
from app.logging import setup_logging

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    setup_logging()
    logger.info("app_startup", app_name=settings.app_name)
    yield
    logger.info("app_shutdown")


app = FastAPI(
    title=settings.app_name,
    lifespan=lifespan,
)

app.include_router(healthz_router)
app.include_router(v1_router)