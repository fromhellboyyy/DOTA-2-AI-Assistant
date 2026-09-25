from fastapi import APIRouter

from app.api.v1 import health, version

v1_router = APIRouter(prefix="/api/v1")
v1_router.include_router(version.router)
v1_router.include_router(health.router)