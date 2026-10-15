from fastapi import APIRouter

from app.api.v1 import health, process, version

v1_router = APIRouter(prefix="/api/v1")
v1_router.include_router(version.router)
v1_router.include_router(health.router)
v1_router.include_router(process.router)
