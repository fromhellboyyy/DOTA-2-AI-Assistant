import time

from fastapi import APIRouter

from app.db.session import check_db_connection

router = APIRouter(tags=["meta"])


@router.get("/health")
async def health() -> dict:
    components: dict[str, dict] = {}

    start = time.perf_counter()
    db_ok, db_info = await check_db_connection()
    elapsed = round(time.perf_counter() - start, 4)

    components["postgresql"] = {
        "status": "ok" if db_ok else "error",
        "version": db_info,
        "response_time_s": elapsed,
    }

    overall = "ok" if db_ok else "degraded"

    return {
        "status": overall,
        "components": components,
    }