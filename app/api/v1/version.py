from fastapi import APIRouter

import app

router = APIRouter(tags=["meta"])


@router.get("/version")
async def version() -> dict[str, str]:
    return {"version": app.__version__}