from sqlalchemy import text

from app.db.session import engine

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS task_results (
    task_id TEXT PRIMARY KEY,
    prediction INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

INSERT_RESULT = """
INSERT INTO task_results (task_id, prediction)
VALUES (:task_id, :prediction)
ON CONFLICT (task_id) DO NOTHING
"""

SELECT_RESULT = """
SELECT task_id, prediction
FROM task_results
WHERE task_id = :task_id
"""


async def ensure_schema() -> None:
    async with engine.begin() as conn:
        await conn.execute(text(CREATE_TABLE))


async def save_result(task_id: str, prediction: int) -> bool:
    async with engine.begin() as conn:
        result = await conn.execute(
            text(INSERT_RESULT),
            {"task_id": task_id, "prediction": prediction},
        )
        return result.rowcount > 0


async def get_result(task_id: str) -> dict | None:
    async with engine.connect() as conn:
        row = (await conn.execute(text(SELECT_RESULT), {"task_id": task_id})).first()
    if row is None:
        return None
    return {"task_id": row.task_id, "prediction": row.prediction}
