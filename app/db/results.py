from opentelemetry import trace
from sqlalchemy import text

from app.db.session import engine

tracer = trace.get_tracer("app.db")

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
    with tracer.start_as_current_span(
        "db.save_result",
        attributes={
            "db.system": "postgresql",
            "db.operation": "INSERT",
            "db.sql.table": "task_results",
            "task.id": task_id,
        },
    ) as span:
        async with engine.begin() as conn:
            result = await conn.execute(
                text(INSERT_RESULT),
                {"task_id": task_id, "prediction": prediction},
            )
            inserted = result.rowcount > 0
        span.set_attribute("db.inserted", inserted)
        return inserted


async def get_result(task_id: str) -> dict | None:
    async with engine.connect() as conn:
        row = (await conn.execute(text(SELECT_RESULT), {"task_id": task_id})).first()
    if row is None:
        return None
    return {"task_id": row.task_id, "prediction": row.prediction}
