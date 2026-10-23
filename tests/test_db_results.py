from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.db.results import ensure_schema, get_result, save_result


class AsyncCM:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *args):
        return None


@pytest.mark.asyncio
async def test_ensure_schema():
    mock_conn = AsyncMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value = AsyncCM(mock_conn)

    with patch("app.db.results.engine", mock_engine):
        await ensure_schema()
        mock_conn.execute.assert_called_once()


@pytest.mark.asyncio
async def test_save_result():
    mock_result = MagicMock()
    mock_result.rowcount = 1
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = mock_result
    mock_engine = MagicMock()
    mock_engine.begin.return_value = AsyncCM(mock_conn)

    with patch("app.db.results.engine", mock_engine):
        inserted = await save_result("task-1", 1)
        assert inserted is True


@pytest.mark.asyncio
async def test_save_result_duplicate():
    mock_result = MagicMock()
    mock_result.rowcount = 0
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = mock_result
    mock_engine = MagicMock()
    mock_engine.begin.return_value = AsyncCM(mock_conn)

    with patch("app.db.results.engine", mock_engine):
        inserted = await save_result("task-1", 1)
        assert inserted is False


@pytest.mark.asyncio
async def test_get_result_found():
    mock_row = MagicMock()
    mock_row.task_id = "task-1"
    mock_row.prediction = 1
    mock_result = MagicMock()
    mock_result.first.return_value = mock_row
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = mock_result
    mock_engine = MagicMock()
    mock_engine.connect.return_value = AsyncCM(mock_conn)

    with patch("app.db.results.engine", mock_engine):
        res = await get_result("task-1")
        assert res == {"task_id": "task-1", "prediction": 1}


@pytest.mark.asyncio
async def test_get_result_not_found():
    mock_result = MagicMock()
    mock_result.first.return_value = None
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = mock_result
    mock_engine = MagicMock()
    mock_engine.connect.return_value = AsyncCM(mock_conn)

    with patch("app.db.results.engine", mock_engine):
        res = await get_result("task-unknown")
        assert res is None
