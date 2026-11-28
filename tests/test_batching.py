import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.batching import MicroBatcher


class Recorder:
    def __init__(self, delay: float = 0.05):
        self.sizes: list[int] = []
        self.delay = delay

    def __call__(self, rows):
        self.sizes.append(len(rows))
        time.sleep(self.delay)
        if any(row["x"] < 0 for row in rows):
            raise ValueError("poison")
        return [int(row["x"]) for row in rows]


@pytest.fixture
def executor():
    pool = ThreadPoolExecutor(max_workers=2)
    yield pool
    pool.shutdown(wait=True)


def make(recorder, executor, size=4, wait=0.05, parallel=1):
    return MicroBatcher(recorder, size, wait, parallel, lambda: executor)


async def test_requests_are_grouped_into_batches(executor):
    recorder = Recorder()
    batcher = make(recorder, executor)

    results = await asyncio.gather(*[batcher.submit({"x": i}) for i in range(8)])
    await batcher.aclose()

    assert results == list(range(8))
    assert recorder.sizes == [4, 4]


async def test_partial_batch_flushes_after_timeout(executor):
    recorder = Recorder()
    batcher = make(recorder, executor, wait=0.05)

    results = await asyncio.gather(*[batcher.submit({"x": i}) for i in range(3)])
    await batcher.aclose()

    assert results == [0, 1, 2]
    assert recorder.sizes == [3]


async def test_single_request_is_not_stuck(executor):
    recorder = Recorder()
    batcher = make(recorder, executor)

    assert await asyncio.wait_for(batcher.submit({"x": 5}), timeout=2) == 5
    await batcher.aclose()


async def test_poison_item_is_isolated(executor):
    recorder = Recorder()
    batcher = make(recorder, executor)

    results = await asyncio.gather(
        batcher.submit({"x": 1}),
        batcher.submit({"x": -1}),
        batcher.submit({"x": 3}),
        return_exceptions=True,
    )
    await batcher.aclose()

    assert results[0] == 1
    assert isinstance(results[1], ValueError)
    assert results[2] == 3


async def test_event_loop_not_blocked_during_inference(executor):
    recorder = Recorder(delay=0.3)
    batcher = make(recorder, executor)
    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            await asyncio.sleep(0.02)
            ticks += 1

    task = asyncio.create_task(ticker())
    await asyncio.gather(*[batcher.submit({"x": i}) for i in range(4)])
    task.cancel()
    await batcher.aclose()

    assert ticks >= 5


async def test_parallel_batches_use_pool(executor):
    recorder = Recorder(delay=0.2)
    batcher = make(recorder, executor, size=2, parallel=2)

    started = time.monotonic()
    await asyncio.gather(*[batcher.submit({"x": i}) for i in range(4)])
    elapsed = time.monotonic() - started
    await batcher.aclose()

    assert recorder.sizes == [2, 2]
    assert elapsed < 0.38


async def test_wrong_prediction_count_falls_back_to_single_calls(executor):
    batcher = MicroBatcher(lambda rows: [1], 2, 0.05, 1, lambda: executor)

    results = await asyncio.gather(
        batcher.submit({"x": 1}),
        batcher.submit({"x": 2}),
        return_exceptions=True,
    )
    await batcher.aclose()

    assert results[0] == 1
    assert results[1] == 1


async def test_batch_logs_do_not_inherit_first_caller_context(executor):
    import contextvars

    marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="none")
    seen: list[str] = []

    def predict(rows):
        seen.append(marker.get())
        return [0 for _ in rows]

    batcher = MicroBatcher(predict, 2, 0.05, 1, lambda: executor)
    marker.set("first-caller")
    await batcher.submit({"x": 1})
    await batcher.aclose()

    assert seen == ["none"]
