import asyncio
import contextlib
import contextvars
from collections.abc import Callable, Sequence
from concurrent.futures import Executor
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

PredictBatch = Callable[[Sequence[dict[str, float]]], Sequence[int]]


class MicroBatcher:
    def __init__(
        self,
        predict_batch: PredictBatch,
        max_batch_size: int,
        max_wait_sec: float,
        max_parallel_batches: int,
        executor_factory: Callable[[], Executor],
    ) -> None:
        self._predict_batch = predict_batch
        self._max_batch_size = max(1, max_batch_size)
        self._max_wait_sec = max(0.0, max_wait_sec)
        self._max_parallel = max(1, max_parallel_batches)
        self._executor_factory = executor_factory
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue[tuple[dict[str, float], asyncio.Future[int]]] | None = None
        self._slots: asyncio.Semaphore | None = None
        self._runner: asyncio.Task[None] | None = None
        self._batches: set[asyncio.Task[None]] = set()

    def _ensure_started(self) -> asyncio.Queue[tuple[dict[str, float], asyncio.Future[int]]]:
        loop = asyncio.get_running_loop()

        stale = self._queue is None or self._runner is None or self._runner.done()

        if self._loop is not loop or stale:
            self._loop = loop
            self._queue = asyncio.Queue()
            self._slots = asyncio.Semaphore(self._max_parallel)
            self._batches = set()
            self._runner = loop.create_task(
                self._run(self._queue, self._slots),
                context=contextvars.Context(),
            )

        return self._queue

    async def submit(self, features: dict[str, float]) -> int:
        queue = self._ensure_started()
        future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        queue.put_nowait((features, future))
        return await future

    async def _collect(
        self,
        queue: asyncio.Queue[tuple[dict[str, float], asyncio.Future[int]]],
    ) -> list[tuple[dict[str, float], asyncio.Future[int]]]:
        loop = asyncio.get_running_loop()
        first = await queue.get()
        items = [first]
        deadline = loop.time() + self._max_wait_sec

        while len(items) < self._max_batch_size:
            while len(items) < self._max_batch_size and not queue.empty():
                items.append(queue.get_nowait())

            if len(items) >= self._max_batch_size:
                break

            remaining = deadline - loop.time()
            if remaining <= 0:
                break

            try:
                items.append(await asyncio.wait_for(queue.get(), timeout=remaining))
            except TimeoutError:
                break

        return items

    async def _run(
        self,
        queue: asyncio.Queue[tuple[dict[str, float], asyncio.Future[int]]],
        slots: asyncio.Semaphore,
    ) -> None:
        loop = asyncio.get_running_loop()

        while True:
            await slots.acquire()

            try:
                items = await self._collect(queue)
            except BaseException:
                slots.release()
                raise

            task = loop.create_task(self._execute(items, slots))
            self._batches.add(task)
            task.add_done_callback(self._batches.discard)

    async def _call(self, rows: list[dict[str, float]]) -> list[int]:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(self._executor_factory(), self._predict_batch, rows)
        predictions = [int(value) for value in result]

        if len(predictions) != len(rows):
            raise ValueError(f"model returned {len(predictions)} predictions for {len(rows)} rows")

        return predictions

    @staticmethod
    def _resolve(
        future: asyncio.Future[int],
        value: int | None,
        error: BaseException | None,
    ) -> None:
        if future.done():
            return
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(value if value is not None else 0)

    async def _execute(
        self,
        items: list[tuple[dict[str, float], asyncio.Future[int]]],
        slots: asyncio.Semaphore,
    ) -> None:
        try:
            live = [(row, fut) for row, fut in items if not fut.done()]
            if not live:
                return

            rows = [row for row, _ in live]
            logger.info("batch_inference", size=len(rows))

            try:
                predictions = await self._call(rows)
            except Exception as exc:
                if len(live) == 1:
                    self._resolve(live[0][1], None, exc)
                    return

                logger.warning("batch_failed_isolating", size=len(live), error=str(exc))
                await self._isolate(live)
                return

            for (_, fut), prediction in zip(live, predictions, strict=True):
                self._resolve(fut, prediction, None)
        except asyncio.CancelledError:
            for _, fut in items:
                if not fut.done():
                    fut.cancel()
            raise
        finally:
            slots.release()

    async def _isolate(self, live: list[tuple[dict[str, float], asyncio.Future[int]]]) -> None:
        for row, fut in live:
            try:
                prediction = (await self._call([row]))[0]
            except Exception as exc:
                self._resolve(fut, None, exc)
            else:
                self._resolve(fut, prediction, None)

    async def aclose(self) -> None:
        runner = self._runner
        self._runner = None

        if runner is not None:
            runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await runner

        pending: list[Any] = list(self._batches)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        queue = self._queue
        if queue is not None:
            while not queue.empty():
                _, fut = queue.get_nowait()
                if not fut.done():
                    fut.cancel()
