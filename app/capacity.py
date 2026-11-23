import argparse
import math
import os

DEFAULT_TARGET_WAIT_SEC = 5.0
DEFAULT_INFERENCE_DELAY_SEC = 1.0
DEFAULT_BATCH_SIZE = 4
DEFAULT_CONCURRENCY = 1
DEFAULT_REPLICAS = 1


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return float(raw)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw)


def inference_delay_sec() -> float:
    return _env_float("INFERENCE_DELAY_SEC", DEFAULT_INFERENCE_DELAY_SEC)


def batch_size() -> int:
    return max(1, _env_int("BATCH_SIZE", DEFAULT_BATCH_SIZE))


def concurrency() -> int:
    return max(1, _env_int("CONCURRENCY_LIMIT", DEFAULT_CONCURRENCY))


def replicas() -> int:
    return max(1, _env_int("WORKER_REPLICAS", DEFAULT_REPLICAS))


def target_wait_sec() -> float:
    return _env_float("QUEUE_TARGET_WAIT_SEC", DEFAULT_TARGET_WAIT_SEC)


def worker_rate(
    concurrency_limit: int | None = None,
    batch: int | None = None,
    delay_sec: float | None = None,
) -> float:
    c = concurrency() if concurrency_limit is None else concurrency_limit
    b = batch_size() if batch is None else batch
    d = inference_delay_sec() if delay_sec is None else delay_sec
    return c * b / d


def total_rate(worker_count: int | None = None) -> float:
    n = replicas() if worker_count is None else worker_count
    return worker_rate() * n


def computed_queue_limit(worker_count: int | None = None) -> int:
    count = replicas() if worker_count is None else worker_count
    value = target_wait_sec() * worker_rate() * count
    return max(1, math.ceil(round(value, 6)))


def queue_limit(worker_count: int | None = None) -> int:
    override = os.getenv("MAX_QUEUE_SIZE")
    if override is not None and override.strip():
        return int(override)
    return computed_queue_limit(worker_count)


def retry_after_sec(limit: int | None = None, worker_count: int | None = None) -> int:
    override = _env_int("RETRY_AFTER_SECONDS", 0)
    if override > 0:
        return override
    size = queue_limit(worker_count) if limit is None else limit
    return max(1, math.ceil(round(size / total_rate(worker_count), 6)))


def prefetch_count() -> int:
    override = _env_int("PREFETCH_COUNT", 0)
    if override > 0:
        return override
    return concurrency() * batch_size()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "field",
        choices=["limit", "computed_limit", "retry_after", "prefetch", "worker_rate", "total_rate"],
    )
    args = parser.parse_args()

    values = {
        "limit": queue_limit,
        "computed_limit": computed_queue_limit,
        "retry_after": retry_after_sec,
        "prefetch": prefetch_count,
        "worker_rate": worker_rate,
        "total_rate": total_rate,
    }
    value = values[args.field]()
    print(f"{value:g}" if isinstance(value, float) else value)  # noqa: T201


if __name__ == "__main__":
    main()
