import pytest

from app import capacity

ENV_NAMES = [
    "INFERENCE_DELAY_SEC",
    "BATCH_SIZE",
    "CONCURRENCY_LIMIT",
    "WORKER_REPLICAS",
    "QUEUE_TARGET_WAIT_SEC",
    "MAX_QUEUE_SIZE",
    "RETRY_AFTER_SECONDS",
    "PREFETCH_COUNT",
]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_default_worker_rate():
    assert capacity.worker_rate() == 4.0


def test_default_limit_follows_formula():
    assert capacity.queue_limit() == 20


def test_limit_scales_with_replicas(monkeypatch):
    monkeypatch.setenv("WORKER_REPLICAS", "2")
    assert capacity.queue_limit() == 40


def test_limit_uses_batch_and_delay(monkeypatch):
    monkeypatch.setenv("BATCH_SIZE", "8")
    monkeypatch.setenv("INFERENCE_DELAY_SEC", "2")
    monkeypatch.setenv("CONCURRENCY_LIMIT", "2")
    monkeypatch.setenv("WORKER_REPLICAS", "3")
    assert capacity.worker_rate() == 8.0
    assert capacity.queue_limit() == 120


def test_limit_uses_target_wait(monkeypatch):
    monkeypatch.setenv("QUEUE_TARGET_WAIT_SEC", "10")
    assert capacity.queue_limit() == 40


def test_explicit_override_wins(monkeypatch):
    monkeypatch.setenv("MAX_QUEUE_SIZE", "100000")
    assert capacity.queue_limit() == 100000
    assert capacity.computed_queue_limit() == 20


def test_empty_override_is_ignored(monkeypatch):
    monkeypatch.setenv("MAX_QUEUE_SIZE", "")
    assert capacity.queue_limit() == 20


def test_prefetch_equals_pool_times_batch(monkeypatch):
    monkeypatch.setenv("CONCURRENCY_LIMIT", "2")
    monkeypatch.setenv("BATCH_SIZE", "4")
    assert capacity.prefetch_count() == 8


def test_retry_after_equals_target_wait():
    assert capacity.retry_after_sec() == 5


def test_retry_after_env_override(monkeypatch):
    monkeypatch.setenv("RETRY_AFTER_SECONDS", "9")
    assert capacity.retry_after_sec() == 9
