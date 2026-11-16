import os

from opentelemetry import metrics

API_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)

_INFERENCE_FACTORS = (0.2, 0.5, 0.8, 0.9, 1.0, 1.05, 1.1, 1.25, 1.5, 2.0, 4.0, 10.0)


def inference_buckets() -> list[float]:
    delay = max(float(os.getenv("INFERENCE_DELAY_SEC", "0.5")), 0.001)
    return sorted({round(delay * factor, 4) for factor in _INFERENCE_FACTORS})


_meter = metrics.get_meter("app")

API_REQUESTS = _meter.create_counter(
    "api_requests",
    description="HTTP requests by route, method and status",
)
API_DURATION = _meter.create_histogram(
    "api_request_duration",
    unit="s",
    description="HTTP response time by route and method",
)

SOURCE_PUBLISHED = _meter.create_counter(
    "source_published",
    description="Tasks accepted by the broker",
)
SOURCE_NACKS = _meter.create_counter(
    "source_broker_nacks",
    description="Publish rejections (reject-publish overflow)",
)
SOURCE_WAIT = _meter.create_counter(
    "source_wait",
    unit="s",
    description="Total time the source spent in backoff",
)

WORKER_PROCESSED = _meter.create_counter(
    "worker_tasks_processed",
    description="Processed tasks by outcome (ok, error, dlq)",
)
WORKER_INFERENCE = _meter.create_histogram(
    "worker_inference_duration",
    unit="s",
    description="Model inference duration",
)
WORKER_IN_FLIGHT = _meter.create_up_down_counter(
    "worker_tasks_in_flight",
    description="Tasks being processed right now",
)
WORKER_REDELIVERIES = _meter.create_counter(
    "worker_redeliveries",
    description="Deliveries with attempt greater than zero",
)
WORKER_DEAD_LETTERS = _meter.create_counter(
    "worker_dead_letters",
    description="Messages that reached the dead letter queue",
)
