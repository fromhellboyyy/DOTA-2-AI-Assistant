import pytest
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

from app.logger import add_trace_context, mask_text, redact_secrets
from app.metrics import inference_buckets
from app.ml.registry import parse_model_uri, resolve_model_version
from app.telemetry import telemetry_enabled, telemetry_middlewares


def test_mask_text_hides_dsn_password():
    masked = mask_text("failed to connect amqp://rabbit:hunter2@rabbitmq:5672/")
    assert "hunter2" not in masked
    assert "amqp://rabbit:***@rabbitmq:5672/" in masked


def test_mask_text_keeps_plain_urls():
    assert mask_text("http://mlflow:5000/api") == "http://mlflow:5000/api"


def test_redact_secrets_masks_keys_and_values():
    event = {
        "event": "x",
        "password": "abc",
        "error": "postgresql+asyncpg://postgres:postgres@db:5432/app refused",
        "count": 3,
    }
    result = redact_secrets(None, "info", event)
    assert result["password"] == "***"  # noqa: S105
    assert "postgres:postgres" not in result["error"]
    assert result["count"] == 3


def test_add_trace_context_inside_span():
    context = SpanContext(
        trace_id=int("4bf92f3577b34da6a3ce929d0e0e4736", 16),
        span_id=int("00f067aa0ba902b7", 16),
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    with trace.use_span(NonRecordingSpan(context)):
        result = add_trace_context(None, "info", {"event": "x"})
    assert result["trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert result["span_id"] == "00f067aa0ba902b7"


def test_add_trace_context_without_span():
    result = add_trace_context(None, "info", {"event": "x"})
    assert "trace_id" not in result
    assert "span_id" not in result


def test_inference_buckets_follow_config(monkeypatch):
    monkeypatch.setenv("INFERENCE_DELAY_SEC", "0.5")
    buckets = inference_buckets()
    assert buckets == sorted(buckets)
    assert 0.5 in buckets
    assert buckets[0] < 0.5 < buckets[-1]


def test_inference_buckets_scale_with_delay(monkeypatch):
    monkeypatch.setenv("INFERENCE_DELAY_SEC", "2")
    assert 2.0 in inference_buckets()


@pytest.mark.parametrize(
    ("disabled", "endpoint", "expected"),
    [
        (None, None, False),
        (None, "http://collector:4318", True),
        ("true", "http://collector:4318", False),
    ],
)
def test_telemetry_enabled(monkeypatch, disabled, endpoint, expected):
    monkeypatch.delenv("OTEL_SDK_DISABLED", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    if disabled is not None:
        monkeypatch.setenv("OTEL_SDK_DISABLED", disabled)
    if endpoint is not None:
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
    assert telemetry_enabled() is expected


def test_middlewares_empty_when_disabled(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    assert telemetry_middlewares() == ()


def test_parse_model_uri():
    assert parse_model_uri("models:/Dota2WinPredictor@champion") == (
        "Dota2WinPredictor",
        "champion",
    )
    assert parse_model_uri("runs:/abc/model") == (None, None)


def test_resolve_model_version_unknown_for_foreign_uri():
    assert resolve_model_version("runs:/abc/model", "http://localhost:5000") == (
        "unknown",
        "unknown",
    )
