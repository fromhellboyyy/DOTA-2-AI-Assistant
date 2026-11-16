import logging
import os
import socket

from opentelemetry import metrics, trace
from opentelemetry.metrics import CallbackOptions, Observation

from app.metrics import API_BUCKETS, inference_buckets

logger = logging.getLogger(__name__)

_state = {"initialized": False}


def telemetry_enabled() -> bool:
    if os.getenv("OTEL_SDK_DISABLED", "false").strip().lower() == "true":
        return False
    return bool(os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))


def _rss_bytes() -> int:
    try:
        with open("/proc/self/statm", encoding="ascii") as handle:
            return int(handle.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError, AttributeError):
        return 0


def _rss_callback(_options: CallbackOptions):
    yield Observation(_rss_bytes())


def _build_resource():
    from opentelemetry.sdk.resources import Resource

    attributes: dict[str, str] = {}
    if "service.instance.id" not in os.getenv("OTEL_RESOURCE_ATTRIBUTES", ""):
        attributes["service.instance.id"] = socket.gethostname()
    return Resource.create(attributes)


def setup_telemetry(*, instrument_db: bool = False) -> None:
    if _state["initialized"] or not telemetry_enabled():
        return
    _state["initialized"] = True

    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = _build_resource()

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(tracer_provider)

    views = [
        View(
            instrument_name="worker_inference_duration",
            aggregation=ExplicitBucketHistogramAggregation(inference_buckets()),
        ),
        View(
            instrument_name="api_request_duration",
            aggregation=ExplicitBucketHistogramAggregation(list(API_BUCKETS)),
        ),
    ]
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())],
        views=views,
    )
    metrics.set_meter_provider(meter_provider)

    metrics.get_meter("app.runtime").create_observable_gauge(
        "process_memory_rss",
        callbacks=[_rss_callback],
        unit="By",
        description="Resident set size of the process",
    )

    if instrument_db:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

        from app.db.session import engine

        SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)


def instrument_fastapi(application) -> None:
    if not telemetry_enabled():
        return
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(application, excluded_urls="healthz,api/v1/health")


def telemetry_middlewares() -> tuple:
    if not telemetry_enabled():
        return ()
    try:
        from faststream.rabbit.opentelemetry import RabbitTelemetryMiddleware
    except ImportError:
        logger.warning("faststream_otel_middleware_unavailable")
        return ()
    return (RabbitTelemetryMiddleware(),)


def shutdown_telemetry() -> None:
    if not _state["initialized"]:
        return
    for provider in (trace.get_tracer_provider(), metrics.get_meter_provider()):
        shutdown = getattr(provider, "shutdown", None)
        if shutdown is not None:
            try:
                shutdown()
            except Exception:
                logger.exception("telemetry_shutdown_failed")
