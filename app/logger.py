import logging
import os
import re
import sys

import structlog
from opentelemetry import trace

from app.config import settings

DSN_PASSWORD = re.compile(r"(?P<head>[a-zA-Z][a-zA-Z0-9+.\-]*://[^:/@\s]+:)[^@/\s]+(?P<tail>@)")
SENSITIVE_KEYS = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "authorization",
        "aws_secret_access_key",
        "database_url",
        "rabbitmq_url",
    }
)
QUIET_LOGGERS = {
    "uvicorn.access": logging.WARNING,
    "aiohttp.access": logging.WARNING,
    "faststream.access": logging.WARNING,
    "sqlalchemy.engine": logging.WARNING,
    "aio_pika": logging.WARNING,
    "aiormq": logging.WARNING,
}


def mask_text(value: str) -> str:
    return DSN_PASSWORD.sub(r"\g<head>***\g<tail>", value)


def redact_secrets(_logger, _method, event_dict):
    for key, value in list(event_dict.items()):
        if key.lower() in SENSITIVE_KEYS:
            event_dict[key] = "***"
        elif isinstance(value, str):
            event_dict[key] = mask_text(value)
    return event_dict


def add_trace_context(_logger, _method, event_dict):
    span_context = trace.get_current_span().get_span_context()
    if span_context.is_valid:
        event_dict["trace_id"] = format(span_context.trace_id, "032x")
        event_dict["span_id"] = format(span_context.span_id, "016x")
    return event_dict


def add_service_fields(_logger, _method, event_dict):
    event_dict.setdefault("service", os.getenv("OTEL_SERVICE_NAME", "app"))
    event_dict.setdefault("env", os.getenv("DEPLOYMENT_ENVIRONMENT", "dev"))
    return event_dict


def setup_logging() -> None:
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        add_service_fields,
        add_trace_context,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        redact_secrets,
    ]

    if settings.log_format == "json":
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    for existing in logging.root.manager.loggerDict.values():
        if isinstance(existing, logging.Logger) and existing.handlers:
            existing.handlers.clear()
            existing.propagate = True

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(settings.log_level.upper())

    for name, level in QUIET_LOGGERS.items():
        logging.getLogger(name).setLevel(level)

    logging.getLogger("opentelemetry").setLevel(
        os.getenv("TELEMETRY_LOG_LEVEL", "CRITICAL").upper()
    )
