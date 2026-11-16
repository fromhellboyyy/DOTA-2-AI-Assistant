import contextlib
import os
import re
from collections.abc import Iterator

import mlflow

MODEL_URI_RE = re.compile(r"^models:/(?P<name>[^@/]+)@(?P<alias>.+)$")

_FAST_FAIL_ENV = {
    "MLFLOW_HTTP_REQUEST_MAX_RETRIES": "1",
    "MLFLOW_HTTP_REQUEST_TIMEOUT": "5",
}


def parse_model_uri(uri: str) -> tuple[str | None, str | None]:
    match = MODEL_URI_RE.match(uri)
    if match is None:
        return None, None
    return match.group("name"), match.group("alias")


@contextlib.contextmanager
def _fast_fail_http() -> Iterator[None]:
    saved = {key: os.environ.get(key) for key in _FAST_FAIL_ENV}
    os.environ.update(_FAST_FAIL_ENV)
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def resolve_model_version(uri: str, tracking_uri: str) -> tuple[str, str]:
    name, alias = parse_model_uri(uri)
    if name is None or alias is None:
        return "unknown", "unknown"
    try:
        with _fast_fail_http():
            client = mlflow.MlflowClient(tracking_uri=tracking_uri, registry_uri=tracking_uri)
            version = client.get_model_version_by_alias(name, alias).version
    except Exception:
        return alias, "unknown"
    return alias, str(version)
