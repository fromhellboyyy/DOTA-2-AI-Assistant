from __future__ import annotations

import re

_KEY_VALUE = re.compile(
    r"""(?ix)
    (?P<key>api[_-]?key|password|passwd|pwd|token|secret|authorization)
    (?P<sep>["']?\s*[:=]\s*["']?)
    (?:Bearer\s+)?[^\s,"'}&]+
    """
)
_SK_TOKEN = re.compile(r"\bsk-[A-Za-z0-9_\-]{8,}")
_URL_CREDS = re.compile(r"(?P<scheme>[a-z][a-z0-9+.\-]*://)(?P<user>[^:/@\s]+):[^@/\s]+@")

MAX_LINE = 400


def redact(text: str, *, max_len: int = MAX_LINE) -> str:
    text = _URL_CREDS.sub(r"\g<scheme>\g<user>:***@", text)
    text = _SK_TOKEN.sub("sk-***", text)
    text = _KEY_VALUE.sub(r"\g<key>\g<sep>***", text)
    if len(text) > max_len:
        text = text[:max_len] + "..."
    return text
