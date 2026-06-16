"""Structured logging.

One JSON object per log line, with a correlation context: a `trace_id` (per
request / per consumed message) plus whatever the call site passes via `extra`
(typically `document_id`, `step`, `attempt`). `document_id` is the field that ties
a document's journey together across the API, the broker and the workers.
"""
from __future__ import annotations

import json
import logging
from contextvars import ContextVar

_trace_id: ContextVar[str | None] = ContextVar("trace_id", default=None)

# Everything already on a LogRecord; anything else is treated as context.
_RESERVED = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime", "taskName"}


def set_trace_id(value: str | None) -> None:
    _trace_id.set(value)


class _ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = _trace_id.get()
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_") and value is not None:
                out[key] = value
        if record.exc_info:
            out["exception"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(_ContextFilter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
