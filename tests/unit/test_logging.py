import json
import logging

from app.observability import JsonFormatter, _ContextFilter, set_trace_id


def _record(**extra) -> logging.LogRecord:
    rec = logging.LogRecord("app.test", logging.INFO, __file__, 1, "hello", None, None)
    for key, value in extra.items():
        setattr(rec, key, value)
    return rec


def test_core_fields_are_emitted():
    out = json.loads(JsonFormatter().format(_record()))
    assert out["level"] == "INFO"
    assert out["logger"] == "app.test"
    assert out["message"] == "hello"
    assert "timestamp" in out


def test_extra_context_is_included():
    out = json.loads(JsonFormatter().format(_record(document_id="d1", step="ocr", attempt=2)))
    assert out["document_id"] == "d1"
    assert out["step"] == "ocr"
    assert out["attempt"] == 2


def test_trace_id_is_attached_by_the_filter():
    set_trace_id("trace-123")
    try:
        rec = _record()
        _ContextFilter().filter(rec)
        out = json.loads(JsonFormatter().format(rec))
        assert out["trace_id"] == "trace-123"
    finally:
        set_trace_id(None)
