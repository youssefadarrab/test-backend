import itertools

from app.models import DocumentStatus, StepName, StepStatus
from app.pipeline.transition import derive_document_status

DONE = StepStatus.DONE.value
ERROR = StepStatus.ERROR.value


def _statuses(ocr, meta, chunk, ext):
    return {
        StepName.OCR.value: ocr,
        StepName.METADATA.value: meta,
        StepName.CHUNKING.value: chunk,
        StepName.EXTERNAL_CALL.value: ext,
    }


def test_ready_only_when_external_call_done():
    s = _statuses(DONE, DONE, DONE, DONE)
    assert derive_document_status(s) == DocumentStatus.READY.value


def test_any_error_means_failed():
    s = _statuses(DONE, ERROR, DONE, StepStatus.PENDING.value)
    assert derive_document_status(s) == DocumentStatus.FAILED.value


def test_external_done_but_another_error_is_failed_not_ready():
    # ERROR dominates READY.
    s = _statuses(ERROR, DONE, DONE, DONE)
    assert derive_document_status(s) == DocumentStatus.FAILED.value


def test_awaiting_callback_is_still_processing():
    s = _statuses(DONE, DONE, DONE, StepStatus.AWAITING_CALLBACK.value)
    assert derive_document_status(s) == DocumentStatus.PROCESSING.value


def test_exhaustive_invariants():
    """Over every combination of step states: ready REQUIRES external_call done and
    no errors; failed REQUIRES an error."""
    values = [s.value for s in StepStatus]
    for ocr, meta, chunk, ext in itertools.product(values, repeat=4):
        result = derive_document_status(_statuses(ocr, meta, chunk, ext))
        has_error = ERROR in (ocr, meta, chunk, ext)
        if result == DocumentStatus.READY.value:
            assert ext == DONE and not has_error
        if result == DocumentStatus.FAILED.value:
            assert has_error
        if has_error:
            assert result == DocumentStatus.FAILED.value
