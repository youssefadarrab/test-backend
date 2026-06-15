from app.models import StepName
from app.pipeline.dag import ENTRY_STEPS, predecessors_of, successors_of


def test_ocr_is_the_single_entry():
    assert ENTRY_STEPS == [StepName.OCR]


def test_metadata_and_chunking_depend_only_on_ocr():
    assert predecessors_of(StepName.METADATA) == {StepName.OCR}
    assert predecessors_of(StepName.CHUNKING) == {StepName.OCR}


def test_external_call_depends_on_both_metadata_and_chunking():
    assert predecessors_of(StepName.EXTERNAL_CALL) == {StepName.METADATA, StepName.CHUNKING}


def test_ocr_fans_out_to_metadata_and_chunking():
    assert set(successors_of(StepName.OCR)) == {StepName.METADATA, StepName.CHUNKING}


def test_metadata_and_chunking_fan_in_to_external_call():
    assert successors_of(StepName.METADATA) == [StepName.EXTERNAL_CALL]
    assert successors_of(StepName.CHUNKING) == [StepName.EXTERNAL_CALL]
    assert successors_of(StepName.EXTERNAL_CALL) == []
