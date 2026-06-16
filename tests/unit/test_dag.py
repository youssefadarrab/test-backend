import pytest

from app.models import StepName
from app.pipeline.dag import PIPELINE, DependencyGraph

OCR, META, CHUNK, EXT = (
    StepName.OCR,
    StepName.METADATA,
    StepName.CHUNKING,
    StepName.EXTERNAL_CALL,
)


def test_entry_and_terminal_nodes():
    assert PIPELINE.entry_nodes == [OCR]
    assert PIPELINE.terminal_nodes == [EXT]


def test_predecessors():
    assert PIPELINE.predecessors_of(META) == {OCR}
    assert PIPELINE.predecessors_of(CHUNK) == {OCR}
    assert PIPELINE.predecessors_of(EXT) == {META, CHUNK}


def test_successors():
    assert set(PIPELINE.successors_of(OCR)) == {META, CHUNK}
    assert PIPELINE.successors_of(META) == [EXT]
    assert PIPELINE.successors_of(EXT) == []


def test_ready_successors_progression():
    assert PIPELINE.ready_successors(set()) == [OCR]                  # nothing done -> ocr
    assert set(PIPELINE.ready_successors({OCR})) == {META, CHUNK}     # ocr done -> fan out
    assert PIPELINE.ready_successors({OCR, META}) == [CHUNK]         # chunking still pending
    assert PIPELINE.ready_successors({OCR, META, CHUNK}) == [EXT]    # both done -> fan in
    assert PIPELINE.ready_successors({OCR, META, CHUNK, EXT}) == []  # all done


def test_cycle_is_rejected():
    with pytest.raises(ValueError):
        DependencyGraph({OCR: {EXT}, EXT: {OCR}})


def test_dangling_edge_is_rejected():
    with pytest.raises(ValueError):
        DependencyGraph({META: {OCR}})  # OCR is not a declared node
