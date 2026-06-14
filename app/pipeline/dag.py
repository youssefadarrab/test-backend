"""The pipeline dependency graph. Topology is declared here and nowhere else.

    ocr ──► metadata ──┐
        └─► chunking ──┴──► external_call

Adding a step is a data change here, not a control-flow rewrite elsewhere. The
transitioner (app/pipeline/transition.py) reads only this to decide what to fire
next.
"""
from app.models import StepName

# successor -> set of predecessors that must all be DONE before it can run.
PREDECESSORS: dict[StepName, set[StepName]] = {
    StepName.OCR: set(),
    StepName.METADATA: {StepName.OCR},
    StepName.CHUNKING: {StepName.OCR},
    StepName.EXTERNAL_CALL: {StepName.METADATA, StepName.CHUNKING},
}

# All steps, in a stable creation order.
ALL_STEPS: list[StepName] = [
    StepName.OCR,
    StepName.METADATA,
    StepName.CHUNKING,
    StepName.EXTERNAL_CALL,
]

# The single entry step (no predecessors).
ENTRY_STEPS: list[StepName] = [s for s in ALL_STEPS if not PREDECESSORS[s]]


def predecessors_of(step: StepName) -> set[StepName]:
    return PREDECESSORS[step]


def successors_of(step: StepName) -> list[StepName]:
    return [s for s, preds in PREDECESSORS.items() if step in preds]
