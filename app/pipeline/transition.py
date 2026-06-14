"""Decide what runs next, exactly once, and keep the document status in sync with
its steps.

Fan-in correctness rests on `claim_step`: when metadata and chunking finish
concurrently, both try to trigger external_call. The atomic
`UPDATE ... WHERE status='pending' RETURNING` lets exactly one flip the row and
publish; the other is a no-op. No locks, no result backend.
"""
from __future__ import annotations

import uuid

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.events.notify import emit_event
from app.models import Document, DocumentStatus, PipelineStep, StepName, StepStatus
from app.pipeline.dag import predecessors_of, successors_of
from app.pipeline.publisher import publish_step


def _step_status_map(session: Session, document_id: uuid.UUID) -> dict[str, str]:
    rows = session.execute(
        select(PipelineStep.name, PipelineStep.status).where(
            PipelineStep.document_id == document_id
        )
    ).all()
    return {name: status for name, status in rows}


def _all_predecessors_done(statuses: dict[str, str], step: StepName) -> bool:
    return all(statuses.get(p.value) == StepStatus.DONE.value for p in predecessors_of(step))


def claim_step(session: Session, document_id: uuid.UUID, step: StepName) -> bool:
    """Atomically move a step PENDING -> QUEUED. Returns True iff this caller won
    the race (and is therefore responsible for publishing it)."""
    res = session.execute(
        update(PipelineStep)
        .where(
            PipelineStep.document_id == document_id,
            PipelineStep.name == step.value,
            PipelineStep.status == StepStatus.PENDING.value,
        )
        .values(status=StepStatus.QUEUED.value)
        .returning(PipelineStep.id)
    )
    claimed = res.first() is not None
    if claimed:
        emit_event(session, document_id, {"step": step.value, "status": StepStatus.QUEUED.value})
    session.commit()
    return claimed


def trigger_successors(session: Session, document_id: uuid.UUID, completed: StepName) -> None:
    """For each successor of a just-completed step, publish it iff all its
    predecessors are DONE. Commit-then-publish (the claim commits first)."""
    statuses = _step_status_map(session, document_id)
    for succ in successors_of(completed):
        if _all_predecessors_done(statuses, succ):
            if claim_step(session, document_id, succ):
                publish_step(document_id, succ)


def derive_document_status(statuses: dict[str, str]) -> str:
    """Pure function of the step states (exhaustively unit-tested):
      - failed  if any step ERROR
      - ready   if external_call DONE
      - else    processing
    """
    if any(s == StepStatus.ERROR.value for s in statuses.values()):
        return DocumentStatus.FAILED.value
    if statuses.get(StepName.EXTERNAL_CALL.value) == StepStatus.DONE.value:
        return DocumentStatus.READY.value
    return DocumentStatus.PROCESSING.value


def recompute_document_status(session: Session, document_id: uuid.UUID) -> str:
    """Derive document status from its steps, persist it, emit on change."""
    statuses = _step_status_map(session, document_id)
    new = derive_document_status(statuses)

    document = session.get(Document, document_id)
    if document is not None and document.status != new:
        document.status = new
        emit_event(session, document_id, {"doc_status": new})
        session.commit()
    return new
