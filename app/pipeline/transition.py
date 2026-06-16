"""Decide what runs next, exactly once, and keep the document status in sync with
its steps.

Fan-in correctness rests on `claim_step`: when metadata and chunking finish
concurrently, both try to trigger external_call. The atomic claim (see
`transactions.claim_pending_step`) lets exactly one flip the row and publish; the
other is a no-op. No locks, no result backend.
"""
from __future__ import annotations

import uuid

from sqlalchemy.orm import Session

from app import transactions
from app.events.notify import emit_event
from app.models import Document, DocumentStatus, StepName, StepStatus
from app.pipeline.dag import predecessors_of, successors_of
from app.pipeline.publisher import publish_step


def _all_predecessors_done(statuses: dict[str, str], step: StepName) -> bool:
    return all(statuses.get(p.value) == StepStatus.DONE.value for p in predecessors_of(step))


def claim_step(session: Session, document_id: uuid.UUID, step: StepName) -> bool:
    """Atomically move a step PENDING -> QUEUED. Returns True iff this caller won
    the race (and is therefore responsible for publishing it). Commits the claim
    and its NOTIFY together."""
    claimed = transactions.claim_pending_step(session, document_id, step.value)
    if claimed:
        emit_event(session, document_id, {"step": step.value, "status": StepStatus.QUEUED.value})
    session.commit()
    return claimed


def trigger_successors(session: Session, document_id: uuid.UUID, completed: StepName) -> None:
    """For each successor of a just-completed step, publish it iff all its
    predecessors are DONE. Commit-then-publish (the claim commits first)."""
    statuses = transactions.step_status_map(session, document_id)
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
    statuses = transactions.step_status_map(session, document_id)
    new = derive_document_status(statuses)

    document = session.get(Document, document_id)
    if document is not None and document.status != new:
        document.status = new
        emit_event(session, document_id, {"doc_status": new})
        session.commit()
    return new
