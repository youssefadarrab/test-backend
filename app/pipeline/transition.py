"""Decide what runs next, exactly once, and keep the document status in sync with
its steps.

`Transitioner` walks the dependency graph and publishes the steps that have become
ready. Fan-in correctness rests on the atomic claim (`transactions.claim_pending_step`):
when metadata and chunking finish concurrently, exactly one caller flips
external_call and publishes; the other is a no-op. No locks, no result backend.
"""
from __future__ import annotations

import uuid

from sqlalchemy.orm import Session

from app import transactions
from app.events.notify import doc_status_event, emit_event, step_event
from app.models import Document, DocumentStatus, StepName, StepStatus
from app.pipeline.dag import PIPELINE, DependencyGraph
from app.pipeline.publisher import publish_step


def derive_document_status(statuses: dict[str, str]) -> str:
    """Pure function of the step states (exhaustively unit-tested):
      - failed  if any step ERROR
      - ready   if external_call DONE
      - else    processing
    """
    if any(status == StepStatus.ERROR.value for status in statuses.values()):
        return DocumentStatus.FAILED.value
    if statuses.get(StepName.EXTERNAL_CALL.value) == StepStatus.DONE.value:
        return DocumentStatus.READY.value
    return DocumentStatus.PROCESSING.value


class Transitioner:
    """Drives the pipeline DAG and the document status. Stateless apart from the
    graph it walks, so a single instance can be shared."""

    def __init__(self, graph: DependencyGraph = PIPELINE) -> None:
        self._graph = graph

    def claim_step(self, session: Session, document_id: uuid.UUID, step: StepName) -> bool:
        """Atomically move a step PENDING -> QUEUED. Returns True iff this caller
        won the race. Commits the claim and its NOTIFY together."""
        claimed = transactions.claim_pending_step(session, document_id, step.value)
        if claimed:
            emit_event(session, document_id, step_event(step.value, StepStatus.QUEUED.value))
        session.commit()
        return claimed

    def trigger_successors(self, session: Session, document_id: uuid.UUID) -> None:
        """Publish every step that has become ready (all predecessors DONE).
        Commit-then-publish; the claim is the exactly-once guard, so re-evaluating
        already-queued steps is harmless."""
        statuses = transactions.step_status_map(session, document_id)
        done = {StepName(name) for name, status in statuses.items() if status == StepStatus.DONE.value}
        for step in self._graph.ready_successors(done):
            if self.claim_step(session, document_id, step):
                publish_step(document_id, step)

    def recompute_document_status(self, session: Session, document_id: uuid.UUID) -> str:
        """Derive document status from its steps, persist it, emit on change."""
        statuses = transactions.step_status_map(session, document_id)
        new = derive_document_status(statuses)
        document = session.get(Document, document_id)
        if document is not None and document.status != new:
            document.status = new
            emit_event(session, document_id, doc_status_event(new))
            session.commit()
        return new
