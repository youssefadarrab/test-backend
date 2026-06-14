"""Per-step worker logic: run the (verbatim) step function, persist the outcome,
then transition the graph. Safe under at-least-once delivery:

  * idempotent: a redelivered message for an already-finished step is a no-op;
  * crash-safe: commit the terminal state, THEN ack, so a crash just redelivers;
  * attempt-bounded: the DB `attempts` counter drives terminal failure, with the
    broker delivery-limit as a backstop.

`handle_step` returns "ack" (consume) or "retry" (nack + requeue).
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.events.notify import emit_event
from app.models import Document, DocumentStatus, PipelineStep, StepName, StepStatus
from app.pipeline import steps as step_fns
from app.pipeline.transition import recompute_document_status, trigger_successors

settings = get_settings()
LOGGER = logging.getLogger("app.worker")

# Statuses from which there is no point (re)running a step.
_TERMINAL_FOR_RUN = {StepStatus.DONE.value, StepStatus.AWAITING_CALLBACK.value, StepStatus.ERROR.value}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _load_steps(session: Session, document_id: uuid.UUID) -> dict[str, PipelineStep]:
    rows = session.execute(
        select(PipelineStep).where(PipelineStep.document_id == document_id)
    ).scalars()
    return {s.name: s for s in rows}


def _run(step: StepName, by_name: dict[str, PipelineStep], document_id: uuid.UUID):
    """Invoke the right verbatim step function with inputs from predecessors."""
    if step is StepName.OCR:
        return {"text": step_fns.ocr()}
    if step is StepName.METADATA:
        text = by_name[StepName.OCR.value].result["text"]
        return step_fns.metadata(text)  # returns a dict
    if step is StepName.CHUNKING:
        text = by_name[StepName.OCR.value].result["text"]
        return {"chunks": step_fns.chunking(text)}
    if step is StepName.EXTERNAL_CALL:
        text = by_name[StepName.OCR.value].result["text"]
        meta = by_name[StepName.METADATA.value].result
        chunks = by_name[StepName.CHUNKING.value].result["chunks"]
        return {"external_job_id": step_fns.external_call(str(document_id), text, meta, chunks)}
    raise ValueError(f"unknown step {step}")  # pragma: no cover


def handle_step(session: Session, step_name: str, payload: dict) -> str:
    document_id = uuid.UUID(payload["document_id"])
    step = StepName(step_name)

    by_name = _load_steps(session, document_id)
    row = by_name.get(step.value)
    if row is None:  # pragma: no cover - message for a deleted document
        return "ack"

    # Idempotency: nothing to do for an already-finished step, or a failed doc.
    if row.status in _TERMINAL_FOR_RUN:
        return "ack"
    document = session.get(Document, document_id)
    if document is None or document.status == DocumentStatus.FAILED.value:
        return "ack"

    # Mark RUNNING (and surface it) before doing the work.
    row.status = StepStatus.RUNNING.value
    row.started_at = row.started_at or _now()
    emit_event(session, document_id, {"step": step.value, "status": StepStatus.RUNNING.value})
    session.commit()

    try:
        result = _run(step, by_name, document_id)
    except Exception as exc:  # the mocks raise ~1/3 of the time
        return _on_failure(session, document_id, row, exc)

    return _on_success(session, document_id, step, row, result)


def _on_success(
    session: Session, document_id: uuid.UUID, step: StepName, row: PipelineStep, result: dict
) -> str:
    if step is StepName.EXTERNAL_CALL:
        # Compute succeeded but the OUTCOME is pending the partner webhook.
        row.external_job_id = result["external_job_id"]
        row.status = StepStatus.AWAITING_CALLBACK.value
        emit_event(
            session,
            document_id,
            {"step": step.value, "status": StepStatus.AWAITING_CALLBACK.value},
        )
        session.commit()
        # No successors; document stays processing until the webhook arrives.
        return "ack"

    row.result = result
    row.status = StepStatus.DONE.value
    row.finished_at = _now()
    emit_event(session, document_id, {"step": step.value, "status": StepStatus.DONE.value})
    session.commit()

    trigger_successors(session, document_id, step)
    recompute_document_status(session, document_id)
    return "ack"


def _on_failure(
    session: Session, document_id: uuid.UUID, row: PipelineStep, exc: Exception
) -> str:
    row.attempts += 1
    error = f"{type(exc).__name__}: {exc}"
    LOGGER.warning("step %s failed (attempt %s): %s", row.name, row.attempts, error)

    if row.attempts >= settings.step_max_attempts:
        row.status = StepStatus.ERROR.value
        row.error_text = error
        row.finished_at = _now()
        emit_event(session, document_id, {"step": row.name, "status": StepStatus.ERROR.value})
        session.commit()
        recompute_document_status(session, document_id)  # -> failed
        return "ack"

    # Not exhausted: hand it back to the broker for redelivery.
    row.status = StepStatus.QUEUED.value
    session.commit()
    return "retry"
