"""Per-step worker logic.

`StepHandler` is a template: the base orchestrates the lifecycle (idempotency,
mark RUNNING, load -> transform -> execute -> transform -> save, then hand off to
the Transitioner, with failure handling). Each concrete handler fills in the
verbatim step call (`execute`) and, where needed, how its inputs/outputs are
shaped.

Safe under at-least-once delivery: idempotent (a redelivered message for a
finished step is a no-op), crash-safe (commit the terminal state, THEN ack),
attempt-bounded (the DB `attempts` counter drives terminal failure, with the
broker delivery-limit as a backstop).

`handle` returns "ack" (consume) or "retry" (nack + requeue).
"""
from __future__ import annotations

import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.events.notify import emit_event
from app.models import Document, DocumentStatus, PipelineStep, StepName, StepStatus
from app.pipeline import steps as step_fns
from app.pipeline.messages import BackendStepPayload
from app.pipeline.transition import Transitioner

settings = get_settings()
LOGGER = logging.getLogger("app.handlers")

# Statuses from which there is no point (re)running a step.
_TERMINAL_FOR_RUN = {StepStatus.DONE.value, StepStatus.AWAITING_CALLBACK.value, StepStatus.ERROR.value}


def _now() -> datetime:
    return datetime.now(timezone.utc)


class StepHandler(ABC):
    """Template for one pipeline step. Subclasses set `step` and implement
    `execute`; the input/output hooks have sensible defaults."""

    step: StepName

    def __init__(self, transitioner: Transitioner | None = None) -> None:
        self._transitioner = transitioner or Transitioner()

    # ---- the verbatim step call (the only required override) ----
    @abstractmethod
    def execute(self, step_input: Any) -> Any:
        """Call the provided step function and return its raw result."""

    # ---- input/output hooks (override as needed) ----
    def load_input(self, by_name: dict[str, PipelineStep]) -> dict[str, PipelineStep]:
        """Pick the predecessor rows this step needs. Default: all of them."""
        return by_name

    def transform_input(self, loaded: dict[str, PipelineStep]) -> Any:
        """Shape the loaded rows into the argument `execute` expects. Default: none."""
        return None

    def transform_output(self, raw_output: Any) -> dict:
        """Shape the step's raw return into the JSON stored on the row. Default: as-is."""
        return raw_output

    def save_output(self, session: Session, document_id: uuid.UUID, row: PipelineStep, output: dict) -> None:
        """Persist a successful result and mark the step DONE."""
        row.result = output
        row.status = StepStatus.DONE.value
        row.finished_at = _now()
        emit_event(session, document_id, {"step": self.step.value, "status": StepStatus.DONE.value})
        session.commit()
        LOGGER.info("step done", extra={"document_id": str(document_id), "step": self.step.value})

    # ---- lifecycle (shared) ----
    def handle(self, session: Session, payload: BackendStepPayload) -> str:
        document_id = payload.document_id
        by_name = self._load_steps(session, document_id)
        row = by_name.get(self.step.value)
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
        emit_event(session, document_id, {"step": self.step.value, "status": StepStatus.RUNNING.value})
        session.commit()

        try:
            step_input = self.transform_input(self.load_input(by_name))
            output = self.transform_output(self.execute(step_input))
        except Exception as exc:  # the mocks raise ~1/3 of the time
            return self._on_failure(session, document_id, row, exc)

        self.save_output(session, document_id, row, output)
        # DONE fans out; AWAITING_CALLBACK (external_call) waits for the webhook.
        if row.status == StepStatus.DONE.value:
            self._transitioner.trigger_successors(session, document_id)
            self._transitioner.recompute_document_status(session, document_id)
        return "ack"

    @staticmethod
    def _load_steps(session: Session, document_id: uuid.UUID) -> dict[str, PipelineStep]:
        rows = session.execute(
            select(PipelineStep).where(PipelineStep.document_id == document_id)
        ).scalars()
        return {s.name: s for s in rows}

    def _on_failure(
        self, session: Session, document_id: uuid.UUID, row: PipelineStep, exc: Exception
    ) -> str:
        row.attempts += 1
        error = f"{type(exc).__name__}: {exc}"
        context = {"document_id": str(document_id), "step": row.name, "attempt": row.attempts, "error": error}

        if row.attempts >= settings.step_max_attempts:
            row.status = StepStatus.ERROR.value
            row.error_text = error
            row.finished_at = _now()
            emit_event(session, document_id, {"step": row.name, "status": StepStatus.ERROR.value})
            session.commit()
            LOGGER.error("step failed permanently", extra=context)
            self._transitioner.recompute_document_status(session, document_id)  # -> failed
            return "ack"

        LOGGER.warning("step failed, will retry", extra=context)
        row.status = StepStatus.QUEUED.value  # hand back to the broker for redelivery
        session.commit()
        return "retry"


class OcrStepHandler(StepHandler):
    step = StepName.OCR

    def execute(self, step_input: Any) -> str:
        return step_fns.ocr()

    def transform_output(self, raw_output: str) -> dict:
        return {"text": raw_output}


class MetadataStepHandler(StepHandler):
    step = StepName.METADATA

    def transform_input(self, loaded: dict[str, PipelineStep]) -> str:
        return loaded[StepName.OCR.value].result["text"]

    def execute(self, step_input: str) -> dict:
        return step_fns.metadata(step_input)


class ChunkingStepHandler(StepHandler):
    step = StepName.CHUNKING

    def transform_input(self, loaded: dict[str, PipelineStep]) -> str:
        return loaded[StepName.OCR.value].result["text"]

    def execute(self, step_input: str) -> list[str]:
        return step_fns.chunking(step_input)

    def transform_output(self, raw_output: list[str]) -> dict:
        return {"chunks": raw_output}


class ExternalCallStepHandler(StepHandler):
    step = StepName.EXTERNAL_CALL

    def transform_input(self, loaded: dict[str, PipelineStep]) -> tuple:
        ocr = loaded[StepName.OCR.value]
        return (
            str(ocr.document_id),
            ocr.result["text"],
            loaded[StepName.METADATA.value].result,
            loaded[StepName.CHUNKING.value].result["chunks"],
        )

    def execute(self, step_input: tuple) -> str:
        return step_fns.external_call(*step_input)

    def transform_output(self, raw_output: str) -> dict:
        return {"external_job_id": raw_output}

    def save_output(self, session: Session, document_id: uuid.UUID, row: PipelineStep, output: dict) -> None:
        # Compute succeeded but the OUTCOME is pending the partner webhook.
        row.external_job_id = output["external_job_id"]
        row.status = StepStatus.AWAITING_CALLBACK.value
        emit_event(
            session, document_id, {"step": self.step.value, "status": StepStatus.AWAITING_CALLBACK.value}
        )
        session.commit()
        LOGGER.info(
            "external call sent, awaiting callback",
            extra={"document_id": str(document_id), "step": self.step.value, "job_id": row.external_job_id},
        )


# One handler per step, sharing a single Transitioner.
_TRANSITIONER = Transitioner()
STEP_HANDLERS: dict[StepName, StepHandler] = {
    StepName.OCR: OcrStepHandler(_TRANSITIONER),
    StepName.METADATA: MetadataStepHandler(_TRANSITIONER),
    StepName.CHUNKING: ChunkingStepHandler(_TRANSITIONER),
    StepName.EXTERNAL_CALL: ExternalCallStepHandler(_TRANSITIONER),
}
