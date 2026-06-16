"""Partner-webhook business logic, independent of the web layer.

The route validates the request (signature + payload shape) and opens a session;
this applies an already-validated result to the document. Returns the response
body the route hands back.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app import transactions
from app.events.notify import emit_event, step_event
from app.models import StepName, StepStatus, WebhookEvent
from app.pipeline.transition import Transitioner
from app.schemas import WebhookPayload, WebhookStatus

LOGGER = logging.getLogger("app.webhooks")
_transitioner = Transitioner()


def apply_partner_result(session: Session, raw: bytes, payload: WebhookPayload) -> dict:
    # Record durably (audit + idempotency), then process.
    session.add(WebhookEvent(job_id=payload.job_id, signature_ok=True, payload=json.loads(raw)))
    session.commit()

    # Resolve the tenant/document SERVER-SIDE via the opaque job_id we issued.
    step = transactions.get_step_by_job_id(session, payload.job_id)
    if step is None:
        # Unknown job_id: nothing to act on. Opaque to probers.
        LOGGER.info("webhook for unknown job_id", extra={"job_id": payload.job_id})
        return {"status": "accepted"}

    # Idempotency: a terminal step ignores repeats.
    if step.status in (StepStatus.DONE.value, StepStatus.ERROR.value):
        LOGGER.info(
            "webhook ignored (already terminal)",
            extra={"job_id": payload.job_id, "document_id": str(step.document_id)},
        )
        return {"status": "duplicate-ignored"}

    step.finished_at = datetime.now(timezone.utc)
    if payload.status == WebhookStatus.COMPLETED:
        step.status = StepStatus.DONE.value
        step.result = payload.result
        emit_event(session, step.document_id, step_event(StepName.EXTERNAL_CALL.value, StepStatus.DONE.value))
    else:  # WebhookStatus.FAILED
        step.status = StepStatus.ERROR.value
        step.error_text = f"partner reported failure: {json.dumps(payload.result)}"
        emit_event(session, step.document_id, step_event(StepName.EXTERNAL_CALL.value, StepStatus.ERROR.value))
    session.commit()

    new_status = _transitioner.recompute_document_status(session, step.document_id)
    LOGGER.info(
        "webhook processed",
        extra={
            "job_id": payload.job_id,
            "document_id": str(step.document_id),
            "partner_status": payload.status,
            "document_status": new_status,
        },
    )
    return {"status": "processed", "document_status": new_status}
