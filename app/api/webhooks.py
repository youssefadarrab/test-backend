from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Header, HTTPException, Request, status

from app import transactions
from app.db import session_scope
from app.events.notify import emit_event
from app.models import StepName, StepStatus, WebhookEvent
from app.pipeline.transition import recompute_document_status
from app.schemas import WebhookPayload
from app.webhook_security import verify_signature

LOGGER = logging.getLogger("app.webhooks")
router = APIRouter(tags=["webhooks"])


@router.post("/webhooks/partner")
async def partner_webhook(
    request: Request,
    x_partner_signature: str | None = Header(default=None),
) -> dict:
    # 1. Verify over the RAW bytes, BEFORE any parsing.
    raw = await request.body()
    signature_ok = verify_signature(raw, x_partner_signature)
    if not signature_ok:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="bad signature")

    try:
        payload = WebhookPayload.model_validate_json(raw)
    except Exception:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="bad payload")

    with session_scope() as session:
        # 2. Record durably (audit + idempotency), then process.
        session.add(
            WebhookEvent(
                job_id=payload.job_id,
                signature_ok=True,
                payload=json.loads(raw),
            )
        )
        session.commit()

        # 3. Resolve the tenant/document SERVER-SIDE via the opaque job_id we issued.
        step = transactions.get_step_by_job_id(session, payload.job_id)
        if step is None:
            # Unknown job_id: nothing to act on. 202 keeps us opaque to probers.
            return {"status": "accepted"}

        # 4. Idempotency: a terminal step ignores repeats.
        if step.status in (StepStatus.DONE.value, StepStatus.ERROR.value):
            return {"status": "duplicate-ignored"}

        if payload.status == "completed":
            step.status = StepStatus.DONE.value
            step.result = payload.result
            step.finished_at = datetime.now(timezone.utc)
            emit_event(
                session,
                step.document_id,
                {"step": StepName.EXTERNAL_CALL.value, "status": StepStatus.DONE.value},
            )
        else:  # "failed"
            step.status = StepStatus.ERROR.value
            step.error_text = f"partner reported failure: {json.dumps(payload.result)}"
            step.finished_at = datetime.now(timezone.utc)
            emit_event(
                session,
                step.document_id,
                {"step": StepName.EXTERNAL_CALL.value, "status": StepStatus.ERROR.value},
            )
        session.commit()

        new_status = recompute_document_status(session, step.document_id)

    return {"status": "processed", "document_status": new_status}
