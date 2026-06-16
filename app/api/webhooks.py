from __future__ import annotations

import logging

from fastapi import APIRouter, Header, HTTPException, Request, status

from app.api.routes_impl import webhooks as impl
from app.db import session_scope
from app.schemas import WebhookPayload, WebhookResponse
from app.webhook_security import verify_signature

LOGGER = logging.getLogger("app.webhooks")
router = APIRouter(tags=["webhooks"])


@router.post("/webhooks/partner", response_model=WebhookResponse)
async def partner_webhook(
    request: Request,
    x_partner_signature: str | None = Header(default=None),
) -> WebhookResponse:
    # Request validation stays in the route: verify over the RAW bytes BEFORE
    # parsing (re-serialising would change the bytes), then parse the shape.
    raw = await request.body()
    if not verify_signature(raw, x_partner_signature):
        LOGGER.warning("webhook rejected: invalid signature")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="bad signature")

    try:
        payload = WebhookPayload.model_validate_json(raw)
    except Exception:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="bad payload")

    with session_scope() as session:
        return impl.apply_partner_result(session, raw, payload)
