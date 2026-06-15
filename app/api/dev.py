"""Local-only helpers, mounted only when ENV=local (see app/main.py).

`/dev/sign-webhook` lets you compute a valid X-Partner-Signature from Swagger so
the inbound webhook is testable end-to-end without a real partner.
"""
from __future__ import annotations

from fastapi import APIRouter, Request

from app.schemas import SignWebhookResponse
from app.webhook_security import compute_signature

router = APIRouter(tags=["dev"])


@router.post("/dev/sign-webhook", response_model=SignWebhookResponse)
async def sign_webhook(request: Request) -> SignWebhookResponse:
    """POST the exact JSON body you intend to send to /webhooks/partner; returns
    the signature for that body's raw bytes."""
    raw = await request.body()
    return SignWebhookResponse(signature=compute_signature(raw))
