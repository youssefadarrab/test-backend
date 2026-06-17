from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum, auto
from typing import Any

from pydantic import BaseModel, Field

from app.enums import LowerStrEnum


class WebhookStatus(LowerStrEnum):
    COMPLETED = auto()
    FAILED = auto()


class LoginRequest(BaseModel):
    email: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class StepOut(BaseModel):
    name: str
    status: str
    attempts: int
    error_text: str | None = None
    external_job_id: str | None = None


class DocumentCreated(BaseModel):
    id: uuid.UUID
    status: str


class DocumentListItem(BaseModel):
    id: uuid.UUID
    filename: str
    uploaded_by: str  # email of the importing user
    status: str
    created_at: datetime


class PaginatedDocuments(BaseModel):
    """One page of documents plus the cursor to fetch the next one.
    `next_cursor` is null when this is the last page."""

    items: list[DocumentListItem]
    next_cursor: str | None = None


class ExtractedData(BaseModel):
    """The pipeline's aggregated output, returned once a document is `ready`."""

    ocr_text: str | None = None
    metadata: dict[str, Any] | None = None
    chunks: list[str] | None = None
    external_job_id: str | None = None
    partner_result: dict[str, Any] | None = None


class DocumentDetail(BaseModel):
    id: uuid.UUID
    filename: str
    uploaded_by: str
    status: str
    steps: list[StepOut]
    # Present only once the document is `ready`.
    data: ExtractedData | None = None


class WebhookPayload(BaseModel):
    job_id: str
    status: WebhookStatus
    result: dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime | None = None


class WebhookOutcome(StrEnum):
    ACCEPTED = "accepted"            # unknown job_id, nothing to do
    DUPLICATE_IGNORED = "duplicate-ignored"  # already-terminal step
    PROCESSED = "processed"


class WebhookResponse(BaseModel):
    status: WebhookOutcome
    document_status: str | None = None


class SignWebhookResponse(BaseModel):
    signature: str
    header: str = "X-Partner-Signature"


# The webhook routes read the raw request body directly (request.body()) so the HMAC
# is computed over the exact bytes received. As a side effect FastAPI generates no
# request-body field for them, which leaves no editable JSON box in Swagger. This
# explicit spec, merged via the routes' `openapi_extra`, restores that editor (with a
# ready-to-edit example) so the webhook is testable end-to-end from /docs.
WEBHOOK_BODY_EXAMPLE = {
    "job_id": "j_abc123def4567890",
    "status": "completed",
    "result": {"indexed_at": "2026-06-06T06:06:06Z"},
    "occurred_at": "2026-06-06T06:06:06Z",
}

WEBHOOK_REQUEST_BODY_OPENAPI = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "required": ["job_id", "status"],
                    "properties": {
                        "job_id": {"type": "string"},
                        "status": {"type": "string", "enum": ["completed", "failed"]},
                        "result": {"type": "object"},
                        "occurred_at": {"type": "string", "format": "date-time"},
                    },
                },
                "example": WEBHOOK_BODY_EXAMPLE,
            }
        },
    }
}
