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
