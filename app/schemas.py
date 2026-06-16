from __future__ import annotations

import uuid
from datetime import datetime
from enum import auto
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


class DocumentDetail(BaseModel):
    id: uuid.UUID
    filename: str
    uploaded_by: str
    status: str
    steps: list[StepOut]
    # Aggregated extracted data, present only once the document is `ready`.
    data: dict[str, Any] | None = None


class WebhookPayload(BaseModel):
    job_id: str
    status: WebhookStatus
    result: dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime | None = None


class SignWebhookResponse(BaseModel):
    signature: str
    header: str = "X-Partner-Signature"
