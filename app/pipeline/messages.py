"""Broker message contracts.

A typed payload so the publisher and the worker share one schema instead of an
ad-hoc dict; pydantic validates the body on the way in and serialises it on the
way out.
"""
from __future__ import annotations

import uuid

from pydantic import BaseModel

from app.models import StepName


class BackendStepPayload(BaseModel):
    """The message a worker consumes to run one step of one document."""

    document_id: uuid.UUID
    step: StepName
