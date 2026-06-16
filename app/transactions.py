"""Data-access helpers: the one place that builds and runs the SQL queries.

Routes, the worker and the transitioner call these named functions instead of
constructing `select(...)`/`update(...)` inline, so the call sites read as intent
and the queries live together.

These functions never commit; the caller owns the transaction boundary. That is
mandatory in two cases: `lock_stale_steps` holds FOR UPDATE locks that must live
until the caller commits, and `claim_pending_step` must be committed together with
the NOTIFY the caller emits right after it.
"""
from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.models import AppUser, Document, PipelineStep, StepStatus


def step_status_map(session: Session, document_id: uuid.UUID) -> dict[str, str]:
    """name -> status for every step of a document."""
    rows = session.execute(
        select(PipelineStep.name, PipelineStep.status).where(
            PipelineStep.document_id == document_id
        )
    ).all()
    return {name: status for name, status in rows}


def get_user_by_email(session: Session, email: str) -> AppUser | None:
    return session.execute(
        select(AppUser).where(AppUser.email == email)
    ).scalar_one_or_none()


def get_step_by_job_id(session: Session, job_id: str) -> PipelineStep | None:
    return session.execute(
        select(PipelineStep).where(PipelineStep.external_job_id == job_id)
    ).scalar_one_or_none()


def list_org_documents(session: Session, org_id: uuid.UUID) -> Sequence[tuple[Document, str]]:
    """(document, uploader_email) for one org, newest first."""
    return session.execute(
        select(Document, AppUser.email)
        .join(AppUser, Document.uploaded_by == AppUser.id)
        .where(Document.org_id == org_id)  # tenant scoping
        .order_by(Document.created_at.desc())
    ).all()


def lock_stale_steps(
    session: Session, statuses: Sequence[str], cutoff: datetime
) -> Sequence[PipelineStep]:
    """In-flight steps not touched since `cutoff`, claimed with FOR UPDATE SKIP
    LOCKED so concurrent reapers don't grab the same rows."""
    return (
        session.execute(
            select(PipelineStep)
            .where(PipelineStep.status.in_(statuses), PipelineStep.updated_at < cutoff)
            .with_for_update(skip_locked=True)
        )
        .scalars()
        .all()
    )


def claim_pending_step(session: Session, document_id: uuid.UUID, step_value: str) -> bool:
    """Atomically move a step PENDING -> QUEUED. True iff this caller won the row
    (the exactly-once fan-in guard)."""
    res = session.execute(
        update(PipelineStep)
        .where(
            PipelineStep.document_id == document_id,
            PipelineStep.name == step_value,
            PipelineStep.status == StepStatus.PENDING.value,
        )
        .values(status=StepStatus.QUEUED.value)
        .returning(PipelineStep.id)
    )
    return res.first() is not None
