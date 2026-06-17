"""Document business logic, independent of the web layer.

The route handlers in app/api/documents.py are thin adapters over these
functions; everything here takes a session and plain values and returns schema
objects (or None), so it can be tested without HTTP.
"""
from __future__ import annotations

import uuid

from sqlalchemy.orm import Session

from app import transactions
from app.models import AppUser, Document, DocumentStatus, PipelineStep, StepName, StepStatus
from app.pagination import decode_cursor, encode_cursor
from app.pipeline.dag import PIPELINE
from app.pipeline.publisher import publish_step
from app.schemas import (
    DocumentCreated,
    DocumentDetail,
    DocumentListItem,
    ExtractedData,
    PaginatedDocuments,
    StepOut,
)
from app.storage import get_storage


def create_document(session: Session, user: AppUser, filename: str, data: bytes) -> DocumentCreated:
    document = Document(
        org_id=user.org_id,
        uploaded_by=user.id,
        filename=filename,
        storage_uri="",
        status=DocumentStatus.PROCESSING.value,
    )
    session.add(document)
    session.flush()  # assign document.id

    document.storage_uri = get_storage().save(user.org_id, document.id, data)

    # Create all step rows up front; the entry step(s) start QUEUED, the rest PENDING.
    entry = set(PIPELINE.entry_nodes)
    for step in PIPELINE.nodes:
        session.add(
            PipelineStep(
                document_id=document.id,
                name=step.value,
                status=StepStatus.QUEUED.value if step in entry else StepStatus.PENDING.value,
            )
        )
    session.commit()

    # Commit-then-publish: the entry step is already QUEUED, so the reaper would
    # re-publish it if this publish were lost.
    for step in PIPELINE.entry_nodes:
        publish_step(document.id, step)

    return DocumentCreated(id=document.id, status=document.status)


def list_documents(
    session: Session, org_id: uuid.UUID, limit: int, cursor: str | None = None
) -> PaginatedDocuments:
    """One keyset page of the org's documents. `cursor` is opaque; a malformed one
    raises ValueError (the route turns it into a 400)."""
    after = decode_cursor(cursor) if cursor else None

    # Fetch one extra row to learn whether a next page exists without a count query.
    rows = transactions.list_org_documents(session, org_id, limit + 1, after)
    has_more = len(rows) > limit
    rows = rows[:limit]

    items = [
        DocumentListItem(
            id=document.id,
            filename=document.filename,
            uploaded_by=email,
            status=document.status,
            created_at=document.created_at,
        )
        for document, email in rows
    ]

    next_cursor = None
    if has_more:
        last_document = rows[-1][0]
        next_cursor = encode_cursor(last_document.created_at, last_document.id)

    return PaginatedDocuments(items=items, next_cursor=next_cursor)


def _extracted_data(steps: dict[str, PipelineStep]) -> ExtractedData:
    ocr_result = steps[StepName.OCR.value].result or {}
    external_step = steps[StepName.EXTERNAL_CALL.value]
    return ExtractedData(
        ocr_text=ocr_result.get("text"),
        metadata=steps[StepName.METADATA.value].result,
        chunks=(steps[StepName.CHUNKING.value].result or {}).get("chunks"),
        external_job_id=external_step.external_job_id,
        partner_result=external_step.result,
    )


def get_document_detail(
    session: Session, org_id: uuid.UUID, document_id: uuid.UUID
) -> DocumentDetail | None:
    """Returns None if the document doesn't exist or belongs to another org
    (the route turns that into a 404, so existence isn't leaked)."""
    document = session.get(Document, document_id)
    if document is None or document.org_id != org_id:
        return None

    steps = {step.name: step for step in document.steps}
    data = _extracted_data(steps) if document.status == DocumentStatus.READY.value else None

    return DocumentDetail(
        id=document.id,
        filename=document.filename,
        uploaded_by=document.uploader.email,
        status=document.status,
        steps=[
            StepOut(
                name=step.name,
                status=step.status,
                attempts=step.attempts,
                error_text=step.error_text,
                external_job_id=step.external_job_id,
            )
            for step in document.steps
        ],
        data=data,
    )
