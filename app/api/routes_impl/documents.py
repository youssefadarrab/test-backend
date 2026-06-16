"""Document business logic, independent of the web layer.

The route handlers in app/api/documents.py are thin adapters over these
functions; everything here takes a session and plain values and returns schema
objects (or None), so it can be tested without HTTP.
"""
from __future__ import annotations

import os
import uuid

from sqlalchemy.orm import Session

from app import transactions
from app.config import get_settings
from app.models import AppUser, Document, DocumentStatus, PipelineStep, StepName, StepStatus
from app.pipeline.dag import PIPELINE
from app.pipeline.publisher import publish_step
from app.schemas import DocumentCreated, DocumentDetail, DocumentListItem, StepOut

settings = get_settings()


def _store_bytes(org_id: uuid.UUID, document_id: uuid.UUID, data: bytes) -> str:
    # The file is never read (mocks only); we just persist it to a volume.
    folder = os.path.join(settings.storage_dir, str(org_id))
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, str(document_id))
    with open(path, "wb") as fh:
        fh.write(data)
    return path


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

    document.storage_uri = _store_bytes(user.org_id, document.id, data)

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


def list_documents(session: Session, org_id: uuid.UUID) -> list[DocumentListItem]:
    return [
        DocumentListItem(
            id=doc.id,
            filename=doc.filename,
            uploaded_by=email,
            status=doc.status,
            created_at=doc.created_at,
        )
        for doc, email in transactions.list_org_documents(session, org_id)
    ]


def _extracted_data(steps: dict[str, PipelineStep]) -> dict:
    ocr = steps[StepName.OCR.value].result or {}
    ext = steps[StepName.EXTERNAL_CALL.value]
    return {
        "ocr_text": ocr.get("text"),
        "metadata": steps[StepName.METADATA.value].result,
        "chunks": (steps[StepName.CHUNKING.value].result or {}).get("chunks"),
        "external_job_id": ext.external_job_id,
        "partner_result": ext.result,
    }


def get_document_detail(
    session: Session, org_id: uuid.UUID, document_id: uuid.UUID
) -> DocumentDetail | None:
    """Returns None if the document doesn't exist or belongs to another org
    (the route turns that into a 404, so existence isn't leaked)."""
    document = session.get(Document, document_id)
    if document is None or document.org_id != org_id:
        return None

    steps = {s.name: s for s in document.steps}
    data = _extracted_data(steps) if document.status == DocumentStatus.READY.value else None

    return DocumentDetail(
        id=document.id,
        filename=document.filename,
        uploaded_by=document.uploader.email,
        status=document.status,
        steps=[
            StepOut(
                name=s.name,
                status=s.status,
                attempts=s.attempts,
                error_text=s.error_text,
                external_job_id=s.external_job_id,
            )
            for s in document.steps
        ],
        data=data,
    )
