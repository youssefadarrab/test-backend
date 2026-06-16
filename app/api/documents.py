from __future__ import annotations

import os
import uuid

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from app import transactions
from app.auth import get_current_user
from app.config import get_settings
from app.db import get_db
from app.models import AppUser, Document, DocumentStatus, PipelineStep, StepName, StepStatus
from app.pipeline.dag import PIPELINE
from app.pipeline.publisher import publish_step
from app.schemas import DocumentCreated, DocumentDetail, DocumentListItem, StepOut

settings = get_settings()
router = APIRouter(tags=["documents"])


def _store_bytes(org_id: uuid.UUID, document_id: uuid.UUID, data: bytes) -> str:
    # The file is never read (mocks only); we just persist it to a volume.
    folder = os.path.join(settings.storage_dir, str(org_id))
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, str(document_id))
    with open(path, "wb") as fh:
        fh.write(data)
    return path


@router.post("/documents", response_model=DocumentCreated, status_code=status.HTTP_201_CREATED)
def upload_document(
    file: UploadFile,
    user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> DocumentCreated:
    document = Document(
        org_id=user.org_id,
        uploaded_by=user.id,
        filename=file.filename or "upload.bin",
        storage_uri="",
        status=DocumentStatus.PROCESSING.value,
    )
    db.add(document)
    db.flush()  # assign document.id

    document.storage_uri = _store_bytes(user.org_id, document.id, file.file.read())

    # Create all step rows up front; the entry step(s) start QUEUED, the rest PENDING.
    entry = set(PIPELINE.entry_nodes)
    for step in PIPELINE.nodes:
        db.add(
            PipelineStep(
                document_id=document.id,
                name=step.value,
                status=StepStatus.QUEUED.value if step in entry else StepStatus.PENDING.value,
            )
        )
    db.commit()

    # Commit-then-publish: the entry step is already QUEUED in the DB; the reaper
    # would re-publish it if this publish were lost.
    for step in PIPELINE.entry_nodes:
        publish_step(document.id, step)

    return DocumentCreated(id=document.id, status=document.status)


@router.get("/documents", response_model=list[DocumentListItem])
def list_documents(
    user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[DocumentListItem]:
    rows = transactions.list_org_documents(db, user.org_id)
    return [
        DocumentListItem(
            id=doc.id,
            filename=doc.filename,
            uploaded_by=email,
            status=doc.status,
            created_at=doc.created_at,
        )
        for doc, email in rows
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


@router.get("/documents/{document_id}", response_model=DocumentDetail)
def get_document(
    document_id: uuid.UUID,
    user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> DocumentDetail:
    document = db.get(Document, document_id)
    # 404 (not 403) on cross-tenant access: do not leak existence.
    if document is None or document.org_id != user.org_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="document not found")

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
