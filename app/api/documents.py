from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from app.api.routes_impl import documents as impl
from app.auth import get_current_user
from app.db import get_db
from app.models import AppUser
from app.schemas import DocumentCreated, DocumentDetail, DocumentListItem

router = APIRouter(tags=["documents"])


@router.post("/documents", response_model=DocumentCreated, status_code=status.HTTP_201_CREATED)
def upload_document(
    file: UploadFile,
    user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> DocumentCreated:
    return impl.create_document(db, user, file.filename or "upload.bin", file.file.read())


@router.get("/documents", response_model=list[DocumentListItem])
def list_documents(
    user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[DocumentListItem]:
    return impl.list_documents(db, user.org_id)


@router.get("/documents/{document_id}", response_model=DocumentDetail)
def get_document(
    document_id: uuid.UUID,
    user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> DocumentDetail:
    detail = impl.get_document_detail(db, user.org_id, document_id)
    # 404 (not 403) on missing/cross-tenant: do not leak existence.
    if detail is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="document not found")
    return detail
