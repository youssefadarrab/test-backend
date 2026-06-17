from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, status
from sqlalchemy.orm import Session

from app.api.routes_impl import documents as impl
from app.auth import get_current_user
from app.db import get_session
from app.models import AppUser
from app.schemas import DocumentCreated, DocumentDetail, PaginatedDocuments

router = APIRouter(tags=["documents"])


@router.post("/documents", response_model=DocumentCreated, status_code=status.HTTP_201_CREATED)
def upload_document(
    file: UploadFile,
    user: AppUser = Depends(get_current_user),
    session: Session = Depends(get_session),
) -> DocumentCreated:
    return impl.create_document(session, user, file.filename or "upload.bin", file.file.read())


@router.get("/documents", response_model=PaginatedDocuments)
def list_documents(
    limit: int = Query(20, ge=1, le=100, description="Page size"),
    cursor: str | None = Query(None, description="Opaque cursor from a previous page's next_cursor"),
    user: AppUser = Depends(get_current_user),
    session: Session = Depends(get_session),
) -> PaginatedDocuments:
    try:
        return impl.list_documents(session, user.org_id, limit, cursor)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid cursor")


@router.get("/documents/{document_id}", response_model=DocumentDetail)
def get_document(
    document_id: uuid.UUID,
    user: AppUser = Depends(get_current_user),
    session: Session = Depends(get_session),
) -> DocumentDetail:
    detail = impl.get_document_detail(session, user.org_id, document_id)
    # 404 (not 403) on missing/cross-tenant: do not leak existence.
    if detail is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="document not found")
    return detail
