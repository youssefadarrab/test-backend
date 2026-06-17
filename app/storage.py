"""File storage behind one interface.

Uploaded bytes are persisted through a Storage backend so the storage target can
change per environment without touching call sites — the same shape as SecretManager.
Today it is local disk; a deployed setup swaps in GCS or S3 behind the same interface.
The returned value is a scheme-qualified URI (file://, gs://, s3://) so whatever later
reads the file knows how to open it.
"""
from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Protocol

from app.config import get_settings


class Storage(Protocol):
    def save(self, org_id: uuid.UUID, document_id: uuid.UUID, data: bytes) -> str:
        """Persist the bytes for a document and return their storage URI."""
        ...


class LocalDiskStorage:
    """Stores files under base_dir/<org_id>/<document_id>. The file is never read in
    this exercise; this just persists the upload to a volume."""

    def __init__(self, base_dir: str) -> None:
        self._base_dir = base_dir

    def save(self, org_id: uuid.UUID, document_id: uuid.UUID, data: bytes) -> str:
        folder = os.path.join(self._base_dir, str(org_id))
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, str(document_id))
        with open(path, "wb") as file_handle:
            file_handle.write(data)
        return Path(os.path.abspath(path)).as_uri()  # file:///...


def get_storage() -> Storage:
    """Resolve the storage backend. Reads settings on each call so an override (e.g.
    a test pointing storage_dir at a tmp dir) is always honored."""
    return LocalDiskStorage(get_settings().storage_dir)
