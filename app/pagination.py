"""Opaque keyset cursors for list endpoints.

A cursor encodes the sort key of the last row on a page — here `(created_at, id)`.
The next page asks for rows ordered strictly after it, so paging stays O(page size)
at any depth and is stable while new rows are inserted (an offset would shift).

The cursor is base64url over `<created_at_iso>|<id>`. It is opaque to clients:
they pass back what they received and never construct it themselves.
"""
from __future__ import annotations

import base64
import binascii
import uuid
from datetime import datetime

_SEPARATOR = "|"


def encode_cursor(created_at: datetime, document_id: uuid.UUID) -> str:
    raw = f"{created_at.isoformat()}{_SEPARATOR}{document_id}".encode()
    return base64.urlsafe_b64encode(raw).decode()


def decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    """Returns (created_at, id). Raises ValueError on any malformed cursor so the
    route can turn it into a 400 instead of a 500."""
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        created_at_iso, document_id = raw.split(_SEPARATOR)
        return datetime.fromisoformat(created_at_iso), uuid.UUID(document_id)
    except (ValueError, binascii.Error) as exception:
        raise ValueError("invalid cursor") from exception
