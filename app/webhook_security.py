"""HMAC signing/verification for the partner webhook.

The signature is computed over the RAW request body bytes. Re-serialising would
change the bytes and break verification.
"""
from __future__ import annotations

import hashlib
import hmac

from app.config import get_settings

settings = get_settings()


def compute_signature(raw_body: bytes) -> str:
    return hmac.new(
        settings.partner_hmac_secret.encode(), raw_body, hashlib.sha256
    ).hexdigest()


def verify_signature(raw_body: bytes, provided: str | None) -> bool:
    if not provided:
        return False
    expected = compute_signature(raw_body)
    return hmac.compare_digest(expected, provided)  # constant-time
