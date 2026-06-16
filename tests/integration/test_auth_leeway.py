"""A token expired within the leeway window is still accepted; beyond it, rejected."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import jwt

from app.config import get_settings

settings = get_settings()


def _token(sub, exp) -> str:
    return jwt.encode({"sub": str(sub), "exp": exp}, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def test_expired_within_leeway_is_accepted(client, seeded):
    # Expired a few seconds ago, inside the default 30s leeway.
    exp = datetime.now(timezone.utc) - timedelta(seconds=5)
    resp = client.get("/v1/docpipe/documents", headers=_auth(_token(seeded["alice"], exp)))
    assert resp.status_code == 200


def test_expired_beyond_leeway_is_rejected(client, seeded):
    exp = datetime.now(timezone.utc) - timedelta(seconds=300)
    resp = client.get("/v1/docpipe/documents", headers=_auth(_token(seeded["alice"], exp)))
    assert resp.status_code == 401
