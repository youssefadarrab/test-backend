"""Idempotent seed: 2 organizations, 1 user each, so the API is usable
immediately. Run after migrations (the init container does both)."""
from __future__ import annotations

import logging

from sqlalchemy import select

from app.db import session_scope
from app.models import AppUser, Organization

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("app.seed")

SEED = [
    ("Acme", "alice@acme.example"),
    ("Globex", "youssef@globex.example"),
]


def seed() -> None:
    with session_scope() as db:
        for org_name, email in SEED:
            org = db.execute(
                select(Organization).where(Organization.name == org_name)
            ).scalar_one_or_none()
            if org is None:
                org = Organization(name=org_name)
                db.add(org)
                db.flush()
            user = db.execute(
                select(AppUser).where(AppUser.email == email)
            ).scalar_one_or_none()
            if user is None:
                db.add(AppUser(org_id=org.id, email=email))
                LOGGER.info("seeded %s / %s", org_name, email)
        db.commit()
    LOGGER.info("seed complete")


if __name__ == "__main__":
    seed()
