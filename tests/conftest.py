"""Test fixtures.

Unit tests (tests/unit) need nothing external. Integration/functional tests use a
real Postgres via testcontainers (so the partial index, JSONB, and the atomic
fan-in UPDATE are exercised against the real engine), with the broker and the
NOTIFY listener stubbed out.

Only fixtures that explicitly depend on `db` start the Postgres container, so the
unit suite runs with no Docker.
"""
from __future__ import annotations

import threading

import pytest
from sqlalchemy import text


# ---------------------------------------------------------------------------
# Postgres (session-scoped) + engine rebind
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def _pg_url():
    # Allow pointing at an already-running Postgres (CI / local) instead of
    # spinning a container.
    import os

    existing = os.environ.get("TEST_DATABASE_URL")
    if existing:
        yield existing
        return

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16") as pg:
        yield pg.get_connection_url()  # postgresql+psycopg2://...


@pytest.fixture(scope="session")
def _engine(_pg_url):
    from sqlalchemy import create_engine

    import app.db as db
    import app.models as models

    db.engine = create_engine(_pg_url, future=True)
    db.SessionLocal.configure(bind=db.engine)
    models.Base.metadata.create_all(db.engine)
    return db.engine


@pytest.fixture
def db(_engine):
    """Function-scoped clean database; truncates after each test."""
    import app.db as db_module

    yield db_module
    with _engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE webhook_event, pipeline_step, document, app_user, organization "
                "RESTART IDENTITY CASCADE"
            )
        )


# ---------------------------------------------------------------------------
# Fakes: broker publish + NOTIFY listener
# ---------------------------------------------------------------------------
class PublishRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self._lock = threading.Lock()

    def __call__(self, document_id, step) -> None:
        with self._lock:
            self.calls.append((document_id, step))


@pytest.fixture
def fake_publish(monkeypatch) -> PublishRecorder:
    recorder = PublishRecorder()
    for mod in ("app.pipeline.transition", "app.api.routes_impl.documents", "app.worker.reaper"):
        monkeypatch.setattr(f"{mod}.publish_step", recorder, raising=True)
    return recorder


@pytest.fixture
def no_listener(monkeypatch):
    monkeypatch.setattr("app.events.notify.broker.start", lambda *a, **k: None)


# ---------------------------------------------------------------------------
# Domain helpers
# ---------------------------------------------------------------------------
@pytest.fixture
def seeded(db):
    """Two orgs, one user each."""
    from app.models import AppUser, Organization

    with db.session_scope() as s:
        acme = Organization(name="Acme")
        globex = Organization(name="Globex")
        s.add_all([acme, globex])
        s.flush()
        alice = AppUser(org_id=acme.id, email="alice@acme.example")
        youssef = AppUser(org_id=globex.id, email="youssef@globex.example")
        s.add_all([alice, youssef])
        s.commit()
        return {"acme": acme.id, "globex": globex.id, "alice": alice.id, "youssef": youssef.id}


@pytest.fixture
def client(db, no_listener, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    import app.config
    import app.main as main

    # Uploads go to a writable temp dir (the prod default /data/storage is a volume).
    monkeypatch.setattr(app.config.get_settings(), "storage_dir", str(tmp_path / "storage"))

    with TestClient(main.app) as c:
        yield c
