from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import get_settings

settings = get_settings()

# pool_pre_ping avoids handing out connections severed by the broker/db restart.
# Pool size is intentionally modest; it must be sized together with the API
# threadpool and Postgres max_connections (see docs/DESIGN.md, the sync/async note).
engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


def get_db() -> Iterator[Session]:
    """FastAPI dependency: one session per request, always closed."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def session_scope() -> Iterator[Session]:
    """Used by workers/reaper outside the request lifecycle."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
