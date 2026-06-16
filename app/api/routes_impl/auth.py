"""Auth business logic, independent of the web layer."""
from __future__ import annotations

from sqlalchemy.orm import Session

from app import transactions
from app.auth import create_access_token
from app.schemas import TokenResponse


def login(session: Session, email: str) -> TokenResponse | None:
    """Return a token for a seeded email, or None if it's unknown."""
    user = transactions.get_user_by_email(session, email)
    if user is None:
        return None
    return TokenResponse(access_token=create_access_token(user))
