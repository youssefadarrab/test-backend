from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.routes_impl import auth as impl
from app.db import get_session
from app.schemas import LoginRequest, TokenResponse

router = APIRouter(tags=["auth"])


@router.post("/auth/login", response_model=TokenResponse)
def login(body: LoginRequest, session: Session = Depends(get_session)) -> TokenResponse:
    """Dev login: exchange a seeded email for a JWT (no password). The token
    carries the user's org_id, which scopes every subsequent request."""
    token = impl.login(session, body.email)
    if token is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unknown email")
    return token
