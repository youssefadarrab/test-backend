from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import create_access_token
from app.db import get_db
from app.models import AppUser
from app.schemas import LoginRequest, TokenResponse

router = APIRouter(tags=["auth"])


@router.post("/auth/login", response_model=TokenResponse)
def login(body: LoginRequest, db: Session = Depends(get_db)) -> TokenResponse:
    """Dev login: exchange a seeded email for a JWT (no password). The token
    carries the user's org_id, which scopes every subsequent request."""
    user = db.execute(select(AppUser).where(AppUser.email == body.email)).scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unknown email")
    return TokenResponse(access_token=create_access_token(user))
