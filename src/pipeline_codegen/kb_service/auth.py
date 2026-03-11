"""Auth dependencies for KB service."""

from __future__ import annotations

from fastapi import Header, HTTPException, status


def require_bearer_token(expected_token: str, authorization: str | None = Header(None)) -> None:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if token != expected_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid bearer token")
