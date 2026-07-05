"""
JWT creation/verification and password hashing utilities.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

import bcrypt
from jose import JWTError, jwt

from config import settings


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def _verify_legacy_sha256(plain: str, hashed: str) -> bool:
    """Verify passwords hashed by the old SHA256 scheme: 'salt:sha256hex'."""
    try:
        salt, digest = hashed.split(":", 1)
        expected = hashlib.sha256((plain + salt).encode()).hexdigest()
        return expected == digest
    except Exception:
        return False


def verify_password(plain: str, hashed: str) -> bool:
    """Verify bcrypt hash, falling back to legacy SHA256 for pre-migration accounts."""
    if hashed.startswith("$2b$") or hashed.startswith("$2a$"):
        try:
            return bcrypt.checkpw(plain.encode(), hashed.encode())
        except Exception:
            return False
    return _verify_legacy_sha256(plain, hashed)


def create_access_token(payload: dict) -> str:
    data = payload.copy()
    data["exp"] = datetime.now(timezone.utc) + timedelta(hours=settings.jwt_expiry_hours)
    return jwt.encode(data, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def decode_token(token: str) -> dict:
    """Raises JWTError if token is invalid or expired."""
    return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
