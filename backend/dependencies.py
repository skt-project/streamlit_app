"""
FastAPI dependency for JWT authentication.
Usage: current_user: UserContext = Depends(require_auth)
"""
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError

from models.auth import UserContext
from services.auth import decode_token

_bearer = HTTPBearer()


def require_auth(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> UserContext:
    try:
        payload = decode_token(credentials.credentials)
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")

    try:
        return UserContext(
            user_id=payload["sub"],
            username=payload["username"],
            role=payload["role"],
            territory=payload.get("territory"),
            distributor_code=payload.get("distributor_code"),
        )
    except KeyError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Malformed token")


def require_role(*roles: str):
    """Factory: Depends(require_role('ho_admin', 'area_manager'))"""
    def _check(user: UserContext = Depends(require_auth)) -> UserContext:
        if user.role not in roles:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")
        return user
    return _check
