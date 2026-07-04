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
        sk = payload.get("salesman_sk")
        return UserContext(
            user_id=payload["sub"],
            username=payload["username"],
            role=payload["role"],
            territory=payload.get("territory"),
            distributor_code=payload.get("distributor_code"),
            brand_group=payload.get("brand_group"),
            salesman_sk=int(sk) if sk else None,
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


def brand_group_filter(
    user: UserContext,
    param_name: str = "bg",
    table_alias: str = "",
) -> tuple[str, list]:
    """
    Returns (SQL fragment, BQ params) to filter by brand_group.
    ho_admin (brand_group=None) gets no filter — sees all groups.
    Pass table_alias (e.g. "v") when the query joins multiple tables with brand_group.
    """
    from services.bq import BQClient
    if user.role == "ho_admin" or not user.brand_group:
        return "", []
    col = f"{table_alias}.brand_group" if table_alias else "brand_group"
    return f"AND {col} = @{param_name}", [BQClient.p(param_name, "STRING", user.brand_group)]
