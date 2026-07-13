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

# Business group → brand mapping.
# Group A (SKT): Skintific, Timephoria, Facerinna
# Group B (G2G): G2G, Bodibreze, Nextprime
# DEMO: unrestricted — sees all brands and all salesmen (for demo/testing accounts)
BRAND_GROUPS: dict[str, list[str]] = {
    "SKT": ["Skintific", "Timephoria", "Facerinna"],
    "G2G": ["G2G", "Bodibreze", "Nextprime"],
    "DEMO": ["Skintific", "Timephoria", "Facerinna", "G2G", "Bodibreze", "Nextprime"],
}

# brand_groups that bypass all SQL row-level filters (see all routes, all salesmen)
_UNRESTRICTED_GROUPS = {"DEMO"}


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
            brand_group=payload.get("brand_group") or None,
            salesman_sk=sk or None,
        )
    except KeyError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Malformed token")


def require_role(*roles: str):
    """Factory: Depends(require_role('ho_admin', 'dm'))"""
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
    Returns (SQL fragment, BQ params) to filter by brand_group column on dim_salesman.
    ho_admin and dm see all brands; users without a brand_group get no filter.
    Pass table_alias (e.g. "sm") when the query joins multiple tables.
    """
    from services.bq import BQClient
    if user.role in ("ho_admin", "dm") or not user.brand_group or user.brand_group in _UNRESTRICTED_GROUPS:
        return "", []
    col = f"{table_alias}.brand_group" if table_alias else "brand_group"
    return f"AND {col} = @{param_name}", [BQClient.p(param_name, "STRING", user.brand_group)]


def brand_list_filter(
    user: UserContext,
    col: str = "brand",
    param_prefix: str = "bgb",
) -> tuple[str, list]:
    """
    Returns (SQL fragment, BQ params) to restrict a `brand` column to the brands
    that belong to the user's business group.  Used for tables (e.g. spv_target)
    that store the brand name rather than a brand_group foreign key.

    ho_admin / dm / no brand_group → no restriction (sees all brands).
    Unknown brand_group            → restrict to nothing (AND 1=0).
    """
    from services.bq import BQClient
    if user.role in ("ho_admin", "dm") or not user.brand_group or user.brand_group in _UNRESTRICTED_GROUPS:
        return "", []
    brands = BRAND_GROUPS.get(user.brand_group, [])
    if not brands:
        return "AND 1=0", []
    placeholders = ", ".join(f"@{param_prefix}_{i}" for i in range(len(brands)))
    params = [BQClient.p(f"{param_prefix}_{i}", "STRING", b) for i, b in enumerate(brands)]
    return f"AND {col} IN ({placeholders})", params
