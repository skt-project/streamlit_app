"""
POST /auth/login   — verify credentials, return JWT
GET  /auth/me      — return current user from JWT
POST /auth/users   — create a new user (ho_admin only)
"""
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from google.cloud import bigquery

from config import settings
from dependencies import require_auth, require_role
from models.auth import LoginRequest, TokenResponse, UserContext
from services.auth import create_access_token, hash_password, verify_password
from services.bq import BQClient

router = APIRouter(prefix="/auth", tags=["auth"])


def _get_user_by_username(username: str) -> dict | None:
    bq = BQClient.get()
    sql = f"""
        SELECT user_id, username, password_hash, role, territory, distributor_code, brand_group, is_active, salesman_sk
        FROM {settings.table('users')}
        WHERE username = @username
        LIMIT 1
    """
    return bq.query_one(sql, [bq.p("username", "STRING", username)])


@router.post("/login", response_model=TokenResponse)
def login(body: LoginRequest):
    user = _get_user_by_username(body.username)
    if not user or not user.get("is_active"):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    if not verify_password(body.password, user["password_hash"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    # Update last_login
    bq = BQClient.get()
    bq.execute(
        f"""
        UPDATE {settings.table('users')}
        SET last_login = CURRENT_TIMESTAMP()
        WHERE user_id = @user_id
        """,
        [bq.p("user_id", "STRING", user["user_id"])],
    )

    sk = user.get("salesman_sk")
    token_payload = {
        "sub": user["user_id"],
        "username": user["username"],
        "role": user["role"],
        "territory": user.get("territory"),
        "distributor_code": user.get("distributor_code"),
        "brand_group": user.get("brand_group"),
        "salesman_sk": int(sk) if sk else None,
    }
    token = create_access_token(token_payload)

    return TokenResponse(
        access_token=token,
        user=UserContext(
            user_id=user["user_id"],
            username=user["username"],
            role=user["role"],
            territory=user.get("territory"),
            distributor_code=user.get("distributor_code"),
            brand_group=user.get("brand_group"),
            salesman_sk=int(sk) if sk else None,
        ),
    )


@router.get("/me", response_model=UserContext)
def me(current_user: UserContext = Depends(require_auth)):
    return current_user


class _CreateUserRequest(LoginRequest):
    role: str = "salesman"
    territory: str | None = None
    distributor_code: str | None = None
    brand_group: str | None = None  # 'SKT' | 'G2G' | None for ho_admin
    email: str | None = None


@router.post("/users", status_code=201)
def create_user(
    body: _CreateUserRequest,
    _: UserContext = Depends(require_role("ho_admin")),
):
    """Create a new STEP user. Only ho_admin can call this."""
    existing = _get_user_by_username(body.username)
    if existing:
        raise HTTPException(status_code=409, detail="Username already exists")

    bq = BQClient.get()
    user_id = str(uuid.uuid4())
    bq.execute(
        f"""
        INSERT INTO {settings.table('users')}
          (user_id, username, email, password_hash, role, territory, distributor_code, brand_group, is_active, created_at)
        VALUES
          (@user_id, @username, @email, @password_hash, @role, @territory, @distributor_code, @brand_group, TRUE, CURRENT_TIMESTAMP())
        """,
        [
            bq.p("user_id", "STRING", user_id),
            bq.p("username", "STRING", body.username),
            bq.p("email", "STRING", body.email),
            bq.p("password_hash", "STRING", hash_password(body.password)),
            bq.p("role", "STRING", body.role),
            bq.p("territory", "STRING", body.territory),
            bq.p("distributor_code", "STRING", body.distributor_code),
            bq.p("brand_group", "STRING", body.brand_group),
        ],
    )
    bq.cache.invalidate("users:")
    return {"user_id": user_id, "username": body.username, "role": body.role, "brand_group": body.brand_group}
