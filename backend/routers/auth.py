"""
POST /auth/login              — verify credentials, return JWT
GET  /auth/me                 — return current user from JWT
POST /auth/users              — create a new user (ho_admin only)
POST /auth/reset-password     — reset password with a reset token
"""
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from google.cloud import bigquery
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

from config import settings
from dependencies import require_auth, require_role
from models.auth import LoginRequest, TokenResponse, UserContext
from services.audit import log_event
from services.auth import (
    create_access_token,
    decode_token,
    hash_password,
    verify_password,
)
from services.bq import BQClient

router = APIRouter(prefix="/auth", tags=["auth"])
limiter = Limiter(key_func=get_remote_address)


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
@limiter.limit("20/minute")
def login(request: Request, body: LoginRequest):
    user = _get_user_by_username(body.username)
    if not user or not user.get("is_active"):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    if not verify_password(body.password, user["password_hash"]):
        log_event("user.login_failed", "user", user["user_id"], body.username)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    bq = BQClient.get()

    # Silently upgrade legacy SHA-256 hashes to bcrypt on successful login.
    # This migrates old accounts incrementally without admin intervention.
    if not user["password_hash"].startswith(("$2b$", "$2a$")):
        try:
            bq.execute(
                f"UPDATE {settings.table('users')} SET password_hash = @pw WHERE user_id = @uid",
                [bq.p("pw", "STRING", hash_password(body.password)), bq.p("uid", "STRING", user["user_id"])],
            )
        except Exception:
            pass  # hash upgrade must never block login

    bq.execute(
        f"UPDATE {settings.table('users')} SET last_login = CURRENT_TIMESTAMP() WHERE user_id = @user_id",
        [bq.p("user_id", "STRING", user["user_id"])],
    )
    log_event("user.login", "user", user["user_id"], user["username"])

    sk = user.get("salesman_sk")
    brand_group = user.get("brand_group")

    # Real SE accounts are linked via salesman_sk to dim_salesman which carries
    # brand_group — the users table may not have it populated, so fall back.
    if not brand_group and sk:
        sm_row = bq.query_one(
            f"SELECT brand_group FROM {settings.table('dim_salesman')}"
            " WHERE salesman_sk = @sk LIMIT 1",
            [bq.p("sk", "STRING", sk)],
        )
        if sm_row:
            brand_group = sm_row.get("brand_group") or None

    token_payload = {
        "sub": user["user_id"],
        "username": user["username"],
        "role": user["role"],
        "territory": user.get("territory"),
        "distributor_code": user.get("distributor_code"),
        "brand_group": brand_group,
        "salesman_sk": sk or None,
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
            brand_group=brand_group,
            salesman_sk=sk or None,
        ),
    )


@router.get("/me", response_model=UserContext)
def me(current_user: UserContext = Depends(require_auth)):
    return current_user


# ── Password reset ──────────────────────────────────────────────────────────

class ResetTokenResponse(BaseModel):
    reset_token: str
    expires_in: str = "24 hours"
    note: str = "Pass this token to the user. They use POST /auth/reset-password to set a new password."


class ResetPasswordRequest(BaseModel):
    reset_token: str
    new_password: str


@router.post("/reset-password")
def reset_password(body: ResetPasswordRequest):
    """Use a reset token (issued by admin) to set a new password."""
    try:
        payload = decode_token(body.reset_token)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")

    if payload.get("purpose") != "password_reset":
        raise HTTPException(status_code=400, detail="Invalid token purpose")

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=400, detail="Malformed token")

    if len(body.new_password) < 8:
        raise HTTPException(status_code=422, detail="Password must be at least 8 characters")

    bq = BQClient.get()
    bq.execute(
        f"UPDATE {settings.table('users')} SET password_hash = @pw, updated_at = CURRENT_TIMESTAMP() WHERE user_id = @uid",
        [bq.p("pw", "STRING", hash_password(body.new_password)), bq.p("uid", "STRING", user_id)],
    )
    log_event("user.password_reset", "user", user_id, "system")
    return {"message": "Password updated successfully."}


# ── Create user (legacy, kept for backward compat) ──────────────────────────

class _CreateUserRequest(LoginRequest):
    role: str = "salesman"
    territory: str | None = None
    distributor_code: str | None = None
    brand_group: str | None = None
    email: str | None = None


@router.post("/users", status_code=201)
def create_user(
    body: _CreateUserRequest,
    current_user: UserContext = Depends(require_role("ho_admin")),
):
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
            bq.p("user_id",           "STRING", user_id),
            bq.p("username",          "STRING", body.username),
            bq.p("email",             "STRING", body.email),
            bq.p("password_hash",     "STRING", hash_password(body.password)),
            bq.p("role",              "STRING", body.role),
            bq.p("territory",         "STRING", body.territory),
            bq.p("distributor_code",  "STRING", body.distributor_code),
            bq.p("brand_group",       "STRING", body.brand_group),
        ],
    )
    log_event("user.create", "user", user_id, current_user.username, payload={"role": body.role})
    return {"user_id": user_id, "username": body.username, "role": body.role}
