"""
GET  /admin/users           — list users
POST /admin/users           — create user
PUT  /admin/users/{id}      — update user
PATCH /admin/users/{id}     — toggle active
"""
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from config import settings
from dependencies import require_role
from models.auth import UserContext
from services.audit import log_event
from services.auth import create_access_token, hash_password as _hash_password
from services.bq import BQClient

router = APIRouter(prefix="/admin", tags=["admin"])

SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"


VALID_ROLES = {"salesman", "spv", "asm", "dm", "ho_admin", "demo"}


class UserCreate(BaseModel):
    username: str
    full_name: str
    password: str
    role: str
    email: str | None = None
    brand_group: str | None = None
    salesman_sk: str | None = None


class UserUpdate(BaseModel):
    full_name: str | None = None
    password: str | None = None
    role: str | None = None
    email: str | None = None
    brand_group: str | None = None
    salesman_sk: str | None = None


class ToggleActive(BaseModel):
    is_active: bool


@router.get("/users")
def list_users(
    search: str | None = Query(None),
    role: str | None = Query(None),
    is_active: bool | None = Query(None),  # None = show all (active + inactive + demo)
    current_user: UserContext = Depends(require_role("ho_admin")),
):
    bq = BQClient.get()
    clauses: list[str] = []
    params: list = []
    if search:
        clauses.append("(LOWER(username) LIKE LOWER(CONCAT('%',@q,'%')) OR LOWER(full_name) LIKE LOWER(CONCAT('%',@q,'%')))")
        params.append(bq.p("q", "STRING", search))
    if role:
        clauses.append("role = @role")
        params.append(bq.p("role", "STRING", role))
    # is_active filter is OPTIONAL — when omitted, HO admin sees ALL users
    # including demo accounts and deactivated users.
    if is_active is not None:
        clauses.append("is_active = @active")
        params.append(bq.p("active", "BOOL", is_active))

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    cache_key = f"admin:users:{search or ''}:{role or ''}:{is_active}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    rows = bq.query(
        f"""
        SELECT user_id, username, full_name, role, email, brand_group, salesman_sk, is_active
        FROM {SFA_WEB}.users
        {where}
        ORDER BY is_active DESC, role, full_name
        LIMIT 500
        """,
        params,
    )
    bq.cache.set(cache_key, rows, ttl=30)
    return rows


@router.post("/users", status_code=201)
def create_user(
    body: UserCreate,
    current_user: UserContext = Depends(require_role("ho_admin")),
):
    if len(body.password) < 8:
        raise HTTPException(status_code=422, detail="Password must be at least 8 characters")
    if body.role not in VALID_ROLES:
        raise HTTPException(status_code=422, detail=f"Invalid role. Must be one of: {sorted(VALID_ROLES)}")

    bq = BQClient.get()
    existing = bq.query_one(
        f"SELECT user_id FROM {SFA_WEB}.users WHERE username = @u",
        [bq.p("u", "STRING", body.username)],
    )
    if existing:
        raise HTTPException(status_code=400, detail="Username already exists")

    new_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    pw_hash = _hash_password(body.password)

    bq.execute(
        f"""
        INSERT INTO {SFA_WEB}.users
          (user_id, username, full_name, password_hash, role, email,
           brand_group, salesman_sk, is_active, created_at, updated_at)
        VALUES
          (@id, @u, @name, @pw, @role, @email,
           @bg, @sk, TRUE, @now, @now)
        """,
        [
            bq.p("id",    "STRING",    new_id),
            bq.p("u",     "STRING",    body.username),
            bq.p("name",  "STRING",    body.full_name),
            bq.p("pw",    "STRING",    pw_hash),
            bq.p("role",  "STRING",    body.role),
            bq.p("email", "STRING",    body.email or ""),
            bq.p("bg",    "STRING",    body.brand_group or None),
            bq.p("sk",    "STRING",    body.salesman_sk or ""),
            bq.p("now",   "TIMESTAMP", now),
        ],
    )
    bq.cache.invalidate("admin:users:")
    return {"user_id": new_id, "message": "User created."}


@router.put("/users/{user_id}")
def update_user(
    user_id: str,
    body: UserUpdate,
    current_user: UserContext = Depends(require_role("ho_admin")),
):
    if body.password is not None and len(body.password) < 8:
        raise HTTPException(status_code=422, detail="Password must be at least 8 characters")
    if body.role is not None and body.role not in VALID_ROLES:
        raise HTTPException(status_code=422, detail=f"Invalid role. Must be one of: {sorted(VALID_ROLES)}")

    bq = BQClient.get()
    sets, params = [], []
    if body.full_name:
        sets.append("full_name = @name"); params.append(bq.p("name", "STRING", body.full_name))
    if body.password:
        sets.append("password_hash = @pw"); params.append(bq.p("pw", "STRING", _hash_password(body.password)))
    if body.role:
        sets.append("role = @role"); params.append(bq.p("role", "STRING", body.role))
    if body.email is not None:
        sets.append("email = @email"); params.append(bq.p("email", "STRING", body.email))
    if body.brand_group is not None:
        sets.append("brand_group = @bg"); params.append(bq.p("bg", "STRING", body.brand_group))
    if body.salesman_sk is not None:
        sets.append("salesman_sk = @sk"); params.append(bq.p("sk", "STRING", body.salesman_sk))

    if not sets:
        raise HTTPException(status_code=400, detail="No fields to update")

    now = datetime.now(timezone.utc).isoformat()
    sets.append("updated_at = @now"); params.append(bq.p("now", "TIMESTAMP", now))
    params.append(bq.p("id", "STRING", user_id))
    bq.execute(
        f"UPDATE {SFA_WEB}.users SET {', '.join(sets)} WHERE user_id = @id",
        params,
    )
    bq.cache.invalidate("admin:users:")
    return {"message": "User updated."}


@router.patch("/users/{user_id}")
def toggle_active(
    user_id: str,
    body: ToggleActive,
    current_user: UserContext = Depends(require_role("ho_admin")),
):
    bq = BQClient.get()
    now = datetime.now(timezone.utc).isoformat()
    bq.execute(
        f"UPDATE {SFA_WEB}.users SET is_active = @active, updated_at = @now WHERE user_id = @id",
        [bq.p("active", "BOOL", body.is_active), bq.p("now", "TIMESTAMP", now), bq.p("id", "STRING", user_id)],
    )
    bq.cache.invalidate("admin:users:")
    log_event("user.toggle_active", "user", user_id, current_user.username,
              payload={"is_active": body.is_active})
    return {"message": "User status updated."}


@router.post("/users/{user_id}/reset-token")
def generate_reset_token(
    user_id: str,
    current_user: UserContext = Depends(require_role("ho_admin")),
):
    """Generate a 24-hour password reset token for a user. Give this token to the user."""
    bq = BQClient.get()
    user = bq.query_one(
        f"SELECT user_id, username FROM {SFA_WEB}.users WHERE user_id = @id AND is_active = TRUE",
        [bq.p("id", "STRING", user_id)],
    )
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    token = create_access_token({
        "sub": user_id,
        "username": user["username"],
        "purpose": "password_reset",
    })
    log_event("user.reset_token_generated", "user", user_id, current_user.username)
    return {
        "reset_token": token,
        "expires_in": "24 hours",
        "note": "Pass this token to the user. They POST to /api/v1/auth/reset-password with it.",
    }
