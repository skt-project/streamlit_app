"""
GET  /admin/users           — list users
POST /admin/users           — create user
PUT  /admin/users/{id}      — update user
PATCH /admin/users/{id}     — toggle active
"""
import hashlib
import os
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from config import settings
from dependencies import require_role
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/admin", tags=["admin"])

SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"


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


def _hash_password(plain: str) -> str:
    salt = os.urandom(16).hex()
    hashed = hashlib.sha256((plain + salt).encode()).hexdigest()
    return f"{salt}:{hashed}"


@router.get("/users")
def list_users(
    search: str | None = Query(None),
    role: str | None = Query(None),
    current_user: UserContext = Depends(require_role("ho_admin")),
):
    bq = BQClient.get()
    clauses, params = [], []
    if search:
        clauses.append("(LOWER(username) LIKE LOWER(CONCAT('%',@q,'%')) OR LOWER(full_name) LIKE LOWER(CONCAT('%',@q,'%')))")
        params.append(bq.p("q", "STRING", search))
    if role:
        clauses.append("role = @role")
        params.append(bq.p("role", "STRING", role))

    where = ("WHERE " + " AND ".join(clauses) + " AND is_deleted = FALSE") if clauses else "WHERE is_deleted = FALSE"
    return bq.query(
        f"""
        SELECT user_id, username, full_name, role, email, brand_group, salesman_sk, is_active
        FROM {SFA_WEB}.users
        {where}
        ORDER BY role, full_name
        LIMIT 500
        """,
        params,
    )


@router.post("/users", status_code=201)
def create_user(
    body: UserCreate,
    current_user: UserContext = Depends(require_role("ho_admin")),
):
    bq = BQClient.get()
    existing = bq.query_one(
        f"SELECT user_id FROM {SFA_WEB}.users WHERE username = @u AND is_deleted = FALSE",
        [bq.p("u", "STRING", body.username)],
    )
    if existing:
        raise HTTPException(status_code=400, detail="Username already exists")

    new_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    pw_hash = _hash_password(body.password)
    sk = int(body.salesman_sk) if body.salesman_sk and body.salesman_sk.isdigit() else None

    bq.execute(
        f"""
        INSERT INTO {SFA_WEB}.users
          (user_id, username, full_name, password_hash, role, email,
           brand_group, salesman_sk, is_active, created_at, is_deleted)
        VALUES
          (@id, @u, @name, @pw, @role, @email,
           @bg, @sk, TRUE, @now, FALSE)
        """,
        [
            bq.p("id",    "STRING",    new_id),
            bq.p("u",     "STRING",    body.username),
            bq.p("name",  "STRING",    body.full_name),
            bq.p("pw",    "STRING",    pw_hash),
            bq.p("role",  "STRING",    body.role),
            bq.p("email", "STRING",    body.email or ""),
            bq.p("bg",    "STRING",    body.brand_group or ""),
            bq.p("sk",    "INT64",     sk or 0),
            bq.p("now",   "TIMESTAMP", now),
        ],
    )
    return {"user_id": new_id, "message": "User created."}


@router.put("/users/{user_id}")
def update_user(
    user_id: str,
    body: UserUpdate,
    current_user: UserContext = Depends(require_role("ho_admin")),
):
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
        sk = int(body.salesman_sk) if body.salesman_sk.isdigit() else 0
        sets.append("salesman_sk = @sk"); params.append(bq.p("sk", "INT64", sk))

    if not sets:
        raise HTTPException(status_code=400, detail="No fields to update")

    now = datetime.now(timezone.utc).isoformat()
    sets.append("updated_at = @now"); params.append(bq.p("now", "TIMESTAMP", now))
    params.append(bq.p("id", "STRING", user_id))
    bq.execute(
        f"UPDATE {SFA_WEB}.users SET {', '.join(sets)} WHERE user_id = @id AND is_deleted = FALSE",
        params,
    )
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
        f"UPDATE {SFA_WEB}.users SET is_active = @active, updated_at = @now WHERE user_id = @id AND is_deleted = FALSE",
        [bq.p("active", "BOOL", body.is_active), bq.p("now", "TIMESTAMP", now), bq.p("id", "STRING", user_id)],
    )
    return {"message": "User status updated."}
