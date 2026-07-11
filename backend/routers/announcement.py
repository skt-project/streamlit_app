"""
GET  /announcements          — list announcements
POST /announcements          — create (ho_admin only)
"""
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from config import settings
from dependencies import require_auth, require_role
from models.auth import UserContext
from services.bq import BQClient
from services.push import send_push_bulk

router = APIRouter(prefix="/announcements", tags=["announcements"])

SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"


class AnnouncementCreate(BaseModel):
    type: str
    title: str
    body: str
    audience: str = "Semua"


@router.get("")
def list_announcements(
    type: str | None = Query(None),
    limit: int = Query(50, le=200),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    type_clause = "AND type = @atype" if type else ""
    params = [bq.p("lim", "INT64", limit)]
    if type:
        params.append(bq.p("atype", "STRING", type))

    cache_key = f"announcements:{type or 'all'}:{limit}"
    rows = bq.query_cached(
        cache_key,
        f"""
        SELECT announcement_id, type, title, body, audience, created_by, created_at
        FROM {SFA_WEB}.announcement
        WHERE is_deleted = FALSE {type_clause}
        ORDER BY created_at DESC
        LIMIT @lim
        """,
        params,
        ttl=300,  # 5 minutes — announcements change infrequently
    )
    return [
        {**r, "created_at": str(r["created_at"])} for r in rows
    ]


@router.post("", status_code=201)
def create_announcement(
    body: AnnouncementCreate,
    current_user: UserContext = Depends(require_role("ho_admin")),
):
    bq = BQClient.get()
    new_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    bq.execute(
        f"""
        INSERT INTO {SFA_WEB}.announcement
          (announcement_id, type, title, body, audience, created_by, created_at, is_deleted)
        VALUES
          (@id, @tp, @title, @body, @audience, @author, @now, FALSE)
        """,
        [
            bq.p("id",       "STRING",    new_id),
            bq.p("tp",       "STRING",    body.type),
            bq.p("title",    "STRING",    body.title),
            bq.p("body",     "STRING",    body.body),
            bq.p("audience", "STRING",    body.audience),
            bq.p("author",   "STRING",    current_user.username),
            bq.p("now",      "TIMESTAMP", now),
        ],
    )
    bq.cache.invalidate("announcements:")  # bust list cache after write
    # Push to all users who have registered a device token
    role_clause = "AND role = @role" if body.audience != "Semua" else ""
    token_params = []
    if body.audience != "Semua":
        token_params.append(bq.p("role", "STRING", body.audience))
    token_rows = bq.query(
        f"""
        SELECT push_token FROM {SFA_WEB}.users
        WHERE is_active = TRUE AND push_token IS NOT NULL
          AND STARTS_WITH(push_token, 'ExponentPushToken[') {role_clause}
        """,
        token_params,
    )
    messages = [
        {"to": r["push_token"], "title": body.title, "body": body.body,
         "sound": "default", "data": {"type": "announcement", "id": new_id}}
        for r in token_rows
    ]
    if messages:
        send_push_bulk(messages)

    return {"announcement_id": new_id, "message": "Pengumuman berhasil dibuat."}
