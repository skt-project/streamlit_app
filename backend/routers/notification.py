"""
GET  /notifications                      — list for current user
POST /notifications/{id}/read            — mark one read
POST /notifications/mark-all-read        — mark all read
POST /notifications/register-push-token  — store Expo push token for this user
"""
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from config import settings
from dependencies import require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/notifications", tags=["notifications"])

SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"


def _notif_cache_key(user_id: str) -> str:
    return f"notif:{user_id}"


@router.get("")
def list_notifications(current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
    cache_key = _notif_cache_key(current_user.user_id)
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached
    rows = bq.query(
        f"""
        SELECT notification_id, type, title, body, is_read, deep_link, created_at
        FROM {SFA_WEB}.notification
        WHERE user_id = @uid AND is_deleted = FALSE
        ORDER BY created_at DESC
        LIMIT 100
        """,
        [bq.p("uid", "STRING", current_user.user_id)],
    )
    result = [{**r, "created_at": str(r["created_at"])} for r in rows]
    bq.cache.set(cache_key, result, ttl=60)  # 60s — matches frontend staleTime
    return result


@router.post("/{notification_id}/read")
def mark_read(notification_id: str, current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
    bq.execute(
        f"""
        UPDATE {SFA_WEB}.notification
        SET is_read = TRUE
        WHERE notification_id = @nid AND user_id = @uid
        """,
        [bq.p("nid", "STRING", notification_id), bq.p("uid", "STRING", current_user.user_id)],
    )
    bq.cache.invalidate(_notif_cache_key(current_user.user_id))
    return {"message": "Marked as read."}


@router.post("/mark-all-read")
def mark_all_read(current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
    bq.execute(
        f"""
        UPDATE {SFA_WEB}.notification
        SET is_read = TRUE
        WHERE user_id = @uid AND is_read = FALSE AND is_deleted = FALSE
        """,
        [bq.p("uid", "STRING", current_user.user_id)],
    )
    bq.cache.invalidate(_notif_cache_key(current_user.user_id))
    return {"message": "All notifications marked as read."}


class PushTokenRequest(BaseModel):
    push_token: str


@router.post("/register-push-token")
def register_push_token(
    body: PushTokenRequest,
    current_user: UserContext = Depends(require_auth),
):
    """Store Expo push token so the server can send push notifications to this device."""
    bq = BQClient.get()
    bq.execute(
        f"""
        UPDATE {SFA_WEB}.users
        SET push_token = @token
        WHERE user_id = @uid
        """,
        [
            bq.p("token", "STRING", body.push_token),
            bq.p("uid", "STRING", current_user.user_id),
        ],
    )
    return {"message": "Push token registered."}
