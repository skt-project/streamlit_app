"""
GET  /notifications                   — list for current user
POST /notifications/{id}/read         — mark one read
POST /notifications/mark-all-read     — mark all read
"""
from fastapi import APIRouter, Depends

from config import settings
from dependencies import require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/notifications", tags=["notifications"])

SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"


@router.get("")
def list_notifications(current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
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
    return [
        {**r, "created_at": str(r["created_at"])} for r in rows
    ]


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
    return {"message": "All notifications marked as read."}
