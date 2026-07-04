"""
GET  /approvals                  — list approval requests
POST /approvals/{id}/approve     — approve
POST /approvals/{id}/reject      — reject (comment required)
"""
import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from config import settings
from dependencies import require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/approvals", tags=["approvals"])

SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"


class DecisionBody(BaseModel):
    comment: str = ""


@router.get("")
def list_approvals(
    status: str = Query("pending"),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    if status == "pending":
        status_clause = "AND ar.status = 'pending'"
    else:
        status_clause = "AND ar.status IN ('approved','rejected','revision')"

    rows = bq.query(
        f"""
        SELECT
          ar.approval_id,
          ar.type,
          ar.title,
          ar.submitted_by,
          ar.submitted_at,
          ar.current_value,
          ar.proposed_value,
          ar.reason,
          ar.status,
          ar.comments_json
        FROM {SFA_WEB}.approval_request ar
        WHERE ar.is_deleted = FALSE {status_clause}
        ORDER BY ar.submitted_at DESC
        LIMIT 100
        """,
        [],
    )

    result = []
    for r in rows:
        comments = []
        if r.get("comments_json"):
            try:
                comments = json.loads(r["comments_json"])
            except Exception:
                comments = []
        result.append({
            "approval_id":    r["approval_id"],
            "type":           r["type"],
            "title":          r["title"],
            "submitted_by":   r["submitted_by"],
            "submitted_at":   str(r["submitted_at"]),
            "current_value":  r.get("current_value"),
            "proposed_value": r["proposed_value"],
            "reason":         r["reason"],
            "status":         r["status"],
            "sla_hours":      48,
            "comments":       comments,
        })
    return result


def _update_approval(approval_id: str, decision: str, comment: str, user: UserContext):
    bq = BQClient.get()
    row = bq.query_one(
        f"SELECT status, comments_json FROM {SFA_WEB}.approval_request WHERE approval_id = @id AND is_deleted = FALSE",
        [bq.p("id", "STRING", approval_id)],
    )
    if not row:
        raise HTTPException(status_code=404, detail="Approval request not found")
    if row["status"] != "pending":
        raise HTTPException(status_code=400, detail="Request is no longer pending")

    comments = []
    if row.get("comments_json"):
        try:
            comments = json.loads(row["comments_json"])
        except Exception:
            pass
    if comment:
        comments.append({
            "author":     user.username,
            "role":       user.role,
            "body":       comment,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

    new_status  = "approved" if decision == "approve" else "rejected"
    now         = datetime.now(timezone.utc).isoformat()
    bq.execute(
        f"""
        UPDATE {SFA_WEB}.approval_request
        SET status = @status, decided_by = @decider, decided_at = @now, comments_json = @cjson
        WHERE approval_id = @id
        """,
        [
            bq.p("status",  "STRING",    new_status),
            bq.p("decider", "STRING",    user.username),
            bq.p("now",     "TIMESTAMP", now),
            bq.p("cjson",   "STRING",    json.dumps(comments)),
            bq.p("id",      "STRING",    approval_id),
        ],
    )
    return {"message": f"Request {new_status}."}


@router.post("/{approval_id}/approve")
def approve(approval_id: str, body: DecisionBody, current_user: UserContext = Depends(require_auth)):
    if current_user.role not in ("asm", "dm", "ho_admin"):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return _update_approval(approval_id, "approve", body.comment, current_user)


@router.post("/{approval_id}/reject")
def reject(approval_id: str, body: DecisionBody, current_user: UserContext = Depends(require_auth)):
    if current_user.role not in ("asm", "dm", "ho_admin"):
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    if not body.comment:
        raise HTTPException(status_code=400, detail="Comment required for rejection")
    return _update_approval(approval_id, "reject", body.comment, current_user)
