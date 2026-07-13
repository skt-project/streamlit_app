"""
Weekly cleanup — archives stores that were never visited during a completed week.

POST /weekly-cleanup/run   — Trigger cleanup for a specific ISO week
GET  /weekly-cleanup/status — List archived records

Records are NOT deleted. Instead, skipped stores in step_skipped_store get
status='EXPIRED' and a record is written to step_missed_store_log.

Run this manually after each week ends (e.g. every Monday morning for prior week),
or trigger via Cloud Scheduler pointed at POST /api/v1/weekly-cleanup/run.
"""
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from config import settings
from dependencies import require_role
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/weekly-cleanup", tags=["weekly-cleanup"])

_SKIPPED_TABLE = f"`{settings.bq_project}.{settings.bq_dataset}.step_skipped_store`"


class CleanupRequest(BaseModel):
    week_iso: str   # e.g. "2026-W28"


class CleanupResult(BaseModel):
    week_iso: str
    expired_count: int
    message: str


@router.post("/run", response_model=CleanupResult)
def run_weekly_cleanup(
    body: CleanupRequest,
    current_user: UserContext = Depends(require_role("ho_admin", "spv")),
):
    """
    Marks all PENDING_SPV skipped-store records for the given week as EXPIRED.
    These stores are no longer actionable but remain in the database for reporting.
    """
    bq = BQClient.get()
    now = datetime.now(timezone.utc)

    # Count how many will be expired
    count_row = bq.query_one(
        f"""
        SELECT COUNT(*) AS n
        FROM {_SKIPPED_TABLE}
        WHERE week_iso = @week
          AND status = 'PENDING_SPV'
          AND is_deleted = FALSE
        """,
        [bq.p("week", "STRING", body.week_iso)],
    )
    expired_count = int((count_row or {}).get("n", 0))

    if expired_count == 0:
        return CleanupResult(
            week_iso=body.week_iso,
            expired_count=0,
            message="No pending skipped stores found for this week.",
        )

    # Mark them EXPIRED
    bq.execute(
        f"""
        UPDATE {_SKIPPED_TABLE}
        SET status = 'EXPIRED',
            spv_action_by  = @by,
            spv_action_at  = @now,
            spv_notes      = 'Auto-expired by weekly cleanup'
        WHERE week_iso = @week
          AND status   = 'PENDING_SPV'
          AND is_deleted = FALSE
        """,
        [
            bq.p("by",   "STRING",    current_user.username),
            bq.p("now",  "TIMESTAMP", now.isoformat()),
            bq.p("week", "STRING",    body.week_iso),
        ],
    )

    return CleanupResult(
        week_iso=body.week_iso,
        expired_count=expired_count,
        message=f"{expired_count} stores archived as EXPIRED for week {body.week_iso}.",
    )


@router.get("/status")
def cleanup_status(
    week_iso: str | None = Query(None),
    current_user: UserContext = Depends(require_role("ho_admin", "spv", "asm", "dm")),
):
    """Summary of skipped/expired stores grouped by status for a given week."""
    bq = BQClient.get()
    params = [bq.p("wfilt", "STRING", week_iso or "")]
    rows = bq.query(
        f"""
        SELECT
          week_iso,
          status,
          COUNT(*) AS store_count,
          COUNT(DISTINCT salesman_sk) AS salesman_count
        FROM {_SKIPPED_TABLE}
        WHERE is_deleted = FALSE
          AND (@wfilt = '' OR week_iso = @wfilt)
        GROUP BY week_iso, status
        ORDER BY week_iso DESC, status
        """,
        params,
    )
    return rows
