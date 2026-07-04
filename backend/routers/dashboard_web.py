"""
GET /dashboard/web  — Web dashboard KPIs, comply, leaderboard, announcements
"""
from datetime import date, timedelta

from fastapi import APIRouter, Depends

from config import settings
from dependencies import brand_group_filter, require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/dashboard", tags=["dashboard-web"])

SFA_STEP = f"`{settings.bq_project}.sfa_step`"
SFA_WEB  = f"`{settings.bq_project}.{settings.bq_dataset}`"


@router.get("/web")
def get_web_dashboard(current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
    today = date.today().isoformat()
    month_start = date.today().replace(day=1).isoformat()
    bg_clause, bg_params = brand_group_filter(current_user, "bg", "v")

    # ── Comply summary ──────────────────────────────────────────────────────────
    comply_rows = bq.query(
        f"""
        SELECT
          brand,
          SUM(management_target) AS management_target,
          SUM(spv_target)        AS spv_target,
          SAFE_DIVIDE(SUM(spv_target), NULLIF(SUM(management_target), 0)) * 100 AS comply_pct
        FROM {SFA_WEB}.spv_target
        WHERE period_month = DATE_TRUNC(CURRENT_DATE(), MONTH)
          AND approval_status IN ('submitted','approved')
        GROUP BY brand
        ORDER BY brand
        """,
        [],
    )

    total_mgmt  = sum(r.get("management_target", 0) or 0 for r in comply_rows)
    total_spv   = sum(r.get("spv_target", 0) or 0 for r in comply_rows)
    comply_pct  = round((total_spv / total_mgmt * 100) if total_mgmt > 0 else 0.0, 1)

    # ── Route compliance (MTD) ──────────────────────────────────────────────────
    rc_row = bq.query_one(
        f"""
        SELECT
          COUNTIF(is_visited) AS visited,
          COUNT(*)            AS planned
        FROM {SFA_STEP}.vw_route_compliance
        WHERE visit_date BETWEEN @ms AND @today
        """,
        [bq.p("ms", "DATE", month_start), bq.p("today", "DATE", today)],
    ) or {}
    visited = rc_row.get("visited", 0) or 0
    planned = rc_row.get("planned", 0) or 0
    rc_pct  = round((visited / planned * 100) if planned > 0 else 0.0, 1)

    # ── Achievement leaderboard (top 10 by visit MTD) ──────────────────────────
    leaderboard = bq.query(
        f"""
        SELECT
          v.salesman_sk,
          sm.salesman_name,
          COUNT(*) AS visit_mtd,
          COUNTIF(v.effective_call = 'YES') AS ec_mtd,
          SAFE_DIVIDE(COUNTIF(v.effective_call='YES'), NULLIF(COUNT(*),0))*100 AS ec_rate
        FROM {settings.table('fact_visit')} v
        JOIN {SFA_STEP}.dim_salesman sm USING (salesman_sk)
        WHERE v.visit_date BETWEEN @ms AND @today AND v.is_deleted = FALSE
          {bg_clause}
        GROUP BY v.salesman_sk, sm.salesman_name
        ORDER BY visit_mtd DESC
        LIMIT 10
        """,
        [bq.p("ms", "DATE", month_start), bq.p("today", "DATE", today)] + bg_params,
    )

    # ── Recent announcements ────────────────────────────────────────────────────
    announcements = bq.query(
        f"""
        SELECT announcement_id, type, title, body, audience, created_at
        FROM {SFA_WEB}.announcement
        WHERE is_deleted = FALSE
        ORDER BY created_at DESC
        LIMIT 5
        """,
        [],
    )

    # ── Today's visit KPIs ──────────────────────────────────────────────────────
    today_row = bq.query_one(
        f"""
        SELECT
          COUNT(*) AS total_visits,
          COUNTIF(effective_call = 'YES') AS ec_today
        FROM {settings.table('fact_visit')} v
        WHERE v.visit_date = @today AND v.is_deleted = FALSE
          {bg_clause}
        """,
        [bq.p("today", "DATE", today)] + bg_params,
    ) or {}

    return {
        "comply_pct":        comply_pct,
        "comply_brands":     comply_rows,
        "route_comply_pct":  rc_pct,
        "visit_today":       int(today_row.get("total_visits", 0) or 0),
        "ec_today":          int(today_row.get("ec_today", 0) or 0),
        "leaderboard":       leaderboard,
        "announcements":     [
            {**a, "created_at": str(a["created_at"])} for a in announcements
        ],
        "month_start":       month_start,
        "today":             today,
    }
