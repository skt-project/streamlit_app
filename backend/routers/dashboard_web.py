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

SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"


@router.get("/web")
def get_web_dashboard(
    current_user: UserContext = Depends(require_auth),
    date_from: str | None = None,
    date_to: str | None = None,
    brand: str | None = None,
    region: str | None = None,
):
    bq = BQClient.get()
    today = (date_to or date.today().isoformat())
    month_start = (date_from or date.today().replace(day=1).isoformat())
    bg_clause, bg_params = brand_group_filter(current_user, "bg", "v")

    # Additional optional filters (brand overrides bg_clause, region filters dim_salesman join)
    extra_visit_clause = ""
    extra_params: list = []
    if brand and not bg_clause:
        extra_visit_clause += " AND sm.brand_group = @filter_brand"
        extra_params.append(bq.p("filter_brand", "STRING", brand))
    if region:
        extra_visit_clause += " AND sm.region = @filter_region"
        extra_params.append(bq.p("filter_region", "STRING", region))

    # ── Comply summary — cached 2 min (changes only when target rows are updated) ─
    comply_cache_key = f"dashboard:comply:{current_user.brand_group or 'all'}"
    comply_rows = bq.query_cached(
        comply_cache_key,
        f"""
        SELECT
          brand,
          SUM(t.management_target) AS management_target,
          SUM(t.spv_target)        AS spv_target,
          SAFE_DIVIDE(SUM(t.spv_target), NULLIF(SUM(t.management_target), 0)) * 100 AS comply_pct
        FROM {SFA_WEB}.spv_target t
        WHERE t.period_month = DATE_TRUNC(CURRENT_DATE(), MONTH)
          AND t.approval_status IN ('submitted','approved')
        GROUP BY t.brand
        ORDER BY t.brand
        """,
        [],
        ttl=120,
    )

    total_mgmt  = sum(r.get("management_target", 0) or 0 for r in comply_rows)
    total_spv   = sum(r.get("spv_target", 0) or 0 for r in comply_rows)
    comply_pct  = round((total_spv / total_mgmt * 100) if total_mgmt > 0 else 0.0, 1)

    # ── Route compliance (MTD): visits with matching route plan entries ──────────
    rc_cache_key = f"dashboard:rc:{month_start}:{today}"
    rc_row = bq.query_one_cached(
        rc_cache_key,
        f"""
        SELECT
          COUNT(DISTINCT CONCAT(v.salesman_sk, v.outlet_sk)) AS visited,
          COUNT(DISTINCT CONCAT(r.salesman_sk, r.outlet_sk)) AS planned
        FROM {SFA_WEB}.fact_route_plan_pjp r
        LEFT JOIN {settings.table('fact_visit')} v
          ON r.salesman_sk = v.salesman_sk
         AND r.outlet_sk   = v.outlet_sk
         AND v.visit_date BETWEEN @ms AND @today
         AND v.is_deleted  = FALSE
        WHERE r.is_deleted = FALSE
        """,
        [bq.p("ms", "DATE", month_start), bq.p("today", "DATE", today)],
        ttl=120,
    ) or {}
    visited = rc_row.get("visited", 0) or 0
    planned = rc_row.get("planned", 0) or 0
    rc_pct  = round((visited / planned * 100) if planned > 0 else 0.0, 1)

    # ── Achievement leaderboard — cached 2 min (aggregate, not real-time critical) ─
    lb_cache_key = f"dashboard:leaderboard:{current_user.brand_group or 'all'}:{today}:{region or ''}"
    leaderboard = bq.query_cached(
        lb_cache_key,
        f"""
        SELECT
          v.salesman_sk,
          sm.salesman_name,
          sm.region,
          COUNT(*) AS visit_mtd,
          COUNTIF(v.effective_call = 'YES') AS ec_mtd,
          SAFE_DIVIDE(COUNTIF(v.effective_call='YES'), NULLIF(COUNT(*),0))*100 AS ec_rate
        FROM {settings.table('fact_visit')} v
        JOIN {SFA_WEB}.dim_salesman sm USING (salesman_sk)
        WHERE v.visit_date BETWEEN @ms AND @today AND v.is_deleted = FALSE
          {bg_clause} {extra_visit_clause}
        GROUP BY v.salesman_sk, sm.salesman_name, sm.region
        ORDER BY visit_mtd DESC
        LIMIT 10
        """,
        [bq.p("ms", "DATE", month_start), bq.p("today", "DATE", today)] + bg_params + extra_params,
        ttl=120,
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
        JOIN {SFA_WEB}.dim_salesman sm USING (salesman_sk)
        WHERE v.visit_date = @today AND v.is_deleted = FALSE
          {bg_clause} {extra_visit_clause}
        """,
        [bq.p("today", "DATE", today)] + bg_params + extra_params,
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
