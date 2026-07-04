"""
GET /schedule/today   — today's stores for a salesman (online check)
GET /schedule/download — full week bundle for offline cache
"""
from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query

from config import settings
from dependencies import require_auth
from models.auth import UserContext
from models.schedule import ScheduleDownloadResponse, ScheduleStoreOut
from services.bq import BQClient

router = APIRouter(prefix="/schedule", tags=["schedule"])

_DAYS_ID = {1: "Senin", 2: "Selasa", 3: "Rabu", 4: "Kamis", 5: "Jumat", 6: "Sabtu"}

_STORE_COLS = """
    p.route_plan_sk,
    p.outlet_sk,
    p.source_outlet_code,
    o.store_name,
    o.address,
    o.latitude,
    o.longitude,
    o.brand,
    o.brand_group,
    o.store_grade,
    p.visit_day_of_week,
    p.visit_week_pattern,
    p.visit_frequency_code,
    p.distributor_code
"""


def _is_odd_week(d: date) -> bool:
    return (d.isocalendar()[1] % 2) == 1


def _week_stores(bq: BQClient, salesman_sk: str, target_date: date) -> list[dict]:
    """Return stores scheduled for the ISO week containing target_date."""
    day_id = _DAYS_ID.get(target_date.isoweekday(), "Senin")
    is_odd = _is_odd_week(target_date)

    rows = bq.query(
        f"""
        SELECT {_STORE_COLS}
        FROM {settings.table('fact_route_plan_pjp')} p
        LEFT JOIN {settings.table('vw_outlet_active')} o USING (outlet_sk)
        WHERE p.salesman_sk = @sk
          AND p.is_deleted = FALSE
          AND p.visit_day_of_week = @day
          AND (
            p.visit_week_pattern IS NULL OR p.visit_week_pattern = ''
            OR p.visit_frequency_code IN ('F4', 'F4+')
            OR (@is_odd = TRUE  AND p.visit_week_pattern = 'Minggu Ganjil')
            OR (@is_odd = FALSE AND p.visit_week_pattern = 'Minggu Genap')
          )
        ORDER BY o.store_name
        """,
        [
            bq.p("sk",     "STRING", salesman_sk),
            bq.p("day",    "STRING", day_id),
            bq.p("is_odd", "BOOL",   is_odd),
        ],
    )
    return rows


@router.get("/today", response_model=ScheduleDownloadResponse)
def get_today_schedule(
    salesman_sk: str | None = Query(None),
    current_user: UserContext = Depends(require_auth),
):
    # Prefer JWT salesman_sk so mobile never needs to pass it explicitly
    sk = current_user.salesman_sk or salesman_sk or ""
    if not sk:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="salesman_sk required — link this user to a salesman in Administration")
    bq = BQClient.get()
    today = date.today()
    rows = _week_stores(bq, sk, today)
    iso_year, iso_week, _ = today.isocalendar()
    return ScheduleDownloadResponse(
        salesman_sk=sk,
        week=f"{iso_year}-W{iso_week:02d}",
        stores=[ScheduleStoreOut(**r) for r in rows],
        total=len(rows),
    )


@router.get("/download", response_model=ScheduleDownloadResponse)
def download_week_schedule(
    salesman_sk: str | None = Query(None),
    week: str | None = Query(None, description="ISO week YYYY-Www e.g. 2026-W28"),
    current_user: UserContext = Depends(require_auth),
):
    """
    Returns all stores for the entire specified ISO week — used for offline caching.
    Includes ALL days so SE can plan ahead. Stores include lat/lon for GPS.
    """
    sk = current_user.salesman_sk or salesman_sk or ""
    if not sk:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="salesman_sk required — link this user to a salesman in Administration")
    bq = BQClient.get()

    if week:
        year, wnum = int(week[:4]), int(week[6:])
        monday = date.fromisocalendar(year, wnum, 1)
    else:
        today = date.today()
        monday = today - timedelta(days=today.isoweekday() - 1)

    iso_year, iso_week, _ = monday.isocalendar()
    is_odd = _is_odd_week(monday)

    rows = bq.query(
        f"""
        SELECT {_STORE_COLS}
        FROM {settings.table('fact_route_plan_pjp')} p
        LEFT JOIN {settings.table('vw_outlet_active')} o USING (outlet_sk)
        WHERE p.salesman_sk = @sk
          AND p.is_deleted = FALSE
          AND (
            p.visit_week_pattern IS NULL OR p.visit_week_pattern = ''
            OR p.visit_frequency_code IN ('F4', 'F4+')
            OR (@is_odd = TRUE  AND p.visit_week_pattern = 'Minggu Ganjil')
            OR (@is_odd = FALSE AND p.visit_week_pattern = 'Minggu Genap')
          )
        ORDER BY p.visit_day_of_week, o.store_name
        """,
        [
            bq.p("sk",     "STRING", sk),
            bq.p("is_odd", "BOOL",   is_odd),
        ],
    )

    return ScheduleDownloadResponse(
        salesman_sk=sk,
        week=f"{iso_year}-W{iso_week:02d}",
        stores=[ScheduleStoreOut(**r) for r in rows],
        total=len(rows),
    )
