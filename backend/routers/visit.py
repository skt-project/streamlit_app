"""
Visit module — complete field-visit lifecycle.

POST /visit/checkin
POST /visit/{id}/checkout
POST /visit/{id}/submit
PUT  /visit/{id}/approve
PUT  /visit/{id}/reject
PUT  /visit/{id}/resubmit
GET  /visit
GET  /visit/{id}

GPS distance is recorded but NEVER blocks any operation.
offline_mode=true skips all server-side blocking checks.
"""
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from config import settings
from dependencies import brand_group_filter, require_auth
from models.auth import UserContext
from models.visit import (
    ApproveRequest, CheckinRequest, CheckinResponse,
    CheckoutRequest, RejectRequest, ResubmitRequest,
    SubmitRequest, VisitItemOut, VisitListResponse, VisitOut,
)
from services.bq import BQClient
from services.geo import distance_or_none

router = APIRouter(prefix="/visit", tags=["visit"])

GPS_WARN_THRESHOLD_M = 200.0  # informational only — not a blocker

_VISIT_COLS = """
    visit_id, salesman_sk, outlet_sk, schedule_id,
    visit_date, visit_type, brand_group,
    checkin_time, checkin_latitude, checkin_longitude,
    checkin_photo_url, checkin_distance_m,
    checkout_time, checkout_latitude, checkout_longitude,
    checkout_photo_url,
    total_demand, effective_call, notes, duration_minutes,
    visit_status, approval_status,
    spv_username, spv_approved_at,
    asm_username, asm_approved_at,
    ddm_username, ddm_approved_at,
    rejection_notes, revision_count,
    created_at, updated_at
"""


def _next_approval_status(current: str, role: str) -> str:
    """Return next approval_status when caller with `role` approves."""
    transitions = {
        ("SUBMITTED",    "spv"):         "SPV_APPROVED",
        ("PENDING_SPV",  "spv"):         "SPV_APPROVED",
        ("SPV_APPROVED", "asm"):         "ASM_APPROVED",
        ("ASM_APPROVED", "ddm"):         "DDM_APPROVED",
        ("DDM_APPROVED", "ho_admin"):    "COMPLETED",
    }
    # also handle lower-case roles stored as sfa_role
    role_map = {"se": "se", "spv": "spv", "asm": "asm", "ddm": "ddm", "ho_admin": "ho_admin"}
    r = role_map.get(role, role)
    key = (current, r)
    if key not in transitions:
        raise HTTPException(status_code=403, detail=f"Role '{role}' cannot approve visits in status '{current}'")
    return transitions[key]


def _row_to_visit(row: dict, items: list[dict] | None = None) -> VisitOut:
    gps_warn = False
    dist = row.get("checkin_distance_m")
    if dist is not None and dist > GPS_WARN_THRESHOLD_M:
        gps_warn = True
    return VisitOut(
        **{k: row.get(k) for k in VisitOut.model_fields if k not in ("items", "gps_warning")},
        gps_warning=gps_warn,
        items=[VisitItemOut(**i) for i in (items or [])],
    )


# ------------------------------------------------------------------
# POST /visit/checkin
# ------------------------------------------------------------------
@router.post("/checkin", response_model=CheckinResponse, status_code=201)
def checkin(body: CheckinRequest, current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
    now = datetime.now(timezone.utc)
    captured = body.captured_at or now

    # Idempotency — same schedule already checked in → return existing
    if body.schedule_id:
        existing = bq.query_one(
            f"SELECT visit_id, checkin_distance_m FROM {settings.table('fact_visit')} "
            "WHERE schedule_id = @sid AND is_deleted = FALSE",
            [bq.p("sid", "STRING", body.schedule_id)],
        )
        if existing:
            dist = existing.get("checkin_distance_m")
            return CheckinResponse(
                visit_id=existing["visit_id"],
                checkin_distance_m=dist,
                gps_warning=(dist or 0) > GPS_WARN_THRESHOLD_M,
                offline_mode=body.offline_mode,
            )

    # Fetch outlet GPS for distance calculation
    outlet_lat, outlet_lon = None, None
    if body.outlet_sk:
        ol = bq.query_one(
            f"SELECT latitude, longitude FROM {settings.table('dim_outlet')} WHERE outlet_sk = @sk",
            [bq.p("sk", "STRING", body.outlet_sk)],
        )
        if ol:
            outlet_lat = ol.get("latitude")
            outlet_lon = ol.get("longitude")

    dist_m = distance_or_none(body.checkin_latitude, body.checkin_longitude, outlet_lat, outlet_lon)

    # Fetch salesman brand_group
    sm = bq.query_one(
        f"SELECT brand_group FROM {settings.table('dim_salesman')} WHERE salesman_sk = @sk",
        [bq.p("sk", "STRING", body.salesman_sk)],
    )
    brand_group = (sm or {}).get("brand_group") or current_user.brand_group

    visit_id = f"VST-{uuid.uuid4().hex[:16].upper()}"

    bq.execute(
        f"""
        INSERT INTO {settings.table('fact_visit')} (
          visit_id, salesman_sk, outlet_sk, schedule_id,
          visit_date, visit_type, brand_group,
          checkin_time, checkin_latitude, checkin_longitude,
          checkin_photo_url, checkin_distance_m,
          visit_status, approval_status,
          revision_count, created_at, updated_at, is_deleted
        ) VALUES (
          @vid, @sm_sk, @out_sk, @sched_id,
          @vdate, @vtype, @bg,
          @cin_time, @cin_lat, @cin_lon,
          @cin_photo, @dist_m,
          'CHECKED_IN', 'DRAFT',
          0, @now, @now, FALSE
        )
        """,
        [
            bq.p("vid",      "STRING",    visit_id),
            bq.p("sm_sk",    "STRING",    body.salesman_sk),
            bq.p("out_sk",   "STRING",    body.outlet_sk),
            bq.p("sched_id", "STRING",    body.schedule_id),
            bq.p("vdate",    "DATE",      body.visit_date.isoformat()),
            bq.p("vtype",    "STRING",    body.visit_type),
            bq.p("bg",       "STRING",    brand_group),
            bq.p("cin_time", "TIMESTAMP", captured.isoformat()),
            bq.p("cin_lat",  "FLOAT64",   body.checkin_latitude),
            bq.p("cin_lon",  "FLOAT64",   body.checkin_longitude),
            bq.p("cin_photo","STRING",    body.checkin_photo_url),
            bq.p("dist_m",   "FLOAT64",   dist_m),
            bq.p("now",      "TIMESTAMP", now.isoformat()),
        ],
    )

    return CheckinResponse(
        visit_id=visit_id,
        checkin_distance_m=dist_m,
        gps_warning=(dist_m or 0) > GPS_WARN_THRESHOLD_M,
        offline_mode=body.offline_mode,
    )


# ------------------------------------------------------------------
# POST /visit/{visit_id}/checkout
# ------------------------------------------------------------------
@router.post("/{visit_id}/checkout", response_model=VisitOut)
def checkout(
    visit_id: str,
    body: CheckoutRequest,
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    now = datetime.now(timezone.utc)
    captured = body.captured_at or now

    visit = bq.query_one(
        f"SELECT * FROM {settings.table('fact_visit')} WHERE visit_id = @vid AND is_deleted = FALSE",
        [bq.p("vid", "STRING", visit_id)],
    )
    if not visit:
        raise HTTPException(status_code=404, detail="Visit not found")

    # Duration
    cin_time = visit.get("checkin_time")
    duration = None
    if cin_time:
        if isinstance(cin_time, str):
            cin_time = datetime.fromisoformat(cin_time)
        duration = max(0, int((captured - cin_time.replace(tzinfo=timezone.utc)).total_seconds() / 60))

    # Insert visit items
    for item in body.items:
        if item.qty > 0:
            item_id = f"VTI-{uuid.uuid4().hex[:16].upper()}"
            demand = round(item.qty * item.stp, 2)
            bq.execute(
                f"""
                INSERT INTO {settings.table('fact_visit_item')} (
                  visit_item_id, visit_id, sku_id, sku_name, brand,
                  brand_group, category, stp, qty, demand, created_at
                ) VALUES (
                  @iid, @vid, @sku_id, @sku_name, @brand,
                  @bg, @cat, @stp, @qty, @demand, @now
                )
                """,
                [
                    bq.p("iid",      "STRING",    item_id),
                    bq.p("vid",      "STRING",    visit_id),
                    bq.p("sku_id",   "STRING",    item.sku_id),
                    bq.p("sku_name", "STRING",    item.sku_name),
                    bq.p("brand",    "STRING",    item.brand),
                    bq.p("bg",       "STRING",    item.brand_group),
                    bq.p("cat",      "STRING",    item.category),
                    bq.p("stp",      "FLOAT64",   item.stp),
                    bq.p("qty",      "INT64",     item.qty),
                    bq.p("demand",   "FLOAT64",   demand),
                    bq.p("now",      "TIMESTAMP", now.isoformat()),
                ],
            )

    bq.execute(
        f"""
        UPDATE {settings.table('fact_visit')} SET
          checkout_time = @cout_time,
          checkout_latitude = @cout_lat, checkout_longitude = @cout_lon,
          checkout_photo_url = @cout_photo,
          total_demand = @demand, effective_call = @ec,
          notes = @notes, duration_minutes = @dur,
          visit_status = 'CHECKED_OUT',
          updated_at = @now
        WHERE visit_id = @vid
        """,
        [
            bq.p("cout_time",  "TIMESTAMP", captured.isoformat()),
            bq.p("cout_lat",   "FLOAT64",   body.checkout_latitude),
            bq.p("cout_lon",   "FLOAT64",   body.checkout_longitude),
            bq.p("cout_photo", "STRING",    body.checkout_photo_url),
            bq.p("demand",     "FLOAT64",   body.total_demand),
            bq.p("ec",         "STRING",    body.effective_call),
            bq.p("notes",      "STRING",    body.notes),
            bq.p("dur",        "INT64",     duration),
            bq.p("now",        "TIMESTAMP", now.isoformat()),
            bq.p("vid",        "STRING",    visit_id),
        ],
    )
    return _get_visit_detail(visit_id, bq)


# ------------------------------------------------------------------
# POST /visit/{visit_id}/submit
# ------------------------------------------------------------------
@router.post("/{visit_id}/submit", response_model=VisitOut)
def submit_visit(
    visit_id: str,
    body: SubmitRequest,
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    now = datetime.now(timezone.utc)

    visit = bq.query_one(
        f"SELECT visit_id, visit_status FROM {settings.table('fact_visit')} WHERE visit_id = @vid AND is_deleted = FALSE",
        [bq.p("vid", "STRING", visit_id)],
    )
    if not visit:
        raise HTTPException(status_code=404, detail="Visit not found")

    bq.execute(
        f"""
        UPDATE {settings.table('fact_visit')} SET
          visit_status = 'SUBMITTED',
          approval_status = 'PENDING_SPV',
          updated_at = @now
        WHERE visit_id = @vid
        """,
        [bq.p("now", "TIMESTAMP", now.isoformat()), bq.p("vid", "STRING", visit_id)],
    )
    return _get_visit_detail(visit_id, bq)


# ------------------------------------------------------------------
# PUT /visit/{visit_id}/approve
# ------------------------------------------------------------------
@router.put("/{visit_id}/approve", response_model=VisitOut)
def approve_visit(
    visit_id: str,
    body: ApproveRequest,
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    now = datetime.now(timezone.utc)

    # Use sfa_role if present, fall back to role
    effective_role = current_user.role
    visit = bq.query_one(
        f"SELECT approval_status FROM {settings.table('fact_visit')} WHERE visit_id = @vid AND is_deleted = FALSE",
        [bq.p("vid", "STRING", visit_id)],
    )
    if not visit:
        raise HTTPException(status_code=404, detail="Visit not found")

    new_status = _next_approval_status(visit["approval_status"], effective_role)

    role_col_map = {
        "spv": ("spv_username", "spv_approved_at"),
        "asm": ("asm_username", "asm_approved_at"),
        "ddm": ("ddm_username", "ddm_approved_at"),
        "ho_admin": ("ddm_username", "ddm_approved_at"),
    }
    user_col, ts_col = role_col_map.get(effective_role, ("spv_username", "spv_approved_at"))

    bq.execute(
        f"""
        UPDATE {settings.table('fact_visit')} SET
          approval_status = @new_status,
          {user_col} = @approver,
          {ts_col} = @now,
          updated_at = @now
        WHERE visit_id = @vid
        """,
        [
            bq.p("new_status", "STRING",    new_status),
            bq.p("approver",   "STRING",    current_user.username),
            bq.p("now",        "TIMESTAMP", now.isoformat()),
            bq.p("vid",        "STRING",    visit_id),
        ],
    )
    return _get_visit_detail(visit_id, bq)


# ------------------------------------------------------------------
# PUT /visit/{visit_id}/reject
# ------------------------------------------------------------------
@router.put("/{visit_id}/reject", response_model=VisitOut)
def reject_visit(
    visit_id: str,
    body: RejectRequest,
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    now = datetime.now(timezone.utc)

    visit = bq.query_one(
        f"SELECT visit_id, revision_count FROM {settings.table('fact_visit')} WHERE visit_id = @vid AND is_deleted = FALSE",
        [bq.p("vid", "STRING", visit_id)],
    )
    if not visit:
        raise HTTPException(status_code=404, detail="Visit not found")

    rev_count = (visit.get("revision_count") or 0) + 1

    bq.execute(
        f"""
        UPDATE {settings.table('fact_visit')} SET
          approval_status = 'REVISION_REQUIRED',
          rejection_notes = @notes,
          revision_count = @rev,
          updated_at = @now
        WHERE visit_id = @vid
        """,
        [
            bq.p("notes", "STRING",    body.rejection_notes),
            bq.p("rev",   "INT64",     rev_count),
            bq.p("now",   "TIMESTAMP", now.isoformat()),
            bq.p("vid",   "STRING",    visit_id),
        ],
    )
    return _get_visit_detail(visit_id, bq)


# ------------------------------------------------------------------
# PUT /visit/{visit_id}/resubmit
# ------------------------------------------------------------------
@router.put("/{visit_id}/resubmit", response_model=VisitOut)
def resubmit_visit(
    visit_id: str,
    body: ResubmitRequest,
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    now = datetime.now(timezone.utc)

    visit = bq.query_one(
        f"SELECT visit_id FROM {settings.table('fact_visit')} WHERE visit_id = @vid AND is_deleted = FALSE",
        [bq.p("vid", "STRING", visit_id)],
    )
    if not visit:
        raise HTTPException(status_code=404, detail="Visit not found")

    # Delete old items and re-insert
    bq.execute(
        f"DELETE FROM {settings.table('fact_visit_item')} WHERE visit_id = @vid",
        [bq.p("vid", "STRING", visit_id)],
    )
    for item in body.items:
        if item.qty > 0:
            item_id = f"VTI-{uuid.uuid4().hex[:16].upper()}"
            bq.execute(
                f"""
                INSERT INTO {settings.table('fact_visit_item')} (
                  visit_item_id, visit_id, sku_id, sku_name, brand,
                  brand_group, category, stp, qty, demand, created_at
                ) VALUES (
                  @iid, @vid, @sku_id, @sku_name, @brand,
                  @bg, @cat, @stp, @qty, @demand, @now
                )
                """,
                [
                    bq.p("iid",      "STRING",    item_id),
                    bq.p("vid",      "STRING",    visit_id),
                    bq.p("sku_id",   "STRING",    item.sku_id),
                    bq.p("sku_name", "STRING",    item.sku_name),
                    bq.p("brand",    "STRING",    item.brand),
                    bq.p("bg",       "STRING",    item.brand_group),
                    bq.p("cat",      "STRING",    item.category),
                    bq.p("stp",      "FLOAT64",   item.stp),
                    bq.p("qty",      "INT64",     item.qty),
                    bq.p("demand",   "FLOAT64",   round(item.qty * item.stp, 2)),
                    bq.p("now",      "TIMESTAMP", now.isoformat()),
                ],
            )

    update_parts = ["total_demand = @demand", "approval_status = 'PENDING_SPV'",
                    "visit_status = 'SUBMITTED'", "updated_at = @now"]
    params = [bq.p("demand", "FLOAT64", body.total_demand), bq.p("now", "TIMESTAMP", now.isoformat()),
              bq.p("vid", "STRING", visit_id)]

    if body.notes is not None:
        update_parts.append("notes = @notes")
        params.append(bq.p("notes", "STRING", body.notes))
    if body.checkout_photo_url:
        update_parts.append("checkout_photo_url = @photo")
        params.append(bq.p("photo", "STRING", body.checkout_photo_url))

    bq.execute(
        f"UPDATE {settings.table('fact_visit')} SET {', '.join(update_parts)} WHERE visit_id = @vid",
        params,
    )
    return _get_visit_detail(visit_id, bq)


# ------------------------------------------------------------------
# GET /visit
# ------------------------------------------------------------------
@router.get("", response_model=VisitListResponse)
def list_visits(
    salesman_sk: str | None = Query(None),
    visit_date: str | None = Query(None),
    status: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    bg_clause, bg_params = brand_group_filter(current_user)
    conditions = [f"TRUE {bg_clause}"]
    params = list(bg_params)

    # Role scoping
    if current_user.role == "se" or current_user.role == "SE":
        conditions.append("AND salesman_sk = @self_sk")
        params.append(bq.p("self_sk", "STRING", current_user.user_id))
    elif salesman_sk:
        conditions.append("AND salesman_sk = @sm_sk")
        params.append(bq.p("sm_sk", "STRING", salesman_sk))

    if visit_date:
        conditions.append("AND visit_date = @vdate")
        params.append(bq.p("vdate", "DATE", visit_date))
    if status:
        conditions.append("AND approval_status = @status")
        params.append(bq.p("status", "STRING", status))

    where = " ".join(conditions)
    offset = (page - 1) * page_size

    total = (bq.query_one(
        f"SELECT COUNT(*) AS n FROM {settings.table('fact_visit')} WHERE {where} AND is_deleted = FALSE",
        params,
    ) or {}).get("n", 0)

    rows = bq.query(
        f"""
        SELECT v.*, sm.salesman_name, o.store_name
        FROM (
            SELECT {_VISIT_COLS}
            FROM {settings.table('fact_visit')}
            WHERE {where} AND is_deleted = FALSE
            ORDER BY created_at DESC
            LIMIT @lim OFFSET @off
        ) v
        LEFT JOIN {settings.table('dim_salesman')} sm ON v.salesman_sk = sm.salesman_sk
        LEFT JOIN {settings.table('dim_outlet')} o ON v.outlet_sk = o.outlet_sk
        """,
        params + [bq.p("lim", "INT64", page_size), bq.p("off", "INT64", offset)],
    )

    return VisitListResponse(
        items=[_row_to_visit(r) for r in rows],
        total=total,
        page=page,
        page_size=page_size,
        has_next=(offset + page_size) < total,
    )


# ------------------------------------------------------------------
# GET /visit/{visit_id}
# ------------------------------------------------------------------
@router.get("/{visit_id}", response_model=VisitOut)
def get_visit(visit_id: str, current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
    visit = _get_visit_detail(visit_id, bq)
    if not visit:
        raise HTTPException(status_code=404, detail="Visit not found")
    return visit


def _get_visit_detail(visit_id: str, bq: BQClient) -> VisitOut:
    row = bq.query_one(
        f"SELECT {_VISIT_COLS} FROM {settings.table('fact_visit')} WHERE visit_id = @vid AND is_deleted = FALSE",
        [bq.p("vid", "STRING", visit_id)],
    )
    if not row:
        raise HTTPException(status_code=404, detail="Visit not found")
    items = bq.query(
        f"""
        SELECT visit_item_id, sku_id, sku_name, brand, category, stp, qty, demand
        FROM {settings.table('fact_visit_item')}
        WHERE visit_id = @vid
        ORDER BY sku_name
        """,
        [bq.p("vid", "STRING", visit_id)],
    )
    return _row_to_visit(row, items)
