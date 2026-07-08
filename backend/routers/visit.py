"""
Visit module — complete field-visit lifecycle.

POST /visit/checkin
POST /visit/{id}/checkout
POST /visit/{id}/submit
PUT  /visit/{id}/approve
PUT  /visit/{id}/reject
PUT  /visit/{id}/resubmit
PUT  /visit/{id}/final-qty       — SPV adjusts quantities
GET  /visit/{id}/pdf             — Generate & download offering letter PDF
GET  /visit
GET  /visit/{id}

GPS distance is recorded but NEVER blocks any operation.
offline_mode=true skips all server-side blocking checks.
"""
import io
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from config import settings
from dependencies import BRAND_GROUPS, brand_group_filter, require_auth, _UNRESTRICTED_GROUPS
from models.auth import UserContext
from models.visit import (
    ApproveRequest, CheckinRequest, CheckinResponse,
    CheckoutRequest, RejectRequest, ResubmitRequest,
    SubmitRequest, UpdateFinalQtyRequest, VisitItemOut, VisitListResponse, VisitOut,
)
from services.bq import BQClient
from services.geo import distance_or_none
from services.push import send_push

router = APIRouter(prefix="/visit", tags=["visit"])

GPS_WARN_THRESHOLD_M = 200.0  # informational only — not a blocker


def _notify_user(bq: BQClient, user_id: str, ntype: str, title: str, body: str, deep_link: str | None = None) -> None:
    """Insert an in-app notification row and fire a push if the user has a token."""
    now = datetime.now(timezone.utc)
    notif_id = f"NOTIF-{uuid.uuid4().hex[:16].upper()}"
    try:
        bq.execute(
            f"""
            INSERT INTO {settings.table('notification')} (
              notification_id, user_id, type, title, body,
              is_read, is_deleted, deep_link, created_at
            ) VALUES (
              @nid, @uid, @ntype, @title, @body,
              FALSE, FALSE, @dl, @now
            )
            """,
            [
                bq.p("nid",   "STRING",    notif_id),
                bq.p("uid",   "STRING",    user_id),
                bq.p("ntype", "STRING",    ntype),
                bq.p("title", "STRING",    title),
                bq.p("body",  "STRING",    body),
                bq.p("dl",    "STRING",    deep_link),
                bq.p("now",   "TIMESTAMP", now.isoformat()),
            ],
        )
    except Exception:
        pass  # notification failure must never block the main operation

    try:
        user_row = bq.query_one(
            f"SELECT push_token FROM {settings.table('users')} WHERE user_id = @uid",
            [bq.p("uid", "STRING", user_id)],
        )
        if user_row and user_row.get("push_token"):
            send_push(user_row["push_token"], title, body, data={"deep_link": deep_link or ""})
    except Exception:
        pass


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
    """Return next approval_status when caller with `role` approves.
    Flow: SE submits → PENDING_SPV → (SPV) SPV_APPROVED → (distributor_admin) COMPLETED
    """
    transitions = {
        ("SUBMITTED",    "spv"):               "SPV_APPROVED",
        ("PENDING_SPV",  "spv"):               "SPV_APPROVED",
        ("SPV_APPROVED", "distributor_admin"): "COMPLETED",
    }
    key = (current, role)
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

    # Guard: reject items whose brand falls outside the user's business group.
    # ho_admin and accounts with no brand_group are unrestricted.
    # Items are NOT inserted here — they are only stored in BigQuery after Submit to SPV.
    if current_user.brand_group and current_user.role != "ho_admin":
        allowed = set(BRAND_GROUPS.get(current_user.brand_group, []))
        for item in body.items:
            if item.qty > 0 and item.brand and item.brand not in allowed:
                raise HTTPException(
                    status_code=403,
                    detail=f"Brand '{item.brand}' tidak diizinkan untuk group Anda",
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
        f"SELECT visit_id, visit_status, salesman_sk FROM {settings.table('fact_visit')} WHERE visit_id = @vid AND is_deleted = FALSE",
        [bq.p("vid", "STRING", visit_id)],
    )
    if not visit:
        raise HTTPException(status_code=404, detail="Visit not found")

    # Idempotency: if already SUBMITTED (e.g. sync engine retry after client timeout) return immediately
    if visit.get("visit_status") == "SUBMITTED":
        return _get_visit_detail(visit_id, bq)

    # Brand group guard (defence-in-depth — checkout already validated)
    if current_user.brand_group and current_user.role != "ho_admin":
        allowed = set(BRAND_GROUPS.get(current_user.brand_group, []))
        for item in body.items:
            if item.qty > 0 and item.brand and item.brand not in allowed:
                raise HTTPException(
                    status_code=403,
                    detail=f"Brand '{item.brand}' tidak diizinkan untuk group Anda",
                )

    # Delete any partial inserts from a previous failed/timed-out attempt (status was not yet SUBMITTED)
    bq.execute(
        f"DELETE FROM {settings.table('fact_visit_item')} WHERE visit_id = @vid",
        [bq.p("vid", "STRING", visit_id)],
    )

    # Insert visit items — first and only write to BigQuery
    # final_qty defaults to qty (SPV can override later via PUT /final-qty)
    for item in body.items:
        if item.qty > 0:
            item_id = f"VTI-{uuid.uuid4().hex[:16].upper()}"
            demand = round(item.qty * item.stp, 2)
            bq.execute(
                f"""
                INSERT INTO {settings.table('fact_visit_item')} (
                  visit_item_id, visit_id, sku_id, sku_name, brand,
                  brand_group, category, sku_size, stp, qty, final_qty, demand, created_at
                ) VALUES (
                  @iid, @vid, @sku_id, @sku_name, @brand,
                  @bg, @cat, @sku_size, @stp, @qty, @qty, @demand, @now
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
                    bq.p("sku_size", "STRING",    getattr(item, "sku_size", None)),
                    bq.p("stp",      "FLOAT64",   item.stp),
                    bq.p("qty",      "INT64",     item.qty),
                    bq.p("demand",   "FLOAT64",   demand),
                    bq.p("now",      "TIMESTAMP", now.isoformat()),
                ],
            )

    # Recalculate total_demand from submitted items (source of truth)
    submitted_demand = sum(round(i.qty * i.stp, 2) for i in body.items if i.qty > 0)
    submitted_ec = "YES" if any(i.qty > 0 for i in body.items) else "NO"

    bq.execute(
        f"""
        UPDATE {settings.table('fact_visit')} SET
          visit_status = 'SUBMITTED',
          approval_status = 'PENDING_SPV',
          total_demand = @demand,
          effective_call = @ec,
          updated_at = @now
        WHERE visit_id = @vid
        """,
        [
            bq.p("demand", "FLOAT64",   submitted_demand),
            bq.p("ec",     "STRING",    submitted_ec),
            bq.p("now",    "TIMESTAMP", now.isoformat()),
            bq.p("vid",    "STRING",    visit_id),
        ],
    )

    # Notify all SPVs of the new pending submission
    try:
        spv_users = bq.query(
            f"SELECT user_id FROM {settings.table('users')} WHERE role = 'spv' AND is_active = TRUE",
            [],
        )
        for spv in spv_users:
            _notify_user(
                bq, spv["user_id"],
                "VISIT_SUBMITTED",
                "Kunjungan Baru Perlu Disetujui",
                f"Kunjungan {visit_id} menunggu persetujuan Anda.",
                deep_link=f"visits/{visit_id}",
            )
    except Exception:
        pass  # notification failure must not block submit

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
        f"SELECT approval_status, salesman_sk FROM {settings.table('fact_visit')} WHERE visit_id = @vid AND is_deleted = FALSE",
        [bq.p("vid", "STRING", visit_id)],
    )
    if not visit:
        raise HTTPException(status_code=404, detail="Visit not found")

    new_status = _next_approval_status(visit["approval_status"], effective_role)

    role_col_map = {
        "spv":               ("spv_username", "spv_approved_at"),
        "distributor_admin": ("ddm_username", "ddm_approved_at"),  # reuse ddm col for dist admin final step
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

    # Notify the salesman that their visit was approved
    if salesman_sk := visit.get("salesman_sk"):
        _notify_user(
            bq, salesman_sk,
            "VISIT_APPROVED",
            "Kunjungan Disetujui",
            f"Kunjungan {visit_id} telah disetujui oleh {current_user.username}.",
            deep_link=f"visits/{visit_id}",
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
        f"SELECT visit_id, revision_count, salesman_sk FROM {settings.table('fact_visit')} WHERE visit_id = @vid AND is_deleted = FALSE",
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

    # Notify the salesman that revision is required
    if salesman_sk := visit.get("salesman_sk"):
        _notify_user(
            bq, salesman_sk,
            "REVISION_REQUIRED",
            "Kunjungan Perlu Direvisi",
            f"Kunjungan {visit_id} perlu direvisi. Catatan: {body.rejection_notes}",
            deep_link=f"visits/{visit_id}/revision",
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

    # Delete old items and re-insert (final_qty resets to qty on resubmit)
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
                  brand_group, category, sku_size, stp, qty, final_qty, demand, created_at
                ) VALUES (
                  @iid, @vid, @sku_id, @sku_name, @brand,
                  @bg, @cat, @sku_size, @stp, @qty, @qty, @demand, @now
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
                    bq.p("sku_size", "STRING",    getattr(item, "sku_size", None)),
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
    store_name: str | None = Query(None, description="Partial store name search"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    bg_clause, bg_params = brand_group_filter(current_user)

    # Build visit-level conditions (applied to fact_visit before join)
    visit_conditions = [f"v.is_deleted = FALSE {bg_clause}"]
    params = list(bg_params)

    # Role scoping
    role = current_user.role
    if role in ("se", "SE"):
        visit_conditions.append("AND v.salesman_sk = @self_sk")
        params.append(bq.p("self_sk", "STRING", current_user.user_id))
    elif role == "distributor_admin":
        # Distributor admin sees only visits from their distributor's outlets
        if current_user.distributor_code:
            visit_conditions.append("AND o.distributor_code = @dist_code")
            params.append(bq.p("dist_code", "STRING", current_user.distributor_code))
        # Distributor admin sees visits that need their action or are already completed
        visit_conditions.append("AND v.approval_status IN ('SPV_APPROVED','COMPLETED')")
    elif salesman_sk:
        visit_conditions.append("AND v.salesman_sk = @sm_sk")
        params.append(bq.p("sm_sk", "STRING", salesman_sk))

    if visit_date:
        visit_conditions.append("AND v.visit_date = @vdate")
        params.append(bq.p("vdate", "DATE", visit_date))
    if status:
        visit_conditions.append("AND v.approval_status = @status")
        params.append(bq.p("status", "STRING", status))

    # Store name search — requires joining dim_outlet
    store_filter = ""
    if store_name:
        store_filter = "AND LOWER(o.store_name) LIKE @store_name"
        params.append(bq.p("store_name", "STRING", f"%{store_name.lower()}%"))

    visit_where = " ".join(visit_conditions)
    offset = (page - 1) * page_size

    # Use inline join so store_name filter and COUNT work together
    join_query = f"""
        FROM {settings.table('fact_visit')} v
        LEFT JOIN {settings.table('dim_salesman')} sm ON v.salesman_sk = sm.salesman_sk
        LEFT JOIN {settings.table('dim_outlet')} o   ON v.outlet_sk  = o.outlet_sk
        WHERE {visit_where} {store_filter}
    """

    total = (bq.query_one(
        f"SELECT COUNT(*) AS n {join_query}",
        params,
    ) or {}).get("n", 0)

    rows = bq.query(
        f"""
        SELECT v.{_VISIT_COLS}, sm.salesman_name, o.store_name, o.distributor_code
        {join_query}
        ORDER BY v.created_at DESC
        LIMIT @lim OFFSET @off
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


# ------------------------------------------------------------------
# PUT /visit/{visit_id}/final-qty  — SPV adjusts final quantities
# ------------------------------------------------------------------
@router.put("/{visit_id}/final-qty", response_model=VisitOut)
def update_final_qty(
    visit_id: str,
    body: UpdateFinalQtyRequest,
    current_user: UserContext = Depends(require_auth),
):
    if current_user.role not in ("spv", "asm", "ddm", "ho_admin"):
        raise HTTPException(status_code=403, detail="Only SPV and above can adjust final quantities")

    bq = BQClient.get()
    now = datetime.now(timezone.utc)

    visit = bq.query_one(
        f"SELECT visit_id, approval_status, total_demand FROM {settings.table('fact_visit')} WHERE visit_id = @vid AND is_deleted = FALSE",
        [bq.p("vid", "STRING", visit_id)],
    )
    if not visit:
        raise HTTPException(status_code=404, detail="Visit not found")

    # Update each item's final_qty
    for fi in body.items:
        bq.execute(
            f"""
            UPDATE {settings.table('fact_visit_item')}
            SET final_qty = @fqty
            WHERE visit_id = @vid AND sku_id = @sku_id
            """,
            [
                bq.p("fqty",   "INT64",  fi.final_qty),
                bq.p("vid",    "STRING", visit_id),
                bq.p("sku_id", "STRING", fi.sku_id),
            ],
        )

    # Recompute final_demand on the visit row for quick reference
    items_rows = bq.query(
        f"SELECT stp, COALESCE(final_qty, qty, 0) AS eff_qty FROM {settings.table('fact_visit_item')} WHERE visit_id = @vid",
        [bq.p("vid", "STRING", visit_id)],
    )
    final_demand = sum(round((r.get("eff_qty") or 0) * (r.get("stp") or 0), 2) for r in items_rows)

    bq.execute(
        f"UPDATE {settings.table('fact_visit')} SET updated_at = @now WHERE visit_id = @vid",
        [bq.p("now", "TIMESTAMP", now.isoformat()), bq.p("vid", "STRING", visit_id)],
    )

    return _get_visit_detail(visit_id, bq)


# ------------------------------------------------------------------
# GET /visit/{visit_id}/pdf  — Generate offering letter PDF
# ------------------------------------------------------------------
@router.get("/{visit_id}/pdf")
def download_pdf(
    visit_id: str,
    current_user: UserContext = Depends(require_auth),
):
    if current_user.role not in ("spv", "asm", "ddm", "ho_admin", "distributor_admin"):
        raise HTTPException(status_code=403, detail="Insufficient permissions to download PDF")

    bq = BQClient.get()
    visit_out = _get_visit_detail(visit_id, bq)

    try:
        from fpdf import FPDF

        pdf = FPDF()
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=15)

        # ── Header ──────────────────────────────────────────────────
        pdf.set_font("Helvetica", "B", 16)
        pdf.cell(0, 10, "SKINTIFIC — SURAT PENAWARAN DEMAND", align="C", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(0, 5, f"Dokumen ini digenerate otomatis  |  Visit ID: {visit_id}", align="C", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(4)
        pdf.set_draw_color(200, 200, 200)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(6)

        # ── Visit metadata ───────────────────────────────────────────
        def row2(label: str, value: str) -> None:
            pdf.set_font("Helvetica", "B", 9)
            pdf.cell(50, 6, label, new_x="RIGHT")
            pdf.set_font("Helvetica", "", 9)
            pdf.cell(0, 6, str(value or "—"), new_x="LMARGIN", new_y="NEXT")

        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(0, 8, "Informasi Kunjungan", new_x="LMARGIN", new_y="NEXT")
        row2("Tanggal Kunjungan :", visit_out.visit_date.strftime("%d %B %Y") if visit_out.visit_date else "—")
        row2("Salesman          :", visit_out.salesman_name or visit_out.salesman_sk)
        row2("Toko              :", visit_out.store_name or visit_out.outlet_sk or "—")
        row2("Distributor       :", visit_out.distributor_code or "—")
        row2("Efektif Call      :", "Ya" if visit_out.effective_call == "YES" else "Tidak")
        row2("Status Approval   :", visit_out.approval_status or "—")
        pdf.ln(4)

        # ── Items table ──────────────────────────────────────────────
        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(0, 8, "Detail Produk", new_x="LMARGIN", new_y="NEXT")

        # Table header
        col_w = [15, 60, 30, 20, 20, 20, 25]
        headers = ["No", "Nama Produk", "Brand", "Ukuran", "Qty SE", "Qty Final", "Demand (Rp)"]
        pdf.set_fill_color(37, 99, 235)
        pdf.set_text_color(255, 255, 255)
        pdf.set_font("Helvetica", "B", 8)
        for w, h in zip(col_w, headers):
            pdf.cell(w, 7, h, border=1, fill=True)
        pdf.ln()
        pdf.set_text_color(0, 0, 0)

        total_qty_se = 0
        total_qty_final = 0
        total_demand_val = 0.0
        pdf.set_font("Helvetica", "", 8)
        for idx, item in enumerate(visit_out.items, start=1):
            eff_qty = item.final_qty if item.final_qty is not None else (item.qty or 0)
            eff_demand = round(eff_qty * (item.stp or 0), 2)
            total_qty_se    += item.qty or 0
            total_qty_final += eff_qty
            total_demand_val += eff_demand
            fill = idx % 2 == 0
            pdf.set_fill_color(248, 250, 252)
            pdf.cell(col_w[0], 6, str(idx), border=1, fill=fill)
            pdf.cell(col_w[1], 6, (item.sku_name or item.sku_id)[:30], border=1, fill=fill)
            pdf.cell(col_w[2], 6, (item.brand or "—")[:18], border=1, fill=fill)
            pdf.cell(col_w[3], 6, (item.sku_size or "—"), border=1, fill=fill)
            pdf.cell(col_w[4], 6, str(item.qty or 0), border=1, fill=fill, align="R")
            pdf.cell(col_w[5], 6, str(eff_qty), border=1, fill=fill, align="R")
            pdf.cell(col_w[6], 6, f"{eff_demand:,.0f}", border=1, fill=fill, align="R")
            pdf.ln()

        # Total row
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_fill_color(226, 232, 240)
        pdf.cell(sum(col_w[:4]), 7, "TOTAL", border=1, fill=True)
        pdf.cell(col_w[4], 7, str(total_qty_se),    border=1, fill=True, align="R")
        pdf.cell(col_w[5], 7, str(total_qty_final), border=1, fill=True, align="R")
        pdf.cell(col_w[6], 7, f"{total_demand_val:,.0f}", border=1, fill=True, align="R")
        pdf.ln(10)

        # ── Signatures ───────────────────────────────────────────────
        pdf.set_font("Helvetica", "", 9)
        sig_w = 60
        pdf.cell(sig_w, 6, "Salesman,",   align="C")
        pdf.cell(10)
        pdf.cell(sig_w, 6, "SPV,",        align="C")
        pdf.cell(10)
        pdf.cell(sig_w, 6, "Distributor,", align="C")
        pdf.ln(20)
        pdf.cell(sig_w, 6, "(" + (visit_out.salesman_name or "___________") + ")", align="C")
        pdf.cell(10)
        pdf.cell(sig_w, 6, "(" + (visit_out.spv_username or "___________") + ")", align="C")
        pdf.cell(10)
        pdf.cell(sig_w, 6, "(_________________)", align="C")
        pdf.ln(8)
        pdf.set_font("Helvetica", "I", 7)
        pdf.set_text_color(150, 150, 150)
        pdf.cell(0, 5, f"Digenerate oleh: {current_user.username}  |  {datetime.now(timezone.utc).strftime('%d-%m-%Y %H:%M')} UTC", align="C")

        pdf_bytes = pdf.output()
    except ImportError:
        raise HTTPException(status_code=501, detail="PDF library (fpdf2) not installed on server")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"PDF generation failed: {exc}")

    # Log the download
    now = datetime.now(timezone.utc)
    dl_id = f"DL-{uuid.uuid4().hex[:16].upper()}"
    try:
        bq.execute(
            f"""
            INSERT INTO `{settings.bq_project}.{settings.bq_dataset}.step_visit_download_log`
              (download_id, visit_id, downloaded_by, user_role, downloaded_at)
            VALUES (@dlid, @vid, @by, @role, @now)
            """,
            [
                bq.p("dlid", "STRING",    dl_id),
                bq.p("vid",  "STRING",    visit_id),
                bq.p("by",   "STRING",    current_user.username),
                bq.p("role", "STRING",    current_user.role),
                bq.p("now",  "TIMESTAMP", now.isoformat()),
            ],
        )
    except Exception:
        pass  # log failure must never block the download

    filename = f"demand_{visit_id}_{now.strftime('%Y%m%d')}.pdf"
    return StreamingResponse(
        io.BytesIO(bytes(pdf_bytes)),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _get_visit_detail(visit_id: str, bq: BQClient) -> VisitOut:
    row = bq.query_one(
        f"""
        SELECT v.{_VISIT_COLS},
               sm.salesman_name,
               o.store_name,
               o.distributor_code
        FROM {settings.table('fact_visit')} v
        LEFT JOIN {settings.table('dim_salesman')} sm ON v.salesman_sk = sm.salesman_sk
        LEFT JOIN {settings.table('dim_outlet')} o   ON v.outlet_sk  = o.outlet_sk
        WHERE v.visit_id = @vid AND v.is_deleted = FALSE
        """,
        [bq.p("vid", "STRING", visit_id)],
    )
    if not row:
        raise HTTPException(status_code=404, detail="Visit not found")

    items = bq.query(
        f"""
        SELECT visit_item_id, sku_id, sku_name, brand, category,
               COALESCE(sku_size, NULL) AS sku_size,
               stp, qty,
               COALESCE(final_qty, NULL) AS final_qty,
               CASE WHEN final_qty IS NOT NULL
                    THEN ROUND(final_qty * stp, 2)
                    ELSE demand
               END AS demand
        FROM {settings.table('fact_visit_item')}
        WHERE visit_id = @vid
        ORDER BY sku_name
        """,
        [bq.p("vid", "STRING", visit_id)],
    )

    # Compute final_demand = sum using final_qty where set, else original qty
    has_override = any(i.get("final_qty") is not None for i in items)
    final_demand = sum(
        round((i.get("final_qty") if i.get("final_qty") is not None else i.get("qty") or 0)
              * (i.get("stp") or 0), 2)
        for i in items
    ) if items else None

    # Download count from audit log (graceful — table may not exist yet)
    download_count = 0
    try:
        dl = bq.query_one(
            f"SELECT COUNT(*) AS n FROM `{settings.bq_project}.{settings.bq_dataset}.step_visit_download_log` WHERE visit_id = @vid",
            [bq.p("vid", "STRING", visit_id)],
        )
        download_count = int((dl or {}).get("n", 0))
    except Exception:
        pass

    enriched_row = dict(row)
    enriched_row["final_demand"] = final_demand if has_override else None
    enriched_row["download_count"] = download_count

    return _row_to_visit(enriched_row, items)
