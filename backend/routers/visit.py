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
    SubmitRequest, UpdateAdjustmentRequest, UpdateFinalQtyRequest,
    UpdateStorePriceRequest,
    VisitItemOut, VisitListResponse, VisitOut,
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
    v.visit_id, v.salesman_sk, v.outlet_sk, v.schedule_id,
    v.visit_date, v.visit_type, v.brand_group,
    v.checkin_time, v.checkin_latitude, v.checkin_longitude,
    v.checkin_photo_url, v.checkin_distance_m,
    v.checkout_time, v.checkout_latitude, v.checkout_longitude,
    v.checkout_photo_url,
    v.total_demand, v.effective_call, v.notes, v.duration_minutes,
    v.visit_status, v.approval_status,
    v.spv_username, v.spv_approved_at,
    v.asm_username, v.asm_approved_at,
    v.ddm_username, v.ddm_approved_at,
    v.rejection_notes, v.revision_count,
    v.created_at, v.updated_at
"""


def _next_approval_status(current: str, role: str) -> str:
    """Return next approval_status when caller with `role` approves.
    Flow: SE submits → PENDING_SPV → (SPV) SPV_APPROVED → (dm / ho_admin) COMPLETED
    """
    transitions = {
        ("SUBMITTED",    "spv"):      "SPV_APPROVED",
        ("PENDING_SPV",  "spv"):      "SPV_APPROVED",
        ("SPV_APPROVED", "dm"):       "COMPLETED",
        ("SPV_APPROVED", "ho_admin"): "COMPLETED",
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
    # Only pass keys present in the row so Pydantic uses model defaults for missing fields
    # (e.g. download_count=0 when not in a list query row)
    return VisitOut(
        **{k: row[k] for k in VisitOut.model_fields if k not in ("items", "gps_warning") and k in row},
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

    # Notify all SPVs in one batch INSERT (avoids N×BQ-call timeout)
    try:
        bq.execute(
            f"""
            INSERT INTO {settings.table('notification')}
              (notification_id, user_id, type, title, body,
               is_read, is_deleted, deep_link, created_at)
            SELECT
              CONCAT('NOTIF-', SUBSTR(TO_HEX(FARM_FINGERPRINT(CONCAT(user_id, @vid))), 2, 16)),
              user_id,
              'VISIT_SUBMITTED',
              'Kunjungan Baru Perlu Disetujui',
              @body,
              FALSE, FALSE, @dl, @now
            FROM {settings.table('users')}
            WHERE role = 'spv' AND is_active = TRUE
            """,
            [
                bq.p("vid",  "STRING",    visit_id),
                bq.p("body", "STRING",    f"Kunjungan {visit_id} menunggu persetujuan Anda."),
                bq.p("dl",   "STRING",    f"visits/{visit_id}"),
                bq.p("now",  "TIMESTAMP", now.isoformat()),
            ],
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
        f"SELECT approval_status, salesman_sk, spv_username FROM {settings.table('fact_visit')} WHERE visit_id = @vid AND is_deleted = FALSE",
        [bq.p("vid", "STRING", visit_id)],
    )
    if not visit:
        raise HTTPException(status_code=404, detail="Visit not found")

    new_status = _next_approval_status(visit["approval_status"], effective_role)

    role_col_map = {
        "spv":      ("spv_username", "spv_approved_at"),
        "dm":       ("ddm_username", "ddm_approved_at"),
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

    # Notify the salesman that their visit was approved
    if salesman_sk := visit.get("salesman_sk"):
        _notify_user(
            bq, salesman_sk,
            "VISIT_APPROVED",
            "Kunjungan Disetujui",
            f"Kunjungan {visit_id} telah disetujui oleh {current_user.username}.",
            deep_link=f"visits/{visit_id}",
        )

    # When DM/HO completes the visit (COMPLETED status), also notify the SPV who approved it
    if effective_role in ("dm", "ho_admin"):
        spv_username = visit.get("spv_username")
        if spv_username:
            spv_row = bq.query_one(
                f"SELECT user_id FROM {settings.table('users')} WHERE username = @uname LIMIT 1",
                [bq.p("uname", "STRING", spv_username)],
            )
            if spv_row:
                _notify_user(
                    bq, spv_row["user_id"],
                    "VISIT_COMPLETED",
                    "Kunjungan Selesai",
                    f"Kunjungan {visit_id} telah disetujui distributor dan berstatus COMPLETED.",
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
    store_name: str | None = Query(None, description="Partial store name search"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    bg_clause, bg_params = brand_group_filter(current_user, table_alias="v")

    # Build visit-level conditions (applied to fact_visit before join)
    visit_conditions = [f"v.is_deleted = FALSE {bg_clause}"]
    params = list(bg_params)

    # Role scoping
    role = current_user.role
    if role == "salesman":
        visit_conditions.append("AND v.salesman_sk = @self_sk")
        params.append(bq.p("self_sk", "STRING", current_user.salesman_sk or current_user.user_id))
    elif role == "dm":
        # DM sees only visits from their distributor's outlets
        if current_user.distributor_code:
            visit_conditions.append("AND o.distributor_code = @dist_code")
            params.append(bq.p("dist_code", "STRING", current_user.distributor_code))
        # DM sees visits that need their action or are already completed
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

    # Use inline join so store_name filter and COUNT work together.
    # dim_salesman may have duplicate salesman_sk rows — deduplicate via subquery.
    join_query = f"""
        FROM {settings.table('fact_visit')} v
        LEFT JOIN (
            SELECT salesman_sk, salesman_name
            FROM {settings.table('dim_salesman')}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY salesman_sk ORDER BY salesman_sk) = 1
        ) sm ON v.salesman_sk = sm.salesman_sk
        LEFT JOIN (
            SELECT outlet_sk, store_name, distributor_code
            FROM {settings.table('dim_outlet')}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY outlet_sk ORDER BY outlet_sk) = 1
        ) o ON v.outlet_sk = o.outlet_sk
        WHERE {visit_where} {store_filter}
    """

    total = (bq.query_one(
        f"SELECT COUNT(*) AS n {join_query}",
        params,
    ) or {}).get("n", 0)

    rows = bq.query(
        f"""
        SELECT {_VISIT_COLS}, sm.salesman_name, o.store_name, o.distributor_code
        {join_query}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY v.visit_id ORDER BY v.updated_at DESC) = 1
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
    if current_user.role not in ("spv", "asm", "dm", "ho_admin"):
        raise HTTPException(status_code=403, detail="Only SPV and above can adjust final quantities")

    bq = BQClient.get()
    now = datetime.now(timezone.utc)

    visit = bq.query_one(
        f"SELECT visit_id, approval_status FROM {settings.table('fact_visit')} WHERE visit_id = @vid AND is_deleted = FALSE",
        [bq.p("vid", "STRING", visit_id)],
    )
    if not visit:
        raise HTTPException(status_code=404, detail="Visit not found")

    # Role-based status guard — ensures SPV acts before dist_admin, not after
    approval_status = visit.get("approval_status", "")
    role = current_user.role
    if role == "spv" and approval_status not in ("PENDING_SPV", "SUBMITTED"):
        raise HTTPException(status_code=403, detail="SPV can only edit final qty for visits pending SPV approval")
    if role in ("dm", "ho_admin") and approval_status not in ("SPV_APPROVED", "COMPLETED"):
        raise HTTPException(status_code=403, detail="DM can only edit final qty for SPV-approved visits")

    if not body.items:
        return _get_visit_detail(visit_id, bq)

    # Single-statement batch UPDATE using CASE expression.
    # Replaces N sequential DML calls (each ~1-3s BQ latency) with one call.
    case_clauses = "\n      ".join(
        f"WHEN @sku_{i} THEN @fqty_{i}" for i in range(len(body.items))
    )
    params: list = [
        bq.p("vid", "STRING", visit_id),
        bq.p("now", "TIMESTAMP", now.isoformat()),
    ]
    for i, fi in enumerate(body.items):
        params.append(bq.p(f"sku_{i}", "STRING", fi.sku_id))
        params.append(bq.p(f"fqty_{i}", "INT64",  fi.final_qty))

    bq.execute(
        f"""
        UPDATE {settings.table('fact_visit_item')}
        SET final_qty = CASE sku_id
          {case_clauses}
          ELSE final_qty
        END
        WHERE visit_id = @vid
        """,
        params,
    )

    bq.execute(
        f"UPDATE {settings.table('fact_visit')} SET updated_at = @now WHERE visit_id = @vid",
        [bq.p("now", "TIMESTAMP", now.isoformat()), bq.p("vid", "STRING", visit_id)],
    )

    return _get_visit_detail(visit_id, bq)


# ------------------------------------------------------------------
# PUT /visit/{visit_id}/store-price  — Distributor admin sets store price
# ------------------------------------------------------------------
@router.put("/{visit_id}/store-price", response_model=VisitOut)
def update_store_price(
    visit_id: str,
    body: UpdateStorePriceRequest,
    current_user: UserContext = Depends(require_auth),
):
    if current_user.role not in ("dm", "ho_admin"):
        raise HTTPException(status_code=403, detail="Only DM or HO admin can set store prices")

    bq = BQClient.get()
    now = datetime.now(timezone.utc)

    visit = bq.query_one(
        f"SELECT visit_id, approval_status FROM {settings.table('fact_visit')} WHERE visit_id = @vid AND is_deleted = FALSE",
        [bq.p("vid", "STRING", visit_id)],
    )
    if not visit:
        raise HTTPException(status_code=404, detail="Visit not found")

    if not body.items:
        return _get_visit_detail(visit_id, bq)

    case_clauses = "\n      ".join(
        f"WHEN @sku_{i} THEN @price_{i}" for i in range(len(body.items))
    )
    params: list = [
        bq.p("vid", "STRING", visit_id),
        bq.p("now", "TIMESTAMP", now.isoformat()),
    ]
    for i, si in enumerate(body.items):
        params.append(bq.p(f"sku_{i}",   "STRING",  si.sku_id))
        params.append(bq.p(f"price_{i}", "FLOAT64", si.price_for_store))

    bq.execute(
        f"""
        UPDATE {settings.table('fact_visit_item')}
        SET price_for_store = CASE sku_id
          {case_clauses}
          ELSE price_for_store
        END
        WHERE visit_id = @vid
        """,
        params,
    )

    bq.execute(
        f"UPDATE {settings.table('fact_visit')} SET updated_at = @now WHERE visit_id = @vid",
        [bq.p("now", "TIMESTAMP", now.isoformat()), bq.p("vid", "STRING", visit_id)],
    )

    return _get_visit_detail(visit_id, bq)


# ------------------------------------------------------------------
# PUT /visit/{visit_id}/adjustment  — Distributor admin invoice adjustment
# ------------------------------------------------------------------
@router.put("/{visit_id}/adjustment", response_model=VisitOut)
def update_adjustment(
    visit_id: str,
    body: UpdateAdjustmentRequest,
    current_user: UserContext = Depends(require_auth),
):
    """Add or reduce the invoice total (delivery fee, discount, promo, misc).
    Positive = surcharge, negative = reduction. Distributor admin only."""
    if current_user.role not in ("dm", "ho_admin"):
        raise HTTPException(status_code=403, detail="Only DM or HO admin can adjust the invoice")

    bq = BQClient.get()
    now = datetime.now(timezone.utc)

    visit = bq.query_one(
        f"SELECT visit_id FROM {settings.table('fact_visit')} WHERE visit_id = @vid AND is_deleted = FALSE",
        [bq.p("vid", "STRING", visit_id)],
    )
    if not visit:
        raise HTTPException(status_code=404, detail="Visit not found")

    bq.execute(
        f"""
        UPDATE {settings.table('fact_visit')} SET
          adjustment_amount = @amt,
          adjustment_note = @note,
          updated_at = @now
        WHERE visit_id = @vid
        """,
        [
            bq.p("amt",  "FLOAT64",   body.adjustment_amount),
            bq.p("note", "STRING",    body.adjustment_note),
            bq.p("now",  "TIMESTAMP", now.isoformat()),
            bq.p("vid",  "STRING",    visit_id),
        ],
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
    if current_user.role not in ("spv", "asm", "dm", "ho_admin"):
        raise HTTPException(status_code=403, detail="Insufficient permissions to download PDF")

    bq = BQClient.get()
    visit_out = _get_visit_detail(visit_id, bq)

    try:
        from fpdf import FPDF

        def _safe(text, maxlen: int = 0) -> str:
            if not text:
                return "-"
            s = str(text).encode("latin-1", errors="replace").decode("latin-1")
            return s[:maxlen] if maxlen else s

        def _fmt_date_id(d) -> str:
            if not d:
                return "-"
            months = ["", "Januari", "Februari", "Maret", "April", "Mei", "Juni",
                      "Juli", "Agustus", "September", "Oktober", "November", "Desember"]
            try:
                return f"{d.day} {months[d.month]} {d.year}"
            except Exception:
                return str(d)

        # ── Palette ─────────────────────────────────────────────────
        C_BLUE   = (37,  99,  235)
        C_DBLUE  = (30,  58,  138)
        C_LBLUE  = (191, 219, 254)   # blue-200
        C_WHITE  = (255, 255, 255)
        C_BG     = (248, 250, 252)   # slate-50
        C_BG2    = (241, 245, 249)   # slate-100
        C_BORDER = (226, 232, 240)   # slate-200
        C_TEXT   = (15,  23,  42)    # slate-900
        C_MUTED  = (100, 116, 139)   # slate-500
        C_GREEN  = (5,   150, 105)
        C_AMBER  = (217, 119, 6)
        C_RED    = (220, 38,  38)

        STATUS_LABELS = {
            "COMPLETED":         "SELESAI",
            "SPV_APPROVED":      "DISETUJUI SPV",
            "PENDING_SPV":       "MENUNGGU SPV",
            "SUBMITTED":         "MENUNGGU SPV",
            "REVISION_REQUIRED": "PERLU REVISI",
            "REJECTED":          "DITOLAK",
            "DRAFT":             "DRAFT",
        }
        STATUS_COLORS = {
            "COMPLETED":         C_GREEN,
            "SPV_APPROVED":      C_BLUE,
            "PENDING_SPV":       C_AMBER,
            "SUBMITTED":         C_AMBER,
            "REVISION_REQUIRED": C_RED,
            "REJECTED":          C_RED,
            "DRAFT":             C_MUTED,
        }

        status       = visit_out.approval_status or "DRAFT"
        status_label = STATUS_LABELS.get(status, status)
        st_color     = STATUS_COLORS.get(status, C_MUTED)

        # Pre-compute totals
        total_qty_final = sum(
            (i.final_qty if i.final_qty is not None else (i.qty or 0))
            for i in visit_out.items
        )
        has_prices = any(i.price_for_store is not None and i.price_for_store > 0 for i in visit_out.items)
        grand_total_price = sum(
            (i.final_qty if i.final_qty is not None else (i.qty or 0)) * (i.price_for_store or 0)
            for i in visit_out.items
            if i.price_for_store is not None
        )

        # ── Document setup ───────────────────────────────────────────
        pdf = FPDF(orientation="P", unit="mm", format="A4")
        pdf.set_margins(15, 15, 15)
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=22)

        # ── HEADER BAND ──────────────────────────────────────────────
        pdf.set_fill_color(*C_BLUE)
        pdf.rect(0, 0, 210, 28, "F")
        pdf.set_fill_color(*C_DBLUE)
        pdf.rect(0, 0, 6, 28, "F")

        pdf.set_xy(9, 6)
        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(*C_WHITE)
        pdf.cell(100, 7, "SKINTIFIC")

        pdf.set_xy(9, 14)
        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(*C_LBLUE)
        pdf.cell(100, 5, "SURAT PENAWARAN ORDER")

        # Right — visit ID + date
        pdf.set_xy(105, 6)
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_text_color(*C_WHITE)
        pdf.cell(90, 5, _safe(visit_out.visit_id), align="R")

        pdf.set_xy(105, 12)
        pdf.set_font("Helvetica", "", 7)
        pdf.set_text_color(*C_LBLUE)
        pdf.cell(90, 5, _fmt_date_id(visit_out.visit_date), align="R")

        # Status pill on header
        pdf.set_xy(105, 19)
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_text_color(*st_color)
        pdf.cell(90, 5, f"[ {status_label} ]", align="R")

        pdf.set_text_color(*C_TEXT)

        # ── INFO CARD ────────────────────────────────────────────────
        card_y = 32
        pdf.set_fill_color(*C_BG)
        pdf.set_draw_color(*C_BORDER)
        pdf.rect(15, card_y, 180, 40, "FD")

        # Column divider
        pdf.set_draw_color(*C_BORDER)
        pdf.line(100, card_y + 1, 100, card_y + 39)

        def _section_label(x: float, y: float, text: str) -> None:
            pdf.set_xy(x, y)
            pdf.set_font("Helvetica", "B", 6)
            pdf.set_text_color(*C_MUTED)
            pdf.cell(80, 4, text.upper())
            pdf.set_draw_color(*C_BORDER)

        def _info_row(x: float, y: float, label: str, value: str) -> None:
            pdf.set_xy(x, y)
            pdf.set_font("Helvetica", "B", 7.5)
            pdf.set_text_color(*C_MUTED)
            pdf.cell(30, 5, label)
            pdf.set_font("Helvetica", "", 7.5)
            pdf.set_text_color(*C_TEXT)
            pdf.cell(50, 5, value)

        _section_label(18, card_y + 3, "Informasi Kunjungan")
        _section_label(103, card_y + 3, "Toko & Distributor")

        r = card_y + 9
        _info_row(18,  r,     "Tanggal",      _fmt_date_id(visit_out.visit_date))
        _info_row(103, r,     "Toko",         _safe(visit_out.store_name or visit_out.outlet_sk, 28))
        _info_row(18,  r + 7, "Salesman",     _safe(visit_out.salesman_name or visit_out.salesman_sk, 28))
        _info_row(103, r + 7, "Distributor",  _safe(visit_out.distributor_code, 28))
        _info_row(18,  r + 14,"Efektif Call", "YA" if visit_out.effective_call == "YES" else "TIDAK")
        _info_row(103, r + 14,"Grup Brand",   _safe(visit_out.brand_group, 20))
        _info_row(18,  r + 21,"Durasi",       f"{visit_out.duration_minutes} menit" if visit_out.duration_minutes else "-")
        _info_row(103, r + 21,"No. Revisi",   str(visit_out.revision_count or 0))

        # ── SUMMARY STATS ROW ────────────────────────────────────────
        stat_y = card_y + 45
        stats = [
            ("TOTAL SKU",    str(len(visit_out.items)),                              "produk"),
            ("QTY FINAL",    str(total_qty_final),                                   "pcs"),
            ("TOTAL HARGA",  f"Rp {grand_total_price:,.0f}" if has_prices else "-",  ""),
            ("STATUS",       status_label,                                            ""),
        ]
        box_w = 43
        for si, (lbl, val, unit) in enumerate(stats):
            bx = 15 + si * (box_w + 0.33)
            # Blue accent top stripe
            pdf.set_fill_color(*C_BLUE)
            pdf.rect(bx, stat_y, box_w, 2.5, "F")
            # Box body
            pdf.set_fill_color(*C_BG)
            pdf.set_draw_color(*C_BORDER)
            pdf.rect(bx, stat_y + 2.5, box_w, 15, "FD")
            # Label
            pdf.set_xy(bx + 2, stat_y + 4)
            pdf.set_font("Helvetica", "B", 5.5)
            pdf.set_text_color(*C_MUTED)
            pdf.cell(box_w - 4, 3.5, lbl)
            # Value
            pdf.set_xy(bx + 2, stat_y + 8)
            fs = 9 if si < 3 else 7
            pdf.set_font("Helvetica", "B", fs)
            pdf.set_text_color(*C_TEXT)
            pdf.cell(box_w - 4, 6, _safe(val))
            if unit:
                pdf.set_xy(bx + 2, stat_y + 14)
                pdf.set_font("Helvetica", "", 5.5)
                pdf.set_text_color(*C_MUTED)
                pdf.cell(box_w - 4, 3, unit)

        pdf.set_xy(15, stat_y + 21)

        # ── PRODUCTS TABLE ───────────────────────────────────────────
        pdf.set_font("Helvetica", "B", 8.5)
        pdf.set_text_color(*C_TEXT)
        pdf.cell(0, 7, "DETAIL PRODUK ORDER", new_x="LMARGIN", new_y="NEXT")

        # col widths: No, Nama Produk, Brand, Qty Final, Harga Toko/PCS, Total Harga = 180 total
        cw = [9, 63, 24, 18, 34, 32]
        col_headers = ["No", "Nama Produk", "Brand", "Qty Final", "Harga Toko/PCS", "Total Harga (Rp)"]
        col_aligns  = ["C",  "L",           "L",     "R",         "R",               "R"]

        # Header row
        pdf.set_fill_color(*C_BLUE)
        pdf.set_text_color(*C_WHITE)
        pdf.set_font("Helvetica", "B", 6.5)
        for w, h, a in zip(cw, col_headers, col_aligns):
            pdf.cell(w, 6.5, h, fill=True, align=a)
        pdf.ln()

        pdf.set_text_color(*C_TEXT)
        pdf.set_font("Helvetica", "", 7)

        for idx, item in enumerate(visit_out.items, 1):
            eff_qty    = item.final_qty if item.final_qty is not None else (item.qty or 0)
            price      = item.price_for_store or 0
            total_price = eff_qty * price
            fq_modified = item.final_qty is not None and item.final_qty != item.qty

            if idx % 2 == 0:
                pdf.set_fill_color(*C_BG2)
            else:
                pdf.set_fill_color(*C_WHITE)

            pdf.cell(cw[0], 6, str(idx), fill=True, align="C", border="B")
            name = _safe(item.sku_name or item.sku_id, 40)
            pdf.cell(cw[1], 6, name, fill=True, border="B")
            pdf.cell(cw[2], 6, _safe(item.brand or "-", 14), fill=True, border="B")

            if fq_modified:
                pdf.set_text_color(*C_BLUE)
            pdf.cell(cw[3], 6, str(eff_qty), fill=True, align="R", border="B")
            pdf.set_text_color(*C_TEXT)

            price_str = f"{price:,.0f}" if price > 0 else "-"
            pdf.cell(cw[4], 6, price_str, fill=True, align="R", border="B")

            total_str = f"{total_price:,.0f}" if price > 0 else "-"
            if price > 0:
                pdf.set_text_color(*C_GREEN)
            pdf.cell(cw[5], 6, total_str, fill=True, align="R", border="B")
            pdf.set_text_color(*C_TEXT)
            pdf.ln()

        # Totals row
        pdf.set_fill_color(*C_BG2)
        pdf.set_font("Helvetica", "B", 7)
        pdf.cell(sum(cw[:3]), 7, "TOTAL", fill=True, border="T")
        pdf.cell(cw[3], 7, str(total_qty_final), fill=True, align="R", border="T")
        pdf.cell(cw[4], 7, "", fill=True, border="T")
        if has_prices:
            pdf.set_text_color(*C_GREEN)
            pdf.cell(cw[5], 7, f"{grand_total_price:,.0f}", fill=True, align="R", border="T")
            pdf.set_text_color(*C_TEXT)
        else:
            pdf.cell(cw[5], 7, "-", fill=True, align="R", border="T")
        pdf.ln(4)

        if has_prices:
            adj_amt = visit_out.adjustment_amount or 0
            final_invoice = grand_total_price + adj_amt
            sy = pdf.get_y() + 1
            box_x, box_w2 = 110, 85
            rows_n = 3 if adj_amt else 1
            pdf.set_draw_color(*C_BORDER)
            pdf.set_fill_color(*C_BG)
            pdf.rect(box_x, sy, box_w2, 8 * rows_n + 3, "FD")

            def _sum_row(i: int, label: str, value: str, bold: bool = False, color=C_TEXT) -> None:
                yy = sy + 2 + i * 8
                pdf.set_xy(box_x + 3, yy)
                pdf.set_font("Helvetica", "B" if bold else "", 8)
                pdf.set_text_color(*(color if bold else C_MUTED))
                pdf.cell(42, 6, label)
                pdf.set_xy(box_x + 3, yy)
                pdf.set_font("Helvetica", "B", 9.5 if bold else 8)
                pdf.set_text_color(*color)
                pdf.cell(box_w2 - 6, 6, value, align="R")

            if adj_amt:
                _sum_row(0, "Subtotal", f"Rp {grand_total_price:,.0f}")
                sign = "+" if adj_amt > 0 else "-"
                a_color = C_AMBER if adj_amt > 0 else C_RED
                a_label = "Adjustment"
                if visit_out.adjustment_note:
                    a_label = f"Adj: {_safe(visit_out.adjustment_note, 16)}"
                _sum_row(1, a_label, f"{sign} Rp {abs(adj_amt):,.0f}", color=a_color)
                # divider before final
                pdf.set_draw_color(*C_BORDER)
                pdf.line(box_x + 3, sy + 2 + 2 * 8 - 1, box_x + box_w2 - 3, sy + 2 + 2 * 8 - 1)
                _sum_row(2, "Final Invoice", f"Rp {final_invoice:,.0f}", bold=True, color=C_GREEN)
            else:
                _sum_row(0, "Grand Total", f"Rp {grand_total_price:,.0f}", bold=True, color=C_GREEN)

            pdf.set_y(sy + 8 * rows_n + 3)
            pdf.ln(6)
            pdf.set_text_color(*C_TEXT)
        else:
            pdf.ln(3)

        # ── APPROVAL INFORMATION ─────────────────────────────────────
        pdf.set_font("Helvetica", "B", 8.5)
        pdf.set_text_color(*C_TEXT)
        pdf.cell(0, 7, "INFORMASI PERSETUJUAN", new_x="LMARGIN", new_y="NEXT")

        appr_rows = []
        if visit_out.spv_username:
            appr_rows.append(("SPV", visit_out.spv_username, visit_out.spv_approved_at, True))
        if visit_out.ddm_username:
            appr_rows.append(("Distributor Manager", visit_out.ddm_username, visit_out.ddm_approved_at, True))

        if appr_rows:
            ay = pdf.get_y()
            row_h = 6.5
            card_h = row_h * len(appr_rows) + 6
            pdf.set_fill_color(*C_BG)
            pdf.set_draw_color(*C_BORDER)
            pdf.rect(15, ay, 180, card_h, "FD")
            for ai, (role_lbl, approver, appr_ts, done) in enumerate(appr_rows):
                ry = ay + 3 + ai * row_h
                pdf.set_xy(18, ry)
                pdf.set_font("Helvetica", "B", 7.5)
                pdf.set_text_color(*C_GREEN)
                pdf.cell(6, 5, "[OK]")
                pdf.set_text_color(*C_TEXT)
                pdf.cell(30, 5, role_lbl)
                pdf.set_font("Helvetica", "", 7.5)
                pdf.cell(50, 5, _safe(approver))
                if appr_ts:
                    pdf.set_font("Helvetica", "I", 6.5)
                    pdf.set_text_color(*C_MUTED)
                    ts_str = str(appr_ts)[:16]
                    pdf.cell(60, 5, ts_str)
                    pdf.set_text_color(*C_TEXT)
            pdf.ln(card_h + 5)
        else:
            pdf.set_font("Helvetica", "I", 7.5)
            pdf.set_text_color(*C_AMBER)
            stage = "SPV" if status in ("PENDING_SPV", "SUBMITTED") else "Distributor Admin"
            pdf.cell(0, 5, f"Menunggu persetujuan {stage}", new_x="LMARGIN", new_y="NEXT")
            pdf.set_text_color(*C_TEXT)
            pdf.ln(5)

        # ── SIGNATURE SECTION ────────────────────────────────────────
        pdf.set_font("Helvetica", "B", 8.5)
        pdf.cell(0, 7, "TANDA TANGAN", new_x="LMARGIN", new_y="NEXT")

        sig_y = pdf.get_y()
        sw = 55
        sg = 7.5
        sig_entries = [
            ("Salesman",          visit_out.salesman_name),
            ("Supervisor (SPV)",  visit_out.spv_username),
            ("Distributor Manager", visit_out.ddm_username),
        ]
        for si2, (s_role, s_name) in enumerate(sig_entries):
            bx2 = 15 + si2 * (sw + sg)
            pdf.set_fill_color(*C_BG)
            pdf.set_draw_color(*C_BORDER)
            pdf.rect(bx2, sig_y, sw, 28, "FD")
            # Role label at top
            pdf.set_xy(bx2 + 2, sig_y + 2)
            pdf.set_font("Helvetica", "B", 6.5)
            pdf.set_text_color(*C_MUTED)
            pdf.cell(sw - 4, 4, s_role, align="C")
            # Name at bottom
            pdf.set_xy(bx2 + 2, sig_y + 22)
            pdf.set_font("Helvetica", "", 7)
            pdf.set_text_color(*C_TEXT)
            n_display = f"({_safe(s_name, 24)})" if s_name else "(__________________)"
            pdf.cell(sw - 4, 4, n_display, align="C")

        pdf.ln(32)

        # ── FOOTER ──────────────────────────────────────────────────
        # Positioned near bottom regardless of content length
        footer_y = 275
        if pdf.get_y() < footer_y:
            pdf.set_y(footer_y)
        pdf.set_draw_color(*C_BORDER)
        pdf.line(15, pdf.get_y(), 195, pdf.get_y())
        pdf.ln(2)
        pdf.set_font("Helvetica", "I", 6)
        pdf.set_text_color(*C_MUTED)
        now_str = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
        pdf.cell(90, 4, f"Digenerate oleh: {_safe(current_user.username)}  |  {now_str}")
        pdf.cell(90, 4, f"Halaman {pdf.page_no()}", align="R")

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

    # Filename: {StoreName}_{OrderDate ddMMyyyy}.pdf  e.g. Guardian_Bandung_13072026.pdf
    import re as _re
    _store = visit_out.store_name or visit_out.outlet_sk or "Order"
    _store = _re.sub(r"[^A-Za-z0-9]+", "_", str(_store)).strip("_") or "Order"
    try:
        _date = visit_out.visit_date.strftime("%d%m%Y")
    except Exception:
        _date = now.strftime("%d%m%Y")
    filename = f"{_store}_{_date}.pdf"
    return StreamingResponse(
        io.BytesIO(bytes(pdf_bytes)),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _get_visit_detail(visit_id: str, bq: BQClient) -> VisitOut:
    row = bq.query_one(
        f"""
        SELECT {_VISIT_COLS},
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

    P = settings.bq_project
    items = bq.query(
        f"""
        WITH
        latest_msdb AS (
          SELECT cust_id, dst_id_skt, dst_id_g2g, dst_id_tph
          FROM `{P}.gt_schema.master_store_database_basis`
          QUALIFY ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY input_date DESC) = 1
        ),
        store_dist AS (
          SELECT
            MAX(m.dst_id_skt) AS dst_skt,
            MAX(m.dst_id_g2g) AS dst_g2g,
            MAX(m.dst_id_tph) AS dst_tph
          FROM {settings.table('fact_visit')} v
          JOIN {settings.table('dim_outlet')} o ON o.outlet_sk = v.outlet_sk
          LEFT JOIN latest_msdb m ON m.cust_id = o.source_outlet_code
          WHERE v.visit_id = @vid
        ),
        latest_stock AS (
          SELECT distributor_code, product_id, current_stock_qty
          FROM `{P}.gt_schema.dist_stock_all_v`
          QUALIFY ROW_NUMBER() OVER (PARTITION BY distributor_code, product_id ORDER BY date DESC) = 1
        )
        SELECT vi.visit_item_id, vi.sku_id, vi.sku_name, vi.brand, vi.category,
               NULL AS sku_size,
               vi.stp, vi.qty, vi.final_qty, vi.demand,
               vi.price_for_store,
               s.current_stock_qty AS warehouse_stock_qty
        FROM {settings.table('fact_visit_item')} vi
        CROSS JOIN store_dist d
        LEFT JOIN latest_stock s
          ON s.distributor_code = CASE
            WHEN UPPER(vi.brand) LIKE '%G2G%' THEN d.dst_g2g
            WHEN UPPER(vi.brand) LIKE '%TPH%' OR UPPER(vi.brand) LIKE '%TIME%' THEN d.dst_tph
            ELSE d.dst_skt
          END
          AND s.product_id = vi.sku_id
        WHERE vi.visit_id = @vid
        ORDER BY vi.sku_name
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

    # Invoice adjustment (graceful — columns may not exist until migration 005 runs)
    adjustment_amount, adjustment_note = None, None
    try:
        adj = bq.query_one(
            f"SELECT adjustment_amount, adjustment_note FROM {settings.table('fact_visit')} WHERE visit_id = @vid",
            [bq.p("vid", "STRING", visit_id)],
        )
        if adj:
            adjustment_amount = adj.get("adjustment_amount")
            adjustment_note = adj.get("adjustment_note")
    except Exception:
        pass  # pre-migration: adjustment columns not present yet

    enriched_row = dict(row)
    enriched_row["final_demand"] = final_demand if has_override else None
    enriched_row["download_count"] = download_count
    enriched_row["adjustment_amount"] = adjustment_amount
    enriched_row["adjustment_note"] = adjustment_note

    return _row_to_visit(enriched_row, items)
