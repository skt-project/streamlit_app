"""
Skipped Store module — tracks stores SE did not visit on scheduled day.

POST /skipped-stores              — SE submits batch of skipped stores
GET  /skipped-stores              — SPV/ASM lists pending skipped stores
GET  /skipped-stores/summary      — count by status for SPV dashboard
PUT  /skipped-stores/{id}/return  — SPV returns store to SE
PUT  /skipped-stores/{id}/execute — SPV records that they will execute the visit
"""
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from config import settings
from dependencies import require_auth, require_role, brand_group_filter
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/skipped-stores", tags=["skipped-stores"])

_TABLE = f"`{settings.bq_project}.{settings.bq_dataset}.step_skipped_store`"


# ── Pydantic models ──────────────────────────────────────────────────────────

class SkippedStoreIn(BaseModel):
    outlet_sk: str
    outlet_name: str | None = None
    distributor_code: str | None = None
    brand_group: str | None = None
    week_iso: str
    visit_date: str          # ISO date YYYY-MM-DD


class BatchSkipRequest(BaseModel):
    salesman_sk: str
    stores: list[SkippedStoreIn]


class SkippedStoreOut(BaseModel):
    skipped_store_id: str
    salesman_sk: str
    outlet_sk: str
    outlet_name: str | None
    distributor_code: str | None
    brand_group: str | None
    week_iso: str
    visit_date: str
    skipped_at: datetime
    status: str
    spv_action_by: str | None
    spv_action_at: datetime | None
    spv_notes: str | None
    executed_visit_id: str | None


class SpvActionRequest(BaseModel):
    notes: str | None = None


# ── Helpers ──────────────────────────────────────────────────────────────────

def _notify_user(bq: BQClient, user_id: str, ntype: str, title: str, body: str, deep_link: str | None = None) -> None:
    now = datetime.now(timezone.utc)
    notif_id = f"NOTIF-{uuid.uuid4().hex[:16].upper()}"
    try:
        bq.execute(
            f"""
            INSERT INTO {settings.table('notification')}
              (notification_id, user_id, type, title, body, is_read, is_deleted, deep_link, created_at)
            VALUES (@nid, @uid, @ntype, @title, @body, FALSE, FALSE, @dl, @now)
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
        pass


def _find_se_user_id(bq: BQClient, salesman_sk: str) -> str | None:
    """Look up the SE's user_id from sfa_web.users via salesman_sk."""
    row = bq.query_one(
        f"SELECT user_id FROM {settings.table('users')} WHERE salesman_sk = @sk AND is_active = TRUE LIMIT 1",
        [bq.p("sk", "STRING", salesman_sk)],
    )
    return (row or {}).get("user_id")


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.post("", status_code=201)
def submit_skipped_stores(
    body: BatchSkipRequest,
    current_user: UserContext = Depends(require_auth),
):
    """SE submits all stores that were on PJP but not visited today."""
    bq = BQClient.get()
    now = datetime.now(timezone.utc)
    inserted = 0

    for store in body.stores:
        # Idempotency: skip if already recorded for same salesman/outlet/date
        existing = bq.query_one(
            f"SELECT skipped_store_id FROM {_TABLE} WHERE salesman_sk = @sk AND outlet_sk = @osk AND visit_date = @vdate AND is_deleted = FALSE",
            [
                bq.p("sk",    "STRING", body.salesman_sk),
                bq.p("osk",   "STRING", store.outlet_sk),
                bq.p("vdate", "DATE",   store.visit_date),
            ],
        )
        if existing:
            continue

        sid = f"SKP-{uuid.uuid4().hex[:16].upper()}"
        bq.execute(
            f"""
            INSERT INTO {_TABLE} (
              skipped_store_id, salesman_sk, outlet_sk, outlet_name,
              distributor_code, brand_group, week_iso, visit_date,
              skipped_at, status, is_deleted, created_at
            ) VALUES (
              @sid, @sk, @osk, @oname, @dc, @bg, @week, @vdate,
              @now, 'PENDING_SPV', FALSE, @now
            )
            """,
            [
                bq.p("sid",   "STRING",    sid),
                bq.p("sk",    "STRING",    body.salesman_sk),
                bq.p("osk",   "STRING",    store.outlet_sk),
                bq.p("oname", "STRING",    store.outlet_name),
                bq.p("dc",    "STRING",    store.distributor_code),
                bq.p("bg",    "STRING",    store.brand_group),
                bq.p("week",  "STRING",    store.week_iso),
                bq.p("vdate", "DATE",      store.visit_date),
                bq.p("now",   "TIMESTAMP", now.isoformat()),
            ],
        )
        inserted += 1

    # Notify SPVs
    if inserted > 0:
        try:
            spvs = bq.query(
                f"SELECT user_id FROM {settings.table('users')} WHERE role = 'spv' AND is_active = TRUE",
                [],
            )
            for spv in spvs:
                _notify_user(
                    bq, spv["user_id"],
                    "SKIPPED_STORE",
                    "Toko Tidak Dikunjungi",
                    f"{inserted} toko belum dikunjungi oleh salesman {body.salesman_sk}.",
                    deep_link="skipped-stores",
                )
        except Exception:
            pass

    if inserted > 0:
        bq.cache.invalidate("skipped:")
    return {"inserted": inserted, "total": len(body.stores)}


@router.get("", response_model=list[SkippedStoreOut])
def list_skipped_stores(
    week_iso: str | None = Query(None),
    status: str | None = Query(None, description="PENDING_SPV | RETURNED_TO_SALESMAN | EXECUTED_BY_SPV | EXPIRED"),
    brand_group: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    conditions = ["is_deleted = FALSE"]
    params: list = []

    if week_iso:
        conditions.append("AND week_iso = @week")
        params.append(bq.p("week", "STRING", week_iso))
    if status:
        conditions.append("AND status = @status")
        params.append(bq.p("status", "STRING", status))
    else:
        # Default: only show actionable items
        conditions.append("AND status = 'PENDING_SPV'")
    if brand_group:
        conditions.append("AND brand_group = @bg")
        params.append(bq.p("bg", "STRING", brand_group))

    where = " ".join(conditions)
    offset = (page - 1) * page_size

    cache_key = f"skipped:list:{current_user.brand_group or 'all'}:{week_iso or ''}:{status or 'PENDING_SPV'}:{brand_group or ''}:{page}:{page_size}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    rows = bq.query(
        f"""
        SELECT skipped_store_id, salesman_sk, outlet_sk, outlet_name,
               distributor_code, brand_group, week_iso, visit_date,
               skipped_at, status, spv_action_by, spv_action_at, spv_notes, executed_visit_id
        FROM {_TABLE}
        WHERE {where}
        ORDER BY skipped_at DESC
        LIMIT @lim OFFSET @off
        """,
        params + [bq.p("lim", "INT64", page_size), bq.p("off", "INT64", offset)],
    )

    result = [SkippedStoreOut(**r) for r in rows]
    bq.cache.set(cache_key, result, ttl=30)  # 30s — SPV workflow data, short TTL to stay near real-time
    return result


@router.get("/summary")
def skipped_store_summary(
    week_iso: str | None = Query(None),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    cache_key = f"skipped:summary:{week_iso or 'all'}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached
    params: list = [bq.p("wfilt", "STRING", week_iso or "")]
    row = bq.query_one(
        f"""
        SELECT
          COUNTIF(status = 'PENDING_SPV')            AS pending,
          COUNTIF(status = 'RETURNED_TO_SALESMAN')   AS returned,
          COUNTIF(status = 'EXECUTED_BY_SPV')        AS executed,
          COUNTIF(status = 'EXPIRED')                AS expired,
          COUNT(*) AS total
        FROM {_TABLE}
        WHERE is_deleted = FALSE
          AND (@wfilt = '' OR week_iso = @wfilt)
        """,
        params,
    )
    result = row or {}
    bq.cache.set(cache_key, result, ttl=30)
    return result


@router.put("/{skipped_store_id}/return", response_model=SkippedStoreOut)
def return_to_salesman(
    skipped_store_id: str,
    body: SpvActionRequest,
    current_user: UserContext = Depends(require_role("spv", "asm", "dm", "ho_admin")),
):
    """SPV sends the skipped store back to the assigned salesman to visit."""
    bq = BQClient.get()
    now = datetime.now(timezone.utc)

    rec = bq.query_one(
        f"SELECT * FROM {_TABLE} WHERE skipped_store_id = @sid AND is_deleted = FALSE",
        [bq.p("sid", "STRING", skipped_store_id)],
    )
    if not rec:
        raise HTTPException(status_code=404, detail="Skipped store record not found")
    if rec["status"] != "PENDING_SPV":
        raise HTTPException(status_code=409, detail=f"Record already actioned: {rec['status']}")

    bq.execute(
        f"""
        UPDATE {_TABLE}
        SET status = 'RETURNED_TO_SALESMAN',
            spv_action_by = @spv, spv_action_at = @now, spv_notes = @notes
        WHERE skipped_store_id = @sid
        """,
        [
            bq.p("spv",   "STRING",    current_user.username),
            bq.p("now",   "TIMESTAMP", now.isoformat()),
            bq.p("notes", "STRING",    body.notes),
            bq.p("sid",   "STRING",    skipped_store_id),
        ],
    )
    bq.cache.invalidate("skipped:")

    # Notify original SE
    if se_uid := _find_se_user_id(bq, rec["salesman_sk"]):
        _notify_user(
            bq, se_uid,
            "SKIPPED_STORE_RETURNED",
            "Toko Dikembalikan ke Anda",
            f"Toko {rec.get('outlet_name', rec['outlet_sk'])} dikembalikan oleh SPV {current_user.username}. Harap kunjungi secepatnya.",
            deep_link="skipped-stores",
        )

    updated = bq.query_one(
        f"SELECT * FROM {_TABLE} WHERE skipped_store_id = @sid",
        [bq.p("sid", "STRING", skipped_store_id)],
    )
    return SkippedStoreOut(**updated)


@router.put("/{skipped_store_id}/execute", response_model=SkippedStoreOut)
def execute_by_spv(
    skipped_store_id: str,
    body: SpvActionRequest,
    current_user: UserContext = Depends(require_role("spv", "asm", "dm", "ho_admin")),
):
    """SPV takes over and will personally execute the visit for the skipped store."""
    bq = BQClient.get()
    now = datetime.now(timezone.utc)

    rec = bq.query_one(
        f"SELECT * FROM {_TABLE} WHERE skipped_store_id = @sid AND is_deleted = FALSE",
        [bq.p("sid", "STRING", skipped_store_id)],
    )
    if not rec:
        raise HTTPException(status_code=404, detail="Skipped store record not found")
    if rec["status"] != "PENDING_SPV":
        raise HTTPException(status_code=409, detail=f"Record already actioned: {rec['status']}")

    bq.execute(
        f"""
        UPDATE {_TABLE}
        SET status = 'EXECUTED_BY_SPV',
            spv_action_by = @spv, spv_action_at = @now, spv_notes = @notes
        WHERE skipped_store_id = @sid
        """,
        [
            bq.p("spv",   "STRING",    current_user.username),
            bq.p("now",   "TIMESTAMP", now.isoformat()),
            bq.p("notes", "STRING",    body.notes),
            bq.p("sid",   "STRING",    skipped_store_id),
        ],
    )
    bq.cache.invalidate("skipped:")

    updated = bq.query_one(
        f"SELECT * FROM {_TABLE} WHERE skipped_store_id = @sid",
        [bq.p("sid", "STRING", skipped_store_id)],
    )
    return SkippedStoreOut(**updated)
