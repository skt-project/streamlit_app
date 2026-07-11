"""
GET  /stock                        — SE's current stock
POST /stock/request                — SE requests stock from SPV
PUT  /stock/request/{id}/approve   — SPV approves
PUT  /stock/request/{id}/reject    — SPV rejects
GET  /stock/requests               — list pending requests (SE or SPV view)
"""
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from config import settings
from dependencies import require_auth
from models.auth import UserContext
from models.stock import (
    StockOut, StockRequestApproveIn, StockRequestIn,
    StockRequestOut, StockRequestRejectIn,
)
from services.bq import BQClient

router = APIRouter(prefix="/stock", tags=["stock"])


@router.get("", response_model=list[StockOut])
def get_stock(
    salesman_sk: str = Query(...),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    cache_key = f"stock:{salesman_sk}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached
    rows = bq.query(
        f"""
        SELECT stock_id, salesman_sk, sku_id, sku_name, brand, brand_group,
               stp, qty_current, assigned_by_sk, updated_at
        FROM {settings.table('fact_salesman_stock')}
        WHERE salesman_sk = @sk
        ORDER BY brand, sku_name
        """,
        [bq.p("sk", "STRING", salesman_sk)],
    )
    result = [StockOut(**r) for r in rows]
    bq.cache.set(cache_key, result, ttl=60)  # 1 min — stock changes on SPV approval only
    return result


@router.post("/request", status_code=201, response_model=StockRequestOut)
def create_stock_request(
    body: StockRequestIn,
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    now = datetime.now(timezone.utc)

    # Find salesman_sk for current user
    sm = bq.query_one(
        f"SELECT salesman_sk FROM {settings.table('users')} WHERE user_id = @uid",
        [bq.p("uid", "STRING", current_user.user_id)],
    )
    salesman_sk = (sm or {}).get("salesman_sk") or current_user.user_id

    # Find SPV
    spv = bq.query_one(
        f"""
        SELECT u2.salesman_sk AS spv_sk
        FROM {settings.table('users')} u1
        JOIN {settings.table('users')} u2 ON u2.username = u1.supervisor_username
        WHERE u1.user_id = @uid
        """,
        [bq.p("uid", "STRING", current_user.user_id)],
    )
    spv_sk = (spv or {}).get("spv_sk") or "UNKNOWN"

    # SKU details
    sku = bq.query_one(
        f"SELECT sku_name FROM {settings.table('dim_sku')} WHERE sku_id = @sid",
        [bq.p("sid", "STRING", body.sku_id)],
    )

    request_id = f"REQ-{uuid.uuid4().hex[:16].upper()}"
    bq.execute(
        f"""
        INSERT INTO {settings.table('fact_stock_request')} (
          request_id, salesman_sk, spv_sk, sku_id, sku_name,
          qty_requested, status, notes_se, created_at, updated_at
        ) VALUES (
          @rid, @sm_sk, @spv_sk, @sku_id, @sku_name,
          @qty, 'PENDING', @notes, @now, @now
        )
        """,
        [
            bq.p("rid",      "STRING",    request_id),
            bq.p("sm_sk",    "STRING",    salesman_sk),
            bq.p("spv_sk",   "STRING",    spv_sk),
            bq.p("sku_id",   "STRING",    body.sku_id),
            bq.p("sku_name", "STRING",    (sku or {}).get("sku_name")),
            bq.p("qty",      "INT64",     body.qty_requested),
            bq.p("notes",    "STRING",    body.notes_se),
            bq.p("now",      "TIMESTAMP", now.isoformat()),
        ],
    )
    return StockRequestOut(
        request_id=request_id, salesman_sk=salesman_sk, spv_sk=spv_sk,
        sku_id=body.sku_id, sku_name=(sku or {}).get("sku_name"),
        qty_requested=body.qty_requested, status="PENDING",
        notes_se=body.notes_se, created_at=now, updated_at=now,
    )


@router.put("/request/{request_id}/approve", response_model=StockRequestOut)
def approve_stock_request(
    request_id: str,
    body: StockRequestApproveIn,
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    now = datetime.now(timezone.utc)

    req = bq.query_one(
        f"SELECT * FROM {settings.table('fact_stock_request')} WHERE request_id = @rid",
        [bq.p("rid", "STRING", request_id)],
    )
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")

    bq.execute(
        f"""
        UPDATE {settings.table('fact_stock_request')} SET
          status = 'APPROVED', qty_approved = @qty, notes_spv = @notes, updated_at = @now
        WHERE request_id = @rid
        """,
        [
            bq.p("qty",   "INT64",     body.qty_approved),
            bq.p("notes", "STRING",    body.notes_spv),
            bq.p("now",   "TIMESTAMP", now.isoformat()),
            bq.p("rid",   "STRING",    request_id),
        ],
    )

    # Update salesman stock
    stock_id = f"STK-{uuid.uuid4().hex[:12].upper()}"
    bq.execute(
        f"""
        MERGE {settings.table('fact_salesman_stock')} T
        USING (SELECT @sm_sk AS salesman_sk, @sku_id AS sku_id) S
        ON T.salesman_sk = S.salesman_sk AND T.sku_id = S.sku_id
        WHEN MATCHED THEN UPDATE SET
          qty_current = T.qty_current + @qty,
          assigned_by_sk = @spv_sk, updated_at = @now
        WHEN NOT MATCHED THEN INSERT (
          stock_id, salesman_sk, sku_id, sku_name, brand, brand_group,
          stp, qty_current, assigned_by_sk, updated_at
        ) SELECT @sid, @sm_sk, @sku_id, r.sku_name, s.brand, s.brand_group,
                 s.stp, @qty, @spv_sk, @now
          FROM {settings.table('fact_stock_request')} r
          JOIN {settings.table('dim_sku')} s USING (sku_id)
          WHERE r.request_id = @rid
          LIMIT 1
        """,
        [
            bq.p("sm_sk",  "STRING",    req["salesman_sk"]),
            bq.p("sku_id", "STRING",    req["sku_id"]),
            bq.p("qty",    "INT64",     body.qty_approved),
            bq.p("spv_sk", "STRING",    req["spv_sk"]),
            bq.p("now",    "TIMESTAMP", now.isoformat()),
            bq.p("sid",    "STRING",    stock_id),
            bq.p("rid",    "STRING",    request_id),
        ],
    )

    bq.cache.invalidate("stock:")

    updated = bq.query_one(
        f"SELECT * FROM {settings.table('fact_stock_request')} WHERE request_id = @rid",
        [bq.p("rid", "STRING", request_id)],
    )
    return StockRequestOut(**updated)


@router.put("/request/{request_id}/reject", response_model=StockRequestOut)
def reject_stock_request(
    request_id: str,
    body: StockRequestRejectIn,
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    now = datetime.now(timezone.utc)

    req = bq.query_one(
        f"SELECT request_id FROM {settings.table('fact_stock_request')} WHERE request_id = @rid",
        [bq.p("rid", "STRING", request_id)],
    )
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")

    bq.execute(
        f"""
        UPDATE {settings.table('fact_stock_request')} SET
          status = 'REJECTED', notes_spv = @notes, updated_at = @now
        WHERE request_id = @rid
        """,
        [
            bq.p("notes", "STRING",    body.notes_spv),
            bq.p("now",   "TIMESTAMP", now.isoformat()),
            bq.p("rid",   "STRING",    request_id),
        ],
    )
    bq.cache.invalidate("stock:")
    updated = bq.query_one(
        f"SELECT * FROM {settings.table('fact_stock_request')} WHERE request_id = @rid",
        [bq.p("rid", "STRING", request_id)],
    )
    return StockRequestOut(**updated)


@router.get("/requests", response_model=list[StockRequestOut])
def list_stock_requests(
    salesman_sk: str | None = Query(None),
    spv_sk: str | None = Query(None),
    status: str | None = Query(None),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    cache_key = f"stock:requests:{salesman_sk or ''}:{spv_sk or ''}:{status or ''}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    conditions = ["TRUE"]
    params: list = []

    if salesman_sk:
        conditions.append("AND salesman_sk = @sm_sk")
        params.append(bq.p("sm_sk", "STRING", salesman_sk))
    if spv_sk:
        conditions.append("AND spv_sk = @spv_sk")
        params.append(bq.p("spv_sk", "STRING", spv_sk))
    if status:
        conditions.append("AND status = @status")
        params.append(bq.p("status", "STRING", status))

    rows = bq.query(
        f"""
        SELECT * FROM {settings.table('fact_stock_request')}
        WHERE {' '.join(conditions)}
        ORDER BY created_at DESC
        LIMIT 200
        """,
        params,
    )
    result = [StockRequestOut(**r) for r in rows]
    bq.cache.set(cache_key, result, ttl=60)
    return result
