"""
GET  /salesman               — paginated list with filters
GET  /salesman/{sk}          — single salesman detail
POST /salesman               — create (writes dim_salesman + audit_log)
PUT  /salesman/{sk}          — update
DELETE /salesman/{sk}        — soft-deactivate (is_active = FALSE)
"""
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from google.cloud import bigquery

from config import settings
from dependencies import brand_group_filter, require_auth, require_role
from models.auth import UserContext
from models.salesman import (
    SalesmanCreateRequest,
    SalesmanListResponse,
    SalesmanOut,
    SalesmanUpdateRequest,
)
from services.bq import BQClient

router = APIRouter(prefix="/salesman", tags=["salesman"])

_SALESMAN_COLS = """
    salesman_sk, source_salesman_code, salesman_name, salesman_type, role_type,
    distributor_code, region, spv_name, asm_name, is_active, brand_group, source_updated_at
"""


def _build_scope_filter(user: UserContext) -> tuple[str, list]:
    """Return (WHERE clause fragment, params) for brand_group + role scoping."""
    bq = BQClient.get()
    bg_clause, bg_params = brand_group_filter(user, "bg_sm")
    role_clause, role_params = "", []
    if user.role == "dm" and user.distributor_code:
        role_clause = "AND distributor_code = @scope_dist"
        role_params = [bq.p("scope_dist", "STRING", user.distributor_code)]
    elif user.role in ("spv", "asm") and user.territory:
        role_clause = "AND region = @scope_region"
        role_params = [bq.p("scope_region", "STRING", user.territory)]
    return f"{bg_clause} {role_clause}".strip(), bg_params + role_params


@router.get("", response_model=SalesmanListResponse)
def list_salesman(
    distributor_code: str | None = Query(None),
    region: str | None = Query(None),
    salesman_type: str | None = Query(None),
    is_active: bool | None = Query(None),
    q: str | None = Query(None, description="Search by name or ID"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    scope_clause, scope_params = _build_scope_filter(current_user)

    conditions = [f"TRUE {scope_clause}"]
    params: list = list(scope_params)

    if distributor_code:
        conditions.append("AND distributor_code = @dist")
        params.append(bq.p("dist", "STRING", distributor_code))
    if region:
        conditions.append("AND region = @region")
        params.append(bq.p("region", "STRING", region))
    if salesman_type:
        conditions.append("AND salesman_type = @stype")
        params.append(bq.p("stype", "STRING", salesman_type))
    if is_active is not None:
        conditions.append("AND is_active = @is_active")
        params.append(bq.p("is_active", "BOOL", is_active))
    if q:
        conditions.append("AND (LOWER(salesman_name) LIKE @q OR LOWER(source_salesman_code) LIKE @q)")
        params.append(bq.p("q", "STRING", f"%{q.lower()}%"))

    where = " ".join(conditions)
    offset = (page - 1) * page_size

    cache_key = (
        f"salesman:list:{current_user.brand_group or 'all'}:{current_user.role}:"
        f"{distributor_code or ''}:{region or ''}:{salesman_type or ''}:"
        f"{is_active}:{q or ''}:{page}:{page_size}"
    )
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    count_row = bq.query_one(
        f"SELECT COUNT(*) AS n FROM {settings.table('vw_salesman_active')} WHERE {where}",
        params,
    )
    total = count_row["n"] if count_row else 0

    rows = bq.query(
        f"""
        SELECT {_SALESMAN_COLS}
        FROM {settings.table('vw_salesman_active')}
        WHERE {where}
        ORDER BY salesman_name
        LIMIT @lim OFFSET @off
        """,
        params + [bq.p("lim", "INT64", page_size), bq.p("off", "INT64", offset)],
    )

    result = SalesmanListResponse(
        items=[SalesmanOut(**r) for r in rows],
        total=total,
        page=page,
        page_size=page_size,
        has_next=(offset + page_size) < total,
    )
    bq.cache.set(cache_key, result, ttl=120)
    return result


@router.get("/{salesman_sk}", response_model=SalesmanOut)
def get_salesman(
    salesman_sk: str,
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    cache_key = f"salesman:{salesman_sk}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached
    row = bq.query_one(
        f"""
        SELECT {_SALESMAN_COLS}
        FROM {settings.table('dim_salesman')}
        WHERE salesman_sk = @sk AND is_deleted = FALSE
        """,
        [bq.p("sk", "STRING", salesman_sk)],
    )
    if not row:
        raise HTTPException(status_code=404, detail="Salesman not found")
    result = SalesmanOut(**row)
    bq.cache.set(cache_key, result, ttl=120)
    return result


@router.post("", status_code=201, response_model=SalesmanOut)
def create_salesman(
    body: SalesmanCreateRequest,
    current_user: UserContext = Depends(require_role("ho_admin", "dm")),
):
    bq = BQClient.get()
    sk = f"STEP-{body.source_salesman_code}"  # simplified key; real would use fn_surrogate_key
    now = datetime.now(timezone.utc)

    bq.execute(
        f"""
        INSERT INTO {settings.table('dim_salesman')} (
          salesman_sk, source_system, source_salesman_code, salesman_name, salesman_type,
          role_type, distributor_code, region, spv_name, asm_name, is_active, sfa_step_loaded_at, is_deleted
        ) VALUES (
          @sk, 'STEP', @code, @name, @stype, @rtype,
          @dist, @region, @spv, @asm, TRUE, CURRENT_TIMESTAMP(), FALSE
        )
        """,
        [
            bq.p("sk", "STRING", sk),
            bq.p("code", "STRING", body.source_salesman_code),
            bq.p("name", "STRING", body.salesman_name),
            bq.p("stype", "STRING", body.salesman_type),
            bq.p("rtype", "STRING", body.role_type),
            bq.p("dist", "STRING", body.distributor_code),
            bq.p("region", "STRING", body.region),
            bq.p("spv", "STRING", body.spv_name),
            bq.p("asm", "STRING", body.asm_name),
        ],
    )

    bq.insert_rows(
        "audit_log",
        [{
            "event_id": str(uuid.uuid4()),
            "event_ts": now.isoformat(),
            "event_date": now.date().isoformat(),
            "dist_code": body.distributor_code or "",
            "session_id": current_user.user_id[:8],
            "entity_type": "SALESMAN",
            "action": "INSERT",
            "entity_id": sk,
            "payload_json": f'{{"salesman_name":"{body.salesman_name}","created_by":"{current_user.username}"}}',
        }]
    )

    bq.cache.invalidate("salesman:")
    return SalesmanOut(
        salesman_sk=sk,
        source_salesman_code=body.source_salesman_code,
        salesman_name=body.salesman_name,
        salesman_type=body.salesman_type,
        role_type=body.role_type,
        distributor_code=body.distributor_code,
        region=body.region,
        spv_name=body.spv_name,
        asm_name=body.asm_name,
        is_active=True,
    )


@router.put("/{salesman_sk}", response_model=SalesmanOut)
def update_salesman(
    salesman_sk: str,
    body: SalesmanUpdateRequest,
    current_user: UserContext = Depends(require_role("ho_admin", "dm")),
):
    bq = BQClient.get()
    existing = bq.query_one(
        f"SELECT {_SALESMAN_COLS} FROM {settings.table('dim_salesman')} WHERE salesman_sk = @sk AND is_deleted = FALSE",
        [bq.p("sk", "STRING", salesman_sk)],
    )
    if not existing:
        raise HTTPException(status_code=404, detail="Salesman not found")

    updates = {k: v for k, v in body.model_dump(exclude_none=True).items()}
    if not updates:
        return SalesmanOut(**existing)

    set_clauses = ", ".join(f"{col} = @{col}" for col in updates)
    params = [bq.p(col, _bq_type(val), val) for col, val in updates.items()]
    params.append(bq.p("sk", "STRING", salesman_sk))

    bq.execute(
        f"""
        UPDATE {settings.table('dim_salesman')}
        SET {set_clauses}, sfa_step_loaded_at = CURRENT_TIMESTAMP()
        WHERE salesman_sk = @sk
        """,
        params,
    )
    bq.cache.invalidate("salesman:")
    return get_salesman(salesman_sk, current_user)


@router.delete("/{salesman_sk}", status_code=204)
def deactivate_salesman(
    salesman_sk: str,
    current_user: UserContext = Depends(require_role("ho_admin", "dm")),
):
    bq = BQClient.get()
    bq.execute(
        f"""
        UPDATE {settings.table('dim_salesman')}
        SET is_active = FALSE, sfa_step_loaded_at = CURRENT_TIMESTAMP()
        WHERE salesman_sk = @sk AND is_deleted = FALSE
        """,
        [bq.p("sk", "STRING", salesman_sk)],
    )
    bq.cache.invalidate("salesman:")


def _bq_type(value) -> str:
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    return "STRING"
