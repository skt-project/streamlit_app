"""Kelengkapan input per region / per store untuk satu tanggal penjualan (dipakai tab Admin).

Aturan "Lengkap": SEMUA SKU di master terisi (Sell Out + Stock) DAN minimal satu foto evidence.
- Belum submit  : tidak ada satu pun baris untuk store itu pada tanggal tsb.
- Belum lengkap : ada baris, tetapi ada SKU belum terisi dan/atau belum ada foto.
Hanya store Non BA dari master yang dihitung; baris untuk store/SKU di luar master diabaikan.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Sequence

from validation import Sku, Store

NO_REGION = "(Tanpa Region)"
STATUS_COMPLETE = "Lengkap"
STATUS_PARTIAL = "Belum lengkap"
STATUS_NONE = "Belum submit"
STATUSES = (STATUS_COMPLETE, STATUS_PARTIAL, STATUS_NONE)


def region_of(store: Store) -> str:
    return store.region or NO_REGION


@dataclass(frozen=True)
class StoreStatus:
    store: Store
    region: str
    sku_filled: int
    sku_total: int
    photo_count: int
    missing_skus: tuple[str, ...]            # kode SKU yang belum terisi
    supervisor: str | None
    last_submit: datetime | None             # updated_at terbaru (UTC)
    status: str


@dataclass(frozen=True)
class EvidenceItem:
    region: str
    store_id: str
    store_name: str
    sku_code: str
    sku_name: str
    sell_out_qty: int | None
    stock_qty: int | None
    supervisor: str | None
    updated_at: datetime | None
    file_id: str
    file_name: str | None


def compute_completeness(
    stores: Mapping[str, Store], skus: Mapping[str, Sku], rows: Sequence[Mapping[str, Any]],
) -> list[StoreStatus]:
    by_store: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for r in rows:
        if r["store_id"] in stores and r["sku_code"] in skus:
            by_store[r["store_id"]][r["sku_code"]] = r        # key unik, jadi tidak ada duplikat

    out: list[StoreStatus] = []
    for store in stores.values():
        srows = by_store.get(store.store_id, {})
        filled = [c for c in skus if c in srows and srows[c].get("sell_out_qty") is not None
                  and srows[c].get("stock_qty") is not None]
        photos = sum(1 for r in srows.values() if r.get("evidence_file_id"))
        missing = tuple(c for c in skus if c not in filled)
        if not srows:
            status = STATUS_NONE
        elif not missing and photos >= 1:
            status = STATUS_COMPLETE
        else:
            status = STATUS_PARTIAL
        latest = max(srows.values(), key=lambda r: r.get("updated_at") or datetime.min, default=None)
        out.append(StoreStatus(
            store=store, region=region_of(store), sku_filled=len(filled), sku_total=len(skus),
            photo_count=photos, missing_skus=missing,
            supervisor=(latest or {}).get("supervisor_name"), last_submit=(latest or {}).get("updated_at"),
            status=status,
        ))
    out.sort(key=lambda s: (s.region, s.store.store_name.casefold(), s.store.store_id))
    return out


def summarize_by_region(statuses: Sequence[StoreStatus]) -> list[dict[str, Any]]:
    agg: dict[str, dict[str, int]] = {}
    for s in statuses:
        a = agg.setdefault(s.region, {STATUS_COMPLETE: 0, STATUS_PARTIAL: 0, STATUS_NONE: 0})
        a[s.status] += 1
    rows = []
    for region in sorted(agg):
        a = agg[region]
        total = sum(a.values())
        rows.append({
            "Region": region, "Total Store": total,
            STATUS_COMPLETE: a[STATUS_COMPLETE], STATUS_PARTIAL: a[STATUS_PARTIAL], STATUS_NONE: a[STATUS_NONE],
            "% Lengkap": round(100 * a[STATUS_COMPLETE] / total, 1) if total else 0.0,
        })
    return rows


def evidence_items(stores: Mapping[str, Store], skus: Mapping[str, Sku],
                   rows: Sequence[Mapping[str, Any]]) -> list[EvidenceItem]:
    """Satu item per baris yang punya foto (foto TERBARU per store+SKU; foto lama tetap di bucket)."""
    items = []
    for r in rows:
        store = stores.get(r["store_id"])
        if store is None or not r.get("evidence_file_id"):
            continue
        sku = skus.get(r["sku_code"])
        items.append(EvidenceItem(
            region=region_of(store), store_id=store.store_id, store_name=store.store_name,
            sku_code=r["sku_code"], sku_name=sku.name if sku else (r.get("sku_name") or ""),
            sell_out_qty=r.get("sell_out_qty"), stock_qty=r.get("stock_qty"),
            supervisor=r.get("supervisor_name"), updated_at=r.get("updated_at"),
            file_id=r["evidence_file_id"], file_name=r.get("evidence_file_name"),
        ))
    items.sort(key=lambda i: (i.region, i.store_name.casefold(), i.store_id, i.sku_code))
    return items
