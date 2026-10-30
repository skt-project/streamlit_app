"""Kelengkapan input per region / per store untuk RENTANG tanggal penjualan (dipakai tab Admin).

Satu "kunjungan" = satu tanggal penjualan yang punya data untuk store itu. Store biasanya dikunjungi seminggu
sekali, jadi status dihitung dari seluruh kunjungan dalam rentang:

- Lengkap       : minimal SATU kunjungan di mana semua SKU master terisi (Sell Out + Stock). Foto tidak mempengaruhi status.
- Belum lengkap : ada data, tetapi tidak ada satu pun kunjungan yang lengkap.
- Belum submit  : tidak ada satu pun baris pada rentang itu.

Hanya store Non BA dari master yang dihitung; baris untuk store/SKU di luar master diabaikan.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Sequence

from validation import Sku, Store

NO_REGION = "(Tanpa Region)"
STATUS_COMPLETE = "Lengkap"
STATUS_PARTIAL = "Belum lengkap"
STATUS_NONE = "Belum submit"
STATUSES = (STATUS_COMPLETE, STATUS_PARTIAL, STATUS_NONE)


def region_of(store: Store) -> str:
    return store.region or NO_REGION


def _d(value: date | None) -> date:
    """Kunci pengurutan yang aman untuk tanggal kosong."""
    return value or date.min


@dataclass(frozen=True)
class StoreStatus:
    store: Store
    region: str
    visits: int                              # jumlah tanggal penjualan yang punya data dalam rentang
    last_visit: date | None                  # tanggal kunjungan terakhir
    best_date: date | None                   # kunjungan terbaik: lengkap terbaru, atau SKU terisi terbanyak
    sku_filled: int                          # pada kunjungan terbaik
    sku_total: int
    missing_skus: tuple[str, ...]            # SKU belum terisi pada kunjungan terbaik
    photo_count: int                         # total foto di seluruh kunjungan dalam rentang
    supervisor: str | None                   # supervisor pada submit terakhir
    last_submit: datetime | None             # updated_at terbaru (UTC)
    status: str


@dataclass(frozen=True)
class EvidenceItem:
    region: str
    store_id: str
    store_name: str
    sales_date: date | None
    sku_code: str
    sku_name: str
    sell_out_qty: int | None
    stock_qty: int | None
    supervisor: str | None
    updated_at: datetime | None
    file_id: str
    file_name: str | None


def _filled(r: Mapping[str, Any] | None) -> bool:
    return r is not None and r.get("sell_out_qty") is not None and r.get("stock_qty") is not None


def compute_completeness(
    stores: Mapping[str, Store], skus: Mapping[str, Sku], rows: Sequence[Mapping[str, Any]],
) -> list[StoreStatus]:
    by_store: dict[str, dict[date | None, dict[str, Mapping[str, Any]]]] = defaultdict(lambda: defaultdict(dict))
    for r in rows:
        if r["store_id"] in stores and r["sku_code"] in skus:
            by_store[r["store_id"]][r.get("tanggal_penjualan")][r["sku_code"]] = r   # key unik → tanpa duplikat

    out: list[StoreStatus] = []
    for store in stores.values():
        visits = by_store.get(store.store_id, {})
        stats = {}                                           # tanggal → (terisi, foto, kode_belum_terisi)
        for d, by_sku in visits.items():
            filled = [c for c in skus if _filled(by_sku.get(c))]
            photos = sum(1 for r in by_sku.values() if r.get("evidence_file_id"))
            stats[d] = (len(filled), photos, tuple(c for c in skus if c not in filled))
        complete = [d for d, (_, _photos, missing) in stats.items() if not missing]
        if not stats:
            status, best = STATUS_NONE, None
        elif complete:
            status, best = STATUS_COMPLETE, max(complete, key=_d)
        else:
            status, best = STATUS_PARTIAL, max(stats, key=lambda d: (stats[d][0], _d(d)))
        all_rows = [r for by_sku in visits.values() for r in by_sku.values()]
        latest = max(all_rows, key=lambda r: r.get("updated_at") or datetime.min, default=None)
        out.append(StoreStatus(
            store=store, region=region_of(store), visits=len(stats),
            last_visit=max(stats, key=_d) if stats else None, best_date=best,
            sku_filled=stats[best][0] if stats else 0, sku_total=len(skus),
            missing_skus=stats[best][2] if stats else tuple(skus),
            photo_count=sum(s[1] for s in stats.values()),
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
    """Satu item per baris yang punya foto (foto TERBARU per store+SKU+tanggal; foto lama tetap di bucket)."""
    items = []
    for r in rows:
        store = stores.get(r["store_id"])
        if store is None or not r.get("evidence_file_id"):
            continue
        sku = skus.get(r["sku_code"])
        items.append(EvidenceItem(
            region=region_of(store), store_id=store.store_id, store_name=store.store_name,
            sales_date=r.get("tanggal_penjualan"),
            sku_code=r["sku_code"], sku_name=sku.name if sku else (r.get("sku_name") or ""),
            sell_out_qty=r.get("sell_out_qty"), stock_qty=r.get("stock_qty"),
            supervisor=r.get("supervisor_name"), updated_at=r.get("updated_at"),
            file_id=r["evidence_file_id"], file_name=r.get("evidence_file_name"),
        ))
    items.sort(key=lambda i: (i.region, i.store_name.casefold(), i.store_id, _d(i.sales_date), i.sku_code))
    return items
