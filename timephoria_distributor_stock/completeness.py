"""Kelengkapan input per region / per distributor untuk rentang tanggal stock (tab Admin).

Satu kunjungan = satu tanggal stock yang punya data untuk distributor itu.

- Lengkap: minimal satu kunjungan di mana semua SKU terisi, dan (total stock > 0 → ada ≥ 1 foto;
  total 0 → foto tidak dibutuhkan).
- Belum lengkap: ada data, tetapi tidak ada kunjungan yang lengkap.
- Belum submit: tidak ada baris pada rentang itu.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Sequence

from validation import Distributor, Sku

NO_REGION = "(Tanpa Region)"
STATUS_COMPLETE = "Lengkap"
STATUS_PARTIAL = "Belum lengkap"
STATUS_NONE = "Belum submit"
STATUSES = (STATUS_COMPLETE, STATUS_PARTIAL, STATUS_NONE)


def region_of(distributor: Distributor) -> str:
    return distributor.region or NO_REGION


def _d(value: date | None) -> date:
    return value or date.min


@dataclass(frozen=True)
class DistributorStatus:
    distributor: Distributor
    region: str
    visits: int
    last_visit: date | None
    best_date: date | None
    sku_filled: int
    sku_total: int
    missing_skus: tuple[str, ...]
    photo_count: int
    supervisor: str | None
    last_submit: datetime | None
    status: str


@dataclass(frozen=True)
class EvidenceItem:
    region: str
    distributor_code: str
    distributor_name: str
    tanggal_stock: date | None
    spu_code: str
    spu_name: str
    stock_qty: int | None
    supervisor: str | None
    updated_at: datetime | None
    file_id: str
    file_name: str | None


def _filled(row: Mapping[str, Any] | None) -> bool:
    return row is not None and row.get("stock_qty") is not None


def _visit_complete(by_sku: Mapping[str, Mapping[str, Any]], skus: Mapping[str, Sku]) -> bool:
    if any(not _filled(by_sku.get(code)) for code in skus):
        return False
    total = sum(int(by_sku[code]["stock_qty"]) for code in skus)
    photos = sum(1 for code in skus if by_sku[code].get("evidence_file_id"))
    if total > 0:
        return photos >= 1
    return True


def compute_completeness(
    distributors: Mapping[str, Distributor], skus: Mapping[str, Sku], rows: Sequence[Mapping[str, Any]],
) -> list[DistributorStatus]:
    by_dist: dict[str, dict[date | None, dict[str, Mapping[str, Any]]]] = defaultdict(lambda: defaultdict(dict))
    for row in rows:
        if row.get("distributor_code") in distributors and row.get("spu_code") in skus:
            by_dist[row["distributor_code"]][row.get("tanggal_stock")][row["spu_code"]] = row

    out: list[DistributorStatus] = []
    for distributor in distributors.values():
        visits = by_dist.get(distributor.distributor_code, {})
        stats = {}
        for day, by_sku in visits.items():
            filled = [code for code in skus if _filled(by_sku.get(code))]
            photos = sum(1 for row in by_sku.values() if row.get("evidence_file_id"))
            stats[day] = (len(filled), photos, tuple(code for code in skus if code not in filled), _visit_complete(by_sku, skus))
        complete = [day for day, info in stats.items() if info[3]]
        if not stats:
            status, best = STATUS_NONE, None
        elif complete:
            status, best = STATUS_COMPLETE, max(complete, key=_d)
        else:
            status, best = STATUS_PARTIAL, max(stats, key=lambda day: (stats[day][0], _d(day)))
        all_rows = [row for by_sku in visits.values() for row in by_sku.values()]
        latest = max(all_rows, key=lambda row: row.get("updated_at") or datetime.min, default=None)
        out.append(DistributorStatus(
            distributor=distributor, region=region_of(distributor), visits=len(stats),
            last_visit=max(stats, key=_d) if stats else None, best_date=best,
            sku_filled=stats[best][0] if stats else 0, sku_total=len(skus),
            missing_skus=stats[best][2] if stats else tuple(skus),
            photo_count=sum(info[1] for info in stats.values()),
            supervisor=(latest or {}).get("supervisor_name"),
            last_submit=(latest or {}).get("updated_at"),
            status=status,
        ))
    out.sort(key=lambda s: (s.region, s.distributor.distributor_name.casefold(), s.distributor.distributor_code))
    return out


def summarize_by_region(statuses: Sequence[DistributorStatus]) -> list[dict[str, Any]]:
    agg: dict[str, dict[str, int]] = {}
    for status in statuses:
        bucket = agg.setdefault(status.region, {STATUS_COMPLETE: 0, STATUS_PARTIAL: 0, STATUS_NONE: 0})
        bucket[status.status] += 1
    rows = []
    for region in sorted(agg):
        bucket = agg[region]
        total = sum(bucket.values())
        rows.append({
            "Region": region, "Total": total,
            STATUS_COMPLETE: bucket[STATUS_COMPLETE],
            STATUS_PARTIAL: bucket[STATUS_PARTIAL],
            STATUS_NONE: bucket[STATUS_NONE],
            "% Lengkap": round(100 * bucket[STATUS_COMPLETE] / total, 1) if total else 0.0,
        })
    return rows


def evidence_items(
    distributors: Mapping[str, Distributor], skus: Mapping[str, Sku], rows: Sequence[Mapping[str, Any]],
) -> list[EvidenceItem]:
    """Satu item per baris yang punya foto (pointer terbaru per distributor + SPU + tanggal)."""
    items = []
    for row in rows:
        distributor = distributors.get(row.get("distributor_code"))
        if distributor is None or not row.get("evidence_file_id"):
            continue
        sku = skus.get(row.get("spu_code"))
        items.append(EvidenceItem(
            region=region_of(distributor),
            distributor_code=distributor.distributor_code,
            distributor_name=distributor.distributor_name,
            tanggal_stock=row.get("tanggal_stock"),
            spu_code=row.get("spu_code") or "",
            spu_name=sku.name if sku else (row.get("spu_name") or ""),
            stock_qty=row.get("stock_qty"),
            supervisor=row.get("supervisor_name"),
            updated_at=row.get("updated_at"),
            file_id=row["evidence_file_id"],
            file_name=row.get("evidence_file_name"),
        ))
    items.sort(key=lambda item: (
        item.region, item.distributor_name.casefold(), item.distributor_code,
        _d(item.tanggal_stock), item.spu_code,
    ))
    return items


def compare_sku(current: int | None, previous: int | None) -> dict[str, Any]:
    """Selisih dan % terhadap submit sebelumnya. Sebelumnya 0 atau tidak ada → perubahan "baru"."""
    if current is None or previous is None:
        return {
            "sekarang": "" if current is None else current,
            "sebelumnya": "" if previous is None else previous,
            "selisih": "baru",
            "perubahan": "baru",
        }
    delta = current - previous
    perubahan = "baru" if previous == 0 else f"{(100.0 * delta / previous):.1f}%"
    return {"sekarang": current, "sebelumnya": previous, "selisih": delta, "perubahan": perubahan}


def change_marker(perubahan: Any, threshold: float) -> str:
    text = str(perubahan)
    if not text.endswith("%"):
        return ""
    try:
        pct = float(text[:-1])
    except ValueError:
        return ""
    return "⚠️" if abs(pct) >= threshold else ""
