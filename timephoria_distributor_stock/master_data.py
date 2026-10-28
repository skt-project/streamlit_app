"""Master distributor (BigQuery, hanya baca) dan daftar SKU, dengan cadangan data terakhir yang valid.

Cadangan: memori proses + snapshot JSON atomik di `.cache/`. Di Streamlit Cloud file snapshot
hilang saat app di-reboot; selama proses hidup, cadangan di memori tetap dipakai.
Aplikasi tidak pernah menulis ke tabel master.
"""
from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from google.cloud import bigquery

from validation import Distributor, Sku

log = logging.getLogger("timephoria.master")

NO_REGION = "(Tanpa Region)"
_FQN_RE = re.compile(r"^[A-Za-z0-9_:.-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$")

DISTRIBUTOR_SQL = """
SELECT TRIM(distributor_code) AS distributor_code, TRIM(distributor) AS distributor,
       TRIM(region) AS region, TRIM(spv_tph) AS spv
FROM `{fqn}`
WHERE status = 'Active' AND TRIM(IFNULL(spv_tph, '')) NOT IN ('', '-')
  AND IFNULL(brand, '') LIKE '%TPH%'
"""

SKU_SQL = """
SELECT TRIM(sku) AS sku, TRIM(product_name) AS product_name
FROM `{fqn}`
WHERE sku IN UNNEST(@codes)
"""


class MasterError(Exception):
    """Master tidak dapat dibaca. Pesan aman ditampilkan ke user."""


@dataclass
class MasterData:
    distributors: dict[str, Distributor]
    skus: dict[str, Sku]
    diagnostics: list[str] = field(default_factory=list)


@dataclass
class MasterResult:
    data: MasterData
    loaded_at: datetime
    stale: bool = False
    error: str | None = None


def qualified(project: str, dataset: str, table: str) -> str:
    fqn = f"{project}.{dataset}.{table}"
    if not _FQN_RE.match(fqn):
        raise MasterError("Nama tabel master tidak valid.")
    return fqn


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def distributors_from_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, Distributor]:
    """Hanya baris dengan `spv` terisi dan bukan `-`. Region kosong → `(Tanpa Region)`."""
    out: dict[str, Distributor] = {}
    for row in rows:
        code = _text(row.get("distributor_code"))
        spv = _text(row.get("spv"))
        if not code or not spv or spv == "-" or code in out:
            continue
        region = _text(row.get("region")) or NO_REGION
        out[code] = Distributor(code, _text(row.get("distributor")), region, spv)
    return out


def skus_in_order(codes: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Sku], list[str]]:
    """Urutan tampil = urutan `codes`. Kode yang tidak ada di master tetap tampil, nama kosong."""
    found: dict[str, str] = {}
    for row in rows:
        code = _text(row.get("sku"))
        if code and code not in found:
            found[code] = _text(row.get("product_name"))
    skus: dict[str, Sku] = {}
    missing: list[str] = []
    for code in codes:
        code = _text(code)
        if not code or code in skus:
            continue
        name = found.get(code)
        if name is None:
            missing.append(code)
            skus[code] = Sku(code, "")
        else:
            skus[code] = Sku(code, name)
    return skus, missing


def list_regions(distributors: Mapping[str, Distributor]) -> list[str]:
    return sorted({d.region for d in distributors.values()})


def list_spvs(distributors: Mapping[str, Distributor], region: str) -> list[str]:
    return sorted({d.spv for d in distributors.values() if d.region == region})


def list_distributors(distributors: Mapping[str, Distributor], region: str, spv: str) -> list[Distributor]:
    rows = [d for d in distributors.values() if d.region == region and d.spv == spv]
    return sorted(rows, key=lambda d: d.label)


def load_master(client, project: str, dataset: str, sku_codes: Sequence[str]) -> MasterData:
    """Baca distributor Active (spv_tph terisi, brand memuat TPH) dan nama SKU."""
    codes = [c.strip() for c in sku_codes if c and str(c).strip()]
    if not codes:
        raise MasterError("Daftar sku_codes di secrets kosong.")
    dist_fqn = qualified(project, dataset, "master_distributor")
    sku_fqn = qualified(project, dataset, "master_product")
    try:
        dist_job = client.query(DISTRIBUTOR_SQL.format(fqn=dist_fqn))
        dist_rows = [dict(r.items()) for r in dist_job.result()]
        sku_job = client.query(
            SKU_SQL.format(fqn=sku_fqn),
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("codes", "STRING", codes),
            ]),
        )
        sku_rows = [dict(r.items()) for r in sku_job.result()]
    except MasterError:
        raise
    except Exception as exc:
        log.exception("Master BigQuery gagal dibaca")
        raise MasterError(f"Master distributor/SKU gagal dibaca ({exc.__class__.__name__}).") from exc

    distributors = distributors_from_rows(dist_rows)
    if not distributors:
        raise MasterError("Master distributor kosong: tidak ada distributor Active dengan supervisor terisi.")
    skus, missing = skus_in_order(codes, sku_rows)
    notes = [
        f"Distributor: {len(distributors)}. Supervisor: {len({d.spv for d in distributors.values()})}."
        f" Region: {len(list_regions(distributors))}."
    ]
    if missing:
        notes.append("SKU tanpa nama di master produk (tetap ditampilkan): " + ", ".join(missing))
    return MasterData(distributors=distributors, skus=skus, diagnostics=notes)


DEFAULT_SNAPSHOT = Path(__file__).resolve().parent / ".cache" / "master_snapshot.json"
_MEMORY: dict[str, MasterResult] = {}
_UNSET = object()


def save_snapshot(result: MasterResult, path: Path) -> None:
    payload = {
        "loaded_at": result.loaded_at.isoformat(),
        "distributors": [
            {
                "distributor_code": d.distributor_code,
                "distributor_name": d.distributor_name,
                "region": d.region,
                "spv": d.spv,
            }
            for d in result.data.distributors.values()
        ],
        "skus": [{"code": s.code, "name": s.name} for s in result.data.skus.values()],
        "diagnostics": result.data.diagnostics,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def load_snapshot(path: Path) -> MasterResult | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        distributors = {
            d["distributor_code"]: Distributor(
                d["distributor_code"], d["distributor_name"], d["region"], d["spv"],
            )
            for d in raw["distributors"]
        }
        skus = {s["code"]: Sku(s["code"], s["name"]) for s in raw["skus"]}
        if not distributors or not skus:
            return None
        return MasterResult(
            data=MasterData(distributors=distributors, skus=skus, diagnostics=list(raw.get("diagnostics", []))),
            loaded_at=datetime.fromisoformat(raw["loaded_at"]),
        )
    except FileNotFoundError:
        return None
    except Exception:
        log.warning("Snapshot master tidak terbaca: %s", path, exc_info=True)
        return None


def load_master_with_fallback(
    loader: Callable[[], MasterData],
    *,
    key: str = "default",
    snapshot_path: Path | None | object = _UNSET,
    memory: dict[str, MasterResult] | None = None,
    now: Callable[[], datetime] | None = None,
) -> MasterResult:
    """Sukses → simpan cadangan. Gagal → pakai cadangan terakhir. Tanpa cadangan → lempar ulang."""
    mem = _MEMORY if memory is None else memory
    if snapshot_path is _UNSET:
        snapshot_path = DEFAULT_SNAPSHOT
    clock = now or (lambda: datetime.now(timezone.utc))
    try:
        data = loader()
    except Exception as exc:
        log.exception("Master gagal dibaca; mencoba cadangan")
        previous = mem.get(key) or (load_snapshot(snapshot_path) if snapshot_path else None)
        if previous is None:
            raise
        mem[key] = previous
        return MasterResult(data=previous.data, loaded_at=previous.loaded_at, stale=True, error=str(exc))
    result = MasterResult(data=data, loaded_at=clock())
    mem[key] = result
    if snapshot_path:
        try:
            save_snapshot(result, snapshot_path)
        except OSError:
            log.warning("Snapshot master tidak bisa ditulis", exc_info=True)
    return result
