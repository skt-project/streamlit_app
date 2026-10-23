"""Master Store/SKU dengan cadangan: bila pembacaan spreadsheet gagal (header berubah, sheet rusak,
API error), aplikasi tetap memakai data master TERAKHIR YANG VALID.

Cadangan disimpan di memori proses dan di file snapshot lokal (`.cache/master_snapshot.json`).
Di Streamlit Cloud, file snapshot hilang bila app di-reboot/redeploy; selama proses hidup,
cadangan di memori tetap dipakai.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from sheets_master import MasterData
from validation import Sku, Store

log = logging.getLogger("store_sales.master_cache")

DEFAULT_SNAPSHOT = Path(__file__).resolve().parent / ".cache" / "master_snapshot.json"
_MEMORY: dict[str, "MasterResult"] = {}
_UNSET = object()      # penanda "pakai DEFAULT_SNAPSHOT" (dibaca saat dipanggil, bisa di-patch di tes)


@dataclass
class MasterResult:
    data: MasterData
    loaded_at: datetime              # kapan data ini berhasil dibaca dari spreadsheet (UTC)
    stale: bool = False              # True = data lama (pembacaan terbaru gagal)
    error: str | None = None         # alasan pembacaan terbaru gagal


def save_snapshot(result: MasterResult, path: Path) -> None:
    payload = {
        "loaded_at": result.loaded_at.isoformat(),
        "stores": [vars(s) for s in result.data.stores.values()],
        "skus": [vars(k) for k in result.data.skus.values()],
        "diagnostics": result.data.diagnostics,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, path)                     # atomik: snapshot tidak pernah setengah tertulis
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def load_snapshot(path: Path) -> MasterResult | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        stores = {s["store_id"]: Store(**s) for s in raw["stores"]}
        skus = {k["code"]: Sku(**k) for k in raw["skus"]}
        if not stores or not skus:
            return None
        return MasterResult(
            data=MasterData(stores=stores, skus=skus, diagnostics=list(raw.get("diagnostics", []))),
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
    """Baca master lewat `loader`. Sukses → simpan sebagai cadangan. Gagal → pakai cadangan terakhir.

    Bila gagal dan belum ada cadangan sama sekali, exception aslinya dilempar ulang.
    """
    mem = _MEMORY if memory is None else memory
    if snapshot_path is _UNSET:
        snapshot_path = DEFAULT_SNAPSHOT
    clock = now or (lambda: datetime.now(timezone.utc))
    try:
        data = loader()
    except Exception as exc:
        log.exception("Master spreadsheet gagal dibaca; mencoba cadangan")
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
