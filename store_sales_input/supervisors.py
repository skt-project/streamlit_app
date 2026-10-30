"""Daftar Nama Supervisor (SPV) untuk dropdown di form input.

Sumber daftar, per store (Kode Toko = `cust_id`):

1. `master_store_database_basis.distributor_tph` → `master_distributor.distributor` → kolom `spv_tph`
   (hanya distributor berstatus Active).
2. Nama SPV yang pernah diketik manual lewat pilihan "Nama SPV Lainnya" dan sudah tersimpan di tabel transaksi,
   dari store mana pun yang distributor TPH-nya sama. Tidak ada tabel baru: nama ikut tersimpan lewat submit.

Nama distributor di `basis` bisa memuat spasi tak terlihat (mis. non-breaking space pada "PT KARYA ANANDA SUKSES"),
jadi join memakai nama yang sudah dinormalisasi (spasi dirapikan + huruf besar).

Fungsi murni (tanpa Streamlit / BigQuery) dipisah dari kelas BigQuery supaya mudah diuji.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from google.cloud import bigquery

log = logging.getLogger("store_sales.supervisors")

SPV_OTHER = "Nama SPV Lainnya"      # pilihan terbawah di dropdown
MAX_NAME_LEN = 100

_WS_RE = re.compile(r"[\s ]+")
_BLANK = {"", "-"}
_FQN_RE = re.compile(r"^[A-Za-z0-9_:.-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$")

# Normalisasi nama distributor di SQL: rapikan spasi (termasuk U+00A0) + huruf besar.
_NORM = r"UPPER(TRIM(REGEXP_REPLACE({col}, r'[\s\x{{A0}}]+', ' ')))"


class SupervisorLookupError(Exception):
    """Daftar SPV gagal dibaca dari BigQuery. Pesan aman ditampilkan ke user."""


def clean_name(value: str | None) -> str:
    """Rapikan spasi (termasuk non-breaking space) di awal/tengah/akhir."""
    return _WS_RE.sub(" ", value or "").strip()


@dataclass(frozen=True)
class SupervisorData:
    master: Mapping[str, tuple[str, ...]] = field(default_factory=dict)   # store_id → SPV dari master distributor
    custom: Mapping[str, tuple[str, ...]] = field(default_factory=dict)   # store_id → nama manual yang pernah dipakai

    def options_for(self, store_id: str | None) -> list[str]:
        if not store_id:
            return [SPV_OTHER]
        return build_options(self.master.get(store_id, ()), self.custom.get(store_id, ()))


def build_options(master_names: Sequence[str], custom_names: Sequence[str]) -> list[str]:
    """Master dulu (A-Z), lalu nama manual (A-Z), tanpa duplikat (huruf besar/kecil dianggap sama);
    "Nama SPV Lainnya" selalu paling bawah."""
    out: list[str] = []
    seen = {SPV_OTHER.casefold()}
    for group in (master_names, custom_names):
        for name in sorted({clean_name(n) for n in group}, key=str.casefold):
            if name in _BLANK or name.casefold() in seen:
                continue
            seen.add(name.casefold())
            out.append(name)
    out.append(SPV_OTHER)
    return out


def resolve_supervisor(choice: str | None, typed: str | None, options: Sequence[str]) -> str:
    """Nama supervisor yang dipakai. String kosong = belum lengkap.

    Bila nama yang diketik sama dengan salah satu pilihan (abaikan huruf besar/kecil), ejaan pilihan itu yang dipakai
    supaya tidak tercipta varian ganda ("riko saputra" → "Riko Saputra").
    """
    if not choice:
        return ""
    if choice != SPV_OTHER:
        return choice
    name = clean_name(typed)[:MAX_NAME_LEN]
    if not name or name.casefold() == SPV_OTHER.casefold():
        return ""
    for opt in options:
        if opt != SPV_OTHER and opt.casefold() == name.casefold():
            return opt
    return name


class SupervisorDirectory:
    def __init__(self, client: bigquery.Client, basis_fqn: str, distributor_fqn: str, transaction_fqn: str):
        for fqn in (basis_fqn, distributor_fqn, transaction_fqn):
            if not _FQN_RE.match(fqn):
                raise SupervisorLookupError("Nama tabel BigQuery tidak valid.")
        self.client = client
        self.basis_fqn, self.distributor_fqn, self.transaction_fqn = basis_fqn, distributor_fqn, transaction_fqn

    # ── Query builders (dipisah agar bisa diuji tanpa koneksi) ──────────────

    def master_sql(self) -> str:
        return f"""SELECT b.cust_id AS store_id, ARRAY_AGG(DISTINCT TRIM(d.spv_tph)) AS names
FROM `{self.basis_fqn}` b
JOIN `{self.distributor_fqn}` d
  ON {_NORM.format(col='d.distributor')} = {_NORM.format(col='b.distributor_tph')}
WHERE b.cust_id IN UNNEST(@ids)
  AND d.status = 'Active'
  AND TRIM(IFNULL(d.spv_tph, '')) NOT IN ('', '-')
GROUP BY b.cust_id"""

    def custom_sql(self) -> str:
        return f"""WITH nb AS (
  SELECT cust_id, {_NORM.format(col='distributor_tph')} AS k
  FROM `{self.basis_fqn}`
  WHERE TRIM(IFNULL(distributor_tph, '')) != ''
)
SELECT m.cust_id AS store_id, ARRAY_AGG(DISTINCT TRIM(t.supervisor_name)) AS names
FROM nb m
JOIN nb o ON o.k = m.k
JOIN `{self.transaction_fqn}` t ON t.store_id = o.cust_id
WHERE m.cust_id IN UNNEST(@ids)
  AND TRIM(IFNULL(t.supervisor_name, '')) != ''
GROUP BY m.cust_id"""

    # ── Operasi ─────────────────────────────────────────────────────────────

    def _names_by_store(self, sql: str, store_ids: Sequence[str]) -> dict[str, tuple[str, ...]]:
        job = self.client.query(sql, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", list(store_ids))]))
        out: dict[str, tuple[str, ...]] = {}
        for row in job.result():
            r: Any = dict(row.items())
            out[str(r["store_id"])] = tuple(n for n in (r["names"] or []) if n)
        return out

    def load(self, store_ids: Sequence[str]) -> SupervisorData:
        ids = sorted({str(s) for s in store_ids})
        if not ids:
            return SupervisorData()
        try:
            return SupervisorData(master=self._names_by_store(self.master_sql(), ids),
                                  custom=self._names_by_store(self.custom_sql(), ids))
        except Exception as exc:
            log.exception("Gagal membaca daftar SPV")
            raise SupervisorLookupError(f"Daftar SPV gagal dimuat dari BigQuery ({exc.__class__.__name__}).") from exc
