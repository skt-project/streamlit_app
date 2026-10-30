"""Orkestrasi submit: validasi → upload foto (GCS) → MERGE BigQuery → mirror Sheets.

Urutan dipilih agar tidak ada penulisan parsial:
- validasi gagal  → tidak ada panggilan ke storage / BigQuery / Sheets sama sekali;
- upload foto gagal / MERGE BigQuery gagal → tidak ada transaksi tersimpan. Foto yang sudah terlanjur diupload
  TIDAK dihapus (aturan: evidence tidak boleh dihapus tanpa perintah); namanya dicatat di log;
- foto lama yang tergantikan juga tidak dihapus; BigQuery hanya menunjuk ke foto terbaru;
- mirror Sheets gagal → BigQuery sudah benar (source of truth); hasilnya warning, bukan gagal total.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Callable, Mapping, Sequence

from bigquery_store import TransactionRecord
from validation import FinalLine, Issue, Store, make_transaction_id

log = logging.getLogger("store_sales.service")


class SubmissionRejected(Exception):
    """Validasi gagal; tidak ada data yang ditulis."""

    def __init__(self, issues: Sequence[Issue]):
        self.issues = list(issues)
        super().__init__("; ".join(str(i) for i in self.issues))


class SubmissionFailed(Exception):
    """Penyimpanan ke Drive/BigQuery gagal; tidak ada data transaksi yang tersimpan."""


@dataclass
class SavedLine:
    transaction_id: str
    sku_code: str
    sku_name: str
    sell_out_qty: int
    stock_qty: int
    action: str                        # INSERT | UPDATE
    evidence_url: str | None


@dataclass
class SubmitResult:
    lines: list[SavedLine]
    sales_date: date | None = None          # tanggal penjualan (diisi supervisor)
    submitted_at: datetime | None = None    # waktu submit (otomatis, UTC; tampil sebagai WIB)
    bigquery_status: str = "OK"
    sheets_status: str = "OK"
    warnings: list[str] = field(default_factory=list)

    @property
    def inserted(self) -> int:
        return sum(1 for ln in self.lines if ln.action == "INSERT")

    @property
    def updated(self) -> int:
        return sum(1 for ln in self.lines if ln.action == "UPDATE")


def plan_actions(lines: Sequence[FinalLine], existing: Sequence[Mapping]) -> dict[str, str]:
    """Peta sku_code → INSERT/UPDATE berdasarkan transaksi yang sudah ada (tanggal + store sama)."""
    have = {str(r["sku_code"]) for r in existing}
    return {ln.sku_code: ("UPDATE" if ln.sku_code in have else "INSERT") for ln in lines}


class SalesSubmitter:
    def __init__(self, bq, evidence, mirror, clock: Callable[[], datetime] | None = None):
        self.bq = bq
        self.evidence = evidence
        self.mirror = mirror
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def submit(self, *, store: Store, sales_date: date, supervisor_name: str,
               lines: Sequence[FinalLine]) -> SubmitResult:
        """`lines` harus sudah lolos `validate_submission` (dipanggil ulang oleh UI sebelum ini)."""
        if not lines:
            raise SubmissionRejected([Issue(None, "Tidak ada SKU untuk disimpan.")])

        supervisor = supervisor_name.strip()
        now = self.clock()
        try:
            existing = self.bq.fetch_existing(sales_date, store.store_id)
        except Exception as exc:
            raise SubmissionFailed(f"Gagal membaca transaksi lama dari BigQuery: {exc}") from exc
        existing_by_sku = {str(r["sku_code"]): r for r in existing}
        actions = plan_actions(lines, existing)

        # 1) Upload foto baru
        uploaded: dict[str, object] = {}
        try:
            for ln in lines:
                if ln.photo:
                    tx = make_transaction_id(sales_date, store.store_id, ln.sku_code)
                    uploaded[ln.sku_code] = self.evidence.upload(
                        data=ln.photo, sales_date=sales_date, store_id=store.store_id,
                        store_name=store.store_name, sku_code=ln.sku_code, sku_name=ln.sku_name,
                        supervisor_name=supervisor, transaction_id=tx,
                    )
        except Exception as exc:
            self._log_orphans(uploaded)
            raise SubmissionFailed(f"Upload foto evidence gagal; tidak ada data yang disimpan. {exc}") from exc

        # 2) MERGE ke BigQuery (atomik)
        records = []
        for ln in lines:
            ev = uploaded.get(ln.sku_code)
            old = existing_by_sku.get(ln.sku_code)
            records.append(TransactionRecord(
                transaction_id=make_transaction_id(sales_date, store.store_id, ln.sku_code),
                tanggal_penjualan=sales_date,
                store_id=store.store_id, store_name=store.store_name,
                region=store.region, store_group=store.group,
                sku_code=ln.sku_code, sku_name=ln.sku_name,
                sell_out_qty=ln.sell_out_qty, stock_qty=ln.stock_qty,
                supervisor_name=supervisor,
                evidence_file_id=getattr(ev, "file_id", None),
                evidence_file_url=getattr(ev, "url", None),
                evidence_file_name=getattr(ev, "name", None),
                evidence_uploaded_at=getattr(ev, "uploaded_at", None),
                created_at=(old or {}).get("created_at") or now,
                updated_at=now,
            ))
        try:
            self.bq.upsert(records)
        except Exception as exc:
            self._log_orphans(uploaded)
            raise SubmissionFailed(f"Penyimpanan ke BigQuery gagal; tidak ada data yang disimpan. {exc}") from exc

        result = SubmitResult(lines=[], sales_date=sales_date, submitted_at=now)
        # 3) Baca balik dari BigQuery (created_at yang benar) lalu mirror ke Sheets
        saved_rows: list[Mapping] = []
        try:
            saved_rows = self.bq.fetch_by_ids([r.transaction_id for r in records])
        except Exception as exc:
            log.exception("Gagal membaca balik transaksi dari BigQuery")
            result.warnings.append(f"Gagal membaca balik dari BigQuery untuk mirror: {exc}")
        if saved_rows:
            try:
                self.mirror.upsert(saved_rows)
            except Exception as exc:
                log.exception("Mirror Google Sheets gagal setelah BigQuery berhasil")
                result.sheets_status = f"GAGAL: {exc}"
                result.warnings.append(
                    "BigQuery SUDAH berhasil, tetapi reporting mirror Google Sheets gagal diperbarui "
                    f"({exc}). Data aman di BigQuery; mirror akan ikut diperbarui pada submit berikutnya "
                    "untuk key yang sama.")
        else:
            result.sheets_status = "DILEWATI (data BigQuery tidak terbaca)"

        try:
            dups = self.bq.find_duplicate_keys(sales_date, store.store_id)
            if dups:
                result.warnings.append(
                    f"Terdeteksi duplikat key di BigQuery untuk SKU: {', '.join(dups)}. Hubungi admin data.")
        except Exception:
            log.warning("Pengecekan duplikat gagal", exc_info=True)

        result.lines = [
            SavedLine(
                transaction_id=r.transaction_id, sku_code=r.sku_code, sku_name=r.sku_name,
                sell_out_qty=r.sell_out_qty, stock_qty=r.stock_qty,
                action=actions[r.sku_code],
                evidence_url=r.evidence_file_url or (existing_by_sku.get(r.sku_code) or {}).get("evidence_file_url"),
            ) for r in records
        ]
        return result

    @staticmethod
    def _log_orphans(uploaded: Mapping[str, object]) -> None:
        """Foto yang terupload tapi transaksinya tidak tersimpan: dibiarkan, hanya dicatat."""
        for ev in uploaded.values():
            log.warning("Foto yatim dibiarkan di storage (tidak dihapus): %s", getattr(ev, "file_id", ev))
