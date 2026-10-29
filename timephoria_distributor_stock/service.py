"""Orkestrasi submit: validasi → upload foto (GCS) → MERGE BigQuery → mirror Sheets.

- validasi gagal → tidak ada panggilan ke storage / BigQuery / Sheets;
- upload atau MERGE gagal → tidak ada transaksi tersimpan. Foto yang sudah terupload dibiarkan;
- mirror Sheets gagal setelah BigQuery berhasil → peringatan, bukan gagal total.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Callable, Mapping, Sequence

from bigquery_store import TransactionRecord
from validation import PHOTO_REQUIRED_MSG, Distributor, FinalLine, Issue, make_transaction_id

log = logging.getLogger("timephoria.service")


class SubmissionRejected(Exception):
    """Validasi gagal; tidak ada data yang ditulis."""

    def __init__(self, issues: Sequence[Issue]):
        self.issues = list(issues)
        super().__init__("; ".join(str(i) for i in self.issues))


class SubmissionFailed(Exception):
    """Penyimpanan ke GCS/BigQuery gagal; tidak ada transaksi yang tersimpan."""


@dataclass
class SavedLine:
    transaction_id: str
    spu_code: str
    spu_name: str
    stock_qty: int
    action: str
    evidence_url: str | None


@dataclass
class SubmitResult:
    lines: list[SavedLine]
    tanggal_stock: date | None = None
    submitted_at: datetime | None = None
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
    have = {str(r["spu_code"]) for r in existing}
    return {ln.sku_code: ("UPDATE" if ln.sku_code in have else "INSERT") for ln in lines}


class SalesSubmitter:
    def __init__(self, bq, evidence, mirror, clock: Callable[[], datetime] | None = None):
        self.bq = bq
        self.evidence = evidence
        self.mirror = mirror
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def submit(
        self, *, distributor: Distributor, tanggal_stock: date, supervisor_name: str,
        lines: Sequence[FinalLine],
    ) -> SubmitResult:
        if not lines:
            raise SubmissionRejected([Issue(None, "Tidak ada SPU untuk disimpan.")])
        if sum(ln.stock_qty for ln in lines) > 0 and not any(ln.photo for ln in lines):
            raise SubmissionRejected([Issue(None, PHOTO_REQUIRED_MSG)])

        supervisor = supervisor_name.strip()
        now = self.clock()
        try:
            existing = self.bq.fetch_existing(tanggal_stock, distributor.distributor_code)
        except Exception as exc:
            raise SubmissionFailed(f"Gagal membaca stok lama dari BigQuery: {exc}") from exc
        existing_by_code = {str(r["spu_code"]): r for r in existing}
        actions = plan_actions(lines, existing)

        uploaded: dict[str, object] = {}
        try:
            for ln in lines:
                if ln.photo:
                    tx = make_transaction_id(tanggal_stock, distributor.distributor_code, ln.sku_code)
                    uploaded[ln.sku_code] = self.evidence.upload(
                        data=ln.photo, tanggal_stock=tanggal_stock,
                        distributor_code=distributor.distributor_code,
                        distributor_name=distributor.distributor_name,
                        spu_code=ln.sku_code, spu_name=ln.sku_name,
                        supervisor_name=supervisor, transaction_id=tx,
                    )
        except Exception as exc:
            self._log_orphans(uploaded)
            raise SubmissionFailed(f"Upload foto evidence gagal; tidak ada data yang disimpan. {exc}") from exc

        records = []
        for ln in lines:
            ev = uploaded.get(ln.sku_code)
            old = existing_by_code.get(ln.sku_code)
            records.append(TransactionRecord(
                transaction_id=make_transaction_id(tanggal_stock, distributor.distributor_code, ln.sku_code),
                tanggal_stock=tanggal_stock,
                distributor_code=distributor.distributor_code,
                distributor_name=distributor.distributor_name,
                region=distributor.region,
                supervisor_name=supervisor,
                spu_code=ln.sku_code, spu_name=ln.sku_name, stock_qty=ln.stock_qty,
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

        result = SubmitResult(lines=[], tanggal_stock=tanggal_stock, submitted_at=now)
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
            dups = self.bq.find_duplicate_keys(tanggal_stock, distributor.distributor_code)
            if dups:
                result.warnings.append(
                    f"Terdeteksi duplikat key di BigQuery untuk SPU: {', '.join(dups)}. Hubungi admin data.")
        except Exception:
            log.warning("Pengecekan duplikat gagal", exc_info=True)

        result.lines = [
            SavedLine(
                transaction_id=r.transaction_id, spu_code=r.spu_code, spu_name=r.spu_name,
                stock_qty=r.stock_qty, action=actions[r.spu_code],
                evidence_url=r.evidence_file_url or (existing_by_code.get(r.spu_code) or {}).get("evidence_file_url"),
            )
            for r in records
        ]
        return result

    @staticmethod
    def _log_orphans(uploaded: Mapping[str, object]) -> None:
        for ev in uploaded.values():
            log.warning("Foto yatim dibiarkan di storage (tidak dihapus): %s", getattr(ev, "file_id", ev))
