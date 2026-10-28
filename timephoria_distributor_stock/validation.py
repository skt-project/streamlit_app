"""Aturan validasi murni (tanpa Streamlit / Google API) sehingga mudah diuji.

Mencakup: distributor, SKU, stock_qty, duplikat SKU, aturan evidence, dan identitas transaksi.
"""
from __future__ import annotations

import math
import re
import uuid
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping, Sequence

INT64_MAX = 2**63 - 1
MAX_SUPERVISOR_LEN = 100

# Namespace UUID5 untuk transaction_id. JANGAN DIUBAH: mengubahnya membuat semua
# transaction_id lama tidak cocok lagi dengan key (tanggal stock + distributor + SKU).
TRANSACTION_NAMESPACE = uuid.UUID("e68edc98-2183-45c4-8648-874ffe3bb523")

PHOTO_REQUIRED_MSG = "Foto evidence wajib: total Stock lebih dari 0, lampirkan minimal satu foto."


def norm(value: Any) -> str:
    return " ".join(str(value if value is not None else "").split()).casefold()


@dataclass(frozen=True)
class Distributor:
    distributor_code: str
    distributor_name: str
    region: str
    spv: str

    @property
    def label(self) -> str:
        return f"{self.distributor_code} - {self.distributor_name}"


@dataclass(frozen=True)
class Sku:
    code: str
    name: str

    @property
    def label(self) -> str:
        return f"{self.code} - {self.name}" if self.name else self.code


def upsert_key(tanggal_stock: date, distributor_code: str, sku_code: str) -> tuple[str, str, str]:
    return (tanggal_stock.isoformat(), str(distributor_code).strip(), str(sku_code).strip())


def make_transaction_id(tanggal_stock: date, distributor_code: str, sku_code: str) -> str:
    """UUID5 deterministik dari key tanggal stock + distributor + SKU."""
    return str(uuid.uuid5(TRANSACTION_NAMESPACE, "|".join(upsert_key(tanggal_stock, distributor_code, sku_code))))


_INT_RE = re.compile(r"[0-9]+", re.ASCII)
_NEG_INT_RE = re.compile(r"-[0-9]+", re.ASCII)
_DECIMAL_RE = re.compile(r"[-+]?[0-9]*[.,][0-9]+", re.ASCII)


def parse_qty(raw: Any, label: str = "Stock") -> tuple[int | None, str | None]:
    """Kembalikan (qty, None) bila valid atau (None, pesan_error).

    Valid: bilangan bulat >= 0 (nol boleh). Ditolak: kosong, negatif, desimal
    (termasuk "3.0" pada teks), bukan angka, bool, dan angka di atas INT64.
    """
    if raw is None or isinstance(raw, bool):
        return None, f"{label} wajib diisi."
    if isinstance(raw, int):
        value = raw
    elif isinstance(raw, float):
        if not math.isfinite(raw) or not raw.is_integer():
            return None, f"{label} harus bilangan bulat (tidak boleh desimal)."
        value = int(raw)
    else:
        text = str(raw).strip()
        if not text:
            return None, f"{label} wajib diisi."
        if _INT_RE.fullmatch(text):
            value = int(text)
        elif _NEG_INT_RE.fullmatch(text):
            return None, f"{label} tidak boleh negatif."
        elif _DECIMAL_RE.fullmatch(text):
            return None, f"{label} harus bilangan bulat (tidak boleh desimal)."
        else:
            return None, f"{label} harus berupa angka bilangan bulat."
    if value < 0:
        return None, f"{label} tidak boleh negatif."
    if value > INT64_MAX:
        return None, f"{label} terlalu besar."
    return value, None


@dataclass
class LineInput:
    """Satu baris SKU di form, apa adanya dari UI (belum divalidasi)."""
    row_no: int
    sku_code: str | None = None
    stock_raw: Any = None
    photo: bytes | None = None
    photo_error: str | None = None


@dataclass(frozen=True)
class FinalLine:
    row_no: int
    sku_code: str
    sku_name: str
    stock_qty: int
    photo: bytes | None


@dataclass(frozen=True)
class Issue:
    row_no: int | None
    message: str

    def __str__(self) -> str:
        return f"Baris {self.row_no}: {self.message}" if self.row_no else self.message


@dataclass
class ValidationResult:
    errors: list[Issue] = field(default_factory=list)
    warnings: list[Issue] = field(default_factory=list)
    lines: list[FinalLine] = field(default_factory=list)
    total_stock: int = 0
    photo_count: int = 0

    @property
    def ok(self) -> bool:
        return not self.errors


def validate_submission(
    *,
    distributor_code: str | None,
    tanggal_stock: date | None,
    supervisor_name: str | None,
    lines: Sequence[LineInput],
    distributors: Mapping[str, Distributor],
    skus: Mapping[str, Sku],
) -> ValidationResult:
    """Validasi seluruh isian. Tidak punya efek samping.

    SKU yang sama berulang → baris TERAKHIR dipakai (baris sebelumnya tidak divalidasi).
    Foto wajib hanya bila total stock baris yang lolos > 0. Semua stock 0 boleh tanpa foto.
    Error angka pada satu baris tidak menambah pesan "foto wajib".
    """
    res = ValidationResult()

    if not distributor_code or distributor_code not in distributors:
        res.errors.append(Issue(None, "Distributor wajib dipilih."))
    if tanggal_stock is None:
        res.errors.append(Issue(None, "Tanggal Stock wajib diisi."))
    name = (supervisor_name or "").strip()
    if not name:
        res.errors.append(Issue(None, "Nama Supervisor wajib diisi."))
    elif len(name) > MAX_SUPERVISOR_LEN:
        res.errors.append(Issue(None, f"Nama Supervisor maksimal {MAX_SUPERVISOR_LEN} karakter."))

    last_by_sku: dict[str, LineInput] = {}
    no_sku: list[LineInput] = []
    for ln in lines:
        if ln.sku_code:
            if ln.sku_code in last_by_sku:
                prev = last_by_sku[ln.sku_code]
                res.warnings.append(Issue(
                    prev.row_no,
                    f"SKU {ln.sku_code} dimasukkan lebih dari sekali; baris {ln.row_no} "
                    f"(terakhir) yang dipakai, baris ini diabaikan.",
                ))
            last_by_sku[ln.sku_code] = ln
        else:
            no_sku.append(ln)

    for ln in no_sku:
        res.errors.append(Issue(ln.row_no, "SKU wajib dipilih."))

    for ln in sorted(last_by_sku.values(), key=lambda x: x.row_no):
        sku = skus.get(ln.sku_code or "")
        if sku is None:
            res.errors.append(Issue(ln.row_no, f"SKU '{ln.sku_code}' tidak ada di daftar SKU."))
            continue
        stock, err_st = parse_qty(ln.stock_raw, "Stock")
        if err_st:
            res.errors.append(Issue(ln.row_no, err_st))
        if ln.photo_error:
            res.errors.append(Issue(ln.row_no, f"Foto: {ln.photo_error}"))
        if err_st or ln.photo_error:
            continue
        assert stock is not None
        res.lines.append(FinalLine(ln.row_no, sku.code, sku.name, stock, ln.photo))

    if not res.lines and not any(i.row_no for i in res.errors):
        res.errors.append(Issue(None, "Tambahkan minimal satu SKU."))

    res.total_stock = sum(ln.stock_qty for ln in res.lines)
    res.photo_count = sum(1 for ln in last_by_sku.values() if ln.photo)

    if res.photo_count > 0:
        for ln in res.lines:
            if ln.stock_qty > 0 and ln.photo is None:
                res.warnings.append(Issue(
                    ln.row_no,
                    f"PERINGATAN: SKU {ln.sku_code} Stock {ln.stock_qty} tidak memiliki foto evidence.",
                ))

    hard = [e for e in res.errors if e.row_no and not e.message.endswith("wajib diisi.")]
    if not hard and res.total_stock > 0 and res.photo_count == 0:
        res.errors.append(Issue(None, PHOTO_REQUIRED_MSG))
    return res
