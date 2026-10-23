"""Aturan validasi murni (tanpa Streamlit / Google API) sehingga mudah diuji.

Mencakup: master Non BA, SKU, qty (Sell Out dan Stock), duplikat SKU, aturan evidence dan
identitas transaksi (upsert key).
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
# transaction_id lama tidak cocok lagi dengan key (tanggal + store + SKU).
TRANSACTION_NAMESPACE = uuid.UUID("6f1d8c0e-5b0a-5c1e-9d3a-2a7b4e9c1f10")


def norm(value: Any) -> str:
    """Normalisasi untuk perbandingan: trim, rapikan spasi ganda, case-insensitive."""
    return " ".join(str(value if value is not None else "").split()).casefold()


# ── Master ──────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Store:
    store_id: str          # kolom "Cust. ID"
    store_name: str
    region: str
    group: str
    ba_status: str

    @property
    def label(self) -> str:
        return f"{self.store_id} - {self.store_name}"


@dataclass(frozen=True)
class Sku:
    code: str
    name: str

    @property
    def label(self) -> str:
        return f"{self.code} - {self.name}"


def is_non_ba(value: Any) -> bool:
    return norm(value) == "non ba"


# ── Identitas transaksi ─────────────────────────────────────────────────────

def upsert_key(sales_date: date, store_id: str, sku_code: str) -> tuple[str, str, str]:
    return (sales_date.isoformat(), str(store_id).strip(), str(sku_code).strip())


def make_transaction_id(sales_date: date, store_id: str, sku_code: str) -> str:
    """UUID5 deterministik dari key tanggal + store ID + SKU Code."""
    return str(uuid.uuid5(TRANSACTION_NAMESPACE, "|".join(upsert_key(sales_date, store_id, sku_code))))


# ── Quantity ────────────────────────────────────────────────────────────────

_INT_RE = re.compile(r"[0-9]+", re.ASCII)
_NEG_INT_RE = re.compile(r"-[0-9]+", re.ASCII)
_DECIMAL_RE = re.compile(r"[-+]?[0-9]*[.,][0-9]+", re.ASCII)


def parse_qty(raw: Any, label: str = "Qty") -> tuple[int | None, str | None]:
    """Kembalikan (qty, None) bila valid atau (None, pesan_error).

    Valid: bilangan bulat >= 0 (nol boleh). Ditolak: kosong, negatif, desimal
    (termasuk "3.0" pada teks), bukan angka. `label` dipakai di pesan error (mis. "Sell Out").
    """
    if raw is None or isinstance(raw, bool):
        return None, f"{label} wajib diisi."
    if isinstance(raw, int):
        value = raw
    elif isinstance(raw, float):  # nilai numerik dari Excel
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


# ── Input form ──────────────────────────────────────────────────────────────

@dataclass
class LineInput:
    """Satu baris SKU di form, apa adanya dari UI (belum divalidasi)."""
    row_no: int                     # nomor baris di UI (mulai 1)
    sku_code: str | None = None
    sell_out_raw: Any = None        # penjualan store → end consumer
    stock_raw: Any = None           # stock SKU di store
    photo: bytes | None = None      # JPEG hasil kompresi (sudah diproses image_processor)
    photo_error: str | None = None  # galat saat memproses foto

    def is_blank(self) -> bool:
        def empty(v: Any) -> bool:
            return v is None or str(v).strip() == ""
        return (not self.sku_code and empty(self.sell_out_raw) and empty(self.stock_raw)
                and self.photo is None and not self.photo_error)


@dataclass(frozen=True)
class FinalLine:
    row_no: int
    sku_code: str
    sku_name: str
    sell_out_qty: int
    stock_qty: int
    photo: bytes | None


@dataclass(frozen=True)
class Issue:
    row_no: int | None   # None = masalah level form
    message: str

    def __str__(self) -> str:
        return f"Baris {self.row_no}: {self.message}" if self.row_no else self.message


@dataclass
class ValidationResult:
    errors: list[Issue] = field(default_factory=list)
    warnings: list[Issue] = field(default_factory=list)
    lines: list[FinalLine] = field(default_factory=list)
    total_sell_out: int = 0
    total_stock: int = 0
    photo_count: int = 0

    @property
    def ok(self) -> bool:
        return not self.errors


def validate_submission(
    *,
    store_id: str | None,
    sales_date: date | None,
    supervisor_name: str | None,
    lines: Sequence[LineInput],
    stores: Mapping[str, Store],
    skus: Mapping[str, Sku],
) -> ValidationResult:
    """Validasi seluruh isian form. Tidak punya efek samping.

    `stores` harus sudah hanya berisi Non BA. SKU yang sama berulang → baris TERAKHIR
    dipakai (baris sebelumnya tidak divalidasi karena akan dibuang).

    Aturan foto: setiap submit WAJIB memuat minimal satu foto (dari salah satu SKU), berapa pun
    qty-nya, termasuk 0. Foto per SKU bersifat opsional.
    """
    res = ValidationResult()

    if not store_id or store_id not in stores:
        res.errors.append(Issue(None, "Store wajib dipilih dari daftar store Non BA."))
    if sales_date is None:
        res.errors.append(Issue(None, "Tanggal Penjualan wajib diisi."))
    name = (supervisor_name or "").strip()
    if not name:
        res.errors.append(Issue(None, "Nama Supervisor wajib diisi."))
    elif len(name) > MAX_SUPERVISOR_LEN:
        res.errors.append(Issue(None, f"Nama Supervisor maksimal {MAX_SUPERVISOR_LEN} karakter."))

    active = [ln for ln in lines if not ln.is_blank()]

    # Dedup SKU: baris terakhir menang.
    last_by_sku: dict[str, LineInput] = {}
    no_sku: list[LineInput] = []
    for ln in active:
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
            res.errors.append(Issue(ln.row_no, f"SKU '{ln.sku_code}' tidak ada di master SKU."))
            continue
        sell_out, err_so = parse_qty(ln.sell_out_raw, "Sell Out")
        stock, err_st = parse_qty(ln.stock_raw, "Stock")
        for err in (err_so, err_st):
            if err:
                res.errors.append(Issue(ln.row_no, err))
        if ln.photo_error:
            res.errors.append(Issue(ln.row_no, f"Foto: {ln.photo_error}"))
        if err_so or err_st or ln.photo_error:
            continue
        assert sell_out is not None and stock is not None
        res.lines.append(FinalLine(ln.row_no, sku.code, sku.name, sell_out, stock, ln.photo))

    if not res.lines and not any(i.row_no for i in res.errors):
        res.errors.append(Issue(None, "Tambahkan minimal satu SKU."))

    res.total_sell_out = sum(ln.sell_out_qty for ln in res.lines)
    res.total_stock = sum(ln.stock_qty for ln in res.lines)
    # Foto dihitung dari baris aktif (bukan hanya yang lolos validasi), supaya error qty di satu baris
    # tidak memunculkan pesan "foto belum ada" yang menyesatkan.
    res.photo_count = sum(1 for ln in last_by_sku.values() if ln.photo)

    # Aturan evidence: foto mandatory di setiap submit.
    for ln in res.lines:
        if ln.sell_out_qty > 0 and ln.photo is None:
            res.warnings.append(Issue(
                ln.row_no,
                f"PERINGATAN: SKU {ln.sku_code} Sell Out {ln.sell_out_qty} tidak memiliki foto evidence.",
            ))
    if last_by_sku and res.photo_count == 0:
        res.errors.append(Issue(
            None, "Foto evidence wajib: lampirkan minimal satu foto (dari salah satu SKU) pada setiap submit.",
        ))
    return res
