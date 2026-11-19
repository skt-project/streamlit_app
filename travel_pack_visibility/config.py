"""Konfigurasi aplikasi dari secrets `[app]`. Tidak ada ID spreadsheet, bucket, atau kredensial di kode."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping

DEFAULT_TITLE = "Form Upload Visibility Travel Pack BCD Q4'26"
DEFAULT_OUTPUT_WORKSHEET = "TRAVEL_PACK_UPLOAD_LOG"
DEFAULT_EVIDENCE_PREFIX = "Travel Pack Visibility BCD Q4'26"
DEFAULT_ACH_COLUMN = "K"
DEFAULT_MIN_ACH = 4.0

_SHEET_URL_RE = re.compile(r"/spreadsheets/d/([A-Za-z0-9_-]+)")
_COLUMN_RE = re.compile(r"^[A-Z]{1,3}$")
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$")

# Nama header kolom daftar toko yang bisa dipaksa lewat secrets bila tidak terdeteksi otomatis.
HEADER_OVERRIDE_KEYS = {
    "asm": "asm_header",
    "store_code": "store_code_header",
    "store_name": "store_name_header",
    "spv": "spv_header",
    "region": "region_header",
}


class ConfigError(Exception):
    """Secrets tidak lengkap / tidak valid. Pesan aman ditampilkan ke user."""


def extract_sheet_id(value: str) -> str:
    """Terima ID polos atau URL Google Sheets lengkap, kembalikan ID-nya."""
    value = (value or "").strip()
    m = _SHEET_URL_RE.search(value)
    return m.group(1) if m else value


def column_index(letter: str) -> int:
    """'A' -> 0, 'K' -> 10, 'AA' -> 26."""
    idx = 0
    for ch in letter:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


@dataclass(frozen=True)
class AppConfig:
    store_sheet_id: str
    output_sheet_id: str
    evidence_bucket: str
    store_worksheet: str = ""                 # kosong = pindai semua tab, pakai tab pertama yang headernya cocok
    output_worksheet: str = DEFAULT_OUTPUT_WORKSHEET
    evidence_prefix: str = DEFAULT_EVIDENCE_PREFIX
    ach_column: str = DEFAULT_ACH_COLUMN      # kolom "Ach SKU (MOQ)" di daftar toko
    min_ach: float = DEFAULT_MIN_ACH          # toko tampil bila nilai kolom itu >= angka ini
    title: str = DEFAULT_TITLE
    header_overrides: dict[str, str] = field(default_factory=dict)

    @property
    def ach_index(self) -> int:
        return column_index(self.ach_column)


def _number(value: Any, label: str) -> float:
    try:
        return float(str(value).strip().replace(",", "."))
    except ValueError as exc:
        raise ConfigError(f"{label} harus berupa angka.") from exc


def load_config(secrets: Mapping[str, Any]) -> AppConfig:
    app = dict(secrets.get("app", {}) or {})

    def get(key: str, default: str = "") -> str:
        return str(app.get(key, default) or default).strip()

    overrides = {name: get(key) for name, key in HEADER_OVERRIDE_KEYS.items() if get(key)}
    cfg = AppConfig(
        store_sheet_id=extract_sheet_id(get("store_sheet_id")),
        output_sheet_id=extract_sheet_id(get("output_sheet_id")),
        evidence_bucket=get("evidence_bucket"),
        store_worksheet=get("store_worksheet"),
        output_worksheet=get("output_worksheet", DEFAULT_OUTPUT_WORKSHEET),
        evidence_prefix=get("evidence_prefix", DEFAULT_EVIDENCE_PREFIX),
        ach_column=get("ach_sku_column", DEFAULT_ACH_COLUMN).upper(),
        min_ach=_number(app.get("min_ach_sku", DEFAULT_MIN_ACH), "min_ach_sku"),
        title=get("title", DEFAULT_TITLE),
        header_overrides=overrides,
    )
    missing = [k for k, v in (("store_sheet_id", cfg.store_sheet_id), ("output_sheet_id", cfg.output_sheet_id),
                              ("evidence_bucket", cfg.evidence_bucket)) if not v]
    if missing:
        raise ConfigError("Secrets [app] belum lengkap: " + ", ".join(missing))
    if not _COLUMN_RE.match(cfg.ach_column):
        raise ConfigError("ach_sku_column harus huruf kolom, mis. K.")
    if not _BUCKET_RE.match(cfg.evidence_bucket):
        raise ConfigError("evidence_bucket bukan nama bucket GCS yang valid.")
    if not cfg.output_worksheet or len(cfg.output_worksheet) > 100:
        raise ConfigError("output_worksheet tidak valid.")
    return cfg
