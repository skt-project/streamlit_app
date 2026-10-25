"""Daftar SKU dari BigQuery `gt_schema.master_product` untuk fitur "Tambah SKU / nomor BPOM langsung".

Hanya membaca (SELECT sku, product_name, pack_size). Modul ini murni terhadap Streamlit: client BigQuery dioper dari luar
sehingga mudah dites.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

DEFAULT_TABLE = "skintific-data-warehouse.gt_schema.master_product"
_TABLE_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


@dataclass(frozen=True)
class Product:
    sku: str
    product_name: str
    pack_size: str = ""

    @property
    def label(self) -> str:
        return f"{self.sku} - {self.product_name}"


def _clean(value) -> str:
    return "" if value is None else str(value).strip()


def products_from_rows(rows) -> tuple[list[Product], int]:
    """(produk unik per SKU terurut, jumlah SKU yang dilewati karena tanpa SKU/nama produk)."""
    seen, result, skipped = set(), [], 0
    for row in rows:
        sku, name, pack = _clean(row["sku"]), _clean(row["product_name"]), _clean(row["pack_size"])
        if not sku or not name:
            skipped += 1
            continue
        if sku.upper() in seen:
            continue
        seen.add(sku.upper())
        result.append(Product(sku, name, pack))
    return sorted(result, key=lambda p: p.sku.upper()), skipped


def fetch_products(client, table: str = DEFAULT_TABLE) -> tuple[list[Product], int]:
    if not _TABLE_RE.match(table):
        raise ValueError(f"Nama tabel master_product tidak valid: {table!r}")
    rows = client.query(f"SELECT sku, product_name, pack_size FROM `{table}`").result()
    products, skipped = products_from_rows(rows)
    if not products:
        raise ValueError(f"Tabel {table} tidak berisi SKU.")
    return products, skipped
