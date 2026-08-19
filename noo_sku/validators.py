"""Row-level business rules for NOO and SKU uploads.

Rules come from BD Support's own guideline sheets inside the two templates,
which are more specific than the MoM. Where the guideline and the standing
instruction conflict, the conflict is handled rather than silently resolved —
see `SYSTEM_OWNED` handling in `validate_noo`.

Pure functions: they take already-parsed rows plus reference data and return
issues. No Streamlit, no network.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from . import config
from .customer_code import split_customer_code
from .normalize import clean, norm_key

ERROR = "error"
WARNING = "warning"


@dataclass(frozen=True)
class Issue:
    row: int            # sheet row number as the admin sees it in Excel
    column: str
    problem: str
    suggestion: str
    severity: str = ERROR

    def as_text(self) -> str:
        return f"Baris {self.row} — {self.column}: {self.problem} {self.suggestion}".strip()


def _require(value, row, column, issues, suggestion="Wajib diisi."):
    if not clean(value):
        issues.append(Issue(row, column, "Kolom ini kosong.", suggestion))
        return False
    return True


def validate_noo(rows, row_numbers, *, distributor_code, distributor_name,
                 expected_suffix, known_cities=None, store_types=None):
    """Validate NOO rows against the session's distributor context.

    Returns ``(issues, cleaned_rows)``. Cleaned rows carry system-owned fields
    overwritten with session values, so a file can never smuggle in another
    distributor's identity.
    """
    issues: list[Issue] = []
    cleaned = []
    known_cities = {norm_key(c) for c in (known_cities or [])}
    store_types = store_types or config.STORE_TYPES_BY_CHANNEL
    dist_code = norm_key(distributor_code)

    for row, sheet_row in zip(rows, row_numbers):
        g = lambda name: clean(row.get(name, ""))  # noqa: E731

        store_id = g("Store ID (Opsional)")
        if store_id and not re.match(config.STORE_ID_PATTERN, norm_key(store_id)):
            issues.append(Issue(
                sheet_row, "Store ID",
                f'Format "{store_id}" tidak valid.',
                "Gunakan format seperti IEBB01234, atau kosongkan "
                "(kolom ini opsional).",
            ))

        _require(g("Store Name"), sheet_row, "Store Name", issues,
                 "Isi dengan nama toko lengkap.")

        channel = norm_key(g("Channel (GT / MTi)"))
        if not channel:
            issues.append(Issue(sheet_row, "Channel", "Kolom ini kosong.",
                                "Isi dengan GT atau MTI."))
        elif channel not in config.VALID_CHANNELS:
            issues.append(Issue(
                sheet_row, "Channel", f'Nilai "{g("Channel (GT / MTi)")}" tidak dikenali.',
                "Gunakan GT atau MTI.",
            ))

        # --- Customer Code: brand prefix + this distributor's abbreviation ---
        cust_code = norm_key(g("Customer Code"))
        if not cust_code:
            issues.append(Issue(
                sheet_row, "Customer Code", "Kolom ini kosong.",
                _suffix_hint(expected_suffix),
            ))
        else:
            prefix, suffix = split_customer_code(cust_code)
            if prefix is None:
                issues.append(Issue(
                    sheet_row, "Customer Code",
                    f'"{cust_code}" tidak diawali kode brand yang valid.',
                    _suffix_hint(expected_suffix),
                ))
            elif expected_suffix and suffix != expected_suffix:
                issues.append(Issue(
                    sheet_row, "Customer Code",
                    f'"{cust_code}" bukan milik {dist_code}.',
                    _suffix_hint(expected_suffix),
                ))

        # --- Customer Store Code: must carry this distributor's code ---
        store_code = norm_key(g("Customer Store Code"))
        if not store_code:
            issues.append(Issue(
                sheet_row, "Customer Store Code", "Kolom ini kosong.",
                f"Isi dengan {dist_code} + customer ID toko "
                f"(contoh: {dist_code}00010).",
            ))
        elif not store_code.startswith(dist_code):
            issues.append(Issue(
                sheet_row, "Customer Store Code",
                f'"{store_code}" tidak diawali kode distributor Anda.',
                f"Tambahkan {dist_code} di depan customer ID "
                f"(contoh: {dist_code}00010).",
            ))

        city = g("City")
        if not _require(city, sheet_row, "City", issues,
                        "Isi dengan nama kota/kabupaten."):
            pass
        elif known_cities and norm_key(city) not in known_cities:
            issues.append(Issue(
                sheet_row, "City", f'"{city}" tidak ada di daftar acuan.',
                "Periksa ejaan pada sheet 'City & Store Type'. Jika kota "
                "memang belum terdaftar, data tetap bisa diproses.",
                severity=WARNING,
            ))

        _require(g("Store Address"), sheet_row, "Store Address", issues,
                 "Cantumkan alamat selengkap mungkin (jalan, nomor, "
                 "kelurahan, kecamatan, kota, kode pos).")

        # --- Store Type must be legal for the chosen channel ---
        stype = g("Store Type")
        allowed = store_types.get(channel, set())
        if not stype:
            issues.append(Issue(
                sheet_row, "Store Type", "Kolom ini kosong.",
                f"Pilih salah satu: {', '.join(sorted(allowed))}."
                if allowed else "Isi sesuai sheet 'City & Store Type'.",
            ))
        elif allowed and norm_key(stype) not in {norm_key(s) for s in allowed}:
            issues.append(Issue(
                sheet_row, "Store Type",
                f'"{stype}" tidak berlaku untuk channel {channel}.',
                f"Pilih salah satu: {', '.join(sorted(allowed))}.",
            ))

        # --- System-owned columns: warn, then overwrite. Never trust the file.
        # Decision B: Branch Name and Customer Branch Code stay in the template
        # but are SYSTEM-AUTHORITATIVE. A value that disagrees with the
        # authenticated distributor is a hard error - never silently accepted,
        # and never allowed to override the session identity.
        out = dict(row)
        typed_branch = g("Branch Name")
        if typed_branch and norm_key(typed_branch) != norm_key(distributor_name):
            issues.append(Issue(
                sheet_row, "Branch Name",
                f'"{typed_branch}" tidak sama dengan nama distributor pada '
                f'akun yang login ({distributor_name}).',
                f'Perbaiki kolom ini menjadi "{distributor_name}", atau login '
                "menggunakan akun distributor yang sesuai.",
                severity=ERROR,
            ))
        # A distributor code in the file that disagrees with the session is a
        # hard error, never a silent substitution: the file is claiming to be
        # somebody else's data and the admin must resolve that deliberately.
        typed_code = norm_key(g("Customer Branch Code"))
        if typed_code and typed_code != dist_code:
            issues.append(Issue(
                sheet_row, "Customer Branch Code",
                f'Kode distributor "{typed_code}" pada file tidak sama dengan '
                f'akun yang login ({dist_code}).',
                f"Perbaiki kolom ini menjadi {dist_code}, atau login "
                "menggunakan akun distributor yang sesuai.",
                severity=ERROR,
            ))
        out["Branch Name"] = distributor_name
        out["Customer Branch Code"] = dist_code
        out["Channel (GT / MTi)"] = channel or g("Channel (GT / MTi)")
        cleaned.append(out)

    return issues, cleaned


def _suffix_hint(expected_suffix) -> str:
    if not expected_suffix:
        return "Kode singkatan distributor Anda belum terdaftar. Hubungi BD Support."
    codes = ", ".join(f"{p}{expected_suffix}" for p in config.VALID_PREFIXES)
    return f"Gunakan salah satu: {codes}."


def validate_sku(rows, row_numbers, *, distributor_code, product_lookup,
                 strict_names=False, strict_size=False):
    """Validate SKU rows against the principal product master.

    ``product_lookup`` maps normalised SKU code -> ``{"brand", "product_name",
    "pack_size"}``. An unknown code is always an error: without it the brand —
    and therefore the Customer Code — cannot be derived.
    """
    issues: list[Issue] = []
    cleaned = []

    for row, sheet_row in zip(rows, row_numbers):
        g = lambda name: clean(row.get(name, ""))  # noqa: E731

        code = g("Principal Product Code")
        product = None
        if not code:
            issues.append(Issue(
                sheet_row, "Principal Product Code", "Kolom ini kosong.",
                "Isi dengan kode produk prinsipal (contoh: SKINTIFIC-296).",
            ))
        else:
            product = product_lookup.get(norm_key(code))
            if product is None:
                issues.append(Issue(
                    sheet_row, "Principal Product Code",
                    f'"{code}" tidak ditemukan di master produk prinsipal.',
                    "Pastikan kode ditulis sama persis dengan data prinsipal.",
                ))
            elif norm_key(product.get("brand")) not in config.IN_SCOPE_BRANDS:
                issues.append(Issue(
                    sheet_row, "Principal Product Code",
                    f'"{code}" milik brand {product.get("brand")}, '
                    "di luar cakupan mapping ini.",
                    "Mapping ini hanya untuk SKINTIFIC, TIMEPHORIA, dan "
                    "FACERINNA.",
                ))

        name = g("Principal Product Name")
        if not name:
            issues.append(Issue(
                sheet_row, "Principal Product Name", "Kolom ini kosong.",
                "Isi dengan nama produk prinsipal.",
            ))
        elif product and product.get("product_name"):
            if norm_key(name) != norm_key(product["product_name"]):
                issues.append(Issue(
                    sheet_row, "Principal Product Name",
                    f'"{name}" berbeda dari nama di master prinsipal.',
                    f'Nama yang benar: "{product["product_name"]}".',
                    severity=ERROR if strict_names else WARNING,
                ))

        size = g("Product Size (ml/g)")
        if not size:
            issues.append(Issue(
                sheet_row, "Product Size (ml/g)", "Kolom ini kosong.",
                "Isi sesuai spesifikasi prinsipal (contoh: 8g, 30ml).",
            ))
        elif product and product.get("pack_size"):
            if norm_key(size) != norm_key(product["pack_size"]):
                issues.append(Issue(
                    sheet_row, "Product Size (ml/g)",
                    f'"{size}" berbeda dari spesifikasi prinsipal.',
                    f'Spesifikasi yang benar: "{product["pack_size"]}".',
                    severity=ERROR if strict_size else WARNING,
                ))

        _require(g("Customer Product Code ( Di isi oleh Distributor)"),
                 sheet_row, "Customer Product Code", issues,
                 "Isi dengan kode produk milik distributor.")
        _require(g("Customer Product Name  ( Di isi oleh Distributor)"),
                 sheet_row, "Customer Product Name", issues,
                 "Isi dengan nama produk milik distributor.")

        out = dict(row)
        out["_brand"] = (product or {}).get("brand", "")
        cleaned.append(out)

    return issues, cleaned


def split_severity(issues):
    """Partition issues into (errors, warnings)."""
    return ([i for i in issues if i.severity == ERROR],
            [i for i in issues if i.severity == WARNING])
