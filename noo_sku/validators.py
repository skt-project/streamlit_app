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
                 expected_suffix=None, known_cities=None, store_types=None,
                 allowed_branches=None, company_name="", suffix_for=None):
    """Validate NOO rows against the admin's authorised COMPANY scope.

    MoM 31-Aug-2026: a single admin handles every branch of their company, so
    one file may carry several branches. Each row therefore names its own
    ``Customer Branch Code``, and that code must belong to the same company as
    the logged-in account. A code outside the company is a hard error — login
    sets the boundary, the file chooses a branch inside it.

    ``allowed_branches`` maps ``code -> {"name": ...}``; ``suffix_for(code)``
    returns that branch's Customer Code abbreviation.

    Returns ``(issues, cleaned_rows)``.
    """
    issues: list[Issue] = []
    cleaned = []
    known_cities = {norm_key(c) for c in (known_cities or [])}
    store_types = store_types or config.STORE_TYPES_BY_CHANNEL
    dist_code = norm_key(distributor_code)
    allowed = {norm_key(k): v for k, v in (allowed_branches or {}).items()}
    if not allowed:
        allowed = {dist_code: {"name": distributor_name}}
    suffix_for = suffix_for or (lambda _code: expected_suffix)

    for row, sheet_row in zip(rows, row_numbers):
        g = lambda name: clean(row.get(name, ""))  # noqa: E731

        # --- Which branch is this row for? Company scope is the boundary. ---
        row_code = norm_key(g("Customer Branch Code")) or dist_code
        if not norm_key(g("Customer Branch Code")):
            issues.append(Issue(
                sheet_row, "Customer Branch Code", "Kolom ini kosong.",
                f"Isi dengan kode cabang (contoh: {dist_code}). Satu file "
                "boleh berisi beberapa cabang dalam perusahaan yang sama.",
            ))
        elif row_code not in allowed:
            company_label = company_name or "perusahaan Anda"
            issues.append(Issue(
                sheet_row, "Customer Branch Code",
                f'Kode distributor "{row_code}" tidak terdaftar di '
                f'{company_label}.',
                "Gunakan kode cabang yang berada di bawah perusahaan Anda. "
                f"Cabang yang diizinkan: {', '.join(sorted(allowed)[:8])}"
                + (" ..." if len(allowed) > 8 else "") + ".",
            ))
        row_suffix = suffix_for(row_code)

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
                _suffix_hint(row_suffix),
            ))
        else:
            prefix, suffix = split_customer_code(cust_code)
            if prefix is None:
                issues.append(Issue(
                    sheet_row, "Customer Code",
                    f'"{cust_code}" tidak diawali kode brand yang valid.',
                    _suffix_hint(row_suffix),
                ))
            elif row_suffix and suffix != row_suffix:
                issues.append(Issue(
                    sheet_row, "Customer Code",
                    f'"{cust_code}" bukan milik cabang {row_code}.',
                    _suffix_hint(row_suffix),
                ))

        # --- Customer Store Code: must carry this distributor's code ---
        store_code = norm_key(g("Customer Store Code"))
        if not store_code:
            issues.append(Issue(
                sheet_row, "Customer Store Code", "Kolom ini kosong.",
                f"Isi dengan {row_code} + customer ID toko "
                f"(contoh: {row_code}00010).",
            ))
        elif not store_code.startswith(row_code):
            issues.append(Issue(
                sheet_row, "Customer Store Code",
                f'"{store_code}" tidak diawali kode cabang pada baris ini '
                f'({row_code}).',
                f"Tambahkan {row_code} di depan customer ID "
                f"(contoh: {row_code}00010).",
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
        allowed_types = store_types.get(channel, set())
        if not stype:
            issues.append(Issue(
                sheet_row, "Store Type", "Kolom ini kosong.",
                f"Pilih salah satu: {', '.join(sorted(allowed_types))}."
                if allowed_types else "Isi sesuai sheet 'City & Store Type'.",
            ))
        elif allowed_types and norm_key(stype) not in {norm_key(s)
                                                       for s in allowed_types}:
            issues.append(Issue(
                sheet_row, "Store Type",
                f'"{stype}" tidak berlaku untuk channel {channel}.',
                f"Pilih salah satu: {', '.join(sorted(allowed_types))}.",
            ))

        # --- Branch: chosen per row, but only from within the company ------
        out = dict(row)
        branch_name = allowed.get(row_code, {}).get("name", "")
        typed_branch = g("Branch Name")
        if typed_branch and branch_name and \
                norm_key(typed_branch) != norm_key(branch_name):
            issues.append(Issue(
                sheet_row, "Branch Name",
                f'"{typed_branch}" tidak sama dengan nama cabang {row_code} '
                f'({branch_name}).',
                f'Akan diisi otomatis menjadi "{branch_name}".',
                severity=WARNING,
            ))
        out["Branch Name"] = branch_name or distributor_name
        out["Customer Branch Code"] = row_code
        out["Channel (GT / MTi)"] = channel or g("Channel (GT / MTi)")
        cleaned.append(out)

    return issues, cleaned


def _suffix_hint(expected_suffix) -> str:
    if not expected_suffix:
        return "Kode singkatan distributor Anda belum terdaftar. Hubungi BD Support."
    codes = ", ".join(f"{p}{expected_suffix}" for p in config.VALID_PREFIXES)
    return f"Gunakan salah satu: {codes}."


def validate_sku(rows, row_numbers, *, distributor_code, product_lookup,
                 strict_names=False):
    """Validate SKU rows against the principal product master.

    ``product_lookup`` maps normalised SKU code -> ``{"brand", "product_name",
    "pack_size"}``. An unknown Principal Product Code is ALWAYS a hard error
    (MoM 31-Aug-2026 §7): the row must not reach the tracker, and without the
    code the brand — and therefore the Customer Code — cannot be derived.

    The gramasi column was removed from the template on 31-Aug-2026;
    `specification` is filled from master_product instead of being validated.
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
