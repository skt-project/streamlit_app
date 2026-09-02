"""Enriched pool-row construction and the guarded append.

Write safety rules enforced here:

* **Append only.** Nothing in this module deletes, clears, sorts or overwrites.
  The only Sheets call is ``values.append``.
* **Layout assert.** ``assert_layout`` re-reads the live header before every
  write and refuses if it differs from the expected layout by even one column.
  The pool headers are owned by BD Support; the app adapts, never the reverse.
* **Dry run by default.** ``Settings.write_enabled`` must be explicitly true.
* **RAW input**, so Sheets cannot reinterpret codes or execute a leading "=".
* **Identity from the session**, never from the uploaded file.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from . import config
from .normalize import clean, format_input_time, norm_key


#: Left blank for BD Support to formulate (MoM 31-Aug-2026 §5).
POOL_NOO_BD_SUPPORT_FIELDS = ("asm_kam", "spv", "se_kae", "aom")


class LayoutMismatch(RuntimeError):
    """The live pool header is not what this code was written against."""


class PilotLimitExceeded(RuntimeError):
    """More rows were supplied than PILOT mode permits."""


@dataclass
class WriteResult:
    ok: bool
    dry_run: bool
    rows_written: int
    upload_id: str
    destination: str
    message: str
    rows: list = field(default_factory=list)


def new_upload_id() -> str:
    """Batch reference shown to the user and logged.

    Not written to the sheet — neither pool has an upload_id column and adding
    one is out of scope (decision B1). Combined with ``input_time`` and
    ``customer_branch_code`` it still identifies a batch for rollback.
    """
    return uuid.uuid4().hex[:8]


# ─── Row construction ─────────────────────────────────────────────────────────
def build_noo_row(user_row, *, distributor_code=None, dist_values,
                  store_values, when, noo_existing_label="") -> dict:
    """One enriched `POOL NOO STREAMLIT` record, keyed by pool column.

    The branch comes from the row itself — MoM 31-Aug-2026 allows one file to
    carry several branches of the same company, and validation has already
    confirmed the code is inside the authorised company. `distributor_code` is
    only a fallback for a row that somehow carries none.

    User input wins for the fields the admin is responsible for — notably
    ``store_type`` and ``city``, whose vocabulary must be preserved exactly
    (decision B3). Master values are used only to fill a blank.

    `noo_existing_label` is the integrated NOO Detector's verdict — column E
    ("NOO/Existing") of the live 41-column pool — computed by
    `noo_sku.noo_detector.check_reference_id` in the pipeline, never typed by
    the admin (MoM 31-Aug-2026 §3/§4).
    """
    g = lambda name: clean(user_row.get(name, ""))  # noqa: E731
    branch_code = norm_key(g("Customer Branch Code")) or norm_key(
        distributor_code or "")

    row = {column: "" for column in config.POOL_NOO_HEADERS}
    row.update({
        "asm_name": dist_values.get("asm", ""),
        "input_time": format_input_time(when),
        "branch_name": g("Branch Name") or dist_values.get("branch_name", ""),
        "region": dist_values.get("region", ""),
        "store_id": norm_key(g("Store ID (Opsional)")),
        "store_name": g("Store Name"),
        "channel_name": norm_key(g("Channel (GT / MTi)")),
        "customer_code": norm_key(g("Customer Code")),
        "customer_branch_code": branch_code,
        "customer_store_code": norm_key(g("Customer Store Code")),
        "customer_store_name": g("Store Name"),
        # B3: keep the admin's own vocabulary; fall back to master only if blank.
        "city": g("City") or store_values.get("city", ""),
        "store_address": g("Store Address"),
        "store_type": g("Store Type") or store_values.get("store_type", ""),
        "area": store_values.get("area", ""),
        "province": store_values.get("province", ""),
        "NOO/Existing": noo_existing_label,
    })
    # MoM 31-Aug-2026 §5: BD Support formulates the hierarchy themselves.
    # Streamlit must leave these blank - not derived from login, branch, or any
    # existing mapping logic.
    for column in POOL_NOO_BD_SUPPORT_FIELDS:
        row[column] = ""
    for column in config.POOL_NOO_UNUSED:
        row[column] = ""
    return row


def build_sku_row(user_row, *, distributor_code, customer_code, dist_values,
                  product_values, when, company_name="") -> dict:
    """One enriched `POOL SKU STREAMLIT` record, keyed by pool column.

    MoM 31-Aug-2026 §8: `customer_name` is the COMPANY name, not the branch.
    """
    g = lambda name: clean(user_row.get(name, ""))  # noqa: E731

    row = {column: "" for column in config.POOL_SKU_HEADERS}
    row.update({
        "asm": dist_values.get("asm", ""),
        "region": dist_values.get("region", ""),
        "input_time": format_input_time(when),
        "customer_code": norm_key(customer_code),
        "customer_name": company_name or dist_values.get("branch_name", ""),
        "product_code": g("Principal Product Code"),
        "customer_branch_code": norm_key(distributor_code),
        "product_name": product_values.get("product_name", ""),
        "customer_product_code": g("Customer Product Code ( Di isi oleh Distributor)"),
        "customer_product_name": g("Customer Product Name  ( Di isi oleh Distributor)"),
        "specification": product_values.get("specification", ""),
    })
    for column in config.POOL_SKU_UNUSED:
        row[column] = ""
    return row


def to_values(rows, headers) -> list:
    """Dict rows -> list-of-lists in exact pool column order."""
    return [[clean(row.get(column, "")) for column in headers] for row in rows]


# ─── Write guards ─────────────────────────────────────────────────────────────
def read_live_headers(client, tab) -> list:
    values = client.read_values(tab, "A1:BZ1")
    return [clean(c) for c in (values[0] if values else [])]


def assert_layout(client, tab, expected):
    """Refuse to write unless the live header matches `expected` exactly.

    Guards against the pool being restructured underneath us — which has already
    happened once: both pools gained a header between two passes of this project.
    """
    live = read_live_headers(client, tab)
    trimmed = live[:len(expected)]
    if trimmed != list(expected):
        diff = [f"col {i + 1}: sheet={l!r} expected={e!r}"
                for i, (l, e) in enumerate(zip(trimmed, expected)) if l != e]
        if len(live) < len(expected):
            diff.append(f"sheet has {len(live)} header cells, "
                        f"expected at least {len(expected)}")
        raise LayoutMismatch(
            f"Struktur kolom '{tab}' berbeda dari yang diharapkan aplikasi. "
            "Upload dibatalkan agar data tidak masuk ke kolom yang salah. "
            "Detail: " + "; ".join(diff[:5]))
    return True


def append_rows(client, tab, rows, *, headers, settings, upload_id) -> WriteResult:
    """Append enriched rows to a pool tab, honouring dry-run mode."""
    if not rows:
        return WriteResult(False, settings.dry_run, 0, upload_id, tab,
                           "Tidak ada baris yang memenuhi syarat untuk diupload.")

    # PILOT is a deliberately small, hand-verified first write. Refuse rather
    # than silently truncating - a caller who supplies more rows than the pilot
    # allows has misunderstood the mode, and truncation would hide that.
    limit = settings.max_rows
    if limit is not None and len(rows) > limit:
        raise PilotLimitExceeded(
            f"Mode PILOT hanya mengizinkan {limit} baris, "
            f"tetapi {len(rows)} baris diberikan. Kurangi jumlah baris atau "
            "gunakan mode production setelah UAT disetujui.")

    values = to_values(rows, headers)

    # Checked in every mode so a dry run still surfaces a layout drift.
    assert_layout(client, tab, headers)

    if settings.dry_run:
        return WriteResult(
            True, True, len(values), upload_id, tab,
            f"DRY RUN — {len(values)} baris tervalidasi, ter-enrich dan siap "
            "diupload, tetapi TIDAK ditulis ke spreadsheet "
            "(WRITE_ENABLED=false).",
            rows=values)

    client.append_values(tab, values)
    return WriteResult(True, False, len(values), upload_id, tab,
                       f"{len(values)} baris berhasil ditambahkan ke {tab} "
                       f"(mode {settings.mode}).",
                       rows=values)


def verify_written(client, tab, headers, expected_rows, *, input_time,
                   distributor_code):
    """Read the pool back and confirm the rows we just appended are there.

    Matches on the batch's `input_time` plus the distributor, which is the only
    batch key the existing pool supports - there is no upload_id column and
    adding one was explicitly out of scope.
    """
    live = client.read_values(tab, "A2:BZ20000")
    index = {h: i for i, h in enumerate(headers)}
    ti, di = index.get("input_time"), index.get("customer_branch_code")

    def cell(row, i):
        return clean(row[i]) if i is not None and i < len(row) else ""

    found = [r for r in live
             if cell(r, ti) == input_time
             and norm_key(cell(r, di)) == norm_key(distributor_code)]

    expected = to_values(expected_rows, headers)
    matched = 0
    remaining = [list(r) for r in found]
    for want in expected:
        for i, got in enumerate(remaining):
            padded = got + [""] * (len(want) - len(got))
            if padded[:len(want)] == want:
                matched += 1
                remaining.pop(i)
                break
    return {"expected": len(expected), "found_in_batch": len(found),
            "verified": matched, "passed": matched == len(expected)}


def pool_tab_for(kind: str) -> str:
    return config.TAB_POOL_NOO if kind == "NOO" else config.TAB_POOL_SKU


def pool_headers_for(kind: str) -> list:
    return (config.POOL_NOO_HEADERS if kind == "NOO"
            else config.POOL_SKU_HEADERS)
