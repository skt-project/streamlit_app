"""Enriched pool-row construction and the guarded, column-scoped write.

Write safety rules enforced here:

* **Column-scoped, never a blind full-row append.** Fixed 2026-09-03: both
  pools are structured trackers, not append-only tables — BD Support pre-fills
  every row (confirmed down to row 900+ of the NOO pool, long before any
  upload ever reached it) with live spreadsheet formulas in specific columns,
  and owns a couple of manual processing flags outright. The write path here
  computes the single contiguous span of columns Streamlit actually owns (see
  `_owned_write_span`) and writes ONLY to that span — formula and BD-manual
  columns are never included in any request, not even as an explicit blank.
* **Reuses the pre-existing formula row, never a fresh one.** This module
  reads the owned span back first (`_next_target_row`) to find the row right
  after the last non-blank one WITHIN that span — a formula-pre-filled row
  whose owned columns are still blank counts as available — then writes
  there with a plain `values.update` (`SheetsClient.update_range`) against
  that exact row. Deliberately not `values.append`: its `range` argument only
  narrows table *detection*, not where values land — once BD Support's header
  row spans the whole sheet, `append` aligns new data to the detected
  table's own first column (A) regardless of a narrower range, which is
  exactly what broke every live upload on 2026-09-04. `update` has no
  detection step at all: it writes to precisely the cells named, nothing
  else.
* **Layout assert.** ``assert_layout`` re-reads the live header before every
  write and refuses if it differs from the expected layout by even one column.
  The pool headers are owned by BD Support; the app adapts, never the reverse.
* **Dry run by default.** ``Settings.write_enabled`` must be explicitly true.
* **RAW input**, so Sheets cannot reinterpret codes or execute a leading "=".
* **Identity from the session**, never from the uploaded file.

Why a scoped write rather than relying on how Sheets happened to behave so
far: every row written by the previous (full-row) implementation DID keep its
formulas intact, because inserting a row via `INSERT_ROWS` next to a
consistent formula pattern causes Sheets to carry that pattern into the new
row. That is real, observed behaviour — but it is Sheets' own row-insertion
mechanic, not anything this application explicitly requests or controls, and
it would stop protecting the data the moment that pattern breaks for any
reason. The fix here does not depend on it: formula columns are structurally
absent from the write payload, so there is nothing for a favourable coincidence
to save.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from . import config
from .normalize import clean, format_input_time, norm_key

#: Left blank for BD Support to formulate (MoM 31-Aug-2026 §5) — now covered
#: by `config.POOL_NOO_FORMULA_COLUMNS`, kept as an alias for readability at
#: the row-construction call sites below.
POOL_NOO_BD_SUPPORT_FIELDS = ("asm_kam", "spv", "se_kae", "aom")

#: The column every pool row always has, used to anchor the owned-write span
#: (see `_owned_write_span`) and to detect where a batch landed for
#: verification. Present in both pools, always inside the safe span.
ANCHOR_COLUMN = "input_time"


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
                  store_values, when, noo_existing_label="",
                  resolved_store_id="") -> dict:
    """One enriched `POOL NOO STREAMLIT` record, keyed by pool column.

    The branch comes from the row itself — MoM 31-Aug-2026 allows one file to
    carry several branches of the same company, and validation has already
    confirmed the code is inside the authorised company. `distributor_code` is
    only a fallback for a row that somehow carries none.

    User input wins for the fields the admin is responsible for — notably
    ``store_type`` and ``city``, whose vocabulary must be preserved exactly
    (decision B3). Master values are used only to fill a blank.

    `noo_existing_label` and `resolved_store_id` are the integrated NOO
    Detector's verdict (`noo_sku.noo_detector.classify`) — column E
    ("NOO/Existing") and the auto-populated `store_id` respectively. Fixed
    2026-09-03: `store_id` is now ALWAYS the master's own matched identifier,
    never the admin's typed value — populated only when a match was found,
    left blank otherwise so no fake/generated Store ID is ever written.

    The returned dict still carries a value for every pool column, including
    `area`/`province`/`asm_kam`/`spv`/`se_kae`/`aom` — useful context for the
    preview, showing what the system expects — but those specific keys are
    NEVER part of what actually reaches the sheet (see `_owned_write_span`):
    the live cells there are spreadsheet formulas, not Streamlit's to set.
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
        "store_id": norm_key(resolved_store_id),
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
        # Reference only — the live cell is a formula (see module docstring).
        "area": store_values.get("area", ""),
        "province": store_values.get("province", ""),
        "NOO/Existing": noo_existing_label,
    })
    for column in POOL_NOO_BD_SUPPORT_FIELDS:
        row[column] = ""
    for column in config.POOL_NOO_UNUSED:
        row[column] = ""
    return row


def build_sku_row(user_row, *, distributor_code, customer_code, dist_values,
                  product_values, when, company_name="") -> dict:
    """One enriched `POOL SKU STREAMLIT` record, keyed by pool column.

    MoM 31-Aug-2026 §8: `customer_name` is the COMPANY name, not the branch.
    `asm`/`region` are reference-only in the returned dict for the same reason
    as NOO's `area`/`province` — the live `RSA` cell is a formula.
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
    """Dict rows -> list-of-lists in exact pool column order.

    For preview/reporting/testing — shows the FULL conceptual row, including
    columns that are never actually sent to the sheet. The real write uses
    `_scoped_values`, not this.
    """
    return [[clean(row.get(column, "")) for column in headers] for row in rows]


# ─── Column-span ownership ────────────────────────────────────────────────────
def _col_letter(index: int) -> str:
    """0-based column index -> A1-style letters (0 -> 'A', 26 -> 'AA')."""
    letters = ""
    n = index + 1
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _owned_write_span(headers, hard_boundary, unused, anchor=ANCHOR_COLUMN):
    """The contiguous run of columns Streamlit writes. Returns
    (start_index, end_index), both inclusive, 0-based.

    Two different "not mine" sets are needed here, not one:

    * `hard_boundary` (manual flags + live formulas) is what actually stops
      the expansion outward from `anchor`. Using the broader
      config.POOL_*_NOT_OWNED here instead would be wrong: NOO's
      longitude/latitude/visibility_rating/location_rating sit BETWEEN
      store_address and store_type, both genuinely owned columns, so folding
      them into the stop-set would truncate the span at store_address and
      silently drop store_type — a real user-submitted field — from every
      write.
    * `unused` only trims a LEADING or TRAILING run once an edge has run off
      the end of the header array rather than being stopped by a real
      boundary column — e.g. SKU's trailing barcode/description, which sit
      past every genuinely owned column with no formula column to stop at,
      so nothing is lost by excluding them. An edge stopped by an actual
      hard_boundary column is never trimmed: NOO's longitude/.../
      location_rating stay IN the span because they're sandwiched before the
      next hard boundary (asm_kam), and skipping them would need a
      non-contiguous write for no benefit — writing "" there is harmless,
      there being no formula in any of them.
    """
    if anchor not in headers:
        raise ValueError(f"Anchor column {anchor!r} not found in header.")
    i = headers.index(anchor)
    start = i
    while start > 0 and headers[start - 1] not in hard_boundary:
        start -= 1
    end = i
    while end < len(headers) - 1 and headers[end + 1] not in hard_boundary:
        end += 1
    if start == 0:
        while start < end and headers[start] in unused:
            start += 1
    if end == len(headers) - 1:
        while end > start and headers[end] in unused:
            end -= 1
    return start, end


def owned_span_for(kind: str, headers) -> tuple:
    """(start_index, end_index, column_names) of the span Streamlit writes."""
    if kind == "NOO":
        hard_boundary = config.POOL_NOO_HARD_NEVER_TOUCH
        unused = config.POOL_NOO_UNUSED
    else:
        hard_boundary = config.POOL_SKU_HARD_NEVER_TOUCH
        unused = config.POOL_SKU_UNUSED
    start, end = _owned_write_span(headers, hard_boundary, unused)
    return start, end, headers[start:end + 1]


def _scoped_values(rows, span_columns) -> list:
    """Dict rows -> list-of-lists covering ONLY the owned span, in order."""
    return [[clean(row.get(column, "")) for column in span_columns]
           for row in rows]


def _next_target_row(existing_span_rows) -> int:
    """0-based index, relative to the first data row (sheet row 2), of the
    next row Streamlit should write to: the row right after the last one
    with ANY non-blank cell in the owned span.

    `existing_span_rows` must already be sliced to ONLY the owned-span
    columns (e.g. via `client.read_values(tab, "E2:W20000")`) — a row whose
    OTHER columns hold a live formula but whose owned span is still blank is
    exactly the "next available row" this must find and reuse, never a brand
    new row appended past the very end of the sheet.
    """
    target = 0
    for i, row in enumerate(existing_span_rows):
        if any(str(c).strip() for c in row):
            target = i + 1
    return target


# ─── Write guards ─────────────────────────────────────────────────────────────
def read_live_headers(client, tab) -> list:
    values = client.read_values(tab, "A1:BZ1")
    return [clean(c) for c in (values[0] if values else [])]


def assert_layout(client, tab, expected):
    """Refuse to write unless the live header matches `expected` exactly.

    Checks the WHOLE header, not just the owned span — if BD Support changes
    anything about this tab, including a column this app doesn't touch, that
    must stop every write and force a re-audit, because it may mean this
    module's understanding of which columns are safe is now stale.
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


def append_rows(client, tab, rows, *, headers, settings, upload_id,
                kind=None) -> WriteResult:
    """Append enriched rows to a pool tab, honouring dry-run mode.

    Writes ONLY to the contiguous span of columns Streamlit owns for this pool
    (see `owned_span_for`) — BD Support's manual flags and every live
    spreadsheet formula column are structurally excluded from the request,
    never merely set to blank.

    `kind` selects which pool's not-owned set applies ("NOO" or "SKU"); if
    omitted it is inferred from `tab`.
    """
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

    kind = kind or ("NOO" if tab == config.TAB_POOL_NOO else "SKU")

    # Checked in every mode so a dry run still surfaces a layout drift.
    assert_layout(client, tab, headers)

    start, end, span_columns = owned_span_for(kind, headers)
    scoped = _scoped_values(rows, span_columns)
    full_rows = to_values(rows, headers)  # for reporting/preview only

    if settings.dry_run:
        return WriteResult(
            True, True, len(scoped), upload_id, tab,
            f"DRY RUN — {len(scoped)} baris tervalidasi, ter-enrich dan siap "
            "diupload, tetapi TIDAK ditulis ke spreadsheet "
            "(WRITE_ENABLED=false).",
            rows=full_rows)

    start_letter, end_letter = _col_letter(start), _col_letter(end)
    existing = client.read_values(
        tab, f"{start_letter}2:{end_letter}{config.POOL_MAX_ROW}")
    target_index = _next_target_row(existing)
    target_row = target_index + 2       # sheet row 2 = existing[0]
    end_row = target_row + len(scoped) - 1
    client.update_range(tab, f"{start_letter}{target_row}:{end_letter}{end_row}",
                       scoped)
    return WriteResult(True, False, len(scoped), upload_id, tab,
                       f"{len(scoped)} baris berhasil ditambahkan ke {tab} "
                       f"(baris {target_row}-{end_row}, kolom "
                       f"{start_letter}:{end_letter}, mode {settings.mode}).",
                       rows=full_rows)


def verify_written(client, tab, headers, expected_rows, *, input_time,
                   distributor_code, kind=None):
    """Read the pool back and confirm the rows we just appended are there.

    Matches on the batch's `input_time` plus the distributor, which is the
    only batch key the existing pool supports - there is no upload_id column
    and adding one was explicitly out of scope.

    Compares only the OWNED span (what Streamlit actually wrote) — the
    formula columns' live values are computed by the sheet from data this
    module never sent, and would never match a preview-derived expectation.
    """
    kind = kind or ("NOO" if tab == config.TAB_POOL_NOO else "SKU")
    start, end, span_columns = owned_span_for(kind, headers)

    live = client.read_values(tab, "A2:BZ20000")
    index = {h: i for i, h in enumerate(headers)}
    ti, di = index.get("input_time"), index.get("customer_branch_code")

    def cell(row, i):
        return clean(row[i]) if i is not None and i < len(row) else ""

    found = [r for r in live
             if cell(r, ti) == input_time
             and norm_key(cell(r, di)) == norm_key(distributor_code)]

    expected = _scoped_values(expected_rows, span_columns)
    matched = 0
    remaining = []
    for r in found:
        segment = [clean(r[i]) if i < len(r) else "" for i in range(start, end + 1)]
        remaining.append(segment)
    for want in expected:
        for i, got in enumerate(remaining):
            if got == want:
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
