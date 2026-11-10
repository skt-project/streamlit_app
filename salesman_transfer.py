"""Move an existing salesman to another distributor and KEEP the salesman_id.

Why this exists
---------------
`generate_salesman_id()` builds an ID from type + distributor code + running
number, and "Tambah Salesman Baru" is the only path that creates a salesman. So a
salesman who changes branch (ANDI, DST358 -> DST364) was always re-added and got
a brand-new ID (the abandoned GTIDST364001 is what that looked like). The ID is
the salesman's identity, not the distributor's; its prefix is only the
distributor that first issued it. Nothing in the app or in the downstream views
requires the prefix to match the current distributor (85 of the 269 active IDs
already do not), and PJP ownership is decided by `gt_salesman_mapping`
distributor_code alone.

What a transfer does (one atomic BigQuery transaction)
------------------------------------------------------
1. Deactivates the ONE active `gt_salesman_mapping` row, at the distributor the
   admin was shown. Never deleted: it stays as the history of who owned the ID.
2. Inserts a new active mapping row: SAME salesman_id, SAME type, new distributor.
3. Copies the latest `gt_master_salesman` snapshot of the salesman to the new
   distributor (the roster joins HR to mapping by name + distributor, so without
   this the transferred salesman would show up blank, as ANDI did on 2026-10-09).
4. Asserts that exactly one active mapping row exists, at the destination.

Any failing step rolls the whole transaction back, so there is never a state
with two active owners, or an owner with no HR row. PJP rows are NOT touched:
PJP history stays under the distributor that uploaded it.

Pure planning (`plan_transfer`) is separate from BigQuery I/O so the rules are
unit-testable without credentials. Table names are passed in by the caller so
this module does not duplicate the constants in salesman_pjp.py.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone

from google.cloud import bigquery

# ── Reasons a transfer is refused (TransferError.code) ───────────────────────
EMPTY_ID = "empty_id"
NOT_FOUND = "not_found"
INACTIVE = "inactive"
AMBIGUOUS_OWNER = "ambiguous_owner"
SAME_DISTRIBUTOR = "same_distributor"
UNKNOWN_DESTINATION = "unknown_destination"
MISSING_NAME = "missing_name"
NAME_TAKEN = "name_taken"
NO_HR_ROW = "no_hr_row"
NO_SPV = "no_spv"

#: The only HR fields an admin may change while moving. Everything else in the
#: HR snapshot (pay, phone, birth date, ...) is copied by BigQuery unchanged.
SPV_FIELDS = ("nama_spv_internal", "nama_spv_external", "nama_spv_internal_2")

_TABLE_NAME = re.compile(r"^[A-Za-z0-9_.\-]+$")


class TransferError(Exception):
    """A transfer that must not run. `message` is shown to the admin as-is."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class TransferPlan:
    salesman_id: str
    salesman_type: str
    salesman_name: str
    from_distributor: str
    to_distributor: str
    to_distributor_name: str
    to_region: str
    to_asm: str
    spv: dict  # the three SPV_FIELDS exactly as they will be written


# ── Small normalizers ────────────────────────────────────────────────────────

def _blank(value) -> bool:
    if value is None:
        return True
    try:
        if value != value:  # NaN
            return True
    except Exception:  # noqa: BLE001
        pass
    return str(value).strip() == ""


def norm(value) -> str:
    """Code/ID comparison form: trimmed and upper-cased; NaN/None -> ''."""
    return "" if _blank(value) else str(value).strip().upper()


def clean_name(value) -> str:
    """Same shape as salesman_pjp.sanitize_salesman_name (kept in step by a test)."""
    return "" if _blank(value) else re.sub(r"\s+", " ", str(value).strip().upper())


def _is_true(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() == "true"
    try:
        if value != value:
            return False
    except Exception:  # noqa: BLE001
        pass
    return bool(value)


def _opt(value):
    """An optional text field: stripped + upper-cased, or None when blank."""
    return None if _blank(value) else str(value).strip().upper()


# ── Planning (pure) ──────────────────────────────────────────────────────────

def plan_transfer(salesman_id, to_distributor, mapping_rows, hr_row, distributors,
                  spv_overrides=None) -> TransferPlan:
    """Decide whether a salesman may move, and exactly what will be written.

    mapping_rows  every gt_salesman_mapping row for the ID, plus the active rows
                  of the destination distributor (see `fetch_mapping_rows`).
    hr_row        the salesman's latest HR snapshot at the CURRENT distributor,
                  or None. Only the SPV_FIELDS are read.
    distributors  {distributor_code: {"name", "region", "asm"}} of valid
                  (Active) destinations.
    spv_overrides optional {field: value} for SPV_FIELDS; blank keeps the HR value.

    Raises TransferError for every case that must not run. The ID's prefix is
    deliberately never inspected.
    """
    sid = "" if _blank(salesman_id) else str(salesman_id).strip()
    if not sid:
        raise TransferError(EMPTY_ID, "Pilih Salesman ID yang akan dipindahkan.")

    rows = [r for r in mapping_rows if norm(r.get("salesman_id")) == norm(sid)]
    if not rows:
        raise TransferError(NOT_FOUND, f"Salesman ID `{sid}` tidak ditemukan.")

    active = [r for r in rows if _is_true(r.get("is_active"))]
    if not active:
        raise TransferError(
            INACTIVE,
            f"Salesman ID `{sid}` tidak aktif. Hanya salesman aktif yang dapat dipindahkan.")
    if len(active) > 1:
        owners = ", ".join(sorted({norm(r.get("distributor_code")) for r in active}))
        raise TransferError(
            AMBIGUOUS_OWNER,
            f"Salesman ID `{sid}` tercatat aktif {len(active)}x (distributor: {owners}). "
            "Data tidak konsisten — rapikan dulu sebelum memindahkan.")

    src = active[0]
    frm = norm(src.get("distributor_code"))
    to = norm(to_distributor)
    if not to:
        raise TransferError(UNKNOWN_DESTINATION, "Pilih distributor tujuan.")
    if to == frm:
        raise TransferError(
            SAME_DISTRIBUTOR,
            f"`{sid}` sudah aktif di distributor {to}. Tidak ada yang dipindahkan.")
    dest = distributors.get(to)
    if dest is None:
        raise TransferError(
            UNKNOWN_DESTINATION,
            f"Distributor tujuan {to} tidak ditemukan atau tidak aktif di master distributor.")

    name = clean_name(src.get("salesman"))
    if not name:
        raise TransferError(MISSING_NAME, f"Baris mapping `{sid}` tidak punya nama salesman.")

    taken = [r for r in mapping_rows
             if _is_true(r.get("is_active"))
             and norm(r.get("distributor_code")) == to
             and clean_name(r.get("salesman")) == name
             and norm(r.get("salesman_id")) != norm(sid)]
    if taken:
        raise TransferError(
            NAME_TAKEN,
            f"Distributor {to} sudah punya salesman aktif bernama {name} "
            f"(`{taken[0].get('salesman_id')}`). Roster dan HR digabung berdasarkan nama + "
            "distributor, jadi dua salesman bernama sama akan tertukar.")

    if hr_row is None:
        raise TransferError(
            NO_HR_ROW,
            f"Data HR (gt_master_salesman) untuk {name} di distributor {frm} tidak ditemukan. "
            "Lengkapi data salesman dulu agar roster distributor tujuan tidak kosong.")

    spv = {f: _opt(hr_row.get(f)) for f in SPV_FIELDS}
    for field, value in (spv_overrides or {}).items():
        if field in SPV_FIELDS and not _blank(value):
            spv[field] = _opt(value)
    if not spv["nama_spv_internal"]:
        raise TransferError(NO_SPV, "Nama SPV Internal wajib diisi.")

    return TransferPlan(
        salesman_id=str(src.get("salesman_id")).strip(),
        salesman_type=str(src.get("salesman_type") or "").strip(),
        salesman_name=name,
        from_distributor=frm,
        to_distributor=to,
        to_distributor_name=str(dest.get("name") or ""),
        to_region=str(dest.get("region") or ""),
        to_asm=str(dest.get("asm") or ""),
        spv=spv,
    )


# ── The atomic write ─────────────────────────────────────────────────────────

def _check_table(name: str) -> str:
    if not _TABLE_NAME.match(name or ""):
        raise ValueError("unsafe table name")
    return name


def build_transfer_script(mapping_table: str, hr_table: str) -> str:
    """The single BigQuery script that performs a transfer. Values are all
    query parameters (see `build_transfer_params`); only table names are
    interpolated, and those are validated."""
    mapping = _check_table(mapping_table)
    hr = _check_table(hr_table)
    return f"""
BEGIN
  BEGIN TRANSACTION;

  -- 1. Retire the current owner row. It must be exactly one active row, at the
  --    distributor the admin was shown; anything else means the data moved under
  --    us (or a double submit) and the whole transaction is abandoned.
  UPDATE `{mapping}`
  SET is_active = FALSE, updated_at = @now
  WHERE salesman_id = @sid
    AND is_active = TRUE
    AND UPPER(TRIM(distributor_code)) = @from_dist;
  IF @@row_count != 1 THEN
    RAISE USING MESSAGE = 'SOURCE_CHANGED';
  END IF;

  -- 2. New owner row. SAME salesman_id and type; never a generated ID.
  INSERT INTO `{mapping}`
    (salesman_id, salesman_type, distributor_code, salesman, is_active, created_at, updated_at)
  VALUES (@sid, @stype, @to_dist, @name, TRUE, @now, @now);

  -- 3. HR snapshot for the new distributor, copied inside BigQuery from the
  --    latest snapshot at the old one (no pay/phone data passes through the app).
  INSERT INTO `{hr}`
  SELECT * REPLACE (
      @to_dist   AS kode_distributor,
      @to_name   AS nama_distributor,
      @to_region AS region,
      @to_asm    AS asm,
      @spv_int   AS nama_spv_internal,
      @spv_ext   AS nama_spv_external,
      @spv_int2  AS nama_spv_internal_2,
      @now       AS uploaded_at)
  FROM `{hr}`
  WHERE UPPER(TRIM(nama_salesman)) = @name
    AND UPPER(TRIM(kode_distributor)) = @from_dist
  QUALIFY ROW_NUMBER() OVER (ORDER BY uploaded_at DESC) = 1;
  IF @@row_count != 1 THEN
    RAISE USING MESSAGE = 'HR_MISSING';
  END IF;

  -- 4. Post-condition: one active owner, and it is the destination.
  ASSERT (SELECT COUNT(*) FROM `{mapping}` WHERE salesman_id = @sid AND is_active) = 1
    AS 'POSTCONDITION: the ID must have exactly one active mapping row';
  ASSERT (SELECT COUNT(*) FROM `{mapping}`
          WHERE salesman_id = @sid AND is_active
            AND UPPER(TRIM(distributor_code)) = @to_dist) = 1
    AS 'POSTCONDITION: the active mapping row must be at the destination';

  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = @@error.message;
END;
"""


def _utc_now() -> str:
    # created_at / updated_at in gt_salesman_mapping are STRING 'YYYY-MM-DD HH:MM:SS' (UTC).
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def build_transfer_params(plan: TransferPlan, now: str) -> list:
    values = {
        "now": now,
        "sid": plan.salesman_id,
        "stype": plan.salesman_type,
        "name": plan.salesman_name,
        "from_dist": plan.from_distributor,
        "to_dist": plan.to_distributor,
        "to_name": plan.to_distributor_name,
        "to_region": plan.to_region,
        "to_asm": plan.to_asm,
        "spv_int": plan.spv["nama_spv_internal"],
        "spv_ext": plan.spv["nama_spv_external"],
        "spv_int2": plan.spv["nama_spv_internal_2"],
    }
    return [bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in values.items()]


def _explain(exc: Exception) -> str:
    text = str(exc)
    if "SOURCE_CHANGED" in text:
        return ("Salesman tidak lagi aktif di distributor asal — datanya berubah sejak ditinjau "
                "(atau pemindahan sudah dijalankan).")
    if "HR_MISSING" in text:
        return "Data HR salesman di distributor asal tidak ditemukan."
    if "POSTCONDITION" in text:
        return "Pemeriksaan akhir gagal: Salesman ID tidak berakhir dengan tepat satu pemilik aktif."
    first = text.strip().splitlines()[0] if text.strip() else type(exc).__name__
    return first[:300]


def fetch_active_owners(client, mapping_table: str, salesman_id: str) -> list:
    """Distinct distributor codes where the ID is currently active."""
    sql = f"""
        SELECT DISTINCT UPPER(TRIM(distributor_code)) AS distributor_code
        FROM `{_check_table(mapping_table)}`
        WHERE UPPER(TRIM(salesman_id)) = @sid AND is_active = TRUE
        ORDER BY 1
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("sid", "STRING", norm(salesman_id))])
    return [r["distributor_code"] for r in client.query(sql, job_config=cfg).result()]


def execute_transfer(client, plan: TransferPlan, mapping_table: str, hr_table: str,
                     now: str | None = None) -> tuple[bool, str]:
    """Run the transfer as ONE transactional script. Returns (ok, message).

    On failure BigQuery has already rolled the transaction back. When the client
    itself raises (e.g. the connection dropped after the job was submitted) the
    outcome is re-read from the data instead of guessed.
    """
    script = build_transfer_script(mapping_table, hr_table)
    params = build_transfer_params(plan, now or _utc_now())
    try:
        client.query(script, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
        return True, ""
    except Exception as exc:  # noqa: BLE001
        reason = _explain(exc)
        try:
            owners = fetch_active_owners(client, mapping_table, plan.salesman_id)
        except Exception:  # noqa: BLE001
            owners = None
        if owners == [plan.to_distributor]:
            return True, ("Koneksi terputus sebelum konfirmasi diterima, tetapi data menunjukkan "
                          f"`{plan.salesman_id}` SUDAH aktif di {plan.to_distributor}. "
                          "Jangan diulang.")
        if owners == [plan.from_distributor]:
            return False, (f"Pemindahan dibatalkan, tidak ada data yang berubah: {reason} "
                           f"(`{plan.salesman_id}` tetap aktif di {plan.from_distributor}).")
        if owners is None:
            return False, (f"Pemindahan gagal: {reason} Status akhir tidak dapat dipastikan — "
                           "periksa roster kedua distributor sebelum mencoba lagi.")
        return False, (f"PERHATIAN: pemindahan gagal ({reason}) dan `{plan.salesman_id}` kini aktif "
                       f"di: {', '.join(owners) or '(tidak ada)'}. Hubungi admin data sebelum "
                       "mengulang.")


# ── Reads used by the admin screen ───────────────────────────────────────────

def fetch_mapping_rows(client, mapping_table: str, salesman_id: str, to_distributor: str) -> list:
    """Every mapping row for the ID, plus the destination's active rows (for the
    same-name check). Fresh read, never cached."""
    sql = f"""
        SELECT salesman_id, salesman_type, UPPER(TRIM(distributor_code)) AS distributor_code,
               TRIM(salesman) AS salesman, is_active, created_at, updated_at
        FROM `{_check_table(mapping_table)}`
        WHERE UPPER(TRIM(salesman_id)) = @sid
           OR (is_active = TRUE AND UPPER(TRIM(distributor_code)) = @to)
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("sid", "STRING", norm(salesman_id)),
        bigquery.ScalarQueryParameter("to", "STRING", norm(to_distributor)),
    ])
    return [dict(r.items()) for r in client.query(sql, job_config=cfg).result()]


def fetch_latest_hr_row(client, hr_table: str, name: str, distributor_code: str):
    """The latest HR snapshot for name + distributor — only the columns the
    screen needs (no pay or phone data). None if there is none."""
    sql = f"""
        SELECT nama_salesman, nama_spv_internal, nama_spv_external, nama_spv_internal_2,
               status_salesman, uploaded_at
        FROM `{_check_table(hr_table)}`
        WHERE UPPER(TRIM(nama_salesman)) = @name AND UPPER(TRIM(kode_distributor)) = @dist
        ORDER BY uploaded_at DESC
        LIMIT 1
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("name", "STRING", clean_name(name)),
        bigquery.ScalarQueryParameter("dist", "STRING", norm(distributor_code)),
    ])
    rows = list(client.query(sql, job_config=cfg).result())
    return dict(rows[0].items()) if rows else None


def load_plan_inputs(client, mapping_table: str, hr_table: str, salesman_id: str,
                     to_distributor: str):
    """(mapping_rows, hr_row) for `plan_transfer`, read fresh. The HR row is only
    looked up when the ID has exactly one active owner; otherwise `plan_transfer`
    refuses before it would ever need it."""
    rows = fetch_mapping_rows(client, mapping_table, salesman_id, to_distributor)
    active = [r for r in rows
              if norm(r.get("salesman_id")) == norm(salesman_id) and _is_true(r.get("is_active"))]
    hr_row = None
    if len(active) == 1:
        hr_row = fetch_latest_hr_row(client, hr_table, active[0].get("salesman"),
                                     active[0].get("distributor_code"))
    return rows, hr_row


def count_pjp_rows(client, pjp_table: str, salesman_id: str, distributor_code: str,
                   snapshot_month: str) -> int:
    """PJP rows of this ID under a distributor in one month. A transfer leaves
    them where they are; the screen shows the count so nobody is surprised."""
    sql = f"""
        SELECT COUNT(*) AS n FROM `{_check_table(pjp_table)}`
        WHERE UPPER(TRIM(salesman_id)) = @sid
          AND UPPER(TRIM(kode_distributor)) = @dist
          AND snapshot_month = @month
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("sid", "STRING", norm(salesman_id)),
        bigquery.ScalarQueryParameter("dist", "STRING", norm(distributor_code)),
        bigquery.ScalarQueryParameter("month", "STRING", snapshot_month),
    ])
    rows = list(client.query(sql, job_config=cfg).result())
    return int(rows[0]["n"]) if rows else 0
