"""Moving a salesman to another distributor while KEEPING the salesman_id.

Business case: SE ANDI (MIXDST358003, DST358 Palembang) moves to DST364
Prabumulih covering the same stores. The ID is the salesman's identity; adding
him as a "new" salesman at DST364 minted a second ID (the abandoned
GTIDST364001). salesman_transfer.py moves the existing ID instead.

These tests are hermetic (fake BigQuery client, no credentials). The SQL itself
-- transaction, rollback, assertions -- is proven against real BigQuery by
tests/test_salesman_transfer_live.py (opt-in; synthetic rows in TEMP tables).

Run: pytest tests/test_salesman_transfer.py -q
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
for _p in (str(REPO), str(TESTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import salesman_transfer as T  # noqa: E402
from test_pjp_import import SP, _row, _workbook  # noqa: E402

SRC = REPO / "salesman_pjp.py"
MAPPING = "proj.gt_schema.gt_salesman_mapping"
HR = "proj.gt_schema.gt_master_salesman"
PJP = "proj.gt_schema.gt_master_salesman_pjp"

DESTINATIONS = {
    "DST358": {"name": "PT SUKSES JAYA PERMATA - PALEMBANG", "region": "SOUTHERN SUMATERA 1", "asm": "ASM A"},
    "DST364": {"name": "PT SUKSES JAYA PERMATA - PRABUMULIH", "region": "SOUTHERN SUMATERA 1", "asm": "ASM B"},
}


def _map(sid, dist, name, active=True, stype="MIX"):
    return {"salesman_id": sid, "salesman_type": stype, "distributor_code": dist,
            "salesman": name, "is_active": active,
            "created_at": "2026-11-06 02:52:34", "updated_at": "2026-11-06 02:52:34"}


ANDI = _map("MIXDST358003", "DST358", "ANDI")
HR_ROW = {"nama_salesman": "ANDI", "nama_spv_internal": "SPV CONTOH",
          "nama_spv_external": None, "nama_spv_internal_2": None,
          "status_salesman": "Eksklusif", "uploaded_at": "2026-10-09 08:21:33"}


def _plan(rows=None, hr=HR_ROW, to="DST364", sid="MIXDST358003", **kw):
    return T.plan_transfer(sid, to, rows if rows is not None else [ANDI], hr,
                           kw.pop("destinations", DESTINATIONS), **kw)


def _refused(code, **kw):
    with pytest.raises(T.TransferError) as exc:
        _plan(**kw)
    assert exc.value.code == code
    assert exc.value.message
    return exc.value


# ─── 1. A new salesman still gets a generated ID, exactly as before ──────────

def _extract(*names):
    tree = ast.parse(SRC.read_text(encoding="utf-8"), filename=str(SRC))
    picked = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in picked} == set(names)
    mod = ast.Module(body=picked, type_ignores=[])
    ast.fix_missing_locations(mod)
    return mod


@pytest.mark.parametrize("dist,stype,latest,expected", [
    ("DST364", "GTI", 1, "GTIDST364002"),   # next after the abandoned GTIDST364001
    ("DST358", "MIX", 3, "MIXDST358004"),   # 003 (Andi) is never reused at DST358
    ("DST364", "MIX", 0, "MIXDST364001"),   # first MIX at DST364 -- Andi's ID does not count
])
def test_new_salesman_still_generates_the_next_id(dist, stype, latest, expected):
    ns = {"get_latest_running_number": lambda d, t: latest}
    exec(compile(_extract("generate_salesman_id"), str(SRC), "exec"), ns)
    assert ns["generate_salesman_id"](dist, stype) == expected


# ─── 2. The Andi transfer keeps MIXDST358003 ─────────────────────────────────

def test_andi_transfer_keeps_the_existing_id_and_type():
    plan = _plan()
    assert plan.salesman_id == "MIXDST358003"
    assert plan.salesman_type == "MIX"
    assert (plan.from_distributor, plan.to_distributor) == ("DST358", "DST364")
    assert plan.salesman_name == "ANDI"
    assert plan.to_distributor_name == "PT SUKSES JAYA PERMATA - PRABUMULIH"
    assert plan.spv["nama_spv_internal"] == "SPV CONTOH"


def test_no_new_id_is_ever_written_for_the_destination():
    plan = _plan()
    params = {p.name: p.value for p in T.build_transfer_params(plan, "2026-11-09 10:00:00")}
    assert params["sid"] == "MIXDST358003"
    assert not any(isinstance(v, str) and re.fullmatch(r"[A-Z]{3}DST364\d{3}", v)
                   for v in params.values())


def test_prefix_of_the_id_is_not_required_to_match_the_destination():
    # MIXDST358 vs DST364 is the whole point, and 85 active IDs already differ
    # from their distributor (legacy GT* ids such as GTIDST1411 at DST349).
    plan = _plan(rows=[_map("GTIDST1411", "DST349", "GUSTY", stype="GT")],
                 hr={**HR_ROW, "nama_salesman": "GUSTY"}, sid="GTIDST1411",
                 destinations={**DESTINATIONS, "DST349": {"name": "X", "region": "R", "asm": "A"}})
    assert plan.salesman_id == "GTIDST1411" and plan.to_distributor == "DST364"


def test_input_is_normalized_but_the_stored_id_is_kept():
    plan = _plan(sid="  mixdst358003 ", to=" dst364 ")
    assert plan.salesman_id == "MIXDST358003" and plan.to_distributor == "DST364"


def test_history_rows_do_not_confuse_the_plan():
    old_inactive = _map("MIXDST358003", "DST358", "ANDI", active=False)
    assert _plan(rows=[old_inactive, ANDI]).from_distributor == "DST358"


def test_spv_can_be_changed_for_the_new_distributor_blank_keeps_the_old_one():
    plan = _plan(spv_overrides={"nama_spv_internal": " budi santoso ", "nama_spv_external": ""})
    assert plan.spv["nama_spv_internal"] == "BUDI SANTOSO"
    assert plan.spv["nama_spv_external"] is None


# ─── 3. Refusals ─────────────────────────────────────────────────────────────

def test_unknown_id_is_refused():
    _refused(T.NOT_FOUND, sid="MIXDST999999")


def test_empty_id_is_refused():
    _refused(T.EMPTY_ID, sid="  ")


def test_same_distributor_is_refused_not_silently_accepted():
    err = _refused(T.SAME_DISTRIBUTOR, to="dst358")
    assert "Tidak ada yang dipindahkan" in err.message


def test_inactive_salesman_cannot_be_moved():
    _refused(T.INACTIVE, rows=[_map("MIXDST358003", "DST358", "ANDI", active=False)])


def test_an_id_active_in_two_distributors_is_refused_never_auto_repaired():
    twin = _map("MIXDST358003", "DST360", "ANDI")
    err = _refused(T.AMBIGUOUS_OWNER, rows=[ANDI, twin])
    assert "DST358" in err.message and "DST360" in err.message


def test_unknown_or_inactive_destination_is_refused():
    _refused(T.UNKNOWN_DESTINATION, to="DST999")
    _refused(T.UNKNOWN_DESTINATION, to="")


def test_destination_with_an_active_namesake_is_refused():
    # roster and HR are joined by name + distributor, so two ANDI would swap data
    other = _map("GTIDST364002", "DST364", "andi", stype="GTI")
    err = _refused(T.NAME_TAKEN, rows=[ANDI, other])
    assert "GTIDST364002" in err.message


def test_inactive_namesake_at_destination_is_fine():
    # GTIDST364001 / ANDI PRATAMA is the abandoned attempt; it must not block the move
    stale = _map("GTIDST364001", "DST364", "ANDI", active=False, stype="GTI")
    assert _plan(rows=[ANDI, stale]).to_distributor == "DST364"


def test_missing_hr_row_is_refused_instead_of_leaving_a_blank_roster_entry():
    _refused(T.NO_HR_ROW, hr=None)


def test_spv_internal_is_required():
    _refused(T.NO_SPV, hr={**HR_ROW, "nama_spv_internal": None})


def test_nan_values_from_pandas_are_treated_as_blank():
    nan = float("nan")
    plan = _plan(hr={**HR_ROW, "nama_spv_external": nan, "nama_spv_internal_2": nan})
    assert plan.spv["nama_spv_external"] is None


def test_clean_name_matches_the_name_rule_used_when_salesmen_are_created():
    ns = {"re": re}
    exec(compile(_extract("sanitize_salesman_name"), str(SRC), "exec"), ns)
    for raw in ("  andi  pratama ", "ANDI", "m  andi\tperdana", "Al 'Afis"):
        assert T.clean_name(raw) == ns["sanitize_salesman_name"](raw)


# ─── 4. The write is ONE transactional script ────────────────────────────────

SCRIPT = T.build_transfer_script(MAPPING, HR)
# What the engine executes: comments are explanation, not behaviour.
CODE = "\n".join(line.split("--")[0] for line in SCRIPT.splitlines())


def test_script_is_a_single_transaction_with_rollback_on_any_error():
    assert SCRIPT.count("BEGIN TRANSACTION") == 1
    assert SCRIPT.count("COMMIT TRANSACTION") == 1
    assert "EXCEPTION WHEN ERROR THEN" in SCRIPT
    assert SCRIPT.index("ROLLBACK TRANSACTION") > SCRIPT.index("EXCEPTION WHEN ERROR")
    assert SCRIPT.index("BEGIN TRANSACTION") < SCRIPT.index("UPDATE") < SCRIPT.index("COMMIT TRANSACTION")


def test_script_retires_exactly_one_active_row_at_the_shown_distributor():
    update = SCRIPT[SCRIPT.index("UPDATE"):SCRIPT.index("INSERT")]
    assert "is_active = FALSE" in update
    assert "salesman_id = @sid" in update
    assert "is_active = TRUE" in update
    assert "UPPER(TRIM(distributor_code)) = @from_dist" in update
    assert "@@row_count != 1" in update and "SOURCE_CHANGED" in update


def test_script_inserts_the_same_id_never_a_generated_one():
    insert = SCRIPT[SCRIPT.index(f"INSERT INTO `{MAPPING}`"):SCRIPT.index(f"INSERT INTO `{HR}`")]
    assert "VALUES (@sid, @stype, @to_dist, @name, TRUE, @now, @now)" in insert
    assert "generate" not in CODE.lower()


def test_script_copies_the_hr_snapshot_inside_bigquery():
    hr_part = SCRIPT[SCRIPT.index(f"INSERT INTO `{HR}`"):]
    assert "SELECT * REPLACE" in hr_part
    assert "@to_dist   AS kode_distributor" in hr_part
    assert "QUALIFY ROW_NUMBER() OVER (ORDER BY uploaded_at DESC) = 1" in hr_part
    assert "HR_MISSING" in hr_part


def test_script_asserts_one_active_owner_at_the_destination():
    asserts = re.findall(r"ASSERT\s*\((.*?)\)\s*=\s*1\s*AS", CODE, flags=re.S)
    assert len(asserts) == 2
    anywhere, at_destination = asserts
    assert "salesman_id = @sid" in anywhere and "is_active" in anywhere
    assert "@to_dist" not in anywhere          # exactly one active row, wherever it is
    assert "salesman_id = @sid" in at_destination and "is_active" in at_destination
    assert "UPPER(TRIM(distributor_code)) = @to_dist" in at_destination


def test_script_is_non_destructive_and_leaves_pjp_alone():
    low = CODE.lower()
    assert "delete" not in low and "truncate" not in low and "drop " not in low
    assert "master_salesman_pjp" not in low


def test_values_are_parameters_only_never_interpolated():
    assert "MIXDST358003" not in SCRIPT and "ANDI" not in SCRIPT and "DST364" not in SCRIPT


@pytest.mark.parametrize("bad", ["x`; DROP TABLE y; --", "a b", "", "t;t"])
def test_unsafe_table_names_are_rejected(bad):
    with pytest.raises(ValueError):
        T.build_transfer_script(bad, HR)


def test_every_script_parameter_is_supplied_and_nothing_else():
    plan = _plan()
    supplied = {p.name for p in T.build_transfer_params(plan, "2026-11-09 10:00:00")}
    used = set(re.findall(r"@([a-z_0-9]+)", SCRIPT)) - {"error"}  # @@error.message is a system variable
    used -= {"row_count"}
    assert supplied == used


# ─── 5. Execution and partial failure ────────────────────────────────────────

class _Job:
    def __init__(self, rows=None, error=None):
        self._rows, self._error = rows or [], error

    def result(self):
        if self._error:
            raise self._error
        return self._rows


class _Client:
    """Routes by SQL text. Records everything; the script is the only write."""

    def __init__(self, script_error=None, owners=(), owners_error=None):
        self.queries = []
        self.script_error, self.owners, self.owners_error = script_error, list(owners), owners_error

    def query(self, sql, job_config=None):
        self.queries.append(sql)
        if "BEGIN TRANSACTION" in sql:
            return _Job(error=self.script_error)
        if self.owners_error:
            return _Job(error=self.owners_error)
        return _Job(rows=[{"distributor_code": c} for c in self.owners])

    @property
    def writes(self):
        return [q for q in self.queries if "BEGIN TRANSACTION" in q]


def test_success_is_one_job_and_nothing_else():
    client = _Client()
    ok, msg = T.execute_transfer(client, _plan(), MAPPING, HR, now="2026-11-09 10:00:00")
    assert (ok, msg) == (True, "")
    assert len(client.queries) == 1  # no separate deactivate / insert jobs that could half-apply


def test_job_failure_reports_cancelled_and_names_the_unchanged_owner():
    client = _Client(script_error=RuntimeError("HR_MISSING"), owners=["DST358"])
    ok, msg = T.execute_transfer(client, _plan(), MAPPING, HR)
    assert ok is False
    assert "dibatalkan" in msg and "tidak ada data yang berubah" in msg
    assert "MIXDST358003" in msg and "tetap aktif di DST358" in msg
    assert len(client.writes) == 1  # no retry, no compensating write


def test_failed_job_never_leaves_two_active_owners_in_what_it_reports():
    # Whatever the engine says, the report is built from a re-read of the data
    client = _Client(script_error=RuntimeError("boom"), owners=["DST358", "DST364"])
    ok, msg = T.execute_transfer(client, _plan(), MAPPING, HR)
    assert ok is False and "PERHATIAN" in msg and "DST358, DST364" in msg


def test_dropped_connection_after_commit_is_recognised_as_done_not_retried():
    client = _Client(script_error=ConnectionError("reset"), owners=["DST364"])
    ok, msg = T.execute_transfer(client, _plan(), MAPPING, HR)
    assert ok is True and "SUDAH aktif di DST364" in msg and "Jangan diulang" in msg


def test_unknown_outcome_is_said_plainly():
    client = _Client(script_error=ConnectionError("reset"), owners_error=ConnectionError("still down"))
    ok, msg = T.execute_transfer(client, _plan(), MAPPING, HR)
    assert ok is False and "tidak dapat dipastikan" in msg


def test_double_submit_message_is_translated_for_the_admin():
    client = _Client(script_error=RuntimeError("SOURCE_CHANGED"), owners=["DST364"])
    ok, msg = T.execute_transfer(client, _plan(), MAPPING, HR)
    # the second click finds the ID already at DST364: reported as done, not as an error
    assert ok is True and "DST364" in msg


def test_raw_engine_text_is_trimmed_to_one_line():
    err = RuntimeError("first line\nsecond line with the query text SELECT secrets")
    client = _Client(script_error=err, owners=["DST358"])
    ok, msg = T.execute_transfer(client, _plan(), MAPPING, HR)
    assert "first line" in msg and "second line" not in msg and "secrets" not in msg


# ─── 6. Fresh reads ──────────────────────────────────────────────────────────

class _ReadClient:
    def __init__(self, mapping, hr):
        self.mapping, self.hr, self.sql = mapping, hr, []

    def query(self, sql, job_config=None):
        self.sql.append(sql)
        return _Job(rows=self.hr if "nama_spv_internal" in sql else self.mapping)


def test_load_plan_inputs_reads_hr_of_the_current_owner_only():
    client = _ReadClient([ANDI], [HR_ROW])
    rows, hr = T.load_plan_inputs(client, MAPPING, HR, "MIXDST358003", "DST364")
    assert rows == [ANDI] and hr == HR_ROW
    assert len(client.sql) == 2


def test_load_plan_inputs_skips_the_hr_read_when_the_owner_is_ambiguous():
    twin = _map("MIXDST358003", "DST360", "ANDI")
    client = _ReadClient([ANDI, twin], [HR_ROW])
    rows, hr = T.load_plan_inputs(client, MAPPING, HR, "MIXDST358003", "DST364")
    assert hr is None and len(client.sql) == 1


def test_hr_read_selects_no_pay_or_phone_columns():
    client = _ReadClient([], [HR_ROW])
    T.fetch_latest_hr_row(client, HR, "ANDI", "DST358")
    sql = client.sql[0].lower()
    assert "gaji" not in sql and "no_hp" not in sql and "tunjangan" not in sql


def test_pjp_count_reads_only_and_scopes_by_id_distributor_and_month():
    client = _ReadClient([{"n": 7}], [])
    assert T.count_pjp_rows(client, PJP, "MIXDST358003", "DST358", "2026-11") == 7
    sql = client.sql[0]
    assert sql.lstrip().upper().startswith("SELECT")
    assert "snapshot_month = @month" in sql


# ─── 7. PJP: old stays readable, new validates, ownership stays exclusive ────

def _roster(*rows):
    return pd.DataFrame({
        "salesman_id": [r[0] for r in rows], "salesman": [r[2] for r in rows],
        "salesman_label": [r[0] for r in rows], "distributor_code": [r[1] for r in rows]})


def _stores(dist):
    return pd.DataFrame({
        "store_code": ["ST00001"], "store_name": ["TOKO A"], "region": ["R"], "asm": ["A"],
        "distributor_name": ["D"], "distributor_code": [dist], "store_label": ["ST00001"]})


def _validate(sid, selected, roster, store_dist=None):
    stores = _stores(store_dist or selected)
    df = SP["read_template_sheet"](
        _workbook([_row(**{"Salesman ID": sid, "Kode Toko": "ST00001"}, Frekuensi="F4", Hari="SENIN")]),
        "PJP Template", 2, roster, stores)
    df = df[df["kode_toko"].notna() & (df["kode_toko"].astype(str).str.strip() != "")].reset_index(drop=True)
    return SP["validate_pjp_df"](df, {selected: "D"}, store_df=stores, salesman_df=roster,
                                 selected_dist_code=selected)[0]


# roster = ACTIVE rows (load_all_salesman_mapping), as it looks right after the transfer
AFTER = _roster(("MIXDST358003", "DST364", "ANDI"), ("GTIDST358004", "DST358", "RIO"))


def test_new_pjp_at_dst364_with_the_old_prefix_validates():
    assert _validate("MIXDST358003", "DST364", AFTER) == []


def test_the_same_id_cannot_be_used_for_pjp_at_the_old_distributor():
    errors = _validate("MIXDST358003", "DST358", AFTER)
    assert len(errors) == 1
    assert "bukan milik distributor yang dipilih" in errors[0]
    assert "DST364" in errors[0]


def test_other_salesmen_of_the_old_distributor_are_unaffected():
    assert _validate("GTIDST358004", "DST358", AFTER) == []


def test_before_the_transfer_the_id_belongs_to_the_old_distributor():
    before = _roster(("MIXDST358003", "DST358", "ANDI"))
    assert _validate("MIXDST358003", "DST358", before) == []
    assert _validate("MIXDST358003", "DST364", before)


def test_a_deactivated_old_row_is_not_in_the_active_roster_so_it_cannot_resurrect_ownership():
    # load_all_salesman_mapping() only returns is_active rows: after the transfer the
    # DST358 row is inactive and absent, so only the DST364 row can own the ID.
    ids = AFTER[AFTER["salesman_id"] == "MIXDST358003"]
    assert list(ids["distributor_code"]) == ["DST364"]


# ─── 8. Wiring: admin-only, and PJP writes re-check ownership ────────────────

_SRC_TEXT = SRC.read_text(encoding="utf-8")
_TREE = ast.parse(_SRC_TEXT)


def _fn(name):
    return next(n for n in _TREE.body if isinstance(n, ast.FunctionDef) and n.name == name)


def _mentions(node, name):
    return any(isinstance(n, ast.Name) and n.id == name for n in ast.walk(node))


def test_transfer_screen_guards_with_require_admin_before_anything_else():
    seg = ast.get_source_segment(_SRC_TEXT, _fn("_render_transfer_salesman"))
    assert "if not require_admin():" in seg
    assert seg.index("require_admin()") < seg.index("load_all_salesman_mapping()")
    assert seg.index("require_admin()") < seg.index("execute_transfer")
    # a distributor-scoped guard would be the wrong authority for a two-distributor action
    assert "assert_distributor_write_access" not in seg


def test_transfer_screen_is_reachable_only_from_the_admin_dashboard():
    for node in _TREE.body:
        if isinstance(node, ast.FunctionDef) and node.name in ("_render_transfer_salesman",
                                                                "render_admin_dashboard"):
            continue
        assert not _mentions(node, "_render_transfer_salesman"), \
            "the transfer screen leaked into a distributor-facing path"
    assert _mentions(_fn("render_admin_dashboard"), "_render_transfer_salesman")


def test_pjp_update_rereads_salesman_ownership_before_it_deletes_anything():
    assert _SRC_TEXT.count("load_all_salesman_mapping.clear()") == 1
    block_start = _SRC_TEXT.index("load_all_salesman_mapping.clear()")
    assert block_start < _SRC_TEXT.index("ok_del, err_del = delete_pjp_records(")
    assert block_start > _SRC_TEXT.index("if not assert_distributor_write_access(scope_dist_code):")
