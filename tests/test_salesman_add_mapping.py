"""Pin the write order of "Tambah Salesman Baru".

salesman_id lives only in gt_salesman_mapping; the HR row in gt_master_salesman
is joined to it by name + distributor. On 2026-10-09 an SE (ANDI, DST358) was
saved to the HR table without a mapping row, so the SE Database showed a blank
salesman_id. The old flow wrote HR first and mapping second with no rollback.

These tests pin the replacement: mapping first, HR second, and a failed HR write
cancels the mapping it just created.

salesman_pjp.py executes Streamlit UI code at import time and cannot be imported
directly; the needed definitions are AST-extracted into a clean namespace, the
same technique tests/test_pjp_import.py uses.

Run: pytest tests/test_salesman_add_mapping.py -q
"""
from __future__ import annotations

import ast
import types
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "salesman_pjp.py"


def _extract(*names):
    tree = ast.parse(SRC.read_text(encoding="utf-8"), filename=str(SRC))
    picked = [
        n for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name in names
    ]
    assert {n.name for n in picked} == set(names), "function missing from salesman_pjp.py"
    mod = ast.Module(body=picked, type_ignores=[])
    ast.fix_missing_locations(mod)
    return mod


def _build(calls, *, map_result=(True, ""), hr_result=(True, ""), undo_result=(True, "")):
    """add_salesman_with_mapping wired to recording fakes (no BigQuery)."""
    ns = {
        "insert_mapping_record": lambda *a, **k: (calls.append("mapping"), map_result)[1],
        "insert_salesman_record": lambda *a, **k: (calls.append("hr"), hr_result)[1],
        "deactivate_previous_mapping": lambda sid: (calls.append(f"undo:{sid}"), undo_result)[1],
    }
    exec(compile(_extract("add_salesman_with_mapping"), str(SRC), "exec"), ns)
    return ns["add_salesman_with_mapping"]


ARGS = ({"nama_salesman": "ANDI"}, "MIXDST358003", "DST358", "MIX", "ANDI")


def test_mapping_is_written_before_the_hr_row():
    calls = []
    ok, err = _build(calls)(*ARGS[:4], nama_salesman=ARGS[4])
    assert (ok, err) == (True, "")
    assert calls == ["mapping", "hr"]


def test_mapping_failure_writes_no_hr_row():
    """The ANDI case: nothing may be left behind that has no ID."""
    calls = []
    ok, err = _build(calls, map_result=(False, "boom"))(*ARGS[:4], nama_salesman=ARGS[4])
    assert ok is False
    assert calls == ["mapping"]
    assert "boom" in err and "Tidak ada data" in err


def test_hr_failure_cancels_the_mapping_it_just_created():
    calls = []
    ok, err = _build(calls, hr_result=(False, "hr down"))(*ARGS[:4], nama_salesman=ARGS[4])
    assert ok is False
    assert calls == ["mapping", "hr", "undo:MIXDST358003"]
    assert "hr down" in err and "dibatalkan" in err


def test_failed_cancel_is_reported_with_the_id_not_hidden():
    calls = []
    ok, err = _build(
        calls, hr_result=(False, "hr down"), undo_result=(False, "undo down")
    )(*ARGS[:4], nama_salesman=ARGS[4])
    assert ok is False
    assert "MIXDST358003" in err and "undo down" in err and "hubungi admin" in err.lower()


def test_running_number_does_not_swallow_bigquery_errors():
    """A failed lookup used to return 0, so the next ID became ...001 and could
    duplicate an existing code. It must raise instead."""
    class Boom:
        def __init__(self, *a, **k):
            raise RuntimeError("bq unavailable")

    bigquery = types.SimpleNamespace(
        Client=Boom,
        QueryJobConfig=lambda **k: None,
        ScalarQueryParameter=lambda *a: None,
    )
    ns = {
        "bigquery": bigquery,
        "get_credentials": lambda: (None, "proj"),
        "MAPPING_TABLE": "t",
        "pd": __import__("pandas"),
    }
    exec(compile(_extract("get_latest_running_number"), str(SRC), "exec"), ns)
    with pytest.raises(RuntimeError):
        ns["get_latest_running_number"]("DST358", "MIX")
