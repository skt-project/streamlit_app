"""One-to-many PI mapping rules for the RSA PI Validation tab.

Run: pytest tests/test_po_pi_map.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from po_pi_map import plan_pi_mapping, render_ref_badges_html  # noqa: E402

DIST = "AGUNG TERNATE"
FILE = "1ST PO Glad2glow Agung Ternate.xlsx"
REFS = [
    "G2G_AGUNG_TERNATE_26",
    "G2G_AGUNG_TERNATE_26-1",
    "G2G_AGUNG_TERNATE_26-2",
]


def test_accepts_multiple_confirmed_refs_with_one_primary():
    plan = plan_pi_mapping(DIST, FILE, REFS, REFS, REFS[0], "Mapping PI by RSA")
    assert plan.ok
    assert plan.selection.selected_refs == tuple(REFS)
    assert plan.selection.primary_ref == REFS[0]


def test_rejects_empty_selection():
    plan = plan_pi_mapping(DIST, FILE, REFS, [], REFS[0], "Mapping PI by RSA")
    assert not plan.ok
    assert any("minimal satu" in message for message in plan.errors)


def test_rejects_primary_outside_selection():
    plan = plan_pi_mapping(DIST, FILE, REFS, REFS[:2], REFS[2], "Mapping PI by RSA")
    assert not plan.ok
    assert any("Primary PI harus termasuk" in message for message in plan.errors)


def test_rejects_unknown_candidate():
    plan = plan_pi_mapping(
        DIST, FILE, REFS, [REFS[0], "BUKAN-DARI-DMS"], REFS[0], "Mapping PI by RSA"
    )
    assert not plan.ok
    assert any("tidak ada di kandidat" in message for message in plan.errors)


def test_rejects_blank_note():
    plan = plan_pi_mapping(DIST, FILE, REFS, REFS[:1], REFS[0], "   ")
    assert not plan.ok
    assert any("Validation by RSA" in message for message in plan.errors)


def test_primary_badge_uses_distinct_markup():
    html = render_ref_badges_html(REFS, REFS[1])
    assert "PRIMARY · G2G_AGUNG_TERNATE_26-1" in html
    assert html.count("PRIMARY ·") == 1
    assert "#0B6E4F" in html
    assert "G2G_AGUNG_TERNATE_26-2" in html
    assert "G2G_AGUNG_TERNATE_26<" in html or "G2G_AGUNG_TERNATE_26</span>" in html


def test_dedupes_selected_refs_and_keeps_order():
    plan = plan_pi_mapping(
        DIST,
        FILE,
        REFS,
        [REFS[1], REFS[1], REFS[0]],
        REFS[1],
        "Mapping PI by RSA",
    )
    assert plan.ok
    assert plan.selection.selected_refs == (REFS[1], REFS[0])
