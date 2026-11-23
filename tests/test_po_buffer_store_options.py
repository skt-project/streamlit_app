"""Regression: the store multiselect crashed with "unhashable type: 'Series'".

Cause (found on real data, 2026-11-23): 401 stores from the Kalimantan/Sulawesi
staging sheet have an ``id_st`` that is not in ``rsa.inventory_buffer``, so the
LEFT JOIN gave them ``store_code = NULL``. All 401 names then shared one NULL
code, ``store_options.set_index("store_code").loc[nan, "label"]`` returned a
Series, and ``st.multiselect`` raised before the store list was drawn.

Two layers are covered:
* the SQL gives every staging store an identity (its id_st) and never lists a
  store/SKU twice;
* the UI option builder cannot crash whatever the data holds.

Needs ``streamlit`` to be importable (po_buffer.py runs set_page_config on import).

Run: pytest tests/test_po_buffer_store_options.py -q
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

logging.disable(logging.CRITICAL)
try:
    import po_buffer
except Exception as exc:  # pragma: no cover - environment dependent
    pytest.skip(f"po_buffer needs a working streamlit import: {exc}", allow_module_level=True)
finally:
    logging.disable(logging.NOTSET)


def frame(rows):
    return pd.DataFrame(rows, columns=["store_code", "store_name"])


# ---------------------------------------------------------------------------
# UI option builder
# ---------------------------------------------------------------------------

def test_one_option_per_store_code_with_a_readable_label():
    codes, labels = po_buffer.build_store_options(
        frame([("B2", "Beta"), ("A1", "Toko A"), ("A1", "Toko A"), ("A1", "Toko A")])
    )
    assert codes == ["A1", "B2"]
    assert labels == {"A1": "A1 - Toko A", "B2": "B2 - Beta"}


def test_hundreds_of_stores_without_a_code_do_not_crash_and_are_not_offered():
    """The real failure: 400 different names under one NULL code."""
    unmapped = [(None, f"STAGING STORE {n}") for n in range(400)]
    codes, labels = po_buffer.build_store_options(frame([("A1", "Toko A")] + unmapped))
    assert codes == ["A1"] and list(labels) == ["A1"]


def test_every_label_is_a_plain_string_lookup_not_a_series():
    codes, labels = po_buffer.build_store_options(
        frame([("A1", "Toko A"), (None, "X"), (None, "Y"), ("B2", None), ("B2", "")])
    )
    for code in codes:
        assert isinstance(labels[code], str)
        hash(labels[code])


def test_a_code_with_several_names_gives_one_option_and_prefers_a_real_name():
    codes, labels = po_buffer.build_store_options(
        frame([("C3", None), ("C3", ""), ("C3", "Toko Tiga"), ("C3", "Toko Tiga Baru")])
    )
    assert codes == ["C3"]
    assert labels["C3"] == "C3 - Toko Tiga"


def test_a_store_without_a_name_is_still_selectable():
    codes, labels = po_buffer.build_store_options(frame([("D4", None)]))
    assert codes == ["D4"] and labels["D4"] == "D4 - (tanpa nama)"


def test_options_are_sorted_by_label():
    codes, _ = po_buffer.build_store_options(
        frame([("Z9", "Alfa"), ("A1", "Zulu"), ("M5", "Mike")])
    )
    assert codes == ["A1", "M5", "Z9"]     # "A1 - Zulu" < "M5 - Mike" < "Z9 - Alfa"


def test_empty_or_all_unmapped_input_gives_no_options():
    assert po_buffer.build_store_options(frame([])) == ([], {})
    assert po_buffer.build_store_options(frame([(None, "X"), (float("nan"), "Y")])) == ([], {})


# ---------------------------------------------------------------------------
# Region / distributor options: only those that have stores
# ---------------------------------------------------------------------------

def test_master_is_restricted_to_distributors_present_in_the_inventory():
    master = pd.DataFrame({
        "region_g2g": ["East Kalimantan", "West Java", "West Java", "Southern Sulawesi"],
        "distributor": ["PT A", "PT B", "PT C", "PT D"],
    })
    inventory = pd.DataFrame({"distributor_g2g": [" PT A", "PT D ", "PT A", None, "PT NOT IN MASTER"]})
    kept = po_buffer.restrict_master_to_inventory(master, inventory)
    assert sorted(kept["region_g2g"].unique()) == ["East Kalimantan", "Southern Sulawesi"]
    assert sorted(kept["distributor"]) == ["PT A", "PT D"]       # whitespace in the data is ignored


def test_an_empty_inventory_offers_no_region_at_all():
    master = pd.DataFrame({"region_g2g": ["West Java"], "distributor": ["PT B"]})
    assert po_buffer.restrict_master_to_inventory(master, pd.DataFrame({"distributor_g2g": []})).empty


# ---------------------------------------------------------------------------
# SQL guards (the data-level fix is verified against BigQuery separately)
# ---------------------------------------------------------------------------

def test_a_staging_store_unknown_to_inventory_keeps_its_id_st_as_store_code():
    sql = po_buffer.combined_inventory_sql()
    assert "COALESCE(map.store_code, staging.id_st) AS store_code" in sql
    assert "map.store_code AS store_code" not in sql


def test_rows_are_grouped_per_store_and_sku_so_a_sku_is_never_listed_twice():
    sql = po_buffer.combined_inventory_sql()
    group_by = sql.rsplit("GROUP BY", 1)[1].split(") AS grouped_inventory")[0]
    assert "store_code" in group_by and "product_code" in group_by
    assert "id_st" not in group_by           # id_st differs between the two sources of one store
    assert "MAX(id_st) AS id_st" in sql      # ... and is only carried along


def test_the_store_summary_never_builds_a_store_without_a_code(monkeypatch):
    captured = {}

    class Job:
        def to_dataframe(self):
            return pd.DataFrame(columns=[
                "store_code", "store_name", "region", "distributor", "stock_date",
                "total_skus", "total_buffer_qty", "est_order_value",
            ])

    class Client:
        def query(self, sql, job_config=None):
            captured["sql"] = sql
            return Job()

    monkeypatch.setattr(po_buffer, "get_bigquery_client", lambda: Client())
    monkeypatch.setattr(po_buffer, "_distributors_for_filter", lambda region, distributor: ["DIST A"])
    po_buffer.load_store_summary_filtered.clear()
    po_buffer.load_store_summary_filtered("All", "All")
    assert "ib.store_code IS NOT NULL" in captured["sql"]
