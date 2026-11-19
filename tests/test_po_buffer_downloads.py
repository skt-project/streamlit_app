"""File naming and bulk ZIP rules of the PO Suggestion Tool (po_buffer_downloads).

No Streamlit, no BigQuery, no credentials: the PDF renderer is replaced by a
tiny fake that embeds the store it was called for, so these tests can prove each
PDF in the ZIP was built from the right store's data.

Run: pytest tests/test_po_buffer_downloads.py -q
"""
from __future__ import annotations

import io
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import po_buffer_downloads as D  # noqa: E402

DATE = "20261119"


def make_inventory(codes, rows_per_store=3, plan=True):
    """Inventory rows: every store has `rows_per_store` SKUs."""
    records = []
    for code in codes:
        for n in range(rows_per_store):
            records.append(
                {
                    "store_code": code,
                    "store_name": f"Name of {code}",
                    "product_code": f"{code}-SKU{n}",
                    "buffer_plan": 10 + n if plan else None,
                }
            )
    return pd.DataFrame(records)


def make_filter(codes, names=None):
    names = names or {}
    return pd.DataFrame(
        [
            {
                "store_code": code,
                "store_name": names.get(code, f"Name of {code}"),
                "region": "JAWA",
                "distributor": "DIST A",
                "est_order_value": 1000,
            }
            for code in codes
        ]
    )


class FakePdf:
    """Stands in for generate_po_pdf; records what each call was given."""

    def __init__(self):
        self.calls = []

    def __call__(self, store_info, store_detail):
        self.calls.append((store_info, store_detail))
        # Every row it was handed must belong to the store it was called for.
        assert set(store_detail["store_code"]) == {store_info["store_code"]}
        body = f"{store_info['store_code']}|{store_info['store_name']}|{len(store_detail)}"
        return io.BytesIO(b"%PDF-1.4 fake " + body.encode())


def build(codes, pdf=None, names=None, inventory=None):
    pdf = pdf or FakePdf()
    inventory = make_inventory(codes) if inventory is None else inventory
    flt = make_filter(codes, names)
    plan = D.plan_bulk_download(flt, inventory, DATE)
    result = D.build_bulk_zip(plan, inventory, pdf, DATE)
    return result, pdf, plan


def zip_entries(result):
    with zipfile.ZipFile(io.BytesIO(result.zip_bytes)) as archive:
        return {i.filename: archive.read(i) for i in archive.infolist()}


# ---------------------------------------------------------------------------
# Bulk ZIP: one PDF per store
# ---------------------------------------------------------------------------

def test_filter_with_one_store_gives_a_zip_with_one_pdf():
    result, _, _ = build(["S1"])
    entries = zip_entries(result)
    assert len(entries) == 1
    assert list(entries) == ["Name of S1_20261119.pdf"]
    assert result.file_count == 1


def test_filter_with_five_stores_gives_a_zip_with_exactly_five_pdfs():
    codes = [f"S{n}" for n in range(1, 6)]
    result, _, _ = build(codes)
    entries = zip_entries(result)
    assert len(entries) == 5
    assert all(name.endswith(".pdf") for name in entries)
    assert result.file_count == 5
    assert result.excluded == []


def test_each_pdf_is_built_from_its_own_stores_data():
    codes = ["S1", "S2", "S3"]
    result, pdf, _ = build(codes)
    entries = zip_entries(result)
    for code in codes:
        assert f"{code}|Name of {code}|3".encode() in entries[f"Name of {code}_{DATE}.pdf"]
    # one PDF per store, none for a combined multi-store document
    assert [info["store_code"] for info, _ in pdf.calls] == codes


def test_zip_contains_only_pdfs_and_no_temp_files():
    result, _, _ = build(["S1", "S2"])
    with zipfile.ZipFile(io.BytesIO(result.zip_bytes)) as archive:
        assert archive.testzip() is None
        names = archive.namelist()
    assert names and all(n.lower().endswith(".pdf") for n in names)
    assert not any("/" in n or n.startswith(".") or n.endswith((".tmp", ".json", ".txt")) for n in names)


def test_zip_filename_uses_store_count_and_date():
    result, _, _ = build(["S1", "S2", "S3"])
    assert result.zip_name == "PO_Bulk_3_20261119.zip"
    assert D.build_zip_filename(5, "20261119") == "PO_Bulk_5_20261119.zip"


# ---------------------------------------------------------------------------
# Nothing is dropped silently
# ---------------------------------------------------------------------------

def test_store_without_inventory_rows_is_excluded_with_a_reason():
    inventory = make_inventory(["S1", "S2"])  # S3 has no rows at all
    result, _, plan = build(["S1", "S2", "S3"], inventory=inventory)
    assert [p.store_code for p in result.included] == ["S1", "S2"]
    assert [(p.store_code, p.skip_reason) for p in result.excluded] == [("S3", D.SKIP_NO_DETAIL)]
    # included + excluded always accounts for every store of the filter
    assert len(result.included) + len(result.excluded) == len(plan) == 3
    assert len(zip_entries(result)) == 2


def test_store_whose_rows_have_no_buffer_plan_is_excluded_with_a_reason():
    inventory = pd.concat([make_inventory(["S1"]), make_inventory(["S2"], plan=False)])
    result, _, _ = build(["S1", "S2"], inventory=inventory)
    assert [p.store_code for p in result.included] == ["S1"]
    assert result.excluded[0].skip_reason == D.SKIP_NO_PLAN


def test_store_with_blank_code_is_excluded_with_a_reason():
    flt = make_filter(["S1"])
    flt.loc[len(flt)] = [None, "Ghost Store", "JAWA", "DIST A", 0]
    plan = D.plan_bulk_download(flt, make_inventory(["S1"]), DATE)
    assert [p.skip_reason for p in plan] == [None, D.SKIP_NO_CODE]


def test_failing_pdf_is_reported_and_the_other_stores_still_get_their_pdf():
    class Flaky(FakePdf):
        def __call__(self, store_info, store_detail):
            if store_info["store_code"] == "S2":
                raise RuntimeError("boom")
            return super().__call__(store_info, store_detail)

    result, _, _ = build(["S1", "S2", "S3"], pdf=Flaky())
    assert [p.store_code for p in result.included] == ["S1", "S3"]
    assert result.excluded[0].store_code == "S2"
    assert "boom" in result.excluded[0].skip_reason
    assert len(zip_entries(result)) == 2


def test_generator_output_that_is_not_a_pdf_is_treated_as_a_failure():
    result, _, _ = build(["S1"], pdf=lambda info, detail: b"<html>error</html>")
    assert result.zip_bytes is None
    assert result.excluded[0].skip_reason.startswith("Gagal membuat PDF")


def test_no_valid_store_gives_no_zip():
    result, _, _ = build(["S1"], inventory=make_inventory(["S1"], plan=False))
    assert result.zip_bytes is None and result.file_count == 0
    assert len(result.excluded) == 1


# ---------------------------------------------------------------------------
# File names
# ---------------------------------------------------------------------------

def test_filename_format_is_store_name_underscore_date():
    assert D.build_pdf_filename("Store Jakarta Selatan", "20261119") == (
        "Store Jakarta Selatan_20261119.pdf"
    )


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Toko A/B", "Toko A-B"),
        ("Toko A\\B", "Toko A-B"),
        ("Toko: Pusat", "Toko-Pusat"),
        ("Toko * Besar", "Toko-Besar"),
        ("Toko?", "Toko"),
        ('Toko "Maju"', "Toko-Maju"),
        ("<Toko>", "Toko"),
        ("Toko|Baru", "Toko-Baru"),
        ('A/\\:*?"<>|B', "A-B"),
        ("  Toko   Banyak   Spasi  ", "Toko Banyak Spasi"),
        ("Toko Titik...", "Toko Titik"),
        ("Toko\x00Ctrl\x1f", "Toko-Ctrl"),
        ("Toko_Garis_Bawah", "Toko_Garis_Bawah"),
        ("Toko Sehat Sentosa (Cab. 2)", "Toko Sehat Sentosa (Cab. 2)"),
        ("Toko Kopi Ñandú", "Toko Kopi Ñandú"),
    ],
)
def test_illegal_characters_are_sanitized(raw, expected):
    assert D.sanitize_filename_component(raw) == expected
    # and the final name is usable on Windows/macOS/Linux
    name = D.build_pdf_filename(raw, DATE)
    assert not set('\\/:*?"<>|') & set(name)
    assert name.endswith(f"_{DATE}.pdf")


@pytest.mark.parametrize("raw", [None, float("nan"), "", "   ", "///", "...", "??"])
def test_unusable_names_fall_back_instead_of_producing_an_empty_name(raw):
    assert D.sanitize_filename_component(raw) == D.FALLBACK_NAME
    assert D.build_pdf_filename(raw, DATE) == f"{D.FALLBACK_NAME}_{DATE}.pdf"


def test_very_long_names_are_truncated():
    name = D.build_pdf_filename("X" * 500, DATE)
    assert len(name) <= D.MAX_NAME_LENGTH + len(f"_{DATE}.pdf")


def test_windows_device_names_stay_valid_because_of_the_date_suffix():
    assert D.build_pdf_filename("CON", DATE) == "CON_20261119.pdf"


def test_duplicate_store_names_get_the_store_code_in_every_colliding_file():
    stores = [
        {"store_code": "A1", "store_name": "Toko Sama"},
        {"store_code": "B2", "store_name": "Toko Sama"},
        {"store_code": "C3", "store_name": "Toko Lain"},
    ]
    names = D.build_unique_filenames(stores, DATE)
    assert names == {
        "A1": "Toko Sama_A1_20261119.pdf",
        "B2": "Toko Sama_B2_20261119.pdf",
        "C3": "Toko Lain_20261119.pdf",
    }
    assert len(set(names.values())) == 3


def test_names_that_differ_only_by_case_or_illegal_characters_are_still_unique():
    stores = [
        {"store_code": "A1", "store_name": "Toko Sama"},
        {"store_code": "B2", "store_name": "TOKO SAMA"},   # same file on Windows/macOS
        {"store_code": "C3", "store_name": "Toko/Sama"},
    ]
    names = D.build_unique_filenames(stores, DATE)
    assert len({n.casefold() for n in names.values()}) == 3


def test_colliding_codes_after_sanitizing_still_get_unique_names():
    stores = [
        {"store_code": "A/1", "store_name": "Toko Sama"},
        {"store_code": "A-1", "store_name": "Toko Sama"},
    ]
    names = D.build_unique_filenames(stores, DATE)
    assert len({n.casefold() for n in names.values()}) == 2


def test_duplicate_names_inside_a_bulk_zip_do_not_overwrite_each_other():
    names = {"S1": "Toko Sama", "S2": "Toko Sama", "S3": "Toko Sama"}
    result, _, _ = build(["S1", "S2", "S3"], names=names)
    entries = zip_entries(result)
    assert len(entries) == 3
    assert sorted(entries) == [
        "Toko Sama_S1_20261119.pdf",
        "Toko Sama_S2_20261119.pdf",
        "Toko Sama_S3_20261119.pdf",
    ]


def test_unique_names_do_not_depend_on_row_order():
    a = [{"store_code": "A1", "store_name": "Sama"}, {"store_code": "B2", "store_name": "Sama"}]
    assert D.build_unique_filenames(a, DATE) == D.build_unique_filenames(a[::-1], DATE)


# ---------------------------------------------------------------------------
# Date: Asia/Jakarta
# ---------------------------------------------------------------------------

def test_date_is_taken_in_jakarta_not_utc():
    # 18:30 UTC on the 18th is already 01:30 on the 19th in Jakarta (UTC+7)
    assert D.jakarta_date_str(datetime(2026, 11, 18, 18, 30, tzinfo=timezone.utc)) == "20261119"
    # and 16:59 UTC is still the same day in Jakarta
    assert D.jakarta_date_str(datetime(2026, 11, 18, 16, 59, tzinfo=timezone.utc)) == "20261118"


def test_the_default_clock_is_jakarta_too(monkeypatch):
    """The app calls jakarta_date_str() with no argument; that path must be Jakarta."""
    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            instant = datetime(2026, 11, 18, 18, 30, tzinfo=timezone.utc)   # 01:30 on the 19th in Jakarta
            return instant.astimezone(tz) if tz else instant.replace(tzinfo=None)

    monkeypatch.setattr(D, "datetime", Frozen)
    assert D.jakarta_date_str() == "20261119"
    assert D.build_zip_filename(2, D.jakarta_date_str()) == "PO_Bulk_2_20261119.zip"


def test_naive_datetime_is_treated_as_jakarta_local_time():
    assert D.jakarta_date_str(datetime(2026, 11, 19, 0, 5)) == "20261119"


def test_date_format_is_yyyymmdd():
    assert len(D.jakarta_date_str()) == 8 and D.jakarta_date_str().isdigit()


# ---------------------------------------------------------------------------
# Helpers shared by the individual button and the bulk ZIP
# ---------------------------------------------------------------------------

def test_store_info_has_the_shape_generate_po_pdf_expects():
    info = D.build_store_info(
        {"store_code": "S1", "store_name": "N", "region": "R", "distributor": "D", "est_order_value": 5}
    )
    assert info["store_code"] == "S1" and info["distributor_g2g"] == "D"
    assert {"store_code", "store_name", "distributor_g2g", "region"} <= set(info)


def test_store_detail_matches_exactly_one_store():
    inventory = make_inventory(["S1", "S10", "S2"])
    detail = D.get_store_detail(inventory, "S1")
    assert set(detail["store_code"]) == {"S1"} and len(detail) == 3
