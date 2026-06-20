"""Unit / regression / smoke / sanity tests for assessment_logic.py and the two
app files' syntax. No BigQuery credentials, no Streamlit secrets, no network —
every test here runs against pure functions or static analysis only.

Run with: python run_all_tests.py   (from the streamlit_app directory)
or directly: pytest tests/ -v
"""
import ast
import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from assessment_logic import (
    VALUE_THRESHOLDS,
    normalize_username,
    value_to_grade,
    get_sla_grade,
    bad_stock_grade_for_ytd,
    validate_allocation_row,
    dedupe_metric_points,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


# =====================================================================
# UNIT — normalize_username
# =====================================================================
class TestNormalizeUsername:
    def test_lowercases(self):
        assert normalize_username("Irwan") == "irwan"

    def test_mixed_case(self):
        assert normalize_username("IRWAN") == "irwan"

    def test_strips_whitespace(self):
        assert normalize_username("  irwan  ") == "irwan"

    def test_already_normalized(self):
        assert normalize_username("irwan") == "irwan"

    def test_same_username_different_case_collide(self):
        """The exact scenario from the requirement: Irwan / irwan / IRWAN must
        all normalize to the same stored value."""
        variants = ["Irwan", "irwan", "IRWAN", " Irwan ", "IrWaN"]
        normalized = {normalize_username(v) for v in variants}
        assert normalized == {"irwan"}


# =====================================================================
# UNIT — value_to_grade (banded rule: <=0 -> A, 1..threshold -> B, >threshold -> C)
# =====================================================================
class TestValueToGrade:
    @pytest.mark.sanity
    @pytest.mark.parametrize("value,expected", [
        (0, "A"), (-5, "A"),        # zero and negative both clamp to A
        (1, "B"), (2, "B"),         # AR Performance threshold is 2
        (3, "C"), (10, "C"),
    ])
    def test_ar_performance_boundaries(self, value, expected):
        assert value_to_grade("ACCOUNT RECEIVABLE (AR) PERFORMANCE", value) == expected

    @pytest.mark.parametrize("value,expected", [
        (0, "A"), (1, "B"),         # Data Reporting threshold is 1 (no gap)
        (2, "C"), (5, "C"),
    ])
    def test_data_reporting_boundaries(self, value, expected):
        assert value_to_grade("DATA REPORTING COMPLIANCE", value) == expected

    def test_unknown_metric_raises(self):
        with pytest.raises(KeyError):
            value_to_grade("NOT A REAL METRIC", 1)


# =====================================================================
# UNIT — get_sla_grade (all 9 inner x outer combinations)
# =====================================================================
class TestGetSlaGrade:
    BANDS = ["100%", "99%-80%", "<80%"]

    @pytest.mark.sanity
    def test_both_100_is_grade_a(self):
        assert get_sla_grade("100%", "100%") == ("A", 8)

    @pytest.mark.sanity
    def test_any_below_80_is_grade_c(self):
        assert get_sla_grade("<80%", "100%") == ("C", 0)
        assert get_sla_grade("100%", "<80%") == ("C", 0)
        assert get_sla_grade("<80%", "<80%") == ("C", 0)

    def test_partial_band_is_grade_b(self):
        assert get_sla_grade("99%-80%", "100%") == ("B", 4)
        assert get_sla_grade("100%", "99%-80%") == ("B", 4)
        assert get_sla_grade("99%-80%", "99%-80%") == ("B", 4)

    def test_below_80_dominates_over_partial(self):
        """Either being <80% always wins over the other being 99%-80%."""
        assert get_sla_grade("<80%", "99%-80%") == ("C", 0)
        assert get_sla_grade("99%-80%", "<80%") == ("C", 0)

    def test_all_nine_combinations_are_covered(self):
        results = {(i, o): get_sla_grade(i, o) for i in self.BANDS for o in self.BANDS}
        assert len(results) == 9
        assert all(grade in ("A", "B", "C") for grade, _ in results.values())


# =====================================================================
# UNIT / REGRESSION — bad_stock_grade_for_ytd
# =====================================================================
class TestBadStockGradeForYtd:
    @pytest.mark.sanity
    def test_zero_ytd_auto_max_score(self):
        """Regression: this is the exact bug that crashed the app in
        production — 0 YTD must auto-award max score, not error or score 0."""
        grade, bs_allow, utilization, compliance_pct = bad_stock_grade_for_ytd(0, 0)
        assert grade == "A"
        assert compliance_pct == 100.0
        assert bs_allow == 0
        assert utilization == 0

    def test_none_ytd_auto_max_score(self):
        grade, bs_allow, utilization, compliance_pct = bad_stock_grade_for_ytd(None, 500)
        assert grade == "A"
        assert compliance_pct == 100.0

    def test_full_utilization_is_grade_a(self):
        # allowance = 1_000_000_000 * 0.005 = 5_000_000
        grade, bs_allow, utilization, compliance_pct = bad_stock_grade_for_ytd(1_000_000_000, 5_000_000)
        assert grade == "A"
        assert compliance_pct == 100.0
        assert bs_allow == 5_000_000

    def test_80_percent_boundary_is_grade_b(self):
        # 4_000_000 / 5_000_000 = 80% exactly
        grade, _, _, compliance_pct = bad_stock_grade_for_ytd(1_000_000_000, 4_000_000)
        assert grade == "B"
        assert compliance_pct == 80.0

    def test_just_under_80_percent_is_grade_c(self):
        grade, _, _, compliance_pct = bad_stock_grade_for_ytd(1_000_000_000, 3_999_999)
        assert grade == "C"
        assert compliance_pct < 80.0

    def test_compliance_pct_caps_at_100_even_if_overutilized(self):
        grade, _, _, compliance_pct = bad_stock_grade_for_ytd(1_000_000_000, 50_000_000)
        assert grade == "A"
        assert compliance_pct == 100.0


# =====================================================================
# UNIT / REGRESSION — validate_allocation_row
# =====================================================================
class TestValidateAllocationRow:
    CODE_TO_NAME = {"D001": "CV Maju Bersama - Jakarta Pusat"}
    CODE_TO_REGION = {"D001": "Jakarta"}

    @pytest.mark.sanity
    def test_valid_row_with_code(self):
        status, payload = validate_allocation_row(
            "D001", None, None, "SKINTIFIC", "SKINTIFIC-4331", 500,
            self.CODE_TO_NAME, self.CODE_TO_REGION,
        )
        assert status == "ok"
        assert payload["distributor_code"] == "D001"
        assert payload["distributor_name"] == "CV Maju Bersama - Jakarta Pusat"
        assert payload["region"] == "Jakarta"
        assert payload["allocation_target"] == 500

    def test_entirely_blank_row_is_skipped(self):
        """Regression: this is the exact bug found this session — a truly
        blank Excel cell round-trips through pandas as float('nan'), and
        str(float('nan')) == 'nan', not ''. Must still be detected as blank."""
        status, payload = validate_allocation_row(
            float("nan"), float("nan"), float("nan"), float("nan"), float("nan"), float("nan"),
            self.CODE_TO_NAME, self.CODE_TO_REGION,
        )
        assert status == "skip"
        assert payload is None

    def test_nan_string_literal_also_treated_as_blank(self):
        """Belt-and-suspenders: even if something upstream already stringified
        the NaN to the literal text 'nan', it must still count as blank."""
        status, payload = validate_allocation_row(
            "nan", "nan", "nan", "nan", "nan", "nan",
            self.CODE_TO_NAME, self.CODE_TO_REGION,
        )
        assert status == "skip"

    def test_unknown_distributor_code_is_error(self):
        status, message = validate_allocation_row(
            "D999", None, None, "SKINTIFIC", "SKU-1", 100,
            self.CODE_TO_NAME, self.CODE_TO_REGION,
        )
        assert status == "error"
        assert "D999" in message

    def test_blank_code_falls_back_to_raw_distributor_name_and_region(self):
        status, payload = validate_allocation_row(
            "", "Toko Baru Belum Terdaftar", "Kalimantan", "SKINTIFIC", "SKU-2", 200,
            self.CODE_TO_NAME, self.CODE_TO_REGION,
        )
        assert status == "ok"
        assert payload["distributor_code"] is None
        assert payload["distributor_name"] == "Toko Baru Belum Terdaftar"
        assert payload["region"] == "Kalimantan"

    def test_distributor_code_and_allocation_target_are_optional(self):
        """Requirement: both fields must allow blank without rejecting the row."""
        status, payload = validate_allocation_row(
            "", "Some Distributor", "Jakarta", "SKINTIFIC", "SKU-3", float("nan"),
            self.CODE_TO_NAME, self.CODE_TO_REGION,
        )
        assert status == "ok"
        assert payload["distributor_code"] is None
        assert payload["allocation_target"] is None

    def test_brand_and_sku_code_remain_required(self):
        status, message = validate_allocation_row(
            "D001", None, None, "SKINTIFIC", "", 100,
            self.CODE_TO_NAME, self.CODE_TO_REGION,
        )
        assert status == "error"
        assert "required" in message

    def test_negative_allocation_target_is_error(self):
        status, message = validate_allocation_row(
            "D001", None, None, "SKINTIFIC", "SKU-4", -10,
            self.CODE_TO_NAME, self.CODE_TO_REGION,
        )
        assert status == "error"
        assert "negative" in message

    def test_non_numeric_allocation_target_is_error(self):
        status, message = validate_allocation_row(
            "D001", None, None, "SKINTIFIC", "SKU-5", "not-a-number",
            self.CODE_TO_NAME, self.CODE_TO_REGION,
        )
        assert status == "error"


# =====================================================================
# REGRESSION — dedupe_metric_points (the Salesman 5-row double-count bug,
# also the bug found in the v_distributor_assessment_combined BQ view)
# =====================================================================
class TestDedupeMetricPoints:
    @pytest.mark.sanity
    def test_salesman_five_rows_count_once_not_five_times(self):
        """Regression: SALESMAN stores one row per person (up to 5), all
        sharing the same point value. A naive SUM(point) over raw rows
        inflates the total 5x. dedupe_metric_points must collapse to 1x."""
        rows = [{"metric": "SALESMAN", "point": 7} for _ in range(5)]
        rows += [{"metric": "WAREHOUSE FACILITY STANDARD", "point": 3}]
        assert dedupe_metric_points(rows) == 7 + 3

    def test_single_row_metrics_unaffected(self):
        rows = [
            {"metric": "DELIVERY SLA COMPLIANCE", "point": 8},
            {"metric": "INVENTORY CONTROL & STOCK OPNAME", "point": 6},
        ]
        assert dedupe_metric_points(rows) == 14

    def test_full_ass_submission_totals_34(self):
        """End-to-end shape check: a full Area Sales Supervisor submission at
        all-A grades, including the 5-row Salesman fan-out, must total 34 —
        not 34 + 4*7 = 62 from double-counting."""
        rows = (
            [{"metric": "OPERATIONAL LEADER (SPV / OPERATIONAL MANAGER)", "point": 5}]
            + [{"metric": "SALESMAN", "point": 7} for _ in range(5)]
            + [{"metric": "ADMINISTRATIVE & AR SUPPORT", "point": 3}]
            + [{"metric": "WAREHOUSE FACILITY STANDARD", "point": 3}]
            + [{"metric": "DELIVERY SLA COMPLIANCE", "point": 8}]
            + [{"metric": "INVENTORY CONTROL & STOCK OPNAME", "point": 6}]
            + [{"metric": "BAD STOCK HANDLING PERFORMANCE", "point": 2}]
        )
        assert dedupe_metric_points(rows) == 34


# =====================================================================
# SMOKE — both app files must at least parse cleanly. This catches syntax
# errors without needing st.secrets / a BigQuery connection to import them.
# =====================================================================
class TestSmokeSyntax:
    @pytest.mark.sanity
    @pytest.mark.parametrize("filename", [
        "skt_area_execution_capability_v2.py",
        "skt_area_execution_capability_mock.py",
        "assessment_logic.py",
    ])
    def test_file_parses(self, filename):
        source = (REPO_ROOT / filename).read_text(encoding="utf-8")
        ast.parse(source, filename=filename)  # raises SyntaxError on failure
