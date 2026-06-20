"""Pure, side-effect-free logic for the Distributor Operational Assessment app.

No Streamlit, no BigQuery, no I/O of any kind — every function here takes plain
arguments and returns plain values, so it can be imported and unit-tested
without secrets, credentials, or a running app. skt_area_execution_capability_v2.py
and skt_area_execution_capability_mock.py both import from this module instead of
redefining the same rules twice.
"""

import math

VALUE_THRESHOLDS = {
    "ACCOUNT RECEIVABLE (AR) PERFORMANCE": 2,
    "DATA REPORTING COMPLIANCE": 1,
}


def normalize_username(username):
    """The single source of truth for username normalization: trim + lowercase.
    Every create/lookup/login touchpoint should call this rather than inlining
    .strip().lower(), so the rule can't silently drift between call sites."""
    return username.strip().lower()


def value_to_grade(metric_name, value, thresholds=VALUE_THRESHOLDS):
    """Banded rule for value-based bulk metrics (AR Performance, Data Reporting
    Compliance): <=0 -> A (negative values are clamped to the 0 rule),
    1..threshold -> B, >threshold -> C."""
    threshold = thresholds[metric_name]
    if value <= 0:
        return "A"
    elif value <= threshold:
        return "B"
    else:
        return "C"


def get_sla_grade(inner, outer):
    """Delivery SLA grade from inner-city / outer-city compliance bands."""
    if inner == "<80%" or outer == "<80%":
        return "C", 0
    elif inner == "99%-80%" or outer == "99%-80%":
        return "B", 4
    else:
        return "A", 8


def bad_stock_grade_for_ytd(ytd_val, utilization):
    """Pure compliance-% -> grade math for Bad Stock Handling Performance.
    No YTD sell-through data (0 or None) auto-awards max score — there's
    nothing to be out-of-compliance against. Returns
    (grade, bs_allowance, utilization, compliance_pct)."""
    if not ytd_val:
        return "A", 0, 0, 100.0

    bs_allow = ytd_val * 0.005
    compliance_pct = min(100.0, (utilization / bs_allow) * 100)

    if compliance_pct >= 100:
        grade = "A"
    elif compliance_pct >= 80:
        grade = "B"
    else:
        grade = "C"
    return grade, bs_allow, utilization, compliance_pct


def _is_blank(value):
    """True for None, NaN-as-float, empty string, or the literal string 'nan'
    (which is what str(float('nan')) produces — the root cause of the blank-cell
    bug found this session in the allocation upload parser)."""
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    text = str(value).strip()
    return text == "" or text.lower() == "nan"


def validate_allocation_row(raw_code, raw_dist_name, raw_region, raw_brand, raw_sku, raw_target,
                             code_to_name, code_to_region, valid_grades=None):
    """Pure validation/normalization for one NPD/SKU Focus allocation upload row.

    distributor_code and allocation_target are optional (nullable in
    distributor_sku_allocation) — brand and sku_code are the only required
    fields for a row to count as "filled in". An explicitly-typed but
    unrecognized distributor_code is still an error (likely a typo); blank is
    the only accepted way to skip it. When no code is given, falls back to
    whatever raw_dist_name/raw_region were typed instead of forcing NULL.

    Returns a (status, payload) tuple:
      ("skip", None)                 - entirely blank row, not yet filled in
      ("error", "<message>")         - invalid row, caller should report & skip
      ("ok", {...row dict...})       - valid, ready to submit
    """
    code = "" if _is_blank(raw_code) else str(raw_code).strip()
    if code and code not in code_to_name:
        return "error", f"unknown distributor_code '{code}'"

    brand = "" if _is_blank(raw_brand) else str(raw_brand).strip()
    sku_code = "" if _is_blank(raw_sku) else str(raw_sku).strip()

    if brand == "" and sku_code == "":
        return "skip", None
    if brand == "" or sku_code == "":
        return "error", "both brand and sku_code are required"

    if _is_blank(raw_target):
        allocation_target = None
    else:
        try:
            allocation_target = int(float(raw_target))
        except (TypeError, ValueError):
            return "error", f"invalid allocation_target '{raw_target}' for {brand}/{sku_code}"
        if allocation_target < 0:
            return "error", f"allocation_target for {brand}/{sku_code} cannot be negative"

    if code:
        dist_name = code_to_name[code]
        region_val = code_to_region[code]
    else:
        dist_name = None if _is_blank(raw_dist_name) else str(raw_dist_name).strip()
        region_val = None if _is_blank(raw_region) else str(raw_region).strip()

    return "ok", {
        "distributor_code": code or None,
        "distributor_name": dist_name,
        "region": region_val,
        "brand": brand,
        "sku_code": sku_code,
        "allocation_target": allocation_target,
    }


def dedupe_metric_points(rows):
    """Collapses multi-row-per-metric submissions (SALESMAN stores one row per
    person, up to 5, all sharing the same point value) down to one point value
    per metric, then sums — the same fix applied to the
    v_distributor_assessment_combined BigQuery view (MAX(point) grouped by
    metric before SUM). rows: list of dicts with at least 'metric' and 'point'
    keys. Returns the correctly-deduplicated total."""
    per_metric = {}
    for r in rows:
        m = r["metric"]
        per_metric[m] = r["point"]  # all rows for the same metric share the same point value
    return sum(per_metric.values())
