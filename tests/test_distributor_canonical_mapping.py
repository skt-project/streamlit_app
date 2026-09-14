"""
Regression tests for the September-2026 distributor canonicalization.

Covers the contract that BOTH Streamlit apps and the gt_master_salesman_v
view share: identity is `distributor_code`, the display name is the
formatting-normalized master name, and the region is `region_g2g`.

No credentials, no network — the SQL is inspected as text, not executed.
Live-data verification lives in the session's validation scripts.

Run: pytest tests/test_distributor_canonical_mapping.py -q
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from distributor_naming import (  # noqa: E402
    build_master_by_code,
    canonical_dist_cte,
    canonical_name_by_code,
    find_name_variants,
    norm_name_sql,
    normalize_distributor_name,
    variant_is_formatting_only,
)

BANGKA = "PT ANUGRAH SUKSES MANDIRI - BANGKA"
BANGKA_TYPO = "PT ANUGERAH SUKSES MANDIRI - BANGKA"
BELITUNG_SOURCE = "PT ANUGRAH SUKSES MANDIRI- BELITUNG"    # as the sheet has it
BELITUNG_CANON = "PT ANUGRAH SUKSES MANDIRI - BELITUNG"    # business standard
SS1 = "SOUTHERN SUMATERA 1"
SS2 = "SOUTHERN SUMATERA 2"


def _sql_of(path: Path, *, strip_comments: bool = True) -> str:
    text = path.read_text(encoding="utf-8")
    if strip_comments:
        text = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("--"))
    return text


# ═══ DST351 — the reported duplicate ══════════════════════════════════════

def test_dst351_both_spellings_resolve_to_the_canonical_name():
    """Both historical variants must land on the master name, via the CODE."""
    master = build_master_by_code([{"distributor_code": "DST351",
                                    "distributor_name": BANGKA}])
    for stored in (BANGKA, BANGKA_TYPO):
        assert canonical_name_by_code("DST351", master, fallback=stored) == BANGKA


def test_dst351_duplicate_is_detected_as_a_spelling_divergence():
    rows = [{"distributor_code": "DST351", "distributor_name": BANGKA_TYPO},
            {"distributor_code": "DST351", "distributor_name": BANGKA}]
    found = find_name_variants(rows)
    assert set(found) == {"DST351"}
    assert variant_is_formatting_only(found["DST351"]) is False, (
        "ANUGERAH/ANUGRAH is a real spelling divergence, not formatting")


def test_dst351_spelling_is_not_normalized_away():
    """
    Normalization must NOT merge the two spellings. Real distributors differ
    by exactly this much: 'PT CATUR SENTOSA ANUGERAH - BANGKA' is a DIFFERENT
    company from 'PT ANUGRAH SUKSES MANDIRI - BANGKA', both in Bangka. Fuzzy
    merging would have collapsed genuinely distinct businesses.
    """
    assert normalize_distributor_name(BANGKA) != normalize_distributor_name(BANGKA_TYPO)
    assert (normalize_distributor_name("PT CATUR SENTOSA ANUGERAH - BANGKA")
            != normalize_distributor_name(BANGKA))


# ═══ DST352 — the source formatting issue ═════════════════════════════════

def test_dst352_source_value_renders_as_the_business_standard_name():
    """
    The master sheet stores 'MANDIRI- BELITUNG' (no space before the hyphen).
    View/application-level normalization renders the business-standard form
    WITHOUT editing production master data.
    """
    assert normalize_distributor_name(BELITUNG_SOURCE) == BELITUNG_CANON


def test_dst352_normalization_is_formatting_only():
    assert variant_is_formatting_only([BELITUNG_SOURCE, BELITUNG_CANON]) is True


def test_dst352_stays_distinct_from_dst351():
    master = build_master_by_code([
        {"distributor_code": "DST351", "distributor_name": BANGKA},
        {"distributor_code": "DST352", "distributor_name": BELITUNG_SOURCE},
    ])
    assert canonical_name_by_code("DST351", master) != canonical_name_by_code("DST352", master)


# ═══ DST227 — the case-only duplicate ═════════════════════════════════════

def test_dst227_case_only_duplicate_is_detected():
    rows = [{"distributor_code": "DST227", "distributor_name": "UD Mitra Kencana - Manado"},
            {"distributor_code": "DST227", "distributor_name": "UD MITRA KENCANA - MANADO"}]
    found = find_name_variants(rows)
    assert set(found) == {"DST227"}
    assert variant_is_formatting_only(found["DST227"]) is True


def test_dst227_resolves_to_one_uppercase_name():
    for raw in ("UD Mitra Kencana - Manado", "UD MITRA KENCANA - MANADO"):
        assert normalize_distributor_name(raw) == "UD MITRA KENCANA - MANADO"


# ═══ Region: region_g2g, never region ═════════════════════════════════════

def test_canonical_cte_exposes_region_g2g_and_never_aliases_it_to_region():
    """
    master_distributor has BOTH `region` (OLD: Southern Sumatera 2) and
    `region_g2g` (CURRENT: Southern Sumatera 1). The CTE must surface the
    latter under its own name — aliasing it to `region` reads as though the
    stale column were in use and invites exactly that misreading.
    """
    cte = canonical_dist_cte()
    assert "AS region_g2g" in cte
    assert not re.search(r"\bAS\s+region\b(?!_g2g)", cte), \
        "region_g2g must not be aliased to a bare `region`"


def test_canonical_cte_reads_region_g2g_as_its_only_region_source():
    cte = canonical_dist_cte()
    # Every mention of a region column in the CTE is region_g2g.
    for m in re.finditer(r"\bregion\w*", cte):
        assert m.group(0) == "region_g2g", f"unexpected region column: {m.group(0)}"


@pytest.mark.parametrize("app_file,forbidden", [
    ("salesman_pjp.py", "s.region"),
    ("salesman_pjp_v2/salesman_crud.py", "s.region"),
])
def test_apps_do_not_select_the_stale_snapshot_region(app_file, forbidden):
    """
    Both apps must take region from master_distributor, not from the frozen
    gt_master_salesman snapshot (which still reads SOUTHERN SUMATERA 2).
    """
    src = _sql_of(REPO / app_file, strip_comments=False)
    assert f"{forbidden}," not in src and f"{forbidden}\n" not in src, \
        f"{app_file} still selects the stale snapshot region"


@pytest.mark.parametrize("app_file", [
    "salesman_pjp.py",
    "salesman_pjp_v2/salesman_crud.py",
    "salesman_pjp_v2/data_loaders.py",
])
def test_apps_reuse_the_single_canonical_mapping(app_file):
    """No duplicated mapping logic: every app imports the shared CTE."""
    src = (REPO / app_file).read_text(encoding="utf-8")
    assert "canonical_dist_cte" in src, f"{app_file} does not reuse the shared mapping"


def test_v2_resolves_region_from_the_canonical_mapping():
    """salesman_pjp_v2 previously selected `s.region` (the stale snapshot)."""
    src = (REPO / "salesman_pjp_v2" / "salesman_crud.py").read_text(encoding="utf-8")
    assert "d.region_g2g AS region" in src
    assert "canonical_dist_cte()" in src


def test_v2_store_join_is_by_code_not_by_name():
    """The old v2 join was `UPPER(b.distributor_g2g) = d.distributor_name`."""
    src = (REPO / "salesman_pjp_v2" / "data_loaders.py").read_text(encoding="utf-8")
    assert "d.distributor_code = UPPER(TRIM(b.dst_id_g2g))" in src
    assert "= d.distributor_name" not in src


# ═══ SQL / Python normalizer parity ═══════════════════════════════════════

def test_norm_name_sql_mirrors_the_python_normalizer_structurally():
    sql = norm_name_sql("distributor")
    for frag in ("UPPER(TRIM(distributor))", "[.,;:]", "' - '", "TRIM("):
        assert frag in sql, frag
    assert sql.count("REGEXP_REPLACE") == 3


def test_norm_name_sql_targets_the_requested_column():
    assert "s.distributor_g2g" in norm_name_sql("s.distributor_g2g")


# ═══ Historical data must never be mutated ═══════════════════════════════

SCRATCH_VIEW = Path(
    r"C:\Users\JONATH~1\AppData\Local\Temp\claude\d--"
    r"\a339ec34-88e3-4cef-8e45-0e5084a7c3f7\scratchpad\proposed_view.sql")


def test_canonical_cte_contains_no_dml():
    """The shared mapping is read-only by construction."""
    cte = canonical_dist_cte().upper()
    for verb in ("INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "DROP", "ALTER"):
        assert verb not in cte, f"{verb} found in the canonical CTE"


DISTRIBUTOR_LABEL_COLUMNS = {
    "nama_distributor", "region", "asm", "kode_distributor",
}


@pytest.mark.parametrize("app_file", [
    "salesman_pjp.py",
    "salesman_pjp_v2/salesman_crud.py",
])
def test_hr_update_cannot_rewrite_distributor_labels(app_file):
    """
    BOTH apps' update_salesman_record() issue an in-place UPDATE on
    gt_master_salesman — an intentional HR-edit feature, and the only place
    this otherwise append-only table is written row-by-row.

    Neither may touch the distributor labels. Those are historical snapshot
    values: canonicalized for DISPLAY by the view/app mapping, and left
    exactly as uploaded in the base table. The guarantee is structural — the
    functions filter every incoming field through an `allowed` whitelist — so
    this pins that the whitelist excludes them.
    """
    tree = ast.parse((REPO / app_file).read_text(encoding="utf-8"))
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "update_salesman_record")
    allowed = next(
        n.value for n in ast.walk(fn)
        if isinstance(n, ast.Assign)
        and any(getattr(t, "id", None) == "allowed" for t in n.targets)
    )
    keys = {k.value for k in allowed.keys}
    assert keys, "could not read the allowed-field whitelist"
    leaked = keys & DISTRIBUTOR_LABEL_COLUMNS
    assert not leaked, f"{app_file}: HR update must not write {sorted(leaked)}"


@pytest.mark.parametrize("app_file,read_token,write_token", [
    ("salesman_pjp.py", "SALESMAN_VIEW", "SALESMAN_TABLE"),
    ("salesman_pjp_v2/salesman_crud.py", "SALESMAN_VIEW", "SALESMAN_TABLE"),
])
def test_salesman_reads_use_the_view_and_writes_use_the_base_table(
        app_file, read_token, write_token):
    """
    The architecture is: base table = historical snapshots (append-only),
    view = current canonical representation. So every SELECT of a salesman
    snapshot must go through gt_master_salesman_v, while INSERT/UPDATE must
    still target the base table — a view is not writable, and rewriting
    history is forbidden regardless.
    """
    src = (REPO / app_file).read_text(encoding="utf-8")

    # Every FROM/JOIN of the salesman snapshot reads the view.
    for m in re.finditer(r"(?:FROM|JOIN)\s+`\{(SALESMAN_\w+)\}`", src):
        assert m.group(1) == read_token, \
            f"{app_file}: reads `{{{m.group(1)}}}` — snapshot reads must use {read_token}"

    # Every write targets the base table.
    writes = re.findall(r"UPDATE\s+`\{(SALESMAN_\w+)\}`", src)
    writes += re.findall(r"load_table_from_dataframe\([^,]+,\s*(SALESMAN_\w+)", src)
    assert writes, f"{app_file}: expected at least one write path"
    for w in writes:
        assert w == write_token, \
            f"{app_file}: writes to {w} — writes must target {write_token}"


def test_view_name_points_at_the_canonical_view():
    src = (REPO / "salesman_pjp.py").read_text(encoding="utf-8")
    assert 'SALESMAN_VIEW = "skintific-data-warehouse.gt_schema.gt_master_salesman_v"' in src
    cfg = (REPO / "salesman_pjp_v2" / "config.py").read_text(encoding="utf-8")
    assert 'SALESMAN_VIEW     = f"{PROJECT_ID}.gt_schema.gt_master_salesman_v"' in cfg


@pytest.mark.parametrize("app_file", [
    "salesman_pjp_v2/data_loaders.py",
])
def test_loaders_never_write_to_the_salesman_table(app_file):
    """
    The loaders this change touched are read-only: a query that only meant to
    display a row must never rewrite the historical snapshot.
    """
    src = (REPO / app_file).read_text(encoding="utf-8").upper()
    for verb in ("UPDATE `", "DELETE FROM", "TRUNCATE", "MERGE INTO"):
        assert verb not in src, f"{app_file} contains {verb}"


@pytest.mark.parametrize("app_file", [
    "salesman_pjp.py",
    "salesman_pjp_v2/salesman_crud.py",
    "salesman_pjp_v2/data_loaders.py",
])
def test_canonicalization_added_no_write_statements(app_file):
    """
    This change is read-side only. The canonical mapping appears exclusively
    in SELECT/JOIN context — never in an UPDATE SET or an INSERT column list.
    """
    src = (REPO / app_file).read_text(encoding="utf-8")
    for stmt in re.findall(r"(?is)UPDATE\s+`[^`]*`.*?(?:WHERE|$)", src):
        assert "canonical_dist_cte" not in stmt, \
            f"{app_file}: canonical mapping leaked into an UPDATE"
        for col in DISTRIBUTOR_LABEL_COLUMNS:
            assert not re.search(rf"\b{col}\s*=", stmt), \
                f"{app_file}: UPDATE assigns {col}"


@pytest.mark.skipif(not SCRATCH_VIEW.exists(), reason="proposed view DDL not generated")
def test_proposed_view_only_creates_a_view_and_never_touches_base_rows():
    sql = _sql_of(SCRATCH_VIEW).upper()
    assert "CREATE OR REPLACE VIEW" in sql
    for verb in ("INSERT", "UPDATE ", "DELETE", "MERGE", "TRUNCATE", "DROP"):
        assert verb not in sql, f"{verb} found in the proposed view DDL"


@pytest.mark.skipif(not SCRATCH_VIEW.exists(), reason="proposed view DDL not generated")
def test_proposed_view_uses_region_g2g_and_select_star_replace():
    sql = _sql_of(SCRATCH_VIEW)
    assert "COALESCE(d.region_g2g," in sql, "view must read region_g2g"
    assert re.search(r"\bd\.region\b(?!_g2g)", sql) is None, \
        "view must not read the stale `region` column"
    assert "SELECT s.* REPLACE" in sql, \
        "column set/order is preserved via SELECT * REPLACE"


@pytest.mark.skipif(not SCRATCH_VIEW.exists(), reason="proposed view DDL not generated")
def test_proposed_view_is_generated_from_the_shared_cte():
    """The view and the apps must not drift apart."""
    sql = _sql_of(SCRATCH_VIEW)
    assert norm_name_sql("distributor") in sql
    assert "AS region_g2g" in sql
