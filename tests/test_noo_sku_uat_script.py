"""CLI guards on the standalone UAT runner.

Only the guards that fire *before* any network call are exercised here, so the
suite stays credential-free. Everything past those guards is the same
`noo_sku.*` code covered by the other test modules.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "run_noo_sku_uat.py"


def _load():
    spec = importlib.util.spec_from_file_location("run_noo_sku_uat", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


uat = _load()


@pytest.mark.sanity
def test_script_exists_and_exposes_the_three_modes():
    from noo_sku import config

    args = uat.parse_args(["--file", "x.xlsx"])
    assert args.mode == config.MODE_DRY_RUN, "default must be the safe mode"
    for mode in config.MODES:
        assert uat.parse_args(["--file", "x.xlsx", "--mode", mode]).mode == mode


@pytest.mark.sanity
def test_production_without_the_confirmation_flag_is_refused(tmp_path, capsys):
    f = tmp_path / "x.xlsx"
    f.write_bytes(b"")
    code = uat.main(["--mode", "production", "--file", str(f),
                     "--distributor", "DST082"])
    assert code == uat.EXIT_CONFIG
    assert "confirm-production" in capsys.readouterr().out


@pytest.mark.sanity
def test_missing_distributor_is_refused(tmp_path):
    f = tmp_path / "x.xlsx"
    f.write_bytes(b"")
    assert uat.main(["--mode", "dry-run", "--file", str(f)]) == uat.EXIT_CONFIG


@pytest.mark.sanity
def test_missing_input_file_is_refused():
    code = uat.main(["--mode", "dry-run", "--file", "tidak_ada.xlsx",
                     "--distributor", "DST082"])
    assert code == uat.EXIT_CONFIG


def test_an_unknown_mode_is_rejected_by_the_parser():
    with pytest.raises(SystemExit):
        uat.parse_args(["--file", "x.xlsx", "--mode", "banana"])


def test_exit_codes_are_distinct():
    codes = {uat.EXIT_OK, uat.EXIT_VALIDATION, uat.EXIT_CONFIG,
             uat.EXIT_ABORTED, uat.EXIT_WRITE_FAILED, uat.EXIT_VERIFY_FAILED}
    assert len(codes) == 6


def test_script_contains_no_hardcoded_credentials():
    text = SCRIPT.read_text(encoding="utf-8")
    for marker in ("-----BEGIN", "private_key", "client_email", "AIza",
                   "service_account.json", "C:\\", "/home/"):
        assert marker not in text, f"possible credential literal: {marker}"
    # credentials must be resolved through the shared loader, not read here
    assert "load_credentials" in text
    assert "from_service_account" not in text


@pytest.mark.sanity
def test_script_reuses_the_shared_modules_rather_than_reimplementing_them():
    """Brief §21: UAT and Streamlit must share one implementation."""
    text = SCRIPT.read_text(encoding="utf-8")
    for module in ("pipeline", "validators", "enrichment", "writer",
                   "duplicates" if "duplicates" in text else "sources"):
        assert module in text
    # and must not define its own validation or write logic
    assert "def validate_" not in text
    assert "values().append" not in text


# ─── Login source (shared credentials table) ─────────────────────────────────
def test_credentials_live_in_a_dedicated_hashed_table():
    """Auth moved off po_portal_distributor_users: that table stores plaintext
    and three PO-portal apps read it, so it could not be hashed in place."""
    from noo_sku import sources

    app = (REPO / "noo_sku_mapping.py").read_text(encoding="utf-8")
    assert "distributor_passwords" not in app, "no secrets-based passwords"
    assert sources.ACCOUNT_TABLE == "noo_sku_distributor_user"
    assert "sources.load_account" in app
    assert not hasattr(sources, "check_login"), "plaintext login path removed"


def test_the_po_portal_credentials_table_is_no_longer_referenced():
    from noo_sku import sources
    import inspect

    src = inspect.getsource(sources)
    assert "po_portal_distributor_users" not in src


def test_password_writes_are_parameterised():
    import inspect

    from noo_sku import sources

    src = inspect.getsource(sources.set_password)
    assert "@hash" in src and "@code" in src
    assert "must_change_password = FALSE" in src


def test_every_third_party_import_is_declared_in_requirements():
    """Regression: googleapiclient was imported but never declared, so the app
    ran locally and failed on a clean Streamlit Cloud build."""
    import ast
    import re
    import sys

    dist = {"googleapiclient": "google-api-python-client",
            "google": "google-cloud-bigquery"}
    sources = list((REPO / "noo_sku").glob("*.py")) + [
        REPO / "noo_sku_mapping.py", SCRIPT]
    modules = set()
    for path in sources:
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                modules |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules.add(node.module.split(".")[0])

    req = (REPO / "requirements.txt").read_text(encoding="utf-8").lower()
    missing = []
    for mod in modules - set(sys.stdlib_module_names) - {"noo_sku", "tests"}:
        pkg = dist.get(mod, mod)
        if not re.search(rf"^{re.escape(pkg)}\b", req, re.M):
            missing.append(pkg)
    assert not missing, f"undeclared dependencies: {sorted(missing)}"
