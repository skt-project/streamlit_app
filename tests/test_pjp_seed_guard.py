"""Guard against the stale-deadline migration incident recurring.

On 2026-09-16 the seeding script fell back to the legacy ``INPUT_DEADLINE``
constant parsed out of ``salesman_pjp.py``. The working tree it ran from was 21
commits behind production ``main``, so that constant read 2026-09-11 while
production had already moved to 2026-09-16. All 128 distributor accounts were
seeded with a deadline production had abandoned two commits earlier, which
would have locked every distributor out.

These tests pin the guard that makes that impossible: a deadline must be stated
explicitly, and a legacy global constant is a hard error rather than a default.

Run: pytest tests/test_pjp_seed_guard.py -q
"""
from __future__ import annotations

import ast
import sys
from datetime import date
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

import seed_pjp_distributor_accounts as SEED  # noqa: E402

LEGACY = date(2026, 9, 11)


# --- the guard itself ---------------------------------------------------------

def test_explicit_deadline_is_accepted():
    assert SEED.resolve_seed_deadline("2026-09-30", None) == date(2026, 9, 30)


def test_a_legacy_global_constant_is_a_hard_error_not_a_default():
    """The exact incident: a stale INPUT_DEADLINE must never be inherited."""
    with pytest.raises(SEED.StaleMigrationError) as exc:
        SEED.resolve_seed_deadline(None, LEGACY)
    assert "INPUT_DEADLINE" in str(exc.value)


def test_legacy_constant_blocks_even_when_a_deadline_is_supplied():
    """Its presence means the source still has a second source of truth."""
    with pytest.raises(SEED.StaleMigrationError):
        SEED.resolve_seed_deadline("2026-09-30", LEGACY)


@pytest.mark.parametrize("missing", [None, "", "   "])
def test_missing_deadline_is_rejected(missing):
    with pytest.raises(SEED.StaleMigrationError) as exc:
        SEED.resolve_seed_deadline(missing, None)
    assert "--deadline is required" in str(exc.value)


@pytest.mark.parametrize("bad", ["16-09-2026", "2026/09/30", "tomorrow",
                                 "2026-13-01", "20260930"])
def test_malformed_deadline_is_rejected(bad):
    with pytest.raises(SEED.StaleMigrationError):
        SEED.resolve_seed_deadline(bad, None)


def test_guard_never_silently_returns_a_default():
    """No input combination may produce a date the caller did not name."""
    for deadline_arg, legacy in ((None, None), (None, LEGACY), ("", LEGACY)):
        with pytest.raises(SEED.StaleMigrationError):
            SEED.resolve_seed_deadline(deadline_arg, legacy)


# --- wiring: the CLI cannot be invoked without --deadline ---------------------

def test_deadline_argument_is_required_by_the_parser():
    source = (REPO / "scripts" / "seed_pjp_distributor_accounts.py").read_text(
        encoding="utf-8")
    tree = ast.parse(source)
    required = False
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "add_argument"
                and node.args
                and getattr(node.args[0], "value", None) == "--deadline"):
            required = any(k.arg == "required" and k.value.value is True
                           for k in node.keywords)
    assert required, "--deadline must be required=True"


def test_no_fallback_to_the_legacy_constant_remains_in_main():
    """The removed code path, pinned so it cannot be reintroduced."""
    source = (REPO / "scripts" / "seed_pjp_distributor_accounts.py").read_text(
        encoding="utf-8")
    assert "elif legacy_deadline:" not in source
    assert "deadline = legacy_deadline" not in source


def test_main_still_refuses_when_the_password_dict_is_gone():
    """The migration path stays closed now that the dict has been removed."""
    passwords, _ = SEED._module_constants(REPO / "salesman_pjp.py")
    assert not passwords, (
        "DISTRIBUTOR_PASSWORDS is back in salesman_pjp.py - the migration is "
        "supposed to be finished and the dict removed")


def test_production_source_has_no_global_deadline_constant():
    """If this fails, someone restored the second source of truth."""
    _, legacy = SEED._module_constants(REPO / "salesman_pjp.py")
    assert legacy is None, (
        f"salesman_pjp.py defines INPUT_DEADLINE={legacy}; the per-distributor "
        "deadline in sfa_pjp_distributor_accounts is the only source of truth")
