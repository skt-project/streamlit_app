"""One-off migration: hard-coded PJP passwords -> BigQuery account table.

Reads the legacy ``DISTRIBUTOR_PASSWORDS`` dict straight out of
``salesman_pjp.py`` (by AST, so nothing in that module executes), bcrypt-hashes
each password in memory, and MERGEs one account row per distributor into
``gt_schema.sfa_pjp_distributor_accounts``.

Plaintext passwords are never written to disk, never logged, and never printed -
they exist only as local variables for the length of one hash call. The script
prints distributor codes and counts only.

Deadline migration
------------------
``--deadline`` is REQUIRED and has no default. The script refuses to run at all
if ``salesman_pjp.py`` still defines a global ``INPUT_DEADLINE``. See
``resolve_seed_deadline`` for the incident that guard exists to prevent.

Usage
-----
    python scripts/seed_pjp_distributor_accounts.py --deadline 2026-09-30 --dry-run
    python scripts/seed_pjp_distributor_accounts.py --deadline 2026-09-30

Re-running is safe: the MERGE updates existing rows in place rather than
appending. By default an existing account's password is left alone (so a
password the admin has since changed is not reverted to the legacy one); pass
``--overwrite-passwords`` to force the legacy password back on every row.
"""
from __future__ import annotations

import argparse
import ast
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import pjp_accounts  # noqa: E402
import pjp_auth  # noqa: E402

SOURCE = REPO / "salesman_pjp.py"
PROJECT = "skintific-data-warehouse"
DATASET = "gt_schema"
ACTOR = "migration"


def _module_constants(path: Path):
    """Pull DISTRIBUTOR_PASSWORDS and INPUT_DEADLINE out without importing.

    salesman_pjp.py calls st.set_page_config() at import time, so it cannot be
    imported in a plain script. The repo's test suite reads it the same way.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    passwords, deadline = None, None
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        if target.id == "DISTRIBUTOR_PASSWORDS":
            passwords = ast.literal_eval(node.value)
        elif target.id == "INPUT_DEADLINE":
            # datetime(Y, M, D).date()
            try:
                args = [ast.literal_eval(a) for a in node.value.func.value.args]
                deadline = datetime(*args).date()
            except Exception:
                deadline = None
    return passwords, deadline


def _credentials():
    """Service-account credentials, the same way the app resolves them.

    Reads .streamlit/secrets.toml directly rather than through st.secrets, so
    this script does not need a Streamlit runtime.
    """
    try:
        import tomllib
    except ModuleNotFoundError:  # Python < 3.11
        import tomli as tomllib
    from google.oauth2 import service_account

    secrets_path = REPO / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        raise SystemExit(f"Tidak menemukan {secrets_path}. "
                         "Jalankan dari repo dengan secrets.toml tersedia.")
    with secrets_path.open("rb") as fh:
        secrets = tomllib.load(fh)
    info = dict(secrets["connections"]["bigquery"])
    info["private_key"] = info["private_key"].replace("\\n", "\n")
    return (service_account.Credentials.from_service_account_info(info),
            info["project_id"])


def _master_distributor_codes(credentials, project) -> set:
    from google.cloud import bigquery

    client = bigquery.Client(credentials=credentials, project=project)
    sql = f"""
        SELECT DISTINCT UPPER(TRIM(distributor_code)) AS code
        FROM `{project}.{DATASET}.master_distributor`
        WHERE status = 'Active' AND distributor_code IS NOT NULL
    """
    return {r["code"] for r in client.query(sql).result() if r["code"]}


def _master_distributor_names(credentials, project) -> dict:
    from google.cloud import bigquery

    client = bigquery.Client(credentials=credentials, project=project)
    sql = f"""
        SELECT UPPER(TRIM(distributor_code)) AS code,
               ANY_VALUE(UPPER(TRIM(distributor))) AS name
        FROM `{project}.{DATASET}.master_distributor`
        WHERE distributor_code IS NOT NULL
        GROUP BY code
    """
    return {r["code"]: (r["name"] or "") for r in client.query(sql).result()}


class StaleMigrationError(RuntimeError):
    """Raised when a seed run would inherit a deadline instead of stating one."""


def resolve_seed_deadline(deadline_arg, legacy_deadline):
    """The only way a deadline gets into a seed run: stated explicitly.

    This guard exists because of a real incident. This script used to fall back
    to the legacy ``INPUT_DEADLINE`` constant parsed out of ``salesman_pjp.py``.
    The working tree it ran from was 21 commits behind production ``main``, so
    that constant read 2026-09-11 while production had already moved to
    2026-09-16 - and all 128 accounts were seeded with a deadline production had
    abandoned two commits earlier. Every account was locked out.

    A deadline is a business decision. It is never inherited from whatever
    happens to be checked out. So: a legacy global constant is a hard error, and
    an absent ``--deadline`` is a hard error. There is no default.
    """
    if legacy_deadline is not None:
        raise StaleMigrationError(
            f"salesman_pjp.py still defines a global INPUT_DEADLINE "
            f"({legacy_deadline}). That constant is NOT a source of truth and "
            f"may be stale relative to production main. The per-distributor "
            f"deadline lives in sfa_pjp_distributor_accounts.input_deadline. "
            f"Remove the constant, and pass the intended date with --deadline.")
    text = str(deadline_arg or "").strip()
    if not text:
        raise StaleMigrationError(
            "--deadline is required; there is no default. State the intended "
            "date explicitly, e.g. --deadline 2026-09-30.")
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise StaleMigrationError(
            f"--deadline must be YYYY-MM-DD, got {deadline_arg!r}.") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deadline", required=True,
                        help="Initial input_deadline, YYYY-MM-DD. REQUIRED - "
                             "there is no default and no fallback to any "
                             "legacy constant. See resolve_seed_deadline().")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would change; write nothing.")
    parser.add_argument("--overwrite-passwords", action="store_true",
                        help="Force the legacy password onto accounts that "
                             "already exist. Off by default.")
    args = parser.parse_args()

    passwords, legacy_deadline = _module_constants(SOURCE)
    if not passwords:
        print("DISTRIBUTOR_PASSWORDS sudah tidak ada di salesman_pjp.py - "
              "migrasi kemungkinan sudah selesai.")
        return 0

    try:
        deadline = resolve_seed_deadline(args.deadline, legacy_deadline)
    except StaleMigrationError as exc:
        print(f"DIBATALKAN: {exc}")
        return 2

    credentials, project = _credentials()
    print(f"Project          : {project}")
    print(f"Tabel akun       : {DATASET}.{pjp_accounts.ACCOUNT_TABLE}")
    print(f"Akun di source   : {len(passwords)}")
    print(f"Deadline awal    : {deadline} (eksplisit dari --deadline)")
    print(f"Mode             : {'DRY RUN' if args.dry_run else 'WRITE'}")
    print()

    names = _master_distributor_names(credentials, project)
    active_codes = _master_distributor_codes(credentials, project)

    orphans = sorted(c for c in map(pjp_auth.norm_code, passwords)
                     if c not in active_codes)
    if orphans:
        print(f"CATATAN: {len(orphans)} kode punya password tapi tidak Active di "
              f"master_distributor (akun tetap dibuat agar login tidak hilang): "
              f"{', '.join(orphans)}")
        print()

    # One round trip for "which accounts already exist", then one MERGE for all
    # of them - 128 separate statements would be minutes of latency for nothing.
    existing = pjp_accounts.load_existing_codes(credentials, project, DATASET)

    rows, audit, created, updated, skipped = [], [], 0, 0, 0
    for raw_code, plaintext in passwords.items():
        code = pjp_auth.norm_code(raw_code)
        exists = code in existing
        if exists and not args.overwrite_passwords:
            password_hash = None  # MERGE leaves the stored hash untouched
            skipped += 1
        else:
            password_hash = pjp_auth.hash_password(plaintext)

        rows.append({
            "distributor_code": code,
            "distributor_name": names.get(code, ""),
            "username": code,
            "password_hash": password_hash,
            "is_active": True,
            "input_deadline": deadline,
        })
        audit.append((
            code,
            "MIGRATE_ACCOUNT" if exists else pjp_accounts.ACTION_CREATE,
            f"seeded from DISTRIBUTOR_PASSWORDS; deadline={deadline}; "
            f"password={'kept' if password_hash is None else 'set'}",
        ))
        if exists:
            updated += 1
        else:
            created += 1

    if args.dry_run:
        for code, action, _ in audit:
            print(f"  {'UPDATE' if action == 'MIGRATE_ACCOUNT' else 'CREATE'} {code}")
        print()
        print(f"Akan dibuat   : {created}")
        print(f"Akan diupdate : {updated}")
        return 0

    pjp_accounts.bulk_upsert_accounts(credentials, project, rows, ACTOR, DATASET)
    pjp_accounts.bulk_write_audit(credentials, project, audit, ACTOR, DATASET)

    print(f"Dibuat   : {created}")
    print(f"Diupdate : {updated}")
    if skipped:
        print(f"Password lama dipertahankan pada {skipped} akun yang sudah ada "
              f"(pakai --overwrite-passwords untuk menimpa).")

    # --- Verification: every legacy password must authenticate against the
    # --- row that was just written. This is the check that makes it safe to
    # --- delete DISTRIBUTOR_PASSWORDS from the source.
    print()
    print("Verifikasi login terhadap tabel baru...")
    stored = pjp_accounts.load_hashes(credentials, project, DATASET)
    bad = [pjp_auth.norm_code(c) for c, p in passwords.items()
           if not pjp_auth.verify_password(
               p, stored.get(pjp_auth.norm_code(c), ""))]
    if bad:
        print(f"  GAGAL untuk {len(bad)} akun: {', '.join(bad)}")
        print("  JANGAN hapus DISTRIBUTOR_PASSWORDS sebelum ini bersih.")
        return 1
    print(f"  OK - {len(passwords)}/{len(passwords)} password lama "
          f"terverifikasi terhadap hash di BigQuery.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
