#!/usr/bin/env python3
"""Create NOO & SKU Mapping accounts for every active distributor.

One account per `distributor_code` in DIST DATABASE, seeded with the default
password stored as a **bcrypt hash** — the plaintext default is never written
anywhere. Each account gets its own salt, so identical defaults do not produce
identical hashes.

Idempotent: accounts that already exist are skipped, never overwritten, so a
distributor who has already changed their password keeps it. Re-run it whenever
new distributors are onboarded.

    python scripts/init_distributor_accounts.py --dry-run
    python scripts/init_distributor_accounts.py --apply

Exit codes: 0 ok, 1 nothing to do, 2 configuration/access problem.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from noo_sku import auth, config, sources  # noqa: E402
from noo_sku.normalize import norm_key  # noqa: E402

EXIT_OK, EXIT_NOTHING, EXIT_CONFIG = 0, 1, 2


def parse_args(argv=None):
    ap = argparse.ArgumentParser(prog="init_distributor_accounts.py")
    group = ap.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", default=True,
                       help="report what would be created (default)")
    group.add_argument("--apply", action="store_true",
                       help="actually create the missing accounts")
    ap.add_argument("--dataset", default="gt_schema")
    ap.add_argument("--include-inactive", action="store_true",
                    help="also create accounts for non-Active distributors")
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    apply_changes = bool(args.apply)

    try:
        creds, project = sources.load_credentials()
        client = sources.SheetsClient(creds, config.TRACKER_SPREADSHEET_ID)
        distributors = sources.load_distributors(client)
    except Exception as exc:
        print(f"REFUSED: {type(exc).__name__}: {exc}")
        return EXIT_CONFIG

    wanted = {code: rec for code, rec in distributors.items()
              if args.include_inactive or rec["active"]}
    try:
        existing = sources.existing_account_codes(creds, project, args.dataset)
    except Exception as exc:
        print(f"REFUSED: tidak bisa membaca tabel akun: {exc}")
        return EXIT_CONFIG

    missing = sorted(set(wanted) - existing)
    print(f"distributors considered : {len(wanted)}"
          f"{'' if args.include_inactive else ' (Active only)'}")
    print(f"accounts already present: {len(existing & set(wanted))}")
    print(f"accounts to create      : {len(missing)}")

    if not missing:
        print("\nNothing to do — every distributor already has an account.")
        return EXIT_NOTHING

    print(f"\nDefault password will be set for these {len(missing)} accounts "
          "and stored as a bcrypt hash:")
    for code in missing[:15]:
        print(f"   {code}  {wanted[code]['distributor_name'][:44]}")
    if len(missing) > 15:
        print(f"   ... +{len(missing) - 15} more")

    if not apply_changes:
        print("\nDRY RUN — nothing written. Re-run with --apply to create them.")
        return EXIT_OK

    now = datetime.now(timezone.utc).isoformat()
    rows = [{
        "distributor_code": norm_key(code),
        "distributor_name": wanted[code]["distributor_name"],
        # Hashed per account: same default, different salt, different hash.
        "password_hash": auth.hash_password(auth.DEFAULT_PASSWORD),
        "is_active": True,
        "must_change_password": True,
        "created_at": now,
        "updated_at": now,
        "last_login_at": None,
    } for code in missing]

    print(f"\nHashing {len(rows)} passwords and inserting...")
    try:
        created = sources.insert_accounts(creds, project, args.dataset, rows)
    except Exception as exc:
        print(f"FAILED: {type(exc).__name__}: {exc}")
        return EXIT_CONFIG

    print(f"Created {created} accounts. Each must change its password on first "
          "use (must_change_password = TRUE).")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
