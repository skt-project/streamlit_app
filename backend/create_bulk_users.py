"""
Bulk-create sfa_web.users accounts for all active salesmen in dim_salesman.

- Username: lowercase name, spaces -> underscore (e.g. ERNA -> erna, ONSOF PUTRA -> onsof_putra)
- Default password: STEP@2026  (user must change on first login via Admin UI)
- Default role: se  (update via Admin UI for SPV/ASM users after creation)
- salesman_sk: auto-linked from dim_salesman

Usage:
  cd D:\\GitHub\\skintific-step\\backend
  python create_bulk_users.py

Run ONCE. Safe to re-run -- skips salesmen already linked to a user account.
"""
import re
import uuid
from datetime import datetime, timezone

from config import settings
from services.auth import hash_password
from services.bq import BQClient

DEFAULT_PASSWORD = "STEP@2026"
DEFAULT_ROLE = "se"

bq = BQClient.get()
now = datetime.now(timezone.utc).isoformat()


def make_username(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^a-z0-9_]", "", name)
    return name


def get_existing_usernames() -> set:
    rows = bq.query(
        f"SELECT username FROM `{settings.bq_project}.{settings.bq_dataset}.users`"
    )
    return {r["username"] for r in rows}


def get_linked_salesman_sks() -> set:
    rows = bq.query(
        f"SELECT salesman_sk FROM `{settings.bq_project}.{settings.bq_dataset}.users` WHERE salesman_sk IS NOT NULL"
    )
    return {r["salesman_sk"] for r in rows}


def get_all_salesmen() -> list:
    return bq.query(
        f"SELECT salesman_sk, salesman_name FROM `{settings.bq_project}.{settings.bq_dataset}.dim_salesman` "
        "WHERE is_active = TRUE ORDER BY salesman_name"
    )


def main():
    print("Loading existing usernames...")
    existing = get_existing_usernames()
    print(f"  {len(existing)} existing users found")

    print("Loading already-linked salesman_sk values...")
    linked_sks = get_linked_salesman_sks()
    print(f"  {len(linked_sks)} salesman_sk values already linked")

    print("Loading salesmen from dim_salesman...")
    salesmen = get_all_salesmen()
    print(f"  {len(salesmen)} active salesmen found")

    pw_hash = hash_password(DEFAULT_PASSWORD)
    rows_to_insert = []
    skipped = 0

    for s in salesmen:
        sk   = s["salesman_sk"]
        name = s["salesman_name"]

        if sk in linked_sks:
            skipped += 1
            continue

        base = make_username(name)
        username = base
        suffix = 2
        while username in existing:
            username = f"{base}_{suffix}"
            suffix += 1

        rows_to_insert.append({
            "user_id":       str(uuid.uuid4()),
            "username":      username,
            "full_name":     name.title(),
            "password_hash": pw_hash,
            "role":          DEFAULT_ROLE,
            "salesman_sk":   sk,
            "is_active":     True,
            "created_at":    now,
            "updated_at":    now,
        })
        existing.add(username)
        linked_sks.add(sk)

    print(f"  {skipped} skipped (already linked)")
    print(f"  {len(rows_to_insert)} new accounts to create")

    if not rows_to_insert:
        print("Nothing to do.")
        return

    # Streaming insert in batches of 500
    BATCH = 500
    errors_total = 0
    for i in range(0, len(rows_to_insert), BATCH):
        batch = rows_to_insert[i:i + BATCH]
        try:
            bq.insert_rows("users", batch)
            print(f"  Inserted batch {i // BATCH + 1}: {len(batch)} rows")
        except Exception as e:
            errors_total += 1
            print(f"  ERROR batch {i // BATCH + 1}: {e}")

    print(f"\n{'='*60}")
    print(f"Done: {len(rows_to_insert)} created, {skipped} skipped, {errors_total} batch errors")
    print(f"Default password for all new accounts: {DEFAULT_PASSWORD}")
    print("Update SPV/ASM roles via Administration page after creation.")


if __name__ == "__main__":
    main()
