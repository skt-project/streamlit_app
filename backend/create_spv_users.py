"""
Create sfa_web.users accounts for SPVs and ASMs.
These managers are NOT in dim_salesman — they appear only as spv_name/asm_name
on their team members' rows.

- Deduplicates by LOWER(TRIM(name))
- role = 'spv' for spv_name entries, 'asm' for asm_name entries
- salesman_sk = NULL (they're managers, not field SEs)
- Default password: STEP@2026
- Territory must be updated manually via Administration UI
"""
import re
import uuid
from datetime import datetime, timezone

from config import settings
from services.auth import hash_password
from services.bq import BQClient

DEFAULT_PASSWORD = "STEP@2026"
bq = BQClient.get()
p = settings.bq_project
d = settings.bq_dataset
now = datetime.now(timezone.utc).isoformat()


def make_username(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"\s+", "_", name)
    return re.sub(r"[^a-z0-9_]", "", name)


def get_existing_usernames() -> set:
    return {r["username"] for r in bq.query(f"SELECT username FROM `{p}.{d}.users`")}


def get_existing_full_names() -> set:
    rows = bq.query(f"SELECT full_name FROM `{p}.{d}.users` WHERE full_name IS NOT NULL")
    return {r["full_name"].lower().strip() for r in rows}


def fetch_managers(col: str) -> list:
    """Return deduplicated manager names from a spv_name/asm_name column."""
    rows = bq.query(f"""
        SELECT LOWER(TRIM({col})) as norm_name, MAX({col}) as display_name
        FROM `{p}.{d}.dim_salesman`
        WHERE {col} IS NOT NULL AND TRIM({col}) != ''
          AND is_active = TRUE
        GROUP BY LOWER(TRIM({col}))
        ORDER BY display_name
    """)
    return rows


print("Loading existing accounts...")
existing_usernames = get_existing_usernames()
existing_names = get_existing_full_names()
print(f"  {len(existing_usernames)} existing users")

pw_hash = hash_password(DEFAULT_PASSWORD)

for col, role in [("spv_name", "spv"), ("asm_name", "asm")]:
    managers = fetch_managers(col)
    print(f"\n{role.upper()} candidates from {col}: {len(managers)}")

    rows_to_insert = []
    for m in managers:
        norm = m["norm_name"]
        display = m["display_name"].strip().title()

        if norm in existing_names:
            print(f"  skip (exists): {display}")
            continue

        base = make_username(norm)
        username = base
        suffix = 2
        while username in existing_usernames:
            username = f"{base}_{suffix}"
            suffix += 1

        rows_to_insert.append({
            "user_id":       str(uuid.uuid4()),
            "username":      username,
            "full_name":     display,
            "password_hash": pw_hash,
            "role":          role,
            "salesman_sk":   None,
            "is_active":     True,
            "created_at":    now,
            "updated_at":    now,
        })
        existing_usernames.add(username)
        existing_names.add(norm)

    if rows_to_insert:
        bq.insert_rows("users", rows_to_insert)
        print(f"  Created {len(rows_to_insert)} {role} accounts")
    else:
        print(f"  Nothing to create")

print("\nFinal role distribution:")
dist = bq.query(f"""
    SELECT role, COUNT(*) as cnt FROM `{p}.{d}.users`
    WHERE is_active = TRUE GROUP BY role ORDER BY cnt DESC
""")
for r in dist:
    print(f"  {r['role']:15s} {r['cnt']}")

print(f"\nDefault password for all new accounts: {DEFAULT_PASSWORD}")
print("Set territory per SPV/ASM via Administration -> Users.")
