"""
Auto-assign territory to SPV/ASM user accounts.
Territory is derived from the most common region among their subordinates in dim_salesman.
SPV  -> majority region from rows where spv_name matches full_name
ASM  -> majority region from rows where asm_name matches full_name
"""
from config import settings
from services.bq import BQClient

bq = BQClient.get()
p = settings.bq_project
d = settings.bq_dataset


def assign_for_role(role: str, col: str):
    users = bq.query(f"""
        SELECT user_id, full_name
        FROM `{p}.{d}.users`
        WHERE role = @role AND (territory IS NULL OR territory = '') AND is_active = TRUE
    """, [bq.p("role", "STRING", role)])

    print(f"\n{role.upper()}: {len(users)} accounts without territory")
    updated = 0
    skipped = 0

    for u in users:
        full_name = (u["full_name"] or "").strip()
        if not full_name:
            skipped += 1
            continue

        regions = bq.query(f"""
            SELECT region, COUNT(*) AS cnt
            FROM `{p}.{d}.dim_salesman`
            WHERE LOWER(TRIM({col})) = LOWER(TRIM(@name))
              AND region IS NOT NULL AND TRIM(region) != ''
              AND is_active = TRUE
            GROUP BY region ORDER BY cnt DESC LIMIT 1
        """, [bq.p("name", "STRING", full_name)])

        if not regions:
            print(f"  no match: {full_name}")
            skipped += 1
            continue

        territory = regions[0]["region"]
        bq.execute(f"""
            UPDATE `{p}.{d}.users`
            SET territory = @territory, updated_at = CURRENT_TIMESTAMP()
            WHERE user_id = @uid
        """, [
            bq.p("territory", "STRING", territory),
            bq.p("uid", "STRING", u["user_id"]),
        ])
        print(f"  {full_name:40s} -> {territory}")
        updated += 1

    print(f"  Updated: {updated}, Skipped: {skipped}")


# Check if territory column exists
cols = bq.query(f"""
    SELECT column_name
    FROM `{p}.{d}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'users' AND column_name = 'territory'
""")
if not cols:
    print("ERROR: 'territory' column does not exist in users table.")
    print("Run migration to add it first.")
    exit(1)

assign_for_role("spv", "spv_name")
assign_for_role("asm", "asm_name")

print("\nDone. Summary:")
summary = bq.query(f"""
    SELECT role,
           COUNTIF(territory IS NOT NULL AND territory != '') AS with_territory,
           COUNTIF(territory IS NULL OR territory = '') AS without_territory
    FROM `{p}.{d}.users`
    WHERE role IN ('spv', 'asm')
    GROUP BY role ORDER BY role
""")
for r in summary:
    print(f"  {r['role']:6s}  with: {r['with_territory']:3d}  without: {r['without_territory']:3d}")
