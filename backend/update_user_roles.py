"""
Auto-update sfa_web.users.role by cross-referencing dim_salesman hierarchy:
- A salesman whose name appears as spv_name  of others -> role = 'spv'
- A salesman whose name appears as asm_name  of others -> role = 'asm'
- Everyone else stays 'se'

Name matching is case-insensitive TRIM.
Run after create_bulk_users.py.
"""
from services.bq import BQClient
from config import settings

bq = BQClient.get()
p = settings.bq_project
d = settings.bq_dataset

# Preview: who would be reclassified as SPV?
print("=== Salesmen who supervise others (SPV candidates) ===")
spv_rows = bq.query(f"""
    SELECT s.salesman_sk, s.salesman_name, COUNT(DISTINCT sub.salesman_sk) as team_size
    FROM `{p}.{d}.dim_salesman` s
    JOIN `{p}.{d}.dim_salesman` sub
      ON LOWER(TRIM(sub.spv_name)) = LOWER(TRIM(s.salesman_name))
    WHERE s.is_active = TRUE
    GROUP BY s.salesman_sk, s.salesman_name
    ORDER BY team_size DESC
    LIMIT 20
""")
for r in spv_rows:
    print(f"  {r['salesman_name']:40s} team={r['team_size']}")

print(f"\nTotal SPV candidates: {len(spv_rows)}")

print("\n=== Salesmen who manage SPVs (ASM candidates) ===")
asm_rows = bq.query(f"""
    SELECT s.salesman_sk, s.salesman_name, COUNT(DISTINCT sub.salesman_sk) as team_size
    FROM `{p}.{d}.dim_salesman` s
    JOIN `{p}.{d}.dim_salesman` sub
      ON LOWER(TRIM(sub.asm_name)) = LOWER(TRIM(s.salesman_name))
    WHERE s.is_active = TRUE
    GROUP BY s.salesman_sk, s.salesman_name
    ORDER BY team_size DESC
    LIMIT 20
""")
for r in asm_rows:
    print(f"  {r['salesman_name']:40s} team={r['team_size']}")

print(f"\nTotal ASM candidates: {len(asm_rows)}")

# Confirm before updating
answer = input("\nProceed with role updates? (y/n): ").strip().lower()
if answer != "y":
    print("Aborted.")
    exit()

# Update SPVs
if spv_rows:
    print("Updating SPV roles...")
    bq.execute(f"""
        UPDATE `{p}.{d}.users` u
        SET u.role = 'spv', u.updated_at = CURRENT_TIMESTAMP()
        WHERE u.salesman_sk IN (
            SELECT s.salesman_sk
            FROM `{p}.{d}.dim_salesman` s
            JOIN `{p}.{d}.dim_salesman` sub
              ON LOWER(TRIM(sub.spv_name)) = LOWER(TRIM(s.salesman_name))
            WHERE s.is_active = TRUE
            GROUP BY s.salesman_sk
        )
        AND u.role = 'se'
    """)
    print(f"  {len(spv_rows)} SPV users updated")

# Update ASMs
if asm_rows:
    print("Updating ASM roles...")
    bq.execute(f"""
        UPDATE `{p}.{d}.users` u
        SET u.role = 'asm', u.updated_at = CURRENT_TIMESTAMP()
        WHERE u.salesman_sk IN (
            SELECT s.salesman_sk
            FROM `{p}.{d}.dim_salesman` s
            JOIN `{p}.{d}.dim_salesman` sub
              ON LOWER(TRIM(sub.asm_name)) = LOWER(TRIM(s.salesman_name))
            WHERE s.is_active = TRUE
            GROUP BY s.salesman_sk
        )
        AND u.role IN ('se', 'spv')
    """)
    print(f"  {len(asm_rows)} ASM users updated")

print("\nFinal role distribution:")
dist = bq.query(f"""
    SELECT role, COUNT(*) as cnt FROM `{p}.{d}.users`
    WHERE is_active = TRUE GROUP BY role ORDER BY cnt DESC
""")
for r in dist:
    print(f"  {r['role']:15s} {r['cnt']}")
