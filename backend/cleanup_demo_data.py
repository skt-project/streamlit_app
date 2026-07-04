"""
One-time cleanup: remove duplicate announcement + approval_request rows
caused by seed_demo_data.py running twice. Keeps the latest by created_at.
Also removes duplicate admin user.
"""
from services.bq import BQClient

bq = BQClient.get()
p = "skintific-data-warehouse"
d = "sfa_web"

ops = [
    # Keep only the latest 3 announcements (newest by created_at)
    f"""
    DELETE FROM `{p}.{d}.announcement`
    WHERE announcement_id NOT IN (
      SELECT announcement_id FROM (
        SELECT announcement_id, ROW_NUMBER() OVER (PARTITION BY title ORDER BY created_at DESC) AS rn
        FROM `{p}.{d}.announcement`
        WHERE is_deleted = FALSE
      ) WHERE rn = 1
    ) AND is_deleted = FALSE
    """,
    # Keep only the latest 2 approval requests
    f"""
    DELETE FROM `{p}.{d}.approval_request`
    WHERE approval_id NOT IN (
      SELECT approval_id FROM (
        SELECT approval_id, ROW_NUMBER() OVER (PARTITION BY title ORDER BY submitted_at DESC) AS rn
        FROM `{p}.{d}.approval_request`
        WHERE is_deleted = FALSE
      ) WHERE rn = 1
    ) AND is_deleted = FALSE
    """,
    # Remove duplicate admin (keep the one with a full_name set, or newer created_at)
    f"""
    DELETE FROM `{p}.{d}.users`
    WHERE user_id NOT IN (
      SELECT user_id FROM (
        SELECT user_id, ROW_NUMBER() OVER (PARTITION BY username ORDER BY created_at DESC) AS rn
        FROM `{p}.{d}.users`
      ) WHERE rn = 1
    )
    """,
]

for sql in ops:
    try:
        bq.execute(sql)
        print("OK")
    except Exception as e:
        print(f"ERR: {e}")

print("Cleanup done.")
