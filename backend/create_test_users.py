"""
Create 3 fixed test accounts for E2E approval flow testing.

  test_se    / STEP@2026  — SE (uses demo salesman routes)
  test_spv   / STEP@2026  — SPV
  test_dist  / STEP@2026  — distributor_admin (final approver)

Run once from D:\\GitHub\\skintific-step\\backend:
  python create_test_users.py
"""
import uuid
from datetime import datetime, timezone

from config import settings
from services.auth import hash_password
from services.bq import BQClient

bq  = BQClient.get()
p   = settings.bq_project
d   = settings.bq_dataset
now = datetime.now(timezone.utc).isoformat()
pw  = hash_password("STEP@2026")

# Reuse the demo salesman_sk so test_se has routes
DEMO_SK = "demo" + "0" * 28

TEST_USERS = [
    dict(username="test_se",   full_name="Test SE",               role="se",               salesman_sk=DEMO_SK),
    dict(username="test_spv",  full_name="Test SPV",              role="spv",              salesman_sk=None),
    dict(username="test_dist", full_name="Test Distributor Admin", role="distributor_admin", salesman_sk=None),
]

existing = {r["username"] for r in bq.query(f"SELECT username FROM `{p}.{d}.users`")}

for u in TEST_USERS:
    if u["username"] in existing:
        # Reset password and re-activate in case account was deactivated
        bq.execute(
            f"UPDATE `{p}.{d}.users` SET password_hash = @pw, is_active = TRUE, updated_at = @now WHERE username = @un",
            [bq.p("pw", "STRING", pw), bq.p("now", "TIMESTAMP", now), bq.p("un", "STRING", u["username"])],
        )
        print(f"  reset: {u['username']}")
    else:
        bq.execute(
            f"""
            INSERT INTO `{p}.{d}.users`
              (user_id, username, full_name, password_hash, role, salesman_sk, is_active, created_at, updated_at)
            VALUES (@uid, @un, @fn, @pw, @role, @sk, TRUE, @now, @now)
            """,
            [
                bq.p("uid",  "STRING",    str(uuid.uuid4())),
                bq.p("un",   "STRING",    u["username"]),
                bq.p("fn",   "STRING",    u["full_name"]),
                bq.p("pw",   "STRING",    pw),
                bq.p("role", "STRING",    u["role"]),
                bq.p("sk",   "STRING",    u["salesman_sk"]),
                bq.p("now",  "TIMESTAMP", now),
            ],
        )
        print(f"  created: {u['username']}")

print("\nTest accounts ready:")
print("  test_se    / STEP@2026  (SE)")
print("  test_spv   / STEP@2026  (SPV)")
print("  test_dist  / STEP@2026  (Distributor Admin — final approver)")
