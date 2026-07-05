"""Reset test_se password and create a simple 'se1' account for easy mobile testing."""
from services.bq import BQClient
from services.auth import hash_password
from config import settings
import uuid
from datetime import datetime, timezone

bq = BQClient.get()
p = settings.bq_project
d = settings.bq_dataset
now = datetime.now(timezone.utc).isoformat()
pw = hash_password("STEP@2026")

# 1. Reset test_se password
bq.execute(f"""
    UPDATE `{p}.{d}.users`
    SET password_hash = @pw, updated_at = CURRENT_TIMESTAMP()
    WHERE username = 'test_se'
""", [bq.p("pw", "STRING", pw)])
print("test_se password reset to STEP@2026")

# 2. Upsert simple 'se1' account linked to ERNA KRISTIYANI
ERNA_SK = "12d6ab7182f6373386026ec742dd837b"  # from login test earlier
existing = bq.query(f"SELECT user_id FROM `{p}.{d}.users` WHERE username = 'se1'")
if existing:
    bq.execute(f"""
        UPDATE `{p}.{d}.users`
        SET password_hash = @pw, salesman_sk = @sk, is_active = TRUE, updated_at = CURRENT_TIMESTAMP()
        WHERE username = 'se1'
    """, [bq.p("pw", "STRING", pw), bq.p("sk", "STRING", ERNA_SK)])
    print("se1 account updated")
else:
    bq.execute(f"""
        INSERT INTO `{p}.{d}.users`
          (user_id, username, full_name, password_hash, role, salesman_sk, is_active, created_at, updated_at)
        VALUES (@uid, 'se1', 'Test SE', @pw, 'se', @sk, TRUE, @now, @now)
    """, [
        bq.p("uid", "STRING", str(uuid.uuid4())),
        bq.p("pw",  "STRING", pw),
        bq.p("sk",  "STRING", ERNA_SK),
        bq.p("now", "TIMESTAMP", now),
    ])
    print("se1 account created")

print("\nReady accounts:")
print("  username: test_se   password: STEP@2026  (linked to ERNA)")
print("  username: se1       password: STEP@2026  (linked to ERNA KRISTIYANI, 137 routes)")
