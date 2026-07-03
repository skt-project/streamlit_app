"""
One-time script: create the first HO Admin user in sfa_step.users.
Run from the backend/ directory after DDL has been applied:

    pip install passlib[bcrypt] google-cloud-bigquery
    python seed_admin.py

Set BQ_SA_KEY_PATH env var (or use ADC) before running.
"""
import os
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery
from google.oauth2 import service_account
import bcrypt

PROJECT = "skintific-data-warehouse"
DATASET = "sfa_web"
SA_KEY_PATH = os.getenv("BQ_SA_KEY_PATH", "")

USERNAME = "admin"
PASSWORD = "Step@2026!"
ROLE = "ho_admin"

hashed = bcrypt.hashpw(PASSWORD.encode(), bcrypt.gensalt()).decode()

if SA_KEY_PATH:
    creds = service_account.Credentials.from_service_account_file(
        SA_KEY_PATH, scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    client = bigquery.Client(project=PROJECT, credentials=creds)
else:
    client = bigquery.Client(project=PROJECT)

sql = f"""
INSERT INTO `{PROJECT}.{DATASET}.users`
  (user_id, username, password_hash, role, is_active, created_at)
VALUES
  (@uid, @uname, @phash, @role, TRUE, CURRENT_TIMESTAMP())
"""
job = client.query(
    sql,
    job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("uid",   "STRING", str(uuid.uuid4())),
        bigquery.ScalarQueryParameter("uname", "STRING", USERNAME),
        bigquery.ScalarQueryParameter("phash", "STRING", hashed),
        bigquery.ScalarQueryParameter("role",  "STRING", ROLE),
    ])
)
job.result()
print(f"Created user '{USERNAME}' with role '{ROLE}'")
print("Login at /api/v1/auth/login  —  change password immediately via PUT /api/v1/auth/users")
