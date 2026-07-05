from services.bq import BQClient
from config import settings
bq = BQClient.get()
p = settings.bq_project
d = settings.bq_dataset

ERNA_SK = "12d6ab7182f6373386026ec742dd837b"

rows = bq.query(f"""
    SELECT visit_day_of_week, COUNT(*) as cnt
    FROM `{p}.{d}.fact_route_plan_pjp`
    WHERE salesman_sk = @sk AND is_deleted = FALSE
    GROUP BY visit_day_of_week
    ORDER BY cnt DESC
""", [bq.p("sk", "STRING", ERNA_SK)])

print("visit_day_of_week values for ERNA KRISTIYANI:")
for r in rows:
    print(f"  '{r['visit_day_of_week']}' : {r['cnt']} stores")
