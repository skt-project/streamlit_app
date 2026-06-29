# Action Plan — `po_portal_suggestion.py`

**Status: implemented in `po_portal_suggestion_v2.py` (cloned, not a
direct edit).** `po_portal_suggestion.py` itself is untouched — confirmed
via `git diff` showing zero changes. Steps 1, 2, 3, 4, 5, and 7 below are
all applied in the v2 file; Step 6 (pinning repo-wide dependencies) is
intentionally not bundled in since it affects every tool in this repo,
not just this one file. See the top of `po_portal_suggestion_v2.py` for
inline `# v2:` comments marking every change against the original.

The step-by-step breakdown below is kept as the reference for *why* each
change was made and how to verify/roll back each one individually —
useful both for reviewing the v2 file and as a runbook if any of this
needs to be ported elsewhere (e.g. into `po_portal/`, where two of these
same issues — F1 cache, F3 tracking query — also exist).

## Step 0 — Two things to confirm before touching code (do these first, ~30 min total)

1. **Hosting platform.** Check whoever manages deployment, or look at the
   app's URL — a `*.streamlit.app` domain confirms Streamlit Community
   Cloud. If confirmed, note the plan/tier (free tier sleeps idle apps and
   shares one small instance — this alone could explain "occasionally
   down," independent of any code fix below).
2. **A way to test changes before they're live.** You need a local
   `.streamlit/secrets.toml` with BigQuery credentials (ideally a
   read-only service account, not the production write-capable one) so
   you can run `streamlit run po_portal_suggestion.py` locally and see
   the real data before pushing anything.

## Step 1 (P0) — Cache the BigQuery client

**File:** `po_portal_suggestion.py`, lines ~16-33.

```python
# BEFORE
gcp_secrets = dict(st.secrets["connections"]["bigquery"])
gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")
credentials = service_account.Credentials.from_service_account_info(gcp_secrets)

PROJECT_ID = st.secrets["bigquery"]["project"]
DATASET = st.secrets["bigquery"]["dataset"]
...
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
```

```python
# AFTER
PROJECT_ID = st.secrets["bigquery"]["project"]
DATASET = st.secrets["bigquery"]["dataset"]

@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    gcp_secrets = dict(st.secrets["connections"]["bigquery"])
    gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")
    credentials = service_account.Credentials.from_service_account_info(gcp_secrets)
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

bq_client = get_bq_client()
```

Every other line in the file that already says `bq_client.query(...)` or
`bq_client.insert_rows_json(...)` stays exactly the same — `bq_client` is
still a module-level name, it's just now assigned via a cached function
call instead of being built inline every time.

**Verify locally before pushing:** temporarily add `print("BQ CLIENT
BUILT")` inside `get_bq_client()`, run the app, click through several
filters/logins, and confirm that line prints **once**, not once per
click. Remove the print before committing.

**Rollback:** revert this one change — it's fully isolated from
everything else.

## Step 2 (P0) — Push the company filter into the SQL query

**File:** `po_portal_suggestion.py`, lines ~66-101 and ~174-185.

```python
# BEFORE
@st.cache_data(ttl=600)
def load_po_suggestion():
    query = f"""
        SELECT sku_status, brand, region, distributor_company, ...
        FROM `{PROJECT_ID}.{DATASET}.{PO_TABLE}`
    """
    df = bq_client.query(query).to_dataframe()
    for col in ["region", "distributor_branch", "distributor_company"]:
        df[col] = df[col].astype(str).str.strip()
    return df

# ... later ...
po_df = load_po_suggestion()
logged_company = st.session_state["distributor_company"]
if logged_company != "Admin":
    po_df = po_df[po_df["distributor_company"] == logged_company]
```

```python
# AFTER
@st.cache_data(ttl=600)
def load_po_suggestion(company: str | None):
    where = "" if not company else "WHERE distributor_company = @company"
    params = [] if not company else [
        bigquery.ScalarQueryParameter("company", "STRING", company)
    ]
    query = f"""
        SELECT sku_status, brand, region, distributor_company, ...
        FROM `{PROJECT_ID}.{DATASET}.{PO_TABLE}`
        {where}
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = bq_client.query(query, job_config=job_config).to_dataframe()
    for col in ["region", "distributor_branch", "distributor_company"]:
        df[col] = df[col].astype(str).str.strip()
    return df

# ... later ...
logged_company = st.session_state["distributor_company"]
po_df = load_po_suggestion(None if logged_company == "Admin" else logged_company)
# the old post-fetch pandas filter is no longer needed — delete it
```

**Verify locally:** log in as a non-Admin distributor — the table should
show **exactly the same rows** as before (this is a refactor, not a
behavior change). Log in as Admin — should still see everything. If
either looks different, stop and re-check before proceeding.

**Rollback:** revert this one change independently of Step 1.

## Step 3 (P1) — Stop regenerating Excel files on every rerun

**File:** `po_portal_suggestion.py`, lines ~283-306 (PO Suggestion export)
and ~397-414 (PO Tracking export) — same fix applied twice.

```python
# BEFORE (runs on every single rerun, used or not)
output = BytesIO()
with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
    display_df.to_excel(writer, index=False, sheet_name="po_suggestion")
output.seek(0)
st.download_button(label="📥 Download PO Suggestion (Excel)", data=output, ...)
```

```python
# AFTER (only does the work when explicitly asked)
if st.button("📥 Prepare PO Suggestion Excel"):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        display_df.to_excel(writer, index=False, sheet_name="po_suggestion")
    output.seek(0)
    st.download_button(label="📥 Download PO Suggestion (Excel)", data=output, ...)
```

**UX tradeoff, be upfront about this:** this changes a one-click download
into a two-click "Prepare, then Download" flow, because
`st.download_button` needs its file bytes ready before it renders. Check
the actually-installed Streamlit version before doing this — newer
versions added an `on_click` callback option that may allow a smoother
single-click experience; since `requirements.txt` is unpinned (Finding
F6), confirm the real installed version first rather than assuming.

**Verify locally:** confirm the download still produces a correct Excel
file with the currently-filtered data.

**Rollback:** revert independently; if the two-click flow is unpopular,
this is the easiest item to revert without affecting Steps 1-2.

## Step 4 (P1) — PO Tracking: bound the query, fix the wildcard match

**Do this only after a quick product conversation**: what's a reasonable
default time window for "PO Tracking" — last 30/60/90 days? This decides
the default, not the code.

```python
# AFTER (sketch — adapt column/param names to match the real schema)
@st.cache_data(ttl=600)
def load_po_tracking(company: str | None, start_date=None, end_date=None):
    conditions = []
    params = []
    if company and company != "Admin":
        conditions.append("LOWER(distributor_name) LIKE CONCAT('%', LOWER(@company), '%')")
        params.append(bigquery.ScalarQueryParameter("company", "STRING", company))
    if start_date:
        conditions.append("order_date >= @start_date")
        params.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date))
    if end_date:
        conditions.append("order_date <= @end_date")
        params.append(bigquery.ScalarQueryParameter("end_date", "DATE", end_date))
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    query = f"""
        SELECT order_date, distributor_name, customer_order_no, sku,
               product_name, order_qty, unit_price, subtotal
        FROM `dms.gt_po_tracking_all_mv`
        {where}
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = bq_client.query(query, job_config=job_config).to_dataframe()
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    return df
```

Also default the existing `date_input` widget to the chosen window
instead of `value=None`, so the *first* load for any user is already
bounded — that first, unbounded load is the expensive one.

The leading-wildcard `LIKE` is left in place here because removing it
requires knowing whether an exact-match company identifier exists
elsewhere in the schema (e.g. a `distributor_code`) — check that before
changing the match logic; don't guess at a column name that might not
exist.

**Verify locally:** confirm tracking data still appears for a known
distributor and known order, just bounded by date now.

**Rollback:** revert independently.

## Step 5 (P2) — Basic error handling around BigQuery calls

```python
import time

def run_query_with_retry(query, job_config=None, retries=2, backoff=1.5):
    for attempt in range(retries + 1):
        try:
            return bq_client.query(query, job_config=job_config).to_dataframe()
        except Exception:
            if attempt == retries:
                st.error("⚠️ Unable to load data right now. Please try again in a moment.")
                raise
            time.sleep(backoff ** attempt)
```

Replace the bare `bq_client.query(...).to_dataframe()` calls inside
`load_po_suggestion`, `load_po_tracking`, and `check_login` with this
helper. Wrap the `insert_rows_json` feedback submission in a plain
`try/except` with its own friendly message too — it doesn't fit the
retry helper shape since it's a write, not a read.

## Step 6 (P2) — Pin dependencies

**Do not just run `pip freeze` from a fresh local install** — that may
pull different (newer) versions than what's actually running in
production right now, and "fixing" the pin to the wrong version could
introduce the exact kind of silent breakage this step is meant to
prevent. Instead:

1. Get a `pip freeze` output from the actual live/working environment if
   at all possible (e.g., via Streamlit Community Cloud's environment
   inspection, or whoever has shell access to wherever this runs).
2. Pin `requirements.txt` to those exact versions.
3. Treat this as its own deploy, separate from Steps 1-5, during a
   planned window — a pin change can itself surface a break if the
   environment had already silently drifted.

## Step 7 (P2) — Cap the per-company cache

```python
# load_po_tracking's decorator
@st.cache_data(ttl=600, max_entries=20)
def load_po_tracking(company, start_date=None, end_date=None):
    ...
```

Pick `max_entries` based on roughly how many distinct distributor
companies are realistically active at once — 20 is a starting guess, not
a measured number; adjust after watching real usage.

## Deployment process for every step above

1. Test locally first (`streamlit run po_portal_suggestion.py`).
2. Commit with a real, specific message — not "Update
   po_portal_suggestion.py". E.g. `perf: cache BigQuery client and push
   company filter into SQL (fixes repeated full-table fetch)`. A
   reviewable history matters once more than one of these changes is
   live.
3. Deploy during a lower-traffic window if possible.
4. Immediately after deploy: log in as both an Admin-equivalent account
   and a regular distributor account, confirm both Suggestion and
   Tracking tables show the same data they showed before, and confirm
   both Excel downloads and the feedback upload/submit flow still work
   end to end.
5. Keep the previous commit hash noted so rollback is a single `git
   revert` away.

## How to know it actually helped (without fabricating numbers)

There's no monitoring in place today, so "X% faster" can't be claimed
honestly before or after. What you *can* do cheaply:

- Manually time a filter-change interaction (stopwatch is fine) before
  and after Steps 1-2, with nothing else changing in between.
- Ask a few regular users, a few days after deploy, whether the page
  *feels* different — qualitative, but honest, and better than a number
  nobody actually measured.
- If you want real numbers going forward, add minimal timing logs around
  each BigQuery call (`time.time()` before/after, written to a log) as a
  follow-up — this is what would let a future load test produce
  attributable data instead of a single pass/fail.

## Suggested order of work

| Step | What | Why this position |
|---|---|---|
| 0 | Confirm hosting + set up local testing | Unblocks safe testing of everything else |
| 1 | Cache BigQuery client | Smallest, safest, highest-confidence win — do it first to build confidence |
| 2 | Push company filter into SQL | Same confidence level as Step 1, slightly more code to change |
| 3 | Gate Excel exports behind a button | Independent, low-risk, can happen any time after Step 0 |
| 4 | Bound PO Tracking query | Needs a product decision first — start that conversation now, code it once you have an answer |
| 5 | Add retry/error handling | Independent, do whenever convenient |
| 6 | Pin dependencies | Do as its own deploy, not bundled with the others |
| 7 | Cap cache entries | Quick, do alongside Step 4 |
