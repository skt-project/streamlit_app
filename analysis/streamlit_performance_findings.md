# Performance Findings — `po_portal_suggestion.py`

Every finding includes a confidence label:
- **Confirmed (code)** — directly observable in the source, not in dispute.
- **Confirmed (code) + precedent** — observable in the source, and a
  working fix for the same problem already exists elsewhere in this repo.
- **Inference** — a reasoned conclusion from code + known framework/platform
  behavior, but not independently confirmed by logs/metrics/live access.
- **Unknown — requires production data** — cannot be answered from the
  code alone; listed so it's visible as an open question rather than
  silently skipped.

---

## F1 — BigQuery client and credentials rebuilt on every interaction

**Confidence: Confirmed (code) + precedent.**

```python
# po_portal_suggestion.py, lines 18-33 (module-level, runs on every script rerun)
gcp_secrets = dict(st.secrets["connections"]["bigquery"])
gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")
credentials = service_account.Credentials.from_service_account_info(gcp_secrets)
...
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
```

Streamlit re-runs the entire script top-to-bottom on every widget
interaction. This code has no `@st.cache_resource` wrapper, so credential
parsing and BigQuery client construction happen again **every time any
user touches any widget** — every filter change, every login attempt,
every page interaction.

**Proof this is an oversight, not a deliberate choice**: three sibling
tools in this exact repository solve the identical problem correctly:

```python
# po_simulator_v2.py, line 78-80
@st.cache_resource(show_spinner=False)
def get_bq_client():
    return bigquery.Client(credentials=_bq_credentials, project=GCP_PROJECT_ID)

# po_portal/utils/bq_ops.py, line 15-20
@st.cache_resource
def get_bq_client() -> bigquery.Client:
    ...
    return bigquery.Client(credentials=creds, project=st.secrets["bigquery"]["project"])
```

**Performance impact:** every rerun pays client-construction overhead
(auth library work, object instantiation) before any actual query can
run. This scales with `(interactions × concurrent users)`, not just user
count — a single user rapidly adjusting filters generates the same
repeated cost as several different users acting once.

**Severity:** High. **Probability this is actively happening:** Certain
(it's unconditional code, not a conditional path). **Effort to fix:**
Low — wrap in `@st.cache_resource`, identical to the existing pattern in
this same repo. **Expected improvement:** Removes a fixed per-interaction
overhead from literally every user action, for everyone, immediately.

---

## F2 — PO Suggestion query has no server-side filter, fetches every distributor's data

**Confidence: Confirmed (code) + precedent.**

```python
# po_portal_suggestion.py, lines 67-95
@st.cache_data(ttl=600)
def load_po_suggestion():
    query = f"""SELECT ... FROM `{PROJECT_ID}.{DATASET}.{PO_TABLE}`"""
    df = bq_client.query(query).to_dataframe()
    ...
    return df
```
```python
# po_portal_suggestion.py, lines 182-185 — filtering happens AFTER fetch, in pandas
if logged_company != "Admin":
    po_df = po_df[po_df["distributor_company"] == logged_company]
```

Every distributor's PO suggestion data is pulled into the app's memory
and across the network on every cache-miss, regardless of which single
distributor is actually logged in — the filter is applied client-side,
after the full transfer.

**This exact problem is already fixed elsewhere in this codebase:**

```python
# po_portal/utils/bq_ops.py, lines 39-55
@st.cache_data(ttl=600)
def load_po_suggestion(_client, company: str | None = None) -> pd.DataFrame:
    where = "" if not company else "WHERE distributor_company = @company"
    params = [] if not company else [bigquery.ScalarQueryParameter("company", "STRING", company)]
    df = _run(_client, f"""SELECT ... FROM {_tbl('po_portal_suggestion')} {where}""", params)
    ...
```

**Performance impact:** BigQuery bytes scanned/transferred, network
transfer time, and Streamlit-process memory footprint all scale with
*total table size* instead of *one distributor's slice* — for every
non-Admin login. As the underlying table grows (more distributors, more
SKUs, more history), this gets strictly worse with zero code change.

**Severity:** High. **Probability:** Certain. **Effort to fix:** Low —
the working replacement code already exists in this repo and can be
adapted directly. **Expected improvement:** Query payload and memory
footprint drop from "entire table" to "one company's rows" for every
non-Admin session — the larger the table, the bigger this win.

---

## F3 — PO Tracking query: unfiltered for Admin, leading-wildcard LIKE for everyone else

**Confidence: Confirmed (code). Not yet fixed anywhere in this codebase
(including the newer `po_portal/` rewrite) — this is a genuinely open
issue, not just a missed copy-paste.**

```python
# po_portal_suggestion.py, lines 106-151
if company == "Admin":
    query = """SELECT ... FROM `dms.gt_po_tracking_all_mv`"""        # no WHERE, no LIMIT
else:
    query = """SELECT ... FROM `dms.gt_po_tracking_all_mv`
               WHERE LOWER(distributor_name) LIKE CONCAT('%', LOWER(@company), '%')"""
```

Two distinct problems in one query:
1. **Admin path**: no filter and no `LIMIT` — pulls the entire
   materialized view (a table literally named "all") on every cache-miss.
2. **Non-Admin path**: a `LIKE '%value%'` pattern with a **leading**
   wildcard cannot use BigQuery's column pruning/clustering on
   `distributor_name` — it forces a full scan of that column's values for
   every row, regardless of any clustering configured on the underlying
   materialized view.

`po_portal/utils/bq_ops.py` (lines 64-81) contains the same two issues
unchanged — confirming this is a real, currently-unsolved bottleneck
rather than something already addressed in the team's more recent work.

**Performance impact:** Likely the single most expensive query in the
application if `dms.gt_po_tracking_all_mv` is a multi-year order-history
view (the name and the `dms` — data management system — dataset prefix
both suggest a transactional, append-only table, which trend toward
"large and growing").

**Severity:** High. **Probability:** Certain that the query pattern
exists; **Unknown — requires production data** for the actual row count
and actual scan cost, which depends on the real size of
`dms.gt_po_tracking_all_mv` (not available from this repo).
**Effort to fix:** Medium — add a sensible default date range
(e.g. "last 90 days" unless the user explicitly widens it) and replace
the leading-wildcard `LIKE` with an exact match against a normalized
company identifier if one exists, or document why a fuzzy match is
required and bound it with an additional filter (e.g. region) to narrow
the scan. **Expected improvement:** Cannot be quantified without knowing
the real table size — likely the largest single win in this report if
the table is as large as its name suggests.

---

## F4 — Two Excel exports regenerated on every script rerun, unconditionally

**Confidence: Confirmed (code).**

```python
# po_portal_suggestion.py, lines 294-306 (PO Suggestion) and 397-414 (PO Tracking)
output = BytesIO()
with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
    display_df.to_excel(writer, index=False, sheet_name="po_suggestion")
output.seek(0)
st.download_button(label="📥 Download PO Suggestion (Excel)", data=output, ...)
```

Neither block is gated behind a button or a "prepare export" step — both
run on **every** script rerun (every filter change), building a complete
Excel file in memory whether or not the user ever clicks the download
button.

**Performance impact:** `xlsxwriter` serialization is CPU-bound and holds
the Python GIL while running. Because this work repeats on every filter
interaction, every user adjusting a multiselect pays this cost — and
because it's CPU-bound (not I/O-bound), it does not yield the GIL the way
a network call would, so concurrent users' Excel-generation work
competes directly for the same core.

**Severity:** Medium-High. **Probability:** Certain. **Effort to fix:**
Low — wrap each block in `if st.button("Prepare Excel export"):` so the
work only happens on explicit request. **Expected improvement:** Removes
two unconditional CPU-bound operations from the common "just looking at
filtered data" path, which is presumably the majority of interactions.

---

## F5 — Zero error handling around any database call

**Confidence: Confirmed (code) for the absence; Unknown — requires
production data for whether this has actually caused observed downtime.**

No `try`/`except` appears anywhere around `bq_client.query(...)`,
`.to_dataframe()`, or `bq_client.insert_rows_json(...)`. A transient
network issue, an expired/misconfigured credential, a BigQuery quota
throttle, or a schema mismatch surfaces as Streamlit's default unhandled-
exception traceback for whichever user triggered it, with no retry and no
user-facing "please try again" message.

**Business impact:** A failure that would otherwise be a brief, recoverable
blip becomes a hard failure visible to the affected user, and looks
identical to "the app is down" from their point of view even when only
their one request failed.

**Severity:** Medium. **Probability:** Cannot be quantified without
production error logs — listed as a contributing-factor *hypothesis* for
the "occasionally unavailable" symptom, not a confirmed cause.
**Effort to fix:** Low-Medium — wrap query calls with a retry/backoff
helper and a friendly fallback message. **Expected improvement:** Cannot
be quantified without knowing actual failure frequency; directionally
this converts some fraction of hard failures into recovered/retried
requests.

---

## F6 — Unpinned dependencies across the whole repo

**Confidence: Confirmed (code) for the fact; Inference for the impact.**

```
# requirements.txt (shared by every app in the repo, including this one)
streamlit
pandas
google-cloud-bigquery
...
```

No version is pinned for any package. A fresh install/redeploy at two
different points in time can silently resolve to two different sets of
package versions, with no corresponding code change to explain a
behavior shift.

**Why this matters specifically here**: the file itself hasn't changed
since 2026-04-06 (confirmed via `git log`), yet the reported instability
is presumably a recent/ongoing complaint. A behavior change with no
matching code change is exactly the signature unpinned dependencies
produce.

**Severity:** Medium. **Probability:** Unknown — requires production
data (the actual currently-installed package versions, and the redeploy
history, neither available from this repo). **Effort to fix:** Low — pin
exact versions repo-wide based on what's currently known-working, ideally
captured via `pip freeze` from the live environment. **Expected
improvement:** Removes one entire class of "nothing changed but it broke"
incidents going forward; does not retroactively fix anything already
caused by a past silent upgrade.

---

## F7 — `load_po_tracking` cache grows per distinct company, uncapped

**Confidence: Confirmed (code) for the mechanism; Inference for severity
(depends on real data volume).**

`@st.cache_data(ttl=600)` on a function keyed by `company` creates one
cache entry per distinct value of `company` seen within any 10-minute
window — including a separate, unfiltered, full-table entry for
`"Admin"`. There is no `max_entries` argument, so the number of
simultaneously cached dataframes is bounded only by how many distinct
companies log in within the TTL window.

**Performance impact:** Memory footprint scales with
`(distinct active companies in the last 10 min) × (their data size)`,
on top of whatever per-session working memory each active script run
holds. Combined with F3 (the Admin entry holding the entire tracking
table), this is the most plausible **memory-growth** mechanism identified
in this codebase.

**Severity:** Medium. **Probability:** Increases with the number of
distinct active distributor accounts — **Unknown — requires production
data** for how many distinct companies are typically active within a
10-minute window. **Effort to fix:** Low — add `max_entries=` to the
cache decorator, and/or shorten the TTL for the Admin path specifically.
**Expected improvement:** Bounds worst-case cache memory to a known
ceiling instead of an open-ended one.

---

## F8 — Hosting platform unknown, likely single-instance with no autoscaling

**Confidence: Inference only.** See
[streamlit_architecture.md](streamlit_architecture.md) "Hosting" section
for the full reasoning. If this app runs on Streamlit Community Cloud
(or any single-instance deployment with no horizontal scaling), then:

- "Occasionally unavailable" is consistent with idle-sleep/cold-start
  behavior, independent of any code issue.
- "Gets worse with more users" is consistent with all sessions sharing
  one process's CPU/RAM/GIL, independent of any code issue.

**This needs to be confirmed, not assumed.** If the app is actually
deployed on a dedicated VM, Cloud Run, or similar with real resource
guarantees, this finding does not apply and the remaining code-level
findings (F1-F7) become the dominant explanation instead.

---

## Concurrency analysis (qualitative — see caveats)

Streamlit's standard execution model gives each connected session its own
thread, but all session threads share **one Python process, one GIL**.
The practical consequence for this specific app:

- **I/O-bound work** (the BigQuery network calls) releases the GIL while
  waiting — multiple users' queries *can* genuinely overlap in wall-clock
  time.
- **CPU-bound work** (pandas filtering, `xlsxwriter` serialization, JSON
  conversion for the BigQuery insert) holds the GIL — this work
  effectively serializes across all concurrent sessions on a single core,
  regardless of how many CPU cores the host has.

Findings F1 and F4 both add CPU-bound, GIL-holding work to **every**
interaction. As concurrent interactions increase, queueing for the GIL
increases non-linearly (each additional concurrent CPU-bound operation
delays all the others a little more) — this is the mechanism behind "gets
worse as more users access it," independent of the database entirely.

**What cannot be stated without production data:** a specific number of
"safe concurrent users," a specific "degradation point," or a specific
"breaking point." These depend on the host's actual CPU core count, RAM
ceiling, the real row counts in `po_portal_suggestion` and
`dms.gt_po_tracking_all_mv`, and actual observed traffic patterns — none
of which are available from a code-only review. Any specific number
offered without that data would be invented, not estimated, and is
deliberately not provided here. See the recommendation in
[streamlit_improvement_backlog.md](streamlit_improvement_backlog.md) for
how to obtain real numbers safely (instrumentation before load testing,
not load testing the live production app first).

## What was checked and found to be *not* a problem

For balance — not everything in this file is a bottleneck:

- **No N+1 query pattern.** Exactly three read query shapes exist, none
  inside a loop.
- **The feedback insert is a single batched `insert_rows_json` call**,
  not row-by-row — correct practice.
- **The "Submit Feedback" insert is correctly gated behind a button
  click** (unlike the Excel exports) — it does not run on every rerun.
- **Session state usage is minimal** (two scalar values) — not a source
  of memory growth.
- **No evidence of recursive or exponential-complexity logic** anywhere
  in the file — all transformations are linear-scan pandas operations.
