# Architecture Report — `po_portal_suggestion.py`

## System architecture

A single-file Streamlit script (no multi-page app structure, no separate
modules) running as one Python process. There is no application server,
no API layer, and no background worker — Streamlit's own built-in Tornado
server handles HTTP/WebSocket traffic directly, and the entire script
re-executes top-to-bottom on every user interaction (Streamlit's standard
execution model).

```
┌─────────────────────────────────────────────────────────────────┐
│ Browser (one per user)                                          │
│   - Renders Streamlit's frontend (React)                        │
│   - WebSocket connection to the Streamlit server                │
└───────────────────────────┬───────────────────────────────────────┘
                            │ WebSocket (re-sent on every widget interaction)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Streamlit server process (single process, inferred Community     │
│ Cloud-style hosting — see "Hosting" below)                      │
│                                                                   │
│  One ScriptRunner thread per active session, all sharing:        │
│   - One Python interpreter / one GIL                             │
│   - One `st.cache_data` store (shared across ALL sessions)       │
│   - One `st.cache_resource` store (shared across ALL sessions)   │
│                                                                   │
│  po_portal_suggestion.py re-runs top-to-bottom on every:          │
│   - Login button click                                           │
│   - Filter change (multiselect/date_input)                       │
│   - File upload                                                  │
│   - "Submit Feedback" click                                      │
└───────────────────┬─────────────────────────────┬─────────────────┘
                    │                             │
                    ▼                             ▼
     ┌───────────────────────────┐   ┌─────────────────────────────┐
     │ BigQuery (Google Cloud)   │   │ (no other external services)│
     │  - po_portal_distributor_ │   │                              │
     │    users (auth)           │   └─────────────────────────────┘
     │  - po_portal_suggestion   │
     │    (recommended PO qty)   │
     │  - dms.gt_po_tracking_    │
     │    all_mv (order history) │
     │  - po_portal_feedback     │
     │    (write target)         │
     └───────────────────────────┘
```

## Page structure and navigation flow

There is exactly one page (no `st.navigation`/multi-page setup). The page
has four sequential sections, all rendered unconditionally once logged in:

1. Login gate (`st.stop()` if not authenticated — blocks everything below)
2. PO Suggestion table + filters + Excel export
3. PO Tracking table + filters + Excel export
4. Feedback upload + validation + BigQuery insert

There is no routing, no tabs, no sidebar navigation — a user sees all four
sections stacked on one long page every time.

## Session management

- `st.session_state.logged_in` (bool) and `st.session_state.distributor_company`
  (str) are the only two session state values used.
- No logout control exists anywhere in the code — a session stays
  authenticated until the browser tab/connection is closed (Streamlit's
  default session lifecycle), there is no explicit timeout or
  invalidation logic in the app itself.
- No CSRF/XSRF considerations are visible in the app code (Streamlit
  handles this at the framework level by default, not something this
  file overrides).

## State management (`st.session_state`)

Minimal — only the two values above. No evidence of state bloat or
uncontrolled growth within a single session. This is **not** where the
stability/performance risk is concentrated.

## Caching implementation

| Function | Decorator | Keyed by | Notes |
|---|---|---|---|
| `bq_client` / `credentials` setup | **none** | — | Re-executed on every script rerun. See finding in [streamlit_performance_findings.md](streamlit_performance_findings.md) |
| `check_login()` | none (intentional) | — | Correctly uncached — login must reflect current data; only runs on button click, not a hot path |
| `load_po_suggestion()` | `@st.cache_data(ttl=600)` | nothing (no args) | One shared cache entry for the entire table, across every user, for 10 minutes |
| `load_po_tracking(company)` | `@st.cache_data(ttl=600)` | `company` | One cache entry **per distinct company value seen**, including a separate full-table entry for `"Admin"`, each living 10 minutes, no `max_entries` cap |

## Background tasks

None. Everything is synchronous, request-driven. No scheduled jobs, no
async workers, no queue.

## Data flow

```
Login:
  username/password → check_login() → BigQuery query (po_portal_distributor_users)
  → distributor_company stored in session_state

PO Suggestion:
  load_po_suggestion() [cached, ALL companies, ALL rows]
  → pandas filter to logged-in company (post-fetch, in-memory)
  → pandas filter by Region/Company/Branch (multiselect, recomputed every rerun)
  → st.dataframe() render
  → unconditional Excel serialization → st.download_button

PO Tracking:
  load_po_tracking(company) [cached per company, SQL-side LIKE filter for non-Admin]
  → pandas filter by Distributor/Order No/Date (recomputed every rerun)
  → st.dataframe() render
  → unconditional Excel serialization → st.download_button

Feedback:
  file_uploader → pandas read_excel → column validation → regex validation
  → numeric cleaning/coercion → submission_id/submitted_at stamped
  → st.button("Submit Feedback") → bq_client.insert_rows_json() [single batch]
```

## Dependency relationships

`po_portal_suggestion.py` has no internal module dependencies (it imports
nothing else from this repo) — it is fully self-contained. External
library dependencies (from `requirements.txt`, repo-wide, unpinned):
`streamlit`, `pandas`, `google-cloud-bigquery`, `google-auth`, `openpyxl`,
`xlsxwriter`, `xlrd`, `db-dtypes`, `pyarrow`. (`requirements.txt` is
shared across every tool in the repo, not per-app — this file doesn't
declare its own narrower dependency set.)

## External services and APIs

Only one: **Google BigQuery**, accessed via the `google-cloud-bigquery`
Python client with service-account credentials read from
`st.secrets["connections"]["bigquery"]`. No other external API, no email
service, no GCS, no third-party integration in this file (unlike
`po_portal/`, which also uses GCS and SMTP).

## Database interactions

Three distinct queries (no N+1 pattern — query count is low and fixed,
the problem is query *shape*, not query *multiplicity*):

1. `SELECT distributor_company, password_hash FROM po_portal_distributor_users WHERE username = @username AND is_active = TRUE LIMIT 1` — parameterized, has `LIMIT 1`, looks reasonable.
2. `SELECT <23 columns> FROM po_portal_suggestion` — **no WHERE clause, no LIMIT.**
3. `SELECT <8 columns> FROM dms.gt_po_tracking_all_mv [WHERE LOWER(distributor_name) LIKE '%...%']` — **no LIMIT either branch; non-Admin branch's LIKE pattern has a leading wildcard.**

Plus one write: `bq_client.insert_rows_json(po_portal_feedback, records, ...)`
— a single batched insert, not row-by-row.

## File handling process

- **Downloads**: two in-memory Excel files built with `pandas.ExcelWriter`
  + `xlsxwriter`, generated fresh on every script rerun.
- **Uploads**: one `.xlsx` file accepted via `st.file_uploader`, read
  fully into memory with `pd.read_excel`, validated, then discarded after
  the insert (not persisted to disk or cloud storage).

## Authentication flow

```
1. User submits username + password via st.text_input
2. check_login() queries po_portal_distributor_users for that username
   (only if is_active = TRUE)
3. Plaintext comparison: password == stored value from a column named
   "password_hash"
4. On match: session_state.logged_in = True, distributor_company set,
   st.rerun()
5. On mismatch or no row: st.error(), stays on login screen
```

Note: this comparison is a direct equality check against a column named
`password_hash`. If that column genuinely stores a hash (e.g. bcrypt),
this comparison would *always fail* for a correctly-hashed password,
since a plaintext input never equals its own hash — meaning either (a)
the column actually stores plaintext despite its name, or (b) login is
broken for any user whose password was ever hashed correctly. This is a
**security and correctness** finding, included here because it sits
directly in the same code path being analyzed, though it is outside this
report's performance/stability focus — flagging it for separate
follow-up.

## Hosting / deployment topology (inferred, not confirmed)

No Dockerfile, Procfile, Kubernetes manifest, or cloud deployment config
exists anywhere in this repository for this app. The only container-like
config found is `.devcontainer/devcontainer.json`, which is a **GitHub
Codespaces development environment** (launches `whitespace_map.py`, a
different tool, for local dev) — not a production deployment artifact.
Combined with the repo's `requirements.txt`-at-root convention (no
per-app dependency isolation) and 40 commits with auto-generated "Update
po_portal_suggestion.py" messages (consistent with edits made directly
through GitHub's web UI or Streamlit Community Cloud's built-in editor),
the most likely hosting model is **Streamlit Community Cloud**, with each
`.py` file in this repo deployed as its own separate Community Cloud app
pointing at a different main file path.

**This is an inference, not a confirmed fact.** It should be verified
directly with whoever manages deployment before being treated as given —
see the open question in
[streamlit_performance_findings.md](streamlit_performance_findings.md).
If confirmed, it materially changes several conclusions, since Community
Cloud's free/community tier has well-documented constraints (idle-sleep
with cold-start wake, single shared compute instance per app, no
autoscaling, modest CPU/RAM ceiling) that would independently explain
"occasionally unavailable" and "gets worse with more users" regardless of
any code-level fix.
