# Executive Summary — `po_portal_suggestion.py` Stability & Performance

**Scope of this analysis:** static review of the application source code
(`po_portal_suggestion.py`, 613 lines), its dependencies (`requirements.txt`),
its Streamlit config (`.streamlit/config.toml`), its git history (40 commits,
2026-01-26 → 2026-04-06, none since), and a comparison against four sibling
Streamlit tools in the same repository that solve the same problems
(`po_buffer.py`, `po_simulator.py`, `po_simulator_v2.py`, and the newer
`po_portal/utils/bq_ops.py`).

**What this analysis does NOT include, and why:** no production logs, no
APM/monitoring data, no live server metrics (CPU/RAM/disk/network), no
BigQuery query-execution history, no container/infra configuration (none
exists in the repo for this app), and no load testing was performed against
the live application (running load tests against a production app serving
real users without explicit authorization and a controlled window would
itself be a stability risk, and was out of scope/not requested). Every
finding below is graded by how it was obtained — see the confidence label
on each.

## Scoring

These are qualitative assessments derived from code evidence, not
measurements of the live system. A score of 10/10 would require an
incident-free, load-tested, fully-instrumented application — something
that cannot be claimed from code review alone.

| Dimension | Score | Basis |
|---|---|---|
| **Stability** | 3/10 | Zero error handling around any network/database call; unpinned dependencies; no evidence of retry/circuit-breaking |
| **Performance** | 3/10 | Expensive client re-initialization on every interaction; two queries with no server-side filtering or row limits; unconditional file-generation work on every rerun |
| **Scalability** | 3/10 | Single-process Streamlit execution model with CPU-bound work (pandas, Excel serialization) competing for one GIL; per-company cache growth with no entry cap |
| **Maintainability** | 5/10 | Code is short and readable, but duplicated logic (two near-identical Excel-export blocks), no tests, no error messages tied to root cause, 40 "Update po_portal_suggestion.py" commits with no changelog |

## Top Findings (ranked by how directly they explain the reported symptoms)

1. **BigQuery client and credentials are rebuilt on every single user
   interaction, for every user** — not wrapped in `@st.cache_resource`.
   Confirmed by direct comparison: three sibling tools in this same repo
   (`po_buffer.py`, `po_simulator.py`, `po_simulator_v2.py`) already use
   `@st.cache_resource` for this exact pattern. This file is the outlier.
   **(High confidence — confirmed in code.)**
2. **The PO Suggestion query loads every distributor's data on every
   cache-miss**, then filters to the logged-in company in pandas
   afterward, instead of filtering in SQL. `po_portal/`'s own newer
   rewrite of this same query (`po_portal/utils/bq_ops.py`) already does
   this correctly with a `WHERE distributor_company = @company` clause —
   the fix pattern already exists, tested, in this codebase.
   **(High confidence — confirmed in code, fix precedent confirmed.)**
3. **The PO Tracking query for Admin has no filter and no row limit**,
   and the per-distributor version uses a leading-wildcard `LIKE
   '%company%'`, which cannot use BigQuery column pruning/clustering and
   forces a full scan. This issue exists in *both* this app and
   `po_portal/`'s newer version — it is not yet fixed anywhere in the
   codebase. **(High confidence — confirmed in code.)**
4. **Two Excel files are regenerated on every script rerun** (every
   filter change), regardless of whether the user ever clicks download.
   **(High confidence — confirmed in code.)**
5. **No error handling anywhere around a BigQuery call** — a transient
   network blip, auth token expiry, or quota throttle surfaces as a raw
   traceback for that user rather than a retry or graceful message.
   **(High confidence — confirmed in code; cannot confirm without logs
   whether this is the actual proximate cause of reported downtime.)**
6. **No version pins in `requirements.txt`** (`streamlit`, `pandas`,
   `google-cloud-bigquery`, etc. are all unpinned). A routine
   rebuild/redeploy can silently change behavior or resource usage with
   no corresponding code change — plausible explanation for instability
   appearing *without* any recent commit (the file hasn't changed since
   2026-04-06). **(High confidence on the unpinned fact; medium
   confidence this is an active contributor — requires confirming actual
   installed versions and redeploy history.)**
7. **Likely Streamlit Community Cloud hosting** (inferred from: no
   Dockerfile, no Procfile, a Codespaces-oriented devcontainer, and
   conventions matching this repo's other tools) — if correct, this
   tier's known behavior (apps idle-sleep and cold-start, single shared
   instance, modest CPU/RAM ceiling, no autoscaling) independently
   explains "occasionally unavailable" and "gets worse with more users"
   regardless of code quality. **(Inference, not confirmed — needs a
   yes/no from whoever manages deployment.)**

## Major Risks

- **Compounding effect**: findings #1 and #4 both scale with the *number
  of interactions*, not just the number of users — a single user
  rapidly changing filters generates the same repeated overhead as
  several users acting once each. This means the degradation curve is
  steeper than "more users = more load" alone would suggest.
- **Silent data growth**: findings #2 and #3 don't get worse from a code
  change — they get worse automatically as the underlying BigQuery
  tables accumulate more historical rows. A system that "used to be
  fine" can degrade over months with zero code or traffic changes.
- **Unpinned dependencies** (#6) mean the current production behavior
  cannot be fully explained from this repo alone — the actual installed
  versions need to be checked directly.

## Quick Wins (high confidence, low risk, precedent already exists in this repo)

1. Wrap the BigQuery client/credentials setup in `@st.cache_resource` —
   copy the exact pattern already used in `po_simulator_v2.py` line 78-80
   or `po_portal/utils/bq_ops.py` line 15-20.
2. Push the `distributor_company` filter into the `load_po_suggestion()`
   SQL query — copy the exact pattern already used in
   `po_portal/utils/bq_ops.py` line 40-55.
3. Gate both Excel-generation blocks behind their own button click
   (`if st.button("Prepare Excel"):`) instead of running unconditionally
   every rerun.

See [streamlit_improvement_backlog.md](streamlit_improvement_backlog.md)
for the full prioritized list, and
[streamlit_performance_findings.md](streamlit_performance_findings.md) for
the line-by-line evidence behind every item above.
