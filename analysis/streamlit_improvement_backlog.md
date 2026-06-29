# Prioritized Improvement Backlog — `po_portal_suggestion.py`

Ordered by priority. "Risk" is the risk of *making this change*, not the
risk of the underlying problem. Full evidence for each item is in
[streamlit_performance_findings.md](streamlit_performance_findings.md)
(referenced as F1-F8).

| Priority | Category | Issue | Root Cause | Expected Benefit | Effort | Risk |
|---|---|---|---|---|---|---|
| P0 | Caching / Resource mgmt | BigQuery client & credentials rebuilt on every interaction (F1) | Client setup is module-level code, not wrapped in `@st.cache_resource` | Removes fixed per-interaction overhead for every user, every action; biggest "free" win available | Low — copy the existing pattern from `po_simulator_v2.py` or `po_portal/utils/bq_ops.py` in this same repo | Very low — pure refactor, no behavior change |
| P0 | Database / Query design | PO Suggestion query fetches all distributors' data, filters in pandas after fetch (F2) | No `WHERE distributor_company` clause in SQL | Query payload, network transfer, and memory drop from "whole table" to "one company's rows" per non-Admin session | Low — adapt the already-working version in `po_portal/utils/bq_ops.py` | Low — needs a quick check that "Admin" still passes `company=None` correctly |
| P1 | Data processing | Two Excel exports regenerated on every rerun regardless of use (F4) | Export code is unconditional, not gated behind a button | Removes CPU-bound work from the common "just filtering/viewing" path | Low — wrap each export block in `if st.button(...)` | Low — minor UX change (one extra click to export), worth confirming with users |
| P1 | Database / Query design | PO Tracking query: no filter/limit for Admin, leading-wildcard `LIKE` for everyone else (F3) | Query was written without a default date bound or an indexable match pattern; same issue exists unfixed in `po_portal/` too | Likely the single largest reduction in BigQuery scan cost and load time, if the underlying view is as large as its name suggests | Medium — needs a product decision on a sensible default date range, plus checking whether an exact-match company key exists to replace the wildcard `LIKE` | Medium — changes default visible data (e.g. "last 90 days" instead of "everything"); needs sign-off from whoever relies on full history views |
| P2 | Reliability | No error handling around any BigQuery call (F5) | No `try`/`except`, no retry, no user-facing fallback message | Converts some fraction of transient failures from hard crashes into recovered requests or clear, actionable error messages | Low-Medium — add a small retry/backoff wrapper and a friendly `st.error()` fallback | Low |
| P2 | Infrastructure | Repo-wide unpinned dependencies (F6) | `requirements.txt` has no version constraints for any package | Eliminates "nothing changed but it broke" incidents tied to silent dependency upgrades on redeploy | Low — pin to current known-working versions (capture via `pip freeze` from the live/working environment first) | Medium — pinning *after* an already-drifted environment can itself surface a breaking change; should be done deliberately, ideally during a planned maintenance window |
| P2 | Caching / Memory | `load_po_tracking` cache grows one entry per distinct company, uncapped, including a full-table Admin entry (F7) | `@st.cache_data` used with no `max_entries` | Bounds worst-case cache memory instead of leaving it open-ended | Low — add `max_entries=` and/or a shorter TTL specifically for the Admin path | Low |
| P3 | Infrastructure / Verification | Hosting platform not confirmed — likely single-instance, no autoscaling (F8) | No deployment manifest in repo; inferred from absence of evidence | Determines whether further code optimization alone can solve "gets worse with more users," or whether a hosting-tier change is also required | Low effort to *verify* (ask whoever manages deployment); effort to *act on it* depends entirely on the answer | N/A to verify; the action itself (e.g. upgrading hosting tier) carries cost/budget implications, not a code risk |
| P3 | Security (adjacent finding, outside performance scope) | Login compares plaintext password against a column named `password_hash` (noted in [streamlit_architecture.md](streamlit_architecture.md)) | Either passwords aren't actually hashed, or login is broken for hashed passwords | Not a performance item — flagged for separate, dedicated follow-up rather than bundled into this backlog | N/A | N/A |

## Suggested sequencing

1. **P0 items first** (F1, F2) — both have ready-made, already-tested
   reference implementations elsewhere in this same repository, so the
   engineering risk is minimal and the fix is closer to "port existing
   code" than "design something new."
2. **P1 items next** (F4, F3) — F4 is a quick, low-risk win. F3 needs a
   short product conversation first (what's a reasonable default date
   range for PO Tracking?) before the code change, so start that
   conversation in parallel with doing P0.
3. **P2 items** (F5, F6, F7) — meaningfully improve reliability and
   memory predictability, but none of them are believed to be the
   *primary* driver of the reported symptoms on their own.
4. **P3 — verification, not code** (F8) — should happen as early as
   possible in calendar time (it's a quick question to whoever manages
   deployment) even though it's sequenced last here, because the answer
   determines whether P0-P2 alone will be sufficient or whether a hosting
   change is also necessary. The security item should be routed to
   whoever owns authentication, independent of this backlog's timeline.

## Before any load testing

Several questions in this report ("safe concurrent users," "degradation
point") can only be answered with real measurements. The safe order of
operations is:

1. Confirm the hosting platform and its actual resource allocation (P3/F8).
2. Add basic instrumentation (timing logs around each BigQuery call,
   memory snapshots) so a load test produces *attributable* data instead
   of just a pass/fail.
3. Only then run a controlled load test, against a staging copy if one
   can be stood up, or against production during a low-traffic window
   with stakeholders informed in advance — not as an unannounced test
   against the live app serving real distributors.
