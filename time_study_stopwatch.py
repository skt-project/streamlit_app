"""
app.py — Sales Activity Time-Motion Tracker
============================================

Single-file Streamlit application for field sales supervisors to record
store-visit activity durations and distributor check-in/out events,
persisted to Google BigQuery.

Running locally
---------------
    streamlit run app.py

Environment variables (optional overrides)
------------------------------------------
    BQ_PROJECT                       BigQuery project ID
    BQ_DATASET                       BigQuery dataset name
    BQ_TABLE                         BigQuery table name
    GOOGLE_APPLICATION_CREDENTIALS   Path to a service-account JSON key file
    MASTER_DATA_TTL                  Master store data cache TTL in seconds (default 3600)

Credential resolution order
----------------------------
    1. Local JSON file  (GOOGLE_APPLICATION_CREDENTIALS env var)
    2. Streamlit secret  [gcp_service_account]
    3. Streamlit secret  [connections.bigquery]
    4. Application Default Credentials  (Cloud Run / GKE / gcloud ADC)
"""

from __future__ import annotations

import hashlib
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import streamlit as st
from google.api_core import exceptions as gcp_exc
from google.cloud import bigquery
from google.oauth2 import service_account
from streamlit_js_eval import get_geolocation


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================

class BigQueryConfig:
    PROJECT:      str   = os.environ.get("BQ_PROJECT", "skintific-data-warehouse")
    DATASET:      str   = os.environ.get("BQ_DATASET", "gt_schema")
    TABLE:        str   = os.environ.get("BQ_TABLE",   "gt_salesman_time_motion")
    MASTER_TABLE: str   = "master_store_database_basis"
    INSERT_MAX_RETRIES:   int   = 3
    INSERT_RETRY_DELAY_S: float = 1.0
    MASTER_DATA_TTL:      int   = int(os.environ.get("MASTER_DATA_TTL", 3600))

    @classmethod
    def full_table(cls) -> str:
        return f"`{cls.PROJECT}.{cls.DATASET}.{cls.TABLE}`"


class LocaleConfig:
    TZ_NAME:   str      = "Asia/Jakarta"
    TZ:        ZoneInfo = ZoneInfo(TZ_NAME)
    TZ_OFFSET: timedelta = timedelta(hours=7)


LOCAL_CREDENTIALS_PATH: str = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    r"C:\Users\Bella Chelsea\Documents\skintific-data-warehouse-ea77119e2e7a.json",
)


# ============================================================
# Domain constants
# ============================================================

@dataclass(frozen=True)
class Activity:
    key:   str
    label: str
    icon:  str

    def display(self) -> str:
        return f"{self.icon} {self.label}"


@dataclass(frozen=True)
class P1Action:
    key:   str
    label: str
    icon:  str

    def display(self) -> str:
        return f"{self.icon} {self.label}"


ACTIVITIES: List[Activity] = [
    Activity("preparation",        "Preparation",        "🗂️"),
    Activity("greetings",          "Greetings",          "👋"),
    Activity("stock_check",        "Stock Check",        "📦"),
    Activity("merchandise",        "Merchandise",        "🛍️"),
    Activity("payment",            "Payment",            "💳"),
    Activity("sales_presentation", "Sales Presentation", "📊"),
    Activity("taking_order",       "Taking Order",       "📝"),
]

ACTIVITY_MAP: Dict[str, Activity] = {a.key: a for a in ACTIVITIES}

P1_ACTIONS: List[P1Action] = [
    P1Action("dist_in",  "Check In Distributor",  "📥"),
    P1Action("dist_out", "Check Out Distributor", "📤"),
    P1Action("ish_in",   "Check In ISHOMA",       "🍽️"),
    P1Action("ish_out",  "Check Out ISHOMA",      "🍽️"),
]

P1_ACTION_MAP: Dict[str, P1Action] = {a.key: a for a in P1_ACTIONS}


# ============================================================
# SQL
# ============================================================

_MASTER_QUERY = f"""
    SELECT
        UPPER(spv_g2g)         AS spv,
        CASE
            WHEN region_g2g = '' THEN UPPER(region)
            ELSE UPPER(region_g2g)
        END                    AS region,
        UPPER(distributor_g2g) AS distributor,
        cust_id                AS store_id,
        store_name
    FROM `{BigQueryConfig.PROJECT}.{BigQueryConfig.DATASET}.{BigQueryConfig.MASTER_TABLE}`
    WHERE spv_g2g         <> ''
      AND spv_g2g         <> '-'
      AND distributor_g2g <> '-'
    ORDER BY spv, region, distributor, store_name
"""

_DDL = f"""
CREATE TABLE IF NOT EXISTS {BigQueryConfig.full_table()} (
    event_id              STRING    NOT NULL,
    spv                   STRING    NOT NULL,
    region                STRING    NOT NULL,
    distributor           STRING    NOT NULL,
    activity_key          STRING    NOT NULL,
    activity_label        STRING    NOT NULL,
    logged_at             TIMESTAMP NOT NULL,
    store_id              STRING,
    store_name            STRING,
    duration_seconds      INT64,
    started_at            TIMESTAMP,
    ended_at              TIMESTAMP,
    latitude              FLOAT64,
    longitude             FLOAT64,
    location_accuracy_m   INT64,
    created_at            TIMESTAMP,
    device_id             STRING
)
PARTITION BY DATE(logged_at)
CLUSTER BY spv, distributor, store_id
"""

_DEDUP_QUERY = (
    f"SELECT event_id FROM {BigQueryConfig.full_table()} "
    f"WHERE event_id IN UNNEST(@ids)"
)


# ============================================================
# CSS & HTML templates
# ============================================================

_APP_CSS = """
<style>
/* ── Hide Streamlit chrome ── */
#MainMenu { visibility: hidden; }
footer    { visibility: hidden; }
header    { visibility: hidden; }
.block-container { padding-top: 1rem !important; }

/* ── Design tokens ── */
:root {
    --accent:  #f5a623;
    --danger:  #e05c5c;
    --green:   #4ade80;
    --muted:   #6b7280;
    --surface: #12151c;
    --border:  rgba(255, 255, 255, 0.08);
}

/* ── Metric cards ── */
div[data-testid="metric-container"] {
    background:    var(--surface);
    border:        1px solid var(--border);
    border-radius: 12px;
    padding:       12px 16px;
}

/* ── Status badges ── */
.badge {
    display:       inline-block;
    padding:       3px 10px;
    border-radius: 20px;
    font-size:     0.75rem;
    font-weight:   600;
}
.badge-geo-ok {
    background: rgba(45, 212, 191, 0.12);
    color:      #2dd4bf;
    border:     1px solid rgba(45, 212, 191, 0.3);
}
.badge-geo-warn {
    background: rgba(245, 166, 35, 0.12);
    color:      var(--accent);
    border:     1px solid rgba(245, 166, 35, 0.3);
}

/* ── Section label ── */
.section-label {
    font-size:      0.65rem;
    font-weight:    700;
    letter-spacing: 2px;
    text-transform: uppercase;
    color:          var(--muted);
    margin-bottom:  4px;
}
</style>
"""


def _stopwatch_card_html(act_name: str, elapsed_fmt: str, color: str, status: str) -> str:
    return f"""
<div style="
    background:    var(--surface);
    border:        1px solid var(--border);
    border-radius: 14px;
    padding:       24px;
    text-align:    center;
    position:      relative;
    overflow:      hidden;
">
  <div style="
      position:   absolute;
      top: 0; left: 0; right: 0;
      height:     3px;
      background: linear-gradient(90deg, #f5a623, #f97316, #e05c5c);
  "></div>
  <div style="
      font-size:      0.68rem;
      font-weight:    700;
      letter-spacing: 2px;
      text-transform: uppercase;
      color:          var(--muted);
      margin-bottom:  4px;
  ">Current Activity</div>
  <div style="
      font-size:     1.05rem;
      font-weight:   600;
      color:         var(--accent);
      margin-bottom: 12px;
  ">{act_name}</div>
  <div style="
      font-family:    monospace;
      font-size:      3.5rem;
      font-weight:    700;
      color:          {color};
      letter-spacing: -2px;
  ">{elapsed_fmt}</div>
  <div style="
      font-size:      0.72rem;
      color:          var(--muted);
      margin-top:     8px;
      letter-spacing: 1px;
      text-transform: uppercase;
  ">{status}</div>
</div>
"""


# ============================================================
# Formatting & geo helpers
# ============================================================

def _fmt_ms(ms: int) -> str:
    """Format milliseconds as HH:MM:SS."""
    s = max(int(ms / 1000), 0)
    h, remainder = divmod(s, 3600)
    m, sec = divmod(remainder, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}"


def _geo_label(lat: Optional[float], lng: Optional[float], acc: Optional[int]) -> str:
    if lat is not None and lng is not None:
        acc_str = f" ±{acc}m" if acc is not None else ""
        return f"📍 {lat:.5f}, {lng:.5f}{acc_str}"
    return "📍 lokasi tidak tersedia"


def _extract_coords(
    loc: Optional[dict],
) -> Tuple[Optional[float], Optional[float], Optional[int]]:
    """Parse a streamlit-js-eval geolocation payload safely."""
    try:
        if loc and "coords" in loc:
            c   = loc["coords"]
            lat = c.get("latitude")
            lng = c.get("longitude")
            acc = c.get("accuracy")
            if lat is not None and lng is not None:
                return float(lat), float(lng), (int(acc) if acc is not None else None)
    except (TypeError, ValueError, KeyError):
        pass
    return None, None, None


# ============================================================
# Session state
# ============================================================

_STATE_DEFAULTS: Dict[str, Any] = {
    "page":             "setup",
    "spv":              "",
    "region":           "",
    "distributor":      "",
    "store_id":         "",
    "store_name":       "",
    "store_geo_done":   set(),    # store_ids whose GPS has been captured
    "act_key":          "",
    "act_label":        "",
    "timer_running":    False,
    "timer_elapsed_ms": 0,
    "timer_started_at": None,     # datetime | None
    "totals":           {},       # {activity_key: accumulated_ms}
    "write_phase":      None,     # "dist_in" | "store_session" | None
    "pending_payload":  None,     # dict | None
    "master":           None,     # MasterData | None
}


def _init_state() -> None:
    for key, default in _STATE_DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = (
                default.copy() if isinstance(default, (dict, set, list)) else default
            )


# Read-only shorthand accessors
def _spv()              -> str:               return st.session_state.spv
def _region()           -> str:               return st.session_state.region
def _distributor()      -> str:               return st.session_state.distributor
def _store_id()         -> str:               return st.session_state.store_id
def _store_name()       -> str:               return st.session_state.store_name
def _act_key()          -> str:               return st.session_state.act_key
def _act_label()        -> str:               return st.session_state.act_label
def _timer_running()    -> bool:              return st.session_state.timer_running
def _timer_elapsed_ms() -> int:               return st.session_state.timer_elapsed_ms
def _timer_started_at() -> Optional[datetime]: return st.session_state.timer_started_at
def _totals()           -> Dict[str, int]:    return st.session_state.totals
def _geo_done()         -> Set[str]:          return st.session_state.store_geo_done
def _write_phase()      -> Optional[str]:     return st.session_state.write_phase
def _pending_payload()  -> Optional[dict]:    return st.session_state.pending_payload


def _reset_timer() -> None:
    st.session_state.timer_running    = False
    st.session_state.timer_started_at = None
    st.session_state.timer_elapsed_ms = 0


def _reset_activity() -> None:
    st.session_state.act_key   = ""
    st.session_state.act_label = ""


def _reset_store_data() -> None:
    _reset_timer()
    _reset_activity()
    st.session_state.totals = {}
    st.session_state.store_geo_done.discard(st.session_state.store_id)


# ============================================================
# BigQuery — credentials
# ============================================================

def _creds_from_local() -> Tuple[service_account.Credentials, str]:
    if not os.path.exists(LOCAL_CREDENTIALS_PATH):
        raise FileNotFoundError(f"Local credentials not found: {LOCAL_CREDENTIALS_PATH}")
    creds = service_account.Credentials.from_service_account_file(LOCAL_CREDENTIALS_PATH)
    return creds, BigQueryConfig.PROJECT


def _creds_from_secrets(key: str) -> Tuple[service_account.Credentials, str]:
    raw = (
        st.secrets["connections"]["bigquery"]
        if key == "connections"
        else st.secrets[key]
    )
    d = dict(raw)
    if "private_key" in d:
        d["private_key"] = d["private_key"].replace("\\n", "\n")
    creds   = service_account.Credentials.from_service_account_info(d)
    project = d.get("project_id") or d.get("project") or BigQueryConfig.PROJECT
    return creds, project


# ============================================================
# BigQuery — client factory
# ============================================================

@st.cache_resource(show_spinner=False)
def _get_bq_client() -> Optional[bigquery.Client]:
    """
    Resolve credentials and return a cached BigQuery client.
    Tries four sources in priority order; last resort is ADC.
    """
    loaders = [
        ("local_file",           _creds_from_local),
        ("gcp_service_account",  lambda: _creds_from_secrets("gcp_service_account")),
        ("connections_bigquery", lambda: _creds_from_secrets("connections")),
    ]
    for name, fn in loaders:
        try:
            creds, project = fn()
            client = bigquery.Client(credentials=creds, project=project)
            logger.info("BigQuery client ready via '%s' (project=%s)", name, project)
            return client
        except Exception as exc:
            logger.debug("Credential source '%s' skipped: %s", name, exc)

    try:
        client = bigquery.Client(project=BigQueryConfig.PROJECT)
        logger.info("BigQuery client ready via Application Default Credentials")
        return client
    except Exception as exc:
        logger.error("All credential sources exhausted: %s", exc)
        return None


# ============================================================
# BigQuery — schema bootstrap
# ============================================================

def _ensure_schema(client: bigquery.Client) -> None:
    try:
        client.query(_DDL).result()
        logger.info("Schema bootstrap OK: %s", BigQueryConfig.full_table())
    except gcp_exc.GoogleAPIError as exc:
        logger.error("Schema bootstrap failed: %s", exc)


# ============================================================
# BigQuery — master data
# ============================================================

@dataclass
class MasterData:
    spv_list: List[str]
    by_spv:   Dict[str, Any]
    error:    Optional[str] = None

    def ok(self) -> bool:
        return self.error is None


@st.cache_data(ttl=BigQueryConfig.MASTER_DATA_TTL, show_spinner=False)
def _load_master_data() -> MasterData:
    """
    Load the full store hierarchy from BigQuery and cache for MASTER_DATA_TTL seconds.
    On failure, returns a MasterData with the error field set so callers
    can degrade gracefully without crashing.
    """
    client = _get_bq_client()
    if client is None:
        return MasterData(spv_list=[], by_spv={}, error="Cannot connect to BigQuery.")

    try:
        rows = list(client.query(_MASTER_QUERY).result())
    except gcp_exc.GoogleAPIError as exc:
        logger.error("Master data query failed: %s", exc)
        return MasterData(spv_list=[], by_spv={}, error=str(exc))

    by_spv: Dict[str, Any] = {}
    for row in rows:
        spv   = (row.spv         or "").strip()
        reg   = (row.region      or "").strip()
        dist  = (row.distributor or "").strip()
        sid   = str(row.store_id or "").strip()
        sname = (row.store_name  or "").strip()

        if not (spv and reg and dist):
            continue

        spv_node = by_spv.setdefault(spv, {"regions": [], "by_region": {}})
        if reg not in spv_node["regions"]:
            spv_node["regions"].append(reg)

        reg_node = spv_node["by_region"].setdefault(
            reg, {"distributors": [], "by_dist": {}}
        )
        if dist not in reg_node["distributors"]:
            reg_node["distributors"].append(dist)

        store_list: List[Dict] = reg_node["by_dist"].setdefault(dist, [])
        if sid not in {s["store_id"] for s in store_list}:
            store_list.append({"store_id": sid, "store_name": sname})

    return MasterData(spv_list=sorted(by_spv.keys()), by_spv=by_spv)


# ============================================================
# BigQuery — write helpers
# ============================================================

@dataclass
class WriteResult:
    ok:       bool
    message:  str
    inserted: int = 0
    skipped:  int = 0


def _make_event_id(spv: str, key: str, store_id: Optional[str], ts: datetime) -> str:
    raw = f"{spv}|{key}|{store_id or ''}|{ts.isoformat()}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _to_wib(dt: Optional[datetime]) -> Optional[datetime]:
    return dt.astimezone(LocaleConfig.TZ) if dt else None


def _serialise(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}


def _fetch_existing_ids(client: bigquery.Client, ids: List[str]) -> Set[str]:
    if not ids:
        return set()
    job_cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", ids)]
    )
    try:
        return {r.event_id for r in client.query(_DEDUP_QUERY, job_config=job_cfg).result()}
    except gcp_exc.GoogleAPIError as exc:
        logger.warning("Dedup query failed (skipping pre-dedup): %s", exc)
        return set()


def _insert_with_retry(client: bigquery.Client, rows: List[Dict[str, Any]]) -> WriteResult:
    """
    Streaming insert with client-side pre-dedup, server-side idempotency
    via row_ids, and exponential back-off retry on transient errors.
    """
    existing = _fetch_existing_ids(client, [r["event_id"] for r in rows])
    new_rows = [r for r in rows if r["event_id"] not in existing]
    skipped  = len(rows) - len(new_rows)

    if not new_rows:
        return WriteResult(ok=True, message="ℹ️ Semua record sudah tersimpan.", skipped=skipped)

    serialised = [_serialise(r) for r in new_rows]
    row_ids    = [r["event_id"] for r in new_rows]
    table_ref  = BigQueryConfig.full_table().strip("`")
    last_error: Optional[str] = None

    for attempt in range(1, BigQueryConfig.INSERT_MAX_RETRIES + 1):
        try:
            errors = client.insert_rows_json(table_ref, serialised, row_ids=row_ids)
            if not errors:
                msg = f"✅ {len(new_rows)} record tersimpan."
                if skipped:
                    msg += f" ({skipped} duplikat dilewati)"
                logger.info("Insert OK: %d rows, %d skipped", len(new_rows), skipped)
                return WriteResult(ok=True, message=msg, inserted=len(new_rows), skipped=skipped)
            last_error = str(errors)
            logger.warning("Insert attempt %d errors: %s", attempt, last_error)
        except gcp_exc.GoogleAPIError as exc:
            last_error = str(exc)
            logger.warning("Insert attempt %d exception: %s", attempt, last_error)

        if attempt < BigQueryConfig.INSERT_MAX_RETRIES:
            delay = BigQueryConfig.INSERT_RETRY_DELAY_S * (2 ** (attempt - 1))
            logger.info("Retrying in %.1fs…", delay)
            time.sleep(delay)

    return WriteResult(
        ok=False,
        message=(
            f"❌ Insert gagal setelah {BigQueryConfig.INSERT_MAX_RETRIES} "
            f"percobaan: {last_error}"
        ),
    )


# ============================================================
# BigQuery — public write API
# ============================================================

def _write_checkin_event(
    *,
    spv: str,
    region: str,
    distributor: str,
    action_key: str,
    action_label: str,
    event_time_ms: int,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    acc: Optional[int]   = None,
) -> WriteResult:
    """Persist a P1 check-in / check-out point event (instantaneous, no duration)."""
    client = _get_bq_client()
    if client is None:
        return WriteResult(ok=False, message="Tidak dapat terhubung ke BigQuery.")

    logged_at = _to_wib(datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc))
    row: Dict[str, Any] = {
        "event_id":            _make_event_id(spv, action_key, None, logged_at),
        "spv":                 spv,
        "region":              region,
        "distributor":         distributor,
        "activity_key":        action_key,
        "activity_label":      action_label,
        "logged_at":           logged_at,
        "store_id":            None,
        "store_name":          None,
        "duration_seconds":    None,
        "started_at":          None,
        "ended_at":            None,
        "latitude":            lat,
        "longitude":           lng,
        "location_accuracy_m": acc,
        "created_at":          datetime.now(timezone.utc),
        "device_id":           None,
    }
    return _insert_with_retry(client, [row])


def _write_activity_session(
    *,
    spv: str,
    region: str,
    distributor: str,
    activity_key: str,
    activity_label: str,
    store_id: Optional[str],
    store_name: str,
    duration_ms: int,
    ended_at_iso: str,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    acc: Optional[int]   = None,
) -> WriteResult:
    """Persist a timed store-visit activity session."""
    client = _get_bq_client()
    if client is None:
        return WriteResult(ok=False, message="Tidak dapat terhubung ke BigQuery.")

    dur_s   = int(duration_ms / 1000)
    ended   = _to_wib(datetime.fromisoformat(ended_at_iso.replace("Z", "+00:00")))
    started = (ended - timedelta(seconds=dur_s)) if ended and dur_s else None
    sid     = store_id or None

    row: Dict[str, Any] = {
        "event_id":            _make_event_id(spv, activity_key, sid, ended),
        "spv":                 spv,
        "region":              region,
        "distributor":         distributor,
        "activity_key":        activity_key,
        "activity_label":      activity_label,
        "logged_at":           ended,
        "store_id":            sid,
        "store_name":          store_name,
        "duration_seconds":    dur_s,
        "started_at":          started,
        "ended_at":            ended,
        "latitude":            lat,
        "longitude":           lng,
        "location_accuracy_m": acc,
        "created_at":          datetime.now(timezone.utc),
        "device_id":           None,
    }
    return _insert_with_retry(client, [row])


# ============================================================
# App helpers
# ============================================================

def _get_live_ms() -> int:
    """Current elapsed ms, including any in-progress running interval."""
    if _timer_running() and _timer_started_at():
        delta = datetime.now(timezone.utc) - _timer_started_at()
        return _timer_elapsed_ms() + int(delta.total_seconds() * 1000)
    return _timer_elapsed_ms()


def _get_stores() -> List[Dict]:
    master: Optional[MasterData] = st.session_state.get("master")
    if not master:
        return []
    return (
        master.by_spv
              .get(_spv(), {})
              .get("by_region", {})
              .get(_region(), {})
              .get("by_dist", {})
              .get(_distributor(), [])
    )


# ============================================================
# GPS two-phase write handler
# ============================================================

def _handle_write_phase() -> bool:
    """
    Two-phase GPS commit guard.

    When an action needs GPS:
      Phase 1 — caller stores payload dict in ``pending_payload``,
                sets ``write_phase`` to a sentinel string, then reruns.
      Phase 2 — this function fires on the next rerun, calls
                ``get_geolocation()``, waits for coordinates, persists
                the record, clears the pipeline, and reruns again.

    Returns True if a write phase was active; the caller must return
    immediately so the rest of the page is not rendered mid-phase.
    """
    if _write_phase() is None:
        return False

    loc = get_geolocation()
    if loc is None:
        st.info("📡 Mengambil koordinat GPS… pastikan izin lokasi diaktifkan.")
        return True

    lat, lng, acc = _extract_coords(loc)
    payload = _pending_payload()
    phase   = _write_phase()

    # Clear pipeline BEFORE writing — prevents re-trigger if write raises
    st.session_state.write_phase     = None
    st.session_state.pending_payload = None

    if phase == "dist_in":
        result = _write_checkin_event(
            spv=_spv(), region=_region(), distributor=_distributor(),
            action_key=payload["action_key"],
            action_label=payload["action_label"],
            event_time_ms=payload["event_time_ms"],
            lat=lat, lng=lng, acc=acc,
        )
        if result.ok:
            st.success(f"📥 **{payload['action_label']}** disimpan · {_geo_label(lat, lng, acc)}")
        else:
            st.error(result.message)

    elif phase == "store_session":
        sid = payload.get("store_id")
        if sid:
            st.session_state.store_geo_done.add(sid)

        result = _write_activity_session(
            spv=_spv(), region=_region(), distributor=_distributor(),
            activity_key=payload["activity_key"],
            activity_label=payload["activity_label"],
            store_id=payload.get("store_id"),
            store_name=payload.get("store_name", "—"),
            duration_ms=payload["duration_ms"],
            ended_at_iso=payload["ended_at"],
            lat=lat, lng=lng, acc=acc,
        )
        if result.ok:
            st.success(
                f"✅ **{payload['activity_label']}** — "
                f"{_fmt_ms(payload['duration_ms'])} · {_geo_label(lat, lng, acc)}"
            )
        else:
            st.error(result.message)

    else:
        logger.error("Unknown write_phase value: '%s'", phase)

    st.rerun()
    return True


# ============================================================
# Bootstrap
# ============================================================

def _bootstrap() -> None:
    """Load master data and ensure the output table exists (once per session)."""
    if st.session_state.get("master") is not None:
        return

    with st.spinner("⏳ Memuat data dari BigQuery…"):
        st.session_state.master = _load_master_data()

    client = _get_bq_client()
    if client:
        _ensure_schema(client)


# ============================================================
# Page: Setup
# ============================================================

def _page_setup() -> None:
    st.markdown("## ⏱ Time Motion")
    st.markdown("Pilih SPV, Region, dan Distributor sebelum memulai kunjungan.")
    st.divider()

    master: Optional[MasterData] = st.session_state.master
    spv_list = master.spv_list if master else []

    c1, c2 = st.columns(2)
    with c1:
        spv = st.selectbox("👤 Supervisor (SPV)", [""] + spv_list, key="sel_spv")

    region_list: List[str] = (
        master.by_spv.get(spv, {}).get("regions", []) if master and spv else []
    )
    with c2:
        region = st.selectbox(
            "🗺️ Region", [""] + region_list, key="sel_region", disabled=not spv
        )

    dist_list: List[str] = (
        master.by_spv.get(spv, {})
              .get("by_region", {})
              .get(region, {})
              .get("distributors", [])
        if master and spv and region else []
    )
    dist = st.selectbox(
        "🏭 Distributor", [""] + dist_list, key="sel_dist", disabled=not region
    )

    st.markdown("")
    if st.button(
        "Mulai Kunjungan →",
        type="primary",
        disabled=not (spv and region and dist),
        use_container_width=True,
    ):
        st.session_state.spv         = spv
        st.session_state.region      = region
        st.session_state.distributor = dist
        st.session_state.page        = "checkin"
        st.rerun()


# ============================================================
# Page: Check-in
# ============================================================

def _page_checkin() -> None:
    c1, c2 = st.columns([2, 1])
    with c1:
        st.markdown("## ⏱ Time Motion")
    with c2:
        st.info(f"**{_spv()}**  \n{_distributor()} · {_region()}")

    tab_p1, tab_p2 = st.tabs(["📍 Check In/Out", "📋 Activities"])
    with tab_p1:
        _tab_p1()
    with tab_p2:
        _tab_p2()

    st.divider()
    if st.button("← Kembali ke Halaman Awal"):
        if _timer_running():
            st.session_state.timer_elapsed_ms = _get_live_ms()
            _reset_timer()
        st.session_state.page = "setup"
        st.rerun()


# ============================================================
# Tab: P1 — Check In / Out
# ============================================================

def _tab_p1() -> None:
    st.markdown("#### 📋 Catat Aktivitas Check In/Out")

    display_map = {a.display(): a for a in P1_ACTIONS}
    chosen = st.selectbox(
        "Pilih Aktivitas",
        ["— Pilih Aktivitas —"] + [a.display() for a in P1_ACTIONS],
        key="p1_sel",
    )

    if st.button("✅ Catat Waktu", type="primary", use_container_width=True):
        if chosen == "— Pilih Aktivitas —":
            st.warning("Pilih aktivitas terlebih dahulu!")
            return

        action        = display_map[chosen]
        event_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        if action.key == "dist_in":
            # Requires GPS — defer to two-phase writer
            st.session_state.pending_payload = {
                "action_key":    action.key,
                "action_label":  action.label,
                "event_time_ms": event_time_ms,
            }
            st.session_state.write_phase = "dist_in"
            st.rerun()
        else:
            with st.spinner("Menyimpan…"):
                result = _write_checkin_event(
                    spv=_spv(), region=_region(), distributor=_distributor(),
                    action_key=action.key, action_label=action.label,
                    event_time_ms=event_time_ms,
                )
            if result.ok:
                st.success(f"{action.icon} **{action.label}** — {result.message}")
            else:
                st.error(result.message)


# ============================================================
# Tab: P2 — Activity stopwatch
# ============================================================

def _tab_p2() -> None:
    # ── Store selector ────────────────────────────────────────────────────
    st.markdown("#### 🏪 Pilih Toko")
    stores     = _get_stores()
    store_opts = {f"{s['store_name']} ({s['store_id']})": s for s in stores}
    cur_label  = next(
        (k for k, v in store_opts.items() if v["store_id"] == _store_id()), ""
    )

    choice = st.selectbox(
        "Cari / Pilih Toko",
        ["— Pilih Toko —"] + list(store_opts.keys()),
        index=(list(store_opts.keys()).index(cur_label) + 1 if cur_label else 0),
        key="p2_store_sel",
    )
    if choice != "— Pilih Toko —":
        s = store_opts[choice]
        if s["store_id"] != _store_id():
            st.session_state.store_id   = s["store_id"]
            st.session_state.store_name = s["store_name"]

    if _store_id():
        col_geo, col_reset = st.columns([3, 1])
        with col_geo:
            if _store_id() in _geo_done():
                st.markdown(
                    '<span class="badge badge-geo-ok">📍 Lokasi toko sudah terekam</span>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<span class="badge badge-geo-warn">📍 GPS direkam pada aktivitas pertama</span>',
                    unsafe_allow_html=True,
                )
        with col_reset:
            if st.button(
                "🔄 Reset Data",
                use_container_width=True,
                help="Reset timer, aktivitas, dan total waktu untuk toko ini",
            ):
                _reset_store_data()
                st.success("✅ Data toko berhasil direset.")

    st.divider()

    # ── Activity selector ─────────────────────────────────────────────────
    st.markdown("#### 🗂 Stopwatch Aktivitas")

    act_list    = ["— Pilih Aktivitas —"] + [a.display() for a in ACTIVITIES]
    act_key_map = {a.display(): a.key for a in ACTIVITIES}
    cur_act     = ""
    if _act_key():
        a = ACTIVITY_MAP.get(_act_key())
        if a:
            cur_act = a.display()

    act_choice = st.selectbox(
        "Pilih Aktivitas",
        act_list,
        index=(act_list.index(cur_act) if cur_act in act_list else 0),
        key="p2_act_sel",
    )
    if act_choice != "— Pilih Aktivitas —":
        new_key = act_key_map[act_choice]
        if new_key != _act_key():
            if _timer_running():
                st.session_state.timer_elapsed_ms = _get_live_ms()
                _reset_timer()
            a = ACTIVITY_MAP[new_key]
            st.session_state.act_key          = new_key
            st.session_state.act_label        = a.display()
            st.session_state.timer_elapsed_ms = 0

    # ── Stopwatch display ─────────────────────────────────────────────────
    elapsed  = _get_live_ms()
    act_name = _act_label() or "— None Selected —"
    color    = "#4ade80" if _timer_running() else ("#f5a623" if elapsed > 0 else "#e8eaf0")
    status   = "🟢 Recording" if _timer_running() else ("🟡 Paused" if elapsed > 0 else "○ Idle")

    st.markdown(
        _stopwatch_card_html(act_name, _fmt_ms(elapsed), color, status),
        unsafe_allow_html=True,
    )

    # ── Controls ──────────────────────────────────────────────────────────
    c1, c2 = st.columns(2)
    with c1:
        if st.button(
            "▶ Start",
            type="primary",
            disabled=_timer_running() or not _act_key(),
            use_container_width=True,
        ):
            if not _store_id():
                st.warning("Pilih toko terlebih dahulu!")
            else:
                st.session_state.timer_running    = True
                st.session_state.timer_started_at = datetime.now(timezone.utc)
                st.rerun()

    with c2:
        if st.button("⏹ Stop & Save", type="secondary", use_container_width=True):
            _stop_and_save()

    # Auto-refresh every second while timer is live
    if _timer_running():
        time.sleep(1)
        st.rerun()

    # ── Activity totals ───────────────────────────────────────────────────
    st.divider()
    st.markdown(
        '<div class="section-label">📊 Total Waktu Per Aktivitas</div>',
        unsafe_allow_html=True,
    )
    live = _get_live_ms()
    cols = st.columns(4)
    for i, a in enumerate(ACTIVITIES):
        total = _totals().get(a.key, 0)
        if _act_key() == a.key:
            total += live
        with cols[i % 4]:
            st.metric(a.display(), _fmt_ms(total))


# ============================================================
# Stop & Save
# ============================================================

def _stop_and_save() -> None:
    elapsed  = _get_live_ms()
    store_id = _store_id()

    if elapsed <= 0 or not _act_key():
        _reset_timer()
        _reset_activity()
        st.rerun()
        return

    payload = {
        "activity_key":   _act_key(),
        "activity_label": _act_label(),
        "store_id":       store_id,
        "store_name":     _store_name() or "—",
        "duration_ms":    elapsed,
        "ended_at":       datetime.now(timezone.utc).isoformat(),
    }

    # Accumulate totals before resetting state
    st.session_state.totals[_act_key()] = _totals().get(_act_key(), 0) + elapsed

    _reset_timer()
    _reset_activity()

    if store_id and store_id not in _geo_done():
        # First activity for this store → capture GPS coordinates
        st.session_state.pending_payload = payload
        st.session_state.write_phase     = "store_session"
        st.rerun()
    else:
        with st.spinner("Menyimpan…"):
            result = _write_activity_session(
                spv=_spv(), region=_region(), distributor=_distributor(),
                activity_key=payload["activity_key"],
                activity_label=payload["activity_label"],
                store_id=payload.get("store_id"),
                store_name=payload["store_name"],
                duration_ms=payload["duration_ms"],
                ended_at_iso=payload["ended_at"],
            )
        if result.ok:
            st.success(
                f"✅ **{payload['activity_label']}** — "
                f"{_fmt_ms(elapsed)} — {result.message}"
            )
        else:
            st.error(result.message)
        st.rerun()


# ============================================================
# Entry point
# ============================================================

st.set_page_config(
    page_title="Sales Activity Timer",
    page_icon="⏱️",
    layout="wide",
    initial_sidebar_state="collapsed",
)
st.markdown(_APP_CSS, unsafe_allow_html=True)


def main() -> None:
    _init_state()

    # GPS two-phase handler must run first — halts rendering until coords arrive
    if _handle_write_phase():
        return

    _bootstrap()

    master: Optional[MasterData] = st.session_state.get("master")
    if master and not master.ok():
        st.error(f"⚠️ BigQuery error: {master.error}")
    elif master and not master.spv_list:
        st.warning("⚠️ Data master kosong. Hubungi administrator.")

    if st.session_state.page == "setup":
        _page_setup()
    else:
        _page_checkin()


if __name__ == "__main__":
    main()
