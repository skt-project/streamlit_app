import os
import json
import logging
import sys
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Tuple

import streamlit as st
import streamlit.components.v1 as components
from google.oauth2 import service_account
from google.cloud import bigquery

# ============================================================
# CONFIGURATION
# ============================================================
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


class Config:
    BQ_PROJECT = "skintific-data-warehouse"
    BQ_DATASET = "gt_schema"
    BQ_TABLE = "sales_activity_records"
    CREDENTIALS_PATH = r"C:\Users\Bella Chelsea\Documents\skintific-data-warehouse-ea77119e2e7a.json"
    TZ_OFFSET = timedelta(hours=7)  # WIB = UTC+7


st.set_page_config(
    page_title="Sales Activity Timer",
    page_icon="⏱️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ────────────────────────────────────────────────
st.markdown("""
<style>
#MainMenu{visibility:hidden;}footer{visibility:hidden;}header{visibility:hidden;}
.block-container{padding-top:1rem!important;}

:root{
  --accent:#f5a623;--accent2:#e05c5c;--green:#4ade80;
  --blue:#60a5fa;--purple:#a78bfa;--teal:#2dd4bf;
  --muted:#6b7280;
}

div[data-testid="stHorizontalBlock"] button{
  border-radius:9px!important;font-weight:600!important;
}

div[data-testid="metric-container"]{
  background:#12151c;border:1px solid rgba(255,255,255,0.08);
  border-radius:12px;padding:12px 16px;
}

.tag{
  display:inline-block;padding:3px 10px;border-radius:20px;
  font-size:0.75rem;font-weight:600;
  background:rgba(245,166,35,0.15);color:var(--accent);
}
.tag-green{background:rgba(74,222,128,0.15);color:var(--green);}
.tag-red  {background:rgba(224, 92, 92,0.15);color:var(--accent2);}
.tag-blue {background:rgba(96,165,250,0.15);color:var(--blue);}
.tag-purple{background:rgba(167,139,250,0.15);color:var(--purple);}

.section-label{
  font-size:0.65rem;font-weight:700;letter-spacing:2px;
  text-transform:uppercase;color:var(--muted);margin-bottom:4px;
}

.geo-badge {
  display:inline-flex;align-items:center;gap:6px;
  padding:4px 12px;border-radius:20px;font-size:0.75rem;font-weight:600;
  background:rgba(45,212,191,0.12);color:#2dd4bf;border:1px solid rgba(45,212,191,0.25);
}
.geo-badge-warn {
  background:rgba(245,166,35,0.12);color:#f5a623;border:1px solid rgba(245,166,35,0.25);
}
</style>
""", unsafe_allow_html=True)

# ============================================================
# BigQuery queries / DDL
# ============================================================
BQ_QUERY = """
    SELECT upper(spv_g2g) as spv,
    CASE WHEN region_g2g='' THEN upper(region) ELSE upper(region_g2g) END region,
    upper(distributor_g2g) as distributor, cust_id AS store_id, store_name
    FROM `gt_schema.master_store_database_basis`
    WHERE spv_g2g <> '' AND spv_g2g <> '-' AND distributor_g2g<>'-'
    ORDER BY spv, region, distributor, store_name
"""

BQ_DDL = f"""
CREATE TABLE IF NOT EXISTS `{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}` (
    event_id             STRING    NOT NULL,
    spv                  STRING    NOT NULL,
    region               STRING    NOT NULL,
    distributor          STRING    NOT NULL,
    activity_key         STRING    NOT NULL,
    activity_label       STRING    NOT NULL,
    logged_at            TIMESTAMP NOT NULL,
    store_id             STRING,
    store_name           STRING,
    duration_seconds     INT64,
    started_at           TIMESTAMP,
    ended_at             TIMESTAMP,
    latitude             FLOAT64,
    longitude            FLOAT64,
    location_accuracy_m  INT64,
    created_at           TIMESTAMP,
    device_id            STRING
)
PARTITION BY DATE(logged_at)
CLUSTER BY spv, distributor, store_id
"""

# ============================================================
# GEOLOCATION — JS → URL query params → Python
# ============================================================
# Flow:
#   1. User clicks a button that requires GPS
#   2. We set a "awaiting_geo" flag + stash the pending entry in session_state, then st.rerun()
#   3. On the next render, we inject a hidden JS snippet via st.components.v1.html
#   4. The JS calls navigator.geolocation, then rewrites the page URL with
#      ?geo=lat,lng,acc&_geo_trigger=<key> and navigates there
#   5. Streamlit sees the new URL → re-renders → handle_incoming_geo() picks up the
#      params, completes the BQ write, clears the params

GEO_PARAM        = "geo"
GEO_TRIGGER_P1   = "p1_dist_in"
GEO_TRIGGER_P2   = "p2_first_activity"


def _inject_geolocation_trigger(trigger_key: str):
    """Inject JS that grabs GPS and encodes it into the Streamlit URL."""
    html = f"""
    <script>
    (function() {{
        var triggerKey = "{trigger_key}";

        function writeParams(value) {{
            var url = new URL(window.parent.location.href);
            url.searchParams.set("{GEO_PARAM}", value);
            url.searchParams.set("_geo_trigger", triggerKey);
            window.parent.location.replace(url.toString());
        }}

        if (!navigator.geolocation) {{
            writeParams("error:not_supported");
            return;
        }}

        navigator.geolocation.getCurrentPosition(
            function(pos) {{
                var v = pos.coords.latitude + "," + pos.coords.longitude + "," + Math.round(pos.coords.accuracy);
                writeParams(v);
            }},
            function(err) {{
                writeParams("error:" + err.message.replace(/,/g, ";"));
            }},
            {{ enableHighAccuracy: true, timeout: 10000, maximumAge: 0 }}
        );
    }})();
    </script>
    <p style="font-size:0.78rem;color:#6b7280;margin:4px 0;">📡 Mendapatkan koordinat GPS…</p>
    """
    components.html(html, height=35)


def _parse_geo_params() -> Tuple[Optional[float], Optional[float], Optional[int], Optional[str]]:
    """Return (lat, lng, accuracy_m, trigger_key) from URL query params, or all-None."""
    params = st.query_params
    geo_raw = params.get(GEO_PARAM, "")
    trigger  = params.get("_geo_trigger", "") or None
    if not geo_raw or not trigger:
        return None, None, None, None
    if geo_raw.startswith("error:"):
        return None, None, None, trigger
    try:
        parts = geo_raw.split(",")
        return float(parts[0]), float(parts[1]), int(parts[2]) if len(parts) > 2 else None, trigger
    except Exception:
        return None, None, None, trigger


def _clear_geo_params():
    """Remove geo-related query params without discarding other params."""
    keep = {k: v for k, v in st.query_params.items()
            if k not in (GEO_PARAM, "_geo_trigger")}
    st.query_params.clear()
    for k, v in keep.items():
        st.query_params[k] = v


# ============================================================
# BigQuery CLIENT
# ============================================================

def _load_from_streamlit_secrets(key: str) -> Tuple:
    if key == "connections":
        d = dict(st.secrets["connections"]["bigquery"])
    else:
        d = dict(st.secrets[key])
    if "private_key" in d:
        d["private_key"] = d["private_key"].replace("\\n", "\n")
    creds = service_account.Credentials.from_service_account_info(d)
    project = d.get("project_id") or d.get("project") or Config.BQ_PROJECT
    return creds, project


def _load_from_local_file() -> Tuple:
    if not os.path.exists(Config.CREDENTIALS_PATH):
        raise FileNotFoundError(f"Not found: {Config.CREDENTIALS_PATH}")
    creds = service_account.Credentials.from_service_account_file(Config.CREDENTIALS_PATH)
    return creds, Config.BQ_PROJECT


@st.cache_resource
def get_bq_client():
    for name, fn in [
        ("local JSON", _load_from_local_file),
        ("gcp_service_account", lambda: _load_from_streamlit_secrets("gcp_service_account")),
        ("connections.bigquery", lambda: _load_from_streamlit_secrets("connections")),
    ]:
        try:
            creds, project = fn()
            client = bigquery.Client(credentials=creds, project=project)
            logger.info(f"BQ client OK via {name}")
            return client
        except Exception as e:
            logger.warning(f"{name} failed: {e}")
    try:
        client = bigquery.Client()
        logger.info("BQ client OK via ADC")
        return client
    except Exception as e:
        logger.error(f"ADC failed: {e}")
        return None


def ensure_table(client: bigquery.Client):
    try:
        client.query(BQ_DDL).result()
    except Exception as e:
        logger.error(f"DDL failed: {e}")


# ============================================================
# MASTER DATA
# ============================================================

@st.cache_data(ttl=3600)
def load_master_data() -> dict:
    client = get_bq_client()
    if client is None:
        return {"spv_list": [], "by_spv": {}, "error": "Cannot connect to BigQuery."}
    try:
        rows = list(client.query(BQ_QUERY).result())
    except Exception as e:
        return {"spv_list": [], "by_spv": {}, "error": str(e)}

    by_spv: dict = {}
    for row in rows:
        spv = (row.spv or "").strip()
        reg = (row.region or "").strip()
        dist = (row.distributor or "").strip()
        sid = str(row.store_id or "").strip()
        sname = (row.store_name or "").strip()
        if not spv or not reg or not dist:
            continue
        bs = by_spv.setdefault(spv, {"regions": [], "by_region": {}})
        if reg not in bs["regions"]:
            bs["regions"].append(reg)
        br = bs["by_region"].setdefault(reg, {"distributors": [], "by_dist": {}})
        if dist not in br["distributors"]:
            br["distributors"].append(dist)
        br["by_dist"].setdefault(dist, [])
        existing = {s["store_id"] for s in br["by_dist"][dist]}
        if sid not in existing:
            br["by_dist"][dist].append({"store_id": sid, "store_name": sname})

    return {"spv_list": sorted(by_spv.keys()), "by_spv": by_spv}


# ============================================================
# BIGQUERY WRITE
# ============================================================

def _to_wib(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.astimezone(timezone(Config.TZ_OFFSET))


def _make_event_id(spv: str, activity_key: str, store_id: Optional[str], logged_at) -> str:
    ts_str = logged_at.isoformat() if isinstance(logged_at, datetime) else str(logged_at or "")
    raw = f"{spv}|{activity_key}|{store_id or ''}|{ts_str}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _serialize_row(row: dict) -> dict:
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}


def get_existing_event_ids(client: bigquery.Client, event_ids: List[str]) -> set:
    if not event_ids:
        return set()
    id_list = ", ".join(f"'{eid}'" for eid in event_ids)
    try:
        result = client.query(f"""
            SELECT event_id FROM `{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}`
            WHERE event_id IN ({id_list})
        """).result()
        return {row.event_id for row in result}
    except Exception as e:
        logger.warning(f"Dedup check failed: {e}")
        return set()


def write_single_p1_entry(
    spv: str, region: str, distributor: str, entry: dict,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    acc: Optional[int] = None,
) -> Tuple[bool, str]:
    client = get_bq_client()
    if client is None:
        return False, "No BQ client."
    created_at = datetime.now(timezone.utc)
    logged_at = _to_wib(datetime.fromtimestamp(entry["time"] / 1000, tz=timezone.utc))
    event_id = _make_event_id(spv, entry["key"], None, logged_at)
    row = {
        "event_id": event_id, "spv": spv, "region": region, "distributor": distributor,
        "activity_key": entry["key"], "activity_label": entry["label"],
        "logged_at": logged_at, "store_id": None, "store_name": None,
        "duration_seconds": None, "started_at": None, "ended_at": None,
        "latitude": lat, "longitude": lng, "location_accuracy_m": acc,
        "created_at": created_at, "device_id": None,
    }
    return _insert_rows(client, [row])


def write_single_store_session(
    spv: str, region: str, distributor: str, entry: dict,
) -> Tuple[bool, str]:
    client = get_bq_client()
    if client is None:
        return False, "No BQ client."
    created_at = datetime.now(timezone.utc)
    duration_ms = entry.get("duration_ms", 0)
    duration_s = int(duration_ms / 1000)
    ended_at = _to_wib(datetime.fromisoformat(entry["ended_at"].replace("Z", "+00:00")))
    started_at = (ended_at - timedelta(seconds=duration_s)) if ended_at and duration_s else None
    store_id = str(entry.get("store_id", "")) or None
    event_id = _make_event_id(spv, entry["activity_key"], store_id, ended_at)
    row = {
        "event_id": event_id, "spv": spv, "region": region, "distributor": distributor,
        "activity_key": entry["activity_key"], "activity_label": entry["activity_label"],
        "logged_at": ended_at, "store_id": store_id, "store_name": entry.get("store_name"),
        "duration_seconds": duration_s, "started_at": started_at, "ended_at": ended_at,
        "latitude": entry.get("latitude"), "longitude": entry.get("longitude"),
        "location_accuracy_m": entry.get("location_accuracy_m"),
        "created_at": created_at, "device_id": None,
    }
    return _insert_rows(client, [row])


def _insert_rows(client: bigquery.Client, rows: list) -> Tuple[bool, str]:
    all_ids = [r["event_id"] for r in rows]
    existing = get_existing_event_ids(client, all_ids)
    new_rows = [r for r in rows if r["event_id"] not in existing]
    skipped = len(rows) - len(new_rows)

    if not new_rows:
        return True, f"ℹ️ {len(rows)} records already saved — no duplicates inserted."

    table_ref = f"{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}"
    try:
        errors = client.insert_rows_json(
            table_ref,
            [_serialize_row(r) for r in new_rows],
            row_ids=[r["event_id"] for r in new_rows],
        )
        if errors:
            return False, f"BQ insert errors: {errors}"
        msg = f"✅ {len(new_rows)} records saved to BigQuery."
        if skipped:
            msg += f" ({skipped} duplicates skipped)"
        return True, msg
    except Exception as e:
        logger.error(f"BQ insert exception: {e}")
        return False, str(e)


# ============================================================
# SESSION STATE INIT
# ============================================================

def init_state():
    defaults = {
        "page": "setup",
        "spv": "",
        "region": "",
        "distributor": "",
        # P1
        "p1_checked_in": False,
        "p1_pending_entry": None,    # entry dict waiting for GPS before BQ write
        "p1_awaiting_geo": False,
        # P2
        "current_activity_key": "",
        "current_activity_label": "",
        "timer_running": False,
        "timer_elapsed_ms": 0,
        "timer_started_at": None,
        "selected_store_id": "",
        "selected_store_name": "",
        "store_geo_done": set(),     # store_ids whose first-activity GPS is already captured
        "p2_pending_entry": None,    # session dict waiting for GPS before BQ write
        "p2_awaiting_geo": False,
        "totals": {},
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ============================================================
# HELPERS
# ============================================================

ACTIVITIES = [
    {"key": "preparation",        "label": "Preparation",        "icon": "🗂️"},
    {"key": "greetings",          "label": "Greetings",          "icon": "👋"},
    {"key": "stock_check",        "label": "Stock Check",        "icon": "📦"},
    {"key": "merchandise",        "label": "Merchandise",        "icon": "🛍️"},
    {"key": "payment",            "label": "Payment",            "icon": "💳"},
    {"key": "sales_presentation", "label": "Sales Presentation", "icon": "📊"},
    {"key": "taking_order",       "label": "Taking Order",       "icon": "📝"},
]

P1_ACTIONS = {
    "dist_in":  {"label": "Check In Distributor",  "icon": "📥"},
    "dist_out": {"label": "Check Out Distributor", "icon": "📤"},
    "ish_in":   {"label": "Check In ISHOMA",       "icon": "🍽️"},
    "ish_out":  {"label": "Check Out ISHOMA",      "icon": "🍽️"},
}

ACTIVITY_MAP = {a["key"]: a for a in ACTIVITIES}


def fmt_ms(ms: int) -> str:
    s = max(int(ms / 1000), 0)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}"


def fmt_clock(epoch_ms: int) -> str:
    dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone(Config.TZ_OFFSET))
    return dt.strftime("%H:%M:%S")


def has_checked_in() -> bool:
    return st.session_state.p1_checked_in


def get_store_list() -> list:
    master = st.session_state.get("master", {})
    return (
        master.get("by_spv", {})
              .get(st.session_state.spv, {})
              .get("by_region", {})
              .get(st.session_state.region, {})
              .get("by_dist", {})
              .get(st.session_state.distributor, [])
    )


def get_live_elapsed_ms() -> int:
    if st.session_state.timer_running and st.session_state.timer_started_at:
        delta = datetime.now(timezone.utc) - st.session_state.timer_started_at
        return st.session_state.timer_elapsed_ms + int(delta.total_seconds() * 1000)
    return st.session_state.timer_elapsed_ms


# ============================================================
# GEO INTERCEPTION — called at top of every render cycle
# ============================================================

def handle_incoming_geo():
    """
    If URL contains geo query params from a previous JS geolocation call,
    complete the deferred BQ write and clear the params.
    """
    lat, lng, acc, trigger = _parse_geo_params()
    if not trigger:
        return

    # ── P1: distributor check-in ──────────────────────────────
    if trigger == GEO_TRIGGER_P1 and st.session_state.p1_awaiting_geo:
        entry = st.session_state.p1_pending_entry
        if entry:
            _clear_geo_params()
            st.session_state.p1_awaiting_geo = False
            st.session_state.p1_pending_entry = None

            with st.spinner("Menyimpan Check In ke BigQuery…"):
                ok, msg = write_single_p1_entry(
                    st.session_state.spv, st.session_state.region,
                    st.session_state.distributor, entry,
                    lat=lat, lng=lng, acc=acc,
                )
            if ok:
                st.session_state.p1_checked_in = True
                geo_str = f"📍 {lat:.5f}, {lng:.5f} ±{acc}m" if lat is not None else "📍 lokasi tidak tersedia"
                st.success(f"📥 **Check In Distributor** disimpan · {geo_str}")
            else:
                st.error(f"❌ Gagal menyimpan Check In: {msg}")

    # ── P2: first store activity ──────────────────────────────
    elif trigger == GEO_TRIGGER_P2 and st.session_state.p2_awaiting_geo:
        entry = st.session_state.p2_pending_entry
        if entry:
            _clear_geo_params()
            st.session_state.p2_awaiting_geo = False
            st.session_state.p2_pending_entry = None

            entry["latitude"]            = lat
            entry["longitude"]           = lng
            entry["location_accuracy_m"] = acc

            store_id = entry.get("store_id", "")
            if store_id:
                st.session_state.store_geo_done.add(store_id)

            with st.spinner("Menyimpan sesi ke BigQuery…"):
                ok, msg = write_single_store_session(
                    st.session_state.spv, st.session_state.region,
                    st.session_state.distributor, entry,
                )
            if ok:
                geo_str = f"📍 {lat:.5f}, {lng:.5f} ±{acc}m" if lat is not None else "📍 lokasi tidak tersedia"
                st.success(f"✅ **{entry['activity_label']}** — {fmt_ms(entry['duration_ms'])} · {geo_str}")
            else:
                st.error(f"❌ Gagal menyimpan sesi: {msg}")
    else:
        _clear_geo_params()  # stale params


# ============================================================
# PAGES
# ============================================================

def render_setup():
    st.markdown("## ⏱ Time Motion")
    st.markdown("### Selamat Datang")
    st.markdown("Pilih SPV, Region, dan Distributor sebelum memulai kunjungan.")
    st.divider()

    master = st.session_state.get("master", {})
    spv_list = master.get("spv_list", [])

    col1, col2 = st.columns(2)
    with col1:
        spv = st.selectbox("👤 Supervisor (SPV)", [""] + spv_list, key="sel_spv")

    region_list = []
    if spv:
        region_list = master.get("by_spv", {}).get(spv, {}).get("regions", [])
    with col2:
        region = st.selectbox("🗺️ Region", [""] + region_list, key="sel_region",
                              disabled=not spv)

    dist_list = []
    if spv and region:
        dist_list = (master.get("by_spv", {})
                           .get(spv, {})
                           .get("by_region", {})
                           .get(region, {})
                           .get("distributors", []))

    distributor = st.selectbox("🏭 Distributor", [""] + dist_list, key="sel_dist",
                               disabled=not region)

    st.markdown("")
    ready = bool(spv and region and distributor)
    if st.button("Mulai Kunjungan →", type="primary", disabled=not ready, use_container_width=True):
        st.session_state.spv = spv
        st.session_state.region = region
        st.session_state.distributor = distributor
        st.session_state.page = "checkin"
        st.rerun()


def render_checkin():
    col_title, col_badge = st.columns([2, 1])
    with col_title:
        st.markdown("## ⏱ Sales Timer")
        st.caption("Check In / Out & ISHOMA")
    with col_badge:
        st.info(f"**{st.session_state.spv}**  \n{st.session_state.distributor} · {st.session_state.region}")

    tab_checkin, tab_activities = st.tabs(["📍 Check In/Out", "📋 Activities"])

    with tab_checkin:
        _render_p1_tab()

    with tab_activities:
        if not has_checked_in():
            st.warning("⚠️ Wajib Check In Distributor terlebih dahulu.")
        else:
            _render_p2_tab()

    st.divider()
    if st.button("← Kembali ke Halaman Awal"):
        if st.session_state.timer_running:
            st.session_state.timer_elapsed_ms = get_live_elapsed_ms()
            st.session_state.timer_running = False
            st.session_state.timer_started_at = None
        st.session_state.page = "setup"
        st.rerun()


# ============================================================
# P1 TAB — Check In/Out
# ============================================================

def _render_p1_tab():
    st.markdown("#### 📋 Catat Aktivitas Check In/Out")
    st.caption("Check In Distributor merekam lokasi GPS secara otomatis.")

    # While GPS is being fetched, show spinner + inject JS and wait
    if st.session_state.p1_awaiting_geo:
        st.info("📡 Mengakses GPS… Mohon izinkan akses lokasi di browser Anda.")
        _inject_geolocation_trigger(GEO_TRIGGER_P1)
        return  # halt render until URL reloads with geo params

    act_options = {f"{v['icon']} {v['label']}": k for k, v in P1_ACTIONS.items()}
    chosen_label = st.selectbox(
        "Pilih Aktivitas",
        ["— Pilih Aktivitas —"] + list(act_options.keys()),
        key="p1_sel",
    )

    if st.button("✅ Catat Waktu Sekarang", type="primary", use_container_width=True):
        if chosen_label == "— Pilih Aktivitas —":
            st.warning("Pilih aktivitas terlebih dahulu!")
        else:
            key = act_options[chosen_label]
            meta = P1_ACTIONS[key]
            entry = {
                "key":   key,
                "label": meta["label"],
                "icon":  meta["icon"],
                "time":  int(datetime.now(timezone.utc).timestamp() * 1000),
            }

            if key == "dist_in":
                # Stash entry and request GPS — BQ write happens after geo returns
                st.session_state.p1_pending_entry = entry
                st.session_state.p1_awaiting_geo  = True
                st.rerun()
            else:
                # Other P1 actions: write immediately without GPS
                with st.spinner("Menyimpan ke BigQuery…"):
                    ok, msg = write_single_p1_entry(
                        st.session_state.spv, st.session_state.region,
                        st.session_state.distributor, entry,
                    )
                if ok:
                    st.success(f"{meta['icon']} **{meta['label']}** — {msg}")
                    if key == "dist_out":
                        st.session_state.p1_checked_in = False
                else:
                    st.error(f"❌ Gagal menyimpan: {msg}")


# ============================================================
# P2 TAB — Activities / Stopwatch
# ============================================================

def _render_p2_tab():
    # While GPS is being fetched for first store activity, show spinner + inject JS
    if st.session_state.p2_awaiting_geo:
        st.info("📡 Mengakses GPS lokasi toko… Mohon izinkan akses lokasi di browser Anda.")
        _inject_geolocation_trigger(GEO_TRIGGER_P2)
        return  # halt render until URL reloads with geo params

    st.markdown("#### 🏪 Pilih Toko")

    stores = get_store_list()
    store_options = {f"{s['store_name']} ({s['store_id']})": s for s in stores}
    current_store_label = ""
    if st.session_state.selected_store_id:
        match = next((k for k, v in store_options.items()
                      if v["store_id"] == st.session_state.selected_store_id), "")
        current_store_label = match

    store_choice = st.selectbox(
        "Cari / Pilih Toko",
        ["— Pilih Toko —"] + list(store_options.keys()),
        index=(list(store_options.keys()).index(current_store_label) + 1
               if current_store_label else 0),
        key="p2_store_sel",
    )

    if store_choice != "— Pilih Toko —":
        chosen = store_options[store_choice]
        if chosen["store_id"] != st.session_state.selected_store_id:
            st.session_state.selected_store_id   = chosen["store_id"]
            st.session_state.selected_store_name = chosen["store_name"]
            st.rerun()

    # GPS status badge for current store
    store_id = st.session_state.selected_store_id
    if store_id:
        if store_id in st.session_state.store_geo_done:
            st.markdown('<span class="geo-badge">📍 Lokasi toko sudah terekam</span>',
                        unsafe_allow_html=True)
        else:
            st.markdown('<span class="geo-badge geo-badge-warn">📍 GPS akan direkam pada aktivitas pertama</span>',
                        unsafe_allow_html=True)

    st.divider()
    st.markdown("#### 🗂 Stopwatch Aktivitas")

    act_list = ["— Pilih Aktivitas —"] + [f"{a['icon']} {a['label']}" for a in ACTIVITIES]
    act_label_to_key = {f"{a['icon']} {a['label']}": a["key"] for a in ACTIVITIES}

    current_act_label = ""
    if st.session_state.current_activity_key:
        a = ACTIVITY_MAP.get(st.session_state.current_activity_key)
        if a:
            current_act_label = f"{a['icon']} {a['label']}"

    act_choice = st.selectbox(
        "Pilih Aktivitas",
        act_list,
        index=(act_list.index(current_act_label) if current_act_label in act_list else 0),
        key="p2_act_sel",
    )

    if act_choice != "— Pilih Aktivitas —":
        new_key = act_label_to_key[act_choice]
        if new_key != st.session_state.current_activity_key:
            if st.session_state.timer_running:
                st.session_state.timer_elapsed_ms = get_live_elapsed_ms()
                st.session_state.timer_running = False
                st.session_state.timer_started_at = None
            st.session_state.current_activity_key   = new_key
            a = ACTIVITY_MAP[new_key]
            st.session_state.current_activity_label = f"{a['icon']} {a['label']}"
            st.session_state.timer_elapsed_ms = 0
            st.rerun()

    # ── Clock display ─────────────────────────────────────────
    elapsed  = get_live_elapsed_ms()
    act_name = st.session_state.current_activity_label or "— None Selected —"

    st.markdown(f"""
    <div style="background:#12151c;border:1px solid rgba(255,255,255,0.08);border-radius:14px;
                padding:24px;text-align:center;position:relative;overflow:hidden;">
      <div style="position:absolute;top:0;left:0;right:0;height:3px;
                  background:linear-gradient(90deg,#f5a623,#f97316,#e05c5c);"></div>
      <div style="font-size:0.68rem;font-weight:700;letter-spacing:2px;
                  text-transform:uppercase;color:#6b7280;margin-bottom:4px;">Current Activity</div>
      <div style="font-size:1.05rem;font-weight:600;color:#f5a623;margin-bottom:12px;">{act_name}</div>
      <div style="font-family:monospace;font-size:3.5rem;font-weight:700;
                  color:{'#4ade80' if st.session_state.timer_running else '#f5a623' if elapsed>0 else '#e8eaf0'};
                  letter-spacing:-2px;">{fmt_ms(elapsed)}</div>
      <div style="font-size:0.72rem;color:#6b7280;margin-top:8px;letter-spacing:1px;text-transform:uppercase;">
        {'🟢 Recording' if st.session_state.timer_running else '⏸ Paused' if elapsed>0 else '○ Idle'}
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Controls ──────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)

    with col1:
        start_disabled = st.session_state.timer_running or not st.session_state.current_activity_key
        if st.button("▶ Start", type="primary", disabled=start_disabled, use_container_width=True):
            if not st.session_state.selected_store_id:
                st.warning("Pilih toko terlebih dahulu!")
            else:
                st.session_state.timer_running    = True
                st.session_state.timer_started_at = datetime.now(timezone.utc)
                st.rerun()

    with col2:
        if st.button("⏸ Pause", disabled=not st.session_state.timer_running, use_container_width=True):
            st.session_state.timer_elapsed_ms = get_live_elapsed_ms()
            st.session_state.timer_running    = False
            st.session_state.timer_started_at = None
            st.rerun()

    with col3:
        if st.button("⏹ Stop & Save", type="secondary", use_container_width=True):
            _stop_and_save_to_bq()

    # ── Auto-refresh while running ────────────────────────────
    if st.session_state.timer_running:
        import time
        time.sleep(1)
        st.rerun()

    # ── Totals ────────────────────────────────────────────────
    st.divider()
    st.markdown('<div class="section-label">📊 Total Waktu Per Aktivitas (sesi ini)</div>',
                unsafe_allow_html=True)
    live_elapsed = get_live_elapsed_ms()
    cols = st.columns(4)
    for i, a in enumerate(ACTIVITIES):
        total = st.session_state.totals.get(a["key"], 0)
        if st.session_state.current_activity_key == a["key"]:
            total += live_elapsed
        with cols[i % 4]:
            st.metric(label=f"{a['icon']} {a['label']}", value=fmt_ms(total))


# ============================================================
# STOP & SAVE
# ============================================================

def _stop_and_save_to_bq():
    """
    Stop the timer and write the session to BigQuery.
    If this is the first activity for the selected store, capture GPS first.
    """
    elapsed = get_live_elapsed_ms()

    if elapsed <= 0 or not st.session_state.current_activity_key:
        st.session_state.timer_elapsed_ms       = 0
        st.session_state.timer_running          = False
        st.session_state.timer_started_at       = None
        st.session_state.current_activity_key   = ""
        st.session_state.current_activity_label = ""
        st.rerun()
        return

    key      = st.session_state.current_activity_key
    store_id = st.session_state.selected_store_id
    ended_at = datetime.now(timezone.utc).isoformat()

    session_entry = {
        "activity_key":   key,
        "activity_label": st.session_state.current_activity_label,
        "store_id":       store_id,
        "store_name":     st.session_state.selected_store_name or "—",
        "duration_ms":    elapsed,
        "ended_at":       ended_at,
        "latitude":       None,
        "longitude":      None,
        "location_accuracy_m": None,
    }

    # Update running totals
    st.session_state.totals[key] = st.session_state.totals.get(key, 0) + elapsed

    # Reset timer
    st.session_state.timer_elapsed_ms       = 0
    st.session_state.timer_running          = False
    st.session_state.timer_started_at       = None
    st.session_state.current_activity_key   = ""
    st.session_state.current_activity_label = ""

    # First activity for this store → fetch GPS, then write
    if store_id and store_id not in st.session_state.store_geo_done:
        st.session_state.p2_pending_entry = session_entry
        st.session_state.p2_awaiting_geo  = True
        st.rerun()
    else:
        # GPS already recorded for this store — write immediately
        with st.spinner("Menyimpan sesi ke BigQuery…"):
            ok, msg = write_single_store_session(
                st.session_state.spv,
                st.session_state.region,
                st.session_state.distributor,
                session_entry,
            )
        if ok:
            st.success(f"✅ **{session_entry['activity_label']}** — {fmt_ms(elapsed)} — {msg}")
        else:
            st.error(f"❌ Gagal menyimpan sesi: {msg}")
        st.rerun()


# ============================================================
# MAIN
# ============================================================

def main():
    init_state()

    # Must run before any page rendering so geo params are consumed first
    handle_incoming_geo()

    if "master" not in st.session_state:
        with st.spinner("⏳ Memuat data dari BigQuery…"):
            st.session_state.master = load_master_data()
        client = get_bq_client()
        if client:
            ensure_table(client)

    master = st.session_state.master
    if "error" in master and master.get("spv_list") == []:
        st.error(f"⚠️ BigQuery error: {master['error']}")
    elif not master.get("spv_list"):
        st.warning("⚠️ Data kosong. Periksa query / kredensial BigQuery.")

    if st.session_state.page == "setup":
        render_setup()
    else:
        render_checkin()


if __name__ == "__main__":
    main()
